"""User code runs here, never in the API process (§10 worker protocol).

`python -m solera_worker run --objects URL --run RUN --attempt ID`:
read the spec from the attempt file -> refuse on revision mismatch -> resolve `env:` -> load inputs per
annotation (Incremental edges through the upstream key index) -> build ctx ->
run the producer -> for each returned output, work out what changed against
its key index and write the delta file -> take the write fence -> store()
each output unless nothing changed -> rewrite the attempt file with the spec,
the result and the log index, in one PUT. All along, a thread beats, and
the timeline records each step for the run's history.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import importlib
import importlib.util
import inspect
import json
import os
import resource
import secrets
import socket
import sys
import time
import traceback
import typing
from pathlib import Path

from obstore.exceptions import AlreadyExistsError
from solera import lifecycle
from solera.keys.index import DeltaFiles, FileInfo, IndexState, KeyIndex, key_bytes, key_str
from solera.keys.io import ObjectIO, key_cache
from solera.lifecycle import Cancel, Ended
from solera.objects import create
from solera.sdk import (
    UNSET,
    Asset,
    Changes,
    Project,
    Ref,
    Result,
    TimePartitions,
    is_ref_type,
    split_partition,
)
from solera.stores import (
    Batches,
    Keys,
    Patch,
    Scope,
    Sql,
    StoreError,
    WriteError,
    resolve_env,
    store_key_rows,
)

from .reporting import LogShipper, Reporter


def _load_module(path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_project(entrypoint: str) -> Project:
    target, separator, attribute = entrypoint.rpartition(":")
    if target.endswith(".py") or (not separator and entrypoint.endswith(".py")):
        path = Path(target or entrypoint).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Project file not found: {path}")
        module = _load_module(path)
        if separator:
            project = getattr(module, attribute)
        else:
            project = getattr(module, "project", None)
            if project is None:
                candidates = [v for v in vars(module).values() if isinstance(v, Project)]
                if len(candidates) != 1:
                    raise TypeError("Project file must define a `project` or use file.py:attribute")
                project = candidates[0]
    else:
        project = getattr(
            importlib.import_module(target if separator else entrypoint),
            attribute if separator else "project",
        )
    if callable(project) and not isinstance(project, Project):
        project = project()
    if not isinstance(project, Project):
        raise TypeError("Entrypoint must be a Project or a factory returning one")
    return project


def _objects(url: str):
    import obstore

    return obstore.store.from_url(url)


async def _put(objects, key: str, value: bytes):
    import obstore

    await obstore.put_async(objects, key, value, mode="overwrite", use_multipart=False)


async def _get(objects, key: str) -> bytes | None:
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        result = await obstore.get_async(objects, key)
    except (NotFoundError, FileNotFoundError):  # the local store raises the latter
        return None
    return bytes(await result.bytes_async())


class Aborted(Exception):
    """The engine took the attempt's gate first: it may write nothing."""


MAX_EVENTS = 1_000  # per attempt: past this, marks and lazy loads are not recorded


def _rows(value) -> int | None:
    """How many rows a value holds, if it has a length: a list, a DataFrame."""

    if isinstance(value, (str, bytes, dict)):
        return None
    try:
        return len(value)
    except Exception:
        return None


def _cpu_seconds() -> float:
    own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


class Timeline:
    """What the attempt did, when, by this worker's clock (§7): `booted`,
    `imported`, `loaded` (each input), `computing`, `mark` (each `ctx.mark`),
    `computed`, `writing` (the fence is ours), `stored` (each output),
    `finished`. Every heartbeat carries it, and so does the result, with
    what the attempt used: CPU seconds, and — in a process of its own —
    peak memory."""

    def __init__(self, own_process: bool):
        self.events: list[dict] = []
        self.own_process = own_process
        self._cpu = _cpu_seconds()

    def add(self, type_: str, name=None, rows=None, *, optional=False):
        if optional and len(self.events) >= MAX_EVENTS:
            return
        event = {"type": type_, "at": time.time()}
        if name is not None:
            event["name"] = str(name)[:200]
        if rows is not None:
            event["rows"] = rows
        self.events.append(event)

    def report(self) -> dict:
        usage = {"cpu_seconds": round(_cpu_seconds() - self._cpu, 3)}
        if self.own_process:
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            usage["peak_memory"] = peak if sys.platform == "darwin" else peak * 1024
        return {"events": list(self.events), "usage": usage}


class Ctx:
    """The `ctx` argument handed to producers (§2)."""

    def __init__(self, spec, asset: Asset, project: Project, objects, changes, shipper, timeline):
        self._spec, self._objects, self._shipper, self._timeline = spec, objects, shipper, timeline
        self.partition: str = spec["partition"]
        declared = project.manifest["assets"][asset.name]["partitions"]
        self.partitions: dict[str, str] = (
            split_partition(declared["dims"], self.partition) if declared else {}
        )
        self.partition_window = self._window(project, asset)
        self.cursor = spec.get("cursor")
        self.changes = changes
        self.run_id = spec["run"]["id"]
        self.config = spec["run"].get("config") or {}
        self.execution = spec.get("execution") or {
            "executor": "local",
            "kind": "Local",
            "environment": {},
            "placement": {},
        }
        self._stores = project.stores
        self._outputs = [o.name or asset.name for o in asset.outputs]
        self._metadata: dict[str, dict] = {}

    def _window(self, project, asset):
        declared = project.manifest["assets"][asset.name]["partitions"]
        if not declared:
            return None
        windows = {}
        for name, spec in declared["dims"].items():
            if spec["kind"] == "time":
                tp = TimePartitions(
                    spec["start"],
                    spec["every"],
                    end=spec.get("end"),
                    end_offset=spec.get("end_offset"),
                    timezone=spec["timezone"],
                    format=spec["format"],
                )
                windows[name] = tp.window(self.partitions[name])
        if len(windows) == 1:
            return next(iter(windows.values()))
        return windows or None

    def log(self, message: str, level: str = "info", **fields):
        entry = {"at": time.time(), "level": level, "message": str(message), "fields": fields}
        json.dumps(entry, allow_nan=False)
        self._shipper.append(entry)

    async def load(self, ref: Ref, t):
        store = self._stores[ref.store]
        value = await store.load(ref, t, None)
        self._timeline.add("loaded", ref.output, _rows(value), optional=True)
        return value

    def mark(self, name: str):
        """Mark a moment in the run's timeline — `ctx.mark("trained")` —
        to see where the time went between the attempt's own steps."""

        self._timeline.add("mark", name, optional=True)

    def metadata(self, output: str | None = None, /, **values):
        """Record facts about the version this attempt writes — row counts,
        a checksum, a model's score — in the run history (§7), to chart
        across versions. `output` defaults to the asset's only output."""

        if output is None:
            if len(self._outputs) != 1:
                raise ValueError("ctx.metadata: name the output (this asset has several, or none)")
            output = self._outputs[0]
        if output not in self._outputs:
            raise ValueError(f"ctx.metadata: {output!r} is not an output of this asset")
        json.dumps(values, allow_nan=False)
        self._metadata.setdefault(output, {}).update(values)

    def _recorded(self, result) -> dict[str, dict]:
        """What `metadata` recorded, and what the result's `metadata` adds."""

        out = {name: dict(values) for name, values in self._metadata.items()}
        for name, values in ((result.metadata or {}) if isinstance(result, Result) else {}).items():
            if name not in self._outputs:
                raise StoreError(f"Result.metadata names {name!r}, which is not an output of this asset")
            json.dumps(values, allow_nan=False)
            out.setdefault(name, {}).update(values)
        return out


async def _resolve_inputs(spec, project, asset, keys_io, timeline):
    """Load each pin by annotation; build call args + ctx.changes (§5, §10).

    An Incremental edge over a keyed upstream reads its page from the pinned
    key index — the pending deltas in `[from, to]`, or the whole index for a
    full delivery — and loads just those keys. `delivered` reports where the
    page ended, for the engine's watermark (§6). Each input loaded is a
    `loaded` event."""

    manifest_asset = project.manifest["assets"][asset.name]
    edges = manifest_asset["inputs"]
    hints = typing.get_type_hints(asset.fn)
    args, changes, delivered = {}, {}, {}
    for name, pin in spec["inputs"].items():
        edge = edges.get(name)
        if edge is None:  # a dep pin: recorded, never bound
            continue
        param = name
        t = hints.get(param)
        if "refs" in pin:  # AllPartitions
            inner = _dict_inner(t)
            out = {}
            for key, ref_json in pin["refs"].items():
                ref = Ref.from_json(ref_json)
                out[key] = (
                    ref
                    if (inner is not None and is_ref_type(inner))
                    else await project.stores[ref.store].load(ref, inner, None)
                )
            args[param] = out
            timeline.add("loaded", param)
            continue
        ref = Ref.from_json(pin["ref"])
        store = project.stores[ref.store]
        if "changes" in pin:  # Incremental: selection + ctx.changes (§5.1)
            ch = pin["changes"]
            full = bool(ch.get("full"))
            if "batches" in ch:
                lo, hi = (int(v) for v in ch["batches"])
                args[param] = await store.load(ref, t, Batches(lo, hi))
                changes[param] = Changes(rows=args[param], batches=range(lo, hi + 1), full=full)
                timeline.add("loaded", param, _rows(args[param]))
                continue
            if "keys" in ch:  # a run's keys= override: a one-off selection
                upserted, deleted, after = {str(k): (b"", 0) for k in ch["keys"]}, (), None
            else:
                index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
                start = key_bytes(ch["after"]) if ch.get("after") is not None else None
                if full:
                    keys, versions, locators, nxt = await index.page(start, int(ch["limit"]))
                    flags = bytes(len(keys))
                else:
                    keys, versions, flags, locators, nxt = await index.pending(
                        int(ch["from"]), int(ch["to"]), start, int(ch["limit"])
                    )
                upserted = {
                    key_str(k): (v, loc)
                    for k, v, d, loc in zip(keys, versions, flags, locators, strict=True)
                    if not d
                }
                deleted = tuple(key_str(k) for k, d in zip(keys, flags, strict=True) if d)
                after = key_str(nxt) if nxt is not None else None
            args[param] = await store.load(ref, t, Keys(upserted))
            changes[param] = Changes(
                rows=args[param], deleted=deleted, full=full, upserted=tuple(sorted(upserted))
            )
            delivered[param] = {"after": after, "upserted": sorted(upserted), "deleted": list(deleted)}
            timeline.add("loaded", param, _rows(args[param]))
            continue
        if t is not None and is_ref_type(t):
            args[param] = ref
        else:
            args[param] = await store.load(ref, t, None)
            timeline.add("loaded", param, _rows(args[param]))
    return args, changes, delivered


def _dict_inner(t):
    if t is None:
        return None
    if typing.get_origin(t) in (dict, dict):
        args_ = typing.get_args(t)
        if len(args_) == 2:
            return args_[1]
    return None


async def _store_outputs(spec, project, asset, objects, keys_io, result_value, fence, writes, timeline):
    """Store each returned output (§4, §6, §8, §9), in two phases.

    Planning compares each keyed output's write with its key index as pinned
    in the spec, and writes the changes as the batch's delta file: a write
    that changes nothing is not stored at all and keeps the head. Then
    `fence(intents)` takes the attempt's write fence, listing those delta
    files — the keys this attempt is about to change — and only then do the
    stores write. An engine that finds the fence taken by a worker that then
    died keeps the outputs unsettled, and their intents, for the next attempt
    to repair (`_repair`). Returns `{name: entry}` and the cursor."""

    manifest_asset = project.manifest["assets"][asset.name]
    declared = {o["name"]: o for o in manifest_asset["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    priors = {name: Ref.from_json(r) for name, r in (spec.get("prior") or {}).items()}
    pinned = spec.get("outputs") or {}

    if isinstance(result_value, Result):
        values, cursor = result_value.outputs, result_value.cursor
    elif not decls:
        values, cursor = {}, UNSET  # a job: no outputs, return value ignored
    elif len(decls) == 1:
        values, cursor = {next(iter(decls)): result_value}, UNSET
    elif isinstance(result_value, dict) and set(result_value) <= set(decls):
        values, cursor = result_value, UNSET
    else:
        raise StoreError(f"{asset.name}: multi-output assets must return Result(outputs={{...}})")

    # Plan: what each keyed write changes, as delta files.
    plans, intents, entries = {}, {}, {}
    for name, value in values.items():
        if name not in decls:
            raise StoreError(f"{asset.name}: returned undeclared output {name!r}")
        output = decls[name]
        store = project.stores[declared[name]["store"]]
        info = pinned.get(name) or {}
        prior = priors.get(name)
        plan = plans[name] = {"output": output, "store": store, "info": info, "prior": prior}
        if info.get("index") is None:
            if isinstance(value, Sql) and output.incremental:
                raise WriteError(f"{output.name}: Sql writes need a keyed output")
            continue
        index = plan["index"] = KeyIndex(keys_io, None, IndexState.from_json(info["index"]))
        if isinstance(value, Sql):
            if output.is_partition_set:
                raise WriteError(f"{output.name}: Sql writes need a table output")
            # Rows the harness never sees: the store reports the whole new key
            # map once it wrote, so its delta comes after — and needs no repair.
            intents[name] = DeltaFiles([], 0, 0, True).to_json()
            continue
        patch = isinstance(value, Patch)
        # With no prior (a first write, or a full run) a Patch is the whole content.
        replace = not patch or prior is None
        content = value.rows if patch else value
        unsettled = info.get("unsettled") or []
        batch, attempt = int(info["batch"]), spec["attempt"]
        generation = int(spec.get("generation") or 0)  # each entry's locator (lifecycle.md §9.8)
        rows = await asyncio.to_thread(store_key_rows, store, content, output)
        if replace:
            # Every written key against every live one, streamed: the delta goes out as it fills.
            try:
                files, changed = await index.replace(
                    rows, batch, attempt, collect=LISTED, generation=generation
                )
            except ValueError as e:  # a value with no digest, found as the join reaches it
                raise WriteError(f"{output.name}: {e}") from e
        else:
            new = await _versions(output, rows)
            removes = [str(k) for k in value.remove if str(k) not in new]
            own = set(new), set(removes)
            intended = set(await _intended(info, keys_io, unsettled)) if unsettled else set()
            if unsettled:
                new, removes = await _repair(output, store, prior, intended - own[0] - own[1], new, removes)
            delta = await index.changes(
                [key_bytes(k) for k in new],
                list(new.values()),
                [key_bytes(k) for k in removes],
                generation=generation,
            )
            files = await index.write(batch, attempt, delta)
            changed = (
                [k for k, d in zip(delta.keys, delta.deleted, strict=True) if not d],
                [k for k, d in zip(delta.keys, delta.deleted, strict=True) if d],
            )
        if not files.files and info.get("exists") and not unsettled:
            entries[name] = {"unchanged": True}
            del plans[name]
            continue
        plan["keys"] = intents[name] = files.to_json()
        if prior is not None and not (replace and (changed is None or unsettled)):
            # The store writes only what changes: the delta, and for a patch whatever a
            # dead attempt may have left half-done among its keys. A replacement with
            # more changes than it lists, or with dead attempts', rewrites the scope.
            upserted, deleted = ({key_str(k) for k in keys} for keys in changed)
            if replace:
                plan["upserts"], plan["removes"] = frozenset(upserted), frozenset(deleted)
            else:
                plan["upserts"] = frozenset((upserted & own[0]) | (own[0] & intended))
                plan["removes"] = frozenset(deleted | (own[1] & intended))
        if output.is_partition_set:
            if replace:
                elements = {str(e) for e in content or ()}
            else:
                elements = (set(info.get("elements") or ()) - set(removes)) | set(new)
            plan["elements"] = sorted(elements)

    try:
        if plans:  # the gate is taken only by a worker about to write (docs/lifecycle.md §2.4)
            await fence(intents)
    except Aborted:
        # The engine has discarded this attempt's delta files; these came after.
        import obstore

        paths = [plans[n]["index"].path(f["name"]) for n, files in intents.items() for f in files["files"]]
        with contextlib.suppress(Exception):
            await obstore.delete_async(objects, paths)
        raise

    # Write: nothing reaches a store before the fence is ours.
    for name, plan in plans.items():
        value, output, store, info, prior = (
            values[name],
            plan["output"],
            plan["store"],
            plan["info"],
            plan["prior"],
        )
        store_name = declared[name]["store"]
        schema = None
        if output.migrations:
            migrate = getattr(store, "migrate", None)
            if not callable(migrate):
                raise StoreError(
                    f"{output.name}: store {store_name!r} has no migrate for declared migrations"
                )
            try:
                applied = await writes.call(migrate(output, output.migrations))
            except StoreError:
                raise
            except Exception as error:
                raise StoreError(f"{output.name}: migration failed: {error}") from error
            schema = applied[-1] if applied else output.migrations[-1].name
        scope = Scope(
            output=output,
            partition=spec["partition"],
            batch=info.get("batch"),
            attempt=spec["attempt"],
            aliases=tuple(info.get("aliases") or ()),
            upserts=plan.get("upserts"),
            removes=plan.get("removes"),
        )
        written = await writes.call(store.store(value, prior, scope))
        entry = {}
        if "index" in plan and isinstance(value, Sql):
            if written.keys is None:
                raise StoreError(f"{output.name}: store {store_name!r} reported no keys for a Sql write")
            files, _ = await plan["index"].replace(
                written.keys,
                int(info["batch"]),
                spec["attempt"],
                key=output.key,
                revision=output.revision,
                generation=int(spec.get("generation") or 0),
            )
            entry["keys"] = files.to_json()
        elif "index" in plan:
            entry["keys"] = plan["keys"]
            if "elements" in plan:
                entry["elements"] = plan["elements"]
        if written.ref is None:
            continue
        ref = dataclasses.replace(written.ref, store=store_name)
        if schema is not None:
            ref = dataclasses.replace(ref, handle={**(ref.handle or {}), "schema": schema})
        entry["ref"] = ref.to_json()
        rows = _rows(value)
        if rows is not None:
            entry["rows"] = rows
        entries[name] = entry
        timeline.add("stored", name, rows)
    return entries, cursor


REPAIR_PAGE = 100_000
LISTED = 1_000_000  # changed keys a replacement lists for its store; past it, the store rewrites the scope


async def _intended(info, keys_io, unsettled) -> list[str]:
    """The keys dead attempts meant to change (§8): each unsettled intent
    lists the delta files of an attempt that died while writing, and any of
    those writes may have landed."""

    state = IndexState(
        prefix=info["index"]["prefix"],
        log=tuple(
            (n, tuple(FileInfo.from_json(f) for f in intent["files"])) for n, intent in enumerate(unsettled)
        ),
    )
    index = KeyIndex(keys_io, None, state)
    found, after = [], None
    while True:
        keys, _, _, _, after = await index.pending(0, len(unsettled) - 1, after, REPAIR_PAGE)
        found.extend(map(key_str, keys))
        if after is None:
            return found


async def _repair(output, store, prior, left, new, removes):
    """Take in what dead attempts left in the store (§8). Keys this patch
    writes or removes end as it says either way; the others (`left`) are
    read back, and the index learns what landed as part of this commit's
    delta. Returns the key map and removes to check against the index."""

    new, removes, left = dict(new), list(removes), sorted(left)
    for i in range(0, len(left), REPAIR_PAGE):
        page = left[i : i + REPAIR_PAGE]
        loaded = await store.load(prior, None, Keys(dict.fromkeys(page, (b"", 0))))
        found = await _versions(output, await asyncio.to_thread(store_key_rows, store, loaded, output))
        new.update(found)
        removes.extend(k for k in page if k not in found)
    return new, removes


async def _versions(output, rows) -> dict[str, bytes]:
    """A patch's keys and versions — few enough to check one by one."""

    try:
        keys, versions = await asyncio.to_thread(rows.entries)
    except ValueError as e:  # a value with no digest
        raise WriteError(f"{output.name}: {e}") from e
    return dict(zip(map(key_str, keys), versions, strict=True))


def _key_io(objects, objects_url: str, project: Project) -> ObjectIO:
    return ObjectIO(objects, cache=key_cache(project.manifest.get("key_cache"), objects_url))


class Writes:
    """Write-completion evidence (docs/lifecycle.md §2.3): `none` until a
    store call starts; `uncertain` while one runs, or if one raised or was
    abandoned; `complete` once every call made has returned."""

    def __init__(self):
        self.state = lifecycle.NONE

    async def call(self, work):
        self.state = lifecycle.UNCERTAIN
        result = await work
        self.state = lifecycle.COMPLETE
        return result


class _Stop(Exception):
    """A cancel was requested before the gate: stop, writing nothing."""


ENDED = ABORTED = 3  # the exit code of an attempt the engine ended: no result was published
LOSER_POLL = 30.0  # how often an invocation that lost the claim looks for the owner's result


async def run_attempt(
    objects_url: str,
    attempt: str,
    entrypoint: str | Project,
    *,
    run: str,
    channel=None,
    engine_url: str | None = None,
    pool: bool = False,
    own_process: bool = False,
    loser_poll: float = LOSER_POLL,
) -> int:
    """Run one attempt (docs/lifecycle.md §3): read its spec, claim it, run
    it, seal its result.

    The claim is the first write: an invocation that loses it touches
    nothing and, unless it is a pool worker (`pool`), waits for the owner's
    result before exiting, so its exit never reads as the attempt's. The
    engine is reached through `channel`, or over HTTPS at `engine_url` (else
    the spec's); without one, the worker reports through `.worker` alone.

    Returns 0 once a result is published (succeeded or canceled), 1 for a
    failed result, and 3 when the engine ended the attempt first: then no
    result is published. Raises if the result could not be published.
    `own_process` says the process runs this attempt alone: its peak memory
    is the attempt's, and a forced cancel exits it at once."""

    timeline = Timeline(own_process)
    timeline.add("booted", socket.gethostname())
    objects = _objects(objects_url)
    base = lifecycle.base(run, attempt)
    data = await _get(objects, f"{base}{lifecycle.SPEC}")
    if data is None:
        raise StoreError(f"No spec at {base}{lifecycle.SPEC}")
    spec = json.loads(data)
    invocation = secrets.token_hex(8)
    claim = {"invocation": invocation, "host": socket.gethostname(), "pid": os.getpid(), "at": time.time()}
    try:
        await create(objects, f"{base}{lifecycle.WORKER}", json.dumps(claim).encode())
    except AlreadyExistsError:
        if not pool:
            await _await_owner(objects, base, loser_poll)
        return 0
    if channel is None and (engine_url or spec.get("engine")):
        from .channel import HttpChannel

        channel = HttpChannel(engine_url or spec["engine"], spec["project"], attempt, spec["token"])
    loop = asyncio.get_running_loop()
    control = {"cancel": None, "writing": False, "stopped": False, "forced": False}

    def on_cancel(record: Cancel):
        control["cancel"] = record
        if record.phase == "forced":
            on_ended()
        elif not control["writing"] and not control["stopped"]:
            control["stopped"] = True  # requested: stop computing; a writer drains instead
            loop.call_soon_threadsafe(execution.cancel)

    def on_ended():
        control["forced"] = True
        if own_process:
            os._exit(ENDED)  # an engine that ended this attempt takes no result from it
        loop.call_soon_threadsafe(execution.cancel)

    if channel is not None:
        try:
            answer = await channel.start({**claim})
            started = Cancel.from_json(answer.get("cancel"))
        except Ended:
            return ENDED
        except Exception:
            started = None  # unreachable for now: the reporter falls back to `.worker`
    else:
        started = None
    shipper = LogShipper(objects, base, channel, invocation)
    writes = Writes()
    execution = asyncio.create_task(
        _execute(objects, objects_url, base, spec, entrypoint, timeline, shipper, writes, invocation, control)
    )
    reporter = Reporter(
        objects, base, invocation, channel, spec.get("heartbeat", 10), timeline, on_cancel, on_ended
    )
    if started is not None:
        reporter.cancel = started
        on_cancel(started)
    reporter.start()
    flusher = asyncio.create_task(shipper.periodically())
    try:
        try:
            result = await execution
        except (asyncio.CancelledError, _Stop):
            if control["forced"] or not control["stopped"]:
                raise
            result = {"status": "canceled"}  # requested before the gate: nothing written
        if result is None or control["forced"]:
            return ENDED
        await _publish(objects, base, spec, result, invocation, writes, control["cancel"], timeline, shipper)
        flusher.cancel()
        if channel is not None:
            with contextlib.suppress(Exception):
                await channel.finished({"invocation": invocation})
        return 1 if result["status"] == "failed" else 0
    except asyncio.CancelledError:
        if control["forced"] and not asyncio.current_task().cancelling():
            return ENDED
        raise
    finally:
        flusher.cancel()
        await reporter.stop()
        if channel is not None:
            channel.close()


async def _await_owner(objects, base: str, poll: float) -> None:
    """A losing invocation: wait until the owner's result exists, or the
    attempt's objects are gone, before exiting."""

    while await _get(objects, f"{base}{lifecycle.SPEC}") is not None:
        if await _get(objects, f"{base}{lifecycle.RESULT}") is not None:
            return
        await asyncio.sleep(poll)


def _failed(error: BaseException, retryable: bool) -> dict:
    return {
        "status": "failed",
        "error": {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": "".join(traceback.format_exception(error))[-32000:],
            "retryable": retryable,
        },
    }


PUBLISH_TRIES = 6


async def _publish(objects, base, spec, result, invocation, writes, cancel, timeline, shipper) -> None:
    """Seal the result once and create `.result` with exactly those bytes,
    retried as they are: a failure to publish never changes what is
    published. A worker that cannot publish raises, and the engine treats
    it as a worker that died."""

    log = await shipper.finish()
    timeline.add("finished")

    def seal(result: dict) -> bytes:
        body = {"invocation": invocation, **result, "writes": writes.state, **timeline.report(), "log": log}
        if cancel is not None:
            body["cancel"] = cancel.to_json()
        return json.dumps(body, allow_nan=False).encode()

    try:
        data = seal(result)
    except (TypeError, ValueError) as error:  # the result cannot be told as it is
        data = seal(_failed(error, True))
    for attempt in range(PUBLISH_TRIES):
        try:
            await create(objects, f"{base}{lifecycle.RESULT}", data)
            return
        except AlreadyExistsError:
            raise  # only the claim's owner writes it: someone else's bytes are a bug
        except Exception:
            if attempt == PUBLISH_TRIES - 1:
                raise
            await asyncio.sleep(0.2 * 2**attempt)


async def _execute(
    objects, objects_url, base, spec, entrypoint, timeline, shipper, writes, invocation, control
) -> dict | None:
    """Run the attempt: its result, or `None` once the engine ended it."""

    async def fence(intents: dict):
        """Take the gate (docs/lifecycle.md §2.4) before the first store
        write — unless a cancel was requested: then stop, writing nothing.
        A gate already there means the engine ended this attempt."""

        if control["stopped"]:
            raise _Stop()
        body = lifecycle.gate(lifecycle.WRITING, invocation, intents)
        try:
            await create(objects, f"{base}{lifecycle.GATE}", body)
        except AlreadyExistsError:
            raise Aborted(spec["attempt"]) from None
        control["writing"] = True
        timeline.add("writing")

    try:
        project = entrypoint if isinstance(entrypoint, Project) else load_project(entrypoint)
    except Exception as error:
        return _failed(error, False)
    timeline.add("imported")
    if project.manifest["revision"] != spec["revision"]:
        mismatch = (
            f"revision mismatch: spec {spec['revision'][:12]} != project {project.manifest['revision'][:12]}"
        )
        return _failed(StoreError(mismatch), False)
    asset = project.assets[spec["asset"]]
    try:
        keys_io = _key_io(objects, objects_url, project)
        args, changes, delivered = await _resolve_inputs(spec, project, asset, keys_io, timeline)
        ctx = Ctx(spec, asset, project, objects, changes, shipper, timeline)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resolve_env(resource)  # env: secrets resolve in the harness (§5)
        timeline.add("computing")
        value = asset.fn(**args)
        if inspect.isawaitable(value):
            value = await value
        timeline.add("computed")
        metadata = ctx._recorded(value)
        outputs, cursor = await _store_outputs(
            spec, project, asset, objects, keys_io, value, fence, writes, timeline
        )
        for name, values in metadata.items():
            if values and "ref" in outputs.get(name, {}):
                outputs[name]["metadata"] = values
        result = {"status": "succeeded", "outputs": outputs, "delivered": delivered}
        if cursor is not UNSET:
            result["cursor"] = cursor
        return result
    except Aborted:
        return None
    except _Stop:
        raise
    except StoreError as error:
        return _failed(error, getattr(error, "retryable", False))
    except Exception as error:
        return _failed(error, True)


async def run_pool(pool: str, server: str, token: str | None = None, *, project: str | None = None):
    """Pull path (docs/lifecycle.md §10): ask the engine which attempts wait
    on this pool, claim one by creating its `.worker`, run it, repeat. The
    claim decides between workers; discovery is only a hint."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
    host = f"{socket.gethostname()}:{os.getpid()}"
    async with httpx.AsyncClient(base_url=server, headers=headers, timeout=60) as client:
        while project is None:
            try:
                response = await client.get("/api/diagnostics")
                response.raise_for_status()
                project = response.json()["project"]
            except httpx.HTTPError:
                await asyncio.sleep(1.0)
        print(f"[pool] {host} polls pool {pool!r}", flush=True)
        while True:
            try:
                response = await client.get(
                    f"/api/projects/{project}/pools/{pool}/work",
                    params={"wait": 30, "host": host, **{k: v for k, v in capacity.items() if v is not None}},
                )
                response.raise_for_status()
                stages = response.json()["work"]
            except httpx.HTTPError:
                # The server may be briefly unreachable (a restart): a pool worker polls on.
                await asyncio.sleep(1.0)
                continue
            for stage in stages:
                try:
                    code = await run_attempt(
                        stage["objects"],
                        stage["attempt"],
                        os.environ["SOLERA_PROJECT"],
                        run=stage["run"],
                        engine_url=server,
                        pool=True,
                    )
                except Exception:  # it could not publish: the engine treats it as dead
                    traceback.print_exc()
                    continue
                print(f"[pool] {stage['attempt']} exited {code}", flush=True)
                break  # ask again: what waits has changed


async def main():
    args = sys.argv[1:]
    if not args:
        raise SystemExit("usage: solera_worker run|manifest|pool ...")
    mode, rest = args[0], args[1:]
    if mode == "manifest":
        # solera_worker manifest PROJECT OUT
        entrypoint, out = rest[0], rest[1]
        project = load_project(entrypoint)
        Path(out).write_text(json.dumps(project.manifest, allow_nan=False))
        return
    if mode == "run":
        # solera_worker run --objects URL --attempt ID --run RUN (SOLERA_PROJECT env entrypoint)
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        code = await run_attempt(
            options["--objects"],
            options["--attempt"],
            os.environ["SOLERA_PROJECT"],
            run=options["--run"],
            own_process=True,
        )
        raise SystemExit(code)
    if mode == "pool":
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        await run_pool(options["--pool"], options["--server"].rstrip("/"), options.get("--token"))
        return
    raise SystemExit(f"unknown mode: {mode}")
