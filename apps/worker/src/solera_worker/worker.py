"""User code runs here, never in the API process (§10 worker protocol).

`python -m solera_worker run --objects URL --run RUN --attempt ID`:
read the spec from the attempt file -> refuse on revision mismatch -> resolve `env:` -> load inputs per
annotation (Incremental edges through the upstream key index) -> build ctx ->
run the producer -> for each returned output, work out what changed against
its key index and write the delta file -> take the write fence -> store()
each output unless nothing changed -> rewrite the attempt file with the spec,
the result and the log index, in one PUT. All along, a thread beats.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import gzip
import importlib
import importlib.util
import inspect
import json
import os
import sys
import threading
import time
import traceback
import typing
from pathlib import Path

from solera.keys.index import DeltaFiles, FileInfo, IndexState, KeyIndex, key_bytes, key_str
from solera.keys.io import ObjectIO, key_cache
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
    key_map,
    resolve_env,
)


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


async def _create(objects, key: str, value: bytes) -> bool:
    """Create-only PUT: False if the object already exists."""

    import obstore
    from obstore.exceptions import AlreadyExistsError

    try:
        await obstore.put_async(objects, key, value, mode="create", use_multipart=False)
    except AlreadyExistsError:
        return False
    return True


async def _get(objects, key: str) -> bytes | None:
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        result = await obstore.get_async(objects, key)
    except NotFoundError:
        return None
    return bytes(await result.bytes_async())


class Aborted(Exception):
    """The engine took the attempt's write fence first: it may write nothing."""


ABORTED = 3  # the exit code of an aborted attempt


class Heartbeat:
    """Proof of life for an engine with no handle on this worker (§8).

    A thread rewrites `{attempt}.beat` every `interval` seconds — from a
    thread, so a producer that blocks the event loop still beats — and reads
    the write fence each time: once the engine has aborted the attempt,
    `on_abort` runs. A worker that stops marks the beat done, so the engine
    settles it without waiting for three missed beats."""

    def __init__(self, objects, base: str, interval: float, on_abort):
        self.objects, self.base, self.interval, self.on_abort = objects, base, interval, on_abort
        self.aborted = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"beat {base}", daemon=True)

    def start(self):
        self._thread.start()

    def _run(self):
        import obstore
        from obstore.exceptions import NotFoundError

        n = 0
        while True:
            try:
                beat = json.dumps({"at": time.time(), "n": n}).encode()
                obstore.put(self.objects, f"{self.base}.beat", beat, use_multipart=False)
                try:
                    fence = json.loads(bytes(obstore.get(self.objects, f"{self.base}.writing").bytes()))
                except NotFoundError:
                    fence = None
                if fence is not None and fence["state"] == "aborted":
                    self.aborted = True
                    self.on_abort()
                    return
            except Exception:
                pass  # a missed beat: only three in a row make the worker dead
            n += 1
            if self._stop.wait(self.interval):
                return

    async def stop(self):
        self._stop.set()
        await asyncio.to_thread(self._thread.join)
        with contextlib.suppress(Exception):
            await _put(self.objects, f"{self.base}.beat", json.dumps({"done": True}).encode())


class Ctx:
    """The `ctx` argument handed to producers (§2)."""

    def __init__(self, spec, asset: Asset, project: Project, objects, changes, shipper):
        self._spec, self._objects, self._shipper = spec, objects, shipper
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
        return await store.load(ref, t, None)

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


LOG_FLUSH_SECONDS = 2.0
LOG_FLUSH_BYTES = 256 * 1024
LOG_CAP = 100 * 2**20  # compressed bytes per attempt


class LogShipper:
    """`ctx.log` lines as gzip-compressed JSON lines (docs/object-store-state.md §8).

    While the attempt runs, each flush — every 2 s or 256 KB — writes one
    gzip member as `{attempt}.log.{n}`, so the console can tail it. At the
    end the members are joined into `{attempt}.log` (concatenated gzip
    members are one valid gzip file) and the chunks deleted; `index()` lists
    `[byte offset, lines, first timestamp]` per member for range reads. Past
    `LOG_CAP` a truncation marker is written and shipping stops."""

    def __init__(self, objects, base: str):
        self.objects, self.base = objects, base
        self.pending: list[tuple[float, str]] = []
        self.pending_bytes = 0
        self.members: list[bytes] = []
        self.blocks: list[list] = []
        self.size = self.lines = 0
        self.truncated = False
        self._lock = asyncio.Lock()

    def append(self, entry):
        if self.truncated:
            return
        line = json.dumps(entry, allow_nan=False) + "\n"
        self.pending.append((entry["at"], line))
        self.pending_bytes += len(line)
        if self.pending_bytes >= LOG_FLUSH_BYTES:
            try:
                asyncio.get_running_loop().create_task(self.flush())
            except RuntimeError:
                pass  # logging from a thread: the next periodic flush ships it

    async def flush(self):
        async with self._lock:
            if not self.pending or self.truncated:
                return
            lines, self.pending, self.pending_bytes = self.pending, [], 0
            member = gzip.compress("".join(line for _, line in lines).encode(), compresslevel=6, mtime=0)
            if self.size + len(member) > LOG_CAP:
                self.truncated = True
                marker = {
                    "at": lines[0][0],
                    "level": "warning",
                    "message": f"log truncated: the attempt's log reached {LOG_CAP} bytes",
                    "fields": {},
                }
                lines = [(marker["at"], json.dumps(marker) + "\n")]
                member = gzip.compress(lines[0][1].encode(), mtime=0)
            await _put(self.objects, f"{self.base}.log.{len(self.members):06d}", member)
            self.blocks.append([self.size, len(lines), lines[0][0]])
            self.members.append(member)
            self.size += len(member)
            self.lines += len(lines)

    async def periodically(self):
        while True:
            await asyncio.sleep(LOG_FLUSH_SECONDS)
            await self.flush()

    async def finish(self) -> dict:
        """Join the chunks into the attempt's log; returns its index."""

        await self.flush()
        if self.members:
            import obstore

            await _put(self.objects, f"{self.base}.log", b"".join(self.members))
            await obstore.delete_async(
                self.objects, [f"{self.base}.log.{n:06d}" for n in range(len(self.members))]
            )
        return {"blocks": self.blocks, "lines": self.lines, "bytes": self.size, "truncated": self.truncated}


async def _resolve_inputs(spec, project, asset, keys_io):
    """Load each pin by annotation; build call args + ctx.changes (§5, §10).

    An Incremental edge over a keyed upstream reads its page from the pinned
    key index — the pending deltas in `[from, to]`, or the whole index for a
    full delivery — and loads just those keys. `delivered` reports where the
    page ended, for the engine's watermark (§6)."""

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
                continue
            if "keys" in ch:  # a run's keys= override: a one-off selection
                upserted, deleted, after = {str(k): "" for k in ch["keys"]}, (), None
            else:
                index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
                start = key_bytes(ch["after"]) if ch.get("after") is not None else None
                if full:
                    keys, versions, nxt = await index.page(start, int(ch["limit"]))
                    flags = bytes(len(keys))
                else:
                    keys, versions, flags, nxt = await index.pending(
                        int(ch["from"]), int(ch["to"]), start, int(ch["limit"])
                    )
                upserted = {
                    key_str(k): key_str(v) for k, v, d in zip(keys, versions, flags, strict=True) if not d
                }
                deleted = tuple(key_str(k) for k, d in zip(keys, flags, strict=True) if d)
                after = key_str(nxt) if nxt is not None else None
            args[param] = await store.load(ref, t, Keys(upserted))
            changes[param] = Changes(
                rows=args[param], deleted=deleted, full=full, upserted=tuple(sorted(upserted))
            )
            delivered[param] = {"after": after, "upserted": sorted(upserted), "deleted": list(deleted)}
            continue
        if t is not None and is_ref_type(t):
            args[param] = ref
        else:
            args[param] = await store.load(ref, t, None)
    return args, changes, delivered


def _dict_inner(t):
    if t is None:
        return None
    if typing.get_origin(t) in (dict, dict):
        args_ = typing.get_args(t)
        if len(args_) == 2:
            return args_[1]
    return None


async def _store_outputs(spec, project, asset, objects, keys_io, result_value, fence):
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
            # Rows the harness never sees: the store reports the whole new key
            # map once it wrote, so its delta comes after — and needs no repair.
            intents[name] = DeltaFiles([], 0, 0, True).to_json()
            continue
        patch = isinstance(value, Patch)
        new = key_map(output, value.rows if patch else value)
        # With no prior (a first write, or a full run) a Patch is the whole content.
        replace = not patch or prior is None
        removes = [] if replace else [str(k) for k in value.remove if str(k) not in new]
        own = set(new), set(removes)
        unsettled = info.get("unsettled") or []
        intended = set(await _intended(info, keys_io, unsettled)) if unsettled else set()
        if unsettled and not replace:
            new, removes = await _repair(output, store, prior, intended - own[0] - own[1], new, removes)
        delta = await index.changes(
            [key_bytes(k) for k in new],
            [key_bytes(v) for v in new.values()],
            [key_bytes(k) for k in removes],
            replace=replace,
        )
        if not len(delta) and info.get("exists") and not unsettled:
            entries[name] = {"unchanged": True}
            del plans[name]
            continue
        files = (
            await index.write(int(info["batch"]), spec["attempt"], delta)
            if len(delta)
            else DeltaFiles([], 0, 0, True)
        )
        plan["keys"] = intents[name] = files.to_json()
        if prior is not None:
            # The store writes only what changes: the delta, and whatever a
            # dead attempt may have left half-done among this write's keys.
            changed = {key_str(k) for k, d in zip(delta.keys, delta.deleted, strict=True) if not d}
            deleted = {key_str(k) for k, d in zip(delta.keys, delta.deleted, strict=True) if d}
            plan["upserts"] = frozenset((changed & own[0]) | (own[0] & intended))
            stale = (intended - own[0]) if replace else (own[1] & intended)
            plan["removes"] = frozenset(deleted | stale)
        if output.is_partition_set:
            if replace:
                elements = set(new)
            else:
                elements = (set(info.get("elements") or ()) - set(removes)) | set(new)
            plan["elements"] = sorted(elements)

    try:
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
                applied = await migrate(output, output.migrations)
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
        written = await store.store(value, prior, scope)
        entry = {}
        if "index" in plan and isinstance(value, Sql):
            if written.keys is None:
                raise StoreError(f"{output.name}: store {store_name!r} reported no keys for a Sql write")
            new = dict(written.keys)
            delta = await plan["index"].changes(
                [key_bytes(k) for k in new], [key_bytes(v) for v in new.values()], [], replace=True
            )
            files = (
                await plan["index"].write(int(info["batch"]), spec["attempt"], delta)
                if len(delta)
                else DeltaFiles([], 0, 0, True)
            )
            entry["keys"] = files.to_json()
            if output.is_partition_set:
                entry["elements"] = sorted(new)
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
        if isinstance(value, (list, tuple)):
            entry["rows"] = len(value)
        entries[name] = entry
    return entries, cursor


REPAIR_PAGE = 100_000


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
        keys, _, _, after = await index.pending(0, len(unsettled) - 1, after, REPAIR_PAGE)
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
        found = key_map(output, await store.load(prior, None, Keys({k: "" for k in page})))
        new.update(found)
        removes.extend(k for k in page if k not in found)
    return new, removes


def _key_io(objects, objects_url: str, project: Project) -> ObjectIO:
    return ObjectIO(objects, cache=key_cache(project.manifest.get("key_cache"), objects_url))


async def run_attempt(objects_url: str, attempt: str, entrypoint: str | Project, *, run: str, on_abort=None):
    """Run one attempt. The engine created `runs/{run}/{attempt}.json` holding
    the spec; the harness rewrites it once, at the end, with the spec, the
    result and the log index (docs/object-store-state.md §8).

    Returns 0 on success, 1 on failure, and 3 if the engine aborted the
    attempt: then nothing was written. Once aborted, `on_abort` runs — from
    the heartbeat thread; by default the attempt's work is canceled."""

    objects = _objects(objects_url)
    base = f"runs/{run}/{attempt}"
    record = await _get(objects, f"{base}.json")
    if record is None:
        raise StoreError(f"No attempt file at {base}.json")
    spec = json.loads(record)["spec"]
    loop = asyncio.get_running_loop()
    work = asyncio.create_task(_attempt(objects, objects_url, base, spec, entrypoint))
    beat = Heartbeat(
        objects, base, spec.get("heartbeat", 30), on_abort or (lambda: loop.call_soon_threadsafe(work.cancel))
    )
    beat.start()
    try:
        return await work
    except asyncio.CancelledError:
        if not beat.aborted:
            raise
        return ABORTED
    finally:
        await beat.stop()


async def _attempt(objects, objects_url: str, base: str, spec: dict, entrypoint) -> int:
    shipper = LogShipper(objects, base)
    flusher = asyncio.create_task(shipper.periodically())

    async def finish(result: dict):
        flusher.cancel()
        log = await shipper.finish()
        body = {"spec": spec, "result": result, "log": log}
        await _put(objects, f"{base}.json", json.dumps(body, allow_nan=False).encode())

    async def fail(error: BaseException, retryable: bool):
        await finish(
            {
                "status": "failed",
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "traceback": "".join(traceback.format_exception(error))[-32000:],
                    "retryable": retryable,
                },
            }
        )

    async def fence(intents: dict):
        """Take the write fence (§8), or learn that the engine aborted us."""

        body = json.dumps({"state": "writing", "intents": intents}).encode()
        if not await _create(objects, f"{base}.writing", body):
            raise Aborted(spec["attempt"])

    try:
        project = entrypoint if isinstance(entrypoint, Project) else load_project(entrypoint)
    except Exception as error:
        await fail(error, False)
        return 1
    if project.manifest["revision"] != spec["revision"]:
        await fail(
            StoreError(
                f"revision mismatch: spec {spec['revision'][:12]} != project {project.manifest['revision'][:12]}"
            ),
            False,
        )
        return 1
    asset = project.assets[spec["asset"]]
    try:
        keys_io = _key_io(objects, objects_url, project)
        args, changes, delivered = await _resolve_inputs(spec, project, asset, keys_io)
        ctx = Ctx(spec, asset, project, objects, changes, shipper)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resolve_env(resource)  # env: secrets resolve in the harness (§5)
        value = asset.fn(**args)
        if inspect.isawaitable(value):
            value = await value
        metadata = ctx._recorded(value)
        outputs, cursor = await _store_outputs(spec, project, asset, objects, keys_io, value, fence)
        for name, values in metadata.items():
            if values and "ref" in outputs.get(name, {}):
                outputs[name]["metadata"] = values
        result = {"status": "succeeded", "outputs": outputs, "delivered": delivered}
        if cursor is not UNSET:
            result["cursor"] = cursor
        await finish(result)
        return 0
    except Aborted:
        return ABORTED  # the engine has finished this attempt: write nothing, not even a result
    except StoreError as error:
        await fail(error, getattr(error, "retryable", False))
    except Exception as error:
        await fail(error, True)
    finally:
        flusher.cancel()
    return 1


async def run_pool(pool: str, server: str, token: str | None = None):
    """Pull path: register, claim, run the stage, complete (§10)."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(base_url=server, headers=headers, timeout=30) as client:
        capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
        registered = (
            await client.post(
                "/api/workers/register",
                json={"pools": [pool], "capacity": capacity},
            )
        ).json()
        worker_id = registered["worker"]
        print(f"[pool] worker {worker_id} registered in pool {pool!r}", flush=True)
        while True:
            try:
                response = await client.post(
                    "/api/tasks/claim", json={"worker": worker_id, "capacity": capacity}
                )
            except httpx.HTTPError:
                # The server may be briefly unreachable (engine tick pressure,
                # restart) — a pool worker polls forever rather than dying.
                await asyncio.sleep(1.0)
                continue
            if response.status_code == 204:
                await asyncio.sleep(1.0)
                continue
            response.raise_for_status()
            claim = response.json()
            task_id, stage = claim["task"], claim["stage"]
            print(f"[pool] claimed {task_id}", flush=True)
            lease = float(claim.get("lease_seconds", 30))

            async def renew(lease=lease, task_id=task_id):
                while True:
                    await asyncio.sleep(max(lease / 3, 1.0))
                    try:
                        await client.post(f"/api/tasks/{task_id}/renew", json={"worker": worker_id})
                    except Exception:
                        return

            renewal = asyncio.create_task(renew())
            try:
                await run_attempt(
                    stage["objects"], stage["attempt"], os.environ["SOLERA_PROJECT"], run=stage["run"]
                )
                await client.post(f"/api/tasks/{task_id}/complete", json={"worker": worker_id})
                print(f"[pool] completed {task_id}", flush=True)
            finally:
                renewal.cancel()


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
            on_abort=lambda: os._exit(ABORTED),  # an aborted process stops at once
        )
        raise SystemExit(code)
    if mode == "pool":
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        await run_pool(options["--pool"], options["--server"].rstrip("/"), options.get("--token"))
        return
    raise SystemExit(f"unknown mode: {mode}")
