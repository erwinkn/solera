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
import copy
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
from solera import errors, lifecycle
from solera.keys import Rows, encode_file
from solera.keys.index import (
    DeltaFiles,
    DeltaKeys,
    FileInfo,
    IndexState,
    KeyIndex,
    delta_keys,
    key_bytes,
    key_str,
)
from solera.keys.io import ObjectIO, key_cache
from solera.keys.resolver import Ask, answers, request
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
    key_text,
    resolve_env,
    store_key_rows,
)

from . import each
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

    def __init__(
        self, spec, asset: Asset, project: Project, objects, changes, shipper, timeline, keys_io=None
    ):
        self._spec, self._objects, self._shipper, self._timeline = spec, objects, shipper, timeline
        self._keys_io = keys_io
        # The pinned key indexes of keyed inputs, for whole reads of immutable stores.
        self._indexes: dict[tuple, dict] = {}
        for pin in spec["inputs"].values():
            if pin.get("index") is not None and pin.get("ref"):
                self._indexes[(pin["ref"]["output"], pin["ref"].get("partition") or "")] = pin["index"]
            for key, ref in (pin.get("refs") or {}).items():
                if (pin.get("indexes") or {}).get(key) is not None:
                    self._indexes[(ref["output"], ref.get("partition") or "")] = pin["indexes"][key]
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
        # An Each call's key and its upstream version (docs/per-key-processing.md §5).
        self.key: str | None = None
        self.revision: str | None = None

    def _for_key(self, key: str, revision: str) -> Ctx:
        """The `ctx` of one Each call: the same attempt, its key named, its log
        lines tagged with it."""

        one = copy.copy(self)
        one.key, one.revision = key, revision
        return one

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
        if self.key is not None:
            fields = {"key": self.key, **fields}
        entry = {"at": time.time(), "level": level, "message": str(message), "fields": fields}
        json.dumps(entry, allow_nan=False)
        self._shipper.append(entry)

    async def load(self, ref: Ref, t):
        store = self._stores[ref.store]
        index = self._indexes.get((ref.output, ref.partition))
        value = await store.load(ref, t, await _whole(store, self._keys_io, index))
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
    windows = []
    for name, pin in spec["inputs"].items():
        edge = edges.get(name)
        if edge is None or "each" in pin:  # a dep pin: recorded, never bound; an Each page: per key
            continue
        param = name
        t = hints.get(param)
        if "refs" in pin:  # AllPartitions
            inner = _dict_inner(t)
            out = {}
            indexes = pin.get("indexes") or {}
            for key, ref_json in pin["refs"].items():
                ref = Ref.from_json(ref_json)
                if inner is not None and is_ref_type(inner):
                    out[key] = ref
                    continue
                store = project.stores[ref.store]
                out[key] = await store.load(ref, inner, await _whole(store, keys_io, indexes.get(key)))
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
            # The page — a keys= override, inlined by the engine, or read from the
            # pinned index — filtered by the edge's patterns (per-key §11).
            window = await each.read_window(pin, keys_io)
            windows.append(window)
            upserted, deleted, after = window.upserted, window.deleted, window.after
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
            args[param] = await store.load(ref, t, await _whole(store, keys_io, pin.get("index")))
            timeline.add("loaded", param, _rows(args[param]))
    # Every keyed page held keys, and the edges' patterns took none of them: nothing
    # to call the producer with.
    filtered = bool(windows) and all(not w.upserted and not w.deleted for w in windows)
    delivered["*filtered"] = filtered and any(w.read for w in windows)
    return args, changes, delivered


async def _whole(store, keys_io, index_json) -> Keys | None:
    """A whole keyed read from an immutable store, as the `Keys` selection
    of every live entry of its pinned index: such a store names objects from
    `(version, locator)`, and a listing would also show superseded and
    abandoned ones (docs/lifecycle.md §9.8). `None` for any other read."""

    if index_json is None or getattr(store, "writes", "overwrite") != "immutable":
        return None
    index = KeyIndex(keys_io, None, IndexState.from_json(index_json))
    entries, after = {}, None
    while True:
        keys, versions, locators, after = await index.page(after, REPAIR_PAGE)
        entries.update({key_str(k): (v, loc) for k, v, loc in zip(keys, versions, locators, strict=True)})
        if after is None:
            return Keys(entries)


def _dict_inner(t):
    if t is None:
        return None
    if typing.get_origin(t) in (dict, dict):
        args_ = typing.get_args(t)
        if len(args_) == 2:
            return args_[1]
    return None


async def _store_outputs(
    spec,
    project,
    asset,
    objects,
    keys_io,
    result_value,
    fence,
    writes,
    timeline,
    invocation=None,
    channel=None,
):
    """Store each returned output (§4, §6, §8, §9), in two phases.

    Planning compares each keyed output's write with its key index as pinned
    in the spec, and writes the changes as the batch's delta file: a write
    that changes nothing is not stored at all and keeps the head. Then
    `fence(intents, gated)` begins writing: on stores that take a gate
    (all but `immutable` ones), it takes the attempt's gate first, listing
    their delta files — the keys this attempt is about to change — and only
    then do the stores write. An engine that finds the fence taken by a worker that then
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

    def scope_of(name: str, plan: dict) -> Scope:
        info = plan["info"]
        return Scope(
            output=plan["output"],
            partition=spec["partition"],
            batch=info.get("batch"),
            attempt=spec["attempt"],
            aliases=tuple(info.get("aliases") or ()),
            upserts=plan.get("upserts"),
            removes=plan.get("removes"),
            generation=spec.get("generation"),
            invocation=invocation,
        )

    # Acquire: a fenced store's generation, before anything reads the store —
    # repair included (docs/lifecycle.md §9.7).
    plans, intents, entries = {}, {}, {}
    for name in values:
        if name not in decls:
            raise StoreError(f"{asset.name}: returned undeclared output {name!r}")
        plan = plans[name] = {
            "output": decls[name],
            "store": project.stores[declared[name]["store"]],
            "info": pinned.get(name) or {},
            "prior": priors.get(name),
        }
        if getattr(plan["store"], "writes", "overwrite") == "fenced":
            await plan["store"].acquire(scope_of(name, plan))

    # Plan: what each keyed write changes, as delta files. Small writes are
    # resolved by the engine from its cache, all of an attempt's in one request
    # (docs/resolved-commits.md §4); the rest, and any it declines, here.
    asks, pending = [], {}
    for name, value in values.items():
        plan = plans[name]
        output, store, info, prior = plan["output"], plan["store"], plan["info"], plan["prior"]
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
            # Unknown writes: if this attempt dies after its gate, no key list says what landed
            # (docs/lifecycle.md §9.6): the next attempt reconciles the whole slice.
            intents[name] = {**DeltaFiles([], 0, 0, True).to_json(), "unknown": True}
            continue
        patch = isinstance(value, Patch)
        # With no prior (a first write, or a full run) a Patch is the whole content.
        replace = not patch or prior is None
        content = value.rows if patch else value
        unsettled = info.get("unsettled") or []
        batch, attempt = int(info["batch"]), spec["attempt"]
        generation = int(spec.get("generation") or 0)  # each entry's locator (lifecycle.md §9.8)
        rows = await asyncio.to_thread(store_key_rows, store, content, output)
        p = pending[name] = {
            "replace": replace,
            "content": content,
            "unsettled": unsettled,
            "batch": batch,
            "generation": generation,
            "own": (set(), set()),
            "intended": set(),
        }
        live = int(info["index"].get("count", 0))
        if replace:
            if len(rows) + live <= RESOLVE_ENTRIES and len(rows) <= RESOLVE_KEYS:
                keys, versions = await asyncio.to_thread(rows.entries)
                p["run"] = (keys, versions, [])
            else:
                p["rows"] = rows
            continue
        new = await _versions(output, rows)
        try:
            # Once, for every reader below: each key once, in order.
            removes = sorted({k for k in map(key_text, value.remove) if k not in new})
        except WriteError as e:
            raise WriteError(f"{output.name}: {e}") from None
        own = p["own"] = (set(new), set(removes))
        p["new"], p["removes"] = new, removes
        if any(intent.get("unknown") for intent in unsettled):
            # A dead Sql writer's keys are unknown (docs/resolved-commits.md §3): the index
            # takes the whole store as it is, with this patch on top, and the store
            # writes this patch's keys, every one.
            p["files"], _ = await _reconcile(
                output, store, prior, index, content, new, removes, batch, attempt, generation
            )
            p["changed"], p["intended"] = None, own[0] | own[1]
            continue
        if unsettled:
            p["intended"] = set(await _intended(info, keys_io, unsettled))
            new, removes = await _repair(output, store, prior, p["intended"] - own[0] - own[1], new, removes)
            p["new"], p["removes"] = new, removes
        p["run"] = ([key_bytes(k) for k in new], list(new.values()), [key_bytes(k) for k in removes])
    for name, p in pending.items():
        if "run" in p and len(p["run"][0]) + len(p["run"][2]) <= RESOLVE_KEYS:
            asks.append(_ask_for(name, p, plans[name], spec))
    engine = await _ask_engine(channel, invocation, asks)
    for name, p in pending.items():
        plan, info, output = plans[name], plans[name]["info"], plans[name]["output"]
        index, replace, own, intended = plan["index"], p["replace"], p["own"], p["intended"]
        if "files" not in p:
            answer = engine.get(name)
            if answer is not None:
                p["files"], p["changed"] = await _upload(index, p["batch"], spec["attempt"], answer)
            elif replace:
                rows = p.get("rows") or Rows.pairs(list(zip(*p["run"][:2], strict=True)))
                try:
                    p["files"], p["changed"] = await index.replace(
                        rows, p["batch"], spec["attempt"], collect=LISTED, generation=p["generation"]
                    )
                except ValueError as e:  # a value with no digest, found as the join reaches it
                    raise WriteError(f"{output.name}: {e}") from e
            else:
                keys, versions, removes = p["run"]
                p["files"], p["changed"] = await index.resolve(
                    keys,
                    versions,
                    removes,
                    batch=p["batch"],
                    attempt=spec["attempt"],
                    generation=p["generation"],
                    collect=LISTED,
                )
        files, changed, unsettled = p["files"], p["changed"], p["unsettled"]
        if not files.files and info.get("exists") and not unsettled:
            entries[name] = {"unchanged": True}
            del plans[name]
            continue
        plan["keys"] = intents[name] = files.to_json()
        if getattr(plan["store"], "writes", "overwrite") == "immutable":
            # Every object it writes must be named by an index entry, or nothing ever
            # collects it: exactly the keys the delta writes, paged from its files
            # when there are too many to list (docs/lifecycle.md §9.8).
            if changed is None:
                plan["upserts"] = DeltaKeys(keys_io, index.prefix, tuple(files.files))
            else:
                plan["upserts"] = frozenset(map(key_str, changed[0]))
                plan["removes"] = frozenset(map(key_str, changed[1]))
        elif plan["prior"] is not None and not (replace and (changed is None or unsettled)):
            # The store writes only what changes: the delta, and for a patch whatever a
            # dead attempt may have left half-done among its keys. A replacement with
            # more changes than it lists, or with dead attempts', rewrites the scope;
            # a patch with more than it lists writes all of its own keys.
            if changed is None:
                plan["upserts"], plan["removes"] = frozenset(own[0]), frozenset(own[1])
            else:
                upserted, deleted = ({key_str(k) for k in keys} for keys in changed)
                if replace:
                    plan["upserts"], plan["removes"] = frozenset(upserted), frozenset(deleted)
                else:
                    plan["upserts"] = frozenset((upserted & own[0]) | (own[0] & intended))
                    plan["removes"] = frozenset(deleted | (own[1] & intended))
        if output.is_partition_set:
            if replace:
                elements = {str(e) for e in p["content"] or ()}
            else:
                elements = (set(info.get("elements") or ()) - set(p["removes"])) | set(p["new"])
            plan["elements"] = sorted(elements)

    gated = {n for n, plan in plans.items() if getattr(plan["store"], "writes", "overwrite") != "immutable"}
    try:
        if plans:  # only by a worker about to write; a gate only for stores that take one (§2.4, §9.6)
            await fence({n: i for n, i in intents.items() if n in gated}, bool(gated))
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
                applied = await writes.call(migrate(output, output.migrations, scope=scope_of(name, plan)))
            except StoreError:
                raise
            except Exception as error:
                raise StoreError(f"{output.name}: migration failed: {error}") from error
            schema = applied[-1] if applied else output.migrations[-1].name
        written = await writes.call(store.store(value, prior, scope_of(name, plan)))
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
RESOLVE_KEYS = 100_000  # a write the engine resolves: its keys (docs/resolved-commits.md §4)...
RESOLVE_ENTRIES = 2_000_000  # ...and for a replacement, its keys plus the live ones
RESOLVE_TIMEOUT = 5.0  # seconds the worker waits for the engine before resolving itself


def _ask_for(name: str, p: dict, plan: dict, spec: dict) -> Ask:
    keys, versions, removes = p["run"]
    run = sorted([(k, v, 0) for k, v in zip(keys, versions, strict=True)] + [(k, b"", 1) for k in removes])
    data = encode_file([e[0] for e in run], [e[1] for e in run], bytes(e[2] for e in run))
    return Ask(
        name,
        spec["partition"],
        "replace" if p["replace"] else "patch",
        p["batch"],
        p["generation"],
        plan["info"]["index"]["prefix"],
        p["batch"] - 1,
        data,
        len(run),
    )


async def _ask_engine(channel, invocation, asks: list[Ask]) -> dict[str, tuple[dict, bytes | None]]:
    """The engine's answers to the outputs it resolved: a delta or "empty".
    Unreachable, slow, declining: no answer, and the worker resolves itself."""

    if not asks or channel is None or not hasattr(channel, "resolve"):
        return {}
    try:
        body = await asyncio.wait_for(channel.resolve(request(invocation, asks)), RESOLVE_TIMEOUT)
        got = answers(body)
    except Exception:
        return {}
    return {name: a for name, a in got.items() if a[0]["result"] in ("delta", "empty")}


async def _upload(index: KeyIndex, batch: int, attempt: str, answer) -> tuple[DeltaFiles, tuple]:
    """The engine's delta, uploaded as this attempt's own delta file."""

    a, data = answer
    if data is None:
        return DeltaFiles([], 0, 0, True), ([], [])
    name = f"{batch:012d}-{attempt}.0000"
    await index.io.write(index.path(name), data)
    return DeltaFiles([FileInfo.describe(name, 0, data)], a["added"], a["removed"], True), delta_keys(data)


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


async def _reconcile(output, store, prior, index, content, new, removes, batch, attempt, generation):
    """The delta of a patch over a store a dead `Sql` writer changed in ways no
    key list records: the store's rows as they are — streamed back sorted a
    chunk at a time by its `scan`, read as a `Sql` write reports them — with
    this patch's rows in place of their keys' and its removes gone, against
    the pinned index: a streamed replacement. Memory is a chunk and the patch."""

    skip = set(new) | set(removes)
    patch = sorted(
        ((key_bytes(key_text(r[output.key])), r) for r in _rows_of(output, content)), key=lambda e: e[0]
    )
    scan = getattr(store, "scan", None)
    if scan is not None:
        chunks = scan(prior, output, sorted(skip))
    else:  # a store that cannot stream its rows back: read them whole
        loaded = _rows_of(output, await store.load(prior, None, None))
        chunks = [
            sorted(
                (r for r in loaded if key_text(r[output.key]) not in skip),
                key=lambda r: key_bytes(key_text(r[output.key])),
            )
        ]
    stamped = getattr(store, "stamped", None)
    return await index.replace(
        _merged(chunks, patch, output.key),
        batch,
        attempt,
        collect=LISTED,
        key=output.key,
        revision=output.revision,
        exclude=tuple(stamped(output)) if stamped else (),
        generation=generation,
    )


def _rows_of(output, value) -> list:
    """Rows as dicts, from whatever `key_rows` takes: lists of dicts, pandas
    DataFrames, and Arrow (anything with `__arrow_c_stream__`)."""

    if value is None:
        return []
    if type(value).__name__ == "DataFrame" and type(value).__module__.startswith("pandas"):
        return value.to_dict(orient="records")
    if hasattr(value, "__arrow_c_stream__"):
        import pyarrow as pa

        return pa.table(value).to_pylist()
    if not isinstance(value, list):
        raise WriteError(
            f"{output.name}: expected rows (list[dict] or DataFrame), got {type(value).__name__}"
        )
    return value


def _merged(chunks, patch: list, key: str):
    """Row chunks sorted by key, with `patch` — `(key bytes, row)`, sorted, of
    keys the chunks do not hold — merged in."""

    i = 0
    for chunk in chunks:
        out = []
        for row in chunk:
            at = key_bytes(key_text(row[key]))
            while i < len(patch) and patch[i][0] < at:
                out.append(patch[i][1])
                i += 1
            out.append(row)
        yield out
    if i < len(patch):
        yield [row for _, row in patch[i:]]


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
    if channel is None and (engine_url or spec.get("engine")):
        from .channel import HttpChannel

        channel = HttpChannel(engine_url or spec["engine"], spec["project"], attempt, spec["token"])
    try:
        await create(objects, f"{base}{lifecycle.WORKER}", json.dumps(claim).encode())
    except AlreadyExistsError:
        if not pool:
            await _await_owner(objects, base, loser_poll, channel, invocation)
        return 0
    loop = asyncio.get_running_loop()
    control = {"cancel": None, "writing": False, "stopped": False, "forced": False}

    def on_cancel(record: Cancel):
        control["cancel"] = record
        if record.phase == "forced":
            on_ended()
        elif control.get("drain") is not None:
            # An Each page drains (per-key §5): no key starts, finished ones are stored.
            loop.call_soon_threadsafe(control["drain"].set)
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


async def _await_owner(objects, base: str, poll: float, channel=None, invocation: str = "") -> None:
    """A losing invocation: wait until the attempt is over before exiting —
    the owner's result exists, the engine closed or aborted its gate, the
    engine says `ended` (`not_owner` is no news), or its objects are gone."""

    while await _get(objects, f"{base}{lifecycle.SPEC}") is not None:
        if await _get(objects, f"{base}{lifecycle.RESULT}") is not None:
            return
        gate = await _get(objects, f"{base}{lifecycle.GATE}")
        if gate is not None and json.loads(gate)["state"] in (lifecycle.ABORTED, lifecycle.CLOSED):
            return
        if channel is not None:
            try:
                await asyncio.to_thread(channel.beat, {"invocation": invocation, "seq": 0})
            except Ended as answer:
                if answer.reason != "not_owner":
                    return
            except Exception:
                pass  # the engine is unreachable: the objects decide
        await asyncio.sleep(poll)


def _failed(
    error: BaseException, retryable: bool, kind: str | None = None, timing: dict | None = None
) -> dict:
    told = {
        "type": type(error).__name__,
        "message": str(error),
        "traceback": "".join(traceback.format_exception(error))[-32000:],
        "retryable": retryable,
    }
    if kind is not None:
        told["class"] = kind
        told.update({k: v for k, v in (timing or {}).items() if v is not None})
    return {"status": "failed", "error": told}


def _user_failed(error: BaseException, project: Project) -> dict:
    """A failure of user code, by the class it says it is
    (docs/per-key-processing.md §8): `Rejected` is not retried; `Failed`
    and `Abort` follow `retries=`; `Transient` is retried with its own
    timing, which the engine applies."""

    kind, timing = errors.classify(error, project.errors)
    return _failed(error, kind != errors.REJECTED, kind, timing)


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

    async def fence(intents: dict, gated: bool):
        """Begin writing — unless a cancel was requested: then stop, writing
        nothing. Writing to a store that takes a gate, take it first
        (docs/lifecycle.md §2.4): one already there means the engine ended
        this attempt. From here on, a requested cancel drains."""

        if control["stopped"]:
            raise _Stop()
        if gated:
            try:
                await create(
                    objects, f"{base}{lifecycle.GATE}", lifecycle.gate(lifecycle.WRITING, invocation, intents)
                )
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
        filtered = delivered.pop("*filtered", False)
        ctx = Ctx(spec, asset, project, objects, changes, shipper, timeline, keys_io)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resolve_env(resource)  # env: secrets resolve in the harness (§5)
        page = next(((p, pin) for p, pin in spec["inputs"].items() if "each" in pin), None)
        if page is not None:
            control["drain"] = asyncio.Event()
            ran = await each.run(spec, project, asset, *page, args, ctx, keys_io, timeline, control)
            if "abort" in ran:
                return _user_failed(ran["abort"], project)
            value = Result(outputs=ran["values"])
            delivered[page[0]] = ran["delivered"]
        elif filtered:
            # The edges' patterns took none of the page's keys: the producer has
            # nothing to see, and the page commits only its watermark (per-key §11).
            return {"status": "succeeded", "skipped": True, "outputs": {}, "delivered": delivered}
        else:
            timeline.add("computing")
            value = asset.fn(**args)
            if inspect.isawaitable(value):
                value = await value
            timeline.add("computed")
        metadata = ctx._recorded(value)
        outputs, cursor = await _store_outputs(
            spec,
            project,
            asset,
            objects,
            keys_io,
            value,
            fence,
            writes,
            timeline,
            invocation,
            shipper.channel,
        )
        for name, values in metadata.items():
            if values and "ref" in outputs.get(name, {}):
                outputs[name]["metadata"] = values
        result = {"status": "succeeded", "outputs": outputs, "delivered": delivered}
        if page is not None:
            # A drained page commits what finished (docs/lifecycle.md §7).
            result["status"] = "canceled" if ran["drained"] else "succeeded"
            result.update({k: ran[k] for k in ("failures", "key_outcomes", "keys")})
            if ran["skipped"]:
                result["skipped"] = True
        result.update(await _discard_due(spec, project, asset, objects, writes))
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
        return _user_failed(error, project)


async def _discard_due(spec, project, asset, objects, writes) -> dict:
    """Discard the data garbage the engine handed this attempt (docs/
    lifecycle.md §9.8): the objects a commit or a compaction let go of, and
    what attempts that never committed wrote — all past every reader pin.
    The engine runs no store code; the scope's next attempt, which has its
    store, deletes for it. Returns what was done, for the result."""

    import obstore
    from solera.keys._python import decode_garbage, iter_file

    declared = {o["name"]: o for o in project.manifest["assets"][asset.name]["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    discarded, unresolved, files = {}, {}, []

    async def read(path: str) -> bytes | None:
        return await _get(objects, path)

    for name, info in (spec.get("outputs") or {}).items():
        store = project.stores[declared[name]["store"]]
        if not info.get("discard") or getattr(store, "writes", "overwrite") != "immutable":
            continue
        items, done = [], []
        for entry in info["discard"]:
            kind, prefix = entry["kind"], entry.get("prefix") or ""
            if kind in ("delta", "sidecar"):  # what a commit's delta, or a compaction, let go of
                found = [
                    await read(f"{prefix}{f}.{'kx' if kind == 'delta' else 'kg'}") for f in entry["files"]
                ]
                if any(data is None for data in found):  # the names are not known: it stays pending
                    unresolved.setdefault(name, []).append(entry["id"])
                    continue
                for data in found:
                    if kind == "delta":
                        for key, _, _, _, before in iter_file(data):
                            if before is not None:
                                items.append(("key", key_str(key), before[0].hex(), before[1]))
                    else:
                        keys, versions, _, locators = decode_garbage(data)
                        items += [
                            ("key", key_str(k), v.hex(), loc)
                            for k, v, loc in zip(keys, versions, locators, strict=True)
                        ]
                if kind == "sidecar":
                    files += [f"{prefix}{f}.kg" for f in entry["files"]]
            elif kind == "abandoned":  # all an uncommitted attempt wrote carries its generation
                generation = entry["generation"]
                if "prefix" in entry:  # keyed: its delta files name every object it could have written
                    stem = f"{int(entry['batch']):012d}-{entry['attempt']}"
                    async for batch in obstore.list(objects, prefix=prefix):
                        for meta in batch:
                            if meta["path"][len(prefix) :].startswith(stem):
                                data = await read(meta["path"])
                                for key, version, deleted, _, _ in iter_file(data) if data else ():
                                    if not deleted:
                                        items.append(("key", key_str(key), version.hex(), generation))
                                files.append(meta["path"])
                elif entry.get("batch") is not None:
                    items.append(("batch", entry["batch"], generation))
                else:
                    items.append(("value", generation))
            else:
                items += [tuple(i) for i in entry["items"]]
            done.append(entry["id"])
        if not done:
            continue
        scope = Scope(output=decls[name], partition=spec["partition"], attempt=spec["attempt"])
        head = Ref.from_json(info["head"]) if info.get("head") else None  # where its objects live
        await writes.call(store.discard(scope, head, items))
        discarded[name] = done
    out = {"discarded": discarded} if discarded else {}
    if unresolved:
        out["discard_unresolved"] = unresolved
    if files:
        out["discarded_files"] = files
    return out


async def run_pool(pool: str, server: str, token: str | None = None, *, project: str | None = None):
    """Pull path (docs/lifecycle.md §10): ask the engine which attempts wait
    on this pool, claim one by creating its `.worker`, run it, repeat. The
    claim decides between workers; discovery is only a hint. `project` is
    the project's name, by default its manifest's: a pool token reaches the
    pool's routes and nothing else."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
    host = f"{socket.gethostname()}:{os.getpid()}"
    project = project or load_project(os.environ["SOLERA_PROJECT"]).manifest["name"]
    async with httpx.AsyncClient(base_url=server, headers=headers, timeout=60) as client:
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
        raise SystemExit("usage: solera_worker run|manifest|pool|sensors ...")
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
        token = options.get("--token") or os.getenv("SOLERA_POOL_TOKEN") or os.getenv("SOLERA_API_TOKEN")
        await run_pool(options["--pool"], options["--server"].rstrip("/"), token)
        return
    if mode == "sensors":
        # solera_worker sensors --pool NAME --server URL [--token T] (SOLERA_PROJECT env entrypoint)
        from .sensors import HttpSensorChannel, run_sensor_host

        options = dict(zip(rest[::2], rest[1::2], strict=True))
        token = options.get("--token") or next(
            filter(None, map(os.getenv, ("SOLERA_SENSOR_TOKEN", "SOLERA_POOL_TOKEN", "SOLERA_API_TOKEN"))),
            None,
        )
        project = load_project(os.environ["SOLERA_PROJECT"])
        channel = HttpSensorChannel(options["--server"].rstrip("/"), project.manifest["name"], token)
        try:
            await run_sensor_host(channel, project, options["--pool"])
        finally:
            await channel.close()
        # Done ticking, or a tick overran on a thread that cannot be stopped: start afresh.
        os.execv(sys.executable, [sys.executable, "-m", "solera_worker", *args])
    raise SystemExit(f"unknown mode: {mode}")
