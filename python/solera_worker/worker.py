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
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from obstore.exceptions import AlreadyExistsError
from solera import errors, lifecycle
from solera.keys import SortedRun
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
from solera.keys.io import ObjectIO
from solera.keys.reads import Reads
from solera.keys.resolver import Ask, answers, request
from solera.lifecycle import Cancel, Ended
from solera.objects import create
from solera.sdk import (
    UNSET,
    Asset,
    Changes,
    Output,
    Project,
    Ref,
    Result,
    TimePartitions,
    Upstream,
    is_ref_type,
    split_partition,
)
from solera.stores import (
    Batches,
    KeyedWrite,
    Keys,
    Patch,
    Prepared,
    Scope,
    Sql,
    StoreError,
    WriteError,
    prepare_for,
    resolve_env,
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
        self._objects, self._shipper, self._timeline = objects, shipper, timeline
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
        value = await _load_whole(store, ref, t, self._keys_io, index)
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
                out[key] = await _load_whole(store, ref, inner, keys_io, indexes.get(key))
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
                changes[param] = Changes(
                    rows=args[param],
                    full=full,
                    page=int(ch.get("page") or 0),
                    pages=int(ch.get("pages") or 1),
                    final=not ch.get("more"),
                    upstream=Upstream(ref.output, range(lo, hi + 1)),
                )
                timeline.add("loaded", param, _rows(args[param]))
                continue
            # The page — a keys= override, inlined by the engine, or read from the
            # pinned index — filtered by the edge's patterns (per-key §11).
            window = await each.read_window(pin, keys_io)
            windows.append(window)
            upserted, deleted, after = window.upserted, window.deleted, window.after
            args[param] = await store.load(ref, t, Keys(upserted))
            changes[param] = Changes(
                rows=args[param],
                deleted=deleted,
                full=full,
                upserted=tuple(sorted(upserted)),
                page=int(ch.get("page") or 0),
                pages=int(ch.get("pages") or 1),
                final=after is None,  # the delivery ran out: never inferred from `pages`
                upstream=Upstream(ref.output),
            )
            delivered[param] = {"after": after, "upserted": sorted(upserted), "deleted": list(deleted)}
            timeline.add("loaded", param, _rows(args[param]))
            continue
        if t is not None and is_ref_type(t):
            args[param] = ref
        else:
            args[param] = await _load_whole(store, ref, t, keys_io, pin.get("index"))
            timeline.add("loaded", param, _rows(args[param]))
    # Every keyed page held keys, and the edges' patterns took none of them: nothing
    # to call the producer with.
    filtered = bool(windows) and all(not w.upserted and not w.deleted for w in windows)
    delivered["*filtered"] = filtered and any(w.read for w in windows)
    return args, changes, delivered


async def _load_whole(store, ref, t, keys_io, index_json):
    """A whole read. From an immutable store, a keyed one names its objects
    from the live entries of its pinned index (`Keys`), since a listing would
    also show superseded and abandoned ones (docs/lifecycle.md §9.8): it is
    loaded a page of the index at a time, and the pages put together."""

    if index_json is None or store.writes != "immutable":
        return await store.load(ref, t, None)
    index = KeyIndex(keys_io, None, IndexState.from_json(index_json))
    paged = _plain(t)  # a type of the store's own (a DataFrame): one read, which the store makes
    parts, entries, after = [], {}, None
    while True:
        keys, versions, locators, after = await index.page(after, REPAIR_PAGE)
        entries.update({key_str(k): (v, loc) for k, v, loc in zip(keys, versions, locators, strict=True)})
        if paged:
            parts.append(await store.load(ref, t, Keys(entries)))
            entries = {}
        if after is None:
            break
    if not paged:
        return await store.load(ref, t, Keys(entries))
    if len(parts) == 1:
        return parts[0]
    if all(isinstance(p, Mapping) for p in parts):
        return {k: v for p in parts for k, v in p.items()}
    return [item for p in parts for item in p]


def _plain(t) -> bool:
    """Whether a read as `t` is plain Python — untyped, a list or a dict —
    so its pages are put together here."""

    return t is None or t in (list, dict) or typing.get_origin(t) in (list, dict, Mapping)


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

    # Acquire: a fenced store's generation, before anything reads the store —
    # repair included (docs/lifecycle.md §9.7).
    outs: dict[str, _Out] = {}
    for name, value in values.items():
        if name not in decls:
            raise StoreError(f"{asset.name}: returned undeclared output {name!r}")
        store = project.stores[declared[name]["store"]]
        o = outs[name] = _Out(name, decls[name], store, pinned.get(name) or {}, priors.get(name), value)
        if o.kind == "fenced":
            await store.acquire(o.scope(spec, invocation))

    # Prepare and resolve: each keyed write read once, and compared with its key
    # index as pinned in the spec; its changes are the batch's delta file. Small
    # writes are resolved by the engine from its cache, all of an attempt's in one
    # request (docs/resolved-commits.md §4); the rest, and any it declines, here.
    for o in outs.values():
        await _prepare(o, spec, keys_io)
    asks = [_ask_for(o, spec) for o in outs.values() if o.asks()]
    engine = await _ask_engine(channel, invocation, asks)
    intents, entries = {}, {}
    for name, o in list(outs.items()):
        if o.index is None:
            continue
        if o.sql:
            # Rows the harness never sees: the store reports the whole new key
            # map once it wrote, so its delta comes after — and needs no repair.
            # Unknown writes: if this attempt dies after its gate, no key list says what landed
            # (docs/lifecycle.md §9.6): the next attempt reconciles the whole slice.
            intents[name] = {**DeltaFiles([], 0, 0, True).to_json(), "unknown": True}
            continue
        if o.files is None:
            await _resolve(o, spec, engine.get(name))
        if not o.files.files and o.info.get("exists") and not o.unsettled:
            if not _schema_due(o):
                entries[name] = {"unchanged": True}
                del outs[name]
                continue
            # No data to write, but migrations to apply: through the gate, then
            # the same content under the new schema (§4).
            o.schema_only = True
            intents[name] = o.files.to_json()
            continue
        intents[name] = o.files.to_json()
        o.write = dataclasses.replace(_keyed_write(o, keys_io), value=o.value)

    gated = {n for n, o in outs.items() if o.kind != "immutable"}
    try:
        if outs:  # only by a worker about to write; a gate only for stores that take one (§2.4, §9.6)
            await fence({n: i for n, i in intents.items() if n in gated}, bool(gated))
    except Aborted:
        # The engine has discarded this attempt's delta files; these came after.
        import obstore

        paths = [outs[n].index.path(f["name"]) for n, files in intents.items() for f in files["files"]]
        with contextlib.suppress(Exception):
            await obstore.delete_async(objects, paths)
        raise

    # Write: nothing reaches a store before the fence is ours.
    for name, o in outs.items():
        output, store, store_name = o.output, o.store, declared[name]["store"]
        scope = o.scope(spec, invocation)
        schema = None
        if output.migrations:
            migrate = getattr(store, "migrate", None)
            if not callable(migrate):
                raise StoreError(
                    f"{output.name}: store {store_name!r} has no migrate for declared migrations"
                )
            try:
                applied = await writes.call(migrate(output, output.migrations, scope=scope))
            except StoreError:
                raise
            except Exception as error:
                raise StoreError(f"{output.name}: migration failed: {error}") from error
            schema = applied[-1] if applied else output.migrations[-1].name
        if o.schema_only:
            ref = o.head()
            handle = {**(ref.handle or {}), "schema": schema}
            entries[name] = {"ref": dataclasses.replace(ref, handle=handle).to_json(), "keys": intents[name]}
            continue
        written = await writes.call(store.store(o.write or o.value, o.prior, scope))
        entry = {}
        if o.index is not None and o.sql:
            if written.keys is None:
                raise StoreError(f"{output.name}: store {store_name!r} reported no keys for a Sql write")
            files, _ = await o.index.replace(
                written.keys,
                int(o.info["batch"]),
                spec["attempt"],
                key=output.key,
                revision=output.revision,
                generation=int(spec.get("generation") or 0),
            )
            entry["keys"] = files.to_json()
        elif o.index is not None:
            entry["keys"] = intents[name]
            if o.elements is not None:
                entry["elements"] = o.elements
        if written.ref is None:
            continue
        ref = dataclasses.replace(written.ref, store=store_name)
        if schema is not None:
            ref = dataclasses.replace(ref, handle={**(ref.handle or {}), "schema": schema})
        entry["ref"] = ref.to_json()
        rows = _rows(o.value)
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


@dataclasses.dataclass
class _Out:
    """One output's write in an attempt, as the phases leave it: read once
    (`prepared`); for a patch, its `run` of upserts and removes, repair's
    overlaid; resolved against its key index (`files`, and up to `LISTED`
    keys what `changed`); and what its store is given (`write`)."""

    name: str
    output: Output
    store: Any
    info: dict  # the spec's pin of the output
    prior: Ref | None
    value: Any
    index: KeyIndex | None = None
    prepared: Prepared | None = None
    run: SortedRun | None = None
    intended: frozenset[str] = frozenset()  # keys dead attempts meant to change
    files: DeltaFiles | None = None
    changed: tuple | None = None  # ({key: version}, [removed key]), or None past LISTED
    write: KeyedWrite | None = None
    elements: list[str] | None = None
    schema_only: bool = False  # unchanged content, migrations to apply

    @property
    def kind(self) -> str:
        return self.store.writes

    def head(self) -> Ref:
        """The committed ref: the prior, or — a full run withholds it — the head."""

        return self.prior if self.prior is not None else Ref.from_json(self.info["head"])

    @property
    def sql(self) -> bool:
        return isinstance(self.value, Sql)

    @property
    def replace(self) -> bool:
        """A full replacement: a bare write, or with no prior (a first write or
        a full run) a Patch, which is then the whole content."""

        return not isinstance(self.value, Patch) or self.prior is None

    @property
    def unsettled(self) -> list:
        return self.info.get("unsettled") or []

    def scope(self, spec, invocation) -> Scope:
        return Scope(
            output=self.output,
            partition=spec["partition"],
            batch=self.info.get("batch"),
            attempt=spec["attempt"],
            aliases=tuple(self.info.get("aliases") or ()),
            generation=spec.get("generation"),
            invocation=invocation,
        )

    def asks(self) -> bool:
        """Whether the engine is asked to resolve this write (small enough)."""

        if self.prepared is None or self.files is not None:
            return False
        if self.replace:
            live = int(self.info["index"].get("count", 0))
            return (
                len(self.prepared.rows) + live <= RESOLVE_ENTRIES and len(self.prepared.rows) <= RESOLVE_KEYS
            )
        return len(self.run) <= RESOLVE_KEYS


def _schema_due(o: _Out) -> bool:
    """Whether the output declares migrations its head's schema does not
    show applied: then an unchanged write still migrates (§4)."""

    if not o.output.migrations:
        return False
    return (o.head().handle or {}).get("schema") != o.output.migrations[-1].name


async def _prepare(o: _Out, spec, keys_io) -> None:
    """Read a keyed write once, by its store (`Store.prepare`); for a patch,
    its run, and what dead attempts left in the store: repaired key by key
    (`_repair`), or — after an unknown `Sql` write — reconciled whole
    (`_reconcile`), which resolves the write too."""

    if o.info.get("index") is None:
        if o.sql and o.output.incremental:
            raise WriteError(f"{o.output.name}: Sql writes need a keyed output")
        return
    o.index = KeyIndex(keys_io, None, IndexState.from_json(o.info["index"]))
    if o.sql:
        if o.output.is_partition_set:
            raise WriteError(f"{o.output.name}: Sql writes need a table output")
        return
    o.prepared = await asyncio.to_thread(prepare_for, o.store, o.value, o.output)
    if o.replace:
        return
    removes = [key_bytes(k) for k in o.prepared.removes]
    try:
        o.run = await asyncio.to_thread(SortedRun.from_rows, o.prepared.rows, removes)
    except ValueError as e:  # a value with no digest
        raise WriteError(f"{o.output.name}: {e}") from e
    if any(intent.get("unknown") for intent in o.unsettled):
        # A dead Sql writer's keys are unknown (docs/resolved-commits.md §3): the index
        # takes the whole store as it is, with this patch on top, and the store
        # writes this patch's keys, every one.
        o.files, _ = await _reconcile(o, spec)
        return
    if o.unsettled:
        o.intended = frozenset(await _intended(o.info, keys_io, o.unsettled))
        left = [k for k in o.intended if k not in o.prepared.rows and k not in o.prepared.removes]
        if left:
            o.run = await _repair(o, sorted(left))


async def _resolve(o: _Out, spec, answer) -> None:
    """The write's delta: the engine's answer, uploaded as this attempt's
    own; or a replacement streamed against the whole index; or a patch."""

    batch, attempt = int(o.info["batch"]), spec["attempt"]
    generation = int(spec.get("generation") or 0)  # each entry's locator (lifecycle.md §9.8)
    if answer is not None:
        o.files, o.changed = await _upload(o.index, batch, attempt, answer)
    elif o.replace:
        try:
            o.files, o.changed = await o.index.replace(
                o.prepared.rows, batch, attempt, collect=LISTED, generation=generation
            )
        except ValueError as e:  # a value with no digest, found as the join reaches it
            raise WriteError(f"{o.output.name}: {e}") from e
    else:
        o.files, o.changed = await o.index.resolve(
            o.run, batch=batch, attempt=attempt, generation=generation, collect=LISTED
        )


def _keyed_write(o: _Out, keys_io) -> KeyedWrite:
    """What the store is given: each key it writes to its version, and the
    keys it deletes — or the write whole."""

    p, changed = o.prepared, o.changed
    if o.output.is_partition_set:
        own = set(p.take(None))
        if o.replace:
            o.elements = sorted(own)
        else:
            o.elements = sorted((set(o.info.get("elements") or ()) - set(p.removes)) | own)
    if changed is not None:
        upserted = {key_str(k): v for k, v in changed[0].items()}
        deleted = frozenset(map(key_str, changed[1]))
    if o.kind == "immutable":
        # Every object it writes must be named by an index entry, or nothing ever
        # collects it: exactly the keys the delta writes, paged from its files
        # when there are too many to list (docs/lifecycle.md §9.8).
        if changed is None:
            return KeyedWrite(p, DeltaKeys(keys_io, o.index.prefix, tuple(o.files.files)), whole=o.replace)
        return KeyedWrite(p, upserted, deleted, whole=o.replace)
    if o.prior is None or (o.replace and (changed is None or o.unsettled)):
        # A first write, or a replacement with more changes than it lists, or with
        # dead attempts': the scope rewritten.
        return KeyedWrite(p, whole=True)
    if o.replace:
        return KeyedWrite(p, upserted, deleted)
    # A patch: its own keys that changed, and whatever a dead attempt may have
    # left half-done among them; past what it lists, all of its own keys.
    removes = frozenset(p.removes)
    if changed is None:
        return KeyedWrite(p, None, removes)
    mine = {k: v for k, v in upserted.items() if k in p.rows}
    redo = sorted(k for k in o.intended if k in p.rows and k not in mine)
    if redo:
        mine.update(zip(redo, p.rows.versions(redo), strict=True))
    return KeyedWrite(p, mine, deleted | (removes & o.intended))


def _ask_for(o: _Out, spec: dict) -> Ask:
    run = SortedRun.from_rows(o.prepared.rows) if o.replace else o.run
    batch = int(o.info["batch"])
    return Ask(
        o.name,
        spec["partition"],
        "replace" if o.replace else "patch",
        batch,
        int(spec.get("generation") or 0),
        o.info["index"]["prefix"],
        batch - 1,
        run,
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
        return DeltaFiles([], 0, 0, True), ({}, [])
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


async def _repair(o: _Out, left: list[str]) -> SortedRun:
    """Take in what dead attempts left in the store (§8). Keys this patch
    writes or removes end as it says either way; the others (`left`) are
    read back, and the index learns what landed as part of this commit's
    delta: the patch's run with theirs overlaid."""

    found: dict[str, bytes] = {}
    gone: list[str] = []
    for i in range(0, len(left), REPAIR_PAGE):
        page = left[i : i + REPAIR_PAGE]
        loaded = await o.store.load(o.prior, None, Keys(dict.fromkeys(page, (b"", 0))))
        read = await asyncio.to_thread(prepare_for, o.store, loaded, o.output)
        got = dict(await asyncio.to_thread(read.entries))
        found.update(got)
        gone.extend(k for k in page if k not in got)
    keys, versions = o.prepared.rows.entries()
    keys += [key_bytes(k) for k in found]
    versions += list(found.values())
    removes = [key_bytes(k) for k in (*o.prepared.removes, *gone)]
    return SortedRun.of(keys, versions, removes)


async def _reconcile(o: _Out, spec):
    """The delta of a patch over a store a dead `Sql` writer changed in ways no
    key list records: the store's rows as they are — streamed back sorted a
    chunk at a time by its `scan`, versioned as a `Sql` write's rows are,
    folded natively — with the patch's run laid over them (its keys' groups
    in place of the store's, its removes gone),
    against the pinned index: a streamed replacement. Memory is a chunk and
    the patch."""

    output, store = o.output, o.store
    scan = getattr(store, "scan", None)
    if scan is not None:
        chunks = scan(o.prior, output)
        stamped = getattr(store, "stamped", None)
        rows = {"key": output.key, "revision": output.revision}
        rows["exclude"] = tuple(stamped(output)) if stamped is not None else ()
    else:  # a store that cannot stream its rows back: read whole, and sorted as entries
        loaded = await store.load(o.prior, None, None)
        chunks = [await asyncio.to_thread(lambda: prepare_for(store, loaded, output).entries())]
        rows = {}
    return await o.index.replace(
        chunks,
        int(o.info["batch"]),
        spec["attempt"],
        collect=LISTED,
        generation=int(spec.get("generation") or 0),
        overlay=o.run,
        **rows,
    )


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
            control["reads"] = Reads.from_json(answer.get("reads"))  # the engine's answers to its reads
        except Ended:
            return ENDED
        except Exception:
            started = None  # unreachable for now: the reporter falls back to `.worker`
    else:
        started = None
    shipper = LogShipper(objects, base, channel, invocation)
    writes = Writes()
    execution = asyncio.create_task(
        _execute(objects, base, spec, entrypoint, timeline, shipper, writes, invocation, control)
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
        await _publish(objects, base, result, invocation, writes, control["cancel"], timeline, shipper)
        flusher.cancel()
        if channel is not None:
            with contextlib.suppress(Exception):
                answer = await channel.finished({"invocation": invocation})
                await _discard_after(answer, spec, control.get("project"), objects, channel, invocation)
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


async def _publish(objects, base, result, invocation, writes, cancel, timeline, shipper) -> None:
    """Seal the result once and create `.result` with exactly those bytes,
    retried as they are: a failure to publish never changes what is
    published. A worker that cannot publish raises, and the engine treats
    it as a worker that died."""

    log = await shipper.finish()
    timeline.add("finished")

    def seal(result: dict) -> bytes:
        body = {"invocation": invocation, **result, "writes": writes.state, **timeline.report(), "log": log}
        if cancel is not None and "cancel" not in body:  # an Each page sealed its own record
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
    objects, base, spec, entrypoint, timeline, shipper, writes, invocation, control
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
        control["project"] = project  # for the discards after its commit
    except Exception as error:
        return _failed(error, False)
    timeline.add("imported")
    if project.manifest["revision"] != spec["revision"]:
        mismatch = (
            f"revision mismatch: spec {spec['revision'][:12]} != project {project.manifest['revision'][:12]}"
        )
        failed = _failed(StoreError(mismatch), False)
        failed["error"]["build"] = project.manifest.get("build")  # how this host computed its revision
        return failed
    asset = project.assets[spec["asset"]]
    try:
        # Index files straight from the store, but for the reads the engine answered at
        # `start` (docs/resolved-commits.md §7.1); small writes are the engine's too.
        keys_io = ObjectIO(objects, served=control.get("reads"))
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
            # A drained page commits what finished (docs/lifecycle.md §7). Its interrupted
            # keys follow the cancel record it is sealed with, as latched now — after its
            # store writes — and the result carries that record, not a later one (§2.2).
            cancel = control.get("cancel")
            result.update(await ran["finish"](cancel))
            result["status"] = "canceled" if ran["drained"] else "succeeded"
            if cancel is not None:
                result["cancel"] = cancel.to_json()
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


async def _discard_after(answer, spec, project, objects, channel, invocation) -> None:
    """Discard what the engine says is due in this attempt's scope now that
    its commit is durable (docs/lifecycle.md §9.8), and say so. Anything
    that fails here leaves the entries queued for the scope's next attempt:
    deleting a name twice is no harm."""

    if not (answer or {}).get("discard") or project is None:
        return
    due = {"outputs": answer["discard"], "partition": spec["partition"], "attempt": spec["attempt"]}
    done = await _discard_due(due, project, project.assets[spec["asset"]], objects, Writes())
    if done:
        await channel.discarded({"invocation": invocation, "scope": answer["scope"], **done})


def _file_entries(data: bytes):
    """A `.kx` file's entries, every block checked: `(key, version, deleted,
    locator, predecessor)` each, in key order."""

    from solera.keys import check_block, decode_block, parse_index

    index = parse_index(data, len(data))
    for _, offset, size, _, crc in index["blocks"]:
        block = data[offset : offset + size]
        check_block(block, crc)
        yield from zip(*decode_block(block, index["codec"]), strict=True)


async def _discard_due(spec, project, asset, objects, writes) -> dict:
    """Discard the data garbage the engine handed this attempt (docs/
    lifecycle.md §9.8): the objects a commit or a compaction let go of, and
    what attempts that never committed wrote — all past every reader pin.
    The engine runs no store code; the scope's next attempt, which has its
    store, deletes for it. Returns what was done, for the result."""

    import obstore
    from solera.keys import decode_garbage

    declared = {o["name"]: o for o in project.manifest["assets"][asset.name]["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    discarded, unresolved, files = {}, {}, []

    async def read(path: str) -> bytes | None:
        return await _get(objects, path)

    for name, info in (spec.get("outputs") or {}).items():
        store = project.stores[declared[name]["store"]]
        if not info.get("discard") or store.writes != "immutable":
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
                        for key, _, _, _, before in _file_entries(data):
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
                                for key, version, deleted, _, _ in _file_entries(data) if data else ():
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
    pool's routes and nothing else.

    The project is imported once; each attempt runs in a child forked from
    this warm process, as a process of its own: a forced cancel ends the
    child, and with it any thread the attempt left running."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
    host = f"{socket.gethostname()}:{os.getpid()}"
    loaded = load_project(os.environ["SOLERA_PROJECT"])
    project = project or loaded.manifest["name"]
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
                code = await _forked(stage, server, loaded)
                print(f"[pool] {stage['attempt']} exited {code}", flush=True)
                if code != LOST:  # it ran: ask again, what waits has changed
                    break


LOST = 4  # the exit code of an attempt that could not publish: the engine treats it as dead


async def _forked(stage: dict, server: str, project: Project) -> int:
    """Run one pool attempt in a child of this process; its exit code. This
    process must not have used the object store: a forked child cannot use
    the async runtime it would have started."""

    import multiprocessing

    child = multiprocessing.get_context("fork").Process(
        target=_child, args=(stage, server, project), name=f"attempt {stage['attempt']}"
    )
    child.start()
    while child.exitcode is None:  # polled: no thread here, so the next fork copies a quiet process
        await asyncio.sleep(0.1)
    return child.exitcode


def _child(stage: dict, server: str, project: Project) -> None:
    async def attempt():
        code = LOST
        try:
            code = await run_attempt(
                stage["objects"],
                stage["attempt"],
                project,
                run=stage["run"],
                engine_url=server,
                pool=True,
                own_process=True,
            )
        except BaseException:
            traceback.print_exc()
        # Here, not once asyncio.run returns: it would first wait for the
        # executor's threads, a call the attempt gave up on among them.
        _exit(code)

    asyncio.run(attempt())


def _exit(code: int) -> None:
    """End an attempt's process once its result is published (or never
    will be): at once, so threads it abandoned — a canceled synchronous
    call — end with it rather than keep it alive."""

    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


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
        code = LOST
        try:
            code = await run_attempt(
                options["--objects"],
                options["--attempt"],
                os.environ["SOLERA_PROJECT"],
                run=options["--run"],
                own_process=True,
            )
        except BaseException:
            traceback.print_exc()
        _exit(code)
    if mode == "pool":
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        token = options.get("--token") or os.getenv("SOLERA_POOL_TOKEN") or os.getenv("SOLERA_API_TOKEN")
        await run_pool(options["--pool"], options["--server"].rstrip("/"), token)
        return
    if mode == "sensors":
        # solera_worker sensors --pool NAME --server URL [--token T] [--parent PID] (SOLERA_PROJECT env entrypoint)
        from .sensors import ORPHANED, HttpSensorChannel, run_sensor_host

        options = dict(zip(rest[::2], rest[1::2], strict=True))
        token = options.get("--token") or next(
            filter(None, map(os.getenv, ("SOLERA_SENSOR_TOKEN", "SOLERA_POOL_TOKEN", "SOLERA_API_TOKEN"))),
            None,
        )
        project = load_project(os.environ["SOLERA_PROJECT"])
        channel = HttpSensorChannel(options["--server"].rstrip("/"), project.manifest["name"], token)
        parent = int(options["--parent"]) if "--parent" in options else None
        try:
            code = await run_sensor_host(channel, project, options["--pool"], parent=parent)
        finally:
            await channel.close()
        if code == ORPHANED:  # the engine that started this host is gone
            raise SystemExit(0)
        # Done ticking, or a tick overran on a thread that cannot be stopped: start afresh.
        os.execv(sys.executable, [sys.executable, "-m", "solera_worker", *args])
    raise SystemExit(f"unknown mode: {mode}")
