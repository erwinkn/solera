"""User code runs here, never in the API process (§10 worker protocol).

`python -m solera_worker run --objects URL --run RUN --attempt ID`:
read the spec from the attempt file -> refuse on deploy mismatch -> resolve `env:` -> load inputs per
annotation (Incremental inputs through the upstream key index) -> build ctx ->
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
import functools
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

from solera import errors, lifecycle
from solera.keys import SortedEntries
from solera.keys.io import ObjectIO
from solera.keys.layers import (
    DeltaFiles,
    DeltaKeys,
    LayerIndex,
    LayerState,
    Part,
    delta_keys,
    delta_names,
    key_bytes,
    key_str,
    replaced_entries,
)
from solera.keys.resolver import Ask, answers, delta_files, request
from solera.lifecycle import Cancel, Ended
from solera.objects import Conflict, swap
from solera.sdk import (
    UNSET,
    Asset,
    Batch,
    Output,
    Project,
    Ref,
    Result,
    TimePartitions,
    Upstream,
    dict_arg,
    split_partition,
)
from solera.stores import (
    Commits,
    KeyedWrite,
    Keys,
    Opaque,
    Patch,
    Prepared,
    StoreError,
    WriteContext,
    WriteError,
    check_loaded,
    prepare_for,
    resolve_env,
)
from solera.tasks import Tasks, retrying

from . import each
from .observed import Observed
from .reporting import LogShipper, Reporter
from .sources import reader


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
        self,
        spec,
        asset: Asset,
        project: Project,
        objects,
        batch,
        shipper,
        timeline,
        keys_io=None,
        observed=None,
    ):
        self._objects, self._shipper, self._timeline = objects, shipper, timeline
        self._keys_io = keys_io
        self._observed = observed if observed is not None else Observed()
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
        self.batch = batch
        self.run_id = spec["run"]["id"]
        self.config = spec["run"].get("config") or {}
        self.placement = spec.get("placement") or {
            "executor": "local",
            "kind": "Local",
            "config": {},
            "options": {},
        }
        self._stores = project.stores
        self._outputs = [o.name or asset.name for o in asset.outputs]
        self._pinned = spec.get("outputs") or {}
        self._metadata: dict[str, dict] = {}
        # A per-key call's key, and the generation of its upstream entry: the
        # version it runs at (docs/per-key-processing.md §5, docs/versions.md).
        self.key: str | None = None
        self.generation: int | None = None

    def _for_key(self, key: str, generation: int) -> Ctx:
        """The `ctx` of one per-key call: the same attempt, its key named, its log
        lines tagged with it."""

        one = copy.copy(self)
        one.key, one.generation = key, generation
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

    async def load(self, ref: Ref | str | None = None, t=None):
        """Read what the producer holds, whole. `ctx.load()`, or
        `ctx.load("name")` for one of several outputs: the asset's own
        output as committed at the attempt's pin — what a total kept from
        `ctx.batch` changes builds on — or None before its first commit, and
        in a full run until that run's first commit: the write that starts
        the content over builds on nothing. `ctx.load(ref, t)`: a ref an
        input gave. Not an input read of the attempt's: lineage does not
        record it."""

        index = None
        if not isinstance(ref, Ref):
            info = self._pinned.get(self._output("load", ref)) or {}
            if info.get("before") is None or info.get("reset"):
                return None
            ref, index = Ref.from_json(info["before"]), info.get("index")
        store = self._stores[ref.store]
        index = index or self._indexes.get((ref.output, ref.partition))
        value = await _load_whole(_unobserved, store, ref, t, self._keys_io, index)
        self._timeline.add("loaded", ref.output, _rows(value), optional=True)
        return value

    def mark(self, name: str):
        """Mark a moment in the run's timeline — `ctx.mark("trained")` —
        to see where the time went between the attempt's own steps."""

        self._timeline.add("mark", name, optional=True)

    def _output(self, call: str, output: str | None) -> str:
        """The output a call names, by default the asset's only one."""

        if output is None:
            if len(self._outputs) != 1:
                raise ValueError(f"ctx.{call}: name the output (this asset has several, or none)")
            output = self._outputs[0]
        if output not in self._outputs:
            raise ValueError(f"ctx.{call}: {output!r} is not an output of this asset")
        return output

    def metadata(self, output: str | None = None, /, **values):
        """Record facts about the version this attempt writes — row counts,
        a checksum, a model's score — in the run history (§7), to chart
        across versions. `output` defaults to the asset's only output."""

        output = self._output("metadata", output)
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


async def _resolve_inputs(spec, project, asset, keys_io, timeline, observed: Observed):
    """Load each pin by annotation; build call args + ctx.batch (§5, §10).

    An Incremental input over a keyed upstream loads the keys of the batch
    the engine planned, each with its class, classed again from what a
    source served (`each.observe`); `delivered` reports what it observed of
    each, for the engine's record (docs/observed-set.md). Each input loaded
    is a `loaded` event; `observed` records what each read saw."""

    manifest_asset = project.manifest["assets"][asset.name]
    inputs = manifest_asset["inputs"]
    hints = project.hints[asset.name]  # resolved once, at registration
    args, batch, delivered = {}, {}, {}
    nothing = []  # per keyed batch: whether a source served none of its classes
    for name, pin in spec["inputs"].items():
        input = inputs.get(name)
        if input is None or "each" in pin:  # a dep pin: recorded, never bound; a per-key batch: per key
            continue
        param = name
        t = hints.get(param)
        if "refs" in pin:  # a whole fan-in: by partition
            inner = dict_arg(t)
            out = {}
            indexes = pin.get("indexes") or {}
            as_ref = pin.get("load", "data") == "ref"  # decided at registration, as the engine read for it
            for key, ref_json in pin["refs"].items():
                ref = Ref.from_json(ref_json)
                if as_ref:
                    out[key] = ref
                    continue
                store = reader(project, ref)
                out[key] = await _load_whole(
                    observed.load,
                    store,
                    ref,
                    inner,
                    keys_io,
                    indexes.get(key),
                    _key_column(project, ref.output),
                )
            args[param] = out
            timeline.add("loaded", param)
            continue
        ref = Ref.from_json(pin["ref"])
        store = reader(project, ref)
        if "batch" in pin:  # Incremental: selection + ctx.batch (§5.1)
            ch = pin["batch"]
            full = bool(ch.get("full"))
            if "commits" in ch:
                lo, hi = (int(v) for v in ch["commits"])
                args[param] = await observed.load(store, ref, t, Commits(lo, hi))
                batch[param] = Batch(
                    rows=args[param],
                    full=full,
                    index=int(ch.get("index") or 0),
                    count=int(ch.get("count") or 1),
                    final=not ch.get("more"),
                    upstream=Upstream(ref.output, range(lo, hi + 1)),
                )
                timeline.add("loaded", param, _rows(args[param]))
                continue
            keys = ch["keys"]
            load = {k: g for k, cls, _, g, _ in keys if cls != "removed"}
            args[param] = await observed.load(store, ref, t, Keys(load))
            served = getattr(store, "served", None)
            if served is None:
                # Not a source: rows follow the store, classes the index (D100). A key a
                # current-only store no longer has is delivered in its class with no
                # row, unless the head still names it: the store is behind, SourceBehind.
                key = _key_column(project, ref.output)
                await each.gone_since(ref.output, key, args[param], load, pin, keys_io)
            classes, seen = each.observe(keys, served)
            batch[param] = Batch(
                rows=args[param],
                added=tuple(classes["added"]),
                updated=tuple(classes["updated"]),
                removed=tuple(classes["removed"]),
                unchanged=tuple(classes["unchanged"]),
                full=full,
                index=int(ch["index"]),
                count=int(ch["count"]),
                final=bool(ch["final"]),
                upstream=Upstream(ref.output),
                served={k: v for k, v in seen.items() if v is not None},
            )
            delivered[param] = {"observed": seen}
            if not full:  # a full run's first batch starts its consumer over, keys or none
                nothing.append(keys and not any(classes.values()))
            timeline.add("loaded", param, _rows(args[param]))
            continue
        if pin.get("load", "data") == "ref":  # decided at registration, as the engine read for it
            args[param] = ref
        else:
            args[param] = await _load_whole(
                observed.load, store, ref, t, keys_io, pin.get("index"), _key_column(project, ref.output)
            )
            timeline.add("loaded", param, _rows(args[param]))
    # Every keyed batch held keys, and a source served none of them that changes
    # anything: nothing to call the producer with; its observations commit.
    delivered["*nothing"] = bool(nothing) and all(nothing)
    return args, batch, delivered


async def _unobserved(store, ref, t, selection):
    return await store.load(ref, t, selection)


def _key_column(project, output: str) -> str | None:
    """The key column of an output an attempt reads: a source's or an asset's."""

    if output in project.sources:
        return project.sources[output].key
    for asset in project.assets.values():
        for o in asset.outputs:
            if o.name == output:
                return o.key
    return None


async def _load_whole(load, store, ref, t, keys_io, index_json, key: str | None = None):
    """A whole read. From an immutable store, a keyed one names its objects
    from the live entries of its pinned index (`Keys`), since a listing would
    also show superseded and abandoned ones (docs/lifecycle.md §9.8): it is
    loaded a page of the index at a time, and the pages put together. Every
    key the index names must come back (`check_loaded`)."""

    if index_json is None or store.writes != "immutable":
        return await load(store, ref, t, None)
    index = LayerIndex(keys_io, LayerState.from_json(index_json))
    paged = _plain(t)  # a type of the store's own (a DataFrame): one read, which the store makes
    parts, entries, after = [], {}, None
    while True:
        rows, after = await index.delta(None, after=after, first=REPAIR_PAGE)
        entries.update((key_str(r[0]), r[3]) for r in rows)
        if paged:
            parts.append(await store.load(ref, t, Keys(entries)))
            check_loaded(ref.output, key, parts[-1], entries)
            entries = {}
        if after is None:
            break
    if not paged:
        value = await store.load(ref, t, Keys(entries))
        check_loaded(ref.output, key, value, entries)
        return value
    if len(parts) == 1:
        return parts[0]
    if all(isinstance(p, Mapping) for p in parts):
        return {k: v for p in parts for k, v in p.items()}
    return [item for p in parts for item in p]


def _plain(t) -> bool:
    """Whether a read as `t` is plain Python — untyped, a list or a dict —
    so its pages are put together here."""

    return t is None or t in (list, dict) or typing.get_origin(t) in (list, dict, Mapping)


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
    worker_id=None,
    channel=None,
):
    """Store each returned output (§4, §6, §8, §9), in two phases.

    Planning compares each keyed output's write with its key index as pinned
    in the spec, and writes the changes as the commit's delta file: every key
    written is one, at the attempt's generation (docs/versions.md), and a
    write that changes nothing — an empty patch, a set listed again — is
    not stored at all and keeps the head. Then
    `fence(intents, gated)` begins writing: on stores that take a gate
    (all but `immutable` ones), it takes the attempt's gate first, listing
    their delta files — the keys this attempt is about to change — and only
    then do the stores write. An engine that finds the fence taken by a worker that then
    died keeps the outputs owing a repair, and their intents, for the next attempt
    to repair (`_repair`). Returns `{name: entry}` and the cursor."""

    manifest_asset = project.manifest["assets"][asset.name]
    declared = {o["name"]: o for o in manifest_asset["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
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
        o = outs[name] = _Out(name, decls[name], store, pinned.get(name) or {}, value)
        if o.kind == "fenced":
            await store.acquire(o.context(spec, worker_id), o.prior)

    # Prepare and resolve: each keyed write read once, and compared with its key
    # index as pinned in the spec; its changes are the commit's delta file. Small
    # writes are resolved by the engine from its cache, all of an attempt's in one
    # request (docs/resolved-commits.md §4); the rest, and any it declines, here.
    for o in outs.values():
        await _prepare(o, spec, keys_io)
    asks = [_ask_for(o, spec) for o in outs.values() if o.asks()]
    engine = await _ask_engine(channel, worker_id, asks)
    intents, entries = {}, {}
    for name, o in list(outs.items()):
        if o.index is None:
            continue
        if o.opaque:
            # Rows the worker never sees: the store reports the whole new key
            # map once it wrote, so its delta comes after — and needs no repair.
            # Unknown writes: if this attempt dies after its gate, no key list says what landed
            # (docs/lifecycle.md §9.6): the next attempt reconciles the whole partition.
            intents[name] = {
                **DeltaFiles(Part(), 0, 0, int(spec.get("generation") or 0)).to_json(),
                "unknown": True,
            }
            continue
        if o.files is None:
            await _resolve(o, spec, engine.get(name))
        if not o.files.part.files and o.prior is not None and not o.repairs:
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
        # The engine has cleaned up this attempt's delta files; these came after.
        import obstore

        paths = [outs[n].index.state.path(f) for n, files in intents.items() for f in delta_names(files)]
        with contextlib.suppress(Exception):
            await obstore.delete_async(objects, paths)
        raise

    # Write: nothing reaches a store before the fence is ours.
    for name, o in outs.items():
        output, store, store_name = o.output, o.store, declared[name]["store"]
        context = o.context(spec, worker_id)
        schema = None
        if output.migrations:
            migrate = getattr(store, "migrate", None)
            if not callable(migrate):
                raise StoreError(
                    f"{output.name}: store {store_name!r} has no migrate for declared migrations"
                )
            try:
                applied = await writes.call(
                    migrate(output, output.migrations, context=context, prior=o.prior)
                )
            except StoreError:
                raise
            except Exception as error:
                raise StoreError(f"{output.name}: migration failed: {error}") from error
            schema = applied[-1] if applied else output.migrations[-1].name
        if o.schema_only:
            ref = o.prior
            handle = {**(ref.handle or {}), "schema": schema}
            entries[name] = {"ref": dataclasses.replace(ref, handle=handle).to_json(), "keys": intents[name]}
            continue
        written = await writes.call(store.store(o.write or o.value, o.prior, context))
        entry = {}
        if o.index is not None and o.opaque:
            if written.keys is None:
                raise StoreError(f"{output.name}: store {store_name!r} reported no keys for an opaque write")
            files, _ = await o.index.write_replace(
                chunks=written.keys,
                name=f"{int(o.info['commit_number']):012d}-{spec['attempt']}",
                generation=int(spec.get("generation") or 0),
                replaced=o.kind == "immutable",
            )
            entry["keys"] = files.to_json()
        elif o.index is not None:
            entry["keys"] = intents[name]
        if written.ref is None:
            continue
        ref = dataclasses.replace(written.ref, store=store_name)
        if ref != o.prior:  # a store that wrote nothing gives back its prior: the head stands
            ref = dataclasses.replace(ref, generation=int(spec.get("generation") or 0))
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
CLEANUPS_AT_ONCE = 64  # store.cleanup calls in flight
LISTED = 1_000_000  # changed keys a replacement lists for its store; past it, the store rewrites it
RESOLVE_KEYS = 100_000  # a write the engine resolves: its keys (docs/resolved-commits.md §4)...
RESOLVE_ENTRIES = 2_000_000  # ...and for a replacement, its keys plus the live ones
RESOLVE_TIMEOUT = 5.0  # seconds the worker waits for the engine before resolving itself


@dataclasses.dataclass
class _Out:
    """One output's write in an attempt, as the phases leave it: read once
    (`prepared`); for a patch, its `run` of upserts and removes, repair's
    with it; resolved against its key index (`files`, and up to `LISTED`
    keys what `changed`); and what its store is given (`write`)."""

    name: str
    output: Output
    store: Any
    info: dict  # the spec's pin of the output
    value: Any
    index: LayerIndex | None = None
    prepared: Prepared | None = None
    run: SortedEntries | None = None
    intended: frozenset[str] = frozenset()  # keys dead attempts meant to change
    files: DeltaFiles | None = None
    changed: tuple | None = None  # ([written key], [removed key]), or None past LISTED
    write: KeyedWrite | None = None
    schema_only: bool = False  # unchanged content, migrations to apply

    @property
    def kind(self) -> str:
        return self.store.writes

    @property
    def prior(self) -> Ref | None:
        """The committed ref: where the content is, and — unless the write
        starts it over (`reset`) — what it builds on."""

        before = self.info.get("before")
        return Ref.from_json(before) if before is not None else None

    @property
    def reset(self) -> bool:
        """Whether the write starts the content over: a first write, or a full run."""

        return bool(self.info.get("reset"))

    @property
    def opaque(self) -> bool:
        return isinstance(self.value, Opaque)

    @property
    def replace(self) -> bool:
        """A full replacement: a bare write, or a Patch that starts the content
        over, which is then the whole content."""

        return not isinstance(self.value, Patch) or self.reset

    @property
    def repairs(self) -> list:
        return self.info.get("repairs") or []

    def context(self, spec, worker_id) -> WriteContext:
        return WriteContext(
            output=self.output,
            partition=spec["partition"],
            home=self.info.get("home"),
            commit_number=self.info.get("commit_number"),
            attempt=spec["attempt"],
            reset=self.reset,
            generation=spec.get("generation"),
            worker_id=worker_id,
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
    return (o.prior.handle or {}).get("schema") != o.output.migrations[-1].name


async def _prepare(o: _Out, spec, keys_io) -> None:
    """Read a keyed write once, by its store (`Store.prepare`); for a patch,
    its run, and what dead attempts left in the store: repaired key by key
    (`_repair`), or — after an unknown opaque write — reconciled whole
    (`_reconcile`), which resolves the write too (docs/versions.md §5)."""

    if o.info.get("index") is None:
        if o.opaque and o.output.incremental:
            raise WriteError(f"{o.output.name}: an opaque write needs a keyed output")
        return
    o.index = LayerIndex(keys_io, LayerState.from_json(o.info["index"]))
    if o.opaque:
        if o.output.is_dynamic_partitions:
            raise WriteError(f"{o.output.name}: an opaque write needs a table output")
        return
    o.prepared = await asyncio.to_thread(prepare_for, o.store, o.value, o.output)
    if o.replace:
        return
    removes = [key_bytes(k) for k in o.prepared.removes]
    try:
        o.run = await asyncio.to_thread(SortedEntries.from_rows, o.prepared.rows, removes)
    except ValueError as e:  # a key both written and removed
        raise WriteError(f"{o.output.name}: {e}") from e
    if any(intent.get("unknown") for intent in o.repairs):
        # A dead opaque writer's keys are unknown (docs/versions.md §5): the index takes
        # every key the store holds, with this patch on top, and the store writes
        # this patch's keys, every one.
        o.files, _ = await _reconcile(o, spec)
        return
    if o.repairs:
        o.intended = frozenset(await _intended(o.info, keys_io, o.repairs))
        left = [k for k in o.intended if k not in o.prepared.rows and k not in o.prepared.removes]
        if left:
            o.run = await _repair(o, sorted(left))


async def _resolve(o: _Out, spec, answer) -> None:
    """The write's delta: the engine's answer, uploaded as this attempt's
    own; or a replacement streamed against the whole index; or a patch."""

    name = f"{int(o.info['commit_number']):012d}-{spec['attempt']}"
    generation = int(spec.get("generation") or 0)  # each key's version (docs/versions.md)
    replaced = o.kind == "immutable"  # its cleanup deletes what each change replaced
    if answer is not None:
        o.files, o.changed = await _upload(o.index, name, generation, answer)
    elif o.replace:
        o.files, o.changed = await o.index.write_replace(
            o.prepared.rows, name=name, generation=generation, replaced=replaced, collect=LISTED
        )
    else:
        o.files, o.changed = await o.index.write_patch(
            o.run, name=name, generation=generation, replaced=replaced, collect=LISTED
        )


def _keyed_write(o: _Out, keys_io) -> KeyedWrite:
    """What the store is given: the keys it writes, and the keys it
    deletes — or the write whole."""

    p, changed = o.prepared, o.changed
    if changed is not None:
        upserted = frozenset(map(key_str, changed[0]))
        deleted = frozenset(map(key_str, changed[1]))
    if o.kind == "immutable":
        # Every object it writes must be named by an index entry, or nothing ever
        # collects it: exactly the keys the delta writes, paged from its files
        # when there are too many to list (docs/lifecycle.md §9.8).
        if changed is None:
            return KeyedWrite(
                p, DeltaKeys(keys_io, o.index.state.prefix, o.files.part, o.files.generation), reset=o.replace
            )
        return KeyedWrite(p, upserted, deleted, reset=o.replace)
    if o.reset or (o.replace and (changed is None or o.repairs)):
        # A first write, or a replacement with more changes than it lists, or with
        # dead attempts': the partition rewritten.
        return KeyedWrite(p, reset=True)
    if o.replace:
        return KeyedWrite(p, upserted, deleted)
    # A patch: its own keys — not those a repair found in the store, which are
    # there already — and its removes, of keys the index holds or a dead attempt
    # may have written; past what it lists, all of its own keys.
    removes = frozenset(p.removes)
    if changed is None:
        return KeyedWrite(p, None, removes)
    return KeyedWrite(p, frozenset(k for k in upserted if k in p.rows), deleted | (removes & o.intended))


def _ask_for(o: _Out, spec: dict) -> Ask:
    run = SortedEntries.from_rows(o.prepared.rows) if o.replace else o.run
    commit_number = int(o.info["commit_number"])
    return Ask(
        o.name,
        spec["partition"],
        "replace" if o.replace else "patch",
        commit_number,
        int(spec.get("generation") or 0),
        o.info["index"]["prefix"],
        commit_number - 1,
        run,
    )


async def _ask_engine(channel, worker_id, asks: list[Ask]) -> dict[str, tuple[dict, bytes | None]]:
    """The engine's answers to the outputs it resolved: a delta or "empty".
    Unreachable, slow, declining: no answer, and the worker resolves itself."""

    if not asks or channel is None or not hasattr(channel, "resolve"):
        return {}
    try:
        body = await asyncio.wait_for(channel.resolve(request(worker_id, asks)), RESOLVE_TIMEOUT)
        got = answers(body)
    except Exception:
        return {}
    return {name: a for name, a in got.items() if a[0]["result"] in ("delta", "empty")}


async def _upload(index: LayerIndex, name: str, generation: int, answer) -> tuple[DeltaFiles, tuple]:
    """The engine's delta, uploaded as this attempt's own (`name`: its commit and attempt)."""

    a, data = answer
    if data is None:
        return DeltaFiles(Part(), 0, 0, generation), ([], [])
    await index.io.write(index.state.path(f"{name}-0.lay"), data)
    return delta_files(a, name, generation), delta_keys(data, generation)


async def _intended(info, keys_io, repairs) -> list[str]:
    """The keys dead attempts meant to change (§8): each repair intent
    names the delta of an attempt that died while writing (`DeltaFiles`),
    and any of those writes may have landed: every key it writes or removes."""

    prefix, found = info["index"]["prefix"], set()
    for intent in repairs:
        delta = DeltaFiles.from_json(intent)
        for f in delta.part.files:
            data = await keys_io.read_whole(f"{prefix}{f.name}", f.size)
            written, removed = delta_keys(data, delta.generation)
            found.update(map(key_str, written + removed))
    return sorted(found)


async def _repair(o: _Out, left: list[str]) -> SortedEntries:
    """Take in what dead attempts left in the store (docs/versions.md §5),
    whose fence this attempt holds: they can write nothing more. Keys this
    patch writes or removes end as it says either way; of the others
    (`left`), the store says which it holds (`keys`), never their values. A
    key it holds takes this attempt's generation — whether or not the dead
    write landed, it changed — and one it lacks is removed: a tombstone if
    the index holds it, nothing if not."""

    held: set[str] = set()
    for i in range(0, len(left), REPAIR_PAGE):
        page = left[i : i + REPAIR_PAGE]
        chunks = await asyncio.to_thread(lambda page=page: list(o.store.keys(o.prior, page)))
        held.update(k for chunk in chunks for k in chunk)
    keys, payloads = o.prepared.rows.entries()
    keys += [key_bytes(k) for k in sorted(held)]
    payloads += [None] * len(held)
    removes = [key_bytes(k) for k in (*o.prepared.removes, *(k for k in left if k not in held))]
    return SortedEntries.of(keys, payloads, removes)


async def _reconcile(o: _Out, spec):
    """The delta of a patch over a store a dead opaque writer changed in ways no
    key list records (docs/versions.md §5): every key the store holds —
    streamed back sorted a chunk at a time by its `keys` — at this attempt's
    generation, with the patch's run laid over them (its keys in place of
    the store's, its removes gone), against the pinned index: a streamed
    replacement, so a live key the store lacks is removed. Memory is a chunk
    and the patch."""

    return await o.index.write_replace(
        chunks=o.store.keys(o.prior, None),
        overlay=o.run,
        name=f"{int(o.info['commit_number']):012d}-{spec['attempt']}",
        generation=int(spec.get("generation") or 0),
        replaced=o.kind == "immutable",
        collect=LISTED,
    )


class Writes:
    """Write-completion evidence (docs/lifecycle.md §2.3): `none` until a
    store call starts; `writing` while one runs, or if one raised or was
    abandoned; `complete` once every call made has returned."""

    def __init__(self):
        self.state = lifecycle.NONE

    async def call(self, work):
        self.state = lifecycle.WRITING
        result = await work
        self.state = lifecycle.COMPLETE
        return result


class _Stop(Exception):
    """A cancel was requested before the gate: stop, writing nothing."""


class ControlFile:
    """The attempt's control file as this worker last read or wrote it
    (docs/lifecycle.md §2.4): each swap names that version. A worker never
    creates the file: the engine did, before the launch."""

    def __init__(self, objects, run: str, attempt: str, worker_id: str):
        self.objects, self.run, self.attempt, self.worker_id = objects, run, attempt, worker_id
        self.path = f"{lifecycle.base(run, attempt)}{lifecycle.CONTROL}"
        self.etag: str | None = None
        self.intents: dict | None = None  # what it meant to write, once it took the gate

    async def own(self, claim: dict) -> str:
        """Swap `open` to `owned`: `owned` if this worker now owns the
        attempt, `lost` if another worker does, `ended` if the engine ended
        it or the file is gone."""

        try:
            found = await lifecycle.read_control(self.objects, self.run, self.attempt)
        except lifecycle.Malformed:  # not a file this worker can own: write nothing
            return lifecycle.ENDED
        while True:
            if found is None or found[0]["state"] == lifecycle.ENDED:
                return lifecycle.ENDED
            if found[0]["state"] != lifecycle.OPEN:
                return "lost"
            body = lifecycle.control(lifecycle.OWNED, worker_id=self.worker_id, **claim)
            try:
                self.etag = await swap(self.objects, self.path, body, found[1])
                return lifecycle.OWNED
            except Conflict:  # another worker, or the engine, came first: see which
                try:
                    found = await lifecycle.read_control(self.objects, self.run, self.attempt)
                except lifecycle.Malformed:
                    return lifecycle.ENDED

    async def move(self, state: str, **fields) -> bool:
        """Swap to `state` from the version this worker last wrote: `False`
        if refused. Only the engine writes over an owner, so a refusal means
        it ended the attempt (or retention took the file): write nothing
        more."""

        body = lifecycle.control(state, worker_id=self.worker_id, **fields)
        try:
            self.etag = await swap(self.objects, self.path, body, self.etag)
        except Conflict:
            return False
        if state == lifecycle.WRITING:
            self.intents = fields.get("intents") or {}
        return True


ENDED = ABORTED = 3  # the exit code of an attempt the engine ended: no result was published
LOSER_POLL = 30.0  # how often an worker that lost the claim looks for the owner's result


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
    """Run one attempt (docs/lifecycle.md §3): read its spec, own it, run
    it, seal its result — each a swap of its control file (§2.4).

    Owning is the first write: a worker that loses it touches nothing and,
    unless it is a pool worker (`pool`), waits for the attempt to end before
    exiting, so its exit never reads as the attempt's. The
    engine is reached through `channel`, or over HTTPS at `engine_url` (else
    the spec's); without one, the worker reports through `.beat` alone.

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
    worker_id = secrets.token_hex(8)
    claim = {"host": socket.gethostname(), "pid": os.getpid(), "at": time.time()}
    if channel is None and (engine_url or spec.get("engine")):
        from .channel import AttemptChannel, HttpTransport

        transport = HttpTransport(engine_url or spec["engine"], spec["token"])
        channel = AttemptChannel(transport, spec["project"], attempt)
    control_file = ControlFile(objects, run, attempt, worker_id)
    owned = await control_file.own(claim)
    if owned == lifecycle.ENDED:
        return ENDED
    if owned != lifecycle.OWNED:
        if not pool:
            await _await_owner(objects, run, attempt, loser_poll, channel, worker_id)
        return 0
    loop = asyncio.get_running_loop()
    control = {"cancel": None, "writing": False, "stopped": False, "forced": False}

    def on_cancel(record: Cancel):
        control["cancel"] = record
        if record.phase == "forced":
            on_ended()
        elif control.get("drain") is not None:
            # A per-key batch drains (per-key §5): no key starts, finished ones are stored.
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
            answer = await channel.start({"worker_id": worker_id, **claim})
            started = Cancel.from_json(answer.get("cancel"))
        except Ended:
            return ENDED
        except Exception:
            started = None  # unreachable for now: the reporter falls back to `.beat`
    else:
        started = None
    shipper = LogShipper(objects, base, channel, worker_id)
    writes = Writes()
    execution = asyncio.create_task(
        _execute(
            objects, base, spec, entrypoint, timeline, shipper, writes, control_file, control, objects_url
        )
    )
    reporter = Reporter(
        objects, base, worker_id, channel, spec.get("heartbeat", 10), timeline, on_cancel, on_ended
    )
    if started is not None:
        reporter.cancel = started
        on_cancel(started)
    reporter.start()
    tasks = Tasks("attempt")
    flusher = shipper.ship_on(tasks)
    try:
        try:
            result = await execution
        except (asyncio.CancelledError, _Stop):
            if control["forced"] or not control["stopped"]:
                raise
            result = {"status": "canceled"}  # requested before the gate: nothing written
        if result is None or control["forced"]:
            return ENDED
        if not await _publish(control_file, result, writes, control["cancel"], timeline, shipper):
            return ENDED  # the engine ended the attempt first: its result is not taken
        flusher.cancel()
        if channel is not None:
            with contextlib.suppress(Exception):
                await channel.finished({"worker_id": worker_id})
        return 1 if result["status"] == "failed" else 0
    except asyncio.CancelledError:
        if control["forced"] and not asyncio.current_task().cancelling():
            return ENDED
        raise
    finally:
        await tasks.close()
        await reporter.stop()
        if channel is not None:
            await channel.close()


async def _await_owner(objects, run: str, attempt: str, poll: float, channel=None, worker_id="") -> None:
    """A losing worker: wait until the attempt is over before exiting —
    its control file is final (sealed or ended) or gone, or the engine says
    `ended` (`not_owner` is no news)."""

    while True:
        try:
            found = await lifecycle.read_control(objects, run, attempt)
        except lifecycle.Malformed:  # nothing an owner of this version wrote: no reason to wait
            return
        if found is None or found[0]["state"] in lifecycle.FINAL:
            return
        if channel is not None:
            try:
                await asyncio.to_thread(channel.beat, {"worker_id": worker_id, "seq": 0})
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


async def _publish(control_file, result, writes, cancel, timeline, shipper) -> bool:
    """Seal the result once into the control file (`sealed`), retried with
    exactly those bytes: a failure to publish never changes what is
    published. `False` if the engine ended the attempt first: then the
    result is not taken. A worker that cannot publish raises, and the
    engine treats it as a worker that died."""

    log = await shipper.finish()
    timeline.add("finished")

    def seal(result: dict) -> dict:
        body = {
            "worker_id": control_file.worker_id,
            **result,
            "write": writes.state,
            **timeline.report(),
            "log": log,
        }
        if cancel is not None and "cancel" not in body:  # a per-key batch sealed its own record
            body["cancel"] = cancel.to_json()
        if control_file.intents is not None:  # what a repair reads back, if this one fails
            body["intents"] = control_file.intents
        lifecycle.control(lifecycle.SEALED, result=body)  # raises for what JSON cannot hold
        return body

    try:
        body = seal(result)
    except (TypeError, ValueError) as error:  # the result cannot be told as it is
        body = seal(_failed(error, True))
    return await retrying(
        lambda: control_file.move(lifecycle.SEALED, result=body), tries=PUBLISH_TRIES, base=0.2
    )


async def _execute(
    objects, base, spec, entrypoint, timeline, shipper, writes, control_file, control, state
) -> dict | None:
    """Run the attempt: its result, or `None` once the engine ended it. Every
    store root it may write or clean up is first claimed for its namespace,
    whose state is at `state` (`claim`)."""

    worker_id = control_file.worker_id

    async def fence(intents: dict, gated: bool):
        """Begin writing — unless a cancel was requested: then stop, writing
        nothing. Writing to a store that takes a gate, take it first: swap
        the control file to `writing`, with the intents (docs/lifecycle.md
        §2.4). Refused, the engine ended this attempt. From here on, a
        requested cancel drains."""

        if control["stopped"]:
            raise _Stop()
        if gated and not await control_file.move(lifecycle.WRITING, intents=intents):
            raise Aborted(spec["attempt"])
        control["writing"] = True
        timeline.add("writing")

    try:
        project = entrypoint if isinstance(entrypoint, Project) else load_project(entrypoint)
    except Exception as error:
        return _failed(error, False)
    timeline.add("imported")
    if project.manifest["deploy"] != spec["deploy"]:
        mismatch = f"deploy mismatch: spec {spec['deploy'][:12]} != project {project.manifest['deploy'][:12]}"
        failed = _failed(StoreError(mismatch), False)
        failed["error"]["build"] = project.manifest.get("build")  # how this host computed its deploy
        return failed
    if (cleanup := spec.get("cleanup")) is not None:  # a cleanup task: no asset of its own (§9.8, K25)
        try:
            if "outputs" in cleanup:  # a partition's entries, deleted through its output's store
                await claim(_stores_of(project, cleanup["asset"]), state)
                done = await _cleanup_due(cleanup, project, project.assets[cleanup["asset"]], objects, writes)
                return {"status": "succeeded", "outputs": {}, **done}
            return await _retire(cleanup, project, writes, state)  # an output life's leftovers
        except Exception as error:  # a store that refused: retried within the task's budget
            return _failed(error, getattr(error, "retryable", True))
    asset = project.assets[spec["asset"]]
    observed = Observed()
    try:
        await claim(_stores_of(project, asset.name), state)
        # Index files straight from the store; small writes are the engine's
        # (docs/resolved-commits.md §4).
        keys_io = ObjectIO(objects)
        args, batch, delivered = await _resolve_inputs(spec, project, asset, keys_io, timeline, observed)
        nothing = delivered.pop("*nothing", False)
        ctx = Ctx(spec, asset, project, objects, batch, shipper, timeline, keys_io, observed)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resolve_env(resource)  # env: secrets resolve in the worker (§5)
        each_input = next(((p, pin) for p, pin in spec["inputs"].items() if "each" in pin), None)
        if each_input is not None:
            control["drain"] = asyncio.Event()
            ran = await each.run(spec, project, asset, *each_input, args, ctx, keys_io, timeline, control)
            await observed.close()
            if "abort" in ran:
                return _user_failed(ran["abort"], project)
            value = Result(outputs=ran["values"])
            delivered[each_input[0]] = ran["delivered"]
        elif nothing:
            # A source served none of the batch's keys that changes anything: the producer
            # has nothing to see, and the batch commits only what it observed.
            return {"status": "succeeded", "skipped": True, "outputs": {}, "delivered": delivered}
        else:
            await observed.close()  # the inputs' moment ends: a long producer holds no snapshot
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
            worker_id,
            shipper.channel,
        )
        for name, values in metadata.items():
            if values and "ref" in outputs.get(name, {}):
                outputs[name]["metadata"] = values
        result = {"status": "succeeded", "outputs": outputs, "delivered": delivered}
        if read := observed.report():
            result["read"] = read
        if each_input is not None:
            # A drained batch commits what finished (docs/lifecycle.md §7). Its interrupted
            # keys follow the cancel record it is sealed with, as latched now — after its
            # store writes — and the result carries that record, not a later one (§2.2).
            cancel = control.get("cancel")
            result.update(await ran["finish"](cancel))
            result["status"] = "canceled" if ran["drained"] else "succeeded"
            if cancel is not None:
                result["cancel"] = cancel.to_json()
            if ran["skipped"]:
                result["skipped"] = True
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
    finally:
        with contextlib.suppress(Exception):  # a reader that will not close holds nothing we need
            await observed.close()


async def _retire(entry: dict, project, writes, state: str) -> dict:
    """A cleanup task (K25): delete what an output life left in a store it
    was removed or moved from — `store.cleanup(output, home, before=G)`, G
    the first generation after, so a later life of the name keeps what it
    wrote. A built-in store is rebuilt from its manifest description; a
    store of the project's own is the one of that name it still declares,
    and gone, the task fails for good, saying so."""

    from solera.stores import rebuild

    if entry.get("built_in") is not None:
        store = rebuild(entry["built_in"], project.home)
    elif entry["store"] in project.stores:
        store = project.stores[entry["store"]]
    else:
        gone = StoreError(
            f"store {entry['store']!r} is the project's own and no longer declared: restore it, "
            f"or give it up (solera cleanups {entry['output']} --clear)"
        )
        return _failed(gone, False)
    decl = entry.get("decl") or {}
    output = Output(
        entry["output"], key=decl.get("key"), incremental=decl.get("incremental"), **decl.get("config", {})
    )
    await claim([store], state)
    await writes.call(store.cleanup(output, home=entry["home"], before=entry["before"]))
    return {"status": "succeeded", "outputs": {}, "cleaned": entry["id"]}


async def claim(stores, state: str) -> None:
    """Claim each store's root, where a store has one to claim, for the
    namespace whose state is at `state` — `{location}/{namespace}` (F43): a
    root another namespace owns is refused, with a `StoreError`."""

    at, _, namespace = state.rstrip("/").rpartition("/")
    for store in stores:
        if callable(getattr(store, "claim", None)):
            await store.claim(namespace, at)


def _stores_of(project, asset: str) -> list:
    """The stores an asset's outputs are on."""

    names = {o["store"] for o in project.manifest["assets"][asset]["outputs"]}
    return [project.stores[n] for n in sorted(names)]


async def _cleanup_due(spec, project, asset, objects, writes) -> dict:
    """Clean up what the engine handed this cleanup task (docs/lifecycle.md
    §9.8) — all past every reader pin — as identity patterns for
    `store.cleanup` (docs/stores.md § Cleanup), through the store and
    declaration of the output's asset; the engine runs no store code:

    - a step of the partition's cleanup cursor (`deltas`): the deltas of its
      commits, in order, each naming the generations its updates and removes
      replaced — deleted, and the cursor moves past them (`cleaned_to`). A
      crash before that replays the step: deletes are idempotent;
    - entries: what attempts that never committed wrote, a value's previous
      object, an append output's earlier commits (`cleaned_up`).

    Returns what was done, for the result."""

    declared = {o["name"]: o for o in project.manifest["assets"][asset.name]["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    cleaned_up, cleaned_to = {}, {}

    for name, info in (spec.get("outputs") or {}).items():
        store = project.stores[declared[name]["store"]]
        if store.writes != "immutable" or not (info.get("cleanup") or info.get("deltas")):
            continue
        clean = functools.partial(
            store.cleanup, decls[name], home=info.get("home"), partition=spec["partition"]
        )

        async def delete(patterns, clean=clean):
            for i in range(0, len(patterns), CLEANUPS_AT_ONCE):
                await writes.call(asyncio.gather(*(clean(**p) for p in patterns[i : i + CLEANUPS_AT_ONCE])))

        step = info.get("deltas")
        for d in (step or {}).get("deltas") or ():  # in commit order
            for f in d["files"]:
                if not f.endswith(".lay"):
                    continue
                data = await _get(objects, f"{d['prefix']}{f}")
                if data is None:  # kept until the cursor passes it: lost, not owed
                    raise RuntimeError(f"cleanup of {name}: delta file {d['prefix']}{f} is missing")
                replaced = replaced_entries(data, int(d["generation"]))
                await delete([{"key": key_str(k), "generation": g} for k, g in replaced])
        if step:
            cleaned_to[name] = int(step["to"])
        patterns, done = [], []
        for entry in info.get("cleanup") or ():
            kind = entry["kind"]
            if kind in ("abandoned", "version"):  # all an uncommitted attempt wrote; a value's object
                patterns.append({"generation": entry["generation"]})  # an abandoned delta: the orphan rule
            elif kind == "commits":  # an append output started over: its earlier commits
                patterns += [{"key": n} for n in range(int(entry["from"]), int(entry["to"]) + 1)]
            done.append(entry["id"])
        await delete(patterns)
        if done:
            cleaned_up[name] = done
    out = {"cleaned_up": cleaned_up} if cleaned_up else {}
    if cleaned_to:
        out["cleaned_to"] = cleaned_to
    return out


async def run_pool(pool: str, server: str, token: str | None = None, *, project: str | None = None):
    """Pull path (docs/lifecycle.md §10): ask the engine which attempts wait
    on this pool, own one by swapping its control file, run it, repeat. The
    control file decides between workers; discovery is only a hint. `project` is
    the project's name, by default its manifest's: a pool token reaches the
    pool's routes and nothing else.

    Each attempt runs in a child forked from a forkserver — a process that
    has imported the framework and nothing else, and started no thread —
    and imports the project itself: no project code's threads or locks are
    ever forked half-held. The child is the attempt's own process: a forced
    cancel ends it, and with it any thread the attempt left running. (This
    process has threads — an HTTP client's name lookups — and fork copies
    only the forking one: unsafe on macOS.)"""

    import httpx

    from .channel import HttpTransport, PoolChannel

    capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
    host = f"{socket.gethostname()}:{os.getpid()}"
    project = project or load_project(os.environ["SOLERA_PROJECT"]).manifest["name"]
    context = _attempts()
    channel = PoolChannel(HttpTransport(server, token, timeout=60), project, pool)
    try:
        print(f"[pool] {host} polls pool {pool!r}", flush=True)
        while True:
            try:
                stages = await channel.work(host, {k: v for k, v in capacity.items() if v is not None})
            except httpx.HTTPError:
                # The server may be briefly unreachable (a restart): a pool worker polls on.
                await asyncio.sleep(1.0)
                continue
            for stage in stages:
                code = await _forked(stage, server, context)
                print(f"[pool] {stage['attempt']} exited {code}", flush=True)
                if code != LOST:  # it ran: ask again, what waits has changed
                    break
    finally:
        await channel.close()


LOST = 4  # the exit code of an attempt that could not publish: the engine treats it as dead


def _attempts():
    """Where pool attempts start: a forkserver with the framework imported,
    never the project — project code may start threads or hold locks a
    fork would copy without them."""

    import multiprocessing

    context = multiprocessing.get_context("forkserver")
    context.set_forkserver_preload(["solera_worker.worker"])
    return context


async def _forked(stage: dict, server: str | None, context=None) -> int:
    """Run one pool attempt in a process of its own, forked from the
    forkserver; its exit code."""

    child = (context or _attempts()).Process(
        target=_child, args=(stage, server), name=f"attempt {stage['attempt']}"
    )
    child.start()
    await asyncio.get_running_loop().run_in_executor(None, child.join)
    return child.exitcode


def _child(stage: dict, server: str | None) -> None:
    project = os.environ["SOLERA_PROJECT"]  # imported here, by the attempt: a failure goes in its result

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
        # solera_worker manifest PROJECT OUT: the manifest, and the files of the code it runs
        entrypoint, out = rest[0], rest[1]
        project = load_project(entrypoint)
        Path(out).write_text(json.dumps({**project.manifest, "watch": project.files()}, allow_nan=False))
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
        from .channel import HttpTransport, SensorChannel
        from .sensors import ORPHANED, run_sensor_host

        options = dict(zip(rest[::2], rest[1::2], strict=True))
        token = options.get("--token") or next(
            filter(None, map(os.getenv, ("SOLERA_SENSOR_TOKEN", "SOLERA_POOL_TOKEN", "SOLERA_API_TOKEN"))),
            None,
        )
        project = load_project(os.environ["SOLERA_PROJECT"])
        transport = HttpTransport(options["--server"], token, timeout=60)
        channel = SensorChannel(transport, project.manifest["name"])
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
