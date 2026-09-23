"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import dataclasses
import inspect
import json
import os
import tempfile
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from .sdk import BlobRef, JsonRef, Output, Ref, digest, is_ref_type


class StoreError(Exception):
    retryable = False


class StoreConflict(StoreError):
    """A fenced write refused: the live marker differs from `prior` (§3)."""


class StaleRead(StoreError):
    """A pinned read refused: live marker != ref.version (§3). Retryable."""

    retryable = True


class WriteError(StoreError):
    """Malformed write: duplicate keys, wrong shape, disallowed op."""


@dataclass(frozen=True)
class Patch:
    """Partial write: replace the named keys, delete `remove` (§4)."""

    rows: Any
    remove: Any = ()


@dataclass(frozen=True)
class Sql:
    """PostgresStore only: materialize a SELECT, or run a statement verbatim (§4)."""

    stmt: str


@dataclass(frozen=True)
class Keys:
    """A `key -> revision` selection passed to `store.load` (§4)."""

    revisions: Mapping[str, str]


@dataclass(frozen=True)
class Batches:
    """An inclusive `[lo, hi]` batch-range selection passed to `store.load`
    on an unkeyed incremental output (§2.2)."""

    lo: int
    hi: int


@dataclass(frozen=True)
class Scope:
    """A write scope: `batch` is the engine-assigned delta batch for
    incremental outputs, `baseline` the committed head the delta diffs
    against (present even when `prior` is withheld under a full run)."""

    output: Output
    partition: str
    batch: int | None = None
    baseline: Ref | None = None


@dataclass(frozen=True)
class Delta:
    """The output-diff one incremental commit produced (§2.1). `upserted` is a
    `key -> revision` map for keyed outputs, `None` when the store reports only
    a row count. `reset` marks a batch whose write superseded all prior
    content (a full run or a keyed replacement); its `deleted` lists every
    key the write dropped, so folds apply deltas forward without clearing."""

    batch: int
    rows: int = 0
    upserted: Mapping[str, str] | None = None
    deleted: tuple = ()
    reset: bool = False

    def to_json(self) -> dict:
        payload = {"batch": self.batch, "rows": self.rows}
        if self.upserted is not None:
            payload["upserted"] = dict(self.upserted)
        if self.deleted:
            payload["deleted"] = list(self.deleted)
        if self.reset:
            payload["reset"] = True
        return payload


@dataclass(frozen=True)
class Written:
    ref: Ref
    delta: Delta | None = None


def delta_path(output: str, partition: str, batch: int) -> str:
    """The object path one commit's delta lands at (§2.1)."""

    return f"deltas/{output}/{quote(partition or '_', safe='')}/{batch:012d}.json"


def next_batch(ref) -> int:
    """The delta batch a write after `ref` lands at. Accepts a Ref or its
    JSON dict form (heads live as dicts in state)."""

    if ref is None:
        return 0
    meta = (ref.meta if isinstance(ref, Ref) else ref.get("meta")) or {}
    delta = meta.get("delta")
    if delta is not None:
        return int(delta["batch"]) + 1
    handle = (ref.handle if isinstance(ref, Ref) else ref.get("handle")) or {}
    if "batches" in handle:
        return int(handle["batches"][1]) + 1
    return 0


@runtime_checkable
class Store(Protocol):
    version: str = "1"
    ref_type: type[Ref] = Ref

    def can_load(self, t: type | None, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Batches | None) -> Any: ...


def resolve_env(value: Any) -> Any:
    """`env:NAME` indirection for store/resource config, resolved in the harness.
    Dicts and lists are walked; anything else passes through."""

    if isinstance(value, str) and value.startswith("env:"):
        name = value[4:]
        if name not in os.environ:
            raise StoreError(f"Environment variable {name} is not set")
        return os.environ[name]
    if isinstance(value, dict):
        return {k: resolve_env(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(resolve_env(v) for v in value)
    return value


def _is_dataframe_type(t: Any) -> bool:
    return (
        inspect.isclass(t)
        and t.__name__ in ("DataFrame", "GeoDataFrame")
        and t.__module__.split(".")[0] in ("pandas", "geopandas")
    )


def _is_list_of_dicts(t: Any) -> bool:
    return typing.get_origin(t) is list and typing.get_args(t) == (dict,)


def _rows(value: Any, output_name: str) -> list[dict]:
    """Coerce a write payload into a row list."""

    if _is_dataframe(value):
        return value.to_dict(orient="records")
    if isinstance(value, list) and all(isinstance(r, dict) for r in value):
        return [dict(r) for r in value]
    if value is None:
        return []
    raise WriteError(f"{output_name}: expected rows (list[dict] or DataFrame), got {type(value).__name__}")


def _is_dataframe(value: Any) -> bool:
    return type(value).__name__ in ("DataFrame", "GeoDataFrame") and type(value).__module__.split(".")[0] in (
        "pandas",
        "geopandas",
    )


def key_map(output: Output, rows: list[dict]) -> dict[str, str]:
    """The scope's complete `key -> revision` map for a row payload (§4)."""

    if output.is_partition_set:
        return {str(k): "1" for k in rows}
    result = {}
    for row in rows:
        if output.key not in row:
            raise WriteError(f"{output.name}: row lacks the declared key column {output.key!r}")
        key = str(row[output.key])
        if key in result:
            raise WriteError(f"{output.name}: duplicate key {key!r} in write")
        if output.revision:
            if output.revision not in row:
                raise WriteError(f"{output.name}: row lacks the declared revision column {output.revision!r}")
            result[key] = str(row[output.revision])
        else:
            result[key] = digest(row)
    return result


class JsonStore:
    """Default store (§4): one object per batch for incremental outputs, one
    content-addressed object per version for values and partition sets.

    Object layout under the attempt's `objects` namespace:

        data/{output}/{version}.json                value + partition-set payloads
        data/{output}/{esc(scope)}/b{batch}.json    one batch: {"rows": …} for an
                                                    unkeyed incremental output,
                                                    {"upsert"/"reset", "remove": …}
                                                    for a keyed one
        data/{output}/{esc(scope)}/s{batch}.json    keyed snapshot {"rows": …},
                                                    written every `snapshot_every`
                                                    batches so loads fold a tail
        deltas/{output}/{esc(scope)}/{batch}.json   the published delta a commit
                                                    produced (§2.1)
    """

    version = "1"
    ref_type = JsonRef
    shared_table = False

    def __init__(self, snapshot_every: int = 64):
        self._objects = None
        self.snapshot_every = max(1, int(snapshot_every))

    def bind_objects(self, objects) -> None:
        """The harness hands the attempt's object store to stores without a URL."""

        self._objects = objects

    def can_load(self, t, selection) -> bool:
        if t is None:
            return selection is None
        if is_ref_type(t):
            return issubclass(self.ref_type, t)
        if _is_dataframe_type(t) or _is_list_of_dicts(t) or t in (list, dict):
            return True
        if t in (str, int, float, bool, type(None)):
            return selection is None
        return False

    def can_store(self, t, output) -> bool:
        if output.is_partition_set:
            return t is None or t is list or _is_list_of(t, str) or _is_list_of_dicts(t)
        if output.incremental:
            return t is None or _is_list_of_dicts(t) or _is_dataframe_type(t) or t is list
        return True  # unkeyed value: any JSON value

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        objects = self._require_objects()
        output = scope.output
        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")

        if isinstance(write, Patch):
            if not output.incremental:
                raise WriteError(f"{output.name}: Patch requires an incremental output")
            remove = {str(k) for k in write.remove}
            if output.is_partition_set:
                return await self._store_set(
                    objects, output, scope, prior, write.rows or [], remove, patch=True
                )
            rows = _rows(write.rows, output.name)
            if output.key is None:
                return await self._store_batch(objects, output, scope, prior, rows, remove)
            return await self._store_keyed(objects, output, scope, prior, rows, remove, patch=True)
        if output.is_partition_set:
            return await self._store_set(objects, output, scope, prior, write or [], set(), patch=False)
        if output.key is not None:
            return await self._store_keyed(
                objects, output, scope, prior, _rows(write, output.name), set(), patch=False
            )
        if output.incremental:
            raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
        payload = write
        version = digest(_canonical(payload))
        path = f"data/{output.name}/{version}.json"
        await self._put(objects, path, payload)
        return Written(self._ref(output, scope, {"object": path, "mode": "value", "key": None}, version))

    async def _store_set(self, objects, output, scope, prior, rows, remove, *, patch) -> Written:
        """Partition-set write: one content-addressed payload per version, the
        element list on `meta.partitions`, the membership diff as the delta."""

        if patch:
            old = await self._legacy_rows(objects, prior)
            elements = [str(e) for e in rows]
            payload = [e for e in old if e not in remove and e not in elements] + elements
        else:
            payload = [str(e) for e in rows]
        version = digest(_canonical(payload))
        if prior is not None and version == prior.version:
            return Written(prior)
        baseline = scope.baseline or prior
        if baseline is not None:
            base_elements = (baseline.meta or {}).get("partitions")
            if base_elements is None:
                base_elements = await self._legacy_rows(objects, baseline)
        else:
            base_elements = []
        added = sorted(set(payload) - {str(e) for e in base_elements})
        removed = sorted({str(e) for e in base_elements} - set(payload))
        if not added and not removed:
            if baseline is None and prior is None:
                pass  # a first write still establishes a head
            else:
                return Written(baseline if baseline is not None else prior)
        path = f"data/{output.name}/{version}.json"
        await self._put(objects, path, payload)
        ref = self._ref(output, scope, {"object": path, "mode": "set", "key": output.key}, version)
        ref = dataclasses.replace(ref, meta={"partitions": sorted(payload)})
        batch = scope.batch if scope.batch is not None else next_batch(baseline)
        delta = Delta(
            batch=batch,
            rows=len(added) + len(removed),
            upserted={e: "1" for e in added},
            deleted=tuple(removed),
            reset=prior is None,
        )
        return Written(ref, delta)

    async def _store_batch(self, objects, output, scope, prior, rows, remove) -> Written:
        """Unkeyed incremental write: one `{"rows": …}` object per batch."""

        if remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        if not rows:
            if prior is None and scope.baseline is None:
                raise WriteError(f"{output.name}: empty Patch with no prior head")
            return Written(prior or scope.baseline)
        version = digest([prior.version if prior else "", digest({"rows": _canonical(rows)})])
        if prior is not None and version == prior.version:
            return Written(prior)
        first, last = await self._append_window(objects, output, scope, prior)
        batch = scope.batch if scope.batch is not None else max(last + 1, next_batch(scope.baseline or prior))
        payload = {"rows": rows}
        if prior is None:
            payload["reset"] = True
            first = batch
        await self._put(objects, self._batch_path(output, scope, batch), payload)
        handle = {
            "mode": "batches",
            "prefix": self._prefix(output, scope),
            "batches": [first, batch],
        }
        return Written(
            self._ref(output, scope, handle, version),
            Delta(batch=batch, rows=len(rows), reset=prior is None),
        )

    async def _store_keyed(self, objects, output, scope, prior, rows, remove, *, patch) -> Written:
        """Keyed incremental write: one `{"upsert"/"reset", "remove"}` object
        per batch; the delta diffs the resulting key map against `baseline`."""

        base_state = {}
        if scope.baseline is not None:
            base_state = await self._fold_ref(objects, scope.baseline, output.key)
        base_revs = key_map(output, list(base_state.values()))
        key_map(output, rows)  # validates the key column and duplicate keys
        if patch and not rows and not remove:
            if prior is None and scope.baseline is None:
                raise WriteError(f"{output.name}: empty Patch with no prior head")
            return Written(prior or scope.baseline)
        if patch and prior is not None:
            content = dict(base_state)
            for k in remove:
                content.pop(k, None)
            for row in rows:
                content[str(row[output.key])] = row
        else:
            # A replace write — or any write with no prior — supersedes the
            # scope's content wholesale.
            content = {str(r[output.key]): r for r in rows}
        new_revs = key_map(output, list(content.values()))
        upserted = {k: r for k, r in new_revs.items() if base_revs.get(k) != r}
        deleted = sorted(set(base_revs) - set(new_revs))
        if not upserted and not deleted:
            if prior is None and scope.baseline is None:
                # A first write still establishes a head (possibly empty).
                upserted, deleted = {}, []
            else:
                return Written(prior or scope.baseline)
        if patch:
            version = digest(
                [
                    prior.version if prior else "",
                    digest({"rows": _canonical(rows), "remove": sorted(remove)}),
                ]
            )
        else:
            version = digest(_canonical(rows))
        if prior is not None and version == prior.version:
            return Written(prior)
        handle = (prior.handle or {}) if prior is not None else {}
        batch = scope.batch if scope.batch is not None else next_batch(scope.baseline or prior)
        prefix = self._prefix(output, scope)
        if "batches" in handle:
            payload = (
                {"upsert": rows, "remove": sorted(remove)}
                if patch
                else {"reset": True, "upsert": list(content.values()), "remove": []}
            )
            await self._put(objects, self._batch_path(output, scope, batch), payload)
            first = int(handle["batches"][0])
            snap = await self._maybe_snapshot(
                objects, prefix, output.key, first, batch, handle.get("snapshot", -1)
            )
        else:
            # No prior window (first write, full run, or a pre-batch-layout
            # head): one reset batch carries the whole content.
            await self._put(
                objects,
                self._batch_path(output, scope, batch),
                {"reset": True, "upsert": list(content.values()), "remove": []},
            )
            first = batch
            snap = -1
        handle = {
            "mode": "keyed",
            "key": output.key,
            "prefix": prefix,
            "batches": [first, batch],
            "snapshot": snap,
        }
        delta = Delta(
            batch=batch,
            rows=len(upserted) + len(deleted),
            upserted=upserted,
            deleted=tuple(deleted),
            reset=prior is None or not patch,
        )
        return Written(self._ref(output, scope, handle, version), delta)

    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any:
        if is_ref_type(t):
            return ref
        objects = self._require_objects()
        handle = ref.handle or {}
        mode = handle.get("mode")
        if mode == "batches" and "batches" in handle:
            if isinstance(selection, Batches):
                first, last = selection.lo, selection.hi
            elif selection is None:
                first, last = handle["batches"]
            else:
                raise StoreError(f"{ref.output}: an unkeyed incremental output takes Batches")
            rows = []
            for b in range(int(first), int(last) + 1):
                batch = await self._get(objects, f"{handle['prefix']}b{b:012d}.json")
                if batch is None:
                    continue
                if batch.get("reset"):
                    rows = []
                rows += batch.get("rows", [])
            return self._materialize(rows, t)
        if mode == "keyed" and "batches" in handle:
            if isinstance(selection, Batches):
                raise StoreError(f"{ref.output}: a keyed output takes Keys, not Batches")
            first, last = handle["batches"]
            state = await self._fold(
                objects, handle["prefix"], handle.get("key"), int(first), int(last), handle.get("snapshot")
            )
            rows = list(state.values())
            if selection is not None:
                rows = [r for r in rows if str(r.get(handle.get("key"))) in selection.revisions]
            return self._materialize(rows, t)
        # Single-object payloads: values, partition sets, and handles written
        # before the per-batch layout.
        payload = await self._read(objects, ref)
        if mode == "set":
            elements = payload or []
            if selection is not None:
                elements = [e for e in elements if str(e) in selection.revisions]
            return self._materialize(elements, t)
        if mode == "append":
            batches = (payload or {}).get("batches", {})
            rows = []
            for batch, batch_rows in sorted(batches.items(), key=lambda kv: int(kv[0])):
                if selection is None or str(batch) in getattr(selection, "revisions", {}):
                    rows += batch_rows
            return self._materialize(rows, t)
        if mode == "keyed":
            rows = payload or []
            if selection is not None:
                rows = self._filter_rows(rows, ref, selection)
            return self._materialize(rows, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: unkeyed output cannot serve a selection")
        return self._materialize(payload, t)

    # -- batch layout ---------------------------------------------------------

    @staticmethod
    def _prefix(output, scope) -> str:
        # `_` is the unscoped directory — object paths cannot hold an empty segment.
        return f"data/{output.name}/{quote(scope.partition or '_', safe='')}/"

    def _batch_path(self, output, scope, batch: int) -> str:
        return f"{self._prefix(output, scope)}b{batch:012d}.json"

    def _snap_path(self, prefix: str, batch: int) -> str:
        return f"{prefix}s{batch:012d}.json"

    async def _append_window(self, objects, output, scope, prior) -> tuple[int, int]:
        """(first, last) batch covered by `prior`; a legacy single-object head
        is migrated into per-batch objects once."""

        if prior is None:
            return 0, -1
        handle = prior.handle or {}
        if "batches" in handle:
            return int(handle["batches"][0]), int(handle["batches"][1])
        payload = await self._read(objects, prior)
        batches = (payload or {}).get("batches", {})
        for b, rows in sorted(batches.items(), key=lambda kv: int(kv[0])):
            await self._put(objects, self._batch_path(output, scope, int(b)), {"rows": rows})
        if not batches:
            return 0, -1
        return min(int(b) for b in batches), max(int(b) for b in batches)

    async def _fold_ref(self, objects, ref: Ref, key_col) -> dict:
        """The live `{key: row}` state a keyed ref points at — batch layout or
        a pre-batch-layout payload."""

        handle = ref.handle or {}
        if "batches" in handle:
            first, last = handle["batches"]
            return await self._fold(
                objects, handle["prefix"], key_col, int(first), int(last), handle.get("snapshot")
            )
        payload = await self._read(objects, ref)
        rows = payload if isinstance(payload, list) else []
        return {str(r[key_col]): r for r in rows}

    async def _legacy_rows(self, objects, prior) -> list:
        if prior is None:
            return []
        payload = await self._read(objects, prior)
        return payload if isinstance(payload, list) else []

    async def _maybe_snapshot(self, objects, prefix, key_col, first, last, snap) -> int:
        """Fold the batch window into `s{last}.json` when it is due; returns the
        snapshot batch number to record in the ref handle."""

        base = snap if isinstance(snap, int) and first <= snap else first - 1
        if last - base < self.snapshot_every:
            return snap if isinstance(snap, int) and first <= snap else -1
        state = await self._fold(objects, prefix, key_col, first, last, snap)
        await self._put(objects, self._snap_path(prefix, last), {"rows": list(state.values())})
        return last

    async def _fold(self, objects, prefix, key_col, first, last, snap) -> dict:
        """Replays batches [first..last] over the snapshot at `snap` (if any);
        last writer wins per key, removes apply, `reset` batches start over.
        Missing objects are skipped — retention may have pruned them."""

        state: dict[str, dict] = {}
        start = first
        if isinstance(snap, int) and first <= snap <= last:
            base = await self._get(objects, self._snap_path(prefix, snap))
            if base is not None:
                for row in base.get("rows", []):
                    state[str(row[key_col])] = row
                start = snap + 1
        for b in range(start, last + 1):
            batch = await self._get(objects, f"{prefix}b{b:012d}.json")
            if batch is None:
                continue
            if batch.get("reset"):
                state.clear()
            for row in batch.get("upsert", []):
                state[str(row[key_col])] = row
            for key in batch.get("remove", []):
                state.pop(str(key), None)
        return state

    def _filter_rows(self, rows, ref, selection):
        key_col = (ref.handle or {}).get("key")
        if key_col is None:
            return rows
        return [r for r in rows if str(r.get(key_col)) in selection.revisions]

    def _materialize(self, payload, t):
        if t is None or t is inspect.Parameter.empty:
            return payload
        if _is_dataframe_type(t):
            import pandas as pd

            return pd.DataFrame(payload if isinstance(payload, list) else [payload])
        if t in (list, dict, str, int, float, bool) or _is_list_of_dicts(t):
            return payload
        return payload

    def _ref(self, output, scope, handle, version) -> JsonRef:
        return JsonRef(
            output=output.name,
            store="",
            handle=handle,
            version=version,
            partition=scope.partition,
        )

    async def _put(self, objects, path: str, payload):
        import obstore

        body = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
        await obstore.put_async(objects, path, body, mode="overwrite", use_multipart=False)

    async def _get(self, objects, path: str):
        import obstore
        from obstore.exceptions import NotFoundError

        try:
            result = await obstore.get_async(objects, path)
        except (NotFoundError, FileNotFoundError):
            return None
        return json.loads(bytes(await result.bytes_async()))

    async def _read(self, objects, ref: Ref):
        import obstore

        result = await obstore.get_async(objects, ref.handle["object"])
        return json.loads(bytes(await result.bytes_async()))

    def _require_objects(self):
        if self._objects is None:
            raise StoreError("JsonStore is not bound to an object store")
        return self._objects


def _is_list_of(t: Any, inner: type) -> bool:
    return typing.get_origin(t) is list and typing.get_args(t) == (inner,)


def _canonical(value: Any) -> Any:
    if _is_dataframe(value):
        return value.to_dict(orient="records")
    return value


class BlobStore:
    """`bytes` / `Path` payloads, content-addressed (§4)."""

    version = "1"
    ref_type = BlobRef
    shared_table = False

    def __init__(self, url: str | None = None):
        self.url = url
        self._objects = None
        self._migrate_lock = None

    def bind_objects(self, objects) -> None:
        if self.url is None:
            self._objects = objects

    def _resolve(self):
        if self._objects is not None:
            return self._objects
        if self.url:
            import obstore

            self._objects = obstore.store.from_url(resolve_env(self.url))
            return self._objects
        raise StoreError("BlobStore is not bound to an object store")

    def can_load(self, t, selection) -> bool:
        if selection is not None:
            return False
        return t is None or t in (bytes, Path) or (is_ref_type(t) and issubclass(self.ref_type, t))

    def can_store(self, t, output) -> bool:
        if output.incremental or output.is_partition_set:
            return False
        return t is None or t in (bytes, Path, Callable)

    async def migrate(self, output: Output, migrations) -> list[str]:
        """Apply pending callable migrations over the output's blob prefix,
        recording each in `_migrations.json` next to the data (§4)."""

        import asyncio

        import obstore
        from obstore.exceptions import NotFoundError

        objects = self._resolve()
        prefix = f"blobs/{output.name}/"
        ledger_path = prefix + "_migrations.json"
        if self._migrate_lock is None:
            self._migrate_lock = asyncio.Lock()
        async with self._migrate_lock:
            try:
                result = await obstore.get_async(objects, ledger_path)
                ledger = json.loads(bytes(await result.bytes_async()))
            except (NotFoundError, FileNotFoundError):
                ledger = {"applied": []}
            applied = {entry["name"] for entry in ledger["applied"]}
            for migration in migrations:
                if migration.name in applied:
                    continue
                if not callable(migration.payload):
                    raise StoreError(
                        f"{output.name}: BlobStore migration {migration.name!r} requires a callable payload"
                    )
                value = migration.payload(objects, prefix)
                if inspect.isawaitable(value):
                    await value
                ledger["applied"].append({"name": migration.name, "at": self._now()})
                applied.add(migration.name)
                body = json.dumps(ledger, sort_keys=True, allow_nan=False).encode()
                await obstore.put_async(objects, ledger_path, body, mode="overwrite", use_multipart=False)
            return [m.name for m in migrations if m.name in applied]

    @staticmethod
    def _now() -> str:
        import datetime as dt

        return dt.datetime.now(dt.UTC).isoformat()

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        import obstore

        objects = self._resolve()
        if isinstance(write, Path):
            data = write.read_bytes()
        elif isinstance(write, (bytes, bytearray)):
            data = bytes(write)
        else:
            raise WriteError(
                f"{scope.output.name}: BlobStore accepts bytes or Path, not {type(write).__name__}"
            )
        version = digest({"blob": True, "sha": __import__("hashlib").sha256(data).hexdigest()})
        path = f"blobs/{scope.output.name}/{version}.bin"
        await obstore.put_async(objects, path, data, mode="overwrite", use_multipart=False)
        return Written(
            BlobRef(
                output=scope.output.name,
                store="",
                handle={"object": path, "bytes": len(data)},
                version=version,
                partition=scope.partition,
            ),
            None,
        )

    async def load(self, ref: Ref, t, selection: Keys | None) -> Any:
        import obstore

        if is_ref_type(t):
            return ref
        if selection is not None:
            raise StoreError("BlobStore cannot serve a Keys selection")
        result = await obstore.get_async(self._resolve(), ref.handle["object"])
        data = bytes(await result.bytes_async())
        if t is Path or t is None or t is bytes:
            if t is Path:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".blob")
                tmp.write(data)
                tmp.close()
                return Path(tmp.name)
            return data
        raise StoreError(f"BlobStore cannot load {t}")
