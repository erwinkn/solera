"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

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
    """A write scope (§9): `batch` is the engine-assigned batch number for
    incremental outputs, `attempt` the writing attempt's id (unique, so object
    names built from it never collide), `aliases` the output's former names."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()


@dataclass(frozen=True)
class Written:
    """What a store wrote. `keys` is only for writes the harness never sees as
    rows (`Sql` materialized inside Postgres): the scope's complete new
    `key -> version` map. For every other write the harness derives keys from
    the rows itself (§6, §9)."""

    ref: Ref
    keys: Mapping[str, str] | None = None


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

    The store never works out what changed: the harness does, against the
    output's key index, and skips writes that change nothing (§6).
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
        """Partition-set write: one content-addressed payload per version."""

        elements = [str(e) for e in rows]
        if patch and prior is not None:
            old = await self._payload(objects, prior)
            payload = [e for e in old if e not in remove and e not in elements] + elements
        else:
            payload = elements
        version = digest(_canonical(payload))
        if prior is not None and version == prior.version:
            return Written(prior)
        path = f"data/{output.name}/{version}.json"
        await self._put(objects, path, payload)
        return Written(self._ref(output, scope, {"object": path, "mode": "set", "key": output.key}, version))

    async def _store_batch(self, objects, output, scope, prior, rows, remove) -> Written:
        """Unkeyed incremental write: one `{"rows": …}` object per batch. With
        no prior the batch starts the output over (a first write or a full run)."""

        if remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        if not rows and prior is not None:
            return Written(prior)
        batch = self._batch(scope, prior)
        payload = {"rows": rows}
        prefix = self._prefix(output, scope, prior)
        first = int(prior.handle["batches"][0]) if prior is not None else batch
        if prior is None:
            payload["reset"] = True
        await self._put(objects, f"{prefix}b{batch:012d}.json", payload)
        version = digest([prior.version if prior else "", digest({"rows": _canonical(rows)})])
        handle = {"mode": "batches", "prefix": prefix, "batches": [first, batch]}
        return Written(self._ref(output, scope, handle, version))

    async def _store_keyed(self, objects, output, scope, prior, rows, remove, *, patch) -> Written:
        """Keyed incremental write: one `{"upsert"/"reset", "remove"}` object
        per batch. A replacement, or any write with no prior, starts the batch
        window over; a Patch extends it."""

        key_map(output, rows)  # validates the key column and duplicate keys
        batch = self._batch(scope, prior)
        prefix = self._prefix(output, scope, prior)
        extend = patch and prior is not None and "batches" in (prior.handle or {})
        if extend:
            payload = {"upsert": rows, "remove": sorted(remove)}
            first = int(prior.handle["batches"][0])
            version = digest([prior.version, digest({"rows": _canonical(rows), "remove": sorted(remove)})])
        else:
            payload = {"reset": True, "upsert": rows, "remove": []}
            first = batch
            version = digest(_canonical(rows))
        await self._put(objects, f"{prefix}b{batch:012d}.json", payload)
        snap = (prior.handle or {}).get("snapshot", -1) if extend else -1
        snap = await self._maybe_snapshot(objects, prefix, output.key, first, batch, snap)
        handle = {
            "mode": "keyed",
            "key": output.key,
            "prefix": prefix,
            "batches": [first, batch],
            "snapshot": snap,
        }
        return Written(self._ref(output, scope, handle, version))

    @staticmethod
    def _batch(scope, prior) -> int:
        if scope.batch is not None:
            return scope.batch
        batches = (prior.handle or {}).get("batches") if prior is not None else None
        return int(batches[1]) + 1 if batches else 0

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
        # Single-object payloads: values and partition sets.
        payload = await self._read(objects, ref)
        if mode == "set":
            elements = payload or []
            if selection is not None:
                elements = [e for e in elements if str(e) in selection.revisions]
            return self._materialize(elements, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: unkeyed output cannot serve a selection")
        return self._materialize(payload, t)

    # -- batch layout ---------------------------------------------------------

    @staticmethod
    def _prefix(output, scope, prior=None) -> str:
        """The scope's batch directory: the prior window's, so a renamed output
        keeps extending its batches where they are (§2)."""

        if prior is not None and "prefix" in (prior.handle or {}):
            return prior.handle["prefix"]
        # `_` is the unscoped directory — object paths cannot hold an empty segment.
        return f"data/{output.name}/{quote(scope.partition or '_', safe='')}/"

    def _snap_path(self, prefix: str, batch: int) -> str:
        return f"{prefix}s{batch:012d}.json"

    async def _payload(self, objects, ref) -> list:
        payload = await self._read(objects, ref)
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
