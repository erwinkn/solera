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
class Scope:
    output: Output
    partition: str
    prior_keys: Mapping[str, str] | None


@dataclass(frozen=True)
class Written:
    ref: Ref
    keys: Mapping[str, str] | None = None


@runtime_checkable
class Store(Protocol):
    version: str = "1"
    ref_type: type[Ref] = Ref

    def can_load(self, t: type | None, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | None) -> Any: ...


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
    """Default store (§4): one object per batch for keyed/append outputs, one
    content-addressed object per version for values and partition sets.

    Object layout under the attempt's `objects` namespace:

        data/{output}/{version}.json                value + partition-set payloads
        data/{output}/{esc(scope)}/b{batch}.json    one batch: {"rows": …} for an
                                                    append output, {"upsert": …,
                                                    "remove": …} for a keyed one
        data/{output}/{esc(scope)}/s{batch}.json    keyed snapshot {"rows": …},
                                                    written every `snapshot_every`
                                                    batches so loads fold a tail
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
        if output.key is not None or output.mode == "append":
            return t is None or _is_list_of_dicts(t) or _is_dataframe_type(t) or t is list
        return True  # unkeyed: any JSON value

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        objects = self._require_objects()
        output = scope.output
        prior_keys = dict(scope.prior_keys or {})
        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")

        if isinstance(write, Patch):
            if output.key is None and not output.is_partition_set and output.mode != "append":
                raise WriteError(f"{output.name}: Patch requires a keyed or append output")
            remove = {str(k) for k in write.remove}
            rows = write.rows if output.is_partition_set else _rows(write.rows, output.name)
            if output.mode == "append" and remove:
                raise WriteError(f"{output.name}: remove is not allowed on an append output")
            if not rows and not remove:
                if prior is None:
                    raise WriteError(f"{output.name}: empty Patch with no prior head")
                return Written(prior, prior_keys)
            version = digest(
                [prior.version if prior else "", digest({"rows": _canonical(rows), "remove": sorted(remove)})]
            )
            if prior is not None and version == prior.version:
                return Written(prior, prior_keys)
            if output.mode == "append":
                first, last = await self._append_window(objects, output, scope, prior)
                batch = last + 1
                await self._put(objects, self._batch_path(output, scope, batch), {"rows": rows})
                keys = {**prior_keys, str(batch): digest(_canonical(rows))}
                handle = {"mode": "append", "prefix": self._prefix(output, scope), "batches": [first, batch]}
                return Written(self._ref(output, scope, handle, version), keys)
            if output.is_partition_set:
                old = await self._legacy_rows(objects, prior)
                elements = [str(e) for e in (rows or [])]
                payload = [e for e in old if str(e) not in remove and str(e) not in elements] + elements
                keys = {str(e): "1" for e in payload}
                path = f"data/{output.name}/{version}.json"
                await self._put(objects, path, payload)
                return Written(
                    self._ref(output, scope, {"object": path, "mode": "set", "key": output.key}, version),
                    keys,
                )
            patch_keys = {str(r[output.key]) for r in rows}
            if len(patch_keys) != len(rows):
                raise WriteError(f"{output.name}: duplicate key in Patch")
            first, last, snap, legacy = await self._keyed_window(objects, output, scope, prior)
            batch = last + 1
            prefix = self._prefix(output, scope)
            if legacy is not None:
                # A pre-batch-layout head: fold the old payload into one reset
                # batch and continue per-batch from here.
                merged = [r for r in legacy if str(r.get(output.key)) not in patch_keys | remove] + rows
                await self._put(
                    objects,
                    self._batch_path(output, scope, batch),
                    {"reset": True, "upsert": merged, "remove": []},
                )
                first = batch
            else:
                await self._put(
                    objects,
                    self._batch_path(output, scope, batch),
                    {"upsert": rows, "remove": sorted(remove)},
                )
            snap = await self._maybe_snapshot(objects, prefix, output.key, first, batch, snap)
            keys = {k: v for k, v in prior_keys.items() if k not in remove}
            keys.update(key_map(output, rows))
            keys = {k: v for k, v in keys.items() if k not in remove}
            handle = {
                "mode": "keyed",
                "key": output.key,
                "prefix": prefix,
                "batches": [first, batch],
                "snapshot": snap,
            }
            return Written(self._ref(output, scope, handle, version), keys)

        if output.is_partition_set:
            payload = [str(e) for e in (write or [])]
            keys = {str(e): "1" for e in payload}
            version = digest(_canonical(payload))
            if prior is not None and version == prior.version:
                return Written(prior, prior_keys)
            path = f"data/{output.name}/{version}.json"
            await self._put(objects, path, payload)
            return Written(
                self._ref(output, scope, {"object": path, "mode": "set", "key": output.key}, version), keys
            )
        if output.key is not None:
            rows = _rows(write, output.name)
            version = digest(_canonical(rows))
            if prior is not None and version == prior.version:
                return Written(prior, prior_keys)
            _, last, _, _ = await self._keyed_window(objects, output, scope, prior)
            batch = last + 1
            await self._put(
                objects,
                self._batch_path(output, scope, batch),
                {"reset": True, "upsert": rows, "remove": []},
            )
            handle = {
                "mode": "keyed",
                "key": output.key,
                "prefix": self._prefix(output, scope),
                "batches": [batch, batch],
                "snapshot": -1,
            }
            return Written(self._ref(output, scope, handle, version), key_map(output, rows))
        if output.mode == "append":
            raise WriteError(f"{output.name}: an append output only accepts Patch writes")
        payload, keys = write, None
        version = digest(_canonical(payload))
        path = f"data/{output.name}/{version}.json"
        await self._put(objects, path, payload)
        return Written(
            self._ref(output, scope, {"object": path, "mode": "value", "key": None}, version), keys
        )

    async def load(self, ref: Ref, t, selection: Keys | None) -> Any:
        if is_ref_type(t):
            return ref
        objects = self._require_objects()
        handle = ref.handle or {}
        mode = handle.get("mode")
        if mode == "append" and "batches" in handle:
            first, last = handle["batches"]
            rows = []
            for b in range(int(first), int(last) + 1):
                if selection is not None and str(b) not in selection.revisions:
                    continue
                batch = await self._get(objects, f"{handle['prefix']}b{b:012d}.json")
                rows += (batch or {}).get("rows", [])
            return self._materialize(rows, t)
        if mode == "keyed" and "batches" in handle:
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
                if selection is None or str(batch) in selection.revisions:
                    rows += batch_rows
            return self._materialize(rows, t)
        if mode == "keyed":
            rows = payload or []
            if selection is not None:
                rows = self._filter_rows(rows, ref, selection)
            return self._materialize(rows, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: unkeyed output cannot serve a Keys selection")
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

    async def _keyed_window(self, objects, output, scope, prior):
        """(first, last, snapshot, legacy_rows|None): the prior ref's batch
        window, plus its row list when it predates the batch layout."""

        if prior is None:
            return 0, -1, -1, None
        handle = prior.handle or {}
        if "batches" in handle:
            return (
                int(handle["batches"][0]),
                int(handle["batches"][1]),
                handle.get("snapshot", -1),
                None,
            )
        payload = await self._read(objects, prior)
        return 0, -1, -1, payload if isinstance(payload, list) else []

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
        if output.key or output.mode or output.is_partition_set:
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
