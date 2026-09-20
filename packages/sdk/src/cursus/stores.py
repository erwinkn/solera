"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import inspect
import json
import os
import tempfile
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

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
    """Default store: one content-addressed JSON object per write (§4)."""

    version = "1"
    ref_type = JsonRef
    shared_table = False

    def __init__(self):
        self._objects = None

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
        import obstore

        objects = self._require_objects()
        output = scope.output
        prior_payload = None
        prior_keys = dict(scope.prior_keys or {})
        if prior is not None and isinstance(write, Patch):
            prior_payload = await self._read(objects, prior)

        if isinstance(write, Patch):
            if output.key is None and not output.is_partition_set and output.mode != "append":
                raise WriteError(f"{output.name}: Patch requires a keyed or append output")
            remove = {str(k) for k in write.remove}
            rows = _rows(write.rows, output.name) if not output.is_partition_set else write.rows
            if output.mode == "append":
                if remove:
                    raise WriteError(f"{output.name}: remove is not allowed on an append output")
                batches = dict((prior_payload or {}).get("batches", {}))
                batch = str(max([int(b) for b in batches], default=-1) + 1)
                if not rows:
                    return Written(prior, prior_keys)
                batches[batch] = rows
                payload = {"batches": batches}
                keys = {**prior_keys, batch: digest(rows)}
            else:
                old_rows = prior_payload if isinstance(prior_payload, list) else []
                if output.is_partition_set:
                    elements = [str(e) for e in (write.rows or [])]
                    new_rows = [e for e in old_rows if str(e) not in remove and str(e) not in elements]
                    new_rows += elements
                    payload = new_rows
                    keys = {str(e): "1" for e in new_rows}
                else:
                    patch_keys = {str(r[output.key]) for r in rows}
                    if len(patch_keys) != len(rows):
                        raise WriteError(f"{output.name}: duplicate key in Patch")
                    new_rows = [r for r in old_rows if str(r[output.key]) not in patch_keys | remove]
                    new_rows += rows
                    payload = new_rows
                    keys = {k: v for k, v in prior_keys.items() if k not in remove}
                    keys.update(key_map(output, rows))
                    keys = {k: v for k, v in keys.items() if k not in remove}
            if not rows and not remove:
                if prior is None:
                    raise WriteError(f"{output.name}: empty Patch with no prior head")
                return Written(prior, prior_keys)
            version = digest(
                [prior.version if prior else "", digest({"rows": _canonical(rows), "remove": sorted(remove)})]
            )
        else:
            if output.is_partition_set:
                payload = [str(e) for e in (write or [])]
                keys = {str(e): "1" for e in payload}
            elif output.key is not None:
                payload = _rows(write, output.name)
                keys = key_map(output, payload)
            elif output.mode == "append":
                raise WriteError(f"{output.name}: an append output only accepts Patch writes")
            else:
                payload, keys = write, None
            version = digest(_canonical(payload))

        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")
        body = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
        path = f"data/{output.name}/{version}.json"
        await obstore.put_async(objects, path, body, mode="overwrite", use_multipart=False)
        ref = JsonRef(
            output=output.name,
            store="",
            handle={
                "object": path,
                "mode": output.mode
                or ("set" if output.is_partition_set else "keyed" if output.key else "value"),
                "key": output.key,
            },
            version=version,
            partition=scope.partition,
        )
        return Written(
            ref, keys if (output.key or output.is_partition_set or output.mode == "append") else None
        )

    async def load(self, ref: Ref, t, selection: Keys | None) -> Any:
        if is_ref_type(t):
            return ref
        objects = self._require_objects()
        payload = await self._read(objects, ref)
        mode = (ref.handle or {}).get("mode")
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
        return t is None or t in (bytes, Path)

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
