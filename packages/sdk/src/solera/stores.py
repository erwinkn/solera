"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import pickle
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote, unquote

from .sdk import KEYS, ObjectRef, Output, Ref, is_ref_type


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
    incremental outputs, `attempt` the writing attempt's id, `aliases` the
    output's former names.

    For a keyed output the harness says which keys the write changes against
    the key index: `upserts` are the keys to write, `removes` the keys to
    delete. A store may write only those; `None` means unknown — everything
    in the write, and for a replacement, every key not in it goes."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()
    upserts: frozenset[str] | None = None
    removes: frozenset[str] | None = None


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


def encode(value: Any) -> tuple[bytes, str]:
    """A value's bytes and format: JSON (compact, sorted keys) when it
    round-trips exactly, pickle otherwise — a DataFrame, a tuple, a dict
    with int keys, a NaN."""

    try:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if json.loads(text) == value:
            return text.encode(), "json"
    except (TypeError, ValueError):
        pass
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), "pkl"


def revision(value: Any) -> str:
    """A content revision: the hash of the value's encoding. For JSON it is
    `digest(value)`."""

    return hashlib.sha256(encode(value)[0]).hexdigest()


def entries(output: Output, value: Any) -> dict[str, Any]:
    """A keyed write's content as `key -> entry` (§4): a `keyed=True`
    output's dict, a partition set's elements, or rows by their key column."""

    if output.is_partition_set:
        return {str(e): str(e) for e in value or ()}
    if output.key == KEYS:
        if value is None:
            return {}
        if not isinstance(value, Mapping) or not all(isinstance(k, str) for k in value):
            raise WriteError(
                f"{output.name}: a keyed output takes dict[str, Any], got {type(value).__name__}"
            )
        return dict(value)
    result = {}
    for row in _rows(value, output.name):
        if output.key not in row:
            raise WriteError(f"{output.name}: row lacks the declared key column {output.key!r}")
        key = str(row[output.key])
        if key in result:
            raise WriteError(f"{output.name}: duplicate key {key!r} in write")
        result[key] = row
    return result


def key_map(output: Output, value: Any) -> dict[str, str]:
    """The scope's complete `key -> revision` map for a keyed write (§4)."""

    content = entries(output, value)
    if output.is_partition_set:
        return dict.fromkeys(content, "1")
    if not output.revision:
        return {key: revision(entry) for key, entry in content.items()}
    result = {}
    for key, row in content.items():
        if not isinstance(row, Mapping) or output.revision not in row:
            raise WriteError(f"{output.name}: {key!r} lacks the declared revision field {output.revision!r}")
        result[key] = str(row[output.revision])
    return result


def remove_empty_dirs(objects, prefixes) -> None:
    """A local filesystem store keeps directories its objects left behind and
    lists them; object stores have no directories. Remove the empty ones."""

    root = getattr(objects, "prefix", None)
    if type(objects).__name__ != "LocalStore" or root is None:
        return
    for prefix in sorted(set(prefixes), key=len, reverse=True):
        path = os.path.join(str(root), prefix.strip("/"))
        while os.path.normpath(path) != os.path.normpath(str(root)):
            try:
                os.rmdir(path)
            except OSError:
                break
            path = os.path.dirname(path)


def _segment(name: str) -> str:
    """A path segment for a partition or key: any string, escaped."""

    segment = quote(name, safe="")
    return "%2E" + segment[1:] if segment in (".", "..") else segment


MISSING = object()
PARALLEL = 32  # object requests in flight per load or write


class FileStore:
    """The default store (§4): what an asset returns, as files — one per
    value, per partition, per key, or per batch — overwritten in place.

        {root}/rollup.json                      a value
        {root}/site_status/alpha.json           a value, partition alpha
        {root}/uploads/u-7.pkl                  a keyed output: one object per key
        {root}/site_files/alpha/f-1.json        keyed and partitioned
        {root}/site_events/alpha/000000000042.json
                                                an unkeyed incremental output: one per batch

    Content is JSON when it round-trips exactly, pickle otherwise. There is
    no history: a write replaces what it covers, and a read gets what is
    there now, which may be newer than the version it was pinned to. A
    keyed write touches only the keys the harness found changed (`Scope`),
    and each file is written aside and renamed into place, so a crash never
    leaves half of one.

    `path` defaults to `$SOLERA_DATA`, else `.solera/data` next to the
    project file. A key written as JSON shadows a stale pickle of it, so
    only a pickle write has to delete its JSON twin."""

    version = "1"
    ref_type = ObjectRef
    shared_table = False

    def __init__(self, path: str | os.PathLike | None = None):
        self.path = path
        self.home: str | None = None  # the project's directory, for the default path
        self._stores: dict[str, Any] = {}

    def _objects(self):
        from obstore.store import LocalStore

        if self.path is not None:
            root = os.fspath(self.path)
        else:
            root = os.environ.get("SOLERA_DATA") or os.path.join(self.home or os.getcwd(), ".solera", "data")
        root = os.path.abspath(root)
        if root not in self._stores:
            self._stores[root] = LocalStore(root, mkdir=True)
        return self._stores[root]

    def can_load(self, t, selection) -> bool:
        if is_ref_type(t):
            return issubclass(self.ref_type, t)
        return True

    def can_store(self, t, output) -> bool:
        if t is None:
            return True
        if output.key == KEYS:
            return t in (dict, Mapping) or (
                typing.get_origin(t) in (dict, Mapping) and typing.get_args(t)[:1] == (str,)
            )
        if output.incremental:  # rows, elements or batches: a list
            return t is list or typing.get_origin(t) is list or _is_dataframe_type(t)
        return True  # a value: anything

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        output = scope.output
        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")
        patch = isinstance(write, Patch)
        if patch and not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        base = self._base(output, scope, prior)
        if output.is_partition_set:
            return await self._store_set(write, prior, scope, base)
        if output.key is not None:
            return await self._store_keyed(write, prior, scope, base)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_batch(write, prior, scope, base)
        version = await self._put(base, write)
        return Written(self._ref(scope, {"mode": "value", "path": base}, version))

    async def _store_set(self, write, prior, scope, base) -> Written:
        """A partition set: its element list, as one value."""

        if isinstance(write, Patch):
            elements = list(entries(scope.output, write.rows))
            if prior is not None:
                drop = {str(k) for k in write.remove} | set(elements)
                elements = [e for e in await self._elements(prior) if e not in drop] + elements
        else:
            elements = list(entries(scope.output, write))
        version = await self._put(base, elements)
        return Written(self._ref(scope, {"mode": "set", "path": base}, version))

    async def _store_keyed(self, write, prior, scope, base) -> Written:
        """One object per key. A replacement — or a Patch with no prior, a
        first write or a full run — is the whole content; a Patch changes
        the keys it names."""

        output = scope.output
        patch = isinstance(write, Patch)
        content = entries(output, write.rows if patch else write)
        replace = not patch or prior is None
        if scope.upserts is None:
            upserts = list(content)
        else:
            upserts = [k for k in scope.upserts if k in content]
        if scope.removes is not None:
            removes = set(scope.removes)
        elif replace:
            removes = {k for k in await self._keys(base) if k not in content}
        else:
            removes = {str(k) for k in write.remove}
        written = await self._many(lambda k: self._put(f"{base}/{_segment(k)}", content[k]), upserts)
        await self._many(lambda k: self._delete(f"{base}/{_segment(k)}"), removes)
        start = "" if replace else prior.version
        version = _digest([start, scope.batch, sorted(zip(upserts, written, strict=True)), sorted(removes)])
        handle = {"mode": "keyed", "path": base, "key": output.key}
        return Written(self._ref(scope, handle, version))

    async def _store_batch(self, write: Patch, prior, scope, base) -> Written:
        """An unkeyed incremental write: its items, as one object per batch.
        With no prior (a first write or a full run) the output starts over,
        and earlier batches are deleted."""

        output = scope.output
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        items = write.rows
        if _is_dataframe(items):
            items = items.to_dict(orient="records")
        if not isinstance(items, list):
            raise WriteError(f"{output.name}: a batch is a list, got {type(items).__name__}")
        if not items and prior is not None:
            return Written(prior)
        if scope.batch is not None:
            batch = scope.batch
        else:
            batch = int(prior.handle["batches"][1]) + 1 if prior is not None else 0
        version = await self._put(f"{base}/{batch:012d}", items)
        if prior is None:
            earlier = [k for k in await self._keys(base) if k.isdigit() and int(k) < batch]
            await self._many(lambda k: self._delete(f"{base}/{k}"), earlier)
            first = batch
        else:
            first = int(prior.handle["batches"][0])
            version = _digest([prior.version, version])
        handle = {"mode": "batches", "path": base, "batches": [first, batch]}
        return Written(self._ref(scope, handle, version))

    # -- reads ------------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any:
        if is_ref_type(t):
            return ref
        handle = ref.handle or {}
        mode, base = handle.get("mode"), handle.get("path")
        if base is None:
            raise StoreError(f"{ref.output}: not a {type(self).__name__} ref")
        if mode == "batches":
            if isinstance(selection, Keys):
                raise StoreError(f"{ref.output}: an unkeyed incremental output takes Batches")
            first, last = (int(b) for b in handle["batches"])
            lo, hi = (max(first, selection.lo), selection.hi) if selection is not None else (first, last)
            batches = await self._many(lambda b: self._get(f"{base}/{b:012d}"), range(lo, hi + 1))
            return _materialize([item for b in batches if b is not MISSING for item in b], t)
        if mode == "keyed":
            if isinstance(selection, Batches):
                raise StoreError(f"{ref.output}: a keyed output takes Keys, not Batches")
            keys = list(selection.revisions) if selection is not None else sorted(await self._keys(base))
            found = await self._many(lambda k: self._get(f"{base}/{_segment(k)}"), keys)
            content = {k: v for k, v in zip(keys, found, strict=True) if v is not MISSING}
            if handle.get("key") == KEYS:
                return content
            return _materialize(list(content.values()), t)
        value = await self._get(base)
        if value is MISSING:
            raise StoreError(f"{ref.output}: {base} is gone")
        if mode == "set":
            if selection is not None:
                value = [e for e in value if e in selection.revisions]
            return _materialize(value, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: an unkeyed output cannot serve a selection")
        return value

    async def _elements(self, ref: Ref) -> list[str]:
        value = await self._get((ref.handle or {}).get("path", ""))
        return [] if value is MISSING else list(value)

    # -- objects ----------------------------------------------------------------

    @staticmethod
    def _base(output, scope, prior) -> str:
        """Where the scope's content lives: the prior's place, so a renamed
        output keeps its objects where they are (§2)."""

        if prior is not None and "path" in (prior.handle or {}):
            return prior.handle["path"]
        name = _segment(output.name)
        return f"{name}/{_segment(scope.partition)}" if scope.partition else name

    async def _put(self, base: str, value) -> str:
        """Write `value` at `base`; returns its revision."""

        import obstore

        data, fmt = encode(value)
        await obstore.put_async(self._objects(), f"{base}.{fmt}", data, mode="overwrite", use_multipart=False)
        if fmt == "pkl":
            await self._delete(base, "json")
        return hashlib.sha256(data).hexdigest()

    async def _get(self, base: str):
        import obstore
        from obstore.exceptions import NotFoundError

        for fmt in ("json", "pkl"):
            try:
                result = await obstore.get_async(self._objects(), f"{base}.{fmt}")
            except (NotFoundError, FileNotFoundError):
                continue
            data = bytes(await result.bytes_async())
            return json.loads(data) if fmt == "json" else pickle.loads(data)
        return MISSING

    async def _delete(self, base: str, *formats: str) -> None:
        import obstore
        from obstore.exceptions import NotFoundError

        for fmt in formats or ("json", "pkl"):
            try:
                await obstore.delete_async(self._objects(), f"{base}.{fmt}")
            except (NotFoundError, FileNotFoundError):
                pass

    async def _keys(self, base: str) -> set[str]:
        """The names of the objects in a directory: its keys or batches."""

        import obstore

        found = set()
        async for chunk in obstore.list(self._objects(), prefix=f"{base}/"):
            for meta in chunk:
                name = meta["path"][len(base) + 1 :]
                if "/" not in name and name.endswith((".json", ".pkl")):
                    found.add(unquote(name.rsplit(".", 1)[0]))
        return found

    @staticmethod
    async def _many(fn, items) -> list:
        limit = asyncio.Semaphore(PARALLEL)

        async def one(item):
            async with limit:
                return await fn(item)

        return await asyncio.gather(*(one(item) for item in items))

    @staticmethod
    def _ref(scope, handle, version) -> ObjectRef:
        return ObjectRef(
            output=scope.output.name, store="", handle=handle, version=version, partition=scope.partition
        )


class S3Store(FileStore):
    """FileStore's layout and behavior in a bucket: `S3Store("s3://bucket/prefix")`.
    `options` go to obstore (`region`, `endpoint`, credentials, …); `env:NAME`
    values are resolved in the harness. A PUT is atomic, so there is nothing
    to rename."""

    def __init__(self, url: str, **options: Any):
        super().__init__()
        self.url, self.options = url, options

    def _objects(self):
        if "" not in self._stores:
            import obstore

            self._stores[""] = obstore.store.from_url(resolve_env(self.url), **resolve_env(self.options))
        return self._stores[""]


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def _materialize(items: list, t):
    if _is_dataframe_type(t):
        import pandas as pd

        return pd.DataFrame(items)
    return items
