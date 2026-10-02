"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import pickle
import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote, unquote

from .sdk import KEYS, ObjectRef, Output, Ref, is_ref_type


class StoreError(Exception):
    retryable = False


class WriteError(StoreError):
    """Malformed write: duplicate keys, wrong shape, disallowed op."""


@dataclass(frozen=True)
class Patch:
    """Partial write: replace the named keys, delete `remove` (§4). For a
    keyed rows output, `rows` is the rows themselves, carrying their key
    column, or — by key — `{key: rows}`: each key's group, the key column
    stamped by the store, an empty group a live key with no rows
    (docs/per-key-processing.md §6)."""

    rows: Any
    remove: Any = ()


@dataclass(frozen=True)
class Sql:
    """PostgresStore only: materialize a SELECT, or run a statement verbatim (§4)."""

    stmt: str


@dataclass(frozen=True)
class Keys:
    """A selection passed to `store.load` (§4): `key -> (revision, locator)`,
    as the key index holds them — the version (docs/row-digest.md), and the
    generation that wrote it, from which a store names the key's object
    without listing (lifecycle.md §9.8)."""

    revisions: Mapping[str, tuple[bytes, int]]


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
    in the write, and for a replacement, every key not in it goes. Past
    what fits in a list, an immutable store's `upserts` is a
    `solera.keys.index.DeltaKeys`, whose `pages()` read them from the
    commit's delta files. Asked to write a key the write does not hold, a
    store raises: a requested write never silently disappears."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()
    upserts: Any = None  # frozenset[str], a `DeltaKeys`, or None
    removes: frozenset[str] | None = None
    # The attempt's generation and invocation, for a `fenced` store to check
    # (docs/lifecycle.md §9.7); `None` outside an attempt.
    generation: int | None = None
    invocation: str | None = None


@dataclass(frozen=True)
class Written:
    """What a store wrote. `keys` is only for writes the harness never sees as
    rows (`Sql` materialized inside Postgres): the scope's complete new
    content, sorted by the key's UTF-8 bytes, in chunks the harness pulls one
    at a time after `store` returned — rows (a list of mappings, or Arrow
    data) with the key column and the declared revision, or every column, so
    their versions are those of any other write (docs/row-digest.md). For
    every other write the harness derives keys from the rows itself (§6, §9)."""

    ref: Ref
    keys: Iterable | None = None


@runtime_checkable
class Store(Protocol):
    """A store may also define `key_rows(write, output) -> solera.keys.Rows`:
    a keyed write's content — the whole write, or a `Patch`'s rows — for the
    key index, read from the store's own types (§6, docs/row-digest.md).
    Without it, `key_rows` below reads lists of dicts, dicts, DataFrames and
    Arrow data.

    `writes` says how it writes, which decides what the engine may do once
    it gave up on a writer (docs/lifecycle.md §9.6): `"immutable"` (it only
    ever writes names nothing committed references; it implements
    `discard`), `"fenced"` (it implements `acquire`, and every write checks
    the generation atomically), or `"overwrite"`, the default (a scope whose
    writer may still write is held `late_write_grace` seconds, or — with
    `strict` — until the writer's completion is established)."""

    version: str = "1"
    ref_type: type[Ref] = Ref
    writes: str = "overwrite"
    strict: bool = False
    late_write_grace: float = 120.0

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


def key_text(value: Any) -> str:
    """A key as the index and every store name it: a `str` as it is, an `int`
    (not a `bool`) as its decimal text. Nothing else is a key — the same
    rule native row extraction follows."""

    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise WriteError(f"a key must be a str or an int, not {type(value).__name__}")


def by_key(write: Any, output: Output) -> tuple[Any, list[str]] | None:
    """A by-key write of a keyed rows output — `{key: rows}` — as flat rows
    with the key column stamped, and the keys whose group is empty; `None`
    for any other write. Rows that carry the key column already must agree
    with their key. DataFrames stay a DataFrame, so their values keep their
    Arrow digests (docs/row-digest.md); anything else becomes a list of dicts."""

    if output.key in (None, KEYS) or output.is_partition_set or not isinstance(write, Mapping):
        return None
    column, frames, rows, empty = output.key, [], [], []
    for key, group in write.items():
        if not isinstance(key, str):
            raise WriteError(f"{output.name}: a by-key write takes str keys, got {type(key).__name__}")
        if _is_dataframe(group):
            if column in group.columns and (group[column].astype(str) != key).any():
                raise WriteError(f"{output.name}: rows of {key!r} carry another {column!r}")
            if len(group):
                frames.append(group.assign(**{column: key}))
            else:
                empty.append(key)
            continue
        found = _rows(group, output.name)
        for row in found:
            if column in row and key_text(row[column]) != key:
                raise WriteError(f"{output.name}: a row of {key!r} carries {column}={row[column]!r}")
            row[column] = key
        rows.extend(found)
        if not found:
            empty.append(key)
    if frames and not rows:
        import pandas as pd

        return pd.concat(frames, ignore_index=True), empty
    return rows + [r for f in frames for r in f.to_dict(orient="records")], empty


def entries(output: Output, value: Any) -> dict[str, Any]:
    """A keyed write's content as `key -> entry` (§4): a `keyed=True`
    output's dict, a partition set's elements, or rows grouped by their key
    column — every key holds the list of rows that carry it."""

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
    result: dict[str, list] = {}
    flat = by_key(value, output)
    if flat is not None:
        value = flat[0]
        result.update((k, []) for k in flat[1])
    for row in _rows(value, output.name):
        if output.key not in row:
            raise WriteError(f"{output.name}: row lacks the declared key column {output.key!r}")
        try:
            result.setdefault(key_text(row[output.key]), []).append(row)
        except WriteError as e:
            raise WriteError(f"{output.name}: {e}") from None
    return result


def key_rows(write: Any, output: Output, exclude: tuple[str, ...] = ()):
    """A keyed write's content for the key index (`solera.keys.Rows`), every
    key the group of rows that carry it, with versions computed natively as
    the index reaches each key (docs/row-digest.md). Arrow data (anything
    with `__arrow_c_stream__`) is read in place; a DataFrame becomes Arrow
    through DuckDB. A row's digest leaves out its key column and the
    `exclude`d ones — columns its store adds, so a row digests the same as
    written and as read back."""

    from .keys import Rows

    name = output.name
    try:
        if output.is_partition_set:
            return Rows.keys([str(e) for e in write or ()], b"1")
        if output.key == KEYS:
            if write is None:
                write = {}
            if not isinstance(write, Mapping) or not all(isinstance(k, str) for k in write):
                raise WriteError(f"{name}: a keyed output takes dict[str, Any], got {type(write).__name__}")
            return Rows.values(list(write.items()))
        flat = by_key(write, output)
        if flat is not None:
            return _with_empty(key_rows(flat[0], output, exclude), flat[1])
        if _is_dataframe(write):
            import duckdb

            write = duckdb.connect().from_df(write)
        if hasattr(write, "__arrow_c_stream__"):
            return Rows.arrow(write, output.key, output.revision, list(exclude))
        if write is None:
            write = []
        if not isinstance(write, list) or not all(isinstance(r, Mapping) for r in write):
            raise WriteError(f"{name}: expected rows (list[dict] or DataFrame), got {type(write).__name__}")
        return Rows.records(write, output.key, output.revision, list(exclude))
    except KeyError as e:
        raise WriteError(f"{name}: row lacks the declared key column {output.key!r}") from e
    except ValueError as e:  # a key that is not one, Arrow data without the columns, a value with no digest
        raise WriteError(f"{name}: {e}") from e


def _with_empty(rows, empty: list[str]):
    """`rows`, and keys whose group is empty at the empty group's version —
    whatever `revision=` says, since no row carries one (per-key §6)."""

    if not empty:
        return rows
    from ._native import group_digest
    from .keys import Rows
    from .keys.index import key_bytes

    keys, versions = rows.entries()
    pairs = list(zip(keys, versions, strict=True))
    nothing = group_digest([])
    pairs += [(key_bytes(k), nothing) for k in empty]
    return Rows.pairs(sorted(pairs))


def store_key_rows(store: Store, write: Any, output: Output):
    """`store.key_rows`, or the default `key_rows`."""

    own = getattr(store, "key_rows", None)
    return own(write, output) if own is not None else key_rows(write, output)


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
    value, per key, or per batch — each written once, under a name no other
    attempt writes (docs/lifecycle.md §9.8):

        {root}/rollup@184467.json               a value, by generation 184467
        {root}/site_status/alpha@184467.json    a value, partition alpha
        {root}/uploads/u-7/9c41e0….184467.pkl   a keyed output: one object per key and version
        {root}/site_files/alpha/f-1/5d2a….184467.json
                                                keyed and partitioned: the key's rows
        {root}/site_events/alpha/000000000042.184467.json
                                                an unkeyed incremental output: one per batch

    A name carries the logical version and the writing attempt's generation
    (`Scope.generation`), and a write is create-only: a dead writer only
    ever leaves objects nothing references, and a reader gets exactly the
    version it was pinned to. A keyed read names its objects from the key
    index's `(version, locator)` (`Keys`); a range of batches keeps, per
    batch, the highest generation, which committed it. Superseded objects
    are deleted by `discard`, once nothing can read them.

    Content is JSON when it round-trips exactly, pickle otherwise. `path`
    defaults to `$SOLERA_DATA`, else `.solera/data` next to the project
    file."""

    version = "2"
    writes = "immutable"
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
        generation = int(scope.generation or 0)
        if output.is_partition_set:
            return await self._store_set(write, prior, scope, base, generation)
        if output.key is not None:
            return await self._store_keyed(write, prior, scope, base, generation)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_batch(write, prior, scope, base, generation)
        name = f"{base}@{generation}"
        version = await self._put(name, write)
        return Written(self._ref(scope, {"mode": "value", "path": name, "base": base}, version))

    async def _store_set(self, write, prior, scope, base, generation) -> Written:
        """A partition set: its element list, as one value."""

        if isinstance(write, Patch):
            elements = list(entries(scope.output, write.rows))
            if prior is not None:
                drop = {str(k) for k in write.remove} | set(elements)
                elements = [e for e in await self._elements(prior) if e not in drop] + elements
        else:
            elements = list(entries(scope.output, write))
        name = f"{base}@{generation}"
        version = await self._put(name, elements)
        return Written(self._ref(scope, {"mode": "set", "path": name, "base": base}, version))

    async def _store_keyed(self, write, prior, scope, base, generation) -> Written:
        """One object per key and version. The harness says which keys
        changed (`Scope.upserts`) — listed, or paged from the commit's delta
        files when there are too many to list; only those are written, at
        the versions the key index will hold, so no object goes unnamed.
        Removed keys need no write: the index stops naming them, and
        `discard` deletes what nothing reads."""

        output = scope.output
        patch = isinstance(write, Patch)
        rows = write.rows if patch else write
        content = entries(output, rows)
        from .keys.index import key_str

        keys, versions = store_key_rows(self, rows, output).entries()
        version_of = {key_str(k): v for k, v in zip(keys, versions, strict=True)}

        async def put(key: str) -> str:
            if key not in content:
                raise StoreError(f"{output.name}: asked to write key {key!r}, which the write does not hold")
            return await self._put(self.key_name(base, key, version_of[key], generation), content[key])

        start = "" if (not patch or prior is None) else prior.version
        digest = hashlib.sha256(json.dumps([start, scope.batch]).encode())
        async for page in _pages(scope.upserts, content):
            for key, revision in zip(page, await self._many(put, page), strict=True):
                digest.update(json.dumps([key, revision]).encode())
        removes = (
            sorted(scope.removes)
            if scope.removes is not None
            else sorted(key_text(k) for k in (write.remove if patch else ()))
        )
        version = _digest([digest.hexdigest(), removes])
        handle = {"mode": "keyed", "path": base, "key": output.key}
        return Written(self._ref(scope, handle, version))

    async def _store_batch(self, write: Patch, prior, scope, base, generation) -> Written:
        """An unkeyed incremental write: its items, as one object per batch.
        With no prior (a first write or a full run) the output starts over at
        this batch; earlier ones are no longer read, and go with `discard`."""

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
        version = await self._put(f"{base}/{batch:012d}.{generation}", items)
        if prior is None:
            first = batch
        else:
            first = int(prior.handle["batches"][0])
            version = _digest([prior.version, version])
        handle = {"mode": "batches", "path": base, "batches": [first, batch]}
        return Written(self._ref(scope, handle, version))

    @staticmethod
    def version_name(version: bytes) -> str:
        """A key's version as a file name: its hex, or — past 32 bytes, a long
        declared revision — the hex of the first 16 bytes of its SHA-256."""

        if len(version) > 32:
            version = hashlib.sha256(version).digest()[:16]
        return version.hex()

    @classmethod
    def key_name(cls, base: str, key: str, version: bytes, locator: int) -> str:
        return f"{base}/{_segment(key)}/{cls.version_name(version)}.{int(locator)}"

    async def discard(self, scope: Scope, prior: Ref | None, items: list) -> None:
        """Delete objects nothing reads any more (docs/lifecycle.md §9.8):
        superseded versions, and what attempts that never committed wrote.
        `items` name them: `("key", key, version_hex, locator)`,
        `("path", path)`, `("value", generation)`, `("batch", n, generation)`,
        or `("batches", lo, hi)` — every object of batches lo..hi. Names are
        never reused, so deleting one twice is no harm."""

        base = self._base(scope.output, scope, prior)
        names, ranges = [], []
        for item in items:
            kind = item[0]
            if kind == "key":
                names.append(self.key_name(base, item[1], bytes.fromhex(item[2]), item[3]))
            elif kind == "path":
                names.append(item[1])
            elif kind == "value":
                names.append(f"{base}@{int(item[1])}")
            elif kind == "batch":
                names.append(f"{base}/{int(item[1]):012d}.{int(item[2])}")
            elif kind == "batches":
                ranges.append((int(item[1]), int(item[2])))
            else:
                raise StoreError(f"{scope.output.name}: cannot discard {item!r}")
        if ranges:
            for name in await self._keys(base):
                batch = name.partition(".")[0]
                if batch.isdigit() and any(lo <= int(batch) <= hi for lo, hi in ranges):
                    names.append(f"{base}/{name}")
        await self._many(lambda n: self._delete(n), names)

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
            names = await self._batches(base, lo, hi)
            batches = await self._many(self._get, [names[b] for b in sorted(names)])
            return _materialize([item for b in batches if b is not MISSING for item in b], t)
        if mode == "keyed":
            if not isinstance(selection, Keys):
                raise StoreError(
                    f"{ref.output}: a keyed read names its objects from the key index: it takes Keys"
                )
            keys = list(selection.revisions)
            found = await self._many(
                lambda k: self._get(self.key_name(base, k, *selection.revisions[k])), keys
            )
            content = {k: v for k, v in zip(keys, found, strict=True) if v is not MISSING}
            if handle.get("key") == KEYS:
                return content
            inner = by_key_type(t)
            if inner is not MISSING:  # dict[str, T]: each key's group, as T
                return {k: _materialize(rows, inner) for k, rows in content.items()}
            return _materialize([row for rows in content.values() for row in rows], t)
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

    async def _batches(self, base: str, lo: int, hi: int) -> dict[int, str]:
        """Batch `n` -> its committed object, in `[lo, hi]`: of the objects
        named after `n`, the highest generation's. The attempts that used
        batch `n` all ran between the commits of `n - 1` and `n`, one at a
        time, and the one that committed `n` was the last of them."""

        best: dict[int, tuple[int, str]] = {}
        for name in await self._keys(base):
            batch, _, generation = name.partition(".")
            if batch.isdigit() and generation.isdigit() and lo <= int(batch) <= hi:
                if int(generation) >= best.get(int(batch), (-1, ""))[0]:
                    best[int(batch)] = (int(generation), f"{base}/{name}")
        return {b: name for b, (_, name) in best.items()}

    # -- objects ----------------------------------------------------------------

    @staticmethod
    def _base(output, scope, prior) -> str:
        """Where the scope's content lives: the prior's place, so a renamed
        output keeps its objects where they are (§2)."""

        handle = (prior.handle or {}) if prior is not None else {}
        if "base" in handle or "path" in handle:
            return handle.get("base") or handle["path"]
        name = _segment(output.name)
        return f"{name}/{_segment(scope.partition)}" if scope.partition else name

    async def _put(self, base: str, value) -> str:
        """Create `value` at `base`, once; returns its revision. The same name
        written again is the same attempt's, with the same bytes."""

        from .objects import create

        data, fmt = encode(value)
        await create(self._objects(), f"{base}.{fmt}", data)
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
        """The names of the objects in a directory, without their format."""

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


async def _pages(upserts, content: dict):
    """The keys a keyed write writes, sorted, a page at a time: every key of
    the content, the listed ones, or the pages of a delta's selection."""

    if upserts is None:
        yield sorted(content)
    elif isinstance(upserts, (set, frozenset)):
        yield sorted(upserts)
    else:
        async for page in upserts.pages():
            yield page


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def by_key_type(t: Any) -> Any:
    """`T` of a `dict[str, T]` load — each key's group on its own, as `Each`
    reads a page (per-key §5) — else `MISSING`."""

    if typing.get_origin(t) in (dict, Mapping) and typing.get_args(t)[:1] == (str,):
        return typing.get_args(t)[1]
    return MISSING


def _materialize(items: list, t):
    if _is_dataframe_type(t):
        import pandas as pd

        return pd.DataFrame(items)
    return items
