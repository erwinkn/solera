"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import pickle
import typing
from collections.abc import Iterable, Mapping, Sequence
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

    For a keyed output the harness reads the write once (`prepared`) and
    says which keys it changes against the key index: `upserts` are the keys
    to write, each to the version the index will hold, `removes` the keys to
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
    upserts: Any = None  # Mapping[str, bytes], a `DeltaKeys`, or None
    removes: frozenset[str] | None = None
    # The attempt's generation and invocation, for a `fenced` store to check
    # (docs/lifecycle.md §9.7); `None` outside an attempt.
    generation: int | None = None
    invocation: str | None = None
    prepared: Prepared | None = None


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
    """A store may also define `stamped(output)`: columns it adds to every
    row itself, which a row's digest leaves out (docs/row-digest.md).

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


@dataclass(frozen=True)
class Prepared:
    """A keyed write, read once for the key index and for its store (§4, §6).

    `rows` holds its keys, sorted, each the group of rows that carry it,
    with versions computed natively (`solera.keys.Rows`, docs/row-digest.md):
    the key index reads it to work out what changed, and the store, asked to
    write some keys, reads their groups from `payload` (`groups`) — what the
    producer returned, flattened, kept in its own types until the store
    serializes it: rows (a list of mappings, a DataFrame or an Arrow table),
    a `keyed=True` output's `(key, value)` items, or a partition set's
    elements. A `Patch` also names the keys it `removes`, none it writes."""

    output: Output
    payload: Any
    rows: Any
    patch: bool = False
    removes: tuple[str, ...] = ()

    def groups(self, keys: Sequence[str]) -> list:
        """Each key's group, as its store writes it: the list of its rows as
        mappings, a `keyed=True` output's value, or a partition set's element."""

        try:
            rows, ends = self.rows.find(list(keys))
        except KeyError as e:
            raise StoreError(
                f"{self.output.name}: asked to write key {e.args[0]!r}, which the write does not hold"
            ) from None
        if self.output.is_partition_set:
            return list(keys)
        if self.output.key == KEYS:
            return [self.payload[r][1] for r in rows]
        picked = _take(self.payload, rows)
        return [picked[a:b] for a, b in zip([0, *ends], ends, strict=False)]

    def all_rows(self) -> list[dict]:
        """Every row of a rows output, as mappings."""

        return _take(self.payload, None)

    def entries(self) -> list[tuple[str, bytes]]:
        """Every key and its version, in key order."""

        from .keys.index import key_str

        keys, versions = self.rows.entries()
        return list(zip(map(key_str, keys), versions, strict=True))

    def version(self, prior: Ref | None) -> str:
        """The written ref's version (§3): a replacement's is its content's,
        every key and version; a patch's, its prior's and its own content's
        and removes. Free once the key index has read the write."""

        content = self.rows.digest().hex()
        if self.patch and prior is not None:
            return _digest([prior.version, content, list(self.removes)])
        return _digest(["rows", content])


def _take(payload: Any, rows: list[int] | None) -> list:
    """Rows of `payload` as mappings — those at `rows`, in that order, or all."""

    if isinstance(payload, list):
        return payload if rows is None else [payload[r] for r in rows]
    if _is_dataframe(payload):
        return (payload if rows is None else payload.iloc[rows]).to_dict(orient="records")
    return (payload if rows is None else payload.take(rows)).to_pylist()  # an Arrow table


def prepare(write: Any, output: Output, exclude: tuple[str, ...] = ()) -> Prepared:
    """A keyed write — the whole content, or a `Patch` — read once
    (`Prepared`): lists of mappings, by-key mappings (`{key: rows}`), pandas
    DataFrames (made Arrow through DuckDB), Arrow data, a `keyed=True`
    output's dict, a partition set's elements. A row's digest leaves out
    its key column and the `exclude`d ones — columns its store adds, so a
    row digests the same as written and as read back."""

    from .keys import Rows

    name = output.name
    patch = isinstance(write, Patch)
    content = write.rows if patch else write
    try:
        if output.is_partition_set:
            payload = [str(e) for e in content or ()]
            rows = Rows.keys(payload, b"1")
        elif output.key == KEYS:
            if content is None:
                content = {}
            if not isinstance(content, Mapping) or not all(isinstance(k, str) for k in content):
                raise WriteError(f"{name}: a keyed output takes dict[str, Any], got {type(content).__name__}")
            payload = list(content.items())
            rows = Rows.values(payload)
        else:
            payload, empty = content, []
            flat = by_key(content, output)
            if flat is not None:
                payload, empty = flat
            if _is_dataframe(payload):
                import duckdb

                rows = Rows.arrow(
                    duckdb.connect().from_df(payload), output.key, output.revision, list(exclude), empty
                )
            elif hasattr(payload, "__arrow_c_stream__"):
                import pyarrow as pa

                if not isinstance(payload, pa.Table):  # a stream reads once: hold it
                    payload = pa.table(payload)
                rows = Rows.arrow(payload, output.key, output.revision, list(exclude), empty)
            else:
                if payload is None:
                    payload = []
                if not isinstance(payload, list) or not all(isinstance(r, Mapping) for r in payload):
                    raise WriteError(
                        f"{name}: expected rows (list[dict] or DataFrame), got {type(payload).__name__}"
                    )
                rows = Rows.records(payload, output.key, output.revision, list(exclude), empty)
        removes = ()
        if patch:
            removes = tuple(k for k in sorted(set(map(key_text, write.remove))) if k not in rows)
    except KeyError as e:
        raise WriteError(f"{name}: row lacks the declared key column {output.key!r}") from e
    except ValueError as e:  # a key that is not one, Arrow data without the columns, a value with no digest
        raise WriteError(f"{name}: {e}") from e
    return Prepared(output, payload, rows, patch, removes)


def prepare_for(store: Any, write: Any, output: Output) -> Prepared:
    """`prepare`, leaving out the columns `store` adds itself (`stamped`)."""

    stamped = getattr(store, "stamped", None)
    return prepare(write, output, tuple(stamped(output)) if stamped is not None else ())


def key_rows(write: Any, output: Output, exclude: tuple[str, ...] = ()):
    """A keyed write's `solera.keys.Rows` (`prepare`)."""

    return prepare(write, output, exclude).rows


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
            prepared = scope.prepared or await asyncio.to_thread(prepare_for, self, write, output)
            return await self._store_keyed(prepared, prior, scope, base, generation)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_batch(write, prior, scope, base, generation)
        name = f"{base}@{generation}"
        version = await self._put(name, write)
        return Written(self._ref(scope, {"mode": "value", "path": name, "base": base}, version))

    async def _store_set(self, write, prior, scope, base, generation) -> Written:
        """A partition set: its element list, as one value."""

        elements = (scope.prepared or prepare(write, scope.output)).payload
        if isinstance(write, Patch) and prior is not None:
            drop = {key_text(k) for k in write.remove} | set(elements)
            elements = [e for e in await self._elements(prior) if e not in drop] + elements
        name = f"{base}@{generation}"
        version = await self._put(name, elements)
        return Written(self._ref(scope, {"mode": "set", "path": name, "base": base}, version))

    async def _store_keyed(self, prepared: Prepared, prior, scope, base, generation) -> Written:
        """One object per key and version. The harness says which keys
        changed, each to its version (`Scope.upserts`) — listed, or paged
        from the commit's delta files when there are too many to list; only
        their groups are read from the write and written, named by the
        versions the key index will hold, so no object goes unnamed. Removed
        keys need no write: the index stops naming them, and `discard`
        deletes what nothing reads."""

        async def put(entry) -> None:
            (key, version), group = entry
            await self._put(self.key_name(base, key, version, generation), group)

        async for page in _selected(scope.upserts, prepared):
            groups = await asyncio.to_thread(prepared.groups, [k for k, _ in page])
            await self._many(put, zip(page, groups, strict=True))
        handle = {"mode": "keyed", "path": base, "key": scope.output.key}
        return Written(self._ref(scope, handle, prepared.version(prior)))

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

        import obstore

        objects = self._objects()
        # Listed from `lo` on: an object store lists in key order, so it stops past
        # `hi`; a local directory comes in any order, and is read to its end.
        ordered = type(objects).__name__ != "LocalStore"
        best: dict[int, tuple[int, str]] = {}
        async for chunk in obstore.list(objects, prefix=f"{base}/", offset=f"{base}/{lo:012d}"):
            for meta in chunk:
                name = meta["path"][len(base) + 1 :]
                if "/" in name or not name.endswith((".json", ".pkl")):
                    continue
                batch, _, generation = unquote(name.rsplit(".", 1)[0]).partition(".")
                if not (batch.isdigit() and generation.isdigit()) or int(batch) < lo:
                    continue
                if int(batch) > hi:
                    if ordered:
                        return {b: n for b, (_, n) in best.items()}
                    continue
                if int(generation) >= best.get(int(batch), (-1, ""))[0]:
                    best[int(batch)] = (int(generation), f"{base}/{batch}.{generation}")
        return {b: n for b, (_, n) in best.items()}

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
        """`fn` of each item, `PARALLEL` at a time, results in order: a fixed
        set of workers takes the items one by one, so a million items never
        become a million waiting tasks."""

        items = list(items)
        results: list = [None] * len(items)
        todo = iter(enumerate(items))

        async def worker():
            for i, item in todo:
                results[i] = await fn(item)

        await asyncio.gather(*(worker() for _ in range(min(PARALLEL, len(items)))))
        return results

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


async def _selected(upserts, prepared: Prepared, size: int = 100_000):
    """The keys a keyed write writes, each with its version, sorted, a page at
    a time: every key of the write, the listed ones, or the pages of a
    delta's selection."""

    if upserts is None:
        entries = await asyncio.to_thread(prepared.entries)
        for i in range(0, len(entries), size):
            yield entries[i : i + size]
    elif isinstance(upserts, Mapping):
        entries = sorted(upserts.items())
        for i in range(0, len(entries), size):
            yield entries[i : i + size]
    else:
        async for page in upserts.pages(size):
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
