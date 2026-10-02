"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import pickle
import typing
from collections.abc import Callable, Iterable, Mapping, Sequence
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
    stamped by the store. A key with no rows does not exist: given none, it
    is removed (docs/per-key-processing.md §6)."""

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
    output's former names. What a keyed write changes is the write's own
    (`KeyedWrite`)."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()
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
    """A keyed output's write reaches `store` as a `KeyedWrite`: read once,
    resolved against the key index. A store may define how it is read,
    `prepare(write, output) -> Prepared` — for types of its own; without
    it, `solera.stores.prepare` reads lists of mappings, pandas DataFrames
    and Arrow data — and `stamped(output)`, the columns it adds to every
    row itself, which a row's digest leaves out (docs/row-digest.md).

    `writes` says how a writer the engine gave up on is kept from writing
    over a newer one — every store declares one (docs/stores.md):
    `"immutable"`, it writes only names no other attempt uses, and
    implements `discard`; or `"fenced"`, it implements `acquire`, and every
    write checks the attempt's generation atomically (`solera.fencing`).

    It also says what a load sees (docs/stores.md): an immutable store
    returns exactly the version a ref and selection pin; a fenced store its
    current rows, so a reader may see a newer version than it pinned.
    `solera.testing.stores` checks a store against the contract."""

    version: str = "1"
    ref_type: type[Ref] = Ref
    writes: str  # "immutable" or "fenced"

    def can_load(self, t: type | None, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Batches | None) -> Any: ...

    # immutable: async def discard(self, scope: Scope, prior: Ref | None, items: list) -> None
    # fenced:    async def acquire(self, scope: Scope) -> None


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
    with the key column stamped, and the keys given no rows; `None`
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
    """A keyed write's content, read once (§4, §6). The key index resolves
    it, the store's version is computed from it, and the store writes its
    groups from it: what is hashed is what is stored.

    `rows` are its keys, sorted, each the group of the rows that carry it,
    with versions computed natively (`solera.keys.Rows`,
    docs/row-digest.md). `take(indices)` gives the write's rows at those
    indices — every row for None — as the store persists them: mappings for
    a rows output (a DataFrame's missing values None, an Arrow map a dict),
    `(key, value)` items for `keyed=True`, a partition set's elements; a
    store reading types of its own gives its own. A `Patch` also names the
    keys it `removes`, none of them written."""

    output: Output
    rows: Any
    take: Callable[[Sequence[int] | None], list]
    patch: bool = False
    removes: tuple[str, ...] = ()

    def groups(self, keys: Sequence[str]) -> list:
        """Each key's group, as its store writes it: the list of its rows, a
        `keyed=True` output's value, or a partition set's element."""

        try:
            rows, ends = self.rows.find(list(keys))
        except KeyError as e:
            raise StoreError(
                f"{self.output.name}: asked to write key {e.args[0]!r}, which the write does not hold"
            ) from None
        picked = self.take(rows)
        if self.output.is_partition_set:
            return picked
        if self.output.key == KEYS:
            return [value for _, value in picked]
        return [picked[a:b] for a, b in zip([0, *ends], ends, strict=False)]

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


@dataclass(frozen=True)
class KeyedWrite:
    """A keyed output's write as its store takes it: read once
    (`prepared`), resolved against the key index (§4, §6). A store needs
    four things of it: whether it is the scope's `whole` content (clear the
    scope first), its `removes`, its `pages()` — the keys to write, each with
    its version and group — and the `version` of what it writes.

    `upserts` are the keys to write, each at the version the index will
    hold: a mapping; a `solera.keys.index.DeltaKeys` reading them from the
    commit's delta files when there are too many to list; or None, every key
    of the write. `removes` are deleted, and every other key stays as it is
    — unless the write is `whole`, the scope's whole content: then every key
    not in it goes, and `upserts`, if given, are only those that changed,
    all a store that keeps unchanged keys as they are needs to write.
    `value` is what the producer returned, for a store that writes it as
    it is."""

    prepared: Prepared
    upserts: Any = None
    removes: frozenset[str] = frozenset()
    whole: bool = False
    value: Any = None

    @classmethod
    def of(cls, store: Any, write: Any, output: Output, prior: Ref | None) -> KeyedWrite:
        """A write as a store takes it with no key index to resolve it
        against: a replacement, or a patch's every key and remove."""

        if isinstance(write, KeyedWrite):
            return write
        prepared = prepare_for(store, write, output)
        if not prepared.patch or prior is None:
            return cls(prepared, whole=True, value=write)
        return cls(prepared, removes=frozenset(prepared.removes), value=write)

    async def pages(self, size: int = 100_000):
        """The keys to write, sorted, a page at a time: `(key, version, group)`
        each. Only a page's groups are taken from the write at once."""

        if self.upserts is None or isinstance(self.upserts, Mapping):
            pages = self.iter_pages(size)
            while (page := await asyncio.to_thread(next, pages, None)) is not None:
                yield page
            return
        async for page in self.upserts.pages(size):
            yield await asyncio.to_thread(self._grouped, page)

    def iter_pages(self, size: int = 100_000):
        """`pages()`, for a store that writes on a thread of its own. Not for
        a `DeltaKeys` selection: only immutable stores get one, and they
        page it asynchronously."""

        if self.upserts is None:
            from .keys.index import key_str

            for keys, versions in self.prepared.rows.pages(size):
                yield self._grouped(list(zip(map(key_str, keys), versions, strict=True)))
        elif isinstance(self.upserts, Mapping):
            entries = sorted(self.upserts.items())
            for i in range(0, len(entries), size):
                yield self._grouped(entries[i : i + size])
        else:
            raise StoreError(f"{self.prepared.output.name}: a delta's selection is paged asynchronously")

    def _grouped(self, page: list) -> list:
        groups = self.prepared.groups([k for k, _ in page])
        return [(k, v, g) for (k, v), g in zip(page, groups, strict=True)]

    def version(self, prior: Ref | None) -> str:
        return self.prepared.version(prior)


def prepare(write: Any, output: Output, exclude: tuple[str, ...] = ()) -> Prepared:
    """A keyed write — the whole content, or a `Patch` — read once
    (`Prepared`): a list of mappings, a by-key mapping (`{key: rows}`), a
    pandas DataFrame (read a column at a time, through pandas alone: a
    missing value — NaN, NaT, None, NA — is None), Arrow data (anything with
    `__arrow_c_stream__`, read in place), a `keyed=True` output's dict, a
    partition set's elements. Only the library of the value's own type is
    imported. A row's digest leaves out its key column and the `exclude`d
    ones — columns its store adds, so a row digests the same as written and
    as read back."""

    from .keys import Rows

    name = output.name
    patch = isinstance(write, Patch)
    content = write.rows if patch else write
    empty: list[str] = []
    try:
        if output.is_partition_set:
            elements = [str(e) for e in content or ()]
            rows, take = Rows.keys(elements, b"1"), _taker(elements)
        elif output.key == KEYS:
            if content is None:
                content = {}
            if not isinstance(content, Mapping) or not all(isinstance(k, str) for k in content):
                raise WriteError(f"{name}: a keyed output takes dict[str, Any], got {type(content).__name__}")
            items = list(content.items())
            rows, take = Rows.values(items), _taker(items)
        else:
            payload = content
            flat = by_key(content, output)
            if flat is not None:  # a key given no rows does not exist: a patch removes it
                payload, empty = flat
            args = (output.key, output.revision, list(exclude))
            if _is_dataframe(payload):
                names, columns = _frame(payload, name)
                rows = Rows.columns(names, columns, *args)
                take = _column_taker(names, columns)
            elif hasattr(payload, "__arrow_c_stream__"):
                if type(payload).__module__.startswith("pyarrow") and type(payload).__name__ != "Table":
                    import pyarrow as pa  # a stream reads once: hold it

                    payload = pa.table(payload)
                rows, take = Rows.arrow(payload, *args), _arrow_taker(payload)
            else:
                if payload is None:
                    payload = []
                if not isinstance(payload, list) or not all(isinstance(r, Mapping) for r in payload):
                    raise WriteError(
                        f"{name}: expected rows (list[dict] or DataFrame), got {type(payload).__name__}"
                    )
                rows, take = Rows.records(payload, *args), _taker(payload)
        removes = ()
        if patch:
            gone = set(map(key_text, write.remove)) | set(empty)
            removes = tuple(k for k in sorted(gone) if k not in rows)
    except KeyError as e:
        raise WriteError(f"{name}: row lacks the declared key column {output.key!r}") from e
    except ValueError as e:  # a key that is not one, Arrow data without the columns, a value with no digest
        raise WriteError(f"{name}: {e}") from e
    return Prepared(output, rows, take, patch, removes)


def _taker(items: list) -> Callable:
    return lambda rows: items if rows is None else [items[r] for r in rows]


def _frame(frame: Any, name: str) -> tuple[list[str], list[list]]:
    """A DataFrame's columns as Python values, each missing one None."""

    if not frame.columns.is_unique:
        raise WriteError(f"{name}: a DataFrame's column names must be unique")
    names, columns = [], []
    for column, series in frame.items():
        if not isinstance(column, str):
            raise WriteError(f"{name}: column names must be strings, got {column!r}")
        missing = series.isna().to_numpy()
        values = _column_values(series, missing)
        if missing.any():
            for i in missing.nonzero()[0].tolist():
                values[i] = None
        names.append(column)
        columns.append(values)
    return names, columns


def _column_values(series: Any, missing: Any) -> list:
    """A column's values as Python objects. Timestamps with no nanoseconds
    become `datetime`s, the same instants (and digests) as pandas'
    `Timestamp`s, made and read faster."""

    if series.dtype.kind == "M" and not (series.dt.nanosecond.to_numpy()[~missing] != 0).any():
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)  # an array now, a Series in pandas 3
            return list(series.dt.to_pydatetime())
    return series.tolist()


def _column_taker(names: list[str], columns: list[list]) -> Callable:
    n = len(columns[0]) if columns else 0

    def take(rows):
        return [
            {name: column[r] for name, column in zip(names, columns, strict=True)}
            for r in (range(n) if rows is None else rows)
        ]

    return take


def _arrow_taker(data: Any) -> Callable:
    """Rows of Arrow data as Python values that digest as the Arrow values
    do: maps as dicts. pyarrow is imported only here, for its own data, or
    when another library's has rows to give."""

    table = None

    def take(rows):
        nonlocal table
        if table is None:
            import pyarrow as pa

            table = data if isinstance(data, pa.Table) else pa.table(data)
        return (table if rows is None else table.take(rows)).to_pylist(maps_as_pydicts="strict")

    return take


def prepare_for(store: Any, write: Any, output: Output) -> Prepared:
    """`store.prepare`, or the default `prepare`, leaving out the columns
    the store adds itself (`stamped`)."""

    own = getattr(store, "prepare", None)
    if own is not None:
        return own(write, output)
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
        if output.key is not None:
            if not isinstance(write, KeyedWrite):
                write = await asyncio.to_thread(KeyedWrite.of, self, write, output, prior)
            if output.is_partition_set:
                return await self._store_set(write, prior, scope, base, generation)
            return await self._store_keyed(write, prior, scope, base, generation)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_batch(write, prior, scope, base, generation)
        name = f"{base}@{generation}"
        version = await self._put(name, write)
        return Written(self._ref(scope, {"mode": "value", "path": name, "base": base}, version))

    async def _store_set(self, write: KeyedWrite, prior, scope, base, generation) -> Written:
        """A partition set: its element list, as one value."""

        elements = write.prepared.take(None)
        if not write.whole and prior is not None:
            drop = set(write.removes) | set(write.prepared.removes) | set(elements)
            elements = [e for e in await self._elements(prior) if e not in drop] + elements
        name = f"{base}@{generation}"
        version = await self._put(name, elements)
        return Written(self._ref(scope, {"mode": "set", "path": name, "base": base}, version))

    async def _store_keyed(self, write: KeyedWrite, prior, scope, base, generation) -> Written:
        """One object per key and version: the keys the write changes, each
        named by the version the key index will hold, so no object goes
        unnamed — only their groups are read from the write. Removed keys
        need no write: the index stops naming them, and `discard` deletes
        what nothing reads."""

        async def put(entry) -> None:
            key, version, group = entry
            await self._put(self.key_name(base, key, version, generation), group)

        async for page in write.pages():
            await self._many(put, page)
        handle = {"mode": "keyed", "path": base, "key": scope.output.key}
        return Written(self._ref(scope, handle, write.version(prior)))

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
            # The ref's batches run first..last without a gap: every one a commit wrote.
            if missing := [b for b in range(lo, min(hi, last) + 1) if b not in names]:
                raise StoreError(f"{ref.output}: batch {missing[0]} of {base} is gone")
            batches = await self._many(self._found, [names[b] for b in sorted(names)])
            return _materialize([item for b in batches for item in b], t)
        if mode == "keyed":
            if not isinstance(selection, Keys):
                raise StoreError(
                    f"{ref.output}: a keyed read names its objects from the key index: it takes Keys"
                )
            keys = list(selection.revisions)
            found = await self._many(
                lambda k: self._found(self.key_name(base, k, *selection.revisions[k]), k), keys
            )
            content = dict(zip(keys, found, strict=True))
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
        return list(await self._found((ref.handle or {}).get("path", "")))

    async def _found(self, base: str, key: str | None = None):
        """The object at `base`, which a commit names: gone, it is an error,
        never an empty read."""

        value = await self._get(base)
        if value is MISSING:
            what = f"key {key!r}: " if key is not None else ""
            raise StoreError(f"{what}{base} is gone")
        return value

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
