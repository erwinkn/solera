"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pickle
import typing
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from ..sdk import KEYS, Output, Ref


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
    """PostgresStore only: materialize a query — a SELECT, VALUES or TABLE —
    into the output's table (§4). It is never run as a statement: UPDATE,
    DELETE and DDL are refused; a table changes through a `Migration`."""

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
    output's former names. `reset` says the write starts the content over
    (a full run): `prior` still says where the content is, but nothing of
    it is kept — a store's batches start over at `batch`. What a keyed write
    changes is the write's own (`KeyedWrite`)."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()
    reset: bool = False
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
    it, `solera.stores.prepare` reads plain Python: lists of mappings,
    by-key `{key: rows}`, dicts and elements; `solera.stores.frames` reads
    DataFrames and Arrow, for a store that takes them — and `stamped(output)`, the columns it adds to every
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


def takes(t: Any, output: Output, *, frames: bool = False, values: bool = True) -> bool:
    """Whether a producer annotated `t` returns a write `output` can take: the
    forms the framework defines, once, for a store's `can_store` (which
    registration asks). A `keyed=True` output takes a dict of values; a
    keyed rows output rows — a list of mappings — or rows by key, a dict of
    lists of them; a partition set its elements, a list or a set; an
    unkeyed incremental output a batch of rows, a list; any other output a
    value, anything — unless not `values`, for a store of rows only.
    `frames`: a DataFrame or an Arrow table wherever rows go. Unannotated
    (`t` None) is anything: what a write holds is checked when it is read."""

    if t is None:
        return True
    origin = typing.get_origin(t) or t
    listed = origin in (list, Sequence)
    rows = listed or (frames and _frames_can(t))
    # Keys are strings: a dict's key type, when it says one, is str.
    by_key = origin in (dict, Mapping) and (typing.get_args(t) or (str,))[0] in (str, Any)
    if output.key == KEYS:
        return by_key
    if output.is_partition_set:
        return listed or origin in (set, frozenset)
    if output.key is not None:
        return rows or by_key
    if output.incremental:
        return rows
    return values or rows


def _frames_can(t: Any) -> bool:
    from .frames import can_store

    return can_store(t)


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
    with their key. Each group is a list of mappings."""

    if output.key in (None, KEYS) or output.is_partition_set or not isinstance(write, Mapping):
        return None
    column, rows, empty = output.key, [], []
    for key, group in write.items():
        if not isinstance(key, str):
            raise WriteError(f"{output.name}: a by-key write takes str keys, got {type(key).__name__}")
        found = _rows(group, output.name)
        for row in found:
            if column in row and key_text(row[column]) != key:
                raise WriteError(f"{output.name}: a row of {key!r} carries {column}={row[column]!r}")
            row[column] = key
        rows.extend(found)
        if not found:
            empty.append(key)
    return rows, empty


def _rows(value: Any, name: str) -> list[dict]:
    """A group of plain rows, as a list of dicts."""

    if value is None:
        return []
    if isinstance(value, list) and all(isinstance(r, Mapping) for r in value):
        return [dict(r) for r in value]
    raise WriteError(f"{name}: expected rows (a list of mappings), got {type(value).__name__}")


@dataclass(frozen=True)
class Prepared:
    """A keyed write's content, read once (§4, §6). The key index resolves
    it, the store's version is computed from it, and the store writes its
    groups from it: what is hashed is what is stored.

    `rows` are its keys, sorted, each the group of the rows that carry it,
    with versions computed natively (`solera.keys.Rows`,
    docs/row-digest.md). `take(indices)` gives the write's rows at those
    indices — every row for None — as the store persists them: mappings for
    a rows output, `(key, value)` items for `keyed=True`, a partition set's
    elements; a store reading types of its own gives its own. A `Patch` also
    names the keys it `removes`, none of them written. `kinds`, when the
    reader knows them, are its columns' value kinds (`frames.KINDS`), for a
    store creating a table."""

    output: Output
    rows: Any
    take: Callable[[Sequence[int] | None], list]
    patch: bool = False
    removes: tuple[str, ...] = ()
    kinds: Mapping[str, str] | None = None

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

        from ..keys.index import key_str

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
            from ..keys.index import key_str

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
        return self.prepared.version(None if self.whole else prior)  # a whole write builds on nothing


def prepare(
    write: Any, output: Output, exclude: tuple[str, ...] = (), read: Callable | None = None
) -> Prepared:
    """A keyed write — the whole content, or a `Patch` — read once
    (`Prepared`). The default reads plain Python: a list of mappings, a
    by-key mapping (`{key: rows}`), a `keyed=True` output's dict, a
    partition set's elements. A store taking other types passes `read`:
    `read(content, output, exclude)` gives `(rows, take, empty, kinds)` for
    content it reads, or None to leave it to the default
    (`solera.stores.frames.read` reads DataFrames and Arrow). A row's digest
    leaves out its key column and the `exclude`d ones — columns its store
    adds, so a row digests the same as written and as read back."""

    from ..keys import Rows

    name = output.name
    patch = isinstance(write, Patch)
    content = write.rows if patch else write
    empty: list[str] = []
    kinds = None
    try:
        got = read(content, output, exclude) if read is not None and not output.is_partition_set else None
        if got is not None:
            rows, take, empty, kinds = got
        elif output.is_partition_set:
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
            if payload is None:
                payload = []
            if not isinstance(payload, list) or not all(isinstance(r, Mapping) for r in payload):
                raise WriteError(
                    f"{name}: this store reads plain rows (a list of mappings), got {type(payload).__name__}"
                )
            rows = Rows.records(payload, output.key, output.revision, list(exclude))
            take = _taker(payload)
        removes = ()
        if patch:
            gone = set(map(key_text, write.remove)) | set(empty)
            removes = tuple(k for k in sorted(gone) if k not in rows)
    except KeyError as e:
        raise WriteError(f"{name}: row lacks the declared key column {output.key!r}") from e
    except ValueError as e:  # a key that is not one, Arrow data without the columns, a value with no digest
        raise WriteError(f"{name}: {e}") from e
    return Prepared(output, rows, take, patch, removes, kinds)


def _taker(items: list) -> Callable:
    return lambda rows: items if rows is None else [items[r] for r in rows]


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


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def by_key_type(t: Any) -> Any:
    """`T` of a `dict[str, T]` load — each key's group on its own, as `Each`
    reads a page (per-key §5) — else `MISSING`."""

    if typing.get_origin(t) in (dict, Mapping) and typing.get_args(t)[:1] == (str,):
        return typing.get_args(t)[1]
    return MISSING


from .files import FileStore as FileStore  # noqa: E402 — stores, on the core above
from .files import S3Store as S3Store  # noqa: E402
