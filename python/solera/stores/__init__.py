"""Store protocol and the built-in stores (§3, §4). Runs in the worker."""

from __future__ import annotations

import asyncio
import json
import os
import pickle
import typing
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from ..sdk import KEYS, Output, Ref, dict_arg


class StoreError(Exception):
    retryable = False


class WriteError(StoreError):
    """Malformed write: duplicate keys, wrong shape, disallowed op."""


class SourceBehind(StoreError):
    """A load by `Keys` answered without a key the index names (F33): a
    source read as it is now no longer holds a key its commits say it has.
    Nothing is delivered; the attempt retries under its budget, and the
    source's next commit, removing or restoring the key, settles it."""

    retryable = True


def missing_keys(key: str | None, value: Any, expected: Mapping[str, int]) -> list[str]:
    """Which of `expected` a load by `Keys(expected)` did not answer, sorted.
    Keys are read from what it answered: a by-key mapping's keys, a rows
    list's or a frame's `key` column; a type it cannot read is not checked."""

    got = _loaded_keys(value, key) if expected else None
    return [] if got is None else sorted(k for k in expected if k not in got)


def check_loaded(output: str, key: str | None, value: Any, expected: Mapping[str, int]) -> None:
    """Raise `SourceBehind` if a load by `Keys(expected)` lacks one of them."""

    for k in missing_keys(key, value, expected)[:1]:
        raise SourceBehind(f"{output}: the source index says {k}@{expected[k]} but the source has no {k}")


def _loaded_keys(value: Any, key: str | None) -> set[str] | None:
    if isinstance(value, Mapping):
        return {str(k) for k in value}
    if key is None:
        return None
    if isinstance(value, (list, tuple)):
        if not all(isinstance(r, Mapping) and key in r for r in value):
            return None  # rows without their key column: not readable here
        return {str(r[key]) for r in value}
    try:  # a frame: its key column
        column = value[key]
    except Exception:
        return None
    for to_list in ("to_pylist", "tolist", "to_list"):
        if hasattr(column, to_list):
            return {str(k) for k in getattr(column, to_list)()}
    return None


@dataclass(frozen=True)
class Patch:
    """Partial write: replace the named keys, delete `remove` (§4). For a
    keyed rows output, `rows` is the rows themselves, carrying their key
    column, or — by key — `{key: rows}`: each key's group, the key column
    stamped by the store. A key with no rows does not exist: given none, it
    is removed (docs/per-key-processing.md §6)."""

    rows: Any
    remove: Any = ()


class Opaque:
    """A write its store reads itself — a query it runs in place, say
    (`solera_postgres.Sql`): the worker never sees its rows. Of a keyed
    output only, the store reports the keys the partition holds after it
    wrote (`Written.keys`); a writer that dies after its gate leaves keys
    no one knows, so the next attempt takes the partition's keys whole
    (docs/versions.md §5). A store that takes one says so in `can_store`."""


@dataclass(frozen=True)
class Keys:
    """A selection passed to `store.load` (§4): `key -> generation`, as the
    key index holds them — the generation that last wrote each key, its
    version, from which an immutable store names the key's object without
    listing (docs/versions.md, lifecycle.md §9.8)."""

    generations: Mapping[str, int]


@dataclass(frozen=True)
class Commits:
    """An inclusive `[lo, hi]` commit-range selection passed to `store.load`
    on an unkeyed incremental output (§2.2)."""

    lo: int
    hi: int


@dataclass(frozen=True)
class WriteContext:
    """A write partition (§9): `commit_number` is the engine-assigned commit number for
    incremental outputs, `attempt` the writing attempt's id. `reset` says the write starts the content over
    (a full run): `prior` still says where the content is, but nothing of
    it is kept — a store's commits start over at `commit_number`. What a keyed write
    changes is the write's own (`KeyedWrite`). `home` is the name the
    output's life began under: a store keeps every partition of one life in
    one place derived from it, so a renamed output's new partitions go
    where its old ones are (K25); `None` means the output's own name."""

    output: Output
    partition: str
    home: str | None = None
    commit_number: int | None = None
    attempt: str | None = None
    reset: bool = False
    # The attempt's generation and worker, for a `fenced` store to check
    # (docs/lifecycle.md §9.7); `None` outside an attempt.
    generation: int | None = None
    worker_id: str | None = None


@dataclass(frozen=True)
class Written:
    """What a store wrote: a ref to the new content, which the worker gives
    the attempt's generation. `keys` is only for writes the worker never
    sees as rows (an `Opaque` write, read by the store itself): the keys the partition
    holds now, sorted by their UTF-8 bytes, in chunks — lists of keys — the
    worker pulls one at a time after `store` returned. For every other
    write the worker reads the keys itself (§6, §9)."""

    ref: Ref
    keys: Iterable | None = None


@runtime_checkable
class Store(Protocol):
    """A keyed output's write reaches `store` as a `KeyedWrite`: read once,
    resolved against the key index. A store may define how it is read,
    `prepare(write, output) -> Prepared` — for types of its own; without
    it, `solera.stores.prepare` reads plain Python: lists of mappings,
    by-key `{key: rows}`, dicts and elements; `solera.stores.frames` reads
    DataFrames and Arrow, for a store that takes them.

    `writes` says how a writer the engine gave up on is kept from writing
    over a newer one — every store declares one (docs/stores.md):
    `"immutable"`, it writes only names no other attempt uses; or
    `"fenced"`, it implements `acquire`, every write checks the attempt's
    generation atomically (`solera.fencing`), and `keys(ref, among)` says
    which keys the partition holds — for a repair after a writer died
    (docs/versions.md §5).

    Every store implements `cleanup(output, *, home, partition, key,
    generation, before)`: delete every object of the output life kept
    under `home` the pattern matches — any field left out matches anything,
    `before` is every generation older than it — idempotently. The engine
    asks only for what no pin can read (docs/stores.md § Cleanup).

    It also says what a load sees (docs/stores.md): an immutable store
    returns exactly the content a ref and selection pin; a fenced store its
    current rows, so a reader may see a newer write than it pinned.
    `solera.testing.stores` checks a store against the contract."""

    version: str = "1"
    ref_type: type[Ref] = Ref
    writes: str  # "immutable" or "fenced"

    def can_load(self, t: type | None, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, context: WriteContext) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Commits | None) -> Any: ...

    async def cleanup(
        self, output: Output, *, home=None, partition=None, key=None, generation=None, before=None
    ) -> None: ...

    # fenced:    async def acquire(self, partition: Partition, prior: Ref | None) -> None
    #            def keys(self, ref: Ref, among: list[str] | None) -> Iterable[list[str]]


def resolve_env(value: Any) -> Any:
    """`env:NAME` indirection for store/resource config, resolved in the worker.
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
    lists of them; a dynamic partitions its elements, a list or a set; an
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
    if output.is_dynamic_partitions:
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

    if output.key in (None, KEYS) or output.is_dynamic_partitions or not isinstance(write, Mapping):
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
    """A keyed write's content, read once (§4, §6): the key index resolves
    its keys, and the store writes its groups from it.

    `rows` are its keys, sorted, each the group of the rows that carry it
    (`solera.keys.Rows`): only the key of a row is read (docs/versions.md).
    `take(indices)` gives the write's rows at those
    indices — every row for None — as the store persists them: mappings for
    a rows output, `(key, value)` items for `keyed=True`, a dynamic partitions's
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
        `keyed=True` output's value, or a dynamic partitions's element."""

        try:
            rows, ends = self.rows.find(list(keys))
        except KeyError as e:
            raise StoreError(
                f"{self.output.name}: asked to write key {e.args[0]!r}, which the write does not hold"
            ) from None
        picked = self.take(rows)
        if self.output.is_dynamic_partitions:
            return picked
        if self.output.key == KEYS:
            return [value for _, value in picked]
        return [picked[a:b] for a, b in zip([0, *ends], ends, strict=False)]


@dataclass(frozen=True)
class KeyedWrite:
    """A keyed output's write as its store takes it: read once
    (`prepared`), resolved against the key index (§4, §6). A store needs
    three things of it: whether it is a `reset`, the partition's whole
    content (clear the partition first), its `removes`, and its `chunks()` — the keys to write,
    each with its group.

    `upserts` are the keys to write: a collection of them; a
    `solera.keys.index.DeltaKeys` reading them from the commit's delta
    files when there are too many to list; or None, every key of the write.
    `removes` are deleted, and every other key stays as it is — unless the
    write is a `reset`, the partition's whole content: then every key not in it
    goes, and `upserts`, if given, are only those the delta writes, all a
    store that keeps the others as they are needs to write. `value` is what
    the producer returned, for a store that writes it as it is."""

    prepared: Prepared
    upserts: Any = None
    removes: frozenset[str] = frozenset()
    reset: bool = False
    value: Any = None

    @classmethod
    def of(cls, store: Any, write: Any, output: Output, prior: Ref | None) -> KeyedWrite:
        """A write as a store takes it with no key index to resolve it
        against: a replacement, or a patch's every key and remove."""

        if isinstance(write, KeyedWrite):
            return write
        prepared = prepare_for(store, write, output)
        if not prepared.patch or prior is None:
            return cls(prepared, reset=True, value=write)
        return cls(prepared, removes=frozenset(prepared.removes), value=write)

    async def chunks(self, size: int = 100_000):
        """The keys to write, sorted, a chunk at a time: `(key, group)` each.
        Only a chunk's groups are taken from the write at once."""

        if self.upserts is None or isinstance(self.upserts, Collection):
            chunks = self.iter_chunks(size)
            while (chunk := await asyncio.to_thread(next, chunks, None)) is not None:
                yield chunk
            return
        async for chunk in self.upserts.chunks(size):
            yield await asyncio.to_thread(self._grouped, chunk)

    def iter_chunks(self, size: int = 100_000):
        """`chunks()`, for a store that writes on a thread of its own. Not for
        a `DeltaKeys` selection: only immutable stores get one, and they
        read it asynchronously."""

        if self.upserts is None:
            from ..keys.index import key_str

            for keys, _ in self.prepared.rows.chunks(size):
                yield self._grouped([key_str(k) for k in keys])
        elif isinstance(self.upserts, Collection):
            keys = sorted(self.upserts)
            for i in range(0, len(keys), size):
                yield self._grouped(keys[i : i + size])
        else:
            raise StoreError(f"{self.prepared.output.name}: a delta's selection is paged asynchronously")

    def _grouped(self, keys: list[str]) -> list:
        return list(zip(keys, self.prepared.groups(keys), strict=True))


def prepare(write: Any, output: Output, read: Callable | None = None) -> Prepared:
    """A keyed write — the whole content, or a `Patch` — read once
    (`Prepared`). The default reads plain Python: a list of mappings, a
    by-key mapping (`{key: rows}`), a `keyed=True` output's dict, a
    dynamic partitions's elements. A store taking other types passes `read`:
    `read(content, output)` gives `(rows, take, empty, kinds)` for content
    it reads, or None to leave it to the default
    (`solera.stores.frames.read` reads DataFrames and Arrow). A partition
    set's elements carry an empty version: listed again, they change
    nothing (docs/versions.md §2)."""

    from ..keys import Rows

    name = output.name
    patch = isinstance(write, Patch)
    content = write.rows if patch else write
    empty: list[str] = []
    kinds = None
    try:
        got = read(content, output) if read is not None and not output.is_dynamic_partitions else None
        if got is not None:
            rows, take, empty, kinds = got
        elif output.is_dynamic_partitions:
            partitions = [str(e) for e in content or ()]
            rows, take = Rows.keys(partitions, b""), _taker(partitions)
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
            rows = Rows.records(payload, output.key)
            take = _taker(payload)
        removes = ()
        if patch:
            gone = set(map(key_text, write.remove)) | set(empty)
            removes = tuple(k for k in sorted(gone) if k not in rows)
    except KeyError as e:
        raise WriteError(f"{name}: row lacks the declared key column {output.key!r}") from e
    except ValueError as e:  # a key that is not one, Arrow data without the key column
        raise WriteError(f"{name}: {e}") from e
    return Prepared(output, rows, take, patch, removes, kinds)


def _taker(items: list) -> Callable:
    return lambda rows: items if rows is None else [items[r] for r in rows]


def prepare_for(store: Any, write: Any, output: Output) -> Prepared:
    """`store.prepare`, or the default `prepare`."""

    own = getattr(store, "prepare", None)
    return own(write, output) if own is not None else prepare(write, output)


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


def by_key_type(t: Any) -> Any:
    """`T` of a `dict[str, T]` load — each key's group on its own, as per-key incremental
    reads a page (per-key §5) — else `MISSING`."""

    inner = dict_arg(t)
    return MISSING if inner is None else inner


from .files import FileStore as FileStore  # noqa: E402 — stores, on the core above
from .files import S3Store as S3Store  # noqa: E402
