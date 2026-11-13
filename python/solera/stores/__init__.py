"""Store protocol and the built-in stores (§3, §4). Runs in the worker."""

from __future__ import annotations

import asyncio
import functools
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


class PublicationRefused(StoreError):
    """A full run's publication (`full_run_ended`) the data refuses: its rows
    break a constraint only the published rows can check (a unique index
    spanning partitions, a foreign key). Nothing outside changed; it can
    succeed only once the data or the definition changes, so the engine
    retries it after the output's next commit, never on a timer
    (docs/tracking-design.md §8, D211)."""


class RunLost(StoreError):
    """A write of a full run whose build is gone (a migration dropped it, or
    a restore lost it), and the rows it held with it. Nothing changed; it
    can never succeed, and `full_run_ended` for the run says not
    published: the engine starts the partition's full run over
    (docs/stores.md, "PostgresStore: the version check, and full runs
    built beside")."""


class SourceBehind(StoreError):
    """A load by `Keys` answered without a key the index names: a
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
class WriteContext:
    """A write partition (§9): `attempt` is the writing attempt's id. `reset`
    says the write starts the content over (a full run): `prior` still says
    where the content is, but nothing of it is kept. What a keyed write
    changes is the write's own (`KeyedWrite`). `home` is the name the
    output's life began under: a store keeps every partition of one life in
    one place derived from it, so a renamed output's new partitions go
    where its old ones are (K25); `None` means the output's own name."""

    output: Output
    partition: str
    home: str | None = None
    attempt: str | None = None
    reset: bool = False
    # The attempt's generation and worker, for a `fenced` store to check
    # (docs/lifecycle.md §9.7); `None` outside an attempt.
    generation: int | None = None
    worker_id: str | None = None
    # D211, the new core's writes only (`builds`): a full run builds beside the
    # published rows. `build_run` is the run the log names, by the version of
    # the attempt that wrote its clear: its full run in progress, else the run
    # whose end the store was not yet told (`full_run_ended`); a newer run's
    # journaled clear drops the older debt, so never both. Today's path leaves
    # `builds` off and writes in place; the flag goes at the switch to the new
    # core (B11).
    build_run: int | None = None
    builds: bool = False


Version = int | str
"""A key's version: the generation of the attempt that wrote it (an `int`),
or a source's own word (a `str`). Compared only for equality."""


@dataclass(frozen=True)
class Changed:
    """What a keyed write changed (docs/stores.md, "What a write and a load
    say"): every key it `wrote`, all at the writing attempt's version, and
    every key it `removed` — each sorted by UTF-8 bytes, in chunks (lists of
    keys) pulled once, after `store` returned. `whole`: the write was the
    partition's entire content, so every key it did not write is gone as
    well; the engine finds those in its index, and `removed` may leave them
    out. A key named in `removed` that the partition did not hold changed
    nothing: the engine's index says which did."""

    wrote: Iterable[list[str]]
    removed: Iterable[list[str]] = ()
    whole: bool = False


@dataclass(frozen=True)
class Written:
    """What a store wrote: a ref to the new content, which the worker gives
    the attempt's generation. `keys` is only for writes the worker never
    sees as rows (an `Opaque` write, read by the store itself): the keys the partition
    holds now, sorted by their UTF-8 bytes, in chunks — lists of keys — the
    worker pulls one at a time after `store` returned. For every other
    write the worker reads the keys itself (§6, §9).

    `changed` is the new contract's answer, beside it: what a keyed write
    changed (`Changed`), for every keyed write the store takes as given
    (`KeyedWrite.changed`); None for an unkeyed output."""

    ref: Ref
    keys: Iterable | None = None
    changed: Changed | None = None


@dataclass(frozen=True)
class Read:
    """What `read` read (docs/stores.md, "What a write and a load say"):
    the `value`, as `load` gives it, and the versions it was read at. A
    keyed read gives `versions`, key → the version read: with `Keys`, every
    planned key, None for one the store did not hold; whole, every key read.
    An unkeyed read gives `version`, the greatest version among what it read
    (None when nothing was). A read of a `Ref` reads nothing and says nothing."""

    value: Any
    versions: Mapping[str, Version | None] | None = None
    version: Version | None = None


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
    async def load(self, ref: Ref, t: type, selection: Keys | None) -> Any: ...
    async def read(self, ref: Ref, t: type, selection: Keys | None) -> Read: ...

    async def cleanup(
        self, output: Output, *, home=None, partition=None, key=None, generation=None, before=None
    ) -> None: ...

    # fenced:    async def acquire(self, context: WriteContext, prior: Ref | None) -> None
    #            def keys(self, ref: Ref, among: list[str] | None) -> Iterable[list[str]]
    # in place:  async def written_at(self, versions, prior, context) -> Iterable[list[str]]
    #            async def full_run_ended(self, output, partition, run, newest, *, prior=None, home=None) -> bool
    #            (D196, D211: docs/stores.md, "The contract" and "PostgresStore: the version check,
    #            and full runs built beside")


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
    lists of them; a dynamic partitions its elements, a list or a set; any
    other output a value, anything — unless not `values`, for a store of rows only.
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
    its keys, and the store writes its chunks from it (`KeyedWrite.chunks`).

    `rows` are its keys, sorted, each the group of the rows that carry it
    (`solera.keys.Rows`): only the key of a row is read (docs/versions.md).
    `take(indices)` gives the write's rows at those indices — a sequence of
    ints: a `range` when they are a run, else a `memoryview` — or every row
    for None, as the store persists them: mappings for
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


class Chunk:
    """A chunk of a keyed write's keys to write (`KeyedWrite.chunks`), read
    from the write as columns: `keys`, sorted; `indices`, the positions of
    every key's rows among the rows the write was read from, a key's after
    the previous key's, each key's in the write's order — a `range` when
    they are a run, else a `memoryview` of unsigned ints; `rows`, those rows
    as `Prepared.take` gives them; and `groups()`, each key with its group,
    for a store that writes a key at a time. Each is read from the write
    when the store first asks for it, so a store pays only for what it
    reads: one that writes the producer's Arrow table as it is takes
    `indices` from it, and no row becomes a Python object. Each is taken on
    the thread that asks: an async store asks on a thread of its own."""

    def __init__(self, prepared: Prepared, span: Any):
        self._prepared, self._span = prepared, span

    def __len__(self) -> int:
        return len(self._span)

    @functools.cached_property
    def keys(self) -> list[str]:
        return self._span.keys()

    @functools.cached_property
    def indices(self) -> Sequence[int]:
        return self._span.rows()

    @functools.cached_property
    def rows(self) -> list:
        return self._prepared.take(self.indices)

    def groups(self) -> list[tuple[str, Any]]:
        """Each key with its group, as its store writes it: the list of its
        rows, a `keyed=True` output's value, or a dynamic partitions's
        element."""

        output, keys, rows = self._prepared.output, self.keys, self.rows
        if output.is_dynamic_partitions:
            return list(zip(keys, rows, strict=True))
        if output.key == KEYS:
            return [(key, value) for key, (_, value) in zip(keys, rows, strict=True)]
        ends = self._span.ends()
        return [(key, rows[a:b]) for key, a, b in zip(keys, [0, *ends][:-1], ends, strict=True)]


@dataclass(frozen=True)
class KeyedWrite:
    """A keyed output's write as its store takes it: read once
    (`prepared`), resolved against the key index (§4, §6). A store needs
    three things of it: whether it is a `reset`, the partition's whole
    content (clear the partition first), its `removes`, and its `chunks()` — the keys to write
    and their rows, a `Chunk` at a time.

    `upserts` are the keys to write: a collection of them; a
    `solera.keys.layers.DeltaKeys` reading them from the commit's delta
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
        """The keys to write, sorted, `size` keys at a time: a `Chunk` each,
        whose rows are taken from the write only when the store asks."""

        if self.upserts is None or isinstance(self.upserts, Collection):
            chunks = self.iter_chunks(size)
            while (chunk := await asyncio.to_thread(next, chunks, None)) is not None:
                yield chunk
            return
        async for keys in self.upserts.chunks(size):
            yield await asyncio.to_thread(self._chunk, keys)

    def iter_chunks(self, size: int = 100_000):
        """`chunks()`, for a store that writes on a thread of its own. Not for
        a `DeltaKeys` selection: only immutable stores get one, and they
        read it asynchronously."""

        if self.upserts is None:
            for span in self.prepared.rows.spans(size):
                yield Chunk(self.prepared, span)
        elif isinstance(self.upserts, Collection):
            keys = sorted(self.upserts)
            for i in range(0, len(keys), size):
                yield self._chunk(keys[i : i + size])
        else:
            raise StoreError(f"{self.prepared.output.name}: a delta's selection is paged asynchronously")

    def _chunk(self, keys: list[str]) -> Chunk:
        try:
            return Chunk(self.prepared, self.prepared.rows.span(keys))
        except KeyError as e:
            raise StoreError(
                f"{self.prepared.output.name}: asked to write key {e.args[0]!r}, which the write does not hold"
            ) from None

    def changed(self, size: int = 100_000) -> Changed | None:
        """What this write changes once its store wrote it as given
        (`Written.changed`): the keys it writes — `upserts`, or every key of
        the write — and its `removes`; a reset is the partition's whole
        content. None for a write the worker resolved against the index in a
        form whose keys only the index knows: a `DeltaKeys` selection, or a
        reset that keeps the keys it does not write (`upserts` given)."""

        if self.upserts is not None and (self.reset or not isinstance(self.upserts, Collection)):
            return None
        return Changed(self._wrote(size), _chunks(sorted(self.removes, key=_utf8), size), self.reset)

    def _wrote(self, size: int):
        if self.upserts is not None:
            yield from _chunks(sorted(self.upserts, key=_utf8), size)
            return
        from ..keys.layers import key_str

        for keys, _ in self.prepared.rows.chunks(size):
            yield [key_str(k) for k in keys]


def _utf8(key: str) -> bytes:
    return key.encode("utf-8", "surrogateescape")


def _chunks(keys: list[str], size: int) -> Iterable[list[str]]:
    return (keys[i : i + size] for i in range(0, len(keys), size))


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
            if not isinstance(payload, list):  # its rows are mappings: `Rows.records` checks each
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
    return functools.partial(take_list, items)


def take_list(items: list, rows: Sequence[int] | None) -> list:
    """`items` at `rows` (`Prepared.take`): a slice of a run, else one
    native gather of a `memoryview` of ints."""

    if rows is None:
        return items
    if isinstance(rows, range) and rows.step == 1:
        return items[rows.start : rows.stop]
    if isinstance(rows, memoryview):
        from .._native import gather

        return gather(items, rows)
    return [items[r] for r in rows]


def prepare_for(store: Any, write: Any, output: Output) -> Prepared:
    """`store.prepare`, or the default `prepare`."""

    own = getattr(store, "prepare", None)
    return own(write, output) if own is not None else prepare(write, output)


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


def rebuild(described: dict, home: str | None = None):
    """A built-in store from its description (`describe()`, in the manifest),
    in a worker: a cleanup task's store after the project stopped declaring
    it (K25). `env:` values are resolved as the store uses them; `home`, the
    project's directory, is a FileStore's default place."""

    name, config = described["class"], described["config"]
    if name in ("FileStore", "S3Store"):
        from .files import FileStore, S3Store

        if name == "S3Store":
            return S3Store(config["url"], **config["options"])
        store = FileStore(config["path"])
        store.home = home
        return store
    if name == "PostgresStore":
        from solera_postgres import PostgresStore

        return PostgresStore(**config)
    raise StoreError(f"no built-in store {name!r}")
