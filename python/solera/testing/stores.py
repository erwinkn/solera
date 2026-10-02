"""Conformance scenarios for a store (docs/stores.md, "Scenarios"): the
sequences of writes, loads, acquisitions and discards a store of each kind
must answer exactly as stated. Each scenario is an async function of a
`Harness` that raises `AssertionError` when the store answers otherwise;
the kit needs no test framework. With pytest:

    from solera.testing.stores import Harness, scenarios

    @pytest.fixture
    def harness():
        return Harness(MyStore(dsn), lambda **decl: Output(f"t_{uuid.uuid4().hex[:12]}", store="mine", **decl))

    @pytest.mark.parametrize("scenario", scenarios(MyStore), ids=lambda s: s.__name__)
    async def test_conformance(harness, scenario):
        await scenario(harness)

The kit drives the store as the harness does: keyed writes arrive as a
`KeyedWrite`, resolved against what the engine's key index would hold
(the kit keeps that `Ledger` itself), keyed loads name their keys, and an
unkeyed output's value arrives as it is, a plain list."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..sdk import Output, Ref
from ..stores import Batches, KeyedWrite, Keys, Patch, Scope, StoreError, prepare_for


@dataclass
class Harness:
    """What a scenario needs of the store under test. `output(**decl)` makes
    a fresh `Output` on it — a new name each call, so scenarios never share
    data — with the declaration given (`key="id"`, `incremental=True`, or
    none). `hold(scope)`, which a fenced store's
    harness must give, is an async context manager that opens a write
    transaction of `scope` holding its fence until the block ends, then
    commits it: a newer writer waits for it."""

    store: Any
    output: Callable[..., Output]
    hold: Callable[[Scope], Any] | None = None

    def __post_init__(self):
        if self.store.writes == "fenced" and self.hold is None:
            raise ValueError("a fenced store's Harness needs hold: a write transaction held open")


@dataclass
class Ledger:
    """What the engine's key index would hold for one output scope: each
    live key's version — the generation that wrote it."""

    entries: dict[str, int] = field(default_factory=dict)

    def keys(self) -> Keys:
        return Keys(dict(self.entries))


def scope(
    out: Output,
    generation: int,
    invocation: str = "i",
    batch: int | None = None,
    reset: bool = False,
    partition: str = "",
) -> Scope:
    return Scope(
        output=out,
        partition=partition,
        batch=batch,
        reset=reset,
        attempt=f"kit-{generation}",
        generation=generation,
        invocation=invocation,
    )


async def write(
    h: Harness,
    out: Output,
    rows: list[dict],
    generation: int,
    ledger: Ledger,
    prior: Ref | None = None,
    *,
    remove=(),
    patch: bool = False,
    changed: bool = False,
    invocation: str = "i",
    partition: str = "",
) -> Ref:
    """Write `rows` to a keyed output as the harness would — the scope's whole
    content, or (`patch`) its keys and `remove` — and record in `ledger`
    what the index then holds: every key written, at `generation`. With
    `changed`, a replacement is resolved against the ledger as the worker
    resolves it against the index: its keys are written, the keys it leaves
    out removed, and nothing else cleared."""

    store = h.store
    written_keys = sorted({str(r["id"]) for r in rows})
    if changed:
        prepared = prepare_for(store, rows, out)
        gone = frozenset(set(ledger.entries) - set(written_keys))
        keyed = KeyedWrite(prepared, upserts=frozenset(written_keys), removes=gone, value=rows)
        written = await store.store(keyed, prior, scope(out, generation, invocation, partition=partition))
        for key in gone:
            del ledger.entries[key]
        ledger.entries.update(dict.fromkeys(written_keys, generation))
        return written.ref
    if patch:
        prepared = prepare_for(store, Patch(rows, remove=list(remove)), out)
        keyed = KeyedWrite(
            prepared, upserts=frozenset(written_keys), removes=frozenset(map(str, remove)), value=rows
        )
    else:
        prepared = prepare_for(store, rows, out)
        keyed = KeyedWrite(prepared, whole=True, value=rows)
    written = await store.store(keyed, prior, scope(out, generation, invocation, partition=partition))
    if not patch:
        ledger.entries.clear()
    for key in remove:
        ledger.entries.pop(str(key), None)
    ledger.entries.update(dict.fromkeys(written_keys, generation))
    return written.ref


async def rows(h: Harness, ref: Ref, selection) -> list[tuple[str, str]]:
    """The rows a load gives, as a sorted multiset: a duplicate row shows."""

    found = await h.store.load(ref, list[dict], selection)
    return sorted((str(r["id"]), str(r["v"])) for r in found)


async def now(h: Harness, ref: Ref, ledger: Ledger) -> list[tuple[str, str]]:
    """The scope's content as a reader of `ref` gets it: an immutable store's
    through the keys the index names; a fenced store's whole, as it is."""

    return await rows(h, ref, ledger.keys() if h.store.writes == "immutable" else None)


def keyed(h: Harness) -> Output:
    return h.output(key="id")


# -- every store ---------------------------------------------------------------------------


async def a_replacement_is_the_scopes_whole_content(h: Harness) -> None:
    """Write {a, b}, then replace it with {b, c}: the scope holds b and c; a
    replacement drops the keys it does not name."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}], 1, ledger)
    assert await now(h, first, ledger) == [("a", "1"), ("b", "1")]
    second = await write(h, out, [{"id": "b", "v": "1"}, {"id": "c", "v": "1"}], 2, ledger, first)
    assert await now(h, second, ledger) == [("b", "1"), ("c", "1")]


async def a_patch_changes_only_its_keys(h: Harness) -> None:
    """{a, b}, then a patch writing b at 2 and c, removing a: b at 2 and c;
    every key the patch does not name stays as it was."""

    out, ledger = keyed(h), Ledger()
    first = await write(
        h, out, [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}, {"id": "d", "v": "1"}], 1, ledger
    )
    rows_b_c = [{"id": "b", "v": "2"}, {"id": "c", "v": "1"}]
    second = await write(h, out, rows_b_c, 2, ledger, first, remove=["a"], patch=True)
    assert await now(h, second, ledger) == [("b", "2"), ("c", "1"), ("d", "1")]


async def an_empty_replacement_holds_no_key(h: Harness) -> None:
    """{a}, then a replacement with zero rows: the scope holds nothing."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 1, ledger)
    second = await write(h, out, [], 2, ledger, first)
    assert ledger.entries == {} and await now(h, second, ledger) == []


async def a_write_repeated_by_its_attempt_lands_once(h: Harness) -> None:
    """The same attempt — one generation, one invocation — writes the same
    content twice (a retried call): the same ref, the same content."""

    out, ledger = keyed(h), Ledger()
    content = [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]
    first = await write(h, out, content, 4, ledger)
    again = await write(h, out, content, 4, ledger)
    assert again == first
    assert await now(h, again, ledger) == [("a", "1"), ("b", "1")]


async def batches_append_and_load_by_range(h: Harness) -> None:
    """An unkeyed incremental output: batch 3, then batch 4, read whole and
    by range."""

    out = h.output(incremental=True)
    first = await h.store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out, 1, batch=3))
    second = await h.store.store(Patch([{"id": "b", "v": "1"}]), first.ref, scope(out, 2, batch=4))
    assert await rows(h, second.ref, None) == [("a", "1"), ("b", "1")]
    assert await rows(h, second.ref, Batches(4, 4)) == [("b", "1")]


async def a_batch_written_again_lands_once(h: Harness) -> None:
    """Batch 3, written twice by its attempt (a retried call): its rows once."""

    out = h.output(incremental=True)
    first = await h.store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out, 1, batch=3))
    again = await h.store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out, 1, batch=3))
    second = await h.store.store(Patch([{"id": "b", "v": "1"}]), first.ref, scope(out, 2, batch=4))
    second = await h.store.store(Patch([{"id": "b", "v": "1"}]), first.ref, scope(out, 2, batch=4))
    assert await rows(h, again.ref, None) == [("a", "1")]
    assert await rows(h, second.ref, None) == [("a", "1"), ("b", "1")]


async def a_full_run_starts_the_batches_over(h: Harness) -> None:
    """Batch 3, then batch 4 reset (a full run): only batch 4."""

    out = h.output(incremental=True)
    first = await h.store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out, 1, batch=3))
    reset = await h.store.store(Patch([{"id": "b", "v": "1"}]), first.ref, scope(out, 2, batch=4, reset=True))
    assert await rows(h, reset.ref, None) == [("b", "1")]


async def a_replacement_resolved_writes_its_keys_and_removes_the_rest(h: Harness) -> None:
    """{a, b, c}, then the replacement {a, b at 2} as the worker resolves it
    — not a whole write: its keys, and the keys it removes. a and b written
    at generation 2, c removed."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": k, "v": "1"} for k in "abc"], 1, ledger)
    second = await write(
        h, out, [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], 2, ledger, first, changed=True
    )
    assert ledger.entries == {"a": 2, "b": 2}
    assert await now(h, second, ledger) == [("a", "1"), ("b", "2")]


# -- immutable stores ----------------------------------------------------------------------


async def an_unkeyed_output_is_its_plain_rows(h: Harness) -> None:
    """An unkeyed output's value arrives as the producer returned it — a
    plain list of rows, no `KeyedWrite` — and is its whole content: the
    next write replaces it."""

    out = h.output()
    assert h.store.can_store(list[dict], out), "can_store refuses rows for an unkeyed output"
    first = (await h.store.store([{"id": "a", "v": "1"}], None, scope(out, 1))).ref
    assert await rows(h, first, None) == [("a", "1")]
    second = (await h.store.store([{"id": "b", "v": "1"}, {"id": "c", "v": "1"}], first, scope(out, 2))).ref
    assert await rows(h, second, None) == [("b", "1"), ("c", "1")]


async def partitions_never_touch_each_other(h: Harness) -> None:
    """Two partitions of one output: replacing one leaves the other as it
    was. (A store that shares one table among partitions declares
    `shared_table`; its outputs get a `partition_column`.)"""

    shared = getattr(h.store, "shared_table", False)
    out = h.output(key="id", **({"partition_column": "part"} if shared else {}))
    one, two = Ledger(), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 1, one, partition="p1")
    other = await write(h, out, [{"id": "b", "v": "1"}], 2, two, partition="p2")
    first = await write(h, out, [{"id": "c", "v": "1"}], 3, one, first, partition="p1")
    assert await now(h, first, one) == [("c", "1")]
    assert await now(h, other, two) == [("b", "1")]


async def a_pinned_read_returns_its_version(h: Harness) -> None:
    """a at 1 (generation 5), then a at 2 (generation 9): the ref pinned
    before reads a at 1, the newer one a at 2."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 5, ledger)
    pinned = ledger.keys()
    second = await write(h, out, [{"id": "a", "v": "2"}], 9, ledger, first)
    assert await rows(h, first, pinned) == [("a", "1")]
    assert await now(h, second, ledger) == [("a", "2")]


async def discarding_never_takes_what_is_read(h: Harness) -> None:
    """a at 1 is superseded by a at 2, and an abandoned attempt (generation
    7) wrote b. Discarding the superseded and the abandoned names — twice,
    and names never written — leaves the current content whole."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 5, ledger)
    abandoned = Ledger()
    await write(h, out, [{"id": "b", "v": "1"}], 7, abandoned, first, patch=True)  # never committed
    second = await write(h, out, [{"id": "a", "v": "2"}], 9, ledger, first)
    items = [("key", "a", 5), ("key", "b", 7), ("key", "z", 8)]
    for _ in range(2):
        await h.store.discard(scope(out, 10), second, items)
    assert await now(h, second, ledger) == [("a", "2")]


# -- fenced stores -------------------------------------------------------------------------


async def a_stale_writer_is_refused(h: Harness) -> None:
    """Writer 5 wrote; writer 9 acquired and wrote; writer 5 writes again:
    refused, and writer 9's content is intact."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 5, ledger)
    await h.store.acquire(scope(out, 9), first)
    second = await write(h, out, [{"id": "a", "v": "2"}], 9, ledger, first)
    with _refused():
        await write(h, out, [{"id": "a", "v": "0"}], 5, Ledger(), first)
    assert await now(h, second, ledger) == [("a", "2")]


async def one_generation_admits_one_invocation(h: Harness) -> None:
    """Generation 9 acquired by invocation x: invocation y of the same
    generation (a duplicate) can neither acquire nor write; x acquiring
    again is its own retry."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 5, ledger)
    await h.store.acquire(scope(out, 9, "x"), first)
    with _refused():
        await h.store.acquire(scope(out, 9, "y"), first)
    with _refused():
        await write(h, out, [{"id": "a", "v": "0"}], 9, Ledger(), first, invocation="y")
    await h.store.acquire(scope(out, 9, "x"), first)
    second = await write(h, out, [{"id": "a", "v": "2"}], 9, ledger, first, invocation="x")
    assert await now(h, second, ledger) == [("a", "2")]


async def a_first_write_acquires(h: Harness) -> None:
    """Acquiring a slice that holds nothing yet succeeds; the first write (3)
    takes it, and an older writer (2) is refused after."""

    out, ledger = keyed(h), Ledger()
    await h.store.acquire(scope(out, 3), None)
    first = await write(h, out, [{"id": "a", "v": "1"}], 3, ledger)
    with _refused():
        await write(h, out, [{"id": "a", "v": "0"}], 2, Ledger(), first)
    assert await now(h, first, ledger) == [("a", "1")]


async def the_next_attempt_replaces_what_a_dead_writer_left(h: Harness) -> None:
    """Writer 5 died after its write of c landed, uncommitted. Writer 9
    acquires and writes the scope's content, {a, b}: c is gone, and writer
    5 can write nothing more."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 1, ledger)
    await write(h, out, [{"id": "c", "v": "1"}], 5, Ledger(), first, patch=True)  # landed, never committed
    await h.store.acquire(scope(out, 9), first)
    second = await write(h, out, [{"id": "a", "v": "2"}, {"id": "b", "v": "1"}], 9, ledger, first)
    assert await now(h, second, ledger) == [("a", "2"), ("b", "1")]
    with _refused():
        await write(h, out, [{"id": "c", "v": "2"}], 5, Ledger(), first, patch=True)


async def a_scope_says_which_keys_it_holds(h: Harness) -> None:
    """`keys(ref, among)`: the keys the scope holds, sorted by their bytes —
    among those given, or all of them — never a value (docs/versions.md
    §5). Writer 5's patch of c landed and it died: the scope holds c."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": k, "v": "1"} for k in ("b", "a", "é", "B")], 1, ledger)
    await write(h, out, [{"id": "c", "v": "1"}], 5, Ledger(), first, patch=True)  # landed, never committed

    def held(among):
        return [k for chunk in h.store.keys(first, among) for k in chunk]

    assert held(None) == sorted(["a", "b", "c", "é", "B"], key=str.encode)
    assert held(["c", "d", "a"]) == ["a", "c"]
    assert held([]) == []


async def a_newer_writer_waits_for_an_open_older_one(h: Harness) -> None:
    """Writer 5's transaction is open: writer 9's acquisition waits for it to
    commit, then takes over; writer 5's next write is refused."""

    out, ledger = keyed(h), Ledger()
    first = await write(h, out, [{"id": "a", "v": "1"}], 5, ledger)
    async with h.hold(scope(out, 5)):
        # A store's calls must not block the event loop: the older writer's
        # transaction commits on this loop while the newer one waits.
        acquiring = asyncio.create_task(h.store.acquire(scope(out, 9), first))
        await asyncio.sleep(0.3)
        assert not acquiring.done(), "the newer acquisition did not wait for the open older writer"
    await asyncio.wait_for(acquiring, 10)
    with _refused():
        await write(h, out, [{"id": "a", "v": "0"}], 5, Ledger(), first)


# -- stores that read the current rows (`reads`) -------------------------------------------


async def a_read_reports_the_generation_it_saw(h: Harness) -> None:
    """Writer 3 wrote two outputs, {a: 1} and {b: 1}; writer 5 acquired the
    first and wrote nothing. A reader reads the first: a=1, generation 3 —
    an acquisition is no write. Writer 7 writes both and commits; the
    reader, at its one moment, still reads b=1 and a=1, generation 3 each:
    what it reports is what it read. A new reader reads generation 7's."""

    first, second = keyed(h), keyed(h)
    one = await write(h, first, [{"id": "a", "v": "1"}], 3, Ledger())
    two = await write(h, second, [{"id": "b", "v": "1"}], 3, Ledger())
    await h.store.acquire(scope(first, 5), one)
    async with h.store.reads() as reader:
        assert await _read(reader, one) == ([("a", "1")], 3)
        one = await write(h, first, [{"id": "a", "v": "2"}], 7, Ledger(), one)
        two = await write(h, second, [{"id": "b", "v": "2"}], 7, Ledger(), two)
        assert await _read(reader, two) == ([("b", "1")], 3)
        assert await _read(reader, one) == ([("a", "1")], 3)
    async with h.store.reads() as reader:
        assert await _read(reader, one) == ([("a", "2")], 7)
        assert await _read(reader, two) == ([("b", "2")], 7)


async def _read(reader, ref: Ref) -> tuple[list[tuple[str, str]], int | None]:
    found, generation = await reader.load(ref, list[dict], None)
    return sorted((str(r["id"]), str(r["v"])) for r in found), generation


@contextlib.contextmanager
def _refused():
    try:
        yield
    except StoreError:
        return
    raise AssertionError("the store accepted a write it must refuse")


EVERY = [
    a_replacement_is_the_scopes_whole_content,
    a_patch_changes_only_its_keys,
    an_empty_replacement_holds_no_key,
    a_write_repeated_by_its_attempt_lands_once,
    batches_append_and_load_by_range,
    a_batch_written_again_lands_once,
    a_full_run_starts_the_batches_over,
    a_replacement_resolved_writes_its_keys_and_removes_the_rest,
    an_unkeyed_output_is_its_plain_rows,
    partitions_never_touch_each_other,
]
IMMUTABLE = [a_pinned_read_returns_its_version, discarding_never_takes_what_is_read]
FENCED = [
    a_stale_writer_is_refused,
    one_generation_admits_one_invocation,
    a_first_write_acquires,
    the_next_attempt_replaces_what_a_dead_writer_left,
    a_scope_says_which_keys_it_holds,
    a_newer_writer_waits_for_an_open_older_one,
]
READS = [a_read_reports_the_generation_it_saw]


def scenarios(store: Any) -> list[Callable]:
    """The scenarios a store (or its class) must pass: every store's, its
    kind's, and a current-read store's (`reads`)."""

    found = EVERY + {"immutable": IMMUTABLE, "fenced": FENCED}[store.writes]
    return found + (READS if callable(getattr(store, "reads", None)) else [])
