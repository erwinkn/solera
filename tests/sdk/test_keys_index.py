"""The key index (docs/object-store-state.md §6), checked against a plain dict.

Every step of a random workload — patches, removals, full replacements,
span merges — must produce the delta a dict says it should, the index must
page back exactly the dict's content, and at every live endpoint its view
and `changes` must match the dict as it was (docs/key-index-design.md)."""

import asyncio
import random
from dataclasses import dataclass

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from obstore.store import MemoryStore
from solera.keys import Rows, SortedEntries
from solera.keys.index import CLASSES, IndexState, KeyIndex, Options, Span
from solera.keys.io import ObjectIO

from . import keys_reference as _python


@dataclass
class Written:
    """A delta's entries as its files hold them."""

    keys: list
    generations: list
    deleted: bytes
    payloads: list
    predecessors: list
    added: int
    removed: int

    def __len__(self):
        return len(self.keys)


def run(keys, payloads=None, removes=()):
    return SortedEntries.of(list(keys), None if payloads is None else list(payloads), list(removes))


def entries_of(delta):
    """Every entry of an unwritten delta: key, generation, deleted, payload, predecessor."""

    return [e for d in delta.files for e in _python.iter_file(d)]


def small_options(**kw):
    # Tiny files and blocks so a few thousand keys span several files and blocks.
    base = dict(block_size=512, max_file_bytes=16 * 1024, read_slack=4096)
    base.update(kw)
    return Options(**base)


def filtered_options(**kw):
    # Nothing is small enough to read whole: every span goes through its filters.
    return small_options(whole_threshold=0, small_file=0, **kw)


def streamed_options(**kw):
    # Every patch streams the whole index.
    return small_options(stream_density=0.0, **kw)


def switching_options(**kw):
    # Patches go through the filters, then stream as soon as one block needs reading.
    return small_options(whole_threshold=0, small_file=0, stream_reads=0.0, **kw)


class Harness:
    """Commits against a dict, and keeps every object a write created —
    `(key, generation)` — to check that each one superseded is named for
    collection exactly once it is, by the delta that superseded it: writes
    are exact, so its predecessor is always named. A key written with a
    payload (a source's version) equal to its entry's is unchanged; any
    other write changes it. `endpoints` are the live endpoints merges keep
    (a reader's next commit, each reserved at head + 1); `before[c]` is the
    dict as it was before commit `c`."""

    def __init__(self, options):
        self.io = ObjectIO(MemoryStore())
        self.options = options
        self.state = IndexState()
        self.model: dict[bytes, tuple[int, bytes | None]] = {}  # key -> (generation, payload)
        self.commit_number = 0
        self.created: set[tuple] = set()
        self.named: set[tuple] = set()  # superseded objects a delta named
        self.routes: list[str] = []
        self.endpoints: set[int] = set()
        self.before: dict[int, dict] = {}
        self.merges = 0

    def index(self):
        return KeyIndex(self.io, "keys/out/p", self.state, self.options)

    async def _read(self, idx, files):
        return [
            e for f in files for e in _python.iter_file(await self.io.read_whole(idx.path(f.name), f.size))
        ]

    async def commit(self, keys, payloads=None, removes=(), replace=False):
        payloads = list(payloads) if payloads is not None else [None] * len(keys)
        before = dict(self.model)
        idx = self.index()
        gen = self.commit_number + 1
        if replace:
            items = list(zip(keys, payloads, strict=True))
            random.Random(len(items)).shuffle(items)
            files, changed = await idx.replace(
                Rows.pairs(items), self.commit_number, f"a{self.commit_number}", collect=10**6, generation=gen
            )
        else:
            files, changed = await idx.resolve(
                run(keys, payloads, removes),
                commit_number=self.commit_number,
                attempt=f"a{self.commit_number}",
                generation=gen,
                collect=10**6,
            )
            if self.state.files:
                self.routes.append(idx.route)
        written = await self._read(idx, files.files)
        delta = Written(
            *map(list, zip(*written, strict=True)) if written else ([],) * 5,
            files.added,
            files.removed,
        )
        delta.deleted = bytes(delta.deleted)
        assert changed == ([e[0] for e in written if not e[2]], [e[0] for e in written if e[2]])
        # What the dict says changed.
        if replace:
            after = {k: (gen, p) for k, p in zip(keys, payloads, strict=True)}
        else:
            after = dict(before)
            after.update((k, (gen, p)) for k, p in zip(keys, payloads, strict=True))
            for k in removes:
                after.pop(k, None)
        for k in after:
            if k in before and before[k][1] is not None and before[k][1] == after[k][1]:
                after[k] = before[k]  # at its version: unchanged, the object stays
        expect = {k: e for k, e in after.items() if before.get(k) != e}
        expect_rm = {k for k in before if k not in after}
        got = {
            k: (g, p)
            for k, g, d, p in zip(delta.keys, delta.generations, delta.deleted, delta.payloads, strict=True)
            if not d
        }
        got_rm = {k for k, d in zip(delta.keys, delta.deleted, strict=True) if d}
        assert got == expect, "upserts"
        assert got_rm == expect_rm, "removals"
        assert delta.keys == sorted(delta.keys)
        assert all(g == gen for g in delta.generations)
        for k, p in zip(delta.keys, delta.predecessors, strict=True):
            if p is not None:
                assert k in before and p == before[k][0], "a predecessor names what the key held"
                self.named.add((k, p))
            elif k in before:
                raise AssertionError(f"{k!r}: writes are exact, so its predecessor is named")
        assert delta.added - delta.removed == len(after) - len(before)
        self.created |= {(k, g) for k, (g, _) in after.items()}
        assert files.generation == gen
        self.before[self.commit_number] = before
        self.state = self.state.committed(self.commit_number, files)
        self.commit_number += 1
        self.model = after
        return delta

    def reserve(self):
        """A reader's endpoint, born at head + 1: the state after the head."""

        self.endpoints.add(self.commit_number)

    async def _merge(self, plan=None):
        idx = self.index()
        plan = plan or idx.plan_merge(self.endpoints)
        if plan is None:
            return False
        out = await idx.merge(plan, self.endpoints)
        if out is None:  # a rewrite that drops too little: nothing published
            return False
        self.state = self.state.merged(out.inputs, out.span)
        self.merges += 1
        return True

    async def merge_all(self):
        while await self._merge():
            pass

    async def collapse(self):
        """One merge of every span: only the versions live endpoints see stay."""

        if len(self.state.spans) > 1:
            assert await self._merge((0, len(self.state.spans)))

    def check_collection(self):
        live = {(k, g) for k, (g, _) in self.model.items()}
        assert self.created - live <= self.named, "every superseded object is named once dropped"
        assert not self.named & live

    async def check(self):
        idx = self.index()
        seen, after = {}, None
        while True:
            keys, generations, payloads, after = await idx.page(after, 97)
            for k, g, p in zip(keys, generations, payloads, strict=True):
                assert k not in seen
                seen[k] = (g, p)
            if after is None:
                break
        assert seen == self.model
        assert self.state.count == len(self.model)
        spans = self.state.spans
        assert [s.a for s in spans] == [0, *(s.b + 1 for s in spans)][: len(spans)]  # they tile, from 0
        for s in spans:  # a span's files in key order: a key's versions may cross into the next
            assert all(a.max <= b.min for a, b in zip(s.files, s.files[1:], strict=False))
        for e in self.endpoints:
            if e <= self.state.head:
                await self.check_endpoint(e)

    async def check_endpoint(self, e):
        """At endpoint `e`, the view is the dict before commit `e`, and
        `changes(e -> head)` is the diff from it to now, each key classed."""

        idx, then = self.index(), self.before[e]
        seen, after = {}, None
        while True:
            keys, generations, payloads, after = await idx.page(after, 97, at=e)
            seen.update(zip(keys, zip(generations, payloads, strict=True), strict=True))
            if after is None:
                break
        assert seen == then, f"the view at {e}"
        probe = sorted(then)[::7] + sorted(self.model)[::11] + [b"zz-absent"]
        assert await idx.lookup(probe, at=e) == {k: then[k] for k in probe if k in then}, f"lookups at {e}"
        got = {}
        async for page in idx.changes(e, self.state.head, limit=89):
            for k, c, g, d, p in zip(
                page.keys, page.classes, page.generations, page.deleted, page.payloads, strict=True
            ):
                got[k] = (c, None if d else (g, p))
        expect = {}
        for k in then.keys() | self.model.keys():
            was, now = then.get(k), self.model.get(k)
            if was != now:
                # The net rule: live at both ends at one version (a payload) is neither.
                same = was is not None and now is not None and was[1] is not None and was[1] == now[1]
                expect[k] = (3 if same else CLASSES[(was is not None, now is not None)], now)
        # A key written and put back since `e` may be listed too, as "neither"
        # or "updated" at its old state: it changed in between.
        extra = {k: v for k, v in got.items() if k not in expect}
        assert all(v[1] == self.model.get(k) for k, v in extra.items()), f"changes from {e}"
        assert {k: v for k, v in got.items() if k in expect} == expect, f"changes from {e}"


def key(i):
    return f"k{i:07d}".encode()


def rev(rng):
    """No version (an attempt's write), or one of a few: many unchanged rewrites."""

    n = rng.randrange(5)
    return f"r{n}".encode() if n else None


@pytest.mark.parametrize(
    "options",
    [small_options(), filtered_options(), streamed_options(), switching_options()],
    ids=["whole-reads", "filtered-reads", "streamed", "switching"],
)
async def test_random_workload_matches_a_dict(options):
    rng = random.Random(7)
    h = Harness(options)
    universe = 3000
    for step in range(60):
        op = rng.random()
        if op < 0.08 and step > 5:
            # A full replacement: a random subset, at random versions.
            ks = sorted({key(rng.randrange(universe)) for _ in range(rng.randrange(1, 1500))})
            await h.commit(ks, [rev(rng) for _ in ks], replace=True)
        else:
            n = rng.choice([1, 5, 50, 400])
            ks = sorted({key(rng.randrange(universe)) for _ in range(n)})
            rm = sorted({key(rng.randrange(universe)) for _ in range(n // 5)} - set(ks))
            await h.commit(ks, [rev(rng) for _ in ks], rm)
        if step % 9 == 4:
            h.reserve()
        if step % 23 == 22 and h.endpoints:
            h.endpoints.discard(min(h.endpoints))  # its reader moved on
        if step % 7 == 6:
            await h.merge_all()
        if step % 10 == 9:
            await h.check()
    await h.merge_all()
    await h.check()
    assert h.merges and len(h.state.spans) < 60  # the workload merged
    await h.collapse()
    assert len(h.state.spans) == 1
    await h.check()
    h.check_collection()
    if options.stream_density == 0:
        assert set(h.routes) == {"stream"}
    if options.stream_reads == 0:
        assert {"sparse", "stream"} <= set(h.routes)  # filters cleared some patches, others streamed


_small_keys = st.sets(st.sampled_from([key(i) for i in range(6)]), max_size=4).map(sorted)
_steps = st.lists(
    st.one_of(
        st.tuples(st.just("patch"), _small_keys, _small_keys),
        st.tuples(st.just("replace"), _small_keys, st.just([])),
        st.tuples(st.sampled_from(["merge", "merge late", "reserve"]), st.just([]), st.just([])),
    ),
    max_size=14,
)


@settings(max_examples=150, deadline=None)
@given(steps=_steps)
@example(
    steps=[
        ("patch", [key(0)], []),
        ("patch", [key(0)], []),
        ("reserve", [], []),
        ("replace", [], []),
        ("merge late", [], []),
        ("patch", [key(0), key(1)], []),
        ("patch", [], [key(1)]),
        ("merge", [], []),
    ]
)
def test_any_workload_of_a_few_keys_matches_a_dict(steps):
    """Patches, removals, replacements (empty ones too), reservations and
    merges over six keys, with the whole policy merging eagerly. A merge
    runs in the background in the engine: one `late` is applied after the
    next commit, unless another took its inputs meanwhile (upkeep then drops
    it). After every step the index pages back the dict, the spans tile, and
    every live endpoint sees the dict as it was. The explicit example began
    as F16's regression: a removed key came back when level 0 moved into an
    empty level 1 unmerged."""

    async def workload():
        h = Harness(small_options(base_ratio=1e9, window=2, read_slack=1 << 30))
        late = None  # a merge's outcome, applied after the next commit
        for op, ks, rm in steps:
            if op == "merge":
                await h._merge((0, len(h.state.spans)) if len(h.state.spans) > 1 else None)
            elif op == "merge late":
                if late is None and len(h.state.spans) > 1:
                    late = await h.index().merge((0, len(h.state.spans)), h.endpoints)
            elif op == "reserve":
                h.reserve()
            else:
                if op == "patch":
                    await h.commit(ks, None, sorted(set(rm) - set(ks)))
                else:
                    await h.commit(ks, replace=True)
                if late is not None:  # as upkeep does: dropped if its inputs went meanwhile
                    if h.state.holds(late.inputs, late.names):
                        h.state = h.state.merged(late.inputs, late.span)
                    late = None
            await h.check()

    asyncio.run(workload())


async def test_a_rewrite_is_a_change_unless_at_its_version():
    """docs/versions.md §1–§2: writing a key changes it; only a source's key
    observed again at its stored version is unchanged."""

    h = Harness(small_options())
    ks = [key(i) for i in range(500)]
    await h.commit(ks, [b"r1"] * 500)
    delta = await h.commit(ks, [b"r1"] * 500)
    assert len(delta) == 0 and delta.added == delta.removed == 0
    delta = await h.commit(ks)  # written with no version: every key changed
    assert len(delta) == 500 and delta.added == delta.removed == 0
    delta = await h.commit(ks)
    assert len(delta) == 500


async def test_new_keys_are_cleared_by_the_key_filter():
    """A write of keys the index never held needs no block: no file's key
    filter holds them (but for a rare false positive). Existing keys always
    get their block read, for the predecessor (exact writes)."""

    h = Harness(filtered_options(stream_reads=1e9, stream_density=1.0))
    ks = [key(i) for i in range(0, 8000, 2)]
    await h.commit(ks)
    tails = len(h.state.files)
    h.io.metrics.reset()
    idx = h.index()
    fresh = [key(i) for i in range(1, 8000, 80)]
    files, _ = await idx.resolve(run(fresh), commit_number=9, attempt="n")
    assert idx.route == "sparse" and files.added == len(fresh)
    assert h.io.metrics.gets <= tails + 3  # tails, and a block or two for false positives
    h.io.metrics.reset()
    idx = h.index()
    old = ks[::40]
    files, _ = await idx.resolve(run(old), commit_number=10, attempt="o")
    written = await h._read(idx, files.files)
    assert idx.route == "sparse" and all(e[4] == 1 for e in written)  # read: each names its predecessor


async def test_pending_deltas_newest_wins_and_survive_merges():
    h = Harness(small_options())
    await h.commit([key(1), key(2), key(3)])  # batch 0
    h.reserve()  # a reader at commit 1
    await h.commit([key(2)], None, [key(3)])  # batch 1
    await h.commit([key(4)])  # batch 2
    await h.collapse()
    idx = h.index()
    keys, generations, deleted, _, nxt = await idx.pending(1, 2, None, 10)
    assert list(zip(keys, generations, deleted, strict=True)) == [
        (key(2), 2, 0),
        (key(3), 2, 1),
        (key(4), 3, 0),
    ]
    assert nxt is None
    # Paged, two at a time.
    got, after = [], None
    while True:
        k, _, _, _, after = await idx.pending(0, 2, after, 2)
        got += k
        if after is None:
            break
    assert got == [key(1), key(2), key(3), key(4)]
    # Commit 2 was no endpoint when they merged: the merge kept no boundary there.
    with pytest.raises(LookupError):
        await h.index().pending(2, 2, None, 10)


async def test_a_large_commit_splits_at_max_file_bytes():
    h = Harness(small_options())
    ks = [key(i) for i in range(20000)]  # a few bytes an entry: enough for several 16 KiB files
    await h.commit(ks)
    assert len(h.state.spans) == 1 and len(h.state.files) > 1
    await h.check()


async def test_pages_read_only_the_block_indexes_they_need():
    """A page reads the index part of the few files covering it — never filters,
    never files outside its key range."""

    h = Harness(small_options())
    ks = [key(i) for i in range(20000)]
    rng = random.Random(0)
    await h.commit(ks, [rng.randbytes(16) for _ in ks])  # versions: blocks outweigh filters
    assert len(h.state.files) >= 6
    size = sum(f.size for f in h.state.files)
    h.io.metrics.reset()
    keys, _, _, nxt = await h.index().page(key(10000), 50)
    assert keys == ks[10001:10051] and nxt == ks[10050]  # a full page, across a file boundary
    assert h.io.metrics.gets <= 4
    assert h.io.metrics.bytes_in < size / 5  # the two files it spans, of dozens


class Tracking(ObjectIO):
    """Counts the most requests ever in flight at once."""

    def __init__(self, *args, **kw):
        super().__init__(*args, latency=0.002, **kw)
        self.inflight = self.peak = 0

    async def _get(self, path, start, end):
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        try:
            return await super()._get(path, start, end)
        finally:
            self.inflight -= 1


async def test_spans_are_read_at_once():
    h = Harness(small_options())
    h.io = Tracking(h.io.store)
    rng = random.Random(3)
    await h.commit([key(i) for i in range(2000)])
    for _ in range(6):
        ks = sorted({key(rng.randrange(2000)) for _ in range(20)})
        await h.commit(ks)
    assert len(h.state.spans) == 7
    h.io.peak = 0
    await h.index().delta(run([key(i) for i in range(0, 2000, 50)]))
    assert h.io.peak >= 7  # seven spans, not one after another


async def test_a_patch_reads_blocks_or_streams():
    """docs/resolved-commits.md §6: the sparse reader reads tails, then only
    the blocks of keys the filters cannot decide; a patch dense enough, or one
    whose exact reads would touch more blocks than streaming the index costs,
    streams instead."""

    h = Harness(filtered_options(stream_reads=2.0))
    rng = random.Random(4)
    ks = [key(i) for i in range(4000)]
    vs = [rng.randbytes(16) for _ in ks]  # incompressible versions: data outweighs filters
    await h.commit(ks, vs)
    files = h.state.files
    assert len(files) >= 3 and all(f.size > 2 * f.tail for f in files)
    probe, same = ks[::60], vs[::60]  # every few blocks: no two consecutive

    # Every existing key's entry is read, so the count is exact and predecessors are named.
    idx = h.index()
    out, _ = await idx.resolve(run(probe), commit_number=8, attempt="w")
    written = await h._read(idx, out.files)
    assert len(written) == len(probe) and all(e[4] is not None for e in written)

    # Observed again at their versions: every key needs its block, more than streaming costs.
    idx = h.index()
    files_out, _ = await idx.resolve(run(probe, same), commit_number=9, attempt="x")
    assert idx.route == "stream" and not files_out.files

    # Dense: more of the index than `stream_density` streams at once.
    idx = KeyIndex(h.io, "keys/out/p", h.state, filtered_options(stream_density=0.01))
    h.io.metrics.reset()
    await idx.resolve(run(ks[::50]), commit_number=9, attempt="y")
    assert idx.route == "stream" and h.io.metrics.gets == len(h.state.files)  # each file once, no tail first


async def test_lookup_reads_prior_records_exactly():
    h = Harness(filtered_options())
    ks = [key(i) for i in range(2000)]
    await h.commit(ks, [b"r1"] * len(ks))
    await h.commit(ks[:10], None, ks[10:20])
    got = await h.index().lookup(ks[:30] + [b"zz-absent"])
    assert got == {**{k: (2, None) for k in ks[:10]}, **{k: (1, b"r1") for k in ks[20:30]}}


async def test_a_full_scan_reads_each_block_once():
    """Pages overlap in the files they read — a small file spans every page —
    but a scan never fetches the same block twice."""

    h = Harness(small_options(small_file=0))
    rng = random.Random(5)
    ks = [key(i) for i in range(3000)]
    await h.commit(ks, [rng.randbytes(16) for _ in ks])
    for _ in range(3):
        some = sorted({key(rng.randrange(3000)) for _ in range(300)})
        await h.commit(some, [rng.randbytes(16) for _ in some])
    assert len(h.state.spans) == 4 and all(f.size > 2 * f.tail for f in h.state.files)
    h.io.metrics.reset()
    idx = h.index()
    after, pages = None, 0
    while True:
        _, _, _, after = await idx.page(after, 50)
        pages += 1
        if after is None:
            break
    assert pages >= 50
    assert h.io.metrics.bytes_in <= sum(f.size for f in h.state.files)


async def test_generations_and_predecessors():
    """Entries carry the generation that wrote them; delta entries carry the
    generation the key had before, for an immutable store to clean up the
    object it superseded (lifecycle.md §9.8). A merge into the base with no
    endpoint keeps each live key's newest version, without predecessors."""

    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/out/")

    async def entries(files):
        return [e for f in files for e in _python.iter_file(await io.read_whole(state.path(f.name), f.size))]

    first, _ = await KeyIndex(io, None, state).replace(
        Rows.pairs([(b"a", b"1"), (b"b", b"1"), (b"c", None)]), 0, "w1", generation=10
    )
    assert await entries(first.files) == [
        (b"a", 10, 0, b"1", None),
        (b"b", 10, 0, b"1", None),
        (b"c", 10, 0, None, None),
    ]
    state = state.committed(0, first)
    second, _ = await KeyIndex(io, None, state).replace(
        Rows.pairs([(b"a", b"1"), (b"b", b"2"), (b"d", None)]), 1, "w2", generation=20
    )
    assert await entries(second.files) == [
        (b"b", 20, 0, b"2", 10),  # changed: the object of generation 10 is superseded
        (b"c", 20, 1, None, 10),  # deleted
        (b"d", 20, 0, None, None),  # new: nothing superseded
    ]
    state = state.committed(1, second)
    idx = KeyIndex(io, None, state)
    delta = await idx.delta(run([b"a", b"d"], None, [b"b"]), generation=30)
    assert [(e[0], e[1], e[4]) for e in entries_of(delta)] == [(b"a", 30, 10), (b"b", 30, 20), (b"d", 30, 20)]
    state = state.committed(2, await idx.write(2, "w3", delta, 30))
    keys, generations, payloads, _ = await KeyIndex(io, None, state).page(None, 10)
    assert list(zip(keys, generations, payloads, strict=True)) == [(b"a", 30, None), (b"d", 30, None)]
    merged = await KeyIndex(io, None, state).merge((0, 3), set())
    assert [e[1:] for e in await entries(merged.span.files)] == [(30, 0, None, None), (30, 0, None, None)]


async def test_pages_read_each_file_in_its_own_codec():
    """Round 2, finding 4: a codec belongs to a file, not to its index — a
    page merges a stored file with a compressed one."""

    from solera.keys.index import FileInfo

    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/out/p/")
    for n, (k, codec) in enumerate([(b"a", 0), (b"b", 1)]):
        data = _python.encode_file([k], [5], b"\x00", codec=codec)
        name = f"{n:012d}-x.0000"
        await io.write(state.path(name), data)
        span = Span(n, n, ((n, 5),), (FileInfo.describe(name, data),))
        state = IndexState(spans=(*state.spans, span), prefix=state.prefix)
    idx = KeyIndex(io, None, state)
    assert (await idx.page(None, 10))[0] == [b"a", b"b"]
    assert await idx.lookup([b"a", b"b"]) == {b"a": (5, None), b"b": (5, None)}
