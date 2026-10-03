"""The key index (docs/object-store-state.md §6), checked against a plain dict.

Every step of a random workload — patches, removals, full replacements,
compactions — must produce the delta a dict says it should, and the index
must page back exactly the dict's content."""

import random
from dataclasses import dataclass

import pytest
from obstore.store import MemoryStore
from solera.keys import Rows, SortedEntries, _python
from solera.keys.index import IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO


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
    exact: bool

    def __len__(self):
        return len(self.keys)


def run(keys, payloads=None, removes=()):
    return SortedEntries.of(list(keys), None if payloads is None else list(payloads), list(removes))


def entries_of(delta):
    """Every entry of an unwritten delta: key, generation, deleted, payload, predecessor."""

    return [e for d in delta.files for e in _python.iter_file(d)]


def small_options(**kw):
    # Tiny files and blocks so a few thousand keys exercise several levels.
    base = dict(
        block_size=512,
        max_file_bytes=16 * 1024,
        l0_max_files=3,
        l0_max_bytes=1 << 30,
        level_base=6 * 1024,
        fanout=3,
    )
    base.update(kw)
    return Options(**base)


def filtered_options(**kw):
    # Nothing is small enough to read whole: every level beyond level 0 goes through its filters.
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
    collection exactly once it is: by the delta that superseded it, or by a
    compaction's garbage. A key written with a payload (a source's version)
    equal to its entry's is unchanged; any other write changes it."""

    def __init__(self, options, *, exact=False):
        self.io = ObjectIO(MemoryStore())
        self.options = options
        self.exact = exact
        self.state = IndexState()
        self.model: dict[bytes, tuple[int, bytes | None]] = {}  # key -> (generation, payload)
        self.batch = 0
        self.created: set[tuple] = set()
        self.named: set[tuple] = set()  # superseded objects a delta or a garbage file named
        self.routes: list[str] = []

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
        gen = self.batch + 1
        if replace:
            items = list(zip(keys, payloads, strict=True))
            random.Random(len(items)).shuffle(items)
            files, changed = await idx.replace(
                Rows.pairs(items), self.batch, f"a{self.batch}", collect=10**6, generation=gen
            )
        else:
            files, changed = await idx.resolve(
                run(keys, payloads, removes),
                batch=self.batch,
                attempt=f"a{self.batch}",
                generation=gen,
                exact=self.exact,
                collect=10**6,
            )
            if self.state.files:
                self.routes.append(idx.route)
        written = await self._read(idx, files.files)
        delta = Written(
            *map(list, zip(*written, strict=True)) if written else ([],) * 5,
            files.added,
            files.removed,
            files.exact,
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
            elif k in before and (self.exact or replace or idx.route == "stream"):
                raise AssertionError(f"{k!r}: its old entry was read, so its predecessor is named")
        if delta.exact:
            assert delta.added - delta.removed == len(after) - len(before)
        if self.exact:
            assert delta.exact
        self.created |= {(k, g) for k, (g, _) in after.items()}
        self.state = self.state.committed(self.batch, files, keep_log=True)
        self.batch += 1
        self.model = after
        return delta

    async def _compact(self, plan=None):
        idx = self.index()
        out = await idx.compact(plan, garbage=True)
        if out is None:
            return False
        added, removed, garbage = out
        live = {(k, g) for k, (g, _) in self.model.items()}
        for gf in garbage:
            ks, gs = _python.decode_garbage(await self.io.read_whole(idx.garbage_path(gf.name), gf.size))
            assert len(ks) == gf.entries
            dropped = set(zip(ks, gs, strict=True))
            assert not dropped & live, "garbage never names a live object"
            assert dropped <= self.created
            self.named |= dropped
        self.state = self.state.compacted(added, removed)
        return True

    async def compact_all(self):
        while await self._compact():
            pass

    async def collapse(self):
        """One merge of every file into the deepest level: every shadowed entry is dropped."""

        files = [f for level in self.state.newest_first() for f in level]
        if len(files) > 1:
            await self._compact((files, self.state.depth))

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
        assert await idx.recount() == len(self.model)
        if self.state.count_exact:
            assert self.state.count == len(self.model)
        levels = {f.level for f in self.state.files}
        for n in levels - {0}:  # levels 1+ never overlap
            files = self.state.level(n)
            for a, b in zip(files, files[1:], strict=False):
                assert a.max < b.min


def key(i):
    return f"k{i:07d}".encode()


def rev(rng):
    """No version (an attempt's write), or one of a few: many unchanged rewrites."""

    n = rng.randrange(5)
    return f"r{n}".encode() if n else None


@pytest.mark.parametrize(
    "options, exact",
    [
        (small_options(), False),
        (filtered_options(), False),
        (filtered_options(), True),
        (streamed_options(), False),
        (switching_options(), False),
    ],
    ids=["whole-reads", "filtered-reads", "exact-reads", "streamed", "switching"],
)
async def test_random_workload_matches_a_dict(options, exact):
    rng = random.Random(7)
    h = Harness(options, exact=exact)
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
        if step % 7 == 6:
            await h.compact_all()
        if step % 10 == 9:
            await h.check()
    await h.compact_all()
    await h.check()
    assert h.state.depth >= 2  # the workload exercised more than one level
    await h.collapse()
    await h.check()
    h.check_collection()
    if options.stream_density == 0:
        assert set(h.routes) == {"stream"}
    if options.stream_reads == 0:
        assert {"sparse", "stream"} <= set(h.routes)  # filters cleared some patches, others streamed


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


async def test_filters_skip_block_reads_for_writes():
    """A write of existing keys is decided by the key and tombstone filters
    alone: it changes them whatever their entries hold."""

    h = Harness(filtered_options())
    ks = [key(i) for i in range(4000)]
    await h.commit(ks)
    for _ in range(3):
        await h.compact_all()
    probe = ks[::40]
    h.io.metrics.reset()
    idx = h.index()
    delta = await idx.changes(run(probe))
    assert len(delta) == len(probe) and not delta.exact  # "live" came from filters
    tails = sum(1 for level in h.state.newest_first() for _ in level)
    # Only file tails were read (plus a block or two for rare false positives).
    assert h.io.metrics.gets <= tails + 3


async def test_pending_deltas_newest_wins_and_survive_compaction():
    h = Harness(small_options())
    await h.commit([key(1), key(2), key(3)])  # batch 0
    await h.commit([key(2)], None, [key(3)])  # batch 1
    await h.commit([key(4)])  # batch 2
    await h.compact_all()
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
    # Truncating the log drops batches no consumer needs.
    h.state = h.state.truncated(2)
    with pytest.raises(LookupError):
        await h.index().pending(1, 2, None, 10)


async def test_large_first_commit_goes_straight_to_level_one_and_splits():
    h = Harness(small_options())
    ks = [key(i) for i in range(20000)]  # a few bytes an entry: enough for several 16 KiB files
    await h.commit(ks)
    assert {f.level for f in h.state.files} == {1}
    assert len(h.state.files) > 1  # split at max_file_bytes
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


async def test_level_0_files_are_read_at_once():
    h = Harness(small_options(l0_max_files=100))
    h.io = Tracking(h.io.store)
    rng = random.Random(3)
    await h.commit([key(i) for i in range(2000)])
    for _ in range(6):
        ks = sorted({key(rng.randrange(2000)) for _ in range(20)})
        await h.commit(ks)
    assert len(h.state.level(0)) == 6
    h.io.peak = 0
    await h.index().changes(run([key(i) for i in range(0, 2000, 50)]))
    assert h.io.peak >= 7  # six deltas and level 1, not one after another


async def test_a_patch_reads_blocks_or_streams():
    """docs/resolved-commits.md §6: the sparse reader reads tails, then only
    the blocks of keys the filters cannot decide; a patch dense enough, or one
    whose exact reads would touch more blocks than streaming the index costs,
    streams instead."""

    h = Harness(filtered_options(l0_max_files=100, stream_reads=2.0))
    rng = random.Random(4)
    ks = [key(i) for i in range(4000)]
    vs = [rng.randbytes(16) for _ in ks]  # incompressible versions: data outweighs filters
    await h.commit(ks, vs)
    files = h.state.level(1)
    assert len(files) >= 3 and all(f.size > 2 * f.tail for f in files)
    probe, same = ks[::60], vs[::60]  # every few blocks: no two consecutive

    # Written with no version: the filters decide nearly every key — tails and a false positive's block.
    idx = h.index()
    h.io.metrics.reset()
    delta = await idx.changes(run(probe))
    assert len(delta) == len(probe) and not delta.exact
    assert h.io.metrics.gets <= len(files) + 2

    # Exact: every key's entry is read, so the count is exact and predecessors are named.
    delta = await h.index().changes(run(probe), exact=True)
    assert delta.exact and all(e[4] is not None for e in entries_of(delta))

    # Observed again at their versions: every key needs its block, more than streaming costs.
    idx = h.index()
    files_out, _ = await idx.resolve(run(probe, same), batch=9, attempt="x")
    assert idx.route == "stream" and not files_out.files

    # Dense: more of the index than `stream_density` streams at once.
    idx = KeyIndex(h.io, "keys/out/p", h.state, filtered_options(stream_density=0.01))
    h.io.metrics.reset()
    await idx.resolve(run(ks[::50]), batch=9, attempt="y")
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

    h = Harness(small_options(small_file=0, l0_max_files=100))
    rng = random.Random(5)
    ks = [key(i) for i in range(3000)]
    await h.commit(ks, [rng.randbytes(16) for _ in ks])
    for _ in range(3):
        some = sorted({key(rng.randrange(3000)) for _ in range(300)})
        await h.commit(some, [rng.randbytes(16) for _ in some])
    assert len(h.state.level(0)) == 3 and all(f.size > 2 * f.tail for f in h.state.files)
    h.io.metrics.reset()
    assert await h.index().recount() == 3000
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


async def test_level_0_merges_in_itself_until_it_is_a_tenth_of_level_1():
    h = Harness(small_options(l0_max_files=3, level_base=1 << 30, fanout=10))
    rng = random.Random(9)
    await h.commit([key(i) for i in range(3000)])
    l1 = sum(f.size for f in h.state.level(1))
    pushed = False
    for step in range(200):
        some = sorted({key(rng.randrange(3000)) for _ in range(10)})
        await h.commit(some, [rev(rng) for _ in some])
        plan = h.index().plan_compaction()
        if plan is None:
            continue
        inputs, level = plan
        l0 = sum(f.size for f in h.state.level(0))
        if level == 0:
            assert 10 * l0 < l1 and inputs == h.state.level(0)
        else:
            assert level == 1 and 10 * l0 >= l1
            pushed = True
        await h.compact_all()
        assert len(h.state.level(0)) <= (0 if level else 1)
        if step % 10 == 0:
            await h.check()
        if pushed:
            break
    assert pushed
    # A merged level-0 file stays older than the deltas after it.
    await h.commit([key(1)])
    await h.commit([key(2)])
    await h.commit([key(3)])
    assert h.index().plan_compaction()[1] == 0
    await h.compact_all()
    await h.commit([key(1)])
    await h.check()


async def test_generations_and_predecessors():
    """Entries carry the generation that wrote them; delta entries carry the
    generation the key had before, for an immutable store to discard the
    object it superseded (lifecycle.md §9.8). Compaction keeps generations
    and payloads only."""

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
    state = state.committed(0, first, keep_log=True)
    second, _ = await KeyIndex(io, None, state).replace(
        Rows.pairs([(b"a", b"1"), (b"b", b"2"), (b"d", None)]), 1, "w2", generation=20
    )
    assert await entries(second.files) == [
        (b"b", 20, 0, b"2", 10),  # changed: the object of generation 10 is superseded
        (b"c", 20, 1, None, 10),  # deleted
        (b"d", 20, 0, None, None),  # new: nothing superseded
    ]
    state = state.committed(1, second, keep_log=True)
    idx = KeyIndex(io, None, state)
    delta = await idx.changes(run([b"a", b"d"], None, [b"b"]), generation=30)
    assert [(e[0], e[1], e[4]) for e in entries_of(delta)] == [(b"a", 30, 10), (b"b", 30, 20), (b"d", 30, 20)]
    state = state.committed(2, await idx.write(2, "w3", delta), keep_log=True)
    keys, generations, payloads, _ = await KeyIndex(io, None, state).page(None, 10)
    assert list(zip(keys, generations, payloads, strict=True)) == [(b"a", 30, None), (b"d", 30, None)]
    added, removed, _ = await KeyIndex(io, None, state).compact((state.level(0) + state.level(1), 1))
    assert [e[1:] for e in await entries(added)] == [(30, 0, None, None), (30, 0, None, None)]


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
        state = IndexState(files=(*state.files, FileInfo.describe(name, 0, data)), prefix=state.prefix)
    idx = KeyIndex(io, None, state)
    assert (await idx.page(None, 10))[0] == [b"a", b"b"]
    assert await idx.lookup([b"a", b"b"]) == {b"a": (5, None), b"b": (5, None)}
