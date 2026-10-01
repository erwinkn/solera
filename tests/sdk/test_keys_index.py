"""The key index (docs/object-store-state.md §6), checked against a plain dict.

Every step of a random workload — patches, removals, full replacements,
compactions — must produce the delta a dict says it should, and the index
must page back exactly the dict's content."""

import random

import pytest
from obstore.store import MemoryStore
from solera.keys import Rows, _python
from solera.keys.index import Delta, IndexState, KeyIndex, Options
from solera.keys.io import DiskCache, ObjectIO


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
    # Nothing is small enough to read whole, and whole-level reads never fit the budget:
    # every level beyond level 0 goes through its filters.
    return small_options(whole_threshold=0, request_latency=10.0, **kw)


class Harness:
    def __init__(self, options, *, cache=None):
        self.io = ObjectIO(MemoryStore(), cache=cache)
        self.options = options
        self.state = IndexState()
        self.model: dict[bytes, bytes] = {}
        self.batch = 0

    def index(self):
        return KeyIndex(self.io, "keys/out/p", self.state, self.options)

    async def commit(self, keys, versions, removes=(), replace=False):
        before = dict(self.model)
        idx = self.index()
        if replace:
            # Streamed out as files: read them back.
            items = list(zip(keys, versions, strict=True))
            random.Random(len(items)).shuffle(items)
            rows = Rows.pairs(items)
            files, changed = await idx.replace(rows, self.batch, f"a{self.batch}", collect=10**6)
            written = [
                e[:3]
                for f in files.files
                for e in _python.iter_file(await self.io.read_whole(idx.path(f.name), f.size))
            ]
            delta = Delta(
                [k for k, _, _ in written],
                [v for _, v, _ in written],
                bytes(d for _, _, d in written),
                files.added,
                files.removed,
                files.exact,
            )
            assert changed == ([k for k, _, d in written if not d], [k for k, _, d in written if d])
        else:
            delta = await idx.changes(keys, versions, removes)
            files = await idx.write(self.batch, f"a{self.batch}", delta)
        # What the dict says changed.
        if replace:
            after = dict(zip(keys, versions, strict=True))
        else:
            after = dict(before)
            after.update(zip(keys, versions, strict=True))
            for k in removes:
                after.pop(k, None)
        expect = {k: v for k, v in after.items() if before.get(k) != v}
        expect_rm = {k for k in before if k not in after}
        got = {k: v for k, v, d in zip(delta.keys, delta.versions, delta.deleted, strict=True) if not d}
        got_rm = {k for k, d in zip(delta.keys, delta.deleted, strict=True) if d}
        assert got == expect, "upserts"
        assert got_rm == expect_rm, "removals"
        assert delta.keys == sorted(delta.keys)
        if delta.exact:
            assert delta.added - delta.removed == len(after) - len(before)
        self.state = self.state.committed(self.batch, files, keep_log=True)
        self.batch += 1
        self.model = after
        return delta

    async def compact_all(self):
        while True:
            idx = self.index()
            out = await idx.compact()
            if out is None:
                return
            self.state = self.state.compacted(*out)

    async def check(self):
        idx = self.index()
        seen, after = {}, None
        while True:
            keys, versions, _, after = await idx.page(after, 97)
            for k, v in zip(keys, versions, strict=True):
                assert k not in seen
                seen[k] = v
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


def ver(rng):
    return f"v{rng.randrange(4)}".encode()  # few versions: many unchanged rewrites


@pytest.mark.parametrize(
    "options", [small_options(), filtered_options()], ids=["whole-reads", "filtered-reads"]
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
            await h.commit(ks, [ver(rng) for _ in ks], replace=True)
        else:
            n = rng.choice([1, 5, 50, 400])
            ks = sorted({key(rng.randrange(universe)) for _ in range(n)})
            rm = sorted({key(rng.randrange(universe)) for _ in range(n // 5)} - set(ks))
            await h.commit(ks, [ver(rng) for _ in ks], rm)
        if step % 7 == 6:
            await h.compact_all()
        if step % 10 == 9:
            await h.check()
    await h.compact_all()
    await h.check()
    assert h.state.depth >= 2  # the workload exercised more than one level


async def test_unchanged_rewrite_is_an_empty_delta():
    h = Harness(small_options())
    ks = [key(i) for i in range(500)]
    await h.commit(ks, [b"v1"] * 500)
    delta = await h.commit(ks, [b"v1"] * 500)
    assert len(delta) == 0 and delta.added == delta.removed == 0


async def test_filters_skip_block_reads_for_real_changes():
    """Changed versions of existing keys are cleared by the pair filters alone."""

    h = Harness(filtered_options())
    ks = [key(i) for i in range(4000)]
    await h.commit(ks, [b"v1"] * len(ks))
    for _ in range(3):
        await h.compact_all()
    probe = ks[::40]
    h.io.metrics.reset()
    idx = h.index()
    delta = await idx.changes(probe, [b"v2"] * len(probe))
    assert len(delta) == len(probe) and not delta.exact  # "changed" came from filters
    tails = sum(1 for level in h.state.newest_first() for _ in level)
    # Only file tails were read (plus a block or two for rare false positives).
    assert h.io.metrics.gets <= tails + 3


async def test_pending_deltas_newest_wins_and_survive_compaction():
    h = Harness(small_options())
    await h.commit([key(1), key(2), key(3)], [b"a", b"a", b"a"])  # batch 0
    await h.commit([key(2)], [b"b"], [key(3)])  # batch 1
    await h.commit([key(4)], [b"c"])  # batch 2
    await h.compact_all()
    idx = h.index()
    keys, versions, deleted, _, nxt = await idx.pending(1, 2, None, 10)
    assert list(zip(keys, versions, deleted, strict=True)) == [
        (key(2), b"b", 0),
        (key(3), b"", 1),
        (key(4), b"c", 0),
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
    ks = [key(i) for i in range(5000)]
    await h.commit(ks, [b"v"] * len(ks))
    assert {f.level for f in h.state.files} == {1}
    assert len(h.state.files) > 1  # split at max_file_bytes
    await h.check()


async def test_disk_cache_serves_repeat_reads(tmp_path):
    h = Harness(filtered_options(), cache=DiskCache(str(tmp_path), max_bytes=1 << 30))
    ks = [key(i) for i in range(3000)]
    await h.commit(ks, [b"v1"] * len(ks))
    h.io.metrics.reset()
    await h.index().changes(ks[:50], [b"v2"] * 50)
    first = h.io.metrics.gets
    await h.index().changes(ks[50:100], [b"v2"] * 50)
    assert h.io.metrics.gets == first == 0  # written through the cache on commit: never fetched
    assert h.io.metrics.cache_hits > 0


async def test_pages_read_only_the_block_indexes_they_need():
    """A page reads the index part of the few files covering it — never filters,
    never files outside its key range."""

    h = Harness(small_options())
    ks = [key(i) for i in range(20000)]
    rng = random.Random(0)
    await h.commit(ks, [rng.randbytes(16) for _ in ks])  # row digests: blocks outweigh filters
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
    await h.commit([key(i) for i in range(2000)], [b"v1"] * 2000)
    for _ in range(6):
        ks = sorted({key(rng.randrange(2000)) for _ in range(20)})
        await h.commit(ks, [b"v2"] * len(ks))
    assert len(h.state.level(0)) == 6
    h.io.peak = 0
    await h.index().changes([key(i) for i in range(0, 2000, 50)], [b"v3"] * 40)
    assert h.io.peak >= 7  # six deltas and level 1, not one after another


async def test_the_planner_reads_blocks_or_the_rest_once_it_knows_how_many():
    """The tails first; then, knowing which keys the filters could not clear,
    the fewest requests: their blocks when there are few, else the rest of
    each file in one read."""

    # Everything goes through the filters, and every plan fits the latency budget.
    h = Harness(small_options(whole_threshold=0, request_latency=0.0, l0_max_files=100))
    rng = random.Random(4)
    ks = [key(i) for i in range(4000)]
    vs = [rng.randbytes(16) for _ in ks]  # incompressible: data outweighs filters
    await h.commit(ks, vs)
    files = h.state.level(1)
    assert len(files) >= 3 and all(f.size > 2 * f.tail for f in files)
    probe, same = ks[::60], vs[::60]  # every few blocks: no two consecutive

    # All changed: the filters clear nearly every key, so only tails and a false positive's block.
    h.io.metrics.reset()
    delta = await h.index().changes(probe, [b"v2"] * len(probe))
    assert len(delta) == len(probe)
    assert h.io.metrics.gets <= len(files) + 2

    # All rewritten unchanged: every key needs its block, scattered over each file.
    idx = h.index()
    h.io.metrics.reset()
    delta = await idx.changes(probe, same)
    assert len(delta) == 0
    parsed = [idx._parsed[f.name] for f in files]
    runs = sum(len(p.runs({p.block_of(k) for k in probe} - {-1})) for p in parsed)
    assert runs > len(files) and h.io.metrics.gets == 2 * len(files)  # tails, then the rest


async def test_a_full_scan_reads_each_block_once():
    """Pages overlap in the files they read — a small file spans every page —
    but a scan never fetches the same block twice."""

    h = Harness(small_options(request_latency=0.0, l0_max_files=100))
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
    await h.commit([key(i) for i in range(3000)], [b"v1"] * 3000)
    l1 = sum(f.size for f in h.state.level(1))
    pushed = False
    for step in range(200):
        some = sorted({key(rng.randrange(3000)) for _ in range(10)})
        await h.commit(some, [ver(rng) for _ in some])
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
    await h.commit([key(1)], [b"old"])
    await h.commit([key(2)], [b"x"])
    await h.commit([key(3)], [b"x"])
    assert h.index().plan_compaction()[1] == 0
    await h.compact_all()
    await h.commit([key(1)], [b"new"])
    await h.check()


async def test_locators_and_predecessors():
    """Entries carry the generation that wrote them; delta entries carry the
    key's predecessor `(version, locator)`, for the store to discard the object
    they superseded (lifecycle.md §9.8). Compaction keeps locators only."""

    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/out/")

    async def entries(files):
        return [e for f in files for e in _python.iter_file(await io.read_whole(state.path(f.name), f.size))]

    first, _ = await KeyIndex(io, None, state).replace(
        Rows.pairs([(b"a", b"1"), (b"b", b"1"), (b"c", b"1")]), 0, "w1", generation=10
    )
    assert await entries(first.files) == [(k, b"1", 0, 10, None) for k in (b"a", b"b", b"c")]
    state = state.committed(0, first, keep_log=True)
    second, _ = await KeyIndex(io, None, state).replace(
        Rows.pairs([(b"a", b"1"), (b"b", b"2"), (b"d", b"1")]), 1, "w2", generation=20
    )
    assert await entries(second.files) == [
        (b"b", b"2", 0, 20, (b"1", 10)),  # changed: the object of generation 10 is superseded
        (b"c", b"", 1, 20, (b"1", 10)),  # deleted
        (b"d", b"1", 0, 20, None),  # new: nothing superseded
    ]
    state = state.committed(1, second, keep_log=True)
    idx = KeyIndex(io, None, state)
    delta = await idx.changes([b"a", b"d"], [b"2", b"1"], [b"b"], generation=30)
    assert list(zip(delta.keys, delta.locators, delta.predecessors, strict=True)) == [
        (b"a", 30, (b"1", 10)),
        (b"b", 30, (b"2", 20)),
    ]
    state = state.committed(2, await idx.write(2, "w3", delta), keep_log=True)
    keys, versions, locators, _ = await KeyIndex(io, None, state).page(None, 10)
    assert list(zip(keys, versions, locators, strict=True)) == [(b"a", b"2", 30), (b"d", b"1", 20)]
    added, removed = await KeyIndex(io, None, state).compact((state.level(0) + state.level(1), 1))
    assert [e[3:] for e in await entries(added)] == [(30, None), (20, None)]
