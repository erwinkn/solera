"""Stamped layers (solera.keys.layers) against the per-commit fold.

Random histories of upserts (some with source payloads) and removes, merges
by the rule and at random under a moving cut, and every read checked key by
key against a fold of the commits: Δ(P, H) for every P from the cut to the
head and P = −∞, to the head and to a pinned head, as a sorted key list and
as pages (random sizes, a glob, a predicate, an upper bound, a byte budget);
lookups at the head; replacements. Sizes are shrunk (blocks of 128 raw
bytes, parts over 512 bytes indexed, tiers from 64 bytes) so that merges,
indexed parts, side parts and straddled layers all occur in a few hundred
commits.
"""

from __future__ import annotations

import random
import re

import pytest
from obstore.store import MemoryStore
from solera.keys import Rows, SortedEntries
from solera.keys import layers as L
from solera.keys.io import ObjectIO
from solera.patterns import glob_regex


@pytest.fixture(autouse=True)
def small(monkeypatch):
    monkeypatch.setattr(L, "BLOCK", 128)
    monkeypatch.setattr(L, "SMALL", 512)
    monkeypatch.setattr(L, "TIER_BASE", 64)
    monkeypatch.setattr(L, "SLACK", 256)
    monkeypatch.setattr(L, "FILE_LIMIT", 2048)
    monkeypatch.setattr(L, "WINDOW", 256)


def key(i: int) -> bytes:
    return b"k%04d" % i


class History:
    """An index and the fold of its commits: state after commit c, key →
    (generation, payload)."""

    def __init__(self, rng: random.Random, sources: bool = False):
        self.rng, self.sources = rng, sources
        self.io = ObjectIO(MemoryStore())
        self.state = L.LayerState(prefix="keys/t/", life="l1")
        self.fold: list[dict[bytes, tuple[int, bytes | None]]] = []
        self.pinned: dict[int, L.LayerState] = {}
        self.epoch = 1

    def index(self, state=None) -> L.LayerIndex:
        return L.LayerIndex(self.io, state or self.state)

    async def commit(self, ups: dict[bytes, bytes | None], rms: list[bytes]):
        c = self.state.head + 1
        g = 10 * c + 7
        keys = sorted(ups)
        payloads = [ups[k] for k in keys] if self.sources else None
        written = SortedEntries.of(keys, payloads, sorted(set(rms) - set(ups)))
        delta = await self.index().resolve(written, generation=g, replaced=True)
        files = await self.index().write(f"d{c:06d}-a", delta, g)
        before = self.fold[-1] if self.fold else {}
        self.state = self.state.committed(c, files)
        now = dict(before)
        for k in keys:
            p = ups[k] if self.sources else None
            if k in now and p is not None and now[k][1] == p:
                continue  # an upsert of an equal payload changes nothing
            now[k] = (g, p)
        for k in set(rms) - set(ups):
            now.pop(k, None)
        self.fold.append(now)
        assert self.state.count == len(now)
        self.pinned[c] = self.state
        return delta

    async def merge_some(self, cut: int):
        self.state = self.state.with_cut(cut)
        for _ in range(4):
            plan = self.state.plan()
            if plan is None and self.rng.random() < 0.3 and len(self.state.layers) >= 2:
                lo = self.rng.randrange(len(self.state.layers) - 1)
                plan = ("random", lo, self.rng.randint(2, min(4, len(self.state.layers) - lo)))
            if plan is None:
                return
            _, lo, count = plan
            ids, out = await self.index().merge(lo, count, epoch=self.epoch)
            assert self.state.holds(ids)
            self.state = self.state.merged(ids, out)

    def expected(self, p: int | None, h: int, keys=None, match=None) -> list[tuple]:
        then = {} if p is None else self.fold[p]
        now = self.fold[h]
        out = []
        for k in sorted(set(then) | set(now)):
            if keys is not None and k not in keys:
                continue
            if match is not None and not match(k):
                continue
            a, b = then.get(k), now.get(k)
            if a is not None and b is not None and a[0] == b[0]:
                continue
            if a is None and b is None:
                continue
            out.append((k, a is not None, b is not None, b[0] if b else None, b[1] if b else None))
        return out


async def walk(idx: L.LayerIndex, p, first: int, **kw) -> list[tuple]:
    got, after = [], None
    while True:
        rows, cursor = await idx.delta(p, after=after, first=first, **kw)
        assert len(rows) <= first
        assert after is None or all(r[0] > after for r in rows)
        got += rows
        if cursor is None:
            return got
        after = cursor


async def check(h: History, rng: random.Random) -> int:
    head = h.state.head
    idx = h.index()
    n = 0
    ps = [None] + list(range(max(0, h.state.cut), head))
    for p in rng.sample(ps, min(len(ps), 6)):
        names = sorted({key(rng.randrange(300)) for _ in range(rng.randint(1, 30))})
        rows, cursor = await idx.delta(p, keys=names)
        assert cursor is None and rows == h.expected(p, head, set(names)), (p, "keys")
        assert await walk(idx, p, rng.choice([1, 3, 1000])) == h.expected(p, head), (p, "pages")
        glob = rng.choice([b"k00*", b"*1*", b"*7", b"k?2*"])
        rx = re.compile(glob_regex(glob.decode()).encode())
        assert await walk(idx, p, rng.choice([2, 50]), glob=glob) == h.expected(
            p, head, match=rx.fullmatch
        ), glob

        def take(k):
            return k.endswith((b"1", b"5"))

        assert await walk(idx, p, 3, take=take) == h.expected(p, head, match=take)
        upto = key(rng.randrange(300))
        got = await walk(idx, p, 4, upto=upto)
        assert got == [r for r in h.expected(p, head) if r[0] <= upto], ("upto", upto)
        assert await walk(idx, p, 1000, budget=1) == h.expected(p, head), "budget"
        n += 6
    # A pinned head: the state a batch held then, read after merges since.
    pins = [c for c in h.pinned if c >= max(0, h.state.cut) + 1]
    if pins:
        c = rng.choice(pins)
        pidx = h.index(h.pinned[c])
        p = rng.choice([None] + list(range(max(0, h.state.cut), c)))
        if p is None or p >= h.pinned[c].cut:
            assert await walk(pidx, p, rng.choice([2, 1000])) == h.expected(p, c), ("pinned", c, p)
            n += 1
    found = await idx.lookup([key(i) for i in range(300)])
    want = {k: v for k, v in h.fold[head].items()}
    assert found == want
    return n


@pytest.mark.parametrize("seed", range(8))
async def test_layers_agree_with_the_fold(seed):
    rng = random.Random(seed)
    h = History(rng, sources=seed % 2 == 1)
    readers = [0, 0, 0]
    checks = 0
    for c in range(140):
        live = list(h.fold[-1]) if h.fold else []
        big = rng.random() < 0.04
        ups = {
            key(rng.randrange(300)): rng.choice([b"v1", b"v2", None])
            for _ in range(rng.randint(80, 160) if big else rng.randint(0, 10))
        }
        rms = rng.sample(live, min(len(live), rng.randint(0, 4)))
        await h.commit(ups, rms)
        for i in range(len(readers)):
            if rng.random() < 0.1:
                readers[i] = h.state.head
        await h.merge_some(min(readers))
        if c % 9 == 8:
            checks += await check(h, rng)
    assert checks > 50
    kinds = {x.delta for x in h.state.layers}
    assert len(h.state.layers) < 40 and kinds


async def test_side_parts_graveyard_and_indexes_occur():
    rng = random.Random(3)
    h = History(rng)
    seen = {"graveyard": False, "side above the base": False, "indexed": False, "straddle": False}
    readers = [0, 0]
    for _ in range(220):
        live = list(h.fold[-1]) if h.fold else []
        await h.commit(
            {key(rng.randrange(300)): None for _ in range(rng.randint(0, 10))},
            rng.sample(live, min(len(live), 3)),
        )
        for i in range(2):
            if rng.random() < 0.1:
                readers[i] = h.state.head
        await h.merge_some(min(readers))
        base = h.state.layers[0]
        seen["graveyard"] |= base.side is not None and base.side.entries > 0
        seen["side above the base"] |= any(
            x.side is not None and x.side.entries > 0 for x in h.state.layers[1:]
        )
        seen["indexed"] |= any(x.main.index for x in h.state.layers)
        seen["straddle"] |= any(not x.delta and x.a <= min(readers) < x.b for x in h.state.layers)
    assert all(seen.values()), seen


async def test_a_p_below_the_cut_fails_loudly():
    h = History(random.Random(5))
    for c in range(12):
        await h.commit({key(c): None}, [])
    h.state = h.state.with_cut(6)
    with pytest.raises(L.CutError):
        await h.index().delta(5, first=10)
    h.state = h.state.with_cut(3)  # a stale cut never moves it back
    assert h.state.cut == 6


async def test_generations_must_rise_with_commits():
    h = History(random.Random(1))
    await h.commit({key(1): None}, [])
    delta = await h.index().resolve(SortedEntries.of([key(2)], None, []))
    files = await h.index().write("x", delta, 1)  # not past commit 0's 7
    with pytest.raises(ValueError, match="generation"):
        h.state.committed(1, files)


async def test_a_reading_at_an_unheld_h_is_refused():
    h = History(random.Random(2))
    for c in range(8):
        await h.commit({key(c): None}, [])
    ids, out = await h.index().merge(1, 4, epoch=1)
    h.state = h.state.merged(ids, out)
    assert h.state.at(7).head == 7 and h.state.at(4).head == 4
    with pytest.raises(L.NotHeld):
        h.state.at(2)  # inside the merged layer [1, 4]
    assert h.state.at(4).count is None and h.state.at(None).count == len(h.fold[-1])  # not the head's


async def test_a_replaced_change_names_the_generation_it_replaced():
    h = History(random.Random(4))
    await h.commit({key(1): None, key(2): None}, [])
    delta = await h.commit({key(1): None}, [key(2)])
    entries = L._native.layers_decode(delta.files[0][0], 1, 17)
    assert [(e[0], e[1], e[7]) for e in entries] == [(key(1), True, 7), (key(2), False, 7)]


async def test_a_replacement_removes_what_it_omits():
    h = History(random.Random(6))
    await h.commit({key(i): None for i in range(10)}, [])
    rows = Rows.keys([key(i) for i in range(5, 15)], None)
    files = await h.index().replace_all(rows, name="r1", generation=17)
    st = h.state.committed(1, files)
    assert (files.added, files.removed, st.count) == (5, 5, 10)
    found = await L.LayerIndex(h.io, st).lookup([key(i) for i in range(20)])
    assert found == {key(i): (17, None) for i in range(5, 15)}


async def test_publication_needs_the_inputs_still_there():
    h = History(random.Random(8))
    for c in range(8):
        await h.commit({key(c): None}, [])
    a_ids, a_out = await h.index().merge(2, 3, epoch=1)
    b_ids, b_out = await h.index().merge(3, 3, epoch=2)  # overlapping: another engine's, or a retry
    h.state = h.state.merged(a_ids, a_out)
    assert not h.state.holds(b_ids)
    with pytest.raises(ValueError):
        h.state.merged(b_ids, b_out)
    assert a_out.main.files[0].name.startswith("l1/") and "-e1-" in a_out.main.files[0].name


def test_the_lanes_never_share_an_input_and_attempts_carry_the_life():
    parts = [L.Part((L.FileRef(f"f{i}", 10_000 * 4**i, 1),)) for i in range(6)]
    layers = tuple(L.Layer(i, i, parts[5 - i] if i else parts[5], generation=i + 1) for i in range(6))
    st = L.LayerState(prefix="p/", life="l9", layers=layers, cut=10)
    lane, lo, count = st.plan()
    busy = {x.id for x in st.layers[lo : lo + count]}
    nxt = st.plan(busy=busy)
    assert nxt is None or not busy & {x.id for x in st.layers[nxt[1] : nxt[1] + nxt[2]]}
    assert st.attempt_key(st.layers[:2]).startswith("l9|")


async def test_a_warm_engine_reads_no_object(tmp_path):
    from solera.keys.layer_cache import LayerCache

    rng = random.Random(11)
    h = History(rng)
    for _ in range(60):
        live = list(h.fold[-1]) if h.fold else []
        await h.commit({key(rng.randrange(300)): None for _ in range(8)}, rng.sample(live, min(len(live), 2)))
        await h.merge_some(h.state.head - 5)
    cache = LayerCache(str(tmp_path), disk=10 * 2**20)
    assert await cache.fill(h.io, h.state, sides=True) and cache.warm(h.state)
    idx = L.LayerIndex(h.io, h.state, cache=cache)
    await idx.lookup([key(1)])  # the indexes, into memory
    before = h.io.metrics.snapshot()["gets"]
    idx = L.LayerIndex(h.io, h.state, cache=cache)
    found = await idx.lookup([key(i) for i in range(300)])
    assert found == h.fold[-1]
    assert await walk(idx, h.state.head - 4, 7) == h.expected(h.state.head - 4, h.state.head)
    assert h.io.metrics.snapshot()["gets"] == before  # nothing from the store


async def test_the_engine_installs_what_it_writes_and_evicts_within_budget(tmp_path):
    from solera.keys.layer_cache import LayerCache

    h = History(random.Random(12))
    cache = LayerCache(str(tmp_path), disk=3000)
    for c in range(30):
        idx = L.LayerIndex(h.io, h.state, cache=cache)
        delta = await idx.resolve(SortedEntries.of([key(c), key(c + 100)], None, []))
        files = await idx.write(f"d{c:06d}-a", delta, 10 * c + 7)
        h.state = h.state.committed(c, files)
        h.fold.append(
            {**(h.fold[-1] if h.fold else {}), key(c): (10 * c + 7, None), key(c + 100): (10 * c + 7, None)}
        )
        assert cache.has(h.state.path(files.part.files[0].name))  # installed, no GET
        assert cache.used()[0] <= 3000
    held = h.state.path(h.state.layers[-1].main.files[0].name)
    with cache.hold([held]):
        cache._evict(10**9, None)
        assert cache.has(held)
    found = await L.LayerIndex(h.io, h.state, cache=cache).lookup([key(i) for i in range(200)])
    assert found == h.fold[-1]


@pytest.mark.parametrize("seed", range(3))
async def test_the_delta_interface_over_layers(seed):
    """`solera.keys.delta.delta`, the interface the observed set calls, over
    layers: the same answers as the fold, a key list and pages, to the head
    and to a pinned head (the state then)."""

    from solera.keys.delta import Diff, delta

    rng = random.Random(100 + seed)
    h = History(rng)
    for _ in range(50):
        live = list(h.fold[-1]) if h.fold else []
        await h.commit({key(rng.randrange(300)): None for _ in range(6)}, rng.sample(live, min(len(live), 2)))
        await h.merge_some(max(0, h.state.head - 8))
    head = h.state.head

    def want(p, at, keys=None, take=None):
        return [
            Diff(k.decode(), a, b, g, pl)
            for k, a, b, g, pl in h.expected(
                p, at, keys, None if take is None else (lambda k: take(k.decode()))
            )
        ]

    def take(k: str) -> bool:
        return k.endswith(("3", "8"))

    names = sorted({key(rng.randrange(300)).decode() for _ in range(40)})
    for p in [None, *range(h.state.cut, head)]:
        page = await delta(h.index(), p, keys=names)
        assert page.diffs == want(p, head, {n.encode() for n in names}), p
        got, after = [], None
        while True:
            page = await delta(h.index(), p, after=after, first=rng.choice([1, 5, 1000]), take=take)
            got += page.diffs
            if page.cursor is None:
                break
            after = page.cursor
        assert got == want(p, head, take=take), p
    pinned = max(c for c in h.pinned if c < head and h.pinned[c].cut <= c - 1)
    idx = h.index(h.pinned[pinned])
    assert (await delta(idx, pinned - 1, pinned)).diffs == want(pinned - 1, pinned)


async def test_merge_outputs_carry_their_epoch_and_a_backlog_holds_commits():
    h = History(random.Random(13))
    for c in range(6):
        await h.commit({key(c): None}, [])
    _, out = await h.index().merge(1, 3, epoch=7)
    assert {L.epoch_of(n) for n in out.names()} == {7}
    assert L.epoch_of("d000000000004-a-0.lay") is None  # a commit's delta
    assert not h.state.backlogged()
    many = L.LayerState(
        layers=tuple(L.Layer(i, i, L.Part(), generation=i + 1) for i in range(L.MAX_LAYERS + 1))
    )
    assert many.backlogged()


@pytest.mark.parametrize("streamed", [False, True])
async def test_patches_sparse_or_streamed_write_the_same_delta(monkeypatch, streamed):
    if streamed:
        monkeypatch.setattr(L, "SPARSE_BYTES", 0)  # every patch streams the index
    rng = random.Random(21)
    h = History(rng)
    for _ in range(30):
        await h.commit({key(rng.randrange(300)): None for _ in range(10)}, [])
    live = sorted(h.fold[-1])
    ups = sorted({key(rng.randrange(300)) for _ in range(40)})
    rms = sorted(set(rng.sample(live, 10)) - set(ups))
    run = SortedEntries.of(ups, None, rms)
    files, listed = await h.index().write_patch(
        run, name="p1", generation=10_007, replaced=True, collect=1000
    )
    st = h.state.committed(h.state.head + 1, files)
    now = {**h.fold[-1], **dict.fromkeys(ups, (10_007, None))}
    for k in rms:
        now.pop(k)
    assert await L.LayerIndex(h.io, st).lookup([key(i) for i in range(300)]) == now
    assert sorted(listed[0]) == ups and sorted(listed[1]) == rms
    data = await h.io.read_whole(st.path(files.part.files[0].name), files.part.files[0].size)
    written, removed = L.delta_keys(data, 10_007)
    assert written == ups and removed == rms
    # Updates and removes name what they replaced; adds name nothing.
    assert dict(L.replaced_entries(data, 10_007)) == {
        k: h.fold[-1][k][0] for k in ups + rms if k in h.fold[-1]
    }
    with pytest.raises(ValueError):
        L.replaced_entries(data, 0)  # read without its commit's generation: refused, not wrong
    keys = [
        k
        for chunk in [c async for c in L.DeltaKeys(h.io, st.prefix, files.part, 10_007).chunks(7)]
        for k in chunk
    ]
    assert keys == [k.decode() for k in ups]


async def test_a_streamed_replacement_with_an_overlay():
    h = History(random.Random(22))
    await h.commit({key(i): None for i in range(20)}, [])

    def chunks():  # the store's keys, sorted, a chunk at a time
        yield [key(i) for i in range(10, 25)]
        yield [key(i) for i in range(25, 30)]

    overlay = SortedEntries.of([key(12), key(40)], None, [key(13)])
    files, _ = await h.index().write_replace(chunks=chunks(), overlay=overlay, name="r", generation=17)
    st = h.state.committed(1, files)
    found = await L.LayerIndex(h.io, st).lookup([key(i) for i in range(50)])
    assert sorted(found) == sorted({key(i) for i in range(10, 30)} - {key(13)} | {key(40)})


async def test_the_resolvers_delta_is_the_writers_and_its_candidate_installs_without_a_get(tmp_path):
    from solera.keys.layer_cache import LayerCache

    rng = random.Random(23)
    h = History(rng)
    for _ in range(12):
        await h.commit({key(rng.randrange(100)): None for _ in range(6)}, [])
    cache = LayerCache(str(tmp_path), disk=2**20)
    idx = L.LayerIndex(h.io, h.state, cache=cache)
    run = SortedEntries.of([key(1), key(150)], None, [key(2)])
    patch = await idx.compute(run, generation=127, replaced=True)
    written = await h.index().resolve(run, generation=127, replaced=True)
    assert [f[0] for f in patch.files] == [f[0] for f in written.files]
    full = SortedEntries.of([key(i) for i in range(0, 100, 2)], None, [])
    replaced = await idx.compute(full, replace=True)
    assert replaced.removed == sum(1 for k in h.fold[-1] if k not in {key(i) for i in range(0, 100, 2)})
    path = h.state.path("000000000012-a1-0.lay")
    cache.offer(path, patch.files[0][0])
    gets = h.io.metrics.snapshot()["gets"]
    assert cache.committed(path, len(patch.files[0][0]), h.state.prefix) and cache.has(path)
    assert h.io.metrics.snapshot()["gets"] == gets
    cache.offer(h.state.path("000000000013-a2-0.lay"), b"x")
    cache.forget("a2")
    assert not cache.committed(h.state.path("000000000013-a2-0.lay"), 1, h.state.prefix)


async def test_the_engine_resolves_a_workers_request_from_its_warm_cache(tmp_path):
    from solera.keys import resolver as R
    from solera.keys.layer_cache import LayerCache

    rng = random.Random(29)
    h = History(rng)
    for _ in range(10):
        await h.commit({key(rng.randrange(60)): None for _ in range(5)}, [])
    await h.merge_some(h.state.head)
    cache = LayerCache(str(tmp_path), disk=2**20)
    resolver = R.Resolver(cache, h.io)
    c, g = h.state.head + 1, 10 * (h.state.head + 1) + 7
    p = R.Prepared("", c, g, h.state, h.state.head, False, replaced=True)
    run = SortedEntries.of([key(3), key(70)], None, [key(4), key(90)])
    ask = R.Ask("out", "", "patch", c, g, h.state.prefix, h.state.head, run)

    async def answer():
        body = await resolver.resolve("a1", R.request("w", [ask]), lambda n: p, lambda: True)
        return R.answers(body)["out"]

    first, _ = await answer()
    assert first == {"name": "out", "result": "declined", "reason": "cold"}
    await cache.fill(h.io, h.state)  # joins the fill the decline started
    second, data = await answer()
    assert second["result"] == "delta"
    written = await h.index().resolve(run, generation=g, replaced=True)
    assert data == written.files[0][0]  # the writer's own delta, replaced generations included
    name = f"{c:012d}-a1"
    await h.io.write(h.state.path(f"{name}-0.lay"), data)
    state = h.state.committed(c, R.delta_files(second, name, g))
    assert state.count == h.state.count + written.added - written.removed
    assert cache.committed(h.state.path(f"{name}-0.lay"), len(data), h.state.prefix)  # its candidate
    rows, _ = await L.LayerIndex(h.io, state).delta(h.state.head, keys=[key(3), key(4), key(70), key(90)])
    assert [r[0] for r in rows] == sorted({key(3), key(70)} | ({key(4)} & set(h.fold[-1])))


async def test_indexes_sharing_a_cache_never_read_each_others_files(tmp_path):
    """One attempt names its output's delta and its stored outcomes' delta
    alike (`{commit}-{attempt}-0.lay`), under two prefixes: the engine's
    cache keys what it holds by full path, so each index reads its own."""

    from solera.keys.layer_cache import LayerCache

    io = ObjectIO(MemoryStore())
    cache = LayerCache(str(tmp_path), disk=2**20)
    states = {}
    for prefix, keys in (("keys/out/_/", [b"a", b"b"]), ("failures/out/_/", [b"x"])):
        state = L.LayerState(prefix=prefix, life="1")
        files, _ = await L.LayerIndex(io, state).write_patch(
            SortedEntries.of(keys), name="000000000000-att", generation=7
        )
        states[prefix] = state.committed(0, files)
    for prefix, keys in (("keys/out/_/", [b"a", b"b"]), ("failures/out/_/", [b"x"])):
        found = await L.LayerIndex(io, states[prefix], cache=cache).lookup([b"a", b"b", b"x"])
        assert sorted(found) == keys, prefix
