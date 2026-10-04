"""The two views against the per-commit fold, on small random histories.

    uv run pytest bench/keys/views/test_twoviews.py -q

Every commit is resolved exactly against K, as the bench's are. After every
commit and its upkeep (packs, nodes, base merges, deletions below the floor,
and with `horizon` the anchored compaction): K's lookups and pages equal the
fold at the head, a pinned K equals the fold at its commit, and
`changes(P, N)` for every reader start P and every N up to the head delivers
exactly the keys whose state differs between P - 1 and N, each with its
class and state at N.
"""

from __future__ import annotations

import asyncio
import random
import sys
from pathlib import Path

import pytest
from obstore.store import MemoryStore
from solera.keys import SortedEntries
from solera.keys.index import KeyIndex, Options

sys.path.insert(0, str(Path(__file__).parent))
from twoviews import ADDED, REMOVED, UPDATED, PackIO, TwoViews, read_ahead  # noqa: E402

KEYS = [b"k%03d" % i for i in range(80)]


def classes(then: dict, now: dict) -> dict:
    out = {}
    for k in set(then) | set(now):
        a, b = then.get(k, 0), now.get(k, 0)
        if a == b:
            continue
        out[k] = (0 if b else 2) if not a else (1 if b else 2)
    return out


async def history(
    seed: int,
    *,
    b: int,
    commits: int,
    codec: int,
    unpin_at: int | None = None,
    retention: str = "floor",
    fixed: list[tuple[int, int | None]] = (),
    window: int = 0,
):
    rng = random.Random(seed)
    o = Options(block_size=512, codec=codec, max_file_bytes=4096)
    io = PackIO(MemoryStore())
    v = TwoViews(io, "t/", o, b=b, top=5, retention=retention)
    v.window = window
    folds = []
    base = sorted(rng.sample(KEYS, 30))
    files, _ = await KeyIndex(io, "t/", v.k().state, v.base_o).replace(
        [[k.decode() for k in base]], 0, "base", generation=1
    )
    v.commit(0, files.files)
    folds.append(dict.fromkeys(base, 1))
    readers = {} if fixed else {r: 1 for r in range(3)}  # reader -> next
    pinned = None
    for c in range(1, commits + 1):
        g = 10 * c
        live = [k for k, x in folds[-1].items() if x]
        touched = rng.sample(KEYS, rng.randint(1, 12))
        rms = sorted(k for k in touched if k in live and rng.random() < 0.3)
        ups = sorted(k for k in touched if k not in rms)
        files, _ = await v.k().resolve(SortedEntries.of(ups, None, rms), commit_number=c, attempt=f"a{c}", generation=g)
        v.commit(c, files.files)
        fold = dict(folds[-1])
        for k in ups:
            fold[k] = g
        for k in rms:
            fold[k] = 0
        folds.append(fold)
        for r in readers:
            if rng.random() < 0.15:
                readers[r] = c + 1
        if c == commits // 3 and not fixed:
            v.pin("pass")
            pinned = c
        if unpin_at == c and pinned:
            await v.unpin("pass")
            pinned = None
        v.intervals = [(p, None) for p in readers.values()] + ([(pinned + 1, None)] if pinned else [])
        v.intervals += [(a, e) for a, e in fixed if a <= c]
        await v.upkeep()

        head = {k: x for k, x in fold.items() if x}
        got = await v.lookup(KEYS)
        assert {k: x[0] for k, x in got.items()} == head, (seed, c)
        keys, gens, _, nxt = await v.page(None, 1000)
        assert dict(zip(keys, gens, strict=True)) == head and nxt is None, (seed, c)
        if pinned:
            keys, gens, _, _ = await v.page(None, 1000, pin="pass")
            assert dict(zip(keys, gens, strict=True)) == {k: x for k, x in folds[pinned].items() if x}
        for p, end in sorted(set(v.intervals), key=lambda x: (x[0], 2**62 if x[1] is None else x[1])):
            if p > c:
                continue
            # A floor keeps every N; covers keep the reserved ends only.
            ends = range(p, c + 1) if retention == "floor" else (c if end is None else min(end, c),)
            if p in v.snaps:  # behind the window: the snapshot, merge-joined with the head
                want = classes(folds[p - 1], folds[c])
                got, after, st = {}, None, {}
                while True:
                    page = await v.snapshot_changes(p, after, 7, st)
                    for i, k in enumerate(page.keys):
                        got[k] = page.classes[i]
                        state = 0 if page.deleted[i] else page.generations[i]
                        assert page.deleted[i] or state == folds[c].get(k, 0), (seed, c, p, k)
                    if page.cursor is None:
                        break
                    after = page.cursor
                assert got == want, (seed, c, p, "snapshot")
                some = sorted(rng.sample(KEYS, 10))
                one = await v.snapshot_changes_of(p, some)
                assert dict(zip(one.keys, one.classes, strict=True)) == {k: x for k, x in want.items() if k in some}
                continue
            if retention == "window":
                ends = (c,)
            for n in ends:
                want = classes(folds[p - 1], folds[n])
                got, after = {}, None
                while True:
                    page = await v.changes_page(p, n, after, 7)
                    for i, k in enumerate(page.keys):
                        got[k] = page.classes[i]
                        state = 0 if page.deleted[i] else page.generations[i]
                        assert state == folds[n].get(k, 0), (seed, c, p, n, k)
                    if page.cursor is None:
                        break
                    after = page.cursor
                assert got == want, (seed, c, p, n)
                some = rng.sample(KEYS, 10)
                one = await v.changes_of(p, n, sorted(some))
                assert dict(zip(one.keys, one.classes, strict=True)) == {k: x for k, x in want.items() if k in some}
    return v


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("b", [2, 4])
def test_two_views_follow_the_fold(seed, b):
    asyncio.run(history(seed, b=b, commits=40, codec=1 + seed % 2))


@pytest.mark.parametrize("seed", range(6))
def test_cover_retention_follows_the_fold(seed):
    asyncio.run(history(200 + seed, b=2, commits=40, codec=2, retention="cover"))


@pytest.mark.parametrize("seed", range(6))
def test_window_snapshots_follow_the_fold(seed):
    # T keeps 6 commits; readers that fall behind it catch up from snapshots.
    v = asyncio.run(history(400 + seed, b=2, commits=50, codec=2, retention="window", window=6))
    assert v.written["snapshot"].entries, "some reader fell behind the window"


@pytest.mark.parametrize("seed", range(4))
def test_cover_retention_a25_counterexample(seed):
    # A25 R5: readers at 4 and 16, a paused pass over [4, 7] landing at 8
    # (and one over [4, 6] landing at 7, an end no aligned node shares). Each
    # interval stays exactly readable from single-version nodes, after every
    # commit, base merges and deletions included.
    fixed = [(4, None), (16, None), (4, 7), (8, None), (4, 6), (7, None)]
    v = asyncio.run(history(300 + seed, b=2, commits=48, codec=1, retention="cover", fixed=fixed))
    # Without the paused pass's reserved end, [4, 6] is no longer held: the
    # read fails loudly, never answering from a node that covers more.
    v.intervals = [(4, None), (16, None)]
    asyncio.run(v.upkeep())
    with pytest.raises(LookupError):
        v.cover(4, 6)


@pytest.mark.parametrize("seed", range(4))
def test_retention_keeps_only_chains_and_the_floor(seed):
    # The pin goes at commit 30: afterwards nothing below the readers' floor
    # survives but K's chain, and every read above still matches the fold.
    v = asyncio.run(history(100 + seed, b=2, commits=60, codec=2, unpin_at=30))
    floor, keep = v.floor(), v.chains()
    assert all(s >= floor or (j, s) in keep for j, s in v.nodes)


def test_read_ahead_guard():
    # A selection at r = 5 delivers d live (added at 5); a pass paused at N = 4
    # resumes. K at 4 has no d: without the guard it would say removed, and
    # undo the selection. The entry is newer than N: the pass leaves d alone.
    assert read_ahead({b"d": (5, 50, True)}, {}, 4) == {b"d": None}
    assert read_ahead({b"d": (4, 40, True)}, {}, 4) == {b"d": None}
    # Read before N: classed against K at N.
    assert read_ahead({b"d": (3, 30, True)}, {}, 4) == {b"d": REMOVED}
    assert read_ahead({b"d": (3, 30, True)}, {b"d": (40, None)}, 4) == {b"d": UPDATED}
    assert read_ahead({b"d": (3, 30, True)}, {b"d": (30, None)}, 4) == {b"d": None}
    assert read_ahead({b"d": (3, 0, False)}, {b"d": (40, None)}, 4) == {b"d": ADDED}
    assert read_ahead({b"d": (3, 0, False)}, {}, 4) == {b"d": None}
