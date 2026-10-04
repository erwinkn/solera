"""Format v4 prototype (docs/key-index-design.md, native/src/v4.rs): spans
holding several versions of a key, checked key by key against the
per-commit fold.

Random exact histories over a few keys; spans merged at random (adjacent
windows, across live endpoints, into the base); endpoints born at the head
+ 1 and retired at random; tiny blocks and files, so a key's versions cross
block and file boundaries. After every step, for every reserved range:
`changes(P, N)` (every key and class, paged one key at a time with a merge
between pages), lookups at the head and at each endpoint, scans, and the
read-ahead rule.
"""

from __future__ import annotations

import random
import zlib

import pytest
from solera import _native
from solera import keys as K

KEYS = [b"a", b"b", b"c", b"d", b"e", b"f"]
TINY = {"block_size": 16, "max_file_bytes": 48}
CLASS = {(False, True): 0, (True, True): 1, (True, False): 2, (False, False): 3}


def gen(c: int) -> int:
    """The generation commit `c` writes at (commit 0, the base: 1)."""

    return 1 if c == 0 else 10 * c


def blocks_of(files: list[bytes]) -> tuple[list[bytes], int]:
    out, codec = [], 1
    for data in files:
        footer = K.parse_footer(data[-K.FOOTER_SIZE :])
        tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
        codec = tail["codec"]
        out += [data[off : off + size] for _, off, size, _, _ in tail["blocks"]]
    return out, codec


class History:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.truth: list[dict[bytes, int]] = []  # live keys (key -> generation) after each commit
        self.touched: list[set[bytes]] = []
        self.spans: list[tuple[int, int, list[bytes]]] = []  # oldest first
        self.live: set[int] = set()  # endpoints (commit numbers)
        base = {k: 1 for k in KEYS if rng.random() < 0.5}
        self.truth.append(base)
        self.touched.append(set(base))
        data = K.encode_file(sorted(base), [1] * len(base), bytes(len(base)), predecessors=[None] * len(base))
        files, _ = _native.merge_spans([blocks_of([data])[0]], [1], [], True, **TINY)
        self.spans.append((0, 0, files))

    @property
    def head(self) -> int:
        return len(self.truth) - 1

    def commit(self) -> None:
        c = self.head + 1
        g = gen(c)
        before = self.truth[-1]
        after = dict(before)
        entries = []
        for k in KEYS:
            if self.rng.random() < 0.4:
                if k in before and self.rng.random() < 0.4:
                    entries.append((k, True, before[k]))
                    del after[k]
                else:
                    entries.append((k, False, before.get(k)))
                    after[k] = g
        self.truth.append(after)
        self.touched.append({k for k, _, _ in entries})
        data = K.encode_file(
            [k for k, _, _ in entries],
            [g] * len(entries),
            bytes(d for _, d, _ in entries),
            predecessors=[p for _, _, p in entries],
        )
        files, _ = _native.merge_spans([blocks_of([data])[0]], [1], [], False, **TINY)
        self.spans.append((c, c, files))

    def merge(self, lo: int, count: int) -> None:
        group = self.spans[lo : lo + count]
        runs = [blocks_of(s[2]) for s in reversed(group)]
        files, _ = _native.merge_spans(
            [r[0] for r in runs], [r[1] for r in runs], sorted(gen(e) for e in self.live), lo == 0, **TINY
        )
        self.spans[lo : lo + count] = [(group[0][0], group[-1][1], files)]

    def runs(self, p: int, n: int) -> tuple[list[list[bytes]], list[int]]:
        mine = [s for s in self.spans if s[1] >= p and s[0] <= n]
        got = [blocks_of(s[2]) for s in reversed(mine)]
        return [r[0] for r in got], [r[1] for r in got]

    # -- the truth --------------------------------------------------------------------------

    def expected_changes(self, p: int, n: int) -> dict[bytes, tuple[int, int]]:
        """Keys changed in [p, n]: their class, and their generation at n (the tombstone's if gone)."""

        out = {}
        before, at = self.truth[p - 1], self.truth[n]
        for k in set().union(*self.touched[p : n + 1]):
            last = max(c for c in range(p, n + 1) if k in self.touched[c])
            out[k] = (CLASS[(k in before, k in at)], gen(last))
        return out


def scan(runs: list[list[bytes]], codecs: list[int], bound: int | None, limit: int = 3):
    """Every live key at `bound` through the streaming page job (`Merge.read`),
    pages of `limit`, each run fed one block at a time."""

    out, after = [], None
    while True:
        job = _native.Merge.read(len(runs), after=after, limit=limit, bound=bound)
        fed = [0] * len(runs)
        page = None
        while (step := job.step()) is not None:
            kind, x = step
            if kind == "run":
                if fed[x] == len(runs[x]):
                    job.end(x)
                else:
                    b = runs[x][fed[x]]
                    job.feed(x, b, [(0, len(b), zlib.crc32(b))], codecs[x])
                    fed[x] += 1
            else:
                page = x
        keys, _, gens, _, _, last, more = page
        out += zip(keys, gens, strict=True)
        if not more:
            return [k for k, _ in out], [g for _, g in out]
        after = last


def changes(h: History, p: int, n: int, limit: int = 10**6, between=None) -> dict[bytes, tuple[int, int]]:
    out, after = {}, None
    while True:
        runs, codecs = h.runs(p, n)
        keys, classes, gens, _, _, last, more = _native.span_changes(
            runs, codecs, after, None, limit, gen(p), gen(n + 1)
        )
        for k, c, g in zip(keys, classes, gens, strict=True):
            assert k not in out
            out[k] = (c, g)
        if not more:
            return out
        after = last
        if between:
            between()


def check(h: History, rng: random.Random) -> int:
    checks = 0
    ends = sorted(h.live | {h.head + 1})
    for p in sorted(e for e in h.live if e >= 1):
        for n1 in (e for e in ends if e > p):
            n = n1 - 1
            want = h.expected_changes(p, n)
            assert changes(h, p, n) == want, (p, n)
            # Read-ahead: a key read at r, delivered as it was then.
            got = changes(h, p, n)
            for r in range(p, n + 1):
                for k, (_c, g) in got.items():
                    delivered = k in h.truth[r]
                    mine = None if g <= gen(r) else CLASS[(delivered, k in h.truth[n])]
                    changed = any(k in h.touched[x] for x in range(r + 1, n + 1))
                    truth = CLASS[(delivered, k in h.truth[n])] if changed else None
                    assert mine == truth, (p, r, n, k)
                    checks += 1
            checks += len(want) + 1
    # Lookups and scans at the head and at each endpoint (the state at e - 1).
    for e in sorted(h.live | {h.head + 1}):
        if e == 0:
            continue
        bound = None if e == h.head + 1 else gen(e)  # None: the head
        state = h.truth[e - 1]
        runs, codecs = h.runs(0, e - 1)
        found, gens, deleted, _ = _native.span_lookup(runs, codecs, KEYS, bound)
        for k, f, g, d in zip(KEYS, found, gens, deleted, strict=True):
            live = bool(f) and not d
            assert live == (k in state), (e, k)
            if live:
                assert g == state[k]
            checks += 1
        keys, gens = scan(runs, codecs, bound)
        assert dict(zip(keys, gens, strict=True)) == state, e
        checks += 1
    return checks


def step(h: History, rng: random.Random) -> None:
    x = rng.random()
    if x < 0.45:
        h.commit()
    elif x < 0.6:
        h.live.add(h.head + 1)  # born at the head + 1
    elif x < 0.7 and h.live:
        h.live.discard(rng.choice(sorted(h.live)))
    elif len(h.spans) > 1:
        count = rng.randint(2, min(4, len(h.spans)))
        h.merge(rng.randrange(len(h.spans) - count + 1), count)


@pytest.mark.parametrize("seed", range(150))
def test_spans_agree_with_the_fold(seed):
    rng = random.Random(seed)
    h = History(rng)
    for _ in range(40):
        step(h, rng)
        check(h, rng)


@pytest.mark.parametrize("seed", range(40))
def test_pages_survive_merges_between_them(seed):
    rng = random.Random(1000 + seed)
    h = History(rng)
    for _ in range(25):
        step(h, rng)
    for p in sorted(e for e in h.live if e >= 1):
        n = h.head
        h.live |= {p, n + 1}  # the reader's reservations hold while it pages

        def between():
            if len(h.spans) > 1:
                count = rng.randint(2, min(4, len(h.spans)))
                h.merge(rng.randrange(len(h.spans) - count + 1), count)
            if rng.random() < 0.5:
                h.commit()
                h.live.add(h.head + 1)

        assert changes(h, p, n, limit=1, between=between) == h.expected_changes(p, n)


def test_a12_1_base_keeps_later_tombstones():
    """Consumer at 1; commit 1 adds d, delivered live by a selection at 1;
    commit 2 removes d; a base merge crossing endpoint 1 must keep d's
    tombstone, or the read-ahead never sees the removal."""

    rng = random.Random(0)
    h = History(rng)
    h.truth[0], h.touched[0] = {}, set()
    h.spans = [(0, 0, _native.merge_spans([[]], [1], [], True, **TINY)[0])]
    for c, (deleted, pred) in ((1, (False, None)), (2, (True, 10))):
        data = K.encode_file([b"d"], [gen(c)], bytes([deleted]), predecessors=[pred])
        files, _ = _native.merge_spans([blocks_of([data])[0]], [1], [], False, **TINY)
        h.spans.append((c, c, files))
        h.truth.append({} if deleted else {b"d": gen(c)})
        h.touched.append({b"d"})
    h.live = {1, 3}
    h.merge(0, 3)  # into the base, across endpoint 1
    got = changes(h, 1, 2)
    assert got == {b"d": (3, 20)}  # neither relative to 1, but present: the read-ahead (r = 1) sees 20 > 10
    c, g = got[b"d"]
    assert g > gen(1)  # changed after it was read: delivered live, now gone, so removed


def test_a12_2_changes_clipped_at_n():
    """k added at 1, removed at 2, one span [1, 2] keeping both versions:
    changes(1, 1) is added, not neither."""

    rng = random.Random(0)
    h = History(rng)
    h.truth[0], h.touched[0] = {}, set()
    h.spans = [(0, 0, _native.merge_spans([[]], [1], [], True, **TINY)[0])]
    for c, (deleted, pred) in ((1, (False, None)), (2, (True, 10))):
        data = K.encode_file([b"k"], [gen(c)], bytes([deleted]), predecessors=[pred])
        files, _ = _native.merge_spans([blocks_of([data])[0]], [1], [], False, **TINY)
        h.spans.append((c, c, files))
        h.truth.append({} if deleted else {b"k": gen(c)})
        h.touched.append({b"k"})
    h.live = {1, 2, 3}
    h.merge(1, 2)
    assert changes(h, 1, 1) == {b"k": (0, 10)}
    assert changes(h, 1, 2) == {b"k": (3, 20)}


def test_calibration_merges_ignoring_endpoints_are_caught():
    """The check must fail when merges drop the versions live endpoints see."""

    caught = 0
    for seed in range(30):
        rng = random.Random(seed)
        h = History(rng)
        h.merge = lambda lo, count, h=h: _merge_blind(h, lo, count)
        try:
            for _ in range(40):
                step(h, rng)
                check(h, rng)
        except AssertionError:
            caught += 1
    assert caught >= 20, caught


def _merge_blind(h: History, lo: int, count: int) -> None:
    group = h.spans[lo : lo + count]
    runs = [blocks_of(s[2]) for s in reversed(group)]
    files, _ = _native.merge_spans([r[0] for r in runs], [r[1] for r in runs], [], lo == 0, **TINY)
    h.spans[lo : lo + count] = [(group[0][0], group[-1][1], files)]
