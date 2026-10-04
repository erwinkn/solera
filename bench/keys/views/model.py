"""W53: the two views' structure on metadata (corrected after A25 R7, R8).

The time view (T) is an aligned tree over commit numbers, fanout b: level 1
packs of b deltas, level j >= 2 nodes over b^j commits. The key view (K) is
a base as of commit w plus T's cover of [w + 1, head] (its chain); the base
absorbs the chain when a node at the base level jb (the smallest whose node
holds a quarter of the base) completes. Keys are uniform: n changes over
`keys` live keys touch keys * (1 - exp(-n / keys)) distinct ones, the
density model of the span replays (`bench/keys/spans.py`).

Prints:

1. K's runs, every head of a base cycle enumerated (A25 R7: sampling every
   32nd head hid the low levels): files opened (a pack's sections are one
   object) and merge inputs (each section a run), mean and max.
2. Storage over a whole base cycle (A25 R8), entries per live key, mean and
   peak: the base, K's chain, and T under each retention:
   - window W: T keeps every level of the last W commits (Erwin's policy;
     the same as a commit horizon X = W with the floor rule);
   - cover: T keeps the covers of the readers' intervals (here 100 daily
     readers spread over the day, one hourly, one every commit) and K's chain;
   plus, for the window, each laggard's snapshot (about one live index each).
   The base watermark holds K's chain only, never every level since it.

Usage: python3 bench/keys/views/model.py
"""

from __future__ import annotations

import math
import random

COMMIT = 1_000
DAY = 8_640


def distinct(n: float, keys: int) -> float:
    return keys * -math.expm1(-n / keys)


class Shape:
    def __init__(self, keys: int, b: int = 4, r: float = 4, top: int = 10):
        self.keys, self.b, self.r, self.top = keys, b, r, top
        self.jb = next(j for j in range(2, 40) if self.entries(j) * r >= keys)

    def entries(self, j: int) -> float:
        n = self.b**j * COMMIT
        return n if j <= 1 else distinct(n, self.keys)

    def cover(self, first: int, last: int, top: int | None = None) -> list[tuple[int, int]]:
        """(level, start) units tiling [first, last], every level built."""

        top = self.top if top is None else top
        out, c, b = [], first, self.b
        while c <= last:
            j = next((j for j in range(top, 0, -1) if c % b**j == 0 and c + b**j - 1 <= last), 0)
            out.append((j, c))
            c += b**j
        return out

    def unit_entries(self, unit) -> float:
        return self.entries(unit[0]) if unit[0] >= 1 else COMMIT


def k_runs(s: Shape):
    """Every head of one base cycle: files opened, merge inputs."""

    period = s.b**s.jb
    objs, inputs = [], []
    for h in range(period):  # the base holds commits up to the cycle's start; the chain is [0, h]
        cv = s.cover(0, h, s.jb - 1)
        packs = {c - c % s.b for j, c in cv if j == 0}
        objs.append(1 + sum(1 for j, _ in cv if j >= 1) + len(packs))
        inputs.append(1 + len(cv))
    return sum(objs) / period, max(objs), sum(inputs) / period, max(inputs)


def chain_entries(s: Shape, h: int) -> float:
    return sum(s.unit_entries(u) for u in s.cover(0, h, s.jb - 1))


def window_entries(s: Shape, w: int) -> float:
    """T's entries for a window of w commits: every level's complete nodes in it."""

    total = 0.0
    for j in range(1, s.top + 1):
        if s.b**j > w:
            break
        total += (w / s.b**j) * s.entries(j)
    return total


def cover_entries(s: Shape, head: int, starts: list[int]) -> float:
    units = set()
    for p in starts:
        units |= set(s.cover(p, head))
    return sum(s.unit_entries(u) for u in units)


def main():
    rng = random.Random(1)
    print("1. K's runs over a whole base cycle, b = 4, r = 4 (every head)")
    print("keys\tbase level\tcycle (commits)\tfiles opened mean (max)\tmerge inputs mean (max)")
    shapes = {k: Shape(k) for k in (1_000_000, 100_000_000)}
    for k, s in shapes.items():
        om, ox, im, ix = k_runs(s)
        print(f"{k:,}\t{s.jb}\t{s.b**s.jb:,}\t{om:.1f} ({ox})\t{im:.1f} ({ix})")

    print("\n2. Storage over a whole base cycle, entries per live key: mean (peak)")
    print("keys\tpolicy\tbase\tK chain\tT\tsnapshots per laggard\ttotal")
    for k, s in shapes.items():
        period = s.b**s.jb
        heads = range(0, period, max(1, period // 4096))
        chain = [chain_entries(s, h) / k for h in heads]
        cm, cx = sum(chain) / len(chain), max(chain)
        for name, w in (("window 1 day", DAY), ("window 7 days", 7 * DAY), ("window 30 days", 30 * DAY)):
            t = window_entries(s, w) / k
            print(f"{k:,}\t{name}\t1.00\t{cm:.2f} ({cx:.2f})\t{t:.2f}\t~1 each (a live index)\t{1 + cm + t:.2f} ({1 + cx + t:.2f})")
        # cover: 100 daily readers spread over the day, an hourly one, one every commit
        vals = []
        for _ in range(60):
            head = rng.randrange(10 * DAY, 20 * DAY)
            starts = [head - rng.randrange(DAY) for _ in range(100)] + [head - rng.randrange(360), head]
            vals.append(cover_entries(s, head, starts) / k)
        print(f"{k:,}\tcover, 100 daily + hourly\t1.00\t{cm:.2f} ({cx:.2f})\t{sum(vals) / len(vals):.2f} ({max(vals):.2f})\tnone\t"
              f"{1 + cm + sum(vals) / len(vals):.2f} ({1 + cx + max(vals):.2f})")
        one = []
        for lag in (10_000, 100_000):
            head = 400_000
            one.append(f"{lag:,} behind: {cover_entries(s, head, [head - lag]) / k:.2f}")
        print(f"{k:,}\tcover, one laggard alone\t\t\t{'; '.join(one)}")


if __name__ == "__main__":
    main()
