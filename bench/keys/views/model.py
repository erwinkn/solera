"""W53 phase 1: the two views' structure, replayed on metadata.

The time view (T) is an aligned tree over commit numbers: a level-j node
covers commits [i * b^j, (i + 1) * b^j - 1] and holds each key's net change
there. Level 1 is a pack of b commits' deltas (copied, not merged); levels 2
and up are merged. The key view (K) is a base (every live key, as of commit
w) plus T's nodes from w + 1 to the head: the "head chain". The base absorbs
the chain once the chain holds a 1/r share of the base's entries.

Keys are uniform: n changes over `keys` live keys touch
keys * (1 - exp(-n / keys)) distinct ones. That is the density model the span
replays (`bench/keys/spans.py`) use too, so the numbers compare with
`key-index-design.md`'s replayed tables, not with its measured ones.

Prints, per index size, fanout b and base ratio r:
- background writes per entry committed (the delta itself not counted), for T
  (packs and merged nodes) and for K (base merges);
- K's runs (the base's files count as one run) and entries per live key;
- per reader lag: the nodes its catch-up opens and what it reads relative to
  what changed;
- T's stored entries with a reader a day behind (the floor).

Usage: python3 bench/keys/views/model.py
"""

from __future__ import annotations

import math
from dataclasses import dataclass

COMMIT = 1_000  # keys per commit
DAY = 8_640  # commits a day at one per 10 s


def distinct(n: float, keys: int) -> float:
    """Distinct keys among n uniform changes over `keys` keys."""

    return keys * -math.expm1(-n / keys)


@dataclass
class Shape:
    keys: int
    b: int
    r: float

    def entries(self, level: int) -> float:
        """Entries in one node of `level` (level 0: a delta)."""

        n = self.b**level * COMMIT
        return n if level <= 1 else distinct(n, self.keys)

    def base_level(self) -> int:
        """The lowest level whose node holds a 1/r share of the base: the base
        merges once a node of it completes (so the chain never reaches it)."""

        j = 1
        while self.chain_entries_full(j) < self.keys / self.r:
            j += 1
        return j

    def chain_entries_full(self, j: int) -> float:
        return self.entries(j)

    def cover(self, first: int, last: int, top: int) -> list[int]:
        """The canonical cover of commits [first, last]: node levels, largest
        aligned nodes first, never above `top`."""

        levels = []
        c = first
        while c <= last:
            j = 0
            while j < top and c % self.b ** (j + 1) == 0 and c + self.b ** (j + 1) - 1 <= last:
                j += 1
            levels.append(j)
            c += self.b**j
        return levels

    def chain(self, head: int, top: int) -> list[int]:
        """K's runs above the base at `head`: the cover from the last base
        merge (at a multiple of b^top) to the head."""

        w = (head // self.b**top) * self.b**top
        return self.cover(w, head, top) if head >= w else []


def replay(keys: int, b: int, r: float, top_reader: int = 40_000):
    s = Shape(keys, b, r)
    jb = s.base_level()
    # Levels a reader may use: up to the lag the per-position budget allows
    # (a full read once a catch-up would read more), here top_reader commits.
    jt = max(jb, math.ceil(math.log(top_reader, b)))

    # Background writes per entry committed: packs copy every entry once,
    # merged levels write their distinct keys; levels above the base's are
    # built only while a reader is far enough behind to use them.
    t_k = 1.0 + sum(s.entries(j) / (b**j * COMMIT) for j in range(2, jb + 1))
    t_readers = sum(s.entries(j) / (b**j * COMMIT) for j in range(jb + 1, jt + 1))
    base = keys / (b**jb * COMMIT)  # a full base rewrite every b^jb commits

    # K's runs over a base-merge cycle.
    runs, live = [], []
    period = b**jb
    for head in range(0, period, max(1, period // 2_000)):
        ch = s.chain(head, jb)
        runs.append(1 + len(ch))
        live.append((keys + sum(s.entries(j) for j in ch)) / keys)

    # Catch-ups: a reader `lag` commits behind the head, over many alignments.
    readers = {}
    for lag in (1, 100, 360, 8_640, 10_000):
        nodes, ratio = [], []
        for start in range(10_000, 10_000 + 4 * b**jt, max(1, (4 * b**jt) // 997)):
            cv = s.cover(start, start + lag - 1, jt)
            nodes.append(len(cv))
            ratio.append(sum(s.entries(j) for j in cv) / distinct(lag * COMMIT, keys))
        readers[lag] = (sum(nodes) / len(nodes), max(nodes), sum(ratio) / len(ratio))

    # T's stored entries with the floor a day back: every node from it on.
    stored = sum((DAY / b**j) * s.entries(j) for j in range(1, jt + 1) if b**j <= DAY)
    return jb, jt, t_k, t_readers, base, runs, live, readers, stored


def main():
    print("keys\tb\tr\tbase level\ttop\tT for K\tT for readers\tbase\tK runs mean (max)\t"
          "K entries/live key\t" + "\t".join(f"lag {x}: nodes mean (max), read x changed" for x in
                                             (1, 100, 360, 8640, 10000)) + "\tT stored, floor a day back (entries / live key)")
    for keys in (1_000_000, 100_000_000):
        for b in (2, 4, 8, 16):
            for r in (2, 4, 8):
                jb, jt, tk, tr, base, runs, live, readers, stored = replay(keys, b, r)
                cells = [f"{keys:,}", str(b), str(r), str(jb), str(jt), f"{tk:.2f}", f"{tr:.2f}", f"{base:.2f}",
                         f"{sum(runs) / len(runs):.1f} ({max(runs)})", f"{sum(live) / len(live):.2f}"]
                for lag in (1, 100, 360, 8_640, 10_000):
                    m, x, q = readers[lag]
                    cells.append(f"{m:.1f} ({x}) {q:.2f}x")
                cells.append(f"{stored / keys:.2f}")
                print("\t".join(cells))


if __name__ == "__main__":
    main()
