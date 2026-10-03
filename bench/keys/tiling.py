"""One tiling of spans (docs/key-index-design.md): what the index costs
when its files are key-sorted spans tiling commit time, merged only
between observer boundaries.

    uv run python bench/keys/tiling.py --sizes 1e6,1e8 --policies 4:1000

Replays a workload on span metadata alone, nothing written, with the
density model of amplification.py: a span of random keys holds a share `d`
of the N existing keys (as an independent random subset) plus `extra`
brand-new keys, so merging two spans gives d = 1 - (1-d1)(1-d2) and
extra = e1 + e2. A commit changes `--keys` keys: 97.5% in the existing
key space (updates, removes, re-adds), 2.5% brand new (the 90/5/5 mix of
presence.py; adds and removes balance, so N stays put).

The tiling: the base (N live entries, no tombstones), then spans [a, b].
A boundary is a commit an observer needs to be a span start: here a
consumer's position. The spans between two boundaries form a group (the
first starts with the base); merges (newest entry, oldest predecessor)
stay inside a group:

- into the oldest: once a group's other spans hold 1 / `--base-ratio` of
  its oldest span's entries, the group merges into one span (the first
  group: into the base, one streaming rewrite of it). `--flat` applies
  this to the first group only;
- by size: otherwise a span older than a bigger neighbour merges with it,
  and `way` adjacent spans of one size class (log_way of entries / 1,000)
  merge; past `cap` spans in a group, the `way` adjacent spans with the
  fewest entries merge (measured degenerate: keep `cap` out of reach).

Consumers read every `period` commits (10 s commits: 360 is hourly, 8,640
daily): each read takes its position to the head + 1, and costs reading the
spans past its old position. `--cap-ratio C` drops a position whose spans
hold more than C x N entries (its next read is a full pass).

Printed over the second half of the run: entries merges wrote per entry
committed, spans (all of them: what a head lookup probes), entries held
per live key, the blocks a cold exact lookup reads per existing key (one
per span holding it, plus 0.35% false positives per other span), and the
slowest consumer's catch-up (spans and entries read). The code calls spans
runs.
"""

from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass

FP = 0.0035  # false positives per filter probe, measured (bench/keys/results.md)


@dataclass
class Run:
    a: int
    b: int
    d: float
    extra: float

    def entries(self, n: int) -> float:
        return self.d * n + self.extra


@dataclass
class Consumer:
    period: int
    phase: int
    position: int = 0
    reads: int = 0
    runs_read: int = 0
    entries_read: float = 0.0
    max_runs: int = 0
    dropped: int = 0
    full: int = 0


class Tiling:
    def __init__(self, n: int, way: int, cap: int, base_ratio: float, cap_ratio: float, floors: bool):
        self.n, self.way, self.cap, self.base_ratio, self.cap_ratio = n, way, cap, base_ratio, cap_ratio
        self.floors = floors
        self.k0 = 1000
        self.monotone = True
        self.runs: list[Run] = [Run(-1, -1, 1.0, 0.0)]  # the base first, then oldest first
        self.written = self.read = self.committed = 0.0
        self.base_merges = 0

    def entries(self) -> float:
        return sum(r.entries(self.n) for r in self.runs)

    @staticmethod
    def merged(rs: list[Run]) -> Run:
        miss, extra = 1.0, 0.0
        for r in rs:
            miss *= 1 - r.d
            extra += r.extra
        return Run(rs[0].a, rs[-1].b, 1 - miss, extra)

    def commit(self, c: int, k: int, boundaries: set[int]) -> None:
        r = Run(c, c, 0.975 * k / self.n, 0.025 * k)
        self.runs.append(r)
        self.committed += r.entries(self.n)
        self.compact(boundaries)

    def compact(self, boundaries: set[int]) -> None:
        n = self.n
        changed = True
        while changed:
            changed = False
            start = 0
            for i in range(1, len(self.runs) + 1):
                if i < len(self.runs) and self.runs[i].a not in boundaries:
                    continue
                seg = self.runs[start:i]
                floor, rest = seg[0], seg[1:]
                size = sum(r.entries(n) for r in rest)
                if rest and (self.floors or start == 0) and size * self.base_ratio >= floor.entries(n):
                    # The newer runs of a segment join its oldest (the base: N live entries, no tombstones).
                    out = Run(-1, seg[-1].b, 1.0, 0.0) if start == 0 else self.merged(seg)
                    self.read += floor.entries(n) + size
                    self.written += out.entries(n)
                    self.base_merges += start == 0
                    self.runs[start:i] = [out]
                    changed = True
                    break
                tier = rest if (self.floors or start == 0) else seg
                off = start + len(seg) - len(tier)
                group = self.group(tier)
                if group is not None:
                    j, w = group
                    out = self.merged(tier[j : j + w])
                    self.read += sum(r.entries(n) for r in tier[j : j + w])
                    self.written += out.entries(n)
                    self.runs[off + j : off + j + w] = [out]
                    changed = True
                    break
                start = i

    def tier(self, r: Run) -> int:
        return int(math.log(max(r.entries(self.n), self.k0) / self.k0, self.way))

    def group(self, rs: list[Run]) -> tuple[int, int] | None:
        """The runs to merge in a segment (start, count), newest first: `way`
        adjacent runs of one size tier; past `cap` runs, the `way` adjacent
        runs with the fewest entries."""

        n, w = self.n, self.way
        tiers = [self.tier(r) for r in rs]
        if self.monotone:  # a run older than a bigger one (left by a boundary that went away) joins it
            for j in range(len(rs) - 1):
                if tiers[j] < tiers[j + 1]:
                    return j, 2
        for j in range(len(rs) - w, -1, -1):
            if len(set(tiers[j : j + w])) == 1:
                return j, w
        if len(rs) > self.cap:
            w = min(w, len(rs))
            return min(range(len(rs) - w + 1), key=lambda j: sum(r.entries(n) for r in rs[j : j + w])), w
        return None

    def after(self, p: int) -> list[Run]:
        return [r for r in self.runs if r.a >= p]


def simulate(n, commits, k, periods, way, cap, base_ratio, cap_ratio, floors=True, seed=1) -> dict:
    rng = random.Random(seed)
    t = Tiling(n, way, cap, base_ratio, cap_ratio, floors)
    consumers = [Consumer(p, rng.randrange(p)) for p in periods]
    half = commits // 2
    runs_sum = runs_max = 0
    ent_sum = ent_max = 0.0
    blocks_sum = 0.0
    for c in range(commits):
        if c == half:
            t.written = t.read = t.committed = 0.0
            t.base_merges = 0
            for x in consumers:
                x.reads = x.runs_read = x.max_runs = x.dropped = x.full = 0
                x.entries_read = 0.0
        boundaries = {x.position for x in consumers if x.position is not None}
        t.commit(c, k, boundaries)
        # Consumers due read through the head (c) and move to c + 1.
        for x in consumers:
            if x.position is not None and t.cap_ratio < math.inf:
                if sum(r.entries(n) for r in t.after(x.position)) > t.cap_ratio * n and x.position > 0:
                    x.position, x.dropped = None, x.dropped + 1
            if (c - x.phase) % x.period == 0:
                if x.position is None:
                    x.full += 1
                    x.entries_read += n
                else:
                    rs = t.after(x.position)
                    x.runs_read += len(rs)
                    x.max_runs = max(x.max_runs, len(rs))
                    x.entries_read += sum(r.entries(n) for r in rs)
                x.reads += 1
                x.position = c + 1
        t.compact({x.position for x in consumers if x.position is not None})
        if c >= half:
            runs = len(t.runs)
            ent = t.entries() / n
            runs_sum += runs
            runs_max = max(runs_max, runs)
            ent_sum += ent
            ent_max = max(ent_max, ent)
            # A random existing key: the base holds it, run r with probability d.
            held = sum(r.d for r in t.runs)
            blocks_sum += held + FP * (len(t.runs) - held)
    m = commits - half
    return {
        "amp": t.written / t.committed,
        "read_amp": t.read / t.committed,
        "runs": runs_sum / m,
        "runs_max": runs_max,
        "entries": ent_sum / m,
        "entries_max": ent_max,
        "blocks": blocks_sum / m,
        "base_merges": t.base_merges,
        "consumers": consumers,
    }


SCENARIOS = {
    "one, every commit": [1],
    "+ hourly": [1, 360],
    "+ hourly, daily": [1, 360, 8640],
    "ten, 1 to 8,640": [1, 6, 30, 60, 360, 720, 2160, 4320, 8640, 8640],
    "+ weekly": [1, 60480],
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--commits", default="40000,200000", help="per size")
    ap.add_argument("--keys", type=int, default=1000)
    ap.add_argument("--policies", default="4:1000", help="way:cap, comma-separated")
    ap.add_argument("--base-ratio", type=float, default=4.0)
    ap.add_argument("--cap-ratio", default="inf")
    ap.add_argument("--scenarios", default=";".join(SCENARIOS))
    ap.add_argument(
        "--flat", action="store_true", help="only the first group merges into its oldest (the base)"
    )
    args = ap.parse_args()
    sizes = [int(float(x)) for x in args.sizes.split(",")]
    counts = [int(x) for x in args.commits.split(",")]
    print(
        "| Keys | Consumers (periods) | way:cap | cap C | Written per entry committed | Merge reads per entry | "
        "Runs, mean (max) | Entries per live key, mean (max) | Blocks per cold exact lookup | Base merges | "
        "Catch-up per read: runs mean (max), entries | Positions dropped |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for n, commits in zip(sizes, counts, strict=True):
        for name in args.scenarios.split(";"):
            for pol in args.policies.split(","):
                way, cap = (int(x) for x in pol.split(":"))
                for cr in (float(x) for x in args.cap_ratio.split(",")):
                    r = simulate(
                        n, commits, args.keys, SCENARIOS[name], way, cap, args.base_ratio, cr, not args.flat
                    )
                    slow = max(r["consumers"], key=lambda x: x.period)
                    reads = max(slow.reads, 1)
                    catch = (
                        f"{slow.runs_read / reads:.1f} ({slow.max_runs}), "
                        f"{slow.entries_read / reads / 1e3:,.0f}K (period {slow.period})"
                    )
                    dropped = sum(x.dropped for x in r["consumers"])
                    print(
                        f"| {n:,} | {name} | {way}:{cap} | {cr:g} | {r['amp']:.1f} | {r['read_amp']:.1f} | "
                        f"{r['runs']:.1f} ({r['runs_max']}) | {r['entries']:.2f} ({r['entries_max']:.2f}) | "
                        f"{r['blocks']:.3f} | {r['base_merges']} | {catch} | {dropped} |",
                        flush=True,
                    )


if __name__ == "__main__":
    main()
