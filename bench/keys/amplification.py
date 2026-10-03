"""Compaction write amplification over a long run, from the real planner.

    uv run python bench/keys/amplification.py --sizes 1e6,1e7,1e8 --commits 100000

Replays `KeyIndex.plan_compaction` on file metadata alone — no bytes are
written — for an index of N random keys taking a commit of `--keys` random
updates at a time. A file is a key range with a density (the share of the
range's keys it holds, as independent random subsets); a merge keeps each
key's newest entry, so a range's density merges as 1 − Π(1 − density), and
outputs split the way `merge_files` splits them. Entry sizes are the
measured ones (bench/keys/results.md).

Policies: `now` is the planner as it is; `before` merged every level-0
file straight into level 1 with the level-1 files it overlapped (before
2026-09-30); `ratio R` merges level 0 into level 1 once it holds 1/R of it.
Printed per policy: bytes compaction wrote per byte committed, the level-0
files and bytes a commit reads (at commit time, averaged), and compaction
requests per commit — over the second half of the run, once the levels filled.
"""

from __future__ import annotations

import argparse
import math
import random
from dataclasses import replace

from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import RANGE

ENTRY = 29.0  # bytes per entry, filters included
TAIL = 3.6 / ENTRY  # share of a file after its blocks
RAW = 38  # raw bytes per entry, as `merge_files` counts them


def pos(x: float) -> bytes:
    return b"%.15f" % x


class Sim:
    def __init__(self, n: int, opts: Options, policy: str):
        self.n, self.o, self.policy = n, opts, policy
        self.density: dict[str, float] = {}  # file -> share of its range's keys it holds
        self.state = IndexState()
        self.seq = 0
        self.written = self.committed = 0
        self.gets = self.puts = 0
        # The bottom level, placed as the benchmark builds it: the deepest level moves down until it fits.
        self.state = replace(self.state, files=tuple(self._split([(0.0, 1.0, 1.0)], 1, "b")))
        while (plan := self.plan()) is not None:
            self.apply(plan)

    def info(self, name, level, lo, hi, entries) -> FileInfo:
        size = int(entries * ENTRY)
        return FileInfo(name, level, pos(lo), pos(hi), int(entries), size, int(size * TAIL), 100)

    def _split(self, segments, level, stem) -> list[FileInfo]:
        """Files for merged `segments` (lo, hi, density), cut at the merge's file size."""

        cap = 2 * self.o.max_file_bytes / RAW
        out, start, got = [], segments[0][0], 0.0
        for lo, hi, d in segments:
            per_unit = d * self.n
            while got + per_unit * (hi - lo) >= cap:
                cut = lo + (cap - got) / per_unit
                out.append((start, cut, cap))
                start, got, lo = cut, 0.0, cut
            got += per_unit * (hi - lo)
        if got >= 1:
            out.append((start, segments[-1][1], got))
        files = []
        for a, b, entries in out:
            self.seq += 1
            name = f"{stem}{self.seq:08d}"
            files.append(self.info(name, level, a, b, entries))
            self.density[name] = entries / self.n / max(b - a, 1e-12)
        return files

    def plan(self):
        index = KeyIndex(None, "", self.state, self.o)
        if self.policy != "now":
            l0 = self.state.level(0)
            size = sum(f.size for f in l0)
            if l0 and (len(l0) >= self.o.l0_max_files or size >= self.o.l0_max_bytes):
                l1 = self.state.level(1)
                ratio = math.inf if self.policy == "before" else float(self.policy.split()[1])
                if len(l0) > 1 and size * ratio < sum(f.size for f in l1):
                    return l0, 0
                lo, hi = min(f.min for f in l0), max(f.max for f in l0)
                return l0 + [f for f in l1 if f.max >= lo and f.min <= hi], 1
        return index.plan_compaction()

    def apply(self, plan):
        inputs, level = plan
        if level > self.state.depth:
            added = [replace(f, level=level) for f in inputs]
        else:
            cuts = sorted({float(f.min) for f in inputs} | {float(f.max) for f in inputs})
            segments = []
            for a, b in zip(cuts, cuts[1:], strict=False):
                miss = 1.0
                for f in inputs:
                    if float(f.min) <= a and b <= float(f.max):
                        miss *= 1 - self.density[f.name]
                if miss < 1:
                    segments.append((a, b, 1 - miss))
            stem = f"{inputs[0].name[:12]}-c" if level == 0 else "c"
            added = self._split(segments, level, stem)
            self.written += sum(f.size for f in added)
            self.gets += sum(math.ceil(f.size / RANGE) for f in inputs)
            self.puts += len(added)
        self.state = self.state.compacted(added, [f.name for f in inputs])

    def commit(self, commit_number: int, k: int, rng: random.Random):
        xs = [rng.random() for _ in range(k)]
        lo, hi = min(xs), max(xs)
        name = f"{commit_number:012d}-a"
        f = self.info(name, 0, lo, hi, k)
        self.density[name] = k / self.n / (hi - lo)
        self.state = replace(self.state, files=self.state.files + (f,))
        self.committed += f.size
        while (plan := self.plan()) is not None:
            self.apply(plan)


def run(n: int, commits: int, k: int, policy: str) -> dict:
    sim = Sim(n, Options(), policy)
    rng = random.Random(1)
    l0_files = l0_bytes = 0
    for commit_number in range(commits):
        if commit_number == commits // 2:
            sim.written = sim.committed = sim.gets = sim.puts = 0
        if commit_number >= commits // 2:
            l0 = sim.state.level(0)
            l0_files += len(l0)
            l0_bytes += sum(f.size for f in l0)
        sim.commit(commit_number, k, rng)
    half = commits - commits // 2
    levels = {}
    for f in sim.state.files:
        levels[f.level] = levels.get(f.level, 0) + f.size
    return {
        "amp": sim.written / sim.committed,
        "l0_files": l0_files / half,
        "l0_mb": l0_bytes / half / 1e6,
        "gets": sim.gets / half,
        "puts": sim.puts / half,
        "levels": " · ".join(f"L{lv}: {size / 1e6:,.0f} MB" for lv, size in sorted(levels.items())),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7,1e8")
    ap.add_argument("--commits", type=int, default=100_000)
    ap.add_argument("--keys", type=int, default=1000)
    ap.add_argument("--policies", default="before,now,ratio 3,ratio 30")
    args = ap.parse_args()
    print(f"\n{args.commits:,} commits of {args.keys:,} random keys; the second half measured.\n")
    print(
        "| Keys | Policy | Written per byte committed | Level-0 files read per commit | Level-0 MB read per commit | Compaction GETs / PUTs per commit | Levels at the end |"
    )
    print("|---|---|---|---|---|---|---|")
    for n in (int(float(x)) for x in args.sizes.split(",")):
        for policy in args.policies.split(","):
            r = run(n, args.commits, args.keys, policy)
            print(
                f"| {n:,} | {policy} | {r['amp']:.1f} | {r['l0_files']:.1f} | {r['l0_mb']:.2f} | "
                f"{r['gets']:.2f} / {r['puts']:.3f} | {r['levels']} |",
                flush=True,
            )


if __name__ == "__main__":
    main()
