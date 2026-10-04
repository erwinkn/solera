"""What the span index keeps under load (docs/key-index-design.md), each
lifetime accounted on its own: current spans, files kept by attempt pins,
deltas kept for cleanups, merge outputs not yet published (or abandoned),
and the store's replaced data versions.

    uv run python bench/keys/retention.py --sizes 1e6,1e8

On spans.py's `versions` policy with budgeted upkeep (one merge at a time,
`--rate` entries per commit). Per commit:

- an attempt claims, pins the files of the state it is handed, and ends
  `--attempt` commits later (one attempt in `--slow` lasts 100x as long);
- the commit's delta is kept for its cleanup, done `--cleanup` commits
  later (one cleanup in `--stuck` stays pending for 2,000 commits);
- a merge's output is uploaded when the merge starts and published when it
  ends; one in `--fail` is never published and is reclaimed `--reclaim`
  commits later (the merge is retried);
- the commit replaces ~97.5% of its keys' data objects, which an immutable
  store keeps until no attempt pinned before the commit is running.

Bytes use 10 B per entry (format v3, random ids, filters included).
"""

from __future__ import annotations

import argparse
import random
from collections import deque

from spans import Sim, Workload

ENTRY = 10


def run(n, commits, attempt, slow, cleanup, stuck, fail, reclaim, rate, seed=3) -> dict:
    rng = random.Random(seed)
    w = Workload(n, commits, periods=[1, 360, 8640])
    sim = Sim(w, "versions", 1.0, 1e6, None, float("inf"))
    pins: deque[tuple[int, int, set[int]]] = deque()  # (claimed at, ends at, ids of the files it holds)
    replaced: list[tuple[int, int, float]] = []  # (replaced at, file id, bytes)
    cleanups: deque[tuple[int, float]] = deque()  # (done at, bytes)
    abandoned: deque[tuple[int, float]] = deque()  # (reclaimed at, bytes)
    data: deque[tuple[int, float]] = deque()  # (replaced at, objects)
    positions = [0, 0, 0]
    phases = [0, 17, 4001]
    busy = None  # (ends at, input spans, output entries)
    credit = 0.0
    peak = {k: 0.0 for k in ("current", "pinned", "cleanup", "unpublished", "data_objects")}
    total = {k: 0.0 for k in peak}
    seq = [0]

    def fid(sp) -> int:
        if not hasattr(sp, "fid"):
            seq[0] += 1
            sp.fid = seq[0]
        return sp.fid

    for c in range(commits):
        sim.commit(c, 0.0)
        cleanups.append((c + (2000 if rng.random() < stuck else cleanup), sim.entries(sim.spans[-1]) * ENTRY))
        data.append((c, 0.975 * w.k))
        for i, p in enumerate((1, 360, 8640)):
            if (c - phases[i]) % p == 0:
                positions[i] = c + 1
        live = set(positions)
        held = {fid(s) for s in sim.spans}
        pins.append((c, c + attempt * (100 if rng.random() < slow else 1), held))
        pins = deque(p for p in pins if p[1] > c)
        # Upkeep: one merge at a time; its output exists (unpublished) while it runs.
        credit += rate
        unpublished = 0.0
        while True:
            if busy is None:
                plan = sim.plan(live)
                if plan is None:
                    credit = min(credit, rate)
                    break
                lo, count = plan
                ins = sim.spans[lo : lo + count]
                busy = (ins, sum(sim.entries(s) for s in ins))
                if rng.random() < fail:  # uploaded, never published: reclaimed later, the merge retried
                    abandoned.append((c + reclaim, sim.entries(sim.merged(ins, live)) * ENTRY))
            ins, cost = busy
            if credit < cost:
                unpublished += sim.entries(sim.merged(ins, live)) * ENTRY
                break
            credit -= cost
            i = sim.spans.index(ins[0])
            sim.apply(i, len(ins), live)
            replaced += [(c, fid(s), sim.entries(s) * ENTRY) for s in ins]
            busy = None
        oldest_pin = min((p[0] for p in pins), default=c)
        pinned_ids = set().union(*(h for _, _, h in pins)) if pins else set()
        replaced = [r for r in replaced if r[1] in pinned_ids]
        cleanups = deque(x for x in cleanups if x[0] > c)
        while abandoned and abandoned[0][0] <= c:
            abandoned.popleft()
        while data and data[0][0] < oldest_pin:
            data.popleft()
        now = {
            "current": sum(sim.entries(s) for s in sim.spans) * ENTRY,
            "pinned": sum(b for _, _, b in replaced),
            "cleanup": sum(b for _, b in cleanups),
            "unpublished": unpublished + sum(b for _, b in abandoned),
            "data_objects": sum(o for _, o in data),
        }
        if c >= commits // 2:
            for k, v in now.items():
                peak[k] = max(peak[k], v)
                total[k] += v
    m = commits - commits // 2
    return {"peak": peak, "mean": {k: v / m for k, v in total.items()}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--commits", default="20000,40000")
    ap.add_argument("--attempt", type=int, default=6)
    ap.add_argument("--slow", type=float, default=0.001)
    ap.add_argument("--cleanup", type=int, default=6)
    ap.add_argument("--stuck", type=float, default=0.001)
    ap.add_argument("--fail", type=float, default=0.05)
    ap.add_argument("--reclaim", type=int, default=360)
    ap.add_argument("--rate", type=float, default=2e6)
    args = ap.parse_args()
    print(
        "| Keys | Current spans, mean (peak) | Kept by attempt pins | Deltas kept for cleanups | "
        "Unpublished or abandoned merge outputs | Data objects kept by pins |"
    )
    print("|---|---|---|---|---|---|")
    for n, commits in zip(
        (int(float(x)) for x in args.sizes.split(",")), (int(x) for x in args.commits.split(",")), strict=True
    ):
        r = run(
            n, commits, args.attempt, args.slow, args.cleanup, args.stuck, args.fail, args.reclaim, args.rate
        )
        mean, peak = r["mean"], r["peak"]

        def mb(k, mean=mean, peak=peak):
            return f"{mean[k] / 1e6:,.1f} MB ({peak[k] / 1e6:,.1f})"

        print(
            f"| {n:,} | {mb('current')} | {mb('pinned')} | {mb('cleanup')} | {mb('unpublished')} | "
            f"{mean['data_objects']:,.0f} ({peak['data_objects']:,.0f}) |",
            flush=True,
        )


if __name__ == "__main__":
    main()
