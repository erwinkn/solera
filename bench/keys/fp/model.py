"""Recency-tiered stamped runs (docs/key-index-from-first-principles.md),
replayed on run metadata.

    python3 bench/keys/fp/model.py --sizes 1e6,1e8

The density model is `spans.py`'s: a run of random keys holds a share `d`
of the N existing keys (an independent random subset) plus `extra` keys
outside them (new or temporary), so merging runs gives d = 1 - prod(1 - d_i)
and extra = sum(e_i). The oldest run (the base) holds the N live keys, plus
the tombstones whose removal is newer than the cut.

The policy. Runs tile commit time, newest last. A run's tier is
floor(log_f(bytes / B0)). Upkeep merges:

- **tiers**: the oldest f adjacent runs of the same tier, from the newest
  such group back;
- **the base**: the base and the runs just above it, when they hold at
  least 1/r of the base;

and a merge is allowed only if its output obeys the **reader bound**: if it
reaches above the cut, it holds at most k x (the bytes of every newer run)
+ Z. With `--use floor`, the cut is also raised to the oldest live reader
position (flips and the base's tombstones go sooner, and merges below it
are free); with `--use set`, in addition the bound applies only when a
live reader position lies inside the output.

Upkeep is instant, or one merge at a time per lane (tiers, base) at
`--rate` entries per commit.

Reads, at sampled heads:

- a reader D commits behind reads every run overlapping (P, head]; what it
  needs is the distinct keys changed in that range;
- a cold lookup of 1K random keys (a writer) reads, per run, the faster of
  streaming the whole run and one 16 KiB block per key (`lookup_cost`);
- a first run reads everything; each batch opens every run.
"""

from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass, field

MB = 1e6
BLOCK = 16 * 1024


@dataclass(eq=False)
class Run:
    a: int
    b: int
    d: float
    extra: float
    base: bool = False
    tombs: float = 0.0  # the base's tombstones (removals newer than the cut)


@dataclass
class Workload:
    n: int
    commits: int
    k: int = 1000
    window: int = 60_480  # commits: 7 days at one commit per 10 s
    temp: float = 0.0  # share of each commit's keys that are temporary
    life: int = 100  # commits a temporary key lives
    big_at: int | None = None
    big: int = 1_000_000


@dataclass
class Policy:
    f: int = 4
    kread: float = 4.0
    z: float = 1 * MB
    b0: float = 8 * 1024
    r: float = 4.0
    rate: float | None = None  # entries per commit, per lane; None = instant
    readers: list[int] | None = None  # reader periods (commits); each reads every period
    use: str = "none"  # what of the live P set merges use: none, floor (its oldest), set (all of it)
    use_bound: bool = True


@dataclass
class Sim:
    w: Workload
    p: Policy
    e_delta: float
    e_run: float
    runs: list[Run] = field(default_factory=list)
    written_bytes: float = 0.0
    written_entries: float = 0.0
    committed_bytes: float = 0.0
    committed_entries: float = 0.0
    puts: float = 0.0
    busy: dict = field(default_factory=dict)  # lane -> (publish at, inputs, output)
    cut: int = 0
    removes_per_commit: float = 0.0

    # -- sizes --------------------------------------------------------------

    def entries(self, r: Run) -> float:
        if r.base:
            return self.w.n + r.tombs
        return r.d * self.w.n + r.extra

    def bytes(self, r: Run) -> float:
        per = self.e_delta if r.a == r.b and not r.base else self.e_run
        return self.entries(r) * per

    def head_bytes(self, r: Run) -> float:
        """What head reads (lookups, first runs) read: the base's tombstones
        are in its graveyard file, which only catch-ups read."""
        return self.w.n * self.e_run if r.base else self.bytes(r)

    def tier(self, r: Run) -> int:
        return max(0, int(math.log(max(self.bytes(r), self.p.b0) / self.p.b0, self.p.f)))

    # -- merging ------------------------------------------------------------

    def merged(self, rs: list[Run]) -> Run:
        if rs[0].base:
            b = rs[-1].b
            tombs = self.removes_per_commit * max(0, b - self.cut)
            return Run(rs[0].a, b, 1.0, 0.0, base=True, tombs=tombs)
        miss, extra = 1.0, 0.0
        for r in rs:
            miss *= 1 - r.d
            extra += r.extra
        return Run(rs[0].a, rs[-1].b, 1 - miss, extra)

    def allowed(self, out: Run, newer: list[Run], live: set[int]) -> bool:
        if out.b <= self.cut or not self.p.use_bound:
            return True
        if self.p.use == "set" and not any(out.a < q <= out.b for q in live):
            return True
        return self.bytes(out) <= self.p.kread * sum(self.bytes(r) for r in newer) + self.p.z

    def frozen(self) -> set[int]:
        return {id(r) for (_, ins, _) in self.busy.values() for r in ins}

    def plan(self, lane: str, live: set[int]) -> tuple[int, int] | None:
        rs, fz = self.runs, self.frozen()
        if lane == "base":
            base = rs[0]
            if id(base) in fz:
                return None
            best = None
            acc = 0.0
            for j in range(1, len(rs)):
                if id(rs[j]) in fz:
                    break
                acc += self.bytes(rs[j])
                if acc * self.p.r < self.bytes(base):
                    continue
                out = self.merged(rs[: j + 1])
                if self.allowed(out, rs[j + 1 :], live):
                    best = (0, j + 1)
            return best
        # tiers: newest group of f same-tier adjacent runs, merging its oldest f
        i = len(rs) - 1
        while i >= 1:
            t = self.tier(rs[i])
            j = i
            while j - 1 >= 1 and self.tier(rs[j - 1]) == t:
                j -= 1
            if i - j + 1 >= self.p.f:
                lo = j
                ins = rs[lo : lo + self.p.f]
                if not any(id(r) in fz for r in ins):
                    out = self.merged(ins)
                    if self.allowed(out, rs[lo + self.p.f :], live):
                        return (lo, self.p.f)
            i = j - 1
        return None

    def apply(self, lo: int, count: int, out: Run | None = None) -> None:
        ins = self.runs[lo : lo + count]
        out = out or self.merged(ins)
        self.written_entries += self.entries(out)
        self.written_bytes += self.bytes(out)
        self.puts += max(1, math.ceil(self.bytes(out) / (64 * MB))) + (1 if self.bytes(out) > 64 * 1024 else 0)
        self.runs[lo : lo + count] = [out]

    def upkeep(self, c: int, live: set[int]) -> None:
        if self.p.rate is None:
            for _ in range(10_000):
                step = self.plan("base", live) or self.plan("tiers", live)
                if not step:
                    return
                self.apply(*step)
            raise RuntimeError("upkeep does not converge")
        for lane in ("base", "tiers"):
            if lane in self.busy and self.busy[lane][0] <= c:
                _, ins, out = self.busy.pop(lane)
                lo = self.runs.index(ins[0])
                if self.runs[lo : lo + len(ins)] == ins:
                    self.apply(lo, len(ins), out)
            if lane not in self.busy:
                step = self.plan(lane, live)
                if step:
                    lo, count = step
                    ins = self.runs[lo : lo + count]
                    work = sum(self.entries(r) for r in ins)
                    self.busy[lane] = (c + math.ceil(work / self.p.rate), ins, self.merged(ins))

    def commit(self, c: int, temp_out: float) -> None:
        w = self.w
        k = w.big if c == w.big_at else w.k
        temp = w.temp * w.k if c != w.big_at else 0.0
        d = (k - temp) * 0.975 / w.n
        extra = (k - temp) * 0.025 + temp + temp_out
        r = Run(c, c, d, extra)
        self.runs.append(r)
        self.committed_entries += self.entries(r)
        self.committed_bytes += self.bytes(r)
        self.puts += 1


RTT, PARALLEL, NIC, DECODE = 0.030, 64, 1.25e9, 120e6  # s, requests in flight, B/s, entries/s (4 cores)


def lookup_cost(sim: Sim, keys: int = 1000, filters: bool = False) -> tuple[float, float, float]:
    """GETs, bytes and seconds for `keys` random exact lookups at the head,
    cold, block indexes in hand. Per run, the faster of two plans (fewer
    requests on a tie within 10%):

    - seek: one 16 KiB block per distinct block a key falls in;
    - stream: the whole run in 16 MB range GETs, decoded.

    A plan's time is its request waves (64 in flight, 30 ms each), its bytes
    over the NIC (1.25 GB/s) and its entries decoded (120M/s on 4 cores).
    With `filters` (each non-base run's Bloom filter, 10 bits per key, 1%
    false positives, held by the engine), a run is probed only for the keys
    its filter passes."""
    gets = byts = ents = 0.0
    for r in sim.runs:
        size = sim.head_bytes(r)
        n_ent = size / sim.e_run
        probe = keys
        if filters and not r.base:
            share = min(1.0, sim.entries(r) / sim.w.n)
            probe = keys * (share + 0.01 * (1 - share))
        blocks = max(1.0, size / BLOCK)
        touched = blocks * (1 - (1 - 1 / blocks) ** probe)
        per_block = n_ent / blocks
        seek = (touched, min(size, touched * BLOCK), touched * per_block)
        stream = (math.ceil(size / (16 * MB)), size, n_ent)
        t = lambda p: math.ceil(p[0] / PARALLEL) * RTT + p[1] / NIC + p[2] / DECODE
        best = min((seek, stream), key=t)
        other = stream if best is seek else seek
        if t(other) <= 1.1 * t(best) and other[0] < best[0]:
            best = other
        gets, byts, ents = gets + best[0], byts + best[1], ents + best[2]
    secs = math.ceil(gets / PARALLEL) * RTT + byts / NIC + ents / DECODE
    return gets, byts, secs


def need(sim: Sim, dist: int) -> float:
    """Distinct keys changed in the last `dist` commits (uniform model)."""
    w = sim.w
    d1 = w.k * (1 - w.temp) * 0.975 / w.n
    # Temporary keys differ at the two ends only if alive at exactly one:
    # those added within `life` before P (removed after it), and those added
    # within `life` before the head; the rest were added and removed inside.
    temp = 2 * min(dist, w.life) * w.temp * w.k
    return (1 - (1 - d1) ** dist) * w.n + dist * w.k * (1 - w.temp) * 0.025 + temp


def run(w: Workload, p: Policy, e_delta: float, e_run: float, dists=(1, 100, 360, 8640, 10000), seed=1) -> dict:
    dists = tuple(d for d in dists if d <= w.window)  # older readers are folded
    rng = random.Random(seed)
    sim = Sim(w, p, e_delta, e_run)
    sim.removes_per_commit = w.k * 0.025 + w.temp * w.k
    # Commit 0: the first full load, a base of N keys.
    sim.runs = [Run(0, 0, 1.0, 0.0, base=True)]
    phases = [rng.randrange(per) for per in (p.readers or [])]
    positions = [0 for _ in phases]
    half = w.commits // 2
    temp_due: dict[int, float] = {}
    stats = {"runs": [], "stored": [], "lookup": [], "reads": {d: [] for d in dists}}
    for c in range(1, w.commits):
        if c == half:
            sim.written_bytes = sim.written_entries = sim.committed_bytes = sim.committed_entries = 0.0
            sim.puts = 0.0
        sim.cut = max(0, c - w.window)
        if p.use in ("floor", "set"):
            # The oldest live P: the periodic readers' positions and the
            # measured readers (up to max(dists) behind).
            sim.cut = max(sim.cut, min(positions + [c - max(dists)]))
        if w.temp and c != w.big_at:
            temp_due[c + w.life] = temp_due.get(c + w.life, 0.0) + w.temp * w.k
        sim.commit(c, temp_due.pop(c, 0.0))
        for i, per in enumerate(p.readers or []):
            if (c - phases[i]) % per == 0:
                positions[i] = c
        live = set(positions) | {c - d for d in dists}
        sim.upkeep(c, live)
        if c >= half and c % 97 == 0:
            stats["runs"].append(len(sim.runs))
            stats["stored"].append(sum(sim.bytes(r) for r in sim.runs))
            stats["lookup"].append(lookup_cost(sim))
            stats.setdefault("lookup_f", []).append(lookup_cost(sim, filters=True))
            stats.setdefault("per_key", []).append(sum(sim.entries(r) for r in sim.runs if not r.base) / w.n + 1)
            stats.setdefault("full", []).append(sum(sim.head_bytes(r) for r in sim.runs))
            for dist in dists:
                pos = c - dist  # the reader observed at pos; it reads (pos, c]
                over = [r for r in sim.runs if r.b > pos]
                read = sum(sim.bytes(r) for r in over)
                straddle = sum(sim.bytes(r) for r in over if r.a <= pos)
                stats["reads"][dist].append((read, need(sim, dist), len(over), straddle))
    m = lambda xs: sum(xs) / len(xs)
    out = {
        "runs": m(stats["runs"]),
        "runs_max": max(stats["runs"]),
        "stored": m(stats["stored"]),
        "stored_max": max(stats["stored"]),
        "amp_bytes": sim.written_bytes / sim.committed_bytes,
        "amp_entries": sim.written_entries / sim.committed_entries,
        "puts": sim.puts / (w.commits - half),
        "up_bytes": (sim.written_bytes + sim.committed_bytes) / (w.commits - half),
        "lookup_gets": m([x[0] for x in stats["lookup"]]),
        "lookup_mb": m([x[1] for x in stats["lookup"]]) / MB,
        "lookup_s": m([x[2] for x in stats["lookup"]]),
        "lookup_f_s": m([x[2] for x in stats["lookup_f"]]),
        "lookup_f_mb": m([x[1] for x in stats["lookup_f"]]) / MB,
        "full_mb": m(stats["full"]) / MB,
        "lookup_f_gets": m([x[0] for x in stats["lookup_f"]]),
        "per_key": m(stats["per_key"]),
        "reads": {},
    }
    for dist, xs in stats["reads"].items():
        read = m([x[0] for x in xs])
        needed = m([x[1] for x in xs])
        out["reads"][dist] = {
            "mb": read / MB,
            "ratio": read / (needed * e_run),
            "ratio_max": max(x[0] / (x[1] * e_run) for x in xs),
            "runs": m([x[2] for x in xs]),
            "runs_max": max(x[2] for x in xs),
            "straddle_mb": m([x[3] for x in xs]) / MB,
            "need_keys": needed,
        }
    return out


def entry_bytes(n: int) -> tuple[float, float]:
    # Measured (bench/keys/fp/codec, results.txt): 12-digit ids, zstd-1, 16 KiB
    # blocks. Minimal delta of 1K keys 5.79 B; stamped run entries 8.1 B at
    # 100M, 8.7-9.3 B at 1M.
    return (5.79, 8.1) if n >= 10**8 else (5.79, 9.0)


def scenarios(n: int) -> dict[str, Workload]:
    big = n >= 10**8
    commits = 300_000 if big else 150_000
    return {
        "base trace, 7-day window": Workload(n, commits),
        "1-day window": Workload(n, commits, window=8_640),
        "30-day window": Workload(n, max(commits, 600_000), window=259_200),
        "churn (half of each commit temporary)": Workload(n, commits, temp=0.5),
        "a 1M-key commit mid-trace": Workload(n, commits, big_at=commits // 2 + 1),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--only", default="")
    ap.add_argument("--kread", type=float, default=4.0)
    ap.add_argument("--z", type=float, default=1.0, help="MB")
    ap.add_argument("--f", type=int, default=4)
    ap.add_argument("--r", type=float, default=4.0, help="the base absorbs once the runs above hold 1/r of it")
    ap.add_argument("--rate", default="instant")
    ap.add_argument("--readers", default="8640x100", help="reader periods, e.g. 100 daily: 8640x100")
    ap.add_argument("--use", default="none", choices=["none", "floor", "set"])
    ap.add_argument("--no-bound", action="store_true")
    args = ap.parse_args()
    readers = None
    if args.readers:
        per, count = args.readers.split("x")
        readers = [int(per)] * int(count)
    for n in (int(float(x)) for x in args.sizes.split(",")):
        e_delta, e_run = entry_bytes(n)
        for name, w in scenarios(n).items():
            if args.only and args.only not in name:
                continue
            p = Policy(
                f=args.f,
                r=args.r,
                kread=args.kread,
                z=args.z * MB,
                rate=None if args.rate == "instant" else float(args.rate),
                readers=readers,
                use=args.use,
                use_bound=not args.no_bound,
            )
            r = run(w, p, e_delta, e_run)
            print(
                f"## {n:,} keys, {name} (f={p.f}, k={p.kread:g}, Z={args.z:g} MB, r={p.r:g}, upkeep {args.rate}"
                f", live P: {args.use}{', no bound' if args.no_bound else ''})"
            )
            print(
                f"runs {r['runs']:.1f} ({r['runs_max']}) · stored {r['stored'] / MB:.0f} MB "
                f"({r['stored_max'] / MB:.0f}) · background writes {r['amp_bytes']:.1f}x bytes, "
                f"{r['amp_entries']:.1f}x entries · PUTs/commit {r['puts']:.2f} · uploaded/commit "
                f"{r['up_bytes'] / 1e3:.0f} KB · cold 1K lookup {r['lookup_gets']:.0f} GETs, {r['lookup_mb']:.1f} MB, {r['lookup_s']:.2f} s (filters held: {r['lookup_f_gets']:.0f} GETs, {r['lookup_f_mb']:.1f} MB, {r['lookup_f_s']:.2f} s) · head entries per live key {r['per_key']:.2f} · first run reads {r['full_mb']:.0f} MB"
            )
            for dist, x in r["reads"].items():
                print(
                    f"  {dist:>6} behind: reads {x['mb']:.2f} MB = {x['ratio']:.2f}x needed "
                    f"(max {x['ratio_max']:.2f}x), runs {x['runs']:.1f} ({x['runs_max']}), "
                    f"straddler {x['straddle_mb']:.2f} MB, needed {x['need_keys'] / 1e3:,.0f}K keys"
                )
            print(flush=True)


if __name__ == "__main__":
    main()
