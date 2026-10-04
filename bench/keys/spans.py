"""Spans under observer churn (docs/key-index-design.md): the merge
policy replayed on span metadata, with endpoints that come and go.

    uv run python bench/keys/spans.py --sizes 1e6,1e8

The density model of tiling.py: a segment of random keys holds a share `d`
of the N existing keys (an independent random subset) plus `extra` keys
outside them (brand new or temporary), so merging segments gives
d = 1 - prod(1 - d_i) and extra = sum(e_i).

A span covers commits [a, b] and is a list of segments separated by the
endpoints that were live when it was written. A key keeps one version per
segment it changed in: the version an endpoint sees (the key's state just
before it), and the newest. So a span holds sum(d_i N + e_i) entries, and
a merge coalesces segments whose dividing endpoint is gone. A span starting
at commit 0 keeps live keys only before its first inner endpoint (N).

Policies, both with the same triggers:

- `blocked` (the reviewed design): a merge never crosses a live endpoint;
- `versions`: a merge may cross a live endpoint e when e's reader then
  reads at most `--lam` times what lies at or after e in the output, or at
  most `--z` entries before it.

Triggers: the oldest span absorbs the spans after it (as far as the policy
lets it reach) once they hold a quarter of it; otherwise the newest window
of 4 adjacent spans whose largest holds at most the other three combined
merges. Every merge's largest input holds at most 4 times the others.

Upkeep is `instant`, or one merge at a time at `--rate` entries per commit
(a merge planned now publishes once its inputs are read; endpoints born
meanwhile lie past its inputs). Consumers read every `period` commits; a
read costs the entries of every span overlapping [position, head]. `--cap`
drops a position (no pass under way) whose read would exceed C x N.
"""

from __future__ import annotations

import argparse
import math
import random
from dataclasses import dataclass, field

FP = 0.0035


@dataclass
class Seg:
    start: int
    d: float
    extra: float


@dataclass(eq=False)
class Span:
    a: int
    b: int
    segs: list[Seg]
    size: float = -1.0  # entries, filled in by `Sim.entries`


@dataclass
class Consumer:
    period: int
    phase: int
    position: int | None = 0
    stalled_until: int = -1  # a pass under way until this commit: its endpoint is held, its reads wait
    reads: int = 0
    read: float = 0.0
    need: float = 0.0
    dropped: int = 0


@dataclass
class Workload:
    n: int
    commits: int
    k: int = 1000
    periods: list[int] = field(default_factory=lambda: [1])
    phases: list[int] | None = None
    temp: float = 0.0  # share of each commit's keys that are temporary (added, removed `life` commits later)
    life: int = 100
    big_at: int | None = None  # a commit of `big` keys at this commit
    big: int = 1_000_000
    stall: tuple[int, int] | None = (
        None  # (consumer index, commits): it starts a pass at mid-run and holds it
    )


class Sim:
    def __init__(self, w: Workload, policy: str, lam: float, z: float, rate: float | None, cap: float):
        self.w, self.n = w, w.n
        self.policy, self.lam, self.z, self.rate, self.cap = policy, lam, z, rate, cap
        self.spans: list[Span] = [Span(-1, -1, [Seg(-1, 1.0, 0.0)])]
        self.written = self.committed = 0.0
        self.busy: tuple[int, list[Span], Span] | None = None  # (commit it publishes at, inputs, output)
        self.lanes = {"base": [0.0, None], "tail": [0.0, None]}
        self.fanin = 32
        self.waits = 0

    # -- entries -------------------------------------------------------------------------

    def seg_entries(self, s: Seg) -> float:
        return s.d * self.n + s.extra

    def entries(self, sp: Span) -> float:
        if sp.size < 0:
            sp.size = sum(self.seg_entries(s) for s in sp.segs)
        return sp.size

    # -- merging -------------------------------------------------------------------------

    def merged(self, rs: list[Span], live: set[int]) -> Span:
        segs: list[Seg] = []
        for r in rs:
            for s in r.segs:
                if segs and s.start not in live:
                    p = segs[-1]
                    segs[-1] = Seg(p.start, 1 - (1 - p.d) * (1 - s.d), p.extra + s.extra)
                else:
                    segs.append(Seg(s.start, s.d, s.extra))
        if rs[0].a == -1:  # the span from commit 0: live keys only before its first inner endpoint
            segs[0] = Seg(-1, 1.0, 0.0)
        return Span(rs[0].a, rs[-1].b, segs)

    def guarded(self, rs: list[Span]) -> bool:
        sizes = [self.entries(r) for r in rs]
        return len(rs) == 1 or max(sizes) <= 4 * (sum(sizes) - max(sizes))

    def allowed(self, rs: list[Span], live: set[int]) -> bool:
        if not self.guarded(rs):
            return False
        if self.policy == "blocked":
            return not any(r.a in live for r in rs[1:])
        out = self.merged(rs, live)
        es = [self.seg_entries(s) for s in out.segs]
        total = sum(es)
        before = 0.0
        for s, e in zip(out.segs, es, strict=True):
            if s.start in live and before > 0:
                after = total - before
                if before > max(self.lam * after, self.z):
                    return False
                # `capped`: both temporal ends - a pass ending at `e` must not face much newer data either.
                if self.policy == "capped" and after > max(self.lam * before, self.z):
                    return False
            before += e
        return True

    def forced(self, live: set[int]) -> tuple[int, int] | None:
        """`capped`, past `fanin` spans: the guarded window of 2 to 4 adjacent spans
        (the base excluded) with the fewest entries, whatever the read rule says."""

        sp, best = self.spans, None
        for w in (2, 3, 4):
            for lo in range(1, len(sp) - w + 1):
                rs = sp[lo : lo + w]
                if self.guarded(rs):
                    cost = sum(self.entries(r) for r in rs)
                    if best is None or cost < best[0]:
                        best = (cost, lo, w)
        return None if best is None else (best[1], best[2])

    def plan(
        self, live: set[int], lane: str = "any", frozen: frozenset = frozenset()
    ) -> tuple[int, int] | None:
        """The next merge (start, count). `lane`: "base" plans only merges into the
        oldest span, "tail" only the others; `frozen` spans belong to a merge under way."""

        sp = self.spans
        if frozen:
            if lane == "base":  # it may reach only up to the first span the tail lane holds
                cut = next((i for i, r in enumerate(sp) if id(r) in frozen), len(sp))
                sp = sp[:cut]
        sizes = [self.entries(r) for r in sp]
        if lane == "tail":
            out = self._tail(sp, sizes, live, frozen)
            if out is None and self.policy == "capped" and len(self.spans) > self.fanin:
                f = self.forced(live)
                if f is not None and not any(id(r) in frozen for r in self.spans[f[0] : f[0] + f[1]]):
                    out = f
            return out
        # The oldest span absorbs what follows, as far as it may reach.
        if len(sp) > 1 and (sum(sizes) - sizes[0]) * 4 >= sizes[0]:
            acc, j0 = 0.0, len(sp)
            for j in range(1, len(sp)):
                acc += sizes[j]
                if acc * 4 >= sizes[0]:
                    j0 = j
                    break
            # Candidates: everything, or up to just before a span a live endpoint falls in.
            ends = [len(sp) - 1] + [
                j - 1 for j in range(len(sp) - 1, 0, -1) if any(sp[j].a <= e <= sp[j].b for e in live)
            ]
            for j in ends:
                if j >= j0 and self.allowed(sp[: j + 1], live):
                    return 0, j + 1
        if lane == "base":
            return None
        out = self._tail(sp, sizes, live, frozen)
        if out is None and self.policy == "capped" and len(self.spans) > self.fanin and not frozen:
            out = self.forced(live)
        return out

    def _tail(self, sp, sizes, live, frozen) -> tuple[int, int] | None:
        def free(lo, w):
            return not any(id(r) in frozen for r in sp[lo : lo + w])

        for j in range(len(sp) - 4, -1, -1):
            w = sizes[j : j + 4]
            if j >= 1 and max(w) <= sum(w) - max(w) and free(j, 4) and self.allowed(sp[j : j + 4], live):
                return j, 4
        # A span smaller than its newer neighbour (left behind by an endpoint that went away)
        # joins the shortest guarded window around it.
        for j in range(1, len(sp) - 1):
            if sizes[j] < sizes[j + 1]:
                for w in range(2, 5):
                    for lo in range(max(1, j + 2 - w), min(j, len(sp) - w) + 1):
                        if free(lo, w) and self.allowed(sp[lo : lo + w], live):
                            return lo, w
        # A span whose versions no live endpoint sees any more are a quarter of it is rewritten alone.
        for j in range(1, len(sp)):
            if (
                len(sp[j].segs) > 1
                and free(j, 1)
                and self.entries(self.merged([sp[j]], live)) * 4 <= sizes[j] * 3
            ):
                return j, 1
        return None

    def apply(self, lo: int, count: int, live: set[int]) -> None:
        out = self.merged(self.spans[lo : lo + count], live)
        self.written += self.entries(out)
        self.spans[lo : lo + count] = [out]

    def upkeep(self, c: int, live: set[int]) -> None:
        if self.rate is None:
            while (p := self.plan(live)) is not None:
                self.apply(*p, live)
            return
        # Two lanes, each one merge at a time at `rate`: merges into the oldest span, and the rest.
        waited = False
        for lane in ("base", "tail"):
            self.lanes[lane][0] += self.rate
            while True:
                credit, busy = self.lanes[lane]
                if busy is None:
                    other = self.lanes["tail" if lane == "base" else "base"][1]
                    frozen = frozenset(id(r) for r in other) if other else frozenset()
                    p = self.plan(live, lane, frozen)
                    if p is None:
                        self.lanes[lane][0] = min(credit, self.rate)
                        break
                    lo, count = p
                    busy = self.spans[lo : lo + count]
                    self.lanes[lane][1] = busy
                cost = sum(self.entries(r) for r in busy)
                if credit < cost:
                    waited = True
                    break
                self.lanes[lane][0] -= cost
                i = self.spans.index(busy[0])  # its inputs are still adjacent: no other merge touched them
                self.apply(i, len(busy), live)
                self.lanes[lane][1] = None
        self.waits += waited

    # -- the run -------------------------------------------------------------------------

    def overlapping(self, p: int) -> list[Span]:
        return [r for r in self.spans if r.b >= p]

    def commit(self, c: int, temp_out: float) -> None:
        w, n = self.w, self.n
        k = w.big if c == w.big_at else w.k
        temp = w.temp * w.k if c != w.big_at else 0.0
        d = (k - temp) * 0.975 / n
        extra = (k - temp) * 0.025 + temp + temp_out
        self.spans.append(Span(c, c, [Seg(c, d, extra)]))
        self.committed += d * n + extra


def run(w: Workload, policy: str, lam=1.0, z=1e6, rate=None, cap=math.inf, seed=1) -> dict:
    rng = random.Random(seed)
    sim = Sim(w, policy, lam, z, rate, cap)
    phases = w.phases or [rng.randrange(p) for p in w.periods]
    cs = [Consumer(p, ph) for p, ph in zip(w.periods, phases, strict=True)]
    half = w.commits // 2
    temp_due: dict[int, float] = {}
    spans_sum = spans_max = 0
    ent_sum = ent_max = 0.0
    blocks = 0.0
    for c in range(w.commits):
        if c == half:
            sim.written = sim.committed = 0.0
            sim.waits = 0
            for x in cs:
                x.reads = x.dropped = 0
                x.read = x.need = 0.0
            if w.stall:
                cs[w.stall[0]].stalled_until = c + w.stall[1]
        if w.temp and c != w.big_at:
            temp_due[c + w.life] = temp_due.get(c + w.life, 0.0) + w.temp * w.k
        sim.commit(c, temp_due.pop(c, 0.0))
        for x in cs:
            if x.position is None:
                continue
            if c < x.stalled_until:
                continue  # its pass holds its endpoint and reads nothing new
            due = (c - x.phase) % x.period == 0
            if not due and sim.cap == math.inf:
                continue
            reach = sum(sim.entries(r) for r in sim.overlapping(x.position))
            if not due and reach > sim.cap * w.n:
                x.position, x.dropped = None, x.dropped + 1
                continue
            if due:
                x.reads += 1
                x.read += reach
                # What it needs: the distinct keys changed since its position.
                miss, extra = 1.0, 0.0
                for r in sim.overlapping(x.position):
                    for s in r.segs:
                        if s.start >= x.position or r.a >= x.position:
                            miss *= 1 - s.d
                            extra += s.extra
                x.need += (1 - miss) * w.n + extra
                x.position = c + 1
        for x in cs:
            if x.position is None and (c - x.phase) % x.period == 0:
                x.reads += 1
                x.read += w.n  # a full pass
                x.position = c + 1
        live = {x.position for x in cs if x.position is not None}
        sim.upkeep(c, live)
        if c >= half:
            m = len(sim.spans)
            e = sum(sim.entries(r) for r in sim.spans) / w.n
            spans_sum += m
            spans_max = max(spans_max, m)
            ent_sum += e
            ent_max = max(ent_max, e)
            held = sum(min(1.0, sum(s.d for s in r.segs)) for r in sim.spans)
            blocks += held + FP * (m - held)
    meas = w.commits - half
    slow = max(cs, key=lambda x: x.period)
    return {
        "amp": sim.written / sim.committed,
        "spans": spans_sum / meas,
        "spans_max": spans_max,
        "entries": ent_sum / meas,
        "entries_max": ent_max,
        "blocks": blocks / meas,
        "read": slow.read / max(slow.reads, 1),
        "read_ratio": slow.read / slow.need if slow.need else 1.0,
        "dropped": sum(x.dropped for x in cs),
        "waits": sim.waits / meas,
    }


def scenarios(n: int) -> dict[str, Workload]:
    day, hour = 8640, 360
    long = 200_000 if n >= 10**8 else 40_000
    short = 40_000 if n >= 10**8 else 20_000
    out = {}
    for k in (1, 10, 100, 1000):
        commits = long if k <= 10 else short
        out[f"{k} daily, spread"] = Workload(
            n, commits, periods=[1] + [day] * k, phases=[0] + [i * day // k for i in range(k)]
        )
    out["an endpoint at every commit"] = Workload(n, short, periods=[1000] * 1000, phases=list(range(1000)))
    out["stalled full pass, + hourly"] = Workload(n, long, periods=[1, hour, day], stall=(2, 20_000))
    out["60 staggered hourly"] = Workload(
        n, short, periods=[1] + [hour] * 60, phases=[0] + [i * 6 for i in range(60)]
    )
    out["temporary keys (50%, 100 commits), hourly + daily"] = Workload(
        n, long, periods=[1, hour, day], temp=0.5
    )
    out["1M-key batch after 1K ones, hourly + daily"] = Workload(
        n, long, periods=[1, hour, day], big_at=long // 2 + 1
    )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--policies", default="blocked,versions")
    ap.add_argument("--lam", type=float, default=1.0)
    ap.add_argument("--z", type=float, default=1e6)
    ap.add_argument("--rates", default="instant,2e6", help="upkeep: instant, or entries per commit")
    ap.add_argument("--cap", type=float, default=math.inf)
    ap.add_argument("--only", default="")
    ap.add_argument("--skip", default="", help="scenarios to leave out (substring, ;-separated)")
    ap.add_argument("--blocked-max", type=int, default=101, help="consumers past which `blocked` is not run")
    args = ap.parse_args()
    print(
        "| Keys | Scenario | Policy | Upkeep | Written per entry committed | Spans, mean (max) | "
        "Entries per live key, mean (max) | Blocks per cold lookup | Slowest reader: entries read per read "
        "(x what changed) | Upkeep waits per commit | Positions dropped |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for n in (int(float(x)) for x in args.sizes.split(",")):
        for name, w in scenarios(n).items():
            if args.only and args.only not in name:
                continue
            if any(x and x in name for x in args.skip.split(";")):
                continue
            for policy in args.policies.split(","):
                if policy == "blocked" and len(w.periods) > args.blocked_max:
                    continue
                for rate in args.rates.split(","):
                    r = run(w, policy, args.lam, args.z, None if rate == "instant" else float(rate), args.cap)
                    print(
                        f"| {n:,} | {name} | {policy} | {rate} | {r['amp']:.1f} | {r['spans']:.1f} "
                        f"({r['spans_max']}) | {r['entries']:.2f} ({r['entries_max']:.2f}) | {r['blocks']:.2f} | "
                        f"{r['read'] / 1e3:,.0f}K ({r['read_ratio']:.2f}) | {r['waits']:.2f} | {r['dropped']} |",
                        flush=True,
                    )


if __name__ == "__main__":
    main()
