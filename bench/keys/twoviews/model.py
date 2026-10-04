"""Two views against spans (docs/key-index-two-views.md): the merge policies replayed
on metadata, then a request, round-trip, latency and dollar model per workload.

    python3 bench/keys/twoviews/model.py [--sizes 1e6,1e8] [--quick]

Replayed with spans.py's density model and policy (`Sim`): 1K keys per commit
(97.5% existing, 2.5% new), consumers reading every `period` commits.

- **spans**: `capped`, as built (span cap 32);
- **spans + levers**: `capped` with merging forced past 6 spans (eager merging),
  and one metadata object holding every file's filter and block index;
- **key view (K)**: the same policy with no endpoint but the head's, so merges keep
  one version per key: a single-version LSM whose level 0 is the deltas;
- **time view (T)**: analytic, aligned nodes of b^j commits (j <= L) holding each
  touched key's net change, level 0 the deltas. A reader's cover is the canonical
  decomposition of [P, head] (check.py shows the kept nodes always provide it).

Everything here is an **estimate** on a model: 10 B per entry with filters (1.75 B
of filter), 2,400 entries per 64 KiB block, 64 MiB files, 16 MiB range reads,
files of 2 MiB and spans of 32 MiB read whole, a run streamed past `Options.stream_density` and `stream_reads`, 30 ms per request,
64 requests in parallel, 500 MB/s aggregate. Prices: S3 Standard list (PUT $5 per
million, GET $0.40 per million, $0.023 per GB-month); Railway buckets (requests
and transfer free, $0.015 per GB-month).
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from spans import FP, Sim, Workload  # noqa: E402

ENTRY, FILTER, BLOCK = 10.0, 1.75, 2400
MiB = 2**20
RANGE, FILE, SMALL, WHOLE = 16 * MiB, 64 * MiB, 2 * MiB, 32 * MiB
RTT, PAR, BW, CONN, DECODE = 0.030, 64, 500e6, 80e6, 18e6
DAY, HOUR = 8640, 360
MONTH = 259_200  # commits a month, one every 10 s
S3 = {"put": 5e-6, "get": 0.4e-6, "gb": 0.023}
RAILWAY = {"put": 0.0, "get": 0.0, "gb": 0.015}
K_PER_COMMIT, NEW = 1000, 0.025


# -- replay ------------------------------------------------------------------------------


class Counting(Sim):
    """`Sim`, counting merges and their requests, and sampling the layout."""

    def __init__(self, *a, fanin: int = 32, sample: int = 50, **k):
        super().__init__(*a, **k)
        self.fanin, self.sample = fanin, sample
        self.c = 0
        self.reset()

    def reset(self):
        self.merges = self.gets = self.puts = 0.0
        self.snapshots: list[list[tuple[float, float]]] = []

    def apply(self, lo, count, live):
        ins = [self.entries(r) for r in self.spans[lo : lo + count]]
        super().apply(lo, count, live)
        out = self.entries(self.spans[lo])
        self.merges += 1
        self.gets += sum(math.ceil(e * ENTRY / RANGE) for e in ins)  # inputs read whole
        self.puts += math.ceil(out * ENTRY / FILE) + math.ceil(out * ENTRY / RANGE)  # files, parts

    def layout(self) -> list[tuple[float, float]]:
        """(entries, share of the live keys present) per run, newest first."""

        out = []
        for sp in reversed(self.spans):
            present = min(1.0, sum(s.d for s in sp.segs) + sum(s.extra for s in sp.segs) / self.n)
            out.append((self.entries(sp), present))
        return out

    def commit(self, c, temp_out):
        if self.committed == 0 and self.written == 0:
            self.reset()  # the measured half starts
        if self.c % self.sample == 0:
            self.snapshots.append(self.layout())
        self.c += 1
        super().commit(c, temp_out)


def replay(n: int, commits: int, periods: list[int], phases: list[int], policy: str, fanin: int) -> dict:
    """spans.py's `run`, keeping per-read cover sizes and the sampled layouts."""

    w = Workload(n, commits, periods=periods, phases=phases)
    sim = Counting(w, policy, 1.0, 1e6, None, math.inf, fanin=fanin)
    positions = [0] * len(periods)
    half = commits // 2
    reads: dict[int, list[tuple[float, float, int]]] = {}  # period -> (entries read, changed, runs)
    for c in range(commits):
        if c == half:
            sim.written = sim.committed = 0.0
        sim.commit(c, 0.0)
        for i, (p, ph) in enumerate(zip(periods, phases, strict=True)):
            if (c - ph) % p:
                continue
            over = sim.overlapping(positions[i])
            if c >= half and p > 1:
                miss, extra = 1.0, 0.0
                for r in over:
                    for s in r.segs:
                        if s.start >= positions[i] or r.a >= positions[i]:
                            miss *= 1 - s.d
                            extra += s.extra
                read = sum(sim.entries(r) for r in over)
                reads.setdefault(p, []).append((read, (1 - miss) * n + extra, len(over)))
            positions[i] = c + 1
        sim.upkeep(c, set(positions))
    measured = commits - half
    snaps = sim.snapshots[len(sim.snapshots) // 2 :]
    return {
        "amp": sim.written / sim.committed,
        "merges": sim.merges / measured,
        "gets": sim.gets / measured,
        "puts": sim.puts / measured,
        "snapshots": snaps,
        "runs": sum(len(s) for s in snaps) / len(snaps),
        "runs_max": max(len(s) for s in snaps),
        "entries": sum(sum(e for e, _ in s) for s in snaps) / len(snaps),
        "reads": {
            p: (
                sum(r for r, _, _ in v) / len(v),
                sum(x for _, x, _ in v) / len(v),
                sum(k for _, _, k in v) / len(v),
            )
            for p, v in reads.items()
        },
    }


# -- the time view -------------------------------------------------------------------------


def touched(n: int, m: int) -> float:
    """Distinct keys a node of m commits holds (net changes; new keys stay)."""

    d = K_PER_COMMIT * (1 - NEW) / n
    return n * (1 - (1 - d) ** m) + m * K_PER_COMMIT * NEW


def tview(n: int, b: int, top: int) -> dict:
    per = touched(n, 1)
    amp = sum(touched(n, b**j) / b**j for j in range(1, top + 1)) / per
    gets = puts = 0.0
    for j in range(1, top + 1):
        child, out = touched(n, b ** (j - 1)) * ENTRY, touched(n, b**j) * ENTRY
        gets += b * math.ceil(child / RANGE) / b**j  # children read whole
        puts += (math.ceil(out / FILE) + (math.ceil(out / RANGE) if out > RANGE else 0)) / b**j
    return {"amp": amp, "merges": sum(1 / b**j for j in range(1, top + 1)), "gets": gets, "puts": puts}


def tcover(n: int, b: int, top: int, lag: int, rng: random.Random, samples: int = 200) -> tuple[float, float]:
    """Mean (nodes, entries) of the canonical cover of `lag` commits ending at a random head."""

    nodes = entries = 0.0
    for _ in range(samples):
        head = rng.randrange(10**6, 2 * 10**6)
        x, y = head - lag + 1, head
        while x <= y:
            for j in range(top, -1, -1):
                s = b**j
                if x % s == 0 and x + s - 1 <= y:
                    nodes += 1
                    entries += touched(n, s)
                    x += s
                    break
    return nodes / samples, entries / samples


def tkept(n: int, b: int, top: int, lags: list[int], rng: random.Random, samples: int = 20) -> float:
    """Mean entries the time view keeps for endpoints `lags` behind a random head:
    their chains, the frontier and the top-level nodes (check.py's rule)."""

    from check import kept

    total = 0.0
    for _ in range(samples):
        head = rng.randrange(10**6, 2 * 10**6)
        ends = {head + 1 - lag for lag in lags}
        total += sum(touched(n, b**j) for j, _ in kept(ends, head, b, top))
    return total / samples


# -- requests, round trips, seconds ----------------------------------------------------------


def secs(rts: int, gets: float, nbytes: float, decoded: float = 0.0) -> float:
    """Round trips and request waves, then transfer (aggregate, or one connection's
    share for few large GETs), then decoding (`decoded` entries on ~4 cores)."""

    transfer = max(nbytes / BW, min(nbytes / max(gets, 1), RANGE) / CONN)
    return (rts + max(0, math.ceil(gets / PAR) - 1)) * RTT + transfer + decoded / DECODE


def lookups(layout, keys: int = 1000, metadata: bool = False) -> tuple[int, float, float, float]:
    """Cold exact lookups of `keys` random keys: (round trips, GETs, bytes, entries decoded)."""

    rts, gets, nbytes, decoded = 1, 0.0, 0.0, 0.0
    for e, present in layout:
        size = e * ENTRY
        blocks = e / BLOCK
        hits = keys * (present + FP)
        t = blocks * (1 - math.exp(-hits / blocks)) if blocks > 0 else 0.0
        decoded += t * BLOCK
        reads = max(t * (1 - t / blocks), t * BLOCK * (ENTRY - FILTER) / RANGE, 1)
        # Read whole, or streamed: `Options.stream_density` and `stream_reads` (16 per 8 MiB segment).
        if size <= WHOLE or keys > 0.02 * e or reads > 16 * math.ceil(size / (8 * MiB)):
            gets += math.ceil(size / RANGE)
            nbytes += size
            continue
        rts = 2
        files = math.ceil(size / FILE)
        gets += 0 if metadata else files
        nbytes += e * FILTER
        gets += reads
        nbytes += t * BLOCK * (ENTRY - FILTER)
    if metadata:
        gets += 1
    return rts, gets, nbytes, decoded


def pages(runs: list[float], changed: float, page: int = 100_000, metadata: bool = False):
    """A paged merge of `runs` (entries each) yielding `changed` keys: first page and
    the full read, each (round trips, GETs, bytes)."""

    n_pages = max(1, math.ceil(changed / page))
    phi = min(1.0, page / max(changed, 1))
    big = [e for e in runs if e * ENTRY > SMALL]
    smalls = [e for e in runs if e * ENTRY <= SMALL]  # read whole, one GET each, with the first page
    small = sum(smalls)
    tails = 1 if metadata else sum(math.ceil(e * ENTRY / FILE) for e in big)
    tail_bytes = sum(e / BLOCK * 30 for e in big)
    per_page = sum(max(1, math.ceil(phi * e * ENTRY / RANGE)) for e in big)
    first = (
        2 if big else 1,
        tails + per_page + len(smalls),
        tail_bytes + phi * sum(big) * ENTRY + small * ENTRY,
    )
    full = (
        (1 if big else 0) + n_pages,
        tails + n_pages * per_page + len(smalls),
        tail_bytes + sum(runs) * ENTRY,
    )
    return first, full


# -- the report -----------------------------------------------------------------------------


def scenario(n: int, readers: int, quick: bool):
    """Consumers: one at the head, `readers` daily spread over the day, one hourly."""

    periods = [1] + [DAY] * readers + [HOUR]
    phases = [0] + [i * DAY // readers for i in range(readers)] + [0]
    commits = (40_000 if quick else 120_000) if n >= 10**8 else (20_000 if quick else 40_000)
    return periods, phases, commits


def mean(xs):
    xs = list(xs)
    return sum(xs) / len(xs)


def report(n: int, readers: int, quick: bool, b: int, top: int):
    periods, phases, commits = scenario(n, readers, quick)
    spans = replay(n, commits, periods, phases, "capped", 32)
    eager = replay(n, commits, periods, phases, "capped", 6)
    kview = replay(n, commits, [1], [0], "capped", 32)
    t = tview(n, b, top)
    rng = random.Random(1)
    lags = [rng.randrange(DAY) for _ in range(readers)] + [rng.randrange(HOUR)]
    t_kept = tkept(n, b, top, lags, rng)
    raw = DAY * touched(n, 1)  # deltas kept back to the oldest endpoint (a day)

    designs = {
        "spans, as built": (spans, False),
        "spans + levers": (eager, True),
        "two views": (kview, False),
    }
    print(f"\n### {n:,} keys, {readers} daily readers + 1 hourly (T: fanout {b}, top level {top})\n")
    print("| | spans, as built | spans + levers | two views (K + T) |")
    print("|---|---|---|---|")

    def row(name, f):
        print(f"| {name} | " + " | ".join(f(k, d, m) for k, (d, m) in designs.items()) + " |")

    def two(k, a, b_):
        return b_ if k == "two views" else a

    row("runs at the head, mean (max)", lambda k, d, m: f"{d['runs']:.1f} ({d['runs_max']})")
    row("entries stored per live key (K for two views)", lambda k, d, m: f"{d['entries'] / n:.2f}")
    row(
        "background entry writes per entry committed",
        lambda k, d, m: two(
            k, f"{d['amp']:.1f}", f"{d['amp']:.1f} (K) + {t['amp']:.1f} (T) = {d['amp'] + t['amp']:.1f}"
        ),
    )
    row(
        "background PUTs · GETs per commit",
        lambda k, d, m: two(
            k,
            f"{d['puts']:.2f} · {d['gets']:.2f}",
            f"{d['puts'] + t['puts']:.2f} · {d['gets'] + t['gets']:.2f}",
        ),
    )

    # Writers' cold head lookups, 1K keys.
    def look(k, d, m):
        r = [lookups(s, metadata=m) for s in d["snapshots"]]
        rts, gets, nb, dec = (
            max(x[0] for x in r),
            mean(x[1] for x in r),
            mean(x[2] for x in r),
            mean(x[3] for x in r),
        )
        return f"{rts} RT · {gets:,.0f} GETs · {nb / 1e6:,.0f} MB · {secs(rts, gets, nb, dec):.2f} s"

    row("1K exact head lookups, cold", look)

    # A 100K-key page at the head (and at a pinned position: the same layout, D93).
    def page(k, d, m):
        r = [pages([e for e, _ in s], n, metadata=m)[0] for s in d["snapshots"]]
        rts, gets = max(x[0] for x in r), mean(x[1] for x in r)
        nb = mean(x[2] for x in r)
        return f"{rts} RT · {gets:,.0f} GETs · {nb / 1e6:.1f} MB · {secs(rts, gets, nb, nb / ENTRY):.2f} s"

    row("100K-key page at the head or a pinned snapshot, cold", page)

    # Catch-up of the daily reader and the hourly one.
    for p, label in ((DAY, "daily reader"), (HOUR, "hourly reader")):

        def catch(k, d, m, p=p):
            lag = p
            if k == "two views":
                nodes, ent = tcover(n, b, top, lag, rng)
                runs = [ent / nodes] * round(nodes)
                changed = touched(n, lag)
                ratio = ent / changed
            else:
                ent, changed, nruns = d["reads"][p]
                runs = [ent / nruns] * max(1, round(nruns))
                ratio = ent / changed
            first, full = pages(runs, changed, metadata=m)
            return (
                f"{len(runs)} runs · first page {secs(*first, first[2] / ENTRY):.2f} s · full "
                f"{full[1]:,.0f} GETs, {full[2] / 1e6:,.0f} MB, {secs(*full, full[2] / ENTRY):.1f} s · "
                f"reads {ratio:.2f}× what changed"
            )

        row(f"changes(P, head), {label}", catch)

    # Storage.
    def stored(k, d, m):
        if k == "two views":
            return (
                f"{d['entries'] * ENTRY / 1e9:.2f} GB (K) + {t_kept * ENTRY / 1e9:.2f} GB (T) + "
                f"{raw * ENTRY / 1e9:.2f} GB (raw deltas, a day)"
            )
        return f"{d['entries'] * ENTRY / 1e9:.2f} GB"

    row("bytes stored, mean", stored)

    # Dollars a month, workload by workload: (PUTs, GETs, GB) each.
    def parts(k, d, m) -> dict[str, tuple[float, float, float]]:
        out = {"append: the delta PUT": (MONTH, 0.0, 0.0)}
        out["writers' head lookups, if cold every commit"] = (
            0.0,
            MONTH * mean(lookups(s, metadata=m)[1] for s in d["snapshots"]),
            0.0,
        )
        bg = (MONTH * d["puts"], MONTH * d["gets"])
        if k == "two views":
            bg = (bg[0] + MONTH * t["puts"], bg[1] + MONTH * t["gets"])
        out["background merges and node builds"] = (*bg, 0.0)
        gets = 0.0
        for p, times in ((DAY, 30 * readers), (HOUR, 720)):  # daily readers 30 times, the hourly 720
            if k == "two views":
                nodes, ent = tcover(n, b, top, p, rng, 50)
                runs, changed = [ent / nodes] * round(nodes), touched(n, p)
            else:
                ent, changed, nruns = d["reads"][p]
                runs = [ent / nruns] * max(1, round(nruns))
            gets += times * pages(runs, changed, metadata=m)[1][1]
        out["changes(): every catch-up"] = (0.0, gets, 0.0)
        snap = d["snapshots"][-1]
        out["one full pass (pinned-snapshot pages)"] = (
            0.0,
            pages([e for e, _ in snap], n, metadata=m)[1][1],
            0.0,
        )
        gb = d["entries"] * ENTRY / 1e9 + ((t_kept + raw) * ENTRY / 1e9 if k == "two views" else 0)
        out["bytes stored (lagging readers included)"] = (0.0, 0.0, gb)
        return out

    def money(x: float) -> str:
        return f"${x:,.2f}" if x >= 0.1 else f"${x:.3f}"

    def cost(prices, part: tuple[float, float, float]) -> float:
        return part[0] * prices["put"] + part[1] * prices["get"] + part[2] * prices["gb"]

    table = {k: parts(k, d, m) for k, (d, m) in designs.items()}
    for name in table["spans, as built"]:
        print(
            f"| $/month, S3: {name} | " + " | ".join(money(cost(S3, table[k][name])) for k in designs) + " |"
        )
    for label, prices, skip in (
        ("$/month, S3, total, warm writer", S3, "writers' head lookups, if cold every commit"),
        ("$/month, S3, total, cold writer", S3, None),
        ("$/month, Railway, total (storage only)", RAILWAY, None),
    ):
        print(
            f"| **{label}** | "
            + " | ".join(
                money(sum(cost(prices, v) for name, v in table[k].items() if name != skip)) for k in designs
            )
            + " |"
        )


def fanouts(n: int):
    rng = random.Random(2)
    lags = (
        (HOUR, "hourly"),
        (DAY, "daily"),
        (10_000, "10,000 behind"),
        (7 * DAY, "a week"),
        (30 * DAY, "a month"),
    )
    print(f"\n### Time view at {n:,} keys: fanout and top level\n")
    print(
        "| b | top level (commits per node) | writes per entry committed | "
        + " | ".join(f"{label}: nodes, read × changed" for _, label in lags)
        + " |"
    )
    print("|---|---|---|" + "---|" * len(lags))
    for b, top in ((2, 13), (2, 18), (4, 6), (4, 9), (8, 4), (8, 6), (16, 3), (16, 5)):
        t = tview(n, b, top)
        cells = []
        for lag, _ in lags:
            nodes, ent = tcover(n, b, top, lag, rng, 100)
            cells.append(f"{nodes:.1f}, {ent / touched(n, lag):.2f}×")
        print(f"| {b} | {top} ({b**top:,}) | {t['amp']:.1f} | " + " | ".join(cells) + " |")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--readers", default="10,100")
    ap.add_argument("--b", type=int, default=4)
    ap.add_argument("--top", type=int, default=9)
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args()
    for n in (int(float(x)) for x in args.sizes.split(",")):
        fanouts(n)
        for readers in (int(x) for x in args.readers.split(",")):
            report(n, readers, args.quick, args.b, args.top)


if __name__ == "__main__":
    main()
