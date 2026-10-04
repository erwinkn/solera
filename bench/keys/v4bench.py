"""Format v4 on real files (docs/key-index-design.md): the `capped` policy
running real v4 merges, then paged catch-up, lookups and scans read cold,
each reader in its own process, checked key by key.

    RAYON_NUM_THREADS=3 uv run python bench/keys/v4bench.py --size 1e6 --scenario daily

A trace of `--commits` commits of 1K keys (90% updates, 5% removes, 5% adds,
half re-adds; `--hot`: half of the updates go to 1% of the keys), with a
commit of `--large` keys every `--large-every` commits, over a base of n
keys (generation 1). Every entry names its predecessor exactly. Upkeep runs
spans.py's `capped` policy on the spans' real entry counts per segment and
executes each merge with `_native.v4_merge`. Consumers (per scenario) read
at their period: their positions are live endpoints. Four readers stay 1,
100, 360 and 10,000 commits behind the head; each is born at the head + 1
when its distance comes up.

At the end, per reader at P: `changes(P, H)` paged 100K keys at a time over
the spans overlapping [P, H] (a global key cutoff per page; each span's
blocks below it fetched in range reads), cold (30 ms per request, 80 MB/s
per connection, 64 in parallel), in a fresh process: first page, full read,
GETs, MB, decoded MB, peak RSS above the process's baseline. Every key and
class is compared with the per-commit fold (`_native.presence` over the
deltas of [P, H]). The control reads the same deltas packed in one object
(range reads) and merges them at once (`presence`): its first page needs
every delta's first block, so it is its full read. Then 1K exact lookups at
the head (filters, then the blocks that may hold each key's newest version)
and a 100K-key page mid-index at the head, checked against the trace.
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import math
import multiprocessing as mp
import random
import resource
import shutil
import sys
import time
import zlib
from pathlib import Path

from obstore.store import LocalStore
from solera import _native
from solera import keys as K
from solera.keys.io import RANGE, ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from spans import Seg, Sim, Span, Workload  # noqa: E402

from bench import key_of  # noqa: E402

PER = 1000
PAGE = 100_000
HEAD = 2**64 - 1
BEHIND = (1, 100, 360, 10_000)
WRITER = {"block_size": 65536, "max_file_bytes": 64 * 2**20}
WHOLE = 2 * RANGE  # spans this small are read whole (Options.whole_threshold)


def gen(c: int) -> int:
    return 1 if c < 0 else 1000 + c


def cold(root: Path, latency=0.03, bandwidth=80e6) -> ObjectIO:
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


def blocks_of(data: bytes) -> tuple[list[bytes], int]:
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    return [data[off : off + size] for _, off, size, _, _ in tail["blocks"]], tail["codec"]


# -- the trace ---------------------------------------------------------------------------


class Trace:
    def __init__(self, n: int, hot: bool, seed: int = 11):
        self.n, self.gap, self.hot = n, 10**12 // n, hot
        self.rng = random.Random(seed)
        self.state: dict[int, int | None] = {}  # changed ids: generation, None once removed (else live at 1)
        self.removed: list[int] = []
        self.fresh = n

    def key(self, i: int) -> bytes:
        return key_of(i * self.gap) if i < self.n else key_of((i - self.n) * 7919 % 10**12) + b"n"

    def pick(self) -> int:
        if self.hot and self.rng.random() < 0.5:
            return self.rng.randrange(self.n // 100) * 100
        return self.rng.randrange(self.n)

    def commit(self, c: int, size: int) -> bytes:
        rng, g, chosen = self.rng, gen(c), {}
        while len(chosen) < size:
            x = rng.random()
            if x < 0.95:
                i = self.pick()
                was = self.state.get(i, 1)
                if i in chosen or was is None:
                    continue
                chosen[i] = (x >= 0.90, was)
            elif self.removed and rng.random() < 0.5:
                i = self.removed.pop(rng.randrange(len(self.removed)))
                if i in chosen or self.state.get(i, 1) is not None:
                    continue
                chosen[i] = (False, None)
            else:
                i = self.fresh
                self.fresh += 1
                chosen[i] = (False, None)
        ids = sorted(chosen, key=self.key)
        for i in ids:
            deleted, _ = chosen[i]
            self.state[i] = None if deleted else g
            if deleted:
                self.removed.append(i)
        keys = [self.key(i) for i in ids]
        return K.encode_file(
            keys, [g] * len(keys), bytes(chosen[i][0] for i in ids), predecessors=[chosen[i][1] for i in ids]
        )

    def live_at_head(self, i: int) -> int | None:
        return self.state.get(i, 1)


# -- the tiling ----------------------------------------------------------------------------


class Tiling:
    """Real span files planned by spans.py's `capped` policy on their real entries."""

    def __init__(self, n: int, base: list[bytes]):
        self.sim = Sim(Workload(n, 1), "capped", 1.0, 1e6, None, math.inf)
        self.files: dict[int, list[bytes]] = {}
        root = self.sim.spans[0]
        self.files[id(root)] = base
        self.written_entries = self.written_bytes = 0
        self.delta_entries = self.delta_bytes = 0
        self.merges = 0
        self.max_spans = self.max_files = 0

    def add(self, c: int, data: bytes, entries: int) -> None:
        sp = Span(c, c, [Seg(c, 0.0, float(entries))])
        self.sim.spans.append(sp)
        self.files[id(sp)] = [data]
        self.delta_entries += entries
        self.delta_bytes += len(data)

    def upkeep(self, live: set[int]) -> None:
        while (p := self.sim.plan(live)) is not None:
            lo, count = p
            ins = self.sim.spans[lo : lo + count]
            runs = []
            for sp in reversed(ins):
                blocks, codec = [], 1
                for d in self.files[id(sp)]:
                    b, codec = blocks_of(d)
                    blocks += b
                runs.append((blocks, codec))
            ends = sorted(e for e in live if ins[0].a < e <= ins[-1].b)
            files, segments = _native.v4_merge(
                [r[0] for r in runs], [r[1] for r in runs], [gen(e) for e in ends], lo == 0, **WRITER
            )
            starts = [ins[0].a] + ends
            segs = [
                Seg(s, 0.0, float(k)) for s, k in zip(starts, segments, strict=True) if k or s == ins[0].a
            ]
            if lo == 0:  # the base: the policy counts its initial segment as n live keys
                segs[0] = Seg(-1, 1.0, max(0.0, segs[0].extra - self.sim.n))
            out = Span(ins[0].a, ins[-1].b, segs)
            for sp in ins:
                del self.files[id(sp)]
            self.files[id(out)] = files
            self.sim.spans[lo : lo + count] = [out]
            self.written_entries += sum(segments)
            self.written_bytes += sum(len(f) for f in files)
            self.merges += 1
        self.max_spans = max(self.max_spans, len(self.sim.spans))
        self.max_files = max(self.max_files, sum(len(f) for f in self.files.values()))


# -- readers (run in a fresh process) ------------------------------------------------------


class SpanFiles:
    """One span's files: their block indexes, read once."""

    def __init__(self, names: list[tuple[str, int]]):
        self.names = names
        self.blocks: list[
            tuple[str, bytes, int, int, int, int]
        ] = []  # (file, first key, offset, size, entries, codec)
        self.cache: dict[int, bytes] = {}
        self.counted: set[int] = set()

    async def open(self, io: ObjectIO) -> None:
        """Every file's block index, in parallel: one GET each (the state knows each file's
        index length, as `FileInfo.index` does today)."""

        async def index(name, size, index_len):
            return name, K.parse_index(await io.read(name, size - index_len, size, size), size)

        for name, idx in await asyncio.gather(*(index(n, sz, il) for n, sz, il, _ in self.names)):
            for first, off, bsize, entries, _ in idx["blocks"]:
                self.blocks.append((name, first, off, bsize, entries, idx["codec"]))
        self.firsts = [b[1] for b in self.blocks]

    async def fetch(self, io: ObjectIO, lo: int, hi: int, sizes: dict) -> list[bytes]:
        """Blocks [lo, hi), consecutive ones of a file in one range read (up to RANGE)."""

        need = [i for i in range(lo, hi) if i not in self.cache]
        runs, cur = [], []
        for i in need:
            if cur and (
                i != cur[-1] + 1
                or self.blocks[i][0] != self.blocks[cur[0]][0]
                or self.blocks[i][2] + self.blocks[i][3] - self.blocks[cur[0]][2] > RANGE
            ):
                runs.append(cur)
                cur = []
            cur.append(i)
        if cur:
            runs.append(cur)

        async def get(run):
            name, start = self.blocks[run[0]][0], self.blocks[run[0]][2]
            end = self.blocks[run[-1]][2] + self.blocks[run[-1]][3]
            data = await io.read(name, start, end, sizes[name])
            for i in run:
                off = self.blocks[i][2] - start
                self.cache[i] = data[off : off + self.blocks[i][3]]

        await asyncio.gather(*(get(r) for r in runs))
        return [self.cache[i] for i in range(lo, hi)]


async def paged(io, spans, sizes, after_fn, limit=PAGE, after=None):
    """Pages over `spans` (newest first): each page a global key cutoff past
    which every span's blocks below it hold `limit` entries or more. Yields
    (runs, codecs, after, bound) per page; `after_fn` gets each result back."""

    cutoff, decoded = None, 0
    for s in spans:
        s.cum = [0]
        for b in s.blocks:
            s.cum.append(s.cum[-1] + b[4])
    while True:
        starts = [max(0, bisect.bisect_right(s.firsts, after) - 1) if after is not None else 0 for s in spans]
        cands = sorted(
            {
                s.blocks[i][1]
                for s, st in zip(spans, starts, strict=True)
                for i in range(st + 1, len(s.blocks))
                if cutoff is None or s.blocks[i][1] > cutoff
            }
        )

        def below(first, starts=starts):
            # Whole blocks past each span's start block, below `first`: the start block may reach far beyond.
            return sum(
                max(0, s.cum[bisect.bisect_left(s.firsts, first)] - s.cum[st + 1])
                for s, st in zip(spans, starts, strict=True)
            )

        # The smallest cutoff with `limit` entries below it (they only grow with it).
        lo, hi = 0, len(cands)
        while lo < hi:
            mid = (lo + hi) // 2
            if below(cands[mid]) >= limit:
                hi = mid
            else:
                lo = mid + 1
        bound = cands[lo] if lo < len(cands) else None
        runs, codecs = [], []
        fetches = []
        for s in spans:
            start = max(0, bisect.bisect_right(s.firsts, after) - 1) if after is not None else 0
            end = bisect.bisect_left(s.firsts, bound) if bound is not None else len(s.blocks)
            fetches.append(s.fetch(io, start, end, sizes))
            codecs.append(s.blocks[0][5] if s.blocks else 1)
        runs = await asyncio.gather(*fetches)
        for s, st, r in zip(spans, starts, runs, strict=True):  # each block's decoded size counted once
            for i in range(st, st + len(r)):
                if i not in s.counted:
                    s.counted.add(i)
                    decoded += len(zlib.decompress(s.cache[i]))
        last, more = after_fn(runs, codecs, after, bound)
        cutoff = bound
        if more is None:  # one page wanted
            return decoded
        if more:
            after = last
            continue
        if bound is None:
            return decoded
        after = last if last is not None and (after is None or last > after) else after
        for s in spans:  # drop blocks the next page cannot need
            keep = max(0, bisect.bisect_right(s.firsts, after) - 1) if after is not None else 0
            for i in [i for i in s.cache if i < keep]:
                del s.cache[i]


def reader(job: dict, out) -> None:
    """A fresh process: one measured read, its result through `out`."""

    base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    io = cold(Path(job["root"]))
    t = time.perf_counter()
    first: list[float] = []
    result: dict = {}

    async def run():
        sizes = dict(job["sizes"])
        if job["kind"] == "packed":
            data = await io.read_whole(job["packed"], sizes[job["packed"]])
            offs, at = [], 0
            for size in job["delta_sizes"]:
                offs.append((at, at + size))
                at += size
            rs = [blocks_of(data[a:z]) for a, z in reversed(offs)]
            counts, _, _ = _native.presence([r[0] for r in rs], [r[1] for r in rs], False)
            first.append(time.perf_counter() - t)
            result["counts"] = list(counts)
            return 0
        spans = [SpanFiles(names) for names in job["spans"]]
        await asyncio.gather(*(s.open(io) for s in spans))
        got: dict[bytes, int] = {}
        digest = [0, 0]  # keys, and an order-free checksum of (key, class)

        def changes(runs, codecs, after, bound):
            keys, classes, gens, deleted, _, last, more = _native.v4_changes(
                runs, codecs, after, bound, PAGE, job["g_p"], job["g_n1"]
            )
            digest[0] += len(keys)
            digest[1] = (
                digest[1] + sum(zlib.crc32(k + bytes([c])) for k, c in zip(keys, classes, strict=True))
            ) % 2**64
            if not first:
                first.append(time.perf_counter() - t)
            return last, more

        def scan(runs, codecs, after, bound):
            keys, gens, _, last, more = _native.v4_scan(runs, codecs, after, bound, PAGE, HEAD)
            got.update(zip(keys, gens, strict=True))
            if not first:
                first.append(time.perf_counter() - t)
            return last, None  # one page

        if job["kind"] == "changes":
            decoded = await paged(io, spans, sizes, changes)
            result["digest"] = digest
            return decoded
        if job["kind"] == "scan":
            decoded = await paged(io, spans, sizes, scan, after=job["after"])  # a page mid-index
            result["scan"] = got
            return decoded
        if job["kind"] == "lookup":
            keys = sorted(job["keys"])
            # Tails (filters) of every file, then the blocks that may hold each key's newest version.
            found: dict[bytes, tuple[int, bool]] = {}

            async def tail(name, size, tail_len):
                return K.parse_tail(await io.read(name, size - tail_len, size, size), size)

            files = [f for names in job["spans"] if sum(f[1] for f in names) > WHOLE for f in names]
            names = [f[0] for f in files]
            tails = dict(
                zip(names, await asyncio.gather(*(tail(f[0], f[1], f[3]) for f in files)), strict=True)
            )
            wanted = []
            for s in spans:
                if sum(f[1] for f in s.names) <= WHOLE:  # small spans are read whole, as today
                    wanted.append(list(range(len(s.blocks))))
                    continue
                per_file = {}
                for i, b in enumerate(s.blocks):
                    per_file.setdefault(b[0], []).append(i)
                need = set()
                for name, idxs in per_file.items():
                    tl = tails[name]
                    kf = tl["key_filter"]
                    maybe = _native.bloom_check_keys(kf[2], kf[0], kf[1], keys)
                    firsts = [s.blocks[i][1] for i in idxs]
                    for k, m in zip(keys, maybe, strict=True):
                        if not m or k < tl["min_key"] or k > tl["max_key"]:
                            continue
                        j = bisect.bisect_right(firsts, k) - 1
                        if j >= 0:
                            need.add(idxs[j])
                            if firsts[j] == k and j > 0:
                                need.add(idxs[j - 1])
                wanted.append(sorted(need))

            async def blocks_for(s, idxs):
                if idxs and idxs == list(range(len(s.blocks))):
                    return await s.fetch(io, 0, len(s.blocks), sizes)
                got = await asyncio.gather(*(s.fetch(io, i, i + 1, sizes) for i in idxs))
                return [b for bl in got for b in bl]

            got_blocks = await asyncio.gather(
                *(blocks_for(s, idxs) for s, idxs in zip(spans, wanted, strict=True))
            )
            for s, blocks in zip(spans, got_blocks, strict=True):
                if not blocks:
                    continue
                f, g, d, _ = _native.v4_lookup([blocks], [s.blocks[0][5]], keys, HEAD)
                for k, ff, gg, dd in zip(keys, f, g, d, strict=True):
                    if ff and k not in found:
                        found[k] = (gg, bool(dd))
            first.append(time.perf_counter() - t)
            result["lookup"] = found
            return sum(len(zlib.decompress(b)) for bl in got_blocks for b in bl)
        raise ValueError(job["kind"])

    decoded = asyncio.run(run())
    m = io.metrics.snapshot()
    out.send(
        {
            "first": first[0] if first else None,
            "wall": time.perf_counter() - t,
            "gets": m["gets"],
            "mb": m["bytes_in"] / 1e6,
            "decoded_mb": decoded / 1e6,
            "peak_mb": (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - base) / 2**20,
            **result,
        }
    )


def isolated(job: dict) -> dict:
    ctx = mp.get_context("spawn")
    a, b = ctx.Pipe(duplex=False)
    p = ctx.Process(target=reader, args=(job, b))
    p.start()
    res = a.recv()
    p.join()
    return res


# -- the run --------------------------------------------------------------------------------

SCENARIOS = {
    # name: consumer periods (an every-commit consumer, then others)
    "daily": [1, 360, 8640],
    "staggered": [1] + [360] * 60,
    "many": [1] + [8640] * 100,
    "stalled": [1, 360, 8640],  # the daily one stalls from commit 2,000 on
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="1e6")
    ap.add_argument("--commits", type=int, default=12_000)
    ap.add_argument("--scenario", default="daily", choices=sorted(SCENARIOS))
    ap.add_argument("--hot", action="store_true")
    ap.add_argument("--large", type=int, default=0)
    ap.add_argument("--large-every", type=int, default=3000)
    ap.add_argument("--dir", default="/tmp/v4bench")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    n = int(float(args.size))
    root = Path(args.dir) / f"{n}-{args.scenario}{'-hot' if args.hot else ''}"
    shutil.rmtree(root, ignore_errors=True)
    io = cold(root, latency=0, bandwidth=None)
    t0 = time.perf_counter()

    # The base: n keys at generation 1.
    gap = 10**12 // n
    base, per = [], 2_000_000
    for s in range(0, n, per):
        keys = [key_of(i * gap) for i in range(s, min(n, s + per))]
        base.append(K.encode_file(keys, [1] * len(keys), bytes(len(keys))))
    print(f"base of {n:,} in {time.perf_counter() - t0:.0f} s", flush=True)

    trace = Trace(n, args.hot)
    tiling = Tiling(n, base)
    rng = random.Random(3)
    periods = SCENARIOS[args.scenario]
    phases = [0] + [rng.randrange(p) for p in periods[1:]]
    if args.scenario == "staggered":
        phases = [0] + [i * 6 for i in range(60)]
    if args.scenario == "many":
        phases = [0] + [i * 8640 // 100 for i in range(100)]
    positions = [0] * len(periods)
    head = args.commits - 1
    readers = {}  # behind -> position
    deltas: list[bytes] = []
    t1 = time.perf_counter()
    for c in range(args.commits):
        size = args.large if args.large and c % args.large_every == args.large_every // 2 else PER
        data = trace.commit(c, size)
        deltas.append(data)
        tiling.add(c, data, size)
        for i, p in enumerate(periods):
            if args.scenario == "stalled" and i == 2 and c >= 2000:
                continue
            if (c - phases[i]) % p == 0:
                positions[i] = c + 1
        for b in BEHIND:
            if c == head - b:
                readers[b] = c + 1  # born at the head + 1
        tiling.upkeep(set(positions) | set(readers.values()))
        if c % 2000 == 1999:
            print(
                f"  {c + 1:,} commits, {time.perf_counter() - t1:.0f} s: {len(tiling.sim.spans)} spans, "
                f"compaction {tiling.written_entries / tiling.delta_entries:.1f}x entries",
                flush=True,
            )
    build_s = time.perf_counter() - t1

    # Write the files for the readers.
    sizes, spans_named = {}, []

    async def put_all():
        for sp in reversed(tiling.sim.spans):  # newest first
            names = []
            for j, d in enumerate(tiling.files[id(sp)]):
                name = f"s{sp.a}-{sp.b}.{j:04d}.kx"
                await io.write(name, d)
                sizes[name] = len(d)
                footer = K.parse_footer(d[-K.FOOTER_SIZE :])
                names.append(
                    (name, len(d), len(d) - footer["index_offset"], len(d) - footer["filters_offset"])
                )
            spans_named.append((sp.a, sp.b, names))

    asyncio.run(put_all())
    report = {
        "n": n,
        "scenario": args.scenario
        + (" hot" if args.hot else "")
        + (f" +{args.large:,}-key commits" if args.large else ""),
        "commits": args.commits,
        "build_s": build_s,
        "spans_end": len(tiling.sim.spans),
        "spans_max": tiling.max_spans,
        "files_max": tiling.max_files,
        "merges": tiling.merges,
        "compaction_entries_x": tiling.written_entries / tiling.delta_entries,
        "total_entries_x": (tiling.written_entries + tiling.delta_entries) / tiling.delta_entries,
        "compaction_bytes_x": tiling.written_bytes / tiling.delta_bytes,
        "total_bytes_x": (tiling.written_bytes + tiling.delta_bytes) / tiling.delta_bytes,
        "stored_mb": sum(sizes.values()) / 1e6,
        "reads": [],
    }
    print(json.dumps({k: v for k, v in report.items() if k != "reads"}), flush=True)

    for b, p in sorted(readers.items()):
        mine = [(a, z, names) for a, z, names in spans_named if z >= p]
        # Truth: the per-commit fold over the deltas of [p, head].
        rs = [blocks_of(deltas[c]) for c in range(head, p - 1, -1)]
        _, tk, tc = _native.presence([r[0] for r in rs], [r[1] for r in rs], True)
        truth = dict(zip(tk, tc, strict=True))
        job = {
            "root": str(root),
            "sizes": list(sizes.items()),
            "kind": "changes",
            "spans": [names for _, _, names in mine],
            "g_p": gen(p),
            "g_n1": HEAD,
        }
        r = isolated(job)
        keys_got, sum_got = r.pop("digest")
        want = sum(zlib.crc32(k + bytes([c])) for k, c in truth.items()) % 2**64
        bad = 0 if (keys_got, sum_got) == (len(truth), want) else -1  # -1: the checksums differ
        row = {
            "behind": b,
            "layout": "v4 spans",
            "spans": len(mine),
            "keys": len(truth),
            "mismatches": bad,
            **r,
        }
        report["reads"].append(row)
        print(json.dumps(row), flush=True)
        # The control: the same deltas packed in one object, merged at once.
        packed = b"".join(deltas[c] for c in range(p, head + 1))
        name = f"packed-{b}.bin"

        async def put_packed(name=name, packed=packed):
            await io.write(name, packed)

        asyncio.run(put_packed())
        sizes[name] = len(packed)
        r = isolated(
            {
                "root": str(root),
                "sizes": list(sizes.items()),
                "kind": "packed",
                "packed": name,
                "delta_sizes": [len(deltas[c]) for c in range(p, head + 1)],
            }
        )
        counts = r.pop("counts")
        bad = 0 if counts == [sum(1 for c in truth.values() if c == i) for i in range(4)] else -1
        row = {
            "behind": b,
            "layout": "packed deltas",
            "spans": head - p + 1,
            "keys": len(truth),
            "mismatches": bad,
            **r,
        }
        report["reads"].append(row)
        print(json.dumps(row), flush=True)

    # Lookups at the head: 1K existing keys, from the whole key space (and the hot set when hot).
    look_rng = random.Random(7)
    ids = sorted({look_rng.randrange(n) for _ in range(1000)})
    keys = [trace.key(i) for i in ids]
    all_spans = [names for _, _, names in spans_named]
    r = isolated(
        {"root": str(root), "sizes": list(sizes.items()), "kind": "lookup", "spans": all_spans, "keys": keys}
    )
    found = r.pop("lookup")
    bad = 0
    for i, k in zip(ids, keys, strict=True):
        want = trace.live_at_head(i)
        g, d = found.get(k, (None, True))
        bad += (want is None) != d or (want is not None and g != want)
    row = {
        "behind": None,
        "layout": "v4 lookup 1K at head",
        "spans": len(all_spans),
        "keys": len(keys),
        "mismatches": bad,
        **r,
    }
    report["reads"].append(row)
    print(json.dumps(row), flush=True)
    # A 100K-key page mid-index at the head.
    mid = trace.key(n // 2)
    r = isolated(
        {"root": str(root), "sizes": list(sizes.items()), "kind": "scan", "spans": all_spans, "after": mid}
    )
    page = r.pop("scan")
    bad = 0
    for k, g in list(page.items())[:2000]:
        i = int(k[5:18]) // gap if not k.endswith(b"n") else None
        if i is not None and i * gap == int(k[5:18]):
            bad += trace.live_at_head(i) != g
    row = {
        "behind": None,
        "layout": "v4 scan page at head",
        "spans": len(all_spans),
        "keys": len(page),
        "mismatches": bad,
        **r,
    }
    report["reads"].append(row)
    print(json.dumps(row), flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps(report, default=str) + "\n")
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
