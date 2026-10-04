"""Spans and the two views on one harness: spanbench (branch bench/spanbench,
17026ec) extended with scenarios, a commit horizon and the two views.

    uv run python bench/keys/views/viewbench.py --index views --size 1e6 --scenario base

The trace is spanbench's, call for call (same seed, same numpy draws): a
base of n keys, then commits of 1K keys (90% updates, 5% removes, 5% adds),
each resolved exactly against the index under test; consumers reading every
1, 360 and 8,640 commits; readers kept 1, 100, 360, 8,640 and 10,000 behind
the head (spanbench has no 8,640; added). Scenarios add:

- `daily100`: 100 more consumers reading every 8,640 commits, spread over
  the day;
- `stall`: a full pass that starts at commit 2,000 and never completes: its
  snapshot at 2,000 is read, and it lands at 2,001 (spans: endpoint 2,001;
  views: K pinned at 2,000, a reader at 2,001);
- `churn`: half of each commit is temporary keys: 250 added, and the 250
  added 100 commits earlier removed; the other 500 follow the usual mix.

`--index layers`: stamped layers (W57, `bench/keys/fp/layers.py`,
docs/key-index-from-first-principles.md): minimal deltas, written through
the exact path (a lookup at the head, then the kinds); upkeep with the cut at
the oldest live P (and `--window`, if set, as the window's edge); reads
through `layers.Reader`, cold, every layer index fetched. Its stalled pass
has no pinned snapshot (the design reads only at heads): `pinned` is skipped.

`--horizon X`: a reader more than X commits behind the head is dropped (its
next read is a full pass), a stalled pass cancel-restarted; on both indexes.
`--retention floor|cover|window` (views): T keeps every level from the
oldest reader; or the covers of every reader interval; or a window of
`--window` commits, laggards reading a snapshot of the key view.

Reads are spanbench's, each cold in a fresh process (30 ms per request,
80 MB/s per connection, 64 in parallel), checked key by key against the
fold; plus, for `stall`, the pinned page and its catch-up; a dropped reader
reads a full pass (every page of the key view at the head).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from obstore.store import LocalStore
from solera.keys import Rows, SortedEntries
from solera.keys.index import DeltaFiles, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from twoviews import PackIO, TwoViews  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent.parent / "fp"))
import layers as LY  # noqa: E402

PER = 1000
PAGE = 100_000
BEHIND = (1, 100, 360, 8640, 10_000)
PERIODS = (1, 360, 8640)
STALL = int(__import__("os").environ.get("STALL", "2000"))
PREFIX = "keys/bench/_/"
MUL, MOD = 2_654_435_761, 10**13
INV = pow(MUL, -1, MOD)
CODECS = {"zlib": 1, "zstd": 2}


def key(i: int) -> bytes:
    return b"cust-%013d" % (i * MUL % MOD)


def key_id(k: bytes) -> int:
    return int(k[5:]) * INV % MOD


def values(ids: np.ndarray) -> np.ndarray:
    return ids.astype(np.uint64) * np.uint64(MUL) % np.uint64(MOD)


def peak_bytes() -> int:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r if sys.platform == "darwin" else r * 1024


def options(root: Path) -> Options:
    o = json.loads((root / "options.json").read_text())
    return Options(block_size=o["block_size"], codec=CODECS[o["codec"]], level=1)


# -- the indexes under test ------------------------------------------------------------------


class Spans:
    """The span index as built, upkeep run to quiescence after every commit."""

    def __init__(self, root: Path, opts: Options):
        self.io = ObjectIO(LocalStore(str(root / "store"), mkdir=True))
        self.o, self.state = opts, IndexState(prefix=PREFIX)
        self.merge_entries = self.merge_bytes = self.merge_puts = self.merges = 0

    def index(self) -> KeyIndex:
        return KeyIndex(self.io, None, self.state, self.o)

    def commit(self, c: int, files: DeltaFiles) -> None:
        self.state = self.state.committed(c, files)

    async def upkeep(self, endpoints: set[int], intervals) -> None:
        while True:
            idx = self.index()
            plan = idx.plan_merge(endpoints)
            if plan is None:
                return
            out = await idx.merge(plan, endpoints)
            if out is None:
                return
            self.state = self.state.merged(out.inputs, out.span)
            self.merge_bytes += out.span.size
            self.merge_entries += out.written
            self.merge_puts += len(out.span.files)
            self.merges += 1

    def stored(self) -> int:
        return sum(f.size for f in self.state.files)

    def save(self, root: Path) -> None:
        (root / "state.json").write_text(json.dumps(self.state.to_json()))

    def built(self) -> dict:
        return {
            "spans": len(self.state.spans),
            "entries": sum(f.entries for f in self.state.files),
            "merge_entries": self.merge_entries,
            "merge_bytes": self.merge_bytes,
            "merge_puts": self.merge_puts,
            "merges": self.merges,
        }


class Views:
    def __init__(self, root: Path, opts: Options, retention: str, window: int):
        self.io = PackIO(LocalStore(str(root / "store"), mkdir=True))
        self.v = TwoViews(self.io, PREFIX, opts, retention=retention)
        self.v.window = window
        self.o = opts

    def index(self) -> KeyIndex:
        return self.v.k()

    def commit(self, c: int, files: DeltaFiles) -> None:
        self.v.commit(c, files.files)

    async def upkeep(self, endpoints: set[int], intervals) -> None:
        self.v.intervals = list(intervals)
        await self.v.upkeep()

    def stored(self) -> int:
        return sum(self.v.stored().values())

    def save(self, root: Path) -> None:
        (root / "views.json").write_text(json.dumps(self.v.to_json()))

    def built(self) -> dict:
        w = self.v.written
        return {
            "runs_k": len(self.v.k_runs()),
            "stored_by_kind": self.v.stored(),
            "written": {k: x.__dict__ for k, x in w.items()},
            "merge_entries": sum(x.entries for k, x in w.items() if k != "pack"),
            "pack_entries": w["pack"].entries,
            "merge_bytes": sum(x.bytes for x in w.values()),
            "merge_puts": sum(x.puts for x in w.values()),
        }


class LayersIx:
    """Stamped layers (W57), upkeep run to quiescence after every commit."""

    def __init__(self, root: Path, window: int):
        self.io = ObjectIO(LocalStore(str(root / "store"), mkdir=True))
        self.lx = LY.Layers(self.io, PREFIX, window=window)

    async def commit_keys(self, c: int, g: int, ups: list[bytes], rms: list[bytes]) -> tuple[int, int, int]:
        before = LY.Written(**self.lx.written["delta"].__dict__)
        await self.lx.commit(c, g, ups, rms)
        w = self.lx.written["delta"]
        return w.bytes - before.bytes, w.entries - before.entries, w.puts - before.puts

    async def upkeep(self, endpoints: set[int], intervals) -> None:
        # endpoints are next positions: the reader observed at next - 1
        await self.lx.upkeep(min(endpoints) - 1 if endpoints else None)

    def stored(self) -> int:
        return self.lx.stored()

    def save(self, root: Path) -> None:
        (root / "layers.json").write_text(json.dumps(self.lx.s.to_json()))

    def built(self) -> dict:
        w = self.lx.written
        return {
            "layers": len(self.lx.s.layers),
            "cut": self.lx.s.cut,
            "entries": sum(x.entries for x in self.lx.s.layers),
            "written": {k: x.__dict__ for k, x in w.items()},
            "merge_entries": w["tier"].entries + w["base"].entries,
            "merge_bytes": w["tier"].bytes + w["base"].bytes,
            "merge_puts": w["tier"].puts + w["base"].puts,
            "merges": w["tier"].merges + w["base"].merges,
        }


def sorted_key_chunks(n: int, size: int = 10_000_000):
    """The base's keys in key order: `cust-` and 13 zero-padded digits sort as
    their numbers do."""
    vals = np.sort(values(np.arange(n)))
    for lo in range(0, n, size):
        digits = pc.utf8_lpad(pc.cast(pa.array(vals[lo : lo + size]), pa.string()), 13, "0")
        yield pc.binary_join_element_wise("cust-", digits, "").cast(pa.large_binary())


class LayersRead:
    """The harness's read API over a `layers.Reader` (cold: every index and
    block through the store)."""

    def __init__(self, io, state):
        self.r, self.head = LY.Reader(io, state), state.head

    async def page(self, after, limit):
        keys, _, _, stamps, _, nxt = await self.r.page(None, after, limit)
        return keys, stamps, None, nxt

    async def lookup(self, keys):
        found = await self.r.lookup(keys)
        return {k: (f[1],) for k, f in zip(keys, found, strict=True) if f is not None and f[0]}

    async def write(self, ups):
        found = await self.r.lookup(ups)
        added = sum(1 for f in found if f is None or not f[0])
        return type("Resolved", (), {"added": added, "removed": 0})()


# -- the build -----------------------------------------------------------------------------------


async def build(root: Path, a) -> dict:
    rng = np.random.default_rng(a.seed)
    opts = options(root)
    if a.index == "layers":
        ix = LayersIx(root, a.window if a.window_set else 0)
    else:
        ix = Spans(root, opts) if a.index == "spans" else Views(root, opts, a.retention, a.window)
    n, commits = a.n, a.commits
    capacity = n + commits * (PER // 20) + 1 + (commits * PER // 4 if a.scenario == "churn" else 0)
    gen = np.zeros(capacity, dtype=np.uint32)
    t0 = time.perf_counter()
    if a.index == "layers":
        await ix.lx.load_base(sorted_key_chunks(n), 1)
    else:
        chunks = []
        for lo in range(0, n, 10_000_000):
            ids = np.arange(lo, min(n, lo + 10_000_000))
            digits = pc.utf8_lpad(pc.cast(pa.array(values(ids)), pa.string()), 13, "0")
            chunks.append(pc.binary_join_element_wise("cust-", digits, ""))
        table = pa.table({"k": pa.chunked_array(chunks)})
        base_opts = opts if a.index == "spans" else ix.v.base_o
        files, _ = await KeyIndex(ix.io, None, IndexState(prefix=PREFIX), base_opts).replace(
            Rows.arrow(table, "k"), 0, "base", generation=1
        )
        del table
        ix.commit(0, files)
    gen[:n] = 1
    present = np.arange(n, dtype=np.int64)
    live_n, nxt_id = n, n
    position = dict.fromkeys(PERIODS, 1)
    daily = {i: 1 for i in range(100)} if a.scenario == "daily100" else {}
    readers: dict[int, int] = {}
    stall: int | None = None  # the stalled pass's landing point
    temp: dict[int, np.ndarray] = {}  # churn: commit -> temporary ids added then
    drops: list[tuple[int, str]] = []
    dropped_readers: set[int] = set()
    delta_bytes = delta_entries = delta_puts = 0
    stored_sum = stored_peak = 0
    print(f"  base: {time.perf_counter() - t0:.0f} s", flush=True)
    for c in range(1, commits + 1):
        for d in BEHIND:
            if commits - c + 1 == d:
                readers[d] = c
                np.save(root / f"before-{c}.npy", gen)
        if a.scenario == "stall" and c == STALL + 1:
            np.save(root / f"before-{c}.npy", gen)
        g = c + 1
        k = a.large if a.large_every and c % a.large_every == 0 else PER
        rm_n = add_n = 0 if k != PER else PER // 20
        churn_add = churn_rm = np.empty(0, np.int64)
        if a.scenario == "churn" and k == PER:
            k, rm_n, add_n = PER // 2, PER // 40, PER // 40
        pick = np.unique(rng.integers(0, max(live_n, 1), size=k - add_n)) if k > add_n else np.empty(0, np.int64)
        rng.shuffle(pick)
        upd, rm = present[pick[rm_n:]], present[pick[:rm_n]]
        add = np.arange(nxt_id, nxt_id + add_n, dtype=np.int64)
        nxt_id += add_n
        if a.scenario == "churn":  # 250 temporary keys in, the 250 of 100 commits ago out
            churn_add = np.arange(nxt_id, nxt_id + PER // 4, dtype=np.int64)
            nxt_id += PER // 4
            temp[c] = churn_add
            churn_rm = temp.pop(c - 100, np.empty(0, np.int64))
        for i in sorted(pick[:rm_n].tolist(), reverse=True):
            present[i] = present[live_n - 1]
            live_n -= 1
        if live_n + add_n > len(present):
            present = np.concatenate([present, np.empty(max(add_n, len(present) // 8), dtype=np.int64)])
        present[live_n : live_n + add_n] = add
        live_n += add_n
        ups = sorted(key(int(i)) for i in np.concatenate([upd, add, churn_add]))
        rms = sorted(key(int(i)) for i in np.concatenate([rm, churn_rm]))
        if a.index == "layers":  # the exact path: the written keys looked up at the head
            db, de, dp = await ix.commit_keys(c, g, ups, rms)
            delta_bytes += db
            delta_entries += de
            delta_puts += dp
        else:
            if a.index == "spans":  # as spanbench
                files, _ = await ix.index().resolve(
                    SortedEntries.of(ups, None, rms), commit_number=c, attempt=f"a{c}", generation=g
                )
            else:  # the exact sparse path: resolve's stream-or-sparse switch counts blocks, and
                # 16 KiB blocks of a filterless base make it stream 100M keys for every commit
                idx = ix.index()
                delta = await idx.delta(SortedEntries.of(ups, None, rms), generation=g)
                files = await idx.write(c, f"a{c}", delta, g)
            delta_bytes += sum(f.size for f in files.files)
            delta_entries += sum(f.entries for f in files.files)
            delta_puts += len(files.files)
            ix.commit(c, files)
        gen[upd] = g
        gen[add] = g
        gen[churn_add] = g
        gen[rm] = 0
        gen[churn_rm] = 0
        for p in PERIODS:
            if c % p == 0:
                position[p] = c + 1
        for i in daily:
            if (c - i * 86) % 8640 == 0:
                daily[i] = c + 1
        if a.scenario == "stall" and c == STALL:
            stall = c + 1
            if a.index == "views":
                ix.v.pin("stall")
        # The horizon: anyone more than X behind is dropped (a full pass next).
        if a.horizon:
            lim = c + 1 - a.horizon
            for p in PERIODS:
                if position[p] < lim:
                    drops.append((c, f"consumer/{p}"))
                    position[p] = c + 1  # its next read is a full pass, then it reads on from there
            for i in daily:
                if daily[i] < lim:
                    drops.append((c, f"daily/{i}"))
                    daily[i] = c + 1
            for d, p in list(readers.items()):
                if p < lim and d not in dropped_readers:
                    drops.append((c, f"reader/{d}"))
                    dropped_readers.add(d)
            if stall is not None and stall < lim:
                drops.append((c, "stall"))
                stall = None
                if a.index == "views":
                    await ix.v.unpin("stall")
        live_readers = {d: p for d, p in readers.items() if d not in dropped_readers}
        starts = set(position.values()) | set(daily.values()) | set(live_readers.values())
        if stall is not None:
            starts.add(stall)
        await ix.upkeep(starts, [(p, None) for p in starts])
        st = ix.stored()
        stored_sum += st
        stored_peak = max(stored_peak, st)
        if c % 1000 == 0:
            print(f"  commit {c}: {st / 1e6:.0f} MB stored, {time.perf_counter() - t0:.0f} s", flush=True)
    ix.save(root)
    np.save(root / "head.npy", gen)
    (root / "readers.json").write_text(
        json.dumps({"readers": readers, "dropped": sorted(dropped_readers), "stall": stall, "drops": drops})
    )
    return {
        "index": a.index,
        "scenario": a.scenario,
        "retention": a.retention if a.index == "views" else a.index,
        "window": a.window,
        "horizon": a.horizon,
        "n": n,
        "commits": commits,
        "live": int(live_n),
        "delta_mb": delta_bytes / 1e6,
        "delta_entries": delta_entries,
        "delta_puts": delta_puts,
        "stored_mean_mb": stored_sum / commits / 1e6,
        "stored_peak_mb": stored_peak / 1e6,
        "stored_end_mb": ix.stored() / 1e6,
        "drops": len(drops),
        "drops_by": sorted({who.split("/")[0] for _, who in drops}),
        "build_s": time.perf_counter() - t0,
        **ix.built(),
    }


# -- the reads ----------------------------------------------------------------------------------


async def read(root: Path, what: str, at: int | None) -> dict:
    built = json.loads((root / "built.json").read_text())
    now = np.load(root / "head.npy")
    opts = options(root)
    views = built["index"] == "views"
    layers = built["index"] == "layers"
    if layers:
        io = ObjectIO(LocalStore(str(root / "store")), latency=0.03, bandwidth=80e6, concurrency=64)
        st = LY.State.from_json(json.loads((root / "layers.json").read_text()))
        idx = LayersRead(io, st)
        head = st.head
    elif views:
        io = PackIO(LocalStore(str(root / "store")), latency=0.03, bandwidth=80e6, concurrency=64)
        v = TwoViews.from_json(io, opts, json.loads((root / "views.json").read_text()))
        head, idx = v.head, v.k()
    else:
        io = ObjectIO(LocalStore(str(root / "store")), latency=0.03, bandwidth=80e6, concurrency=64)
        state = IndexState.from_json(json.loads((root / "state.json").read_text()))
        head, idx = state.head, KeyIndex(io, None, state, opts)
    rng = np.random.default_rng(5)
    base = peak_bytes()
    t = time.perf_counter()
    first = None
    out = {}
    if what in ("changes", "full"):
        then = np.load(root / f"before-{at}.npy")
        n_got = bad = 0
        seen = np.zeros(len(now), dtype=bool)

        async def pages():
            if what == "full":  # a dropped reader: a full pass, every key added
                after = None
                while True:
                    keys, gens, _, nxt = await idx.page(after, PAGE)
                    yield keys, bytes(len(keys)), gens, bytes(len(keys))
                    if nxt is None:
                        return
                    after = nxt
            elif layers:  # a reader at position `at` observed at at - 1
                after = None
                while True:
                    keys, ap, ah, stamps, _, nxt = await idx.r.page(at - 1, after, PAGE)
                    cls = bytes(1 if a and h else 0 if h else 2 for a, h in zip(ap, ah, strict=True))
                    yield keys, cls, stamps, bytes(1 - h for h in ah)
                    if nxt is None:
                        return
                    after = nxt
            elif not views:
                async for page in idx.changes(at, head, limit=PAGE):
                    yield page.keys, page.classes, page.generations, page.deleted
            else:
                after = None
                while True:
                    page = await v.changes_page(at, head, after, PAGE)
                    yield page.keys, page.classes, page.generations, page.deleted
                    if page.cursor is None:
                        return
                    after = page.cursor

        async for keys, cls, gens_, dels in pages():
            first = first or time.perf_counter() - t
            ids = np.fromiter((key_id(k) for k in keys), dtype=np.int64, count=len(keys))
            classes = np.frombuffer(cls, dtype=np.uint8)
            gens = np.array(gens_, dtype=np.uint64)
            deleted = np.frombuffer(dels, dtype=np.uint8).astype(bool)
            if what == "full":
                bad += int(np.count_nonzero(gens != now[ids]))
            else:
                was, is_ = then[ids] > 0, now[ids] > 0
                want = np.where(was, np.where(is_, 1, 2), np.where(is_, 0, 3))
                changed = then[ids] != now[ids]
                bad += int(np.count_nonzero(changed & (classes != want)))
                bad += int(np.count_nonzero(np.where(deleted, now[ids] != 0, gens != now[ids])))
            seen[ids] = True
            n_got += len(ids)
        wall = time.perf_counter() - t
        if what == "full":
            bad += int(np.count_nonzero((now > 0) & ~seen))
        else:
            bad += int(np.count_nonzero((then != now) & ~seen))
        out = {"keys": n_got, "mismatches": bad}
    elif what == "pinned" and layers:  # no snapshot of an old commit, by design
        return {"what": what, "at": at, "unsupported": True, "keys": 0, "mismatches": 0, "first_s": 0, "wall_s": 0, "gets": 0, "mb_in": 0, "peak_mb": 0}
    elif what == "pinned":  # the stalled pass's snapshot: a first page
        then = np.load(root / f"before-{at}.npy")
        if views:
            keys, gens, _, _ = await v.page(None, PAGE, pin="stall")
        else:
            keys, gens, _, _ = await idx.page(None, PAGE, at=at)
        wall = time.perf_counter() - t
        live = np.flatnonzero(then)
        vals = np.sort(values(live))[:PAGE]
        want = [b"cust-%013d" % int(x) for x in vals]
        bad = int(keys != want[: len(keys)]) + sum(1 for k, g in zip(keys, gens, strict=True) if then[key_id(k)] != g)
        out = {"keys": len(keys), "mismatches": bad}
    elif what == "write":
        live = np.flatnonzero(now)
        ups = sorted(key(int(i)) for i in rng.choice(live, PER, replace=False))
        delta = await (idx.write(ups) if layers else idx.delta(SortedEntries.of(ups), generation=2**40))
        wall = time.perf_counter() - t
        out = {"keys": len(ups), "mismatches": int(delta.removed != 0 or delta.added != 0)}
    elif what == "lookups":
        probe = sorted({key(int(i)) for i in rng.integers(0, len(now), size=PER)})
        hits = await idx.lookup(probe)
        wall = time.perf_counter() - t
        bad = sum(1 for k in probe if (hits.get(k) or (0,))[0] != now[key_id(k)])
        out = {"keys": len(probe), "mismatches": bad}
    else:
        live = np.flatnonzero(now)
        vals = np.sort(values(live))
        after = b"cust-%013d" % int(vals[len(vals) // 2])
        t = time.perf_counter()
        keys, gens, _, nxt = await idx.page(after, PAGE)
        wall = time.perf_counter() - t
        want = [b"cust-%013d" % int(x) for x in vals[len(vals) // 2 + 1 : len(vals) // 2 + 1 + PAGE]]
        bad = int(keys != want[: len(keys)] or (len(keys) < len(want) and nxt is None))
        bad += sum(1 for k, g in zip(keys, gens, strict=True) if now[key_id(k)] != g)
        out = {"keys": len(keys), "mismatches": bad}
    m = io.metrics.snapshot()
    return {
        **out,
        "what": what,
        "at": at,
        "first_s": first if first is not None else wall,
        "wall_s": wall,
        "gets": m["gets"],
        "mb_in": m["bytes_in"] / 1e6,
        "peak_mb": max(0, peak_bytes() - base) / 1e6,
    }


def isolated(root: Path, what: str, at: int | None) -> dict:
    out = subprocess.run(
        [sys.executable, __file__, "--read", str(root), what, str(at if at is not None else -1)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", choices=("spans", "views", "layers"), default="views")
    ap.add_argument("--retention", choices=("floor", "cover", "window"), default="window")
    ap.add_argument("--window", type=int, default=None, help="views: 8,640 if unset; layers: the cut is at most this far back (none if unset)")
    ap.add_argument("--horizon", type=int, default=0)
    ap.add_argument("--scenario", choices=("base", "daily100", "stall", "churn"), default="base")
    ap.add_argument("--size", default="1e6")
    ap.add_argument("--commits", type=int, default=12_000)
    ap.add_argument("--large", type=int, default=0)
    ap.add_argument("--large-every", type=int, default=0)
    ap.add_argument("--codec", choices=sorted(CODECS), default="zlib")
    ap.add_argument("--block-size", type=int, default=64 * 1024)
    ap.add_argument("--dir", default="/tmp/viewbench")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--read", nargs=3)
    ap.add_argument("--no-reads", action="store_true")
    ap.add_argument("--reads")
    a = ap.parse_args()
    a.window_set = a.window is not None
    if a.window is None:
        a.window = 8640
    if a.read:
        root, what, at = a.read
        print(json.dumps(asyncio.run(read(Path(root), what, None if int(at) < 0 else int(at)))))
        return
    if a.reads:
        root = Path(a.reads)
        built = json.loads((root / "built.json").read_text())
    else:
        a.n = int(float(a.size))
        name = f"{a.index}-{a.scenario}-{a.n}-{a.commits}-{a.large}x{a.large_every}-{a.codec}-{a.block_size >> 10}k-h{a.horizon}"
        if a.index == "views":
            name += f"-{a.retention}{a.window if a.retention == 'window' else ''}"
        if a.index == "layers" and a.window_set:
            name += f"-w{a.window}"
        root = Path(a.dir) / name
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        (root / "options.json").write_text(json.dumps({"codec": a.codec, "block_size": a.block_size}))
        built = asyncio.run(build(root, a))
        (root / "built.json").write_text(json.dumps(built))
        print(json.dumps(built), flush=True)
        if a.no_reads:
            return
    rd = json.loads((root / "readers.json").read_text())
    rows = []
    for d, p in sorted(rd["readers"].items(), key=lambda kv: int(kv[0])):
        rows.append(isolated(root, "full" if int(d) in rd["dropped"] else "changes", p) | {"behind": int(d)})
    if built["scenario"] == "stall" and rd["stall"] is not None:
        rows.append(isolated(root, "pinned", rd["stall"]) | {"behind": "stalled pass: its snapshot"})
        rows.append(isolated(root, "changes", rd["stall"]) | {"behind": "stalled pass: catch-up"})
    rows += [isolated(root, w, None) | {"behind": w} for w in ("write", "lookups", "page")]
    (root / "reads.json").write_text(json.dumps(rows))
    print(json.dumps({"built": built, "reads": rows}))


if __name__ == "__main__":
    main()
