"""Appends and lookups on matched layouts (docs/key-index-design.md):
today's leveled index and the span tiling, built from the same trace by
their real compactions, measured cold and warm.

    RAYON_NUM_THREADS=3 uv run python bench/keys/layouts.py --sizes 1e6,1e8 --commits 20000

An index of n keys (`key_of(i * gap)`, generation 1), then `--commits`
commits of 1K keys: 90% updates, 5% removes, 5% adds (half re-adds), every
entry naming its predecessor exactly. Two layouts take the same deltas:

- `leveled`: each delta joins level 0 and `KeyIndex.plan_compaction` /
  `compact` run after every commit, as upkeep does today;
- `spans`: each delta is a span; the newest window of 4 adjacent spans
  whose largest holds at most the other three combined merges
  (`_native.merge_ranges`); once the spans after the base hold a quarter of
  it, they merge into it (`KeyIndex.compact` into the bottom level: drops
  tombstones and predecessors). One consumer reads every commit, so no
  endpoint lies inside.

Measured at the end, on each layout: an exact lookup of 1K keys drawn from
the whole key space, an exact append of 1K updates (the lookup plus the
delta's PUT), and one 100K-key page mid-index, cold (30 ms per request,
80 MB/s per connection, 64 in parallel) and warm (the engine cache filled,
`EngineCache`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import resource
import shutil
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from obstore.store import LocalStore
from solera import _native
from solera import keys as K
from solera.keys.cache import EngineCache
from solera.keys.index import DeltaFiles, FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from bench import key_of  # noqa: E402

PER = 1000


def cold(root: Path, latency=0.03, bandwidth=80e6) -> ObjectIO:
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


def entries_of(files: list[bytes]) -> int:
    return sum(K.parse_footer(d[-K.FOOTER_SIZE :])["entries"] for d in files)


def blocks_in(data: bytes) -> tuple[list[bytes], int]:
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    return [data[off : off + size] for _, off, size, _, _ in tail["blocks"]], tail["codec"]


class Trace:
    """Commits over ids 0..n-1 (key `key_of(id * gap)`), exact predecessors."""

    def __init__(self, n: int, seed: int = 11):
        self.n, self.gap = n, 10**12 // n
        self.rng = random.Random(seed)
        self.state: dict[int, int | None] = {}  # changed ids: generation, None once removed (else live at 1)
        self.removed: list[int] = []
        self.fresh = n  # brand-new ids beyond n: keys `key_of(id * gap) + b"n"`

    def key(self, i: int) -> bytes:
        return key_of(i * self.gap) if i < self.n else key_of((i - self.n) * 7919 % 10**12) + b"n"

    def commit(self, g: int) -> bytes:
        rng, chosen = self.rng, {}
        while len(chosen) < PER:
            x = rng.random()
            if x < 0.95:
                i = rng.randrange(self.n)
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


async def build_base(io: ObjectIO, prefix: str, n: int, opts: Options, stem: str) -> list[FileInfo]:
    gap = 10**12 // n
    per = max(1000, (2 * opts.max_file_bytes) // 38)
    files = []
    for s in range(0, n, per):
        keys = [key_of(i * gap) for i in range(s, min(n, s + per))]
        data = K.encode_file(keys, [1] * len(keys), bytes(len(keys)), level=opts.level)
        name = f"{stem}-{s // per:05d}"
        await io.write(f"{prefix}{name}.kx", data)
        files.append(FileInfo.describe(name, 1, data))
    return files


class Leveled:
    def __init__(self, io: ObjectIO, prefix: str, base: list[FileInfo], opts: Options):
        self.io, self.prefix, self.opts = io, prefix, opts
        self.state = IndexState(files=tuple(base), prefix=prefix)
        self.written = 0

    async def settle(self) -> None:
        while (plan := KeyIndex(self.io, self.prefix, self.state, self.opts).plan_compaction()) is not None:
            added, removed, _ = await KeyIndex(self.io, self.prefix, self.state, self.opts).compact(plan)
            self.written += sum(f.entries for f in added if f.name not in {r for r in removed})
            self.state = self.state.compacted(added, removed)

    async def add(self, c: int, data: bytes) -> None:
        name = f"{c:012d}-l"
        await self.io.write(f"{self.prefix}{name}.kx", data)
        f = FileInfo.describe(name, 0, data)
        self.state = self.state.committed(c, DeltaFiles([f], 0, 0, True), keep_log=False)
        await self.settle()


class Spans:
    def __init__(self, io: ObjectIO, prefix: str, base: list[FileInfo], opts: Options):
        self.io, self.prefix, self.opts = io, prefix, opts
        self.base = base
        self.spans: list[list[FileInfo]] = []  # after the base, oldest first
        self.written = 0
        self.seq = 0

    def size(self, files: list[FileInfo]) -> int:
        return sum(f.entries for f in files)

    async def put(self, files: list[bytes]) -> list[FileInfo]:
        out = []
        for d in files:
            self.seq += 1
            name = f"s{self.seq:08d}"
            await self.io.write(f"{self.prefix}{name}.kx", d)
            out.append(FileInfo.describe(name, 0, d))
        return out

    async def read(self, files: list[FileInfo]) -> tuple[list[bytes], int]:
        blocks, codec = [], 1
        for f in files:
            b, codec = blocks_in(await self.io.read_whole(f"{self.prefix}{f.name}.kx", f.size))
            blocks += b
        return blocks, codec

    def state(self) -> IndexState:
        levels = list(reversed(self.spans)) + [self.base]
        files = [replace(f, level=i + 1) for i, run in enumerate(levels) for f in run]
        return IndexState(files=tuple(files), prefix=self.prefix)

    async def add(self, c: int, data: bytes) -> None:
        self.spans += [await self.put([data])]
        while True:
            sizes = [self.size(s) for s in self.spans]
            if sum(sizes) * 4 >= self.size(self.base):
                # Into the base: today's compaction into the bottom level (tombstones and predecessors go).
                st = self.state()
                plan = (list(st.files), st.depth)
                idx = KeyIndex(self.io, self.prefix, replace(st, files=st.files), self.opts)
                runs_state = st
                added, removed, _ = await idx.compact(
                    (sorted(runs_state.files, key=lambda f: f.level), st.depth)
                )
                del plan
                self.written += sum(f.entries for f in added)
                self.base = [replace(f, level=1) for f in added]
                self.spans = []
                return
            for j in range(len(sizes) - 4, -1, -1):
                w = sizes[j : j + 4]
                if max(w) <= sum(w) - max(w):
                    group = self.spans[j : j + 4]
                    runs = [await self.read(s) for s in reversed(group)]
                    out = await asyncio.to_thread(
                        _native.merge_ranges, [r[0] for r in runs], [r[1] for r in runs]
                    )
                    self.written += entries_of(out)
                    self.spans[j : j + 4] = [await self.put(out)]
                    break
            else:
                return


async def measure(io: ObjectIO, fn) -> dict:
    io.metrics.reset()
    t = time.perf_counter()
    out = await fn()
    m = io.metrics.snapshot()
    return {
        "wall": time.perf_counter() - t,
        "gets": m["gets"],
        "puts": m["puts"],
        "mb": m["bytes_in"] / 1e6,
        "out": out,
    }


async def size_run(n: int, commits: int, root: Path, opts: Options) -> list[dict]:
    shutil.rmtree(root, ignore_errors=True)
    io = cold(root, latency=0, bandwidth=None)
    t = time.perf_counter()
    lev = Leveled(io, "lev/", await build_base(io, "lev/", n, opts, "b"), opts)
    await lev.settle()
    sp = Spans(io, "spn/", await build_base(io, "spn/", n, opts, "b"), opts)
    print(f"  {n:,}: bases built in {time.perf_counter() - t:.0f} s", flush=True)
    trace = Trace(n)
    t = time.perf_counter()
    committed = 0
    for c in range(commits):
        data = trace.commit(1000 + c)
        committed += PER
        await lev.add(c, data)
        await sp.add(c, data)
        if c % 2000 == 1999:
            print(
                f"  {c + 1:,} commits, {time.perf_counter() - t:.0f} s; written: leveled "
                f"{lev.written / committed:.1f}x, spans {sp.written / committed:.1f}x; "
                f"rss {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30:.1f} GB",
                flush=True,
            )
    layouts = {"leveled": lev.state, "spans": sp.state()}
    written = {"leveled": lev.written / committed, "spans": sp.written / committed}
    live = (
        n
        + sum(1 for i, g in trace.state.items() if i >= n and g is not None)
        - sum(1 for i, g in trace.state.items() if i < n and g is None)
    )
    rng = random.Random(7)
    look = sorted({trace.key(rng.randrange(n)) for _ in range(1000)})
    upd = sorted({trace.key(i) for i in rng.sample(range(n), 1000) if trace.state.get(i, 1) is not None})
    mid = trace.key(n // 2)
    rows = []
    for name, st in layouts.items():
        info = {
            "n": n,
            "layout": name,
            "written": written[name],
            "runs": len(st.newest_first()),
            "files": len(st.files),
            "entries_per_key": sum(f.entries for f in st.files) / live,
            "size_mb": sum(f.size for f in st.files) / 1e6,
            "tails_mb": sum(f.tail for f in st.files) / 1e6,
        }
        tmp = tempfile.mkdtemp(prefix="engine", dir=str(root))
        cache = EngineCache(os.path.join(tmp, "engine"), disk=200 * 2**30)
        assert cache.admit(st)
        await cache.fill(cold(root, latency=0, bandwidth=None), st)
        for mode in ("cold", "warm"):
            ops = {
                "lookup 1K": lambda idx: idx.lookup(look),
                "append 1K updates": None
                if mode == "warm"
                else lambda idx: idx.resolve(
                    K.SortedEntries.of(upd),
                    commit_number=10**9,
                    attempt="m",
                    generation=99,
                    exact=True,
                ),
                "page 100K, mid-index": lambda idx: idx.page(mid, 100_000),
            }
            for op, fn in ops.items():
                if fn is None:  # the engine's warm resolve is its own path (resolved-commits.md)
                    continue
                c = cold(root)
                if mode == "warm":
                    opened = cache.open(st)
                    c.local = opened.handles
                idx = KeyIndex(c, None, st, opts)
                r = await measure(c, lambda idx=idx, fn=fn: fn(idx))
                r.pop("out")
                if mode == "warm":
                    opened.close()
                row = {**info, "mode": mode, "op": op, **r}
                rows.append(row)
                print(json.dumps(row), flush=True)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--commits", type=int, default=20_000)
    ap.add_argument("--dir", default="/tmp/layouts")
    args = ap.parse_args()
    opts = Options()
    rows = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        rows += asyncio.run(size_run(n, args.commits, Path(args.dir) / str(n), opts))
    print(
        "\n| Keys | Layout | Written per entry | Runs | Files | Entries per live key | Size | Tails | Mode | Op | "
        "GETs | PUTs | MB read | Wall |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['n']:,} | {r['layout']} | {r['written']:.1f} | {r['runs']} | {r['files']} | "
            f"{r['entries_per_key']:.2f} | {r['size_mb']:,.0f} MB | {r['tails_mb']:,.0f} MB | {r['mode']} | {r['op']} | "
            f"{r['gets']:,} | {r['puts']} | {r['mb']:,.1f} | {r['wall']:.2f} s |"
        )


if __name__ == "__main__":
    main()
