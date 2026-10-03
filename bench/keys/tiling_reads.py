"""Cold reads over a run tiling against today's leveled layout
(docs/key-index-design.md), real files, today's reader.

    uv run python bench/keys/tiling_reads.py --sizes 1e6,1e8 --dir /tmp/tiling

Builds an index of n keys once (bench.py's `build`), then lays out:

- `leveled`: today's steady state, as presence.py measured it: levels filled
  to their targets by `fill_upper`, plus 7 level-0 deltas of 1K keys;
- `tiling R`: the same base as one run, then tiered runs newest first, 4-way:
  three runs each of 1K, 4K, 16K ... entries, as tiling.py's 4-way policy
  leaves a segment (22 runs: one segment; 43: two, as with a daily and an
  hourly consumer behind). Each run is a random subset of existing keys,
  entries naming their predecessor.

A run maps to a level of `IndexState` (each level is one sorted run, newest
first), so `KeyIndex.lookup` (exact: every key a filter matches gets its
block) and `KeyIndex.page` read it unchanged. Measured cold, 30 ms per
request, 80 MB/s per connection, 64 in parallel (bench.py's model): 1K
random existing keys looked up exactly, and one 100K-key page mid-index.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from dataclasses import replace
from pathlib import Path

from obstore.store import LocalStore
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from bench import build, fill_upper, key_of  # noqa: E402


def cold(root: Path, latency=0.03, bandwidth=80e6) -> ObjectIO:
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


async def measure(io: ObjectIO, fn) -> dict:
    io.metrics.reset()
    t = time.perf_counter()
    out = await fn()
    m = io.metrics.snapshot()
    return {"wall": time.perf_counter() - t, "gets": m["gets"], "read_mb": m["bytes_in"] / 1e6, "out": out}


MAX_RUN = 2_000_000  # entries per file of a tail run: about the 64 MB split at ~30 B raw


async def tail_runs(io, prefix: str, ids: list[int], tiers: int, segments: int) -> list[list[FileInfo]]:
    """Tiered runs, newest first: per segment, three runs per tier of 1K · 4^t entries."""

    rng = random.Random(9)
    sizes = [1000 * 4**t for _ in range(segments) for t in range(tiers) for _ in range(3)]
    runs = []
    for r, m in enumerate(sorted(sizes)):  # smallest (newest) first
        chosen = sorted(rng.sample(ids, min(m, len(ids))))
        files = []
        for s in range(0, len(chosen), MAX_RUN):
            part = chosen[s : s + MAX_RUN]
            keys = [key_of(i) for i in part]
            g = 10_000 - r
            data = K.encode_file(keys, [g] * len(keys), bytes(len(keys)), predecessors=[1] * len(keys))
            name = f"t{segments}-{r:03d}-{s // MAX_RUN:03d}"
            await io.write(f"{prefix}{name}.kx", data)
            files.append(FileInfo.describe(name, 0, data))
        runs.append(files)
    return runs


def layout(base: tuple[FileInfo, ...], runs: list[list[FileInfo]], prefix: str, count: int) -> IndexState:
    files = [replace(f, level=i + 1) for i, run in enumerate(runs) for f in run]
    files += [replace(f, level=len(runs) + 1) for f in base]
    return IndexState(count=count, files=tuple(files), prefix=prefix)


async def size_run(n: int, root: Path, opts: Options) -> list[dict]:
    io = cold(root, latency=0, bandwidth=None)
    prefix = "idx/"
    every = max(1, n // 10_000_000)
    state, sample, _, build_s = await build(io, prefix, n, opts, sample_every=every, big_every=n)
    print(f"  built {n:,} in {build_s:.0f} s", flush=True)
    base = state.files
    ids = [i for i, _ in sample]
    current = {i: (1, v) for i, v in sample}
    per_entry = sum(f.size for f in base) / n

    # Today's steady state.
    upper = await fill_upper(io, prefix, n, opts, state.depth, per_entry, current)
    leveled = replace(state, files=state.files + tuple(upper), prefix=prefix)
    rng = random.Random(5)
    for c in range(7):
        keys = sorted(key_of(i) for i in rng.sample(ids, 1000))
        data = K.encode_file(keys, [50 + c] * len(keys), bytes(len(keys)), predecessors=[1] * len(keys))
        name = f"{c:012d}-steady"
        await io.write(f"{prefix}{name}.kx", data)
        leveled = replace(leveled, files=leveled.files + (FileInfo.describe(name, 0, data),))

    tiers = 7 if n >= 10**8 else 4  # 4-way tiers up to 4M (100M) or 64K (1M): a quarter of the base
    layouts = {"leveled": leveled}
    for segments in (1, 2):
        runs = await tail_runs(io, prefix, ids, tiers, segments)
        layouts[f"tiling {len(runs) + 1}"] = layout(base, runs, prefix, n)

    rows = []
    look = sorted(key_of(i) for i in random.Random(7).sample(ids, 1000))
    for name, st in layouts.items():
        entries = sum(f.entries for f in st.files)
        runs = len(st.newest_first())
        files = len(st.files)
        size = sum(f.size for f in st.files)
        tails = sum(f.tail for f in st.files)
        for what, fn in (
            ("lookup 1K, exact", lambda idx: idx.lookup(look)),
            ("page 100K, mid-index", lambda idx: idx.page(key_of(ids[len(ids) // 2]), 100_000)),
        ):
            c = cold(root)
            idx = KeyIndex(c, prefix, st, opts)
            r = await measure(c, lambda idx=idx, fn=fn: fn(idx))
            out = r.pop("out")
            got = len(out) if what.startswith("lookup") else len(out[0])
            row = {
                "n": n,
                "layout": name,
                "runs": runs,
                "files": files,
                "entries_per_key": entries / n,
                "size_mb": size / 1e6,
                "tails_mb": tails / 1e6,
                "what": what,
                "got": got,
                **r,
            }
            rows.append(row)
            print(json.dumps(row), flush=True)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--dir", default="/tmp/tiling")
    args = ap.parse_args()
    opts = Options()
    rows = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        rows += asyncio.run(size_run(n, Path(args.dir) / str(n), opts))
    print(
        "\n| Keys | Layout | Runs | Files | Entries per live key | Size | Tails | Read | GETs | MB read | Wall |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['n']:,} | {r['layout']} | {r['runs']} | {r['files']} | {r['entries_per_key']:.2f} | "
            f"{r['size_mb']:,.0f} MB | {r['tails_mb']:,.0f} MB | {r['what']} | {r['gets']:,} | {r['read_mb']:,.0f} | "
            f"{r['wall']:.2f} s |"
        )


if __name__ == "__main__":
    main()
