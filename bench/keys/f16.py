"""F16's cost: level 0 merged into level 1, with level 1 empty or full.

    uv run python bench/keys/f16.py --sizes 1e3,1e4,1e5

Before F16's fix, level 0 compacting into an empty level 1 was a relabel: no
bytes read or written. Now it is a merge. This measures that merge against
the same compaction into a level 1 that holds every key.

Level 0 holds 8 files of `n` changed keys each, drawn at random from `10n`
keys (so they overlap); level 1 is either empty or holds all `10n`, at an
older generation. The compaction is the explicit plan `(level 0 + level 1, 1)`.
Objects live on a local file:// store, with no injected latency: wall time
is CPU and local disk. Bytes count everything through `ObjectIO`. Peak RSS is
the compaction's own: each case's files are written by one process, and a
fresh one compacts them and reports how far its peak rose above its resident
memory just before.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pickle
import random
import resource
import subprocess
import sys
import tempfile
import time

from obstore.store import LocalStore
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

L0_FILES = 8


def key_of(i: int) -> bytes:
    return b"cust-%013d" % i


async def setup(root: str, n: int, level1: bool) -> None:
    io = ObjectIO(LocalStore(root))
    opts = Options()
    rng = random.Random(n)
    universe = 10 * n
    files = []
    if level1:  # every key at generation 1, split as compaction splits level 1
        per_file = max(1000, (2 * opts.max_file_bytes) // 38)
        for c, lo in enumerate(range(0, universe, per_file)):
            keys = [key_of(i) for i in range(lo, min(universe, lo + per_file))]
            data = K.encode_file(keys, [1] * len(keys), bytes(len(keys)), level=opts.level)
            await io.write(f"idx/c-base-{c:05d}.kx", data)
            files.append(FileInfo.describe(f"c-base-{c:05d}", 1, data))
    for c in range(L0_FILES):  # one commit each, newer by name
        keys = [key_of(i) for i in sorted(rng.sample(range(universe), n))]
        data = K.encode_file(keys, [2 + c] * len(keys), bytes(len(keys)), level=opts.level)
        name = f"{c + 1:012d}-delta"
        await io.write(f"idx/{name}.kx", data)
        files.append(FileInfo.describe(name, 0, data))
    with open(os.path.join(root, "state.pickle"), "wb") as f:
        pickle.dump(IndexState(count=universe if level1 else 0, files=tuple(files)), f)


def _rss_now() -> int:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True)
    return int(out.stdout) * 1024


async def compact(root: str) -> dict:
    with open(os.path.join(root, "state.pickle"), "rb") as f:
        state = pickle.load(f)
    io = ObjectIO(LocalStore(root))
    idx = KeyIndex(io, "idx/", state, Options())
    plan = (state.level(0) + state.level(1), 1)
    before = _rss_now()
    t = time.perf_counter()
    added, removed, _ = await idx.compact(plan)
    wall = time.perf_counter() - t
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS, kB on Linux
    peak *= 1 if sys.platform == "darwin" else 1024
    m = io.metrics.snapshot()
    return {
        "wall": wall,
        "read": m["bytes_in"],
        "written": m["bytes_out"],
        "rss": max(0, peak - before),
        "out_files": len(added),
        "out_entries": sum(f.entries for f in added),
    }


def run_case(n: int, level1: bool) -> dict:
    with tempfile.TemporaryDirectory(prefix="f16-") as root:
        me = os.path.abspath(__file__)
        subprocess.run([sys.executable, me, "--setup", root, str(n), str(int(level1))], check=True)
        out = subprocess.run(
            [sys.executable, me, "--compact", root], check=True, capture_output=True, text=True
        )
        return {"n": n, "level1": level1, **json.loads(out.stdout)}


def mb(x: int) -> str:
    return f"{x / 1e6:,.1f} MB" if x >= 1e5 else f"{x / 1e3:,.0f} kB"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sizes", default="1e3,1e4,1e5")
    p.add_argument("--repeat", type=int, default=3, help="wall time: the best of this many")
    p.add_argument("--setup", nargs=3)
    p.add_argument("--compact")
    args = p.parse_args()
    if args.setup:
        root, n, level1 = args.setup
        asyncio.run(setup(root, int(n), bool(int(level1))))
        return
    if args.compact:
        print(json.dumps(asyncio.run(compact(args.compact))))
        return
    print("| Keys per level-0 file | Level 1 | Wall | Read | Written | Peak RSS | Out entries |")
    print("|---|---|---|---|---|---|---|")
    for n in (int(float(s)) for s in args.sizes.split(",")):
        for level1 in (False, True):
            runs = [run_case(n, level1) for _ in range(args.repeat)]
            r = min(runs, key=lambda r: r["wall"])
            rss = max(r["rss"] for r in runs)
            wall = f"{r['wall'] * 1000:,.0f} ms" if r["wall"] < 1 else f"{r['wall']:.2f} s"
            held = f"all {10 * n:,} keys" if level1 else "empty"
            print(
                f"| {n:,} | {held} | {wall} | {mb(r['read'])} | {mb(r['written'])} | {mb(rss)} | {r['out_entries']:,} |",
                flush=True,
            )


if __name__ == "__main__":
    main()
