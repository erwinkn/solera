"""A pass far behind (docs/presence-at-position.md's side finding): what it
costs to read every change of a long delta log, page by page with
`KeyIndex.pending`, which starts its merge of the commits over for every
page, against `KeyIndex.pending_pages`, one merge read once.

    uv run python bench/keys/pending.py --commits 1000,10000 --dir /tmp/pending

Builds a delta log of `commits` commits of 1K keys each over a universe of
`--keys` keys (90% updates, 5% removes, 5% inserts), as presence.py's does,
on obstore's LocalStore with the bench's model of a store: 30 ms per request,
80 MB/s per connection. Pages are 100K keys, as a pass's scan reads them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import shutil
import sys
import time
from dataclasses import replace
from pathlib import Path

from obstore.store import LocalStore
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from bench import key_of  # noqa: E402

PAGE = 100_000


def store(root: Path, cold: bool) -> ObjectIO:
    latency, bandwidth = (0.03, 80e6) if cold else (0.0, None)
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


async def delta_log(root: Path, commits: int, universe: int, per: int = 1000) -> IndexState:
    io, rng = store(root, cold=False), random.Random(11)
    state = IndexState(prefix="idx/")
    live, removed, extra = set(range(universe)), [], universe
    for c in range(1, commits + 1):
        chosen: dict[int, bool] = {}
        while len(chosen) < per:
            x = rng.random()
            if x < 0.95:
                i = rng.randrange(universe)
                if i in live and i not in chosen:
                    chosen[i] = x >= 0.90  # deleted
            elif removed and rng.random() < 0.5:
                chosen.setdefault(removed.pop(rng.randrange(len(removed))), False)
            else:
                chosen[extra], extra = False, extra + 1
        order = sorted(chosen, key=key_of)
        for i in order:
            (live.discard if chosen[i] else live.add)(i)
            if chosen[i]:
                removed.append(i)
        data = K.encode_file([key_of(i) for i in order], [1000 + c] * per, bytes(chosen[i] for i in order))
        name = f"{c:012d}-log"
        await io.write(f"{state.prefix}{name}.kx", data)
        state = replace(state, log=state.log + ((c, (FileInfo.describe(name, 0, data),)),))
    return state


async def measure(io: ObjectIO, scan) -> dict:
    io.metrics.reset()
    t, cpu = time.perf_counter(), time.process_time()
    entries = await scan()
    m = io.metrics.snapshot()
    return {
        "wall": round(time.perf_counter() - t, 2),
        "cpu": round(time.process_time() - cpu, 2),
        "gets": m["gets"],
        "mb": round(m["bytes_in"] / 1e6, 1),
        "entries": entries,
    }


async def run(args) -> None:
    for commits in (int(c) for c in args.commits.split(",")):
        root = Path(args.dir) / f"log-{commits}"
        shutil.rmtree(root, ignore_errors=True)
        t = time.perf_counter()
        state = await delta_log(root, commits, int(float(args.keys)))
        print(f"# {commits} commits written in {time.perf_counter() - t:.0f} s", flush=True)

        async def paged(io=None, state=state, commits=commits):
            index, after, n = KeyIndex(io, None, state), None, 0
            while True:
                keys, *_, after = await index.pending(1, commits, after, PAGE)
                n += len(keys)
                if after is None:
                    return n

        async def streamed(io=None, state=state, commits=commits):
            n = 0
            async for keys, *_ in KeyIndex(io, None, state).pending_pages(1, commits, None, PAGE):
                n += len(keys)
            return n

        for name, scan in (("pending, page by page", paged), ("pending_pages, one merge", streamed)):
            io = store(root, cold=True)
            row = {"commits": commits, "how": name, **await measure(io, lambda s=scan, io=io: s(io))}
            print(json.dumps(row), flush=True)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--commits", default="1000,10000")
    parser.add_argument("--keys", default="1e6")
    parser.add_argument("--dir", default="/tmp/pending-bench")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
