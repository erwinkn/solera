"""The engine's warm resolver against a worker resolving cold
(docs/resolved-commits.md §9–§10), at 1M–100M keys.

    uv run python bench/keys/warm.py --s3 http://user:secret@127.0.0.1:9100/bucket --sizes 1e6,1e7,1e8

Each index is built as `bench.py` builds it, then given its steady-state
shape (upper levels filled, seven deltas in level 0). For patches of 1K,
10K and 100K random keys, half rewritten unchanged, it measures:

- cold worker: `KeyIndex.resolve`, reading the store (workers keep no
  cache of index files: a write the engine declines pays this);
- engine: the worker's run and request built, the resolver's whole answer
  — framing, validation, the lookup over the cache's local files, the
  delta — and the delta uploaded by the worker, with the files in the page
  cache, and again after dropping it (local SSD only; needs `sudo`); the
  resolve alone is reported too. HTTP is not in it (`results.md` measures
  it apart).

Every path ends at the same line: the delta file uploaded.

Also the engine's fill of the snapshot: requests, bytes, time, disk.
Requests are injected with latency and bandwidth as in `bench.py`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import replace

sys.path.insert(0, os.path.dirname(__file__))
from solera.keys import SortedRun  # noqa: E402
from solera.keys.cache import EngineCache  # noqa: E402
from solera.keys.index import KeyIndex, Options  # noqa: E402
from solera.keys.io import ObjectIO  # noqa: E402
from solera.keys.resolver import Ask, Prepared, Resolver, answers, request  # noqa: E402

import bench  # noqa: E402


def drop_page_cache() -> bool:
    try:
        subprocess.run(["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
        return True
    except Exception:
        return False


async def timed(fn):
    t, cpu = time.perf_counter(), time.process_time()
    out = await fn()
    return out, time.perf_counter() - t, time.process_time() - cpu


async def run_size(n: int, args) -> list[dict]:
    prefix = f"{args.prefix}n{n}/"
    bench.clear_prefix(prefix)
    rows: list[dict] = []
    tmp = tempfile.mkdtemp(prefix="solera-warm-")
    try:
        opts = Options()
        setup = ObjectIO(bench.store())
        state, sample, _, _ = await bench.build(
            setup, prefix, n, opts, max(1, n // 200_000), max(1, n // 2_000_000)
        )
        current = dict(sample)
        per_entry = sum(f.size for f in state.files) / n
        upper = await bench.fill_upper(setup, prefix, n, opts, state.depth, per_entry, current)
        state = replace(state, files=state.files + tuple(upper), prefix=prefix)
        rng = random.Random(21)
        for b in range(7):  # level 0 as steady state leaves it
            items = sorted(rng.sample(list(current.items()), 1000))
            vers = [rng.randbytes(16) for _ in items]
            files, _ = await KeyIndex(setup, None, state, opts).resolve(
                SortedRun.of([bench.key_of(i) for i, _ in items], vers),
                batch=b + 1,
                attempt="setup",
                generation=b + 1,
            )
            state = state.committed(b + 1, files, keep_log=False)
            current.update((i, v) for (i, _), v in zip(items, vers, strict=True))

        def cold():
            return ObjectIO(bench.store(), latency=args.latency, bandwidth=args.bandwidth)

        # The engine's fill.
        cache = EngineCache(os.path.join(tmp, "engine"), disk=200 * 2**30)
        io = cold()
        assert cache.admit(state)
        _, wall, cpu = await timed(lambda: cache.fill(io, state))
        disk = sum(f.size for f in cache.files.values())
        rows.append(
            {
                "n": n,
                "op": "engine fill",
                "wall": wall,
                "cpu": cpu,
                "gets": io.metrics.gets,
                "mb": io.metrics.bytes_in / 1e6,
                "disk_gb": disk / 1e9,
            }
        )
        if args.recount:
            # The engine's recount: from the store, then over the cache's local copies.
            row = {"n": n, "op": "recount"}
            io = cold()
            live, wall, cpu = await timed(lambda io=io: KeyIndex(io, None, state, opts).recount())
            row["store"] = (wall, cpu, io.metrics.gets, io.metrics.bytes_in / 1e6)
            io = cold()
            with cache.pin(state) as pin:
                again, wall, cpu = await timed(
                    lambda io=io, pin=pin: KeyIndex(io, None, state, opts).recount(pin.handles)
                )
            assert again == live, (again, live)
            row["local"] = (wall, cpu, io.metrics.gets, io.metrics.bytes_in / 1e6)
            rows.append(row)
            print(row, flush=True)
        resolver = Resolver(cache, cold(), opts)

        for k in (int(float(x)) for x in args.patches.split(",") if x):
            items = sorted(rng.sample(list(current.items()), min(k, len(current))))
            keys = [bench.key_of(i) for i, _ in items]
            vers = [v if rng.random() < 0.5 else rng.randbytes(16) for _, v in items]
            row = {"n": n, "op": f"{len(keys):,} keys, half unchanged"}
            io = cold()
            (files, _), wall, cpu = await timed(
                lambda io=io, keys=keys, vers=vers, k=k: KeyIndex(io, None, state, opts).resolve(
                    SortedRun.of(keys, vers), batch=99, attempt=f"c{k}"
                )
            )
            row["cold"] = (wall, cpu, io.metrics.gets, io.metrics.bytes_in / 1e6)
            p = Prepared("", 100, 1, state, 99, True)
            for label in ("engine", "engine_ssd"):
                if label == "engine_ssd" and not drop_page_cache():
                    continue
                io, spent = cold(), {}

                async def engine_path(io=io, keys=keys, vers=vers, p=p, k=k, label=label, spent=spent):
                    run = SortedRun.of(keys, vers)
                    body = request("inv", [Ask("out", "", "patch", 100, 1, state.prefix, 99, run)])
                    t = time.perf_counter()
                    out = await resolver.resolve(f"e{k}{label}", body, lambda name: p, lambda: True)
                    spent["resolve"] = time.perf_counter() - t
                    answer, delta = answers(out)["out"]
                    assert answer["result"] == "delta", answer
                    await io.write(f"{state.prefix}{99:012d}-e{k}{label}.0000.kx", delta)
                    return answer, len(body)

                (answer, size), wall, cpu = await timed(engine_path)
                assert (answer["added"], answer["removed"]) == (files.added, files.removed)
                row[label] = (wall, cpu, io.metrics.gets, size / 1e6)
                row[label + "_resolve"] = spent["resolve"]
            rows.append(row)
            print(row, flush=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        bench.clear_prefix(prefix)
    return rows


def cell(t) -> str:
    if t is None:
        return "—"
    wall, cpu, gets, mb = t
    return f"{bench.fmt_s(wall)} · {gets} GET · {mb:.1f} MB · CPU {bench.fmt_s(cpu)}"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7")
    ap.add_argument("--latency", type=float, default=0.03)
    ap.add_argument("--bandwidth", type=float, default=80e6)
    ap.add_argument("--s3", default=os.environ.get("SOLERA_TEST_S3", ""))
    ap.add_argument("--prefix", default=None)
    ap.add_argument("--recount", action="store_true", help="also the engine's recount, store against local")
    ap.add_argument("--patches", default="1e3,1e4,1e5", help="patch sizes; empty for none")
    args = ap.parse_args()
    base = bench.configure(args.s3) if args.s3 else ""
    args.prefix = base + (args.prefix or f"bench-warm-{uuid.uuid4().hex[:8]}/")
    rows = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        rows += await run_size(n, args)
    print("\nEvery path to the delta uploaded; in brackets, the engine's resolve alone.\n")
    print("| Keys | Patch | Cold worker | Engine, page cache | Engine, SSD only |")
    print("|---|---|---|---|---|")
    for r in rows:
        if r["op"] in ("engine fill", "recount"):
            continue
        print(
            f"| {r['n']:,} | {r['op']} | {cell(r.get('cold'))} | "
            f"{cell(r.get('engine'))} ({bench.fmt_s(r.get('engine_resolve', 0))}) | "
            f"{cell(r.get('engine_ssd'))} ({bench.fmt_s(r.get('engine_ssd_resolve', 0))}) |"
        )
    print("\n| Keys | Engine fill: time · GETs · MB read · CPU | Local files on disk |")
    print("|---|---|---|")
    for r in rows:
        if r["op"] == "engine fill":
            print(
                f"| {r['n']:,} | {bench.fmt_s(r['wall'])} · {r['gets']} GET · {r['mb']:.0f} MB · "
                f"CPU {bench.fmt_s(r['cpu'])} | {r['disk_gb']:.2f} GB |"
            )
    if args.recount:
        print("\n| Keys | Recount from the store | Recount over the cache's local copies |")
        print("|---|---|---|")
        for r in rows:
            if r["op"] == "recount":
                print(f"| {r['n']:,} | {cell(r['store'])} | {cell(r['local'])} |")


if __name__ == "__main__":
    asyncio.run(main())
