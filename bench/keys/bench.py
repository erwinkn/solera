"""K0 benchmark: the key index against an S3-compatible server, at 1M–100M keys.

    uv run python bench/keys/bench.py --sizes 1e6,1e7,1e8 --latency 0.03

Each index is built as bottom-level files in 64 MB chunks (so 100M keys never
sit in memory at once), then every operation from docs/key-index-costs.md runs
on a cold reader (no cache) unless marked warm. `--latency` adds a fixed delay
per request and `--bandwidth` a per-connection transfer rate, to model S3 on
top of a local server. Results print as Markdown.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import shutil
import tempfile
import time

import boto3
from cursus import keys as K
from cursus.keys.index import FileInfo, IndexState, KeyIndex, Options
from cursus.keys.io import DiskCache, ObjectIO
from obstore.store import S3Store

ENDPOINT, USER, SECRET, BUCKET = "http://127.0.0.1:9100", "cursus", "cursus-bench-secret", "cursus-bench"
GET_PRICE, PUT_PRICE = 0.40 / 1e6, 5.0 / 1e6


def store():
    return S3Store(BUCKET, endpoint=ENDPOINT, access_key_id=USER, secret_access_key=SECRET, region="us-east-1",
                   client_options={"allow_http": True})


def reset_bucket(prefix: str):
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=USER, aws_secret_access_key=SECRET,
                      region_name="us-east-1")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})


def key_of(i: int) -> bytes:
    return b"cust-%013d" % i


class Keyspace:
    """Sorted, unique, random-looking ids: gaps drawn around 10^12 / n."""

    def __init__(self, n: int, seed: int = 0):
        self.n = n
        self.rng = random.Random(seed)
        self.gap = max(2, 10**12 // n)

    def chunks(self, size: int):
        ident = 0
        produced = 0
        while produced < self.n:
            m = min(size, self.n - produced)
            ids = []
            for _ in range(m):
                ident += 1 + self.rng.randrange(2 * self.gap)
                ids.append(ident)
            produced += m
            yield ids


async def build(io: ObjectIO, prefix: str, n: int, opts: Options, sample_every: int):
    """The index as bottom-level files; returns its state and a sample of (id, version)."""

    t = time.perf_counter()
    files, sample, entries = [], [], 0
    per_file = max(1000, (2 * opts.max_file_bytes) // 38)  # ~38 B raw per entry
    rng = random.Random(1)
    for n_chunk, ids in enumerate(Keyspace(n).chunks(per_file)):
        keys = [key_of(i) for i in ids]
        versions = [rng.randbytes(16) for _ in ids]
        data = K.encode_file(keys, versions, bytes(len(keys)), level=opts.level)
        name = f"c-build-{n_chunk:05d}"
        await io.write(f"{prefix}{name}.kx", data)
        files.append(FileInfo.describe(name, 1, data))
        for j in range(0, len(ids), sample_every):
            sample.append((ids[j], versions[j]))
        entries += len(keys)
    state = IndexState(count=entries, files=tuple(files))
    # Let the planner place it: the deepest level moves down (no rewrite) until it fits.
    idx = KeyIndex(io, prefix, state, opts)
    while (plan := idx.plan_compaction()) is not None:
        added, removed = await idx.compact(plan)
        state = state.compacted(added, removed)
        idx = KeyIndex(io, prefix, state, opts)
    return state, sample, time.perf_counter() - t


def fmt_s(x: float) -> str:
    return f"{x * 1000:.0f} ms" if x < 1 else f"{x:.1f} s"


async def measure(label, io: ObjectIO, fn):
    io.metrics.reset()
    t = time.perf_counter()
    out = await fn()
    dt = time.perf_counter() - t
    m = io.metrics.snapshot()
    cost = m["gets"] * GET_PRICE + m["puts"] * PUT_PRICE
    return {"op": label, "wall": dt, "gets": m["gets"], "puts": m["puts"], "mb_in": m["bytes_in"] / 1e6,
            "cost": cost, "out": out}


async def run_size(n: int, args) -> list[dict]:
    prefix = f"bench/n{n}/"
    reset_bucket(prefix)
    opts = Options()
    build_io = ObjectIO(store())
    state, sample, build_s = await build(build_io, prefix, n, opts, sample_every=max(1, n // 200_000))
    size = sum(f.size for f in state.files)
    tails = sum(f.tail for f in state.files)
    info = {"n": n, "build_s": build_s, "bytes": size, "files": len(state.files), "depth": state.depth,
            "b_per_entry": size / n, "filter_b_per_entry": tails / n}
    rows = []
    rng = random.Random(2)

    def cold():
        return ObjectIO(store(), latency=args.latency, bandwidth=args.bandwidth)

    def pick(k):
        return rng.sample(sample, min(k, len(sample)))

    async def changes(io, items, *, same_share=0.0):
        items = sorted(items)
        keys = [key_of(i) for i, _ in items]
        vers = [v if rng.random() < same_share else rng.randbytes(16) for _, v in items]
        idx = KeyIndex(io, prefix, state, opts)
        return await idx.changes(keys, vers)

    for label, k, share in (("100 random keys changed", 100, 0.0), ("1K random keys changed", 1000, 0.0),
                            ("1K random keys, half unchanged", 1000, 0.5)):
        io = cold()
        rows.append(await measure(label, io, lambda io=io, k=k, share=share: changes(io, pick(k), same_share=share)))

    # Clustered: 1K consecutive existing ids from the sample's neighbourhood — re-read a
    # contiguous key run by paging, then change those keys.
    io = cold()
    idx = KeyIndex(io, prefix, state, opts)
    start = key_of(sample[len(sample) // 2][0])
    ck, cv, _ = await idx.page(start, 1000)
    io = cold()

    async def clustered(io=io):
        idx = KeyIndex(io, prefix, state, opts)
        return await idx.changes(ck, [rng.randbytes(16) for _ in ck])

    rows.append(await measure("1K clustered keys changed", io, clustered))

    # New keys: ids between existing ones (odd offsets never produced by the keyspace... use a suffix).
    io = cold()

    async def inserts(io=io):
        idx = KeyIndex(io, prefix, state, opts)
        ks = sorted(key_of(i) + b"-new" for i, _ in pick(1000))
        return await idx.changes(ks, [rng.randbytes(16) for _ in ks])

    rows.append(await measure("1K new keys inserted", io, inserts))

    if n >= 1_000_000:
        io = cold()
        rows.append(await measure("100K random keys changed", io, lambda io=io: changes(io, pick(100_000))))

    # Warm: a local disk cache that already holds every file.
    cache_dir = tempfile.mkdtemp(prefix="cursus-bench-cache-")
    try:
        cache = DiskCache(cache_dir, max_bytes=size * 2 + (1 << 30))
        io = ObjectIO(store(), latency=args.latency, bandwidth=args.bandwidth, cache=cache)
        await changes(io, pick(1000))  # warms the cache
        for f in state.files:
            await io.read_whole(f"{prefix}{f.name}.kx", f.size)
        rows.append(await measure("1K random keys changed, disk cache warm", io, lambda io=io: changes(io, pick(1000))))
    finally:
        shutil.rmtree(cache_dir, ignore_errors=True)

    # A full-delivery page and a pending read.
    io = cold()
    after = key_of(sample[len(sample) // 3][0])
    rows.append(await measure("full-delivery page of 10K keys", io,
                              lambda io=io: KeyIndex(io, prefix, state, opts).page(after, 10_000)))

    # Commit a delta, then eight more, then compact level 0.
    io = cold()
    s2 = state
    batch = 0

    async def commit_one(io=io):
        nonlocal s2, batch
        idx = KeyIndex(io, prefix, s2, opts)
        items = sorted(pick(1000))
        delta = await idx.changes([key_of(i) for i, _ in items], [rng.randbytes(16) for _ in items])
        files = await idx.write(batch, f"bench{batch}", delta)
        s2 = s2.committed(batch, files, keep_log=True)
        batch += 1

    rows.append(await measure("commit: 1K random changes + delta write", io, commit_one))
    for _ in range(opts.l0_max_files - 1):
        await commit_one()
    io = cold()

    async def compact(io=io):
        nonlocal s2
        idx = KeyIndex(io, prefix, s2, opts)
        out = await idx.compact()
        s2 = s2.compacted(*out)
        return out

    rows.append(await measure("compaction: 8 delta files into level 1", io, compact))

    if n <= args.max_replace:
        ks_all = []
        for ids in Keyspace(n).chunks(1_000_000):
            ks_all += [key_of(i) for i in ids]
        vr = random.Random(1)
        vs_all = [vr.randbytes(16) for _ in ks_all]  # the same versions the build used
        for j in rng.sample(range(len(ks_all)), len(ks_all) // 100):
            vs_all[j] = b"changed-version!"
        io = cold()
        rows.append(await measure("full replacement, 1% changed", io,
                                  lambda io=io: KeyIndex(io, prefix, state, opts).changes(ks_all, vs_all, replace=True)))
        del ks_all, vs_all

    reset_bucket(prefix)
    return info, rows


def report(results, args):
    print(f"\n### Key index benchmark ({K.IMPL}, {args.latency * 1000:.0f} ms per request, "
          f"{args.bandwidth / 1e6:.0f} MB/s per connection, 64 in parallel)\n")
    print("| Keys | Build | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels |")
    print("|---|---|---|---|---|---|---|")
    for info, _ in results:
        print(f"| {info['n']:,} | {fmt_s(info['build_s'])} | {info['bytes'] / 1e6:,.1f} MB | "
              f"{info['b_per_entry']:.1f} B | {info['filter_b_per_entry']:.2f} B | {info['files']} | {info['depth']} |")
    ops = [r["op"] for r in results[0][1]]
    for extra in (r["op"] for _, rows in results for r in rows):
        if extra not in ops:
            ops.append(extra)
    print("\n| Operation | " + " | ".join(f"{info['n']:,} keys" for info, _ in results) + " |")
    print("|---|" + "---|" * len(results))
    for op in ops:
        cells = []
        for _, rows in results:
            r = next((x for x in rows if x["op"] == op), None)
            cells.append("—" if r is None else f"{fmt_s(r['wall'])} · {r['gets']} GET {r['puts']} PUT · {r['mb_in']:.1f} MB")
        print(f"| {op} | " + " | ".join(cells) + " |")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7")
    ap.add_argument("--latency", type=float, default=0.03)
    ap.add_argument("--bandwidth", type=float, default=80e6)
    ap.add_argument("--max-replace", type=float, default=1e7)
    args = ap.parse_args()
    results = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        t = time.perf_counter()
        results.append(await run_size(n, args))
        print(f"[{n:,} keys done in {time.perf_counter() - t:.0f} s]", flush=True)
    report(results, args)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    asyncio.run(main())
