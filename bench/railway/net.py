"""The bucket as a network (bench/railway): per-request latency and
per-connection and aggregate throughput, through obstore as Solera reads."""

from __future__ import annotations

import asyncio
import os
import statistics
import time
import uuid

import obstore
from obstore.store import S3Store

store = S3Store(
    os.environ["BUCKET"],
    endpoint=os.environ["ENDPOINT"],
    access_key_id=os.environ["ACCESS_KEY_ID"],
    secret_access_key=os.environ["SECRET_ACCESS_KEY"],
    region="us-east-1",
)
PREFIX = f"net-{uuid.uuid4().hex[:8]}/"


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def show(label, xs):
    ms = [x * 1000 for x in xs]
    print(
        f"NET {label}: p50 {pct(ms, 50):.1f} ms · p95 {pct(ms, 95):.1f} ms · p99 {pct(ms, 99):.1f} ms (n={len(ms)})",
        flush=True,
    )


async def timed(coro):
    t = time.perf_counter()
    out = await coro
    return out, time.perf_counter() - t


async def main():
    small = f"{PREFIX}small"
    await obstore.put_async(store, small, os.urandom(1024))
    for label, call in (
        ("GET 1 KB", lambda: obstore.get_range_async(store, small, start=0, end=1024)),
        ("GET range 100 B", lambda: obstore.get_range_async(store, small, start=100, end=200)),
        ("HEAD", lambda: obstore.head_async(store, small)),
        ("PUT 1 KB", lambda: obstore.put_async(store, f"{PREFIX}put-{uuid.uuid4().hex}", b"x" * 1024)),
        (
            "PUT 1 KB create-if-absent",
            lambda: obstore.put_async(store, f"{PREFIX}c-{uuid.uuid4().hex}", b"x" * 1024, mode="create"),
        ),
        ("LIST 1 page", lambda: obstore.list(store, PREFIX).collect_async()),
    ):
        xs = []
        for _ in range(200):
            _, dt = await timed(call())
            xs.append(dt)
        show(label, xs)

    big = f"{PREFIX}big"
    size = 256 * 2**20
    await obstore.put_async(store, big, os.urandom(size))
    for chunk in (64 * 2**10, 1 * 2**20, 8 * 2**20, 64 * 2**20):
        xs = []
        for i in range(max(3, min(40, size // chunk // 8))):
            off = (i * chunk) % (size - chunk)
            _, dt = await timed(obstore.get_range_async(store, big, start=off, end=off + chunk))
            xs.append(dt)
        mb = chunk / 1e6
        print(
            f"NET one connection, {chunk // 1024} KiB reads: p50 {statistics.median(xs) * 1000:.1f} ms · "
            f"{mb / statistics.median(xs):.0f} MB/s",
            flush=True,
        )
    for parallel, chunk in ((8, 8 * 2**20), (32, 8 * 2**20), (64, 4 * 2**20), (64, 16 * 2**20)):
        offs = [(i * chunk) % (size - chunk) for i in range(parallel)]
        sem = asyncio.Semaphore(parallel)

        async def one(off, sem=sem, chunk=chunk):
            async with sem:
                return await obstore.get_range_async(store, big, start=off, end=off + chunk)

        _, dt = await timed(asyncio.gather(*(one(o) for o in offs)))
        print(
            f"NET aggregate, {parallel} parallel {chunk // 2**20} MiB reads: {parallel * chunk / 1e6 / dt:.0f} MB/s",
            flush=True,
        )
    _, dt = await timed((await obstore.get_async(store, big)).bytes_async())
    print(f"NET one GET of 256 MiB: {size / 1e6 / dt:.0f} MB/s", flush=True)
    data = os.urandom(64 * 2**20)
    _, dt = await timed(obstore.put_async(store, f"{PREFIX}up", data))
    print(f"NET one PUT of 64 MiB: {len(data) / 1e6 / dt:.0f} MB/s", flush=True)

    paths = [m["path"] for page in obstore.list(store, PREFIX) for m in page]
    await obstore.delete_async(store, paths)


asyncio.run(main())
