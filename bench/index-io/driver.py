"""Run one configuration (variant x store x shape x concurrency) in this process; print one JSON line.

Variants:
  A        I/O and decode in Rust (object_store + tokio inside the extension); Python awaits one
           future per query (start_a + call_soon_threadsafe).
  Athread  Same Rust call, but blocking with the GIL released, called via asyncio.to_thread.
  Acache   A with the shared in-process block cache (single-flight per (path, range)).
  B        Python asyncio + obstore fetch each range; Rust (QueryB.step) is called once per
           round to decode what was fetched (GIL released) and return the next ranges.
"""

import argparse
import asyncio
import json
import random
import resource
import statistics
import time

import obstore
from obstore.store import from_url

import index_io_bench as ext

KB = 1000
MB = 1000 * 1000
LAYERS_MB = [2000, 800, 400, 200, 100, 50, 20, 10, 10, 10]
LAYER_DIR = "/home/user/.capy/work/layers/"


def s3(port):
    return {
        "aws_endpoint": f"http://127.0.0.1:{port}",
        "aws_access_key_id": "bench",
        "aws_secret_access_key": "benchbench",
        "aws_region": "us-east-1",
    }


STORES = {
    "file": (f"file://{LAYER_DIR}", {}, {}),
    "s3": ("s3://bench/", s3(9000), {"allow_http": "true"}),
    "s3lat": ("s3://bench/", s3(9101), {"allow_http": "true"}),
}


def at(r, layer, size):
    start = r.randrange(0, LAYERS_MB[layer] * MB - size)
    return (f"L{layer}.bin", start, start + size)


def tail(layer, size):
    end = LAYERS_MB[layer] * MB
    return (f"L{layer}.bin", end - size, end)


def make_plan(shape, r):
    if shape == "q1":  # diff, 1 day, first 10K: 3 rounds, 12 GETs, ~1.7 MB
        ls = r.sample(range(10), 4)
        return [
            [tail(layer, 32 * KB) for layer in ls],
            [at(r, layer, 100 * KB) for layer in ls],
            [at(r, layer, 300 * KB) for layer in ls],
        ]
    if shape == "q2":  # scan at head, 1% of keys: 3 rounds, 33 GETs, ~18 MB
        ls = r.sample(range(10), 3)
        return [
            [tail(layer, 64 * KB) for layer in ls],
            [at(r, r.choice(ls), 200 * KB) for _ in range(10)],
            [at(r, r.choice(ls), 790 * KB) for _ in range(20)],
        ]
    if shape == "q3":  # 1K scattered gets: 10 metadata, 100 segment, 2,300 block GETs, ~133.5 MB
        return [
            [tail(layer, 64 * KB) for layer in range(10)],
            [at(r, layer, 64 * KB) for layer in range(10) for _ in range(10)],
            [at(r, r.randrange(10), 55 * KB) for _ in range(2300)],
        ]
    if shape == "sf":  # every query reads the same 10 MB object
        return [[("sf.bin", 0, 10 * MB)]]
    raise ValueError(shape)


class Counters:
    tasks = 0  # asyncio Tasks created (task factory)
    futures = 0  # other awaitables: obstore futures, to_thread futures, A's completion future
    gets = 0
    bytes = 0


C = Counters()


def task_factory(loop, coro, **kw):
    C.tasks += 1
    return asyncio.Task(coro, loop=loop, **kw)


async def query_a(url, opts, plan, decode_us, conc, cache):
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    C.futures += 1

    def done(res):
        loop.call_soon_threadsafe(_resolve, fut, res)

    ext.start_a(url, opts, plan, decode_us, conc, cache, done)
    res = await fut
    return res[0]


def _resolve(fut, res):
    if isinstance(res, str):
        fut.set_exception(RuntimeError(res))
    else:
        fut.set_result(res)


async def query_athread(url, opts, plan, decode_us, conc, cache):
    C.futures += 1
    res = await asyncio.to_thread(ext.run_a, url, opts, plan, decode_us, conc, cache)
    return res[0]


async def fetch_round(store, ranges, conc):
    out = [None] * len(ranges)
    it = iter(enumerate(ranges))

    async def worker():
        for i, (path, start, end) in it:
            C.futures += 1
            out[i] = await obstore.get_range_async(store, path, start=start, end=end)

    await asyncio.gather(*(worker() for _ in range(min(conc, len(ranges)))))
    return out


async def query_b(store, plan, decode_us, conc):
    q = ext.QueryB(plan, decode_us)
    ranges = q.step([])
    fetched = 0
    while ranges is not None:
        bufs = await fetch_round(store, ranges, conc)
        n = sum(len(b) for b in bufs)
        fetched += n
        C.gets += len(bufs)
        C.bytes += n
        C.futures += 1
        ranges = await asyncio.to_thread(q.step, bufs)
    return fetched


async def lag_monitor(stop, out):
    while not stop.is_set():
        t = time.perf_counter()
        await asyncio.sleep(0.01)
        out.append(time.perf_counter() - t - 0.01)


async def main(a):
    loop = asyncio.get_running_loop()
    loop.set_task_factory(task_factory)
    url, s3cfg, client = STORES[a.store]
    a_opts = list({**s3cfg, **client}.items())
    store = from_url(url, config=s3cfg or None, client_options=client or None)
    ext.calibrate()

    def run_one(plan):
        if a.variant == "B":
            return query_b(store, plan, a.decode_us, a.conc)
        fn = query_athread if a.variant == "Athread" else query_a
        return fn(url, a_opts, plan, a.decode_us, a.conc, a.variant == "Acache")

    # Warm up: connection pools, runtimes, store construction.
    await run_one(make_plan("q1", random.Random(-1)))
    ext.reset()
    for k in ("tasks", "futures", "gets", "bytes"):
        setattr(C, k, 0)

    plans = [make_plan(a.shape, random.Random(a.seed * 100_000 + i)) for i in range(a.n)]
    expected = sum(e - s for p in plans for rnd in p for _, s, e in rnd)
    lat = []

    async def timed(plan):
        t = time.perf_counter()
        got = await run_one(plan)
        lat.append(time.perf_counter() - t)
        return got

    stop = asyncio.Event()
    lags = []
    mon = asyncio.create_task(lag_monitor(stop, lags))
    await asyncio.sleep(0.05)
    tasks0 = C.tasks
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    th0 = time.thread_time()
    t0 = time.perf_counter()
    got = await asyncio.gather(*(timed(p) for p in plans))
    wall = time.perf_counter() - t0
    th1 = time.thread_time()
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    stop.set()
    await mon
    assert sum(got) == expected, (sum(got), expected)
    gets, nbytes, calls = ext.stats()
    if a.variant == "B":
        gets, nbytes = C.gets, C.bytes
    lat.sort()
    return {
        "variant": a.variant,
        "store": a.store,
        "shape": a.shape,
        "n": a.n,
        "decode_us": a.decode_us,
        "seed": a.seed,
        "wall_s": wall,
        "p50_s": statistics.median(lat),
        "p95_s": lat[min(len(lat) - 1, int(0.95 * len(lat)))],
        "cpu_s": (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime),
        "loop_cpu_s": th1 - th0,
        "max_lag_ms": max(lags) * 1000 if lags else 0.0,
        "crossings": calls,
        "awaitables": (C.tasks - tasks0) + C.futures,
        "gets": gets,
        "mb": nbytes / MB,
        "rss_mb": ru1.ru_maxrss / 1024,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("variant", choices=["A", "Athread", "Acache", "B"])
    p.add_argument("store", choices=list(STORES))
    p.add_argument("shape", choices=["q1", "q2", "q3", "sf"])
    p.add_argument("n", type=int)
    p.add_argument("--decode-us", type=float, default=10_000.0, help="CPU microseconds per MB fetched")
    p.add_argument("--conc", type=int, default=64, help="max in-flight GETs per query")
    p.add_argument("--seed", type=int, default=0)
    print(json.dumps(asyncio.run(main(p.parse_args()))))
