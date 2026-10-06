"""T45: does engine-side planning scale? One script, every number.

Builds an upstream key index of N keys as stamped layers (a base, then 30
days of churn: 30 daily commits of 1% of the keys, the last day as 96
commits of ~1%/96, and a last commit of 100 keys), merged by today's
merge rule as upkeep would, with the cut at the oldest reader (30 days
back). Then plans 10,000-key batches with today's planner (`owed.batch`
over `LayerIndex`, the engine's `LayerCache`) from many concurrent tasks
on one event loop, as `Observing._observe` does, and measures latency,
CPU, event-loop lag, memory, cache disk and hit rates, and cold start.

    python bench/planning/plan.py build --keys 10000000 --root ~/solera-bench/10m
    python bench/planning/plan.py run --root ~/solera-bench/10m --scenario day --tasks 10
    python bench/planning/plan.py all          # both sizes, the whole matrix, results.md

Each `run` is one scenario in its own process (memory and cold start are
per process). `all` runs each under systemd-run with a 4-CPU quota, the
size of an engine machine, and the engine's thread pool sized to it.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import gc
import json
import os
import random
import resource
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import pyarrow as pa
from obstore.store import LocalStore
from solera.keys import SortedEntries
from solera.keys.io import ObjectIO
from solera.keys.layer_cache import LayerCache
from solera.keys.layers import LayerIndex, LayerState, key_bytes
from solera_server import observed, owed

HERE = Path(__file__).parent
PREFIX = "keys/upstream/"
LIFE = "1"
CHUNK = 1_000_000  # keys per streamed chunk of the base
DAYS, PER_DAY, CHURN = 30, 96, 0.01  # 30 days back, 96 commits a day, 1% of the keys a day
BATCH = 10_000
THREADS = 8  # the engine's default executor on a 4-vCPU machine: min(32, cpus + 4)
ASIS = "08c40c2"  # main as the spike began: the planner as it is (`--planner asis`)


def planner(kind: str):
    """`owed` as of `kind`: "streaming" (this branch), or "asis" — main's
    owed.py and LayerIndex._bound, loaded from git, so one script runs both."""

    if kind == "streaming":
        return owed
    import importlib.util
    import types

    from solera.keys import layers

    def source(path: str) -> str:
        return subprocess.run(
            ["git", "show", f"{ASIS}:{path}"], capture_output=True, text=True, check=True
        ).stdout

    spec = importlib.util.spec_from_loader("solera_server.owed_asis", loader=None)
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "solera_server"
    sys.modules["solera_server.owed_asis"] = module  # its dataclasses look themselves up
    exec(compile(source("python/solera_server/owed.py"), "owed_asis.py", "exec"), module.__dict__)
    old = types.ModuleType("solera.keys.layers_asis")
    old.__package__ = "solera.keys"
    sys.modules["solera.keys.layers_asis"] = old
    exec(compile(source("python/solera/keys/layers.py"), "layers_asis.py", "exec"), old.__dict__)
    layers.LayerIndex._bound = old.LayerIndex._bound
    return module


SPACE = 2**64


def key(i: int, n: int) -> str:
    """Key `i` of `n`: random-looking (an object path with a hash in it, 41
    bytes) yet sorted as `i`, so the base streams in order and compresses as
    real keys do, not as a counter."""

    slot = SPACE // n
    mixed = (i * 0x9E3779B97F4A7C15 + 0xBF58476D1CE4E5B9) % SPACE
    mixed ^= mixed >> 31
    return f"s3://bucket/data/{i * slot + mixed % slot:016x}.parquet"


# -- the index --------------------------------------------------------------------------------


async def build(n: int, root: Path) -> dict:
    """The upstream index, written to `root/store`, its state and the commits
    readers hold in `root/state.json`."""

    if root.exists():
        shutil.rmtree(root)
    (root / "store").mkdir(parents=True)
    io = ObjectIO(LocalStore(str(root / "store")))
    state = LayerState(prefix=PREFIX, life=LIFE)
    rng = random.Random(45)
    times: dict[str, float] = {}

    def chunks():
        for lo in range(0, n, CHUNK):
            yield pa.array([key(i, n) for i in range(lo, min(n, lo + CHUNK))])

    started = time.monotonic()
    files, _ = await LayerIndex(io, state).write_replace(chunks=chunks(), name=f"{0:012d}-base", generation=1)
    state = state.committed(0, files)
    times["base"] = time.monotonic() - started
    commits = [max(1, int(n * CHURN))] * (DAYS - 1) + [max(1, int(n * CHURN / PER_DAY))] * PER_DAY + [100]
    endpoints = {"30d": 0}
    merges = writes = 0.0
    for c, size in enumerate(commits, start=1):
        if c == DAYS:  # the last day starts: a consumer a day behind observed here
            endpoints["day"] = state.head
        if c == len(commits):
            endpoints["seconds"] = state.head
        picked = sorted(rng.sample(range(n), size))
        removes = picked[: size // 20]  # 5% of the churn removes keys, the rest updates them
        upserts = picked[size // 20 :]
        run = SortedEntries.of(
            [key_bytes(key(i, n)) for i in upserts], None, [key_bytes(key(i, n)) for i in removes]
        )
        t = time.monotonic()
        # Resolved sparsely, always: write_patch would stream the whole index for a
        # scattered commit (its bytes rule), hours at 100M; the deltas are the same.
        index = LayerIndex(io, state)
        files = await index.write(f"{c:012d}-bench", await index.resolve(run, generation=c + 1), c + 1)
        state = state.committed(c, files)
        writes += time.monotonic() - t
        state = state.with_cut(endpoints["30d"])  # the oldest reader: 30 days back
        t = time.monotonic()
        while (plan := state.plan()) is not None:  # upkeep's merges, run to rest
            _, lo, count = plan
            before = state.referenced()
            ids, layer = await LayerIndex(io, state).merge(lo, count, epoch=1)
            state = state.merged(ids, layer)
            for name in before - state.referenced():  # collection: nothing pins them here
                _delete(root, name)
        merges += time.monotonic() - t
    times |= {"commits": writes, "merges": merges, "total": time.monotonic() - started}
    sizes = {
        "layers": len(state.layers),
        "bytes": sum(x.size for x in state.layers),
        "main_bytes": sum(x.main.size for x in state.layers),
        "side_bytes": sum(x.side.size for x in state.layers if x.side),
    }
    info = {
        "keys": n,
        "state": state.to_json(),
        "endpoints": endpoints,
        "build_seconds": times,
        "index": sizes,
    }
    (root / "state.json").write_text(json.dumps(info))
    return info


def _delete(root: Path, name: str) -> None:
    (root / "store" / PREFIX / name).unlink(missing_ok=True)


# -- planning ----------------------------------------------------------------------------------


class Counting:
    """The object store, counted, with a round trip added to each read (an
    S3 GET's ~30 ms, for cold numbers; 0 for the local disk as it is)."""

    def __init__(self, io: ObjectIO, rtt: float):
        self.io, self.rtt = io, rtt
        self.gets = self.bytes = 0

    async def read(self, path, start, end, size):
        self.gets += 1
        if self.rtt:
            await asyncio.sleep(self.rtt)
        data = await self.io.read(path, start, end, size)
        self.bytes += len(data)
        return data

    async def read_whole(self, path, size):
        self.gets += 1
        if self.rtt:
            await asyncio.sleep(self.rtt)
        data = await self.io.read_whole(path, size)
        self.bytes += len(data)
        return data

    def __getattr__(self, name):
        return getattr(self.io, name)


class Hits(LayerCache):
    """The engine's cache, its disk reads counted."""

    hits = misses = hit_bytes = 0

    def read(self, path, start, end):
        data = super().read(path, start, end)
        if data is None:
            self.misses += 1
        else:
            self.hits += 1
            self.hit_bytes += len(data)
        return data


def record(endpoint: int | None) -> dict:
    """A consumer that observed every key at `endpoint` (None: a full run's empty base)."""

    rec = observed.record(LIFE)
    if endpoint is not None:
        rec["base"] = observed.layer(rec, endpoint, None, {}, LIFE)
    return rec


async def lag_meter(stop: asyncio.Event, samples: list[float], period: float = 0.01) -> None:
    """How late the event loop wakes a 10 ms sleeper: the engine's tick's view."""

    loop = asyncio.get_running_loop()
    while not stop.is_set():
        t = loop.time()
        await asyncio.sleep(period)
        samples.append(max(0.0, loop.time() - t - period))


def drop_page_cache(paths: list[Path]) -> None:
    """Evict our own files from the OS page cache (no root needed), so a cold
    read reads the disk."""

    for top in paths:
        for f in top.rglob("*"):
            if f.is_file():
                fd = os.open(f, os.O_RDONLY)
                try:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(fd)


async def plan_scenario(args) -> dict:
    root = Path(args.root)
    info = json.loads((root / "state.json").read_text())
    state = LayerState.from_json(info["state"])
    n = info["keys"]
    loop = asyncio.get_running_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(THREADS))
    plan = planner(args.planner)
    if args.chunk:
        plan.CHUNK = args.chunk  # differences one Δ read asks for (owed.CHUNK)
    store = Counting(ObjectIO(LocalStore(str(root / "store"))), args.rtt)
    cache_dir = root / f"cache-{os.getpid()}"
    cache = Hits(str(cache_dir), disk=args.cache_disk, memory=2**30)
    if args.page_cold:
        drop_page_cache([root / "store"])
    warm = {}
    if args.warm:  # what the resolver's fill leaves: every main part on the engine's disk
        t = time.monotonic()
        warm["filled"] = await cache.fill(store, state, sides=args.warm == "sides")
        warm["fill_seconds"] = time.monotonic() - t
        warm["fill_gets"], store.gets, store.bytes = store.gets, 0, 0
        if args.page_cold:
            drop_page_cache([cache_dir])
    endpoint = None if args.scenario in ("full", "retries") else info["endpoints"].get(args.scenario, 0)
    if args.scenario == "all":
        endpoint = info["endpoints"]["day"]
    rng = random.Random(args.tasks)
    starts = sorted(rng.sample(range(n), args.tasks))
    latencies: list[float] = []
    sizes: list[int] = []

    async def task(i: int):
        after = key(starts[i], n) if args.tasks > 1 else None
        await walk(state, store, cache, plan, args.scenario, endpoint, after, args.batches, latencies, sizes)

    pauses: list[float] = []  # the garbage collector's, each a stall of the loop
    began: list[float] = []

    def on_gc(phase, info):
        if phase == "start":
            began.append(time.perf_counter())
        elif began:
            pauses.append(time.perf_counter() - began.pop())

    gc.callbacks.append(on_gc)
    stop, lags = asyncio.Event(), []
    meter = asyncio.create_task(lag_meter(stop, lags))
    gc.collect()
    cpu0, wall0 = resource.getrusage(resource.RUSAGE_SELF), time.monotonic()
    loop0 = time.thread_time()  # the event loop's own thread: Python, and native work under the GIL
    kids0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    procs_cpu = 0.0
    if args.where == "loop":  # as the engine plans today: on its own event loop
        await asyncio.gather(*(task(i) for i in range(args.tasks)))
    elif args.where == "thread":  # a planning thread with a loop of its own; the engine's loop only ticks
        planning = asyncio.new_event_loop()
        planning.set_default_executor(concurrent.futures.ThreadPoolExecutor(THREADS))
        thread = threading.Thread(target=planning.run_forever, daemon=True)
        thread.start()

        async def tasks():
            await asyncio.gather(*(task(i) for i in range(args.tasks)))

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(tasks(), planning))
        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(planning.shutdown_asyncgens(), planning))
        planning.call_soon_threadsafe(planning.stop)
    else:  # planning processes, each with its share of the tasks
        afters = [key(starts[i], n) if args.tasks > 1 else None for i in range(args.tasks)]
        with concurrent.futures.ProcessPoolExecutor(args.procs) as pool:
            shares = [afters[k :: args.procs] for k in range(args.procs)]
            jobs = [
                loop.run_in_executor(
                    pool,
                    _process,
                    args.root,
                    args.planner,
                    args.chunk,
                    args.scenario,
                    endpoint,
                    s,
                    args.batches,
                )
                for s in shares
                if s
            ]
            for lat, sz, used in await asyncio.gather(*jobs):
                latencies.extend(lat)
                sizes.extend(sz)
                procs_cpu += used
    wall = time.monotonic() - wall0
    loop_cpu = time.thread_time() - loop0
    cpu1 = resource.getrusage(resource.RUSAGE_SELF)
    kids1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    stop.set()
    await meter
    disk, mem = cache.used()
    shutil.rmtree(cache_dir, ignore_errors=True)
    return {
        "keys": n,
        "scenario": args.scenario,
        "tasks": args.tasks,
        "batches": len(latencies),
        "keys_per_batch": statistics.mean(sizes) if sizes else 0,
        "p50": _pct(latencies, 50),
        "p95": _pct(latencies, 95),
        "max": max(latencies, default=0),
        "wall": wall,
        "cpu": (cpu1.ru_utime + cpu1.ru_stime)
        - (cpu0.ru_utime + cpu0.ru_stime)
        + (kids1.ru_utime + kids1.ru_stime)
        - (kids0.ru_utime + kids0.ru_stime)
        + procs_cpu,
        "where": args.where,
        "loop_cpu": loop_cpu,  # of `cpu`, on the event loop's thread; the rest in its pool
        "lag_p99": _pct(lags, 99),
        "lag_max": max(lags, default=0),
        "gc_max": max(pauses, default=0),
        "gc_total": sum(pauses),
        "rss_mb": cpu1.ru_maxrss / 1024,
        "cache_disk_mb": disk / 2**20,
        "cache_mem_mb": mem / 2**20,
        "disk_hits": cache.hits,
        "disk_misses": cache.misses,
        "gets": store.gets,
        "get_mb": store.bytes / 2**20,
        "rtt": args.rtt,
        "warm": args.warm,
        "page_cold": args.page_cold,
        "planner": args.planner,
        "chunk": args.chunk or plan.CHUNK,
        **warm,
    }


async def walk(state, store, cache, plan, scenario, endpoint, after, batches, latencies, sizes) -> None:
    """One task walking `batches` batches from `after`, a fresh `LayerIndex`
    per plan as `Observing._upstream` makes them."""

    rec = record(endpoint)
    now = plan.Now(state.head, None, {}, LIFE)
    for _ in range(batches):
        index = LayerIndex(store, state, cache=cache)
        t = time.monotonic()
        if scenario == "retries":
            b = await retry_walk(index, after)
        else:
            b = await plan.batch(
                index, rec, now, BATCH, after=after, keys="all" if scenario == "all" else None
            )
        latencies.append(time.monotonic() - t)
        sizes.append(len(b.keys))
        if b.final or b.end is None:
            return
        after = b.end


def _process(root: str, planner_kind: str, chunk: int, scenario: str, endpoint, afters, batches) -> tuple:
    """A planning process's share of the tasks, read from the store's local
    files (the page cache: what a warm engine disk is). Its own loop and pool."""

    async def go():
        info = json.loads((Path(root) / "state.json").read_text())
        state = LayerState.from_json(info["state"])
        asyncio.get_running_loop().set_default_executor(concurrent.futures.ThreadPoolExecutor(2))
        plan = planner(planner_kind)
        if chunk:
            plan.CHUNK = chunk
        store = ObjectIO(LocalStore(str(Path(root) / "store")))
        latencies, sizes = [], []
        await asyncio.gather(
            *(
                walk(state, store, None, plan, scenario, endpoint, a, batches, latencies, sizes)
                for a in afters
            )
        )
        return latencies, sizes

    cpu = time.process_time()  # this call's CPU, every thread of the process (its only call)
    latencies, sizes = asyncio.run(go())
    return latencies, sizes, time.process_time() - cpu


async def retry_walk(index: LayerIndex, after: str | None):
    """`Observing._retry`'s walk, stood in for: the stored outcomes walked in
    pages of `BATCH` from the pass's place, each decoded and tested, until
    `BATCH` are due or `WALK x BATCH` were walked (1% due, scattered), then the
    due keys read at the head. The upstream index stands in for the outcome
    index (same layers, same reads)."""

    from solera_server.observing import WALK

    cursor = key_bytes(after) if after else None
    due, walked, end = [], 0, None
    while len(due) < BATCH and walked < WALK * BATCH:
        rows, nxt = await index.delta(None, after=cursor, first=BATCH)
        for k, _, _, _, _ in rows:
            walked += 1
            if hash(k) % 100 == 0:  # due: scattered, ~1%
                due.append(k.decode())
            end = k
            if len(due) >= BATCH or walked >= WALK * BATCH:
                break
        else:
            if nxt is None:
                break
            cursor = nxt
            continue
        break
    from solera.keys.delta import delta

    found = (await delta(index, None, index.state.head, keys=sorted(due))).diffs if due else []
    return owed.Batch([None] * len(found), after, end.decode() if end else None, end is None)


def _pct(xs: list[float], p: int) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


# -- the matrix ---------------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--keys", type=int, required=True)
    b.add_argument("--root", required=True)
    r = sub.add_parser("run")
    r.add_argument("--root", required=True)
    r.add_argument("--scenario", choices=["seconds", "day", "30d", "full", "all", "retries"], required=True)
    r.add_argument("--tasks", type=int, default=1)
    r.add_argument("--batches", type=int, default=3)
    r.add_argument("--warm", choices=["", "main", "sides"], default="main")
    r.add_argument("--rtt", type=float, default=0.0)
    r.add_argument("--page-cold", action="store_true")
    r.add_argument("--cache-disk", type=int, default=16 * 2**30)
    r.add_argument("--planner", choices=["streaming", "asis"], default="streaming")
    r.add_argument("--chunk", type=int, default=0)
    r.add_argument("--where", choices=["loop", "thread", "process"], default="loop")
    r.add_argument("--procs", type=int, default=4)
    a = sub.add_parser("all")
    a.add_argument("--only", help="run only this group, replacing its rows in results.json")
    args = parser.parse_args()
    if args.cmd == "build":
        print(json.dumps(asyncio.run(build(args.keys, Path(args.root))), default=str)[:2000])
    elif args.cmd == "run":
        print(json.dumps(asyncio.run(plan_scenario(args))))
    else:
        matrix(args.only)


SCENARIOS = ["seconds", "day", "30d", "full", "all", "retries"]
CAP = ["systemd-run", "--user", "--scope", "-q", "-p", "MemoryMax=16G", "-p", "CPUQuota=400%"]


def runs(size: str) -> list[dict]:
    """The matrix for one index size: each run's arguments and its group."""

    out = []
    for tasks in (10, 50):  # today's engine, the planner streaming: on the loop, Δ pages of 1,000
        out += [{"group": "loop", "scenario": sc, "tasks": tasks} for sc in SCENARIOS]
    for where in ("loop", "thread", "process"):  # off the loop, Δ pages of 10,000
        out += [
            {"group": f"{where}-10k", "scenario": sc, "tasks": 50, "chunk": 10_000, "where": where}
            for sc in ("30d", "full")
        ]
    for warm in ("", "main"):  # cold start: an S3 round trip per GET, the cache empty (a restart) or filled
        out += [
            {
                "group": f"cold-{warm or 'empty'}",
                "scenario": sc,
                "tasks": 10,
                "rtt": 0.03,
                "warm": warm,
                "page_cold": True,
            }
            for sc in ("day", "30d", "full")
        ]
    if size != "100m":  # the planner as it is: O(key space) a batch, so one batch each
        out += [
            {"group": "asis", "scenario": sc, "tasks": 10, "planner": "asis"} for sc in ("seconds", "day")
        ]
        out += [
            {"group": "asis", "scenario": sc, "tasks": 1, "batches": 1, "planner": "asis"}
            for sc in ("30d", "full", "all")
        ]
    return out


def matrix(only: str | None = None) -> None:
    """Build what is missing, run every scenario in its own capped process,
    write results.json and results.md beside this script."""

    top = Path(os.environ.get("SOLERA_BENCH", Path.home() / "solera-bench"))
    cap = CAP if shutil.which("systemd-run") else []
    kept = (
        json.loads((HERE / "results.json").read_text()) if only and (HERE / "results.json").exists() else []
    )
    results = [r for r in kept if r.get("group") != only]
    for size, n in (("10m", 10_000_000), ("100m", 100_000_000)):
        root = top / size
        if not (root / "state.json").exists():
            subprocess.run(
                [*cap, sys.executable, __file__, "build", "--keys", str(n), "--root", str(root)], check=True
            )
        build = json.loads((root / "state.json").read_text())
        if not only:
            results.append({"size": size, "group": "build", **build["build_seconds"], **build["index"]})
        for run in runs(size):
            if only and run["group"] != only:
                continue
            argv = [sys.executable, __file__, "run", "--root", str(root), "--scenario", run["scenario"]]
            for flag in ("tasks", "batches", "chunk", "where", "rtt", "warm", "planner"):
                if flag in run:
                    argv += [f"--{flag}", str(run[flag])]
            if run.get("page_cold"):
                argv.append("--page-cold")
            done = subprocess.run([*cap, *argv], capture_output=True, text=True)
            line = done.stdout.strip().splitlines()[-1] if done.stdout.strip() else "{}"
            found = json.loads(line) if done.returncode == 0 else {"error": done.stderr[-2000:]}
            results.append({"size": size, "group": run["group"], **found})
            print(json.dumps(results[-1]), flush=True)
            (HERE / "results.json").write_text(json.dumps(results, indent=1))
    (HERE / "results.md").write_text(table(results))


def table(results: list[dict]) -> str:
    cols = [
        ("size", "{}"),
        ("group", "{}"),
        ("scenario", "{}"),
        ("tasks", "{}"),
        ("batches", "{}"),
        ("p50", "{:.3f}"),
        ("p95", "{:.3f}"),
        ("wall", "{:.1f}"),
        ("cpu", "{:.1f}"),
        ("loop_cpu", "{:.1f}"),
        ("lag_max", "{:.3f}"),
        ("rss_mb", "{:.0f}"),
        ("cache_disk_mb", "{:.0f}"),
        ("disk_hits", "{}"),
        ("gets", "{}"),
        ("get_mb", "{:.1f}"),
    ]
    lines = ["| " + " | ".join(c for c, _ in cols) + " |", "|" + "---|" * len(cols)]
    for r in results:
        if r.get("group") == "build" or "error" in r:
            continue
        lines.append("| " + " | ".join(f.format(r[c]) if c in r else "" for c, f in cols) + " |")
    builds = [r for r in results if r.get("group") == "build"]
    head = [
        "Builds: "
        + "; ".join(
            f"{b['size']}: {b['total']:.0f} s, {b['layers']} layers, {b['bytes'] / 2**20:.0f} MiB"
            for b in builds
        ),
        "",
    ]
    return "\n".join(head + lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
