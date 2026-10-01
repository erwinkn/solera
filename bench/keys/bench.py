"""K0 benchmark: the key index against an S3-compatible server, at 1M–100M keys.

    uv run python bench/keys/bench.py --sizes 1e6,1e7,1e8 --latency 0.03

Each index is built as bottom-level files in 64 MB chunks (so 100M keys never
sit in memory at once), then every operation from docs/key-index-costs.md runs
on a cold reader (no cache) unless marked warm. `--latency` adds a fixed delay
per request and `--bandwidth` a per-connection transfer rate, to model S3 on
top of a local server. Results print as Markdown.

Suites (`--suites`, all by default):
- base: the operations recorded in results.md, in the same order and with the same keys.
- scan: a full scan of the index (the recount), in 100K-key pages.
- load: an initial load of every key, unsorted, through `KeyIndex.replace` (`bulk.py` measures
  the bulk operations' memory, each in a process of its own).
- crossover: the read strategy forced each way (whole levels; tails, then blocks; tails, then the
  rest of each file) against the planner's pick.
- steady: the upper levels filled as steady-state writes leave them, then commits and one
  compaction of each kind.

`--s3` names the server and bucket (default `$SOLERA_TEST_S3`); every object goes under a
unique `--prefix`, deleted afterwards. `--json` saves the results; `--render` prints saved ones.
On S3 itself, from a worker in the bucket's region, inject nothing:

    uv run python bench/keys/bench.py --s3 s3://bucket/prefix --latency 0 --bandwidth 0
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import resource
import shutil
import tempfile
import time
import uuid
from dataclasses import replace
from urllib.parse import unquote, urlsplit

import boto3
from obstore.store import S3Store
from solera import keys as K
from solera.keys import Rows
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import DiskCache, ObjectIO

S3 = {
    "endpoint": "http://127.0.0.1:9100",
    "user": "solera",
    "secret": "solera-bench-secret",
    "bucket": "solera-bench",
}
GET_PRICE, PUT_PRICE = 0.40 / 1e6, 5.0 / 1e6
SUITES = ("base", "scan", "load", "crossover", "steady")


def configure(url: str) -> str:
    """Use S3 itself, `s3://bucket[/prefix]` (region and credentials from the environment),
    or an S3-compatible server, `http://user:secret@host:port/bucket[/prefix]`; returns the prefix."""

    u = urlsplit(url)
    if u.scheme == "s3":
        bucket, prefix = u.netloc, u.path.strip("/")
        S3.update(endpoint=None, user=None, secret=None, bucket=bucket)
    else:
        bucket, _, prefix = u.path.strip("/").partition("/")
        S3.update(
            endpoint=f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
            user=unquote(u.username or ""),
            secret=unquote(u.password or ""),
            bucket=bucket,
        )
    return f"{prefix}/" if prefix else ""


def store():
    if S3["endpoint"] is None:
        return S3Store(S3["bucket"])
    return S3Store(
        S3["bucket"],
        endpoint=S3["endpoint"],
        access_key_id=S3["user"],
        secret_access_key=S3["secret"],
        region="us-east-1",
        client_options={"allow_http": True},
    )


def clear_prefix(prefix: str):
    if S3["endpoint"] is None:
        s3 = boto3.client("s3")
    else:
        s3 = boto3.client(
            "s3",
            endpoint_url=S3["endpoint"],
            aws_access_key_id=S3["user"],
            aws_secret_access_key=S3["secret"],
            region_name="us-east-1",
        )
        try:
            s3.head_bucket(Bucket=S3["bucket"])
        except s3.exceptions.ClientError:
            s3.create_bucket(Bucket=S3["bucket"])
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3["bucket"], Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=S3["bucket"], Delete={"Objects": objs})


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


async def build(io: ObjectIO, prefix: str, n: int, opts: Options, sample_every: int, big_every: int):
    """The index as bottom-level files; returns its state, a sample of (id, version),
    a bigger sample for bulk operations, and the build time."""

    t = time.perf_counter()
    files, sample, big, entries = [], [], [], 0
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
        for j in range(0, len(ids), big_every):
            big.append((ids[j], versions[j]))
        entries += len(keys)
    state = IndexState(count=entries, files=tuple(files))
    # Let the planner place it: the deepest level moves down (no rewrite) until it fits.
    idx = KeyIndex(io, prefix, state, opts)
    while (plan := idx.plan_compaction()) is not None:
        added, removed = await idx.compact(plan)
        state = state.compacted(added, removed)
        idx = KeyIndex(io, prefix, state, opts)
    return state, sample, big, time.perf_counter() - t


def all_entries(n: int) -> tuple[list[bytes], list[bytes]]:
    """Every key with the version the build gave it."""

    ks_all = []
    for ids in Keyspace(n).chunks(1_000_000):
        ks_all += [key_of(i) for i in ids]
    vr = random.Random(1)
    return ks_all, [vr.randbytes(16) for _ in ks_all]


def fmt_s(x: float) -> str:
    return f"{x * 1000:.0f} ms" if x < 1 else f"{x:.1f} s"


async def measure(label, io: ObjectIO, fn):
    io.metrics.reset()
    t = time.perf_counter()
    out = await fn()
    dt = time.perf_counter() - t
    m = io.metrics.snapshot()
    cost = m["gets"] * GET_PRICE + m["puts"] * PUT_PRICE
    return {
        "op": label,
        "wall": dt,
        "gets": m["gets"],
        "puts": m["puts"],
        "mb_in": m["bytes_in"] / 1e6,
        "mb_out": m["bytes_out"] / 1e6,
        "cost": cost,
        "out": out,
    }


def levels(state: IndexState) -> dict[int, tuple[int, float, int]]:
    """Per level: files, MB, entries."""

    out = {}
    for f in state.files:
        files, mb, entries = out.get(f.level, (0, 0.0, 0))
        out[f.level] = (files + 1, mb + f.size / 1e6, entries + f.entries)
    return dict(sorted(out.items()))


# -- the read strategy, forced ---------------------------------------------------------------


class Strategy(KeyIndex):
    """A KeyIndex that reads every level whole (`force="whole"`), or reads the tails and
    then only the blocks (`"blocks"`) or the rest of each file (`"rest"`), or does as the
    planner decides (`force=None`); records the route taken and the planner's estimates."""

    def __init__(self, *args, force: str | None = None, **kw):
        super().__init__(*args, **kw)
        self.force = force
        self.route = "whole"
        self.estimates: dict[str, float] = {}

    def _read_whole(self, level):
        return self.force == "whole" or (self.force is None and super()._read_whole(level))

    async def _filter(self, levels, keys, want):
        self.route = "tails only"
        return await super()._filter(levels, keys, want)

    def _plan_reads(self, needs, spent):
        options = dict(self._read_options(needs))
        n = len(needs)
        self.estimates = {w: spent + self._estimate(options[(w == "rest",) * n]) for w in ("blocks", "rest")}
        if self.force in ("blocks", "rest"):
            rest = [self.force == "rest"] * n
        else:
            rest = super()._plan_reads(needs, spent)
        self.route = "tails, then " + ("rest" if all(rest) else "blocks" if not any(rest) else "mixed")
        return rest


async def crossover(prefix, state, big, opts, cold) -> list[dict]:
    """Every written key changed, then half of them rewritten unchanged — the case the
    filters can't clear."""

    rng = random.Random(30)
    rows = []
    for k, same_share in (
        (1_000, 0.0),
        (10_000, 0.0),
        (100_000, 0.0),
        (1_000_000, 0.0),
        (1_000, 0.5),
        (10_000, 0.5),
        (100_000, 0.5),
    ):
        if k > len(big) // 2:
            continue
        items = sorted(rng.sample(big, k))
        keys = [key_of(i) for i, _ in items]
        vers = [v if rng.random() < same_share else rng.randbytes(16) for _, v in items]
        row = {"k": k, "unchanged": same_share}
        for force in (None, "whole", "blocks", "rest"):
            io = cold()
            idx = Strategy(io, prefix, state, opts, force=force)
            r = await measure(
                f"{k} random keys changed", io, lambda idx=idx, keys=keys, vers=vers: idx.changes(keys, vers)
            )
            r.pop("out")
            row[force or "planner"] = r
            if force is None:
                row["picked"], row["estimates"] = idx.route, idx.estimates
        rows.append(row)
    return rows


# -- the steady state ------------------------------------------------------------------------


async def fill_upper(io, prefix, n, opts, depth, per_entry, current, fill=0.95) -> list[FileInfo]:
    """Levels 1 .. depth-1 as steady-state writes leave them: level L holds `fill` of its
    target size (`level_base · fanout^(L-1)`) in newer versions of random existing keys,
    split into files the way compaction splits them. Updates `current` (id -> version)."""

    rng = random.Random(20)
    share = {
        lv: min(1.0, fill * opts.level_base * opts.fanout ** (lv - 1) / per_entry / n)
        for lv in range(1, depth)
    }
    files: list[FileInfo] = []
    pending = {lv: ([], []) for lv in share}
    raw = dict.fromkeys(share, 0)

    async def flush(lv):
        ks, vs = pending[lv]
        if ks:
            data = K.encode_file(ks, vs, bytes(len(ks)), level=opts.level)
            name = f"u{lv}-{len(files):05d}"
            await io.write(f"{prefix}{name}.kx", data)
            files.append(FileInfo.describe(name, lv, data))
        pending[lv] = ([], [])
        raw[lv] = 0

    for ids in Keyspace(n).chunks(1_000_000):
        for lv in sorted(share, reverse=True):  # deeper first: the newest version ends up in `current`
            for j in sorted(rng.sample(range(len(ids)), round(share[lv] * len(ids)))):
                ks, vs = pending[lv]
                key, v = key_of(ids[j]), rng.randbytes(16)
                ks.append(key)
                vs.append(v)
                raw[lv] += len(key) + len(v) + 4
                if ids[j] in current:
                    current[ids[j]] = v
                if raw[lv] >= 2 * opts.max_file_bytes:  # compaction's split
                    await flush(lv)
    for lv in share:
        await flush(lv)
    return files


def push_plan(state: IndexState, lv: int):
    """The compaction `plan_compaction` picks once level `lv` is over its target: the
    file overlapping the next level least, with the files it overlaps there."""

    below = state.level(lv + 1)

    def overlap(f):
        return [g for g in below if g.max >= f.min and g.min <= f.max]

    pick = min(state.level(lv), key=lambda f: sum(g.size for g in overlap(f)))
    return [pick, *overlap(pick)], lv + 1


async def steady(n, prefix, state, sample, opts, cold) -> tuple[list[dict], dict]:
    """The index as steady-state writes leave it — upper levels filled, level 0 one delta
    short of a compaction, holding on average half the merged level-0 file it pushes into
    level 1 — then operations on it and one compaction of each kind. Checks afterwards that
    every key kept its newest version."""

    setup = ObjectIO(store())
    current = dict(sample)
    t = time.perf_counter()
    per_entry = sum(f.size for f in state.files) / n
    upper = await fill_upper(setup, prefix, n, opts, state.depth, per_entry, current)
    st = replace(state, files=state.files + tuple(upper))
    rng = random.Random(21)
    batch = 1

    def pick(k):
        return sorted(rng.sample(list(current.items()), k))

    async def fill_l0(name):
        """A level-0 file of newer versions of sampled keys: half the size at which
        level 0 merges into level 1."""

        nonlocal st
        l1 = sum(f.size for f in st.level(1))
        items = pick(min(len(current), round(l1 / opts.fanout / 2 / per_entry)))
        vers = [rng.randbytes(16) for _ in items]
        data = K.encode_file([key_of(i) for i, _ in items], vers, bytes(len(items)), level=opts.level)
        await setup.write(f"{prefix}{name}.kx", data)
        st = replace(st, files=st.files + (FileInfo.describe(name, 0, data),))
        current.update((i, v) for (i, _), v in zip(items, vers, strict=True))

    async def commit(io):
        nonlocal st, batch
        items = pick(1000)
        vers = [rng.randbytes(16) for _ in items]
        idx = KeyIndex(io, prefix, st, opts)
        delta = await idx.changes([key_of(i) for i, _ in items], vers)
        st = st.committed(batch, await idx.write(batch, f"steady{batch}", delta), keep_log=False)
        current.update((i, v) for (i, _), v in zip(items, vers, strict=True))
        batch += 1

    await fill_l0(f"{0:012d}-fill")
    for _ in range(opts.l0_max_files - 2):  # level 0 one delta short of a compaction
        await commit(setup)
    shape = {"before": levels(st), "fill_s": time.perf_counter() - t}

    async def changes(io, k, same_share=0.0):
        items = pick(k)
        vers = [v if rng.random() < same_share else rng.randbytes(16) for _, v in items]
        return await KeyIndex(io, prefix, st, opts).changes([key_of(i) for i, _ in items], vers)

    async def compact(io, plan=None):
        nonlocal st
        out = await KeyIndex(io, prefix, st, opts).compact(plan)
        st = st.compacted(*out)
        return None

    rows = []
    io = cold()
    rows.append(await measure("steady: 1K random keys changed", io, lambda io=io: changes(io, 1000)))
    io = cold()
    rows.append(
        await measure("steady: 1K random keys, half unchanged", io, lambda io=io: changes(io, 1000, 0.5))
    )
    io = cold()
    after = key_of(sorted(current)[len(current) // 3])
    rows.append(
        await measure(
            "steady: full-delivery page of 10K keys",
            io,
            lambda io=io: KeyIndex(io, prefix, st, opts).page(after, 10_000),
        )
    )
    io = cold()
    rows.append(
        await measure(
            "steady: full scan (recount), 100K-key pages",
            io,
            lambda io=io: KeyIndex(io, prefix, st, opts).recount(),
        )
    )
    io = cold()
    rows.append(
        await measure("steady: commit: 1K random changes + delta write", io, lambda io=io: commit(io))
    )
    io = cold()
    rows.append(await measure("steady: compaction: level 0, 8 files", io, lambda io=io: compact(io)))
    await fill_l0(f"{batch:012d}-fill")
    io = cold()
    l0 = st.level(0)
    lo, hi = min(f.min for f in l0), max(f.max for f in l0)
    plan = (l0 + [f for f in st.level(1) if f.max >= lo and f.min <= hi], 1)
    rows.append(
        await measure(
            "steady: compaction: level 0, a tenth of level 1, into level 1",
            io,
            lambda io=io, plan=plan: compact(io, plan),
        )
    )
    for lv in range(1, state.depth):
        io = cold()
        plan = push_plan(st, lv)
        rows.append(
            await measure(
                f"steady: compaction: a level-{lv} file into level {lv + 1}",
                io,
                lambda io=io, plan=plan: compact(io, plan),
            )
        )
    shape["after"] = levels(st)

    # The compactions kept each key's newest version: rewriting current versions changes nothing.
    items = pick(1000)
    delta = await KeyIndex(setup, prefix, st, opts).changes(
        [key_of(i) for i, _ in items], [v for _, v in items]
    )
    if len(delta):
        raise AssertionError(f"steady: {len(delta)} of 1,000 unchanged keys changed after the compactions")
    if n <= 10_000_000 and (count := await KeyIndex(setup, prefix, st, opts).recount()) != n:
        raise AssertionError(f"steady: counted {count:,} keys after the compactions, expected {n:,}")
    return rows, shape


# -- one size ------------------------------------------------------------------------------


async def run_size(n: int, args) -> dict:
    prefix = f"{args.prefix}n{n}/"
    clear_prefix(prefix)
    try:
        return await _run_size(n, prefix, args)
    finally:
        clear_prefix(prefix)


async def _run_size(n: int, prefix: str, args) -> dict:
    opts = Options()
    build_io = ObjectIO(store())
    state, sample, big, build_s = await build(
        build_io,
        prefix,
        n,
        opts,
        sample_every=max(1, n // 200_000),
        big_every=max(1, n // 2_000_000),
    )
    size = sum(f.size for f in state.files)
    tails = sum(f.tail for f in state.files)
    info = {
        "n": n,
        "build_s": build_s,
        "bytes": size,
        "files": len(state.files),
        "depth": state.depth,
        "b_per_entry": size / n,
        "filter_b_per_entry": tails / n,
    }
    rows = []
    result = {"info": info, "rows": rows}
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

    if "base" in args.suites:
        for label, k, share in (
            ("100 random keys changed", 100, 0.0),
            ("1K random keys changed", 1000, 0.0),
            ("1K random keys, half unchanged", 1000, 0.5),
        ):
            io = cold()
            rows.append(
                await measure(
                    label, io, lambda io=io, k=k, share=share: changes(io, pick(k), same_share=share)
                )
            )

        # Clustered: 1K consecutive existing ids from the sample's neighbourhood — re-read a
        # contiguous key run by paging, then change those keys.
        io = cold()
        idx = KeyIndex(io, prefix, state, opts)
        start = key_of(sample[len(sample) // 2][0])
        ck, cv, _, _ = await idx.page(start, 1000)
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
            rows.append(
                await measure("100K random keys changed", io, lambda io=io: changes(io, pick(100_000)))
            )

        # Warm: a local disk cache that already holds every file.
        cache_dir = tempfile.mkdtemp(prefix="solera-bench-cache-")
        try:
            cache = DiskCache(cache_dir, max_bytes=size * 2 + (1 << 30))
            io = ObjectIO(store(), latency=args.latency, bandwidth=args.bandwidth, cache=cache)
            await changes(io, pick(1000))  # warms the cache
            for f in state.files:
                await io.read_whole(f"{prefix}{f.name}.kx", f.size)
            rows.append(
                await measure(
                    "1K random keys changed, disk cache warm", io, lambda io=io: changes(io, pick(1000))
                )
            )
        finally:
            shutil.rmtree(cache_dir, ignore_errors=True)

        # A full-delivery page and a pending read.
        io = cold()
        after = key_of(sample[len(sample) // 3][0])
        rows.append(
            await measure(
                "full-delivery page of 10K keys",
                io,
                lambda io=io: KeyIndex(io, prefix, state, opts).page(after, 10_000),
            )
        )

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

        rows.append(await measure("compaction: 8 delta files", io, compact))

    if n <= args.max_replace and ("base" in args.suites or "load" in args.suites):
        ks_all, vs_all = all_entries(n)
        if "base" in args.suites:
            for j in rng.sample(range(len(ks_all)), len(ks_all) // 100):
                vs_all[j] = b"changed-version!"
            io = cold()
            rows.append(
                await measure(
                    "full replacement, 1% changed",
                    io,
                    lambda io=io: KeyIndex(io, prefix, state, opts).replace(
                        Rows.pairs(list(zip(ks_all, vs_all, strict=True))), 1, "replace"
                    ),
                )
            )
        if "load" in args.suites:
            # The first write of an index: every key, in row order rather than key order.
            order = list(range(len(ks_all)))
            random.Random(40).shuffle(order)
            ks, vs = [ks_all[i] for i in order], [vs_all[i] for i in order]
            del ks_all, vs_all, order
            io = cold()

            async def load(io=io):
                idx = KeyIndex(io, f"{prefix}load/", IndexState(), opts)
                return await idx.replace(Rows.pairs(list(zip(ks, vs, strict=True))), 0, "load")

            rows.append(await measure("initial load: every key, unsorted", io, load))
            del ks, vs
        else:
            del ks_all, vs_all

    if "scan" in args.suites:
        io = cold()
        rows.append(
            await measure(
                "full scan (recount), 100K-key pages",
                io,
                lambda io=io: KeyIndex(io, prefix, state, opts).recount(),
            )
        )
        # The engine recounts through its disk cache (`key_cache`): once downloaded, CPU only.
        cache_dir = tempfile.mkdtemp(prefix="solera-bench-cache-")
        try:
            io = ObjectIO(
                store(),
                latency=args.latency,
                bandwidth=args.bandwidth,
                cache=DiskCache(cache_dir, max_bytes=size * 2 + (1 << 30)),
            )
            for f in state.files:
                await io.read_whole(f"{prefix}{f.name}.kx", f.size)
            rows.append(
                await measure(
                    "full scan (recount), disk cache warm",
                    io,
                    lambda io=io: KeyIndex(io, prefix, state, opts).recount(),
                )
            )
        finally:
            shutil.rmtree(cache_dir, ignore_errors=True)

    if "crossover" in args.suites:
        result["crossover"] = await crossover(prefix, state, big, opts, cold)

    if "steady" in args.suites:
        steady_rows, result["shape"] = await steady(n, prefix, state, sample, opts, cold)
        rows += steady_rows

    for r in rows:
        if r["op"].startswith(("full scan", "steady: full scan")) and r["out"] != n:
            raise AssertionError(f"{r['op']}: counted {r['out']:,} keys, expected {n:,}")
    info["peak_rss_gb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20
    return result


# -- report --------------------------------------------------------------------------------


def cell(r: dict | None) -> str:
    if r is None:
        return "—"
    up = f" ↑{r['mb_out']:.1f} MB" if r["mb_out"] >= 0.05 else ""
    return f"{fmt_s(r['wall'])} · {r['gets']} GET {r['puts']} PUT · {r['mb_in']:.1f} MB{up}"


def report(results, args):
    injected = [f"{args['latency'] * 1000:.0f} ms per request"] if args["latency"] else []
    injected += [f"{args['bandwidth'] / 1e6:.0f} MB/s per connection"] if args["bandwidth"] else []
    print(
        f"\n### Key index benchmark ({args['impl']}, {', '.join(injected) or 'nothing injected'}, 64 in parallel)\n"
    )
    print(
        "| Keys | Build | Build rate | Index size | Per entry (incl. filters) | Filters per entry | Files | Levels | Peak RSS |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for res in results:
        info = res["info"]
        print(
            f"| {info['n']:,} | {fmt_s(info['build_s'])} | {info['n'] / info['build_s'] / 1e6:.2f} M keys/s, "
            f"{info['bytes'] / info['build_s'] / 1e6:.0f} MB/s | {info['bytes'] / 1e6:,.1f} MB | "
            f"{info['b_per_entry']:.1f} B | {info['filter_b_per_entry']:.2f} B | {info['files']} | {info['depth']} | "
            f"{info.get('peak_rss_gb', 0):.1f} GB |"
        )
    ops = []
    for res in results:
        for r in res["rows"]:
            if r["op"] not in ops:
                ops.append(r["op"])
    print("\n| Operation | " + " | ".join(f"{res['info']['n']:,} keys" for res in results) + " |")
    print("|---|" + "---|" * len(results))
    for op in ops:
        cells = [cell(next((x for x in res["rows"] if x["op"] == op), None)) for res in results]
        print(f"| {op} | " + " | ".join(cells) + " |")

    shaped = [res for res in results if "shape" in res]
    if shaped:
        print("\nSteady-state shape (level: files, MB), before and after the compactions:\n")
        print("| Keys | Before | After |")
        print("|---|---|---|")
        for res in shaped:

            def fmt(lv):
                return " · ".join(
                    f"L{k}: {v[0]}, {v[1]:,.1f} MB" for k, v in sorted(lv.items(), key=lambda kv: int(kv[0]))
                )

            print(f"| {res['info']['n']:,} | {fmt(res['shape']['before'])} | {fmt(res['shape']['after'])} |")

    crossed = [res for res in results if res.get("crossover")]
    if crossed:
        print("\nRead strategy, forced each way (cold; wall · GETs · MB read):\n")
        print(
            "| Keys | Written keys | Unchanged | Whole levels | Tails, then blocks | Tails, then rest | "
            "Planner picks | Planner's estimate, blocks / rest |"
        )
        print("|---|---|---|---|---|---|---|---|")
        for res in crossed:
            for row in res["crossover"]:
                est = row["estimates"]
                estimate = f"{fmt_s(est['blocks'])} / {fmt_s(est['rest'])}" if est else "—"
                print(
                    f"| {res['info']['n']:,} | {row['k']:,} | {row.get('unchanged', 0):.0%} | {cell(row['whole'])} | "
                    f"{cell(row['blocks'])} | {cell(row['rest'])} | {row['picked']}: "
                    f"{cell(row['planner'])} | {estimate} |"
                )


def jsonable(results):
    for res in results:
        for r in res["rows"]:
            if not isinstance(r.get("out"), int):
                r.pop("out", None)
    return results


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7")
    ap.add_argument("--latency", type=float, default=0.03)
    ap.add_argument("--bandwidth", type=float, default=80e6)
    ap.add_argument("--max-replace", type=float, default=1e7)
    ap.add_argument("--s3", default=os.environ.get("SOLERA_TEST_S3", ""))
    ap.add_argument("--prefix", default=None)
    ap.add_argument("--suites", default=",".join(SUITES))
    ap.add_argument("--json", default=None, help="save the results here")
    ap.add_argument("--render", nargs="*", default=None, help="print a report from saved results")
    args = ap.parse_args()
    if args.render:
        saved = [json.load(open(p)) for p in args.render]
        report([res for s in saved for res in s["results"]], saved[0]["args"])
        return
    base = configure(args.s3) if args.s3 else ""
    args.prefix = base + (args.prefix or f"bench-keys-{uuid.uuid4().hex[:8]}/")
    args.suites = set(args.suites.split(","))
    results = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        t = time.perf_counter()
        results.append(await run_size(n, args))
        print(f"[{n:,} keys done in {time.perf_counter() - t:.0f} s]", flush=True)
    meta = {"impl": "native", "latency": args.latency, "bandwidth": args.bandwidth}
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"args": meta, "results": jsonable(results)}, f, indent=1, default=str)
    report(results, meta)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    asyncio.run(main())
