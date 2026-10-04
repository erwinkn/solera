"""The span key index as built (docs/key-index-design.md), on real files
through `KeyIndex`: writes, upkeep's merges, then cold reads checked key
by key.

    uv run python bench/keys/spanbench.py --size 1e6 --commits 12000 --dir /tmp/spanbench

Build, in one process with no injected latency: a base of n keys
(generation 1), then `--commits` commits of 1K keys — 90% updates, 5%
removes, 5% adds — each resolved by `KeyIndex.resolve` and installed as
its span; with `--large-every`, a commit of `--large` updates that often.
Consumers read every 1, 360 and 8,640 commits (a day of 10 s commits):
each holds its `next` as a live endpoint, moved to the head + 1 when it
reads. Four readers stay 1, 100, 360 and 10,000 commits behind the head,
each reserved at the head + 1 when its distance comes up. After every
commit, upkeep's merge policy (`plan_merge`) runs until it plans nothing,
as an upkeep that keeps up would. The fold — each key's generation, 0 when
absent — is kept as an array by key id (`key` is a bijection), so the
check holds at 100M keys.

Read, cold, each reader in a fresh process (30 ms per request, 80 MB/s per
connection, 64 in parallel): `changes(P → H)` 100K keys a page — first
page, full read, GETs, MB, peak RSS above the process's baseline — every
key and class checked against the fold; then a commit of 1K random
updates resolved (exact: every key's block read), 1K exact lookups at the
head and a 100K-key page mid-index, each checked.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from obstore.store import LocalStore
from solera.keys import Rows, SortedEntries
from solera.keys.index import DeltaFiles, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

PER = 1000
PAGE = 100_000
BEHIND = (1, 100, 360, 10_000)
PERIODS = (1, 360, 8640)
PREFIX = "keys/bench/_/"
MUL, MOD = 2_654_435_761, 10**13
INV = pow(MUL, -1, MOD)


def key(i: int) -> bytes:
    return b"cust-%013d" % (i * MUL % MOD)


def key_id(k: bytes) -> int:
    return int(k[5:]) * INV % MOD


def values(ids: np.ndarray) -> np.ndarray:
    """The numeric part of each id's key: keys sort as these do."""

    return ids.astype(np.uint64) * np.uint64(MUL) % np.uint64(MOD)  # below 2**64 for ids under 6.9e9


def peak_bytes() -> int:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r if sys.platform == "darwin" else r * 1024


CODECS = {"zlib": 1, "zstd": 2}  # each at level 1


def options(root: Path) -> Options:
    """The writer's options a build used: its codec and block size."""

    o = json.loads((root / "options.json").read_text())
    return Options(block_size=o["block_size"], codec=CODECS[o["codec"]], level=1)


async def build(
    root: Path, n: int, commits: int, seed: int, large: int, large_every: int, backfill: int = 0
) -> dict:
    rng = np.random.default_rng(seed)
    io = ObjectIO(LocalStore(str(root / "store"), mkdir=True))
    opts = options(root)
    state = IndexState(prefix=PREFIX)
    capacity = n + commits * (PER // 20) + 1
    gen = np.zeros(capacity, dtype=np.uint32)  # the fold: each id's generation, 0 when absent
    t0 = time.perf_counter()
    if backfill:  # an empty output, filled `backfill` new keys a commit
        state = state.committed(0, DeltaFiles([], 0, 0, 1))
        present, live_n, nxt_id = np.empty(n, dtype=np.int64), 0, 0
    else:
        chunks = []  # 10M keys a chunk: a string array holds at most 2 GiB
        for lo in range(0, n, 10_000_000):
            ids = np.arange(lo, min(n, lo + 10_000_000))
            digits = pc.utf8_lpad(pc.cast(pa.array(values(ids)), pa.string()), 13, "0")
            chunks.append(pc.binary_join_element_wise("cust-", digits, ""))
        table = pa.table({"k": pa.chunked_array(chunks)})
        files, _ = await KeyIndex(io, None, state, opts).replace(
            Rows.arrow(table, "k"), 0, "base", generation=1
        )
        del table
        state = state.committed(0, files)
        gen[:n] = 1
        present = np.arange(n, dtype=np.int64)  # ids live, unordered
        live_n, nxt_id = n, n
    position = dict.fromkeys(PERIODS, 1)  # each consumer's `next`
    readers: dict[int, int] = {}  # distance -> reserved P
    delta_bytes = merge_bytes = merges = delta_entries = merge_entries = 0
    spans_max = 0
    print(f"  base: {time.perf_counter() - t0:.0f} s", flush=True)
    for c in range(1, commits + 1):
        for d in BEHIND:
            if commits - c + 1 == d:  # the head is c - 1: reserve c, d commits behind the end
                readers[d] = c
                np.save(root / f"before-{c}.npy", gen)
        g = c + 1
        k = large if large_every and c % large_every == 0 else PER
        rm_n = add_n = 0 if k != PER else PER // 20
        if backfill:
            k, rm_n, add_n = min(backfill, n - nxt_id), 0, min(backfill, n - nxt_id)
        pick = (
            np.unique(rng.integers(0, max(live_n, 1), size=k - add_n)) if k > add_n else np.empty(0, np.int64)
        )
        rng.shuffle(pick)
        upd, rm = present[pick[rm_n:]], present[pick[:rm_n]]
        add = np.arange(nxt_id, nxt_id + add_n, dtype=np.int64)
        nxt_id += add_n
        for i in sorted(pick[:rm_n].tolist(), reverse=True):  # swap-remove
            present[i] = present[live_n - 1]
            live_n -= 1
        if live_n + add_n > len(present):
            present = np.concatenate([present, np.empty(max(add_n, len(present) // 8), dtype=np.int64)])
        present[live_n : live_n + add_n] = add
        live_n += add_n
        ups = sorted(key(int(i)) for i in np.concatenate([upd, add]))
        rms = sorted(key(int(i)) for i in rm)
        files, _ = await KeyIndex(io, None, state, opts).resolve(
            SortedEntries.of(ups, None, rms), commit_number=c, attempt=f"a{c}", generation=g
        )
        delta_bytes += sum(f.size for f in files.files)
        delta_entries += sum(f.entries for f in files.files)
        state = state.committed(c, files)
        gen[upd] = g
        gen[add] = g
        gen[rm] = 0
        for p in PERIODS:
            if c % p == 0:
                position[p] = c + 1
        endpoints = set(position.values()) | set(readers.values())
        while True:
            idx = KeyIndex(io, None, state, opts)
            plan = idx.plan_merge(endpoints)
            if plan is None:
                break
            out = await idx.merge(plan, endpoints)
            if out is None:
                break
            state = state.merged(out.inputs, out.span)
            merge_bytes += out.span.size
            merge_entries += out.written
            merges += 1
        spans_max = max(spans_max, len(state.spans))
        if c % 1000 == 0:
            print(
                f"  commit {c}: {len(state.spans)} spans, {sum(f.size for f in state.files) / 1e6:.0f} MB, "
                f"{merges} merges, {time.perf_counter() - t0:.0f} s",
                flush=True,
            )
    (root / "state.json").write_text(json.dumps(state.to_json()))
    np.save(root / "head.npy", gen)
    (root / "readers.json").write_text(json.dumps(readers))
    return {
        "n": n,
        "commits": commits,
        "spans": len(state.spans),
        "spans_max": spans_max,
        "index_mb": sum(f.size for f in state.files) / 1e6,
        "entries": sum(f.entries for f in state.files),
        "live": int(live_n),
        "delta_mb": delta_bytes / 1e6,
        "merge_mb": merge_bytes / 1e6,
        "write_amp": (delta_bytes + merge_bytes) / max(delta_bytes, 1),
        "merge_entry_writes": merge_entries / max(delta_entries, 1),  # per entry committed
        "merge_byte_writes": merge_bytes / max(delta_bytes, 1),
        "merges": merges,
        "build_s": time.perf_counter() - t0,
    }


async def read(root: Path, what: str, at: int | None) -> dict:
    state = IndexState.from_json(json.loads((root / "state.json").read_text()))
    now = np.load(root / "head.npy")
    head = state.head
    io = ObjectIO(LocalStore(str(root / "store")), latency=0.03, bandwidth=80e6, concurrency=64)
    idx = KeyIndex(io, None, state, options(root))
    rng = np.random.default_rng(5)
    base = peak_bytes()
    t = time.perf_counter()
    first = None
    if what == "changes":
        then = np.load(root / f"before-{at}.npy")
        n_got = bad = 0
        seen = np.zeros(len(now), dtype=bool)
        async for page in idx.changes(at, head, limit=PAGE):
            first = first or time.perf_counter() - t
            ids = np.fromiter((key_id(k) for k in page.keys), dtype=np.int64, count=len(page.keys))
            classes = np.frombuffer(page.classes, dtype=np.uint8)
            gens = np.array(page.generations, dtype=np.uint64)
            deleted = np.frombuffer(page.deleted, dtype=np.uint8).astype(bool)
            was, is_ = then[ids] > 0, now[ids] > 0
            want = np.where(was, np.where(is_, 1, 2), np.where(is_, 0, 3))
            changed = then[ids] != now[ids]
            # A key changed and put back since P may be listed: its state at the head must hold.
            bad += int(np.count_nonzero(changed & (classes != want)))
            bad += int(np.count_nonzero(np.where(deleted, now[ids] != 0, gens != now[ids])))
            seen[ids] = True
            n_got += len(ids)
        wall = time.perf_counter() - t
        bad += int(np.count_nonzero((then != now) & ~seen))  # every changed key delivered
        out = {"keys": n_got, "mismatches": bad}
    elif what == "write":  # a commit of 1K random updates, resolved cold (not uploaded)
        live = np.flatnonzero(now)
        ups = sorted(key(int(i)) for i in rng.choice(live, PER, replace=False))
        delta = await idx.delta(SortedEntries.of(ups), generation=2**40)
        wall = time.perf_counter() - t
        out = {"keys": len(ups), "mismatches": int(delta.removed != 0 or delta.added != 0)}
    elif what == "lookups":
        probe = sorted({key(int(i)) for i in rng.integers(0, len(now), size=PER)})
        hits = await idx.lookup(probe)
        wall = time.perf_counter() - t
        bad = sum(1 for k in probe if (hits.get(k) or (0,))[0] != now[key_id(k)])
        out = {"keys": len(probe), "mismatches": bad}
    else:  # a page mid-index at the head
        live = np.flatnonzero(now)
        vals = np.sort(values(live))
        after = b"cust-%013d" % int(vals[len(vals) // 2])
        t = time.perf_counter()
        keys, gens, _, nxt = await idx.page(after, PAGE)
        wall = time.perf_counter() - t
        want = [b"cust-%013d" % int(v) for v in vals[len(vals) // 2 + 1 : len(vals) // 2 + 1 + PAGE]]
        # A page may end early with a cursor; what it holds is the view's next keys.
        bad = int(keys != want[: len(keys)] or (len(keys) < len(want) and nxt is None))
        bad += sum(1 for k, g in zip(keys, gens, strict=True) if now[key_id(k)] != g)
        out = {"keys": len(keys), "mismatches": bad}
    m = io.metrics.snapshot()
    return {
        **out,
        "what": what,
        "at": at,
        "first_s": first if first is not None else wall,
        "wall_s": wall,
        "gets": m["gets"],
        "mb_in": m["bytes_in"] / 1e6,
        "peak_mb": max(0, peak_bytes() - base) / 1e6,
    }


def isolated(root: Path, what: str, at: int | None) -> dict:
    out = subprocess.run(
        [sys.executable, __file__, "--read", str(root), what, str(at if at is not None else -1)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", default="1e6")
    ap.add_argument("--commits", type=int, default=12_000)
    ap.add_argument("--large", type=int, default=0, help="keys of the large commits")
    ap.add_argument("--large-every", type=int, default=0)
    ap.add_argument(
        "--backfill", type=int, default=0, help="fill an empty output, this many new keys a commit"
    )
    ap.add_argument("--codec", choices=sorted(CODECS), default="zlib", help="blocks' codec, at level 1")
    ap.add_argument(
        "--block-size", type=int, default=64 * 1024, help="bytes of entries a block holds before compression"
    )
    ap.add_argument("--dir", default="/tmp/spanbench")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--read", nargs=3)
    ap.add_argument("--no-reads", action="store_true", help="build only")
    ap.add_argument("--reads", help="read a finished build's directory")
    args = ap.parse_args()
    if args.read:
        root, what, at = args.read
        print(json.dumps(asyncio.run(read(Path(root), what, None if int(at) < 0 else int(at)))))
        return
    if args.reads:
        root = Path(args.reads)
        built = json.loads((root / "built.json").read_text())
        n = built["n"]
    else:
        n = int(float(args.size))
        if args.backfill:
            args.commits = -(-n // args.backfill)
        root = Path(args.dir) / (
            f"{n}-{args.commits}-{args.large}x{args.large_every}-b{args.backfill}-{args.codec}-{args.block_size >> 10}k"
        )
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True)
        (root / "options.json").write_text(json.dumps({"codec": args.codec, "block_size": args.block_size}))
        built = asyncio.run(
            build(root, n, args.commits, args.seed, args.large, args.large_every, args.backfill)
        )
        (root / "built.json").write_text(json.dumps(built))
        print(json.dumps(built))
        if args.no_reads:
            return
    readers = json.loads((root / "readers.json").read_text())
    rows = [isolated(root, "changes", p) for _, p in sorted(readers.items(), key=lambda kv: int(kv[0]))]
    rows += [isolated(root, w, None) for w in ("write", "lookups", "page")]
    print(
        f"\n{n:,} keys, {built['commits']:,} commits ({json.loads((root / 'options.json').read_text())}): {built['spans']} spans (at most {built['spans_max']}), "
        f"{built['index_mb']:.0f} MB ({built['entries']:,} entries for {built['live']:,} live), "
        f"merges wrote {built['merge_entry_writes']:.1f}× the entries committed ({built['merge_byte_writes']:.1f}× the bytes; "
        f"{built['merges']} merges), build {built['build_s']:.0f} s\n"
    )
    print("| Read | Keys | First page | Full read | GETs | MB | Peak RSS | Mismatches |")
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        label = (
            f"changes({r['at']} → head), {built['commits'] + 1 - r['at']} behind"
            if r["what"] == "changes"
            else r["what"]
        )
        print(
            f"| {label} | {r['keys']:,} | {r['first_s']:.2f} s | {r['wall_s']:.2f} s | {r['gets']:,} | "
            f"{r['mb_in']:.1f} | {r['peak_mb']:.0f} MB | {r['mismatches']} |"
        )


if __name__ == "__main__":
    main()
