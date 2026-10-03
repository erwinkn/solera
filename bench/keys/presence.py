"""Presence at a position (docs/presence-at-position.md): what it costs to
know, for each key changed past a consumer's position, whether it existed
there.

    uv run python bench/keys/presence.py example
    uv run python bench/keys/presence.py run --sizes 1e6,1e8 --dir /tmp/presence
    uv run python bench/keys/presence.py retention --sizes 1e6,1e8

`example` runs the note's worked example through `KeyIndex.resolve` with and
without `exact`, and shows a Bloom false positive. `run` builds an index in
a local directory (obstore's LocalStore: request counts and bytes are exact,
wall times come from the injected 30 ms per request and 80 MB/s per
connection, as in bench.py), steady state, then measures:

- writes: 1K and 10K keys, update-heavy (existing keys) and insert-heavy
  (new keys), cold, as today (filters may infer an update) and exact;
- planning: a delta log of 10,000 commits of 1K keys (90% updates, 5%
  removes, 5% inserts or re-adds), written with exact predecessors; a
  consumer behind by 1, 100 and 10,000 commits classes every changed key
  through the delta log alone (`_native.presence`, the prototype), against
  the pass's own scan (`KeyIndex.pending`) and option B's extra lookups in
  the snapshot at its position.

`retention` replays the compaction planner (amplification.py's `Sim`) to
measure what pinning the index at a position keeps alive (option B).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import shutil
import sys
import time
from dataclasses import replace
from pathlib import Path

from obstore.store import LocalStore
from solera import _native
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
import bench  # noqa: E402
from bench import build, fill_upper, key_of  # noqa: E402

CLASSES = ("added", "updated", "removed", "neither")
CLASS_OF = {(False, True): 0, (True, True): 1, (True, False): 2, (False, False): 3}  # (at position, at head)
GET_PRICE, PUT_PRICE = 0.40 / 1e6, 5.0 / 1e6


def classes(counts) -> dict:
    return dict(zip(CLASSES, counts, strict=True))


async def blocks_of(io: ObjectIO, prefix: str, f: FileInfo) -> tuple[list[bytes], int]:
    """A delta file read whole (deltas are small): its blocks and codec."""

    data = await io.read_whole(f"{prefix}{f.name}.kx", f.size)
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    return [data[off : off + size] for _, off, size, _, _ in tail["blocks"]], tail["codec"]


async def presence(io: ObjectIO, prefix: str, state: IndexState, first: int, last: int, with_keys=False):
    """Every key changed in commits `[first, last]`, classed by the delta log
    alone: what the consumer at `first` needs."""

    logged = dict(state.log)
    files = [f for b in range(last, first - 1, -1) for f in logged[b]]  # newest first
    read = await asyncio.gather(*(blocks_of(io, prefix, f) for f in files))
    runs, codecs = [r[0] for r in read], [r[1] for r in read]
    counts, keys, cls = await asyncio.to_thread(_native.presence, runs, codecs, with_keys)
    return classes(counts), (list(zip(keys, cls, strict=True)) if with_keys else None)


def cold(root: Path, latency=0.03, bandwidth=80e6) -> ObjectIO:
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


async def measure(io: ObjectIO, fn) -> dict:
    io.metrics.reset()
    t, cpu = time.perf_counter(), time.process_time()
    out = await fn()
    m = io.metrics.snapshot()
    return {
        "wall": time.perf_counter() - t,
        "cpu": time.process_time() - cpu,
        "gets": m["gets"],
        "puts": m["puts"],
        "mb": m["bytes_in"] / 1e6,
        "cost": m["gets"] * GET_PRICE + m["puts"] * PUT_PRICE,
        "out": out,
    }


# -- the worked example ---------------------------------------------------------------


async def example(root: Path) -> None:
    """`items` holds a and b. Commits 1-4: add c; remove a; re-add a and
    update b; add d; remove d. Each consumer position classes the keys
    through the delta log; written exactly, and as a cold worker does
    today (the filters infer b's update, which then names no predecessor)."""

    shutil.rmtree(root, ignore_errors=True)
    # Filters on every level, as at 100M keys: nothing is small enough to read whole.
    opts = Options(whole_threshold=0, small_file=0, stream_density=math.inf, stream_reads=math.inf)
    commits = [
        ({b"c"}, set()),  # 1: add c
        (set(), {b"a"}),  # 2: remove a
        ({b"a", b"b"}, set()),  # 3: re-add a, update b
        ({b"d"}, set()),  # 4: add d
        (set(), {b"d"}),  # 5: remove d
    ]
    for exact in (True, False):
        io = cold(root / f"exact{exact}", latency=0, bandwidth=None)
        data = K.encode_file([b"a", b"b"], [1, 1], bytes(2))
        await io.write("x/base.kx", data)
        state = IndexState(count=2, files=(FileInfo.describe("base", 1, data),), prefix="x/")
        for n, (ups, rms) in enumerate(commits, start=1):
            run = K.SortedEntries.of(sorted(ups), None, sorted(rms))
            idx = KeyIndex(io, "x/", state, opts)
            files, _ = await idx.resolve(run, commit_number=n, attempt="a", generation=10 * n, exact=exact)
            state = state.committed(n, files, keep_log=True)
        print(f"\nexact={exact}: index count {state.count} (truth 3), exact count: {state.count_exact}")
        for first in range(1, 6):
            counts, keys = await presence(io, "x/", state, first, 5, with_keys=True)
            named = ", ".join(f"{k.decode()} {CLASSES[c]}" for k, c in keys)
            print(f"  position {first}: {named}")

    # A Bloom false positive: a brand-new key the key filter says may be there.
    io = cold(root / "fp", latency=0, bandwidth=None)
    present = [b"k%06d" % i for i in range(0, 200_000, 2)]
    data = K.encode_file(present, [1] * len(present), bytes(len(present)))
    await io.write("x/base.kx", data)
    state = IndexState(count=len(present), files=(FileInfo.describe("base", 1, data),), prefix="x/")
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    nbits, k, bits = tail["key_filter"]
    new = [b"k%06d" % i for i in range(1, 200_000, 2)]
    hits = K.bloom_check_keys(bits, nbits, k, new)
    fps = [key for key, h in zip(new, hits, strict=True) if h]
    print(f"\nfalse positives: {len(fps)} of {len(new)} new keys ({len(fps) / len(new):.3%})")
    for exact in (False, True):
        idx = KeyIndex(
            io,
            "x/",
            state,
            Options(whole_threshold=0, small_file=0, stream_density=math.inf, stream_reads=math.inf),
        )
        files, _ = await idx.resolve(
            K.SortedEntries.of([fps[0]]), commit_number=1, attempt=f"fp{exact}", generation=2, exact=exact
        )
        after = state.committed(1, files, keep_log=True)
        counts, _ = await presence(io, "x/", after, 1, 1)
        print(
            f"  exact={exact}: {fps[0].decode()} -> index count {after.count} (truth {len(present) + 1}), "
            f"delta log says {[c for c, v in counts.items() if v]}"
        )


# -- the measured runs -------------------------------------------------------------------


async def steady_index(root: Path, n: int, opts: Options):
    """The bench's index (bottom level, then levels 1..depth-1 filled as steady
    writes leave them) with seven 1K-key deltas in level 0; a sample of ids."""

    io = cold(root, latency=0, bandwidth=None)
    prefix = "idx/"
    every = max(1, n // 10_000_000)
    state, sample, _, build_s = await build(io, prefix, n, opts, sample_every=every, big_every=n)
    current = {i: (1, v) for i, v in sample}
    per_entry = sum(f.size for f in state.files) / n
    upper = await fill_upper(io, prefix, n, opts, state.depth, per_entry, current)
    state = replace(state, files=state.files + tuple(upper), prefix=prefix)
    rng = random.Random(5)
    ids = list(current)
    for c in range(7):
        keys = sorted(key_of(i) for i in rng.sample(ids, 1000))
        data = K.encode_file(keys, [50 + c] * len(keys), bytes(len(keys)), predecessors=[1] * len(keys))
        name = f"{c:012d}-steady"
        await io.write(f"{prefix}{name}.kx", data)
        state = replace(state, files=state.files + (FileInfo.describe(name, 0, data),))
    return state, ids, build_s


def new_key(rng: random.Random) -> bytes:
    return key_of(rng.randrange(10**12)) + b"n"  # sorts among the existing keys, never one of them


async def writes(root: Path, state: IndexState, ids: list[int], opts: Options) -> list[dict]:
    rows = []
    rng = random.Random(7)
    for k in (1_000, 10_000):
        for mix in ("update", "insert"):
            keys = (
                sorted(key_of(i) for i in rng.sample(ids, k))
                if mix == "update"
                else sorted({new_key(rng) for _ in range(k)})
            )
            for exact in (False, True):
                io = cold(root)
                idx = KeyIndex(io, state.prefix, state, opts)
                r = await measure(
                    io,
                    lambda idx=idx, keys=keys, exact=exact, mix=mix, k=k: idx.resolve(
                        K.SortedEntries.of(keys),
                        commit_number=10**9,
                        attempt=f"w{k}{mix}{exact}",
                        generation=99,
                        exact=exact,
                    ),
                )
                files, _ = r.pop("out")
                # What the delta says: an upsert naming no predecessor reads as an add.
                named = 0
                for f in files.files:
                    blocks, codec = await blocks_of(cold(root, 0, None), state.prefix, f)
                    for b in blocks:
                        named += sum(p is not None for p in K.decode_block(b, codec)[4])
                truth_added = k if mix == "insert" else 0
                rows.append(
                    {
                        "k": k,
                        "mix": mix,
                        "exact": exact,
                        "route": idx.route,
                        **r,
                        "count_added": files.added,
                        "count_wrong": abs(files.added - truth_added),
                        "log_wrong": abs((k - named) - truth_added),
                    }
                )
                print(json.dumps(rows[-1]), flush=True)
    return rows


async def delta_log(root: Path, state: IndexState, ids: list[int], commits: int, per: int):
    """`commits` deltas of `per` keys over the index — 90% updates, 5% removes,
    5% inserts (half re-adds of removed keys) — each entry naming its
    predecessor exactly, as a model of the keys knows it. Returns the state
    with the log, and the live sets at the positions measured."""

    io = cold(root, latency=0, bandwidth=None)
    rng = random.Random(11)
    gen = dict.fromkeys(ids, 1)  # live keys -> generation
    universe = [key_of(i) for i in ids]
    live = dict(zip(universe, (1 for _ in universe), strict=True))
    removed: list[bytes] = []
    marks = {commits - b + 1 for b in (1, 100, 10_000) if b <= commits}
    at: dict[int, set] = {}
    del gen
    for c in range(1, commits + 1):
        if c in marks:
            at[c] = set(live)
        g = 1000 + c
        chosen: dict[bytes, tuple[bool, int | None]] = {}
        while len(chosen) < per:
            x = rng.random()
            if x < 0.90 or (x < 0.95 and not live):
                key = universe[rng.randrange(len(universe))]
                if key in chosen or key not in live:
                    continue
                chosen[key] = (False, live[key])
            elif x < 0.95:
                key = universe[rng.randrange(len(universe))]
                if key in chosen or key not in live:
                    continue
                chosen[key] = (True, live[key])
            else:
                if removed and rng.random() < 0.5:
                    key = removed.pop(rng.randrange(len(removed)))
                    if key in chosen or key in live:
                        continue
                else:
                    key = new_key(rng)
                    universe.append(key)
                chosen[key] = (False, None)
        keys = sorted(chosen)
        for key in keys:
            deleted, _ = chosen[key]
            if deleted:
                live.pop(key)
                removed.append(key)
            else:
                live[key] = g
        data = K.encode_file(
            keys,
            [g] * len(keys),
            bytes(chosen[key][0] for key in keys),
            predecessors=[chosen[key][1] for key in keys],
        )
        name = f"{c:012d}-log"
        await io.write(f"{state.prefix}{name}.kx", data)
        state = replace(state, log=state.log + ((c, (FileInfo.describe(name, 0, data),)),))
        if c % 1000 == 0:
            print(f"  log: {c} commits", flush=True)
    return state, at, live


async def planning(root: Path, state: IndexState, ids: list[int], opts: Options, commits: int) -> list[dict]:
    t = time.perf_counter()
    logged, at, live_end = await delta_log(root, state, ids, commits, 1000)
    print(f"  log written in {time.perf_counter() - t:.0f} s", flush=True)
    rows = []
    for behind in (1, 100, 10_000):
        if behind > commits:
            continue
        first = commits - behind + 1
        io = cold(root)
        r = await measure(
            io, lambda first=first, io=io: presence(io, state.prefix, logged, first, commits, with_keys=True)
        )
        counts, keys = r.pop("out")
        # Checked against the model's live sets at the position and at the head.
        before = at[first]
        truth = [0, 0, 0, 0]
        for key, _ in keys:
            truth[CLASS_OF[(key in before, key in live_end)]] += 1
        assert classes(truth) == counts, (truth, counts)
        expected = len(live_end) - len(before)  # the count's change over the range
        assert counts["added"] - counts["removed"] == expected
        row = {"behind": behind, "what": "presence (delta log)", **r, **counts}
        rows.append(row)
        print(json.dumps(row), flush=True)

        # The pass's own scan of the same range, newest wins (what it reads anyway).
        io = cold(root)
        idx = KeyIndex(io, state.prefix, logged, opts)

        async def scan(idx=idx, first=first):
            after, n = None, 0
            while True:
                keys_, *_rest, after = await idx.pending(first, commits, after, 100_000)
                n += len(keys_)
                if after is None:
                    return n

        r = await measure(io, scan)
        r["entries"] = r.pop("out")
        rows.append({"behind": behind, "what": "pass scan (pending)", **r})
        print(json.dumps(rows[-1]), flush=True)

        # Option B: exact lookups of the changed keys in the index at the position.
        changed = [key for key, _ in keys]
        io = cold(root)
        snap = KeyIndex(io, state.prefix, state, opts)
        r = await measure(io, lambda snap=snap, changed=changed: snap.lookup(changed))
        found = r.pop("out")
        r["keys"] = len(changed)
        r["found"] = len(found)
        rows.append({"behind": behind, "what": "B: lookups in the snapshot", **r})
        print(json.dumps(rows[-1]), flush=True)
    return rows


async def run(args) -> None:
    out = {}
    for n in [int(float(s)) for s in args.sizes.split(",")]:
        root = Path(args.dir) / f"n{n}"
        shutil.rmtree(root, ignore_errors=True)
        opts = Options()
        t = time.perf_counter()
        state, ids, build_s = await steady_index(root, n, opts)
        levels = bench.levels(state)
        info = {
            "n": n,
            "build_s": time.perf_counter() - t,
            "levels": {lv: [f, round(mb, 1), e] for lv, (f, mb, e) in levels.items()},
            "sample": len(ids),
        }
        print(json.dumps(info), flush=True)
        out[n] = {"info": info, "writes": await writes(root, state, ids, opts)}
        out[n]["planning"] = await planning(root, state, ids, opts, args.commits)
        Path(args.json).write_text(json.dumps(out, indent=1, default=str))
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)


# -- option B's storage: what a pinned snapshot keeps alive ----------------------------------


def retention(args) -> None:
    """Pins taken every 500 commits over 10,000, after `warmup` commits of
    1K random keys; each is measured 1, 100 and 10,000 commits later: the
    bytes of its files compaction has since replaced, which the pin keeps."""

    from amplification import Sim

    offsets = (1, 100, 10_000)
    for n in [int(float(s)) for s in args.sizes.split(",")]:
        sim = Sim(n, Options(), "now")
        rng = random.Random(1)
        starts = [args.warmup + 500 * j for j in range(20)]
        pins: dict[int, dict[str, int]] = {}
        kept: dict[int, list[float]] = {o: [] for o in offsets}
        size = 0.0
        for c in range(starts[-1] + offsets[-1] + 1):
            if c in starts:
                pins[c] = {f.name: f.size for f in sim.state.files}
                size = sum(pins[c].values())
            for s0, files in pins.items():
                if c - s0 in offsets:
                    now = {f.name for f in sim.state.files}
                    kept[c - s0].append(sum(v for name, v in files.items() if name not in now) / size)
            sim.commit(c, 1000, rng)
        row = [f"{n:,}"] + [
            f"{sum(v) / len(v):.0%} mean, {max(v):.0%} max" for v in (kept[o] for o in offsets)
        ]
        print("| " + " | ".join(row) + " |", flush=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("example")
    e.add_argument("--dir", default="/tmp/presence-example")
    r = sub.add_parser("run")
    r.add_argument("--sizes", default="1e6,1e8")
    r.add_argument("--dir", default="/tmp/presence")
    r.add_argument("--commits", type=int, default=10_000)
    r.add_argument("--json", default="/tmp/presence.json")
    r.add_argument("--keep", action="store_true")
    t = sub.add_parser("retention")
    t.add_argument("--sizes", default="1e6,1e8")
    t.add_argument("--warmup", type=int, default=20_000)
    args = ap.parse_args()
    if args.cmd == "example":
        asyncio.run(example(Path(args.dir)))
    elif args.cmd == "run":
        asyncio.run(run(args))
    else:
        retention(args)


if __name__ == "__main__":
    main()
