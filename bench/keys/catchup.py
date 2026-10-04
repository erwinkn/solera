"""Catch-up over real spans (docs/key-index-design.md), paged, against
packed deltas with one resumable merge and the range tree's aligned blocks.

    uv run python bench/keys/catchup.py --sizes 1e6,1e8 --commits 12000

A log of `--commits` commits of 1K keys (90% updates, 5% removes, 5% adds,
half of them re-adds) over n keys, every entry naming its predecessor
exactly. Four consumers sit 1, 100, 360 and 10,000 commits behind the head;
their positions are endpoints. Three layouts of the same log:

- `spans`: after every commit, the span policy runs on real files
  (`_native.merge_ranges`): the newest window of 4 adjacent spans whose
  largest holds at most the other three combined merges, never across an
  endpoint (the commits before the oldest endpoint are left out: the base
  plays no part in a catch-up);
- `packed`: the deltas of [position, head] back to back in one object,
  read in `RANGE`-sized range reads, merged once (a resumable merge holds one
  cursor per delta);
- `aligned`: the range tree, fanout 8: block (l, i) covers commits
  [i 8^l, (i+1) 8^l - 1]; a consumer reads the greedy decomposition.

Spans and aligned blocks are paged 100K keys at a time through
`KeyIndex._scan` (each a sorted run, newest first; a page fetches only the
blocks it needs). Every layout's classes are checked against the per-commit
merge (`_native.presence`). Local store, 30 ms per request, 80 MB/s per
connection, 64 in parallel.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import resource
import shutil
import sys
import time
from pathlib import Path

from obstore.store import LocalStore
from solera import _native
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

sys.path.insert(0, str(Path(__file__).parent))
from bench import key_of  # noqa: E402

PER = 1000
PAGE = 100_000
BEHIND = (1, 100, 360, 10_000)


def cold(root: Path, latency=0.03, bandwidth=80e6) -> ObjectIO:
    return ObjectIO(LocalStore(prefix=str(root), mkdir=True), latency=latency, bandwidth=bandwidth)


def new_key(rng: random.Random) -> bytes:
    return key_of(rng.randrange(10**12)) + b"n"


def write_log(n: int, commits: int, seed: int = 11) -> list[bytes]:
    rng = random.Random(seed)
    gap = 10**12 // n
    state: dict[bytes, int | None] = {}  # changed keys: generation, None once removed (else live at 1)
    removed: list[bytes] = []
    out = []
    for c in range(commits):
        g = 1000 + c
        chosen: dict[bytes, tuple[bool, int | None]] = {}
        while len(chosen) < PER:
            x = rng.random()
            if x < 0.95:
                key = key_of(rng.randrange(n) * gap)
                was = state.get(key, 1)
                if key in chosen or was is None:
                    continue
                chosen[key] = (x >= 0.90, was)
            elif removed and rng.random() < 0.5:
                key = removed.pop(rng.randrange(len(removed)))
                if key in chosen or state.get(key, 1) is not None:
                    continue
                chosen[key] = (False, None)
            else:
                chosen[key := new_key(rng)] = (False, None)
        keys = sorted(chosen)
        for key in keys:
            deleted, _ = chosen[key]
            state[key] = None if deleted else g
            if deleted:
                removed.append(key)
        out.append(
            K.encode_file(
                keys,
                [g] * len(keys),
                bytes(chosen[k][0] for k in keys),
                predecessors=[chosen[k][1] for k in keys],
            )
        )
    return out


def blocks_in(data: bytes) -> tuple[list[bytes], int]:
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    return [data[off : off + size] for _, off, size, _, _ in tail["blocks"]], tail["codec"]


def run_of(files: list[bytes]) -> tuple[list[bytes], int]:
    blocks, codec = [], 1
    for data in files:
        b, codec = blocks_in(data)
        blocks += b
    return blocks, codec


def merge(files_newest_first: list[list[bytes]]) -> list[bytes]:
    runs = [run_of(fs) for fs in files_newest_first]
    return _native.merge_ranges([r[0] for r in runs], [r[1] for r in runs])


def entries(files: list[bytes]) -> int:
    return sum(K.parse_footer(d[-K.FOOTER_SIZE :])["entries"] for d in files)


class Spans:
    """The span policy on real files: spans as (a, b, files), oldest first."""

    def __init__(self, endpoints: set[int]):
        self.endpoints = endpoints
        self.spans: list[tuple[int, int, list[bytes]]] = []
        self.written = self.committed = 0

    def add(self, c: int, data: bytes) -> None:
        self.spans.append((c, c, [data]))
        self.committed += entries([data])
        while True:
            sizes = [entries(s[2]) for s in self.spans]
            for j in range(len(self.spans) - 4, -1, -1):
                w = sizes[j : j + 4]
                inner = {s[0] for s in self.spans[j + 1 : j + 4]}
                if max(w) <= sum(w) - max(w) and not inner & self.endpoints:
                    group = self.spans[j : j + 4]
                    out = merge([s[2] for s in reversed(group)])
                    self.written += entries(out)
                    self.spans[j : j + 4] = [(group[0][0], group[-1][1], out)]
                    break
            else:
                return


def decompose(first: int, last: int, f: int = 8) -> list[tuple[int, int]]:
    out, p = [], first
    while p <= last:
        j = 0
        while p % f ** (j + 1) == 0 and p + f ** (j + 1) - 1 <= last:
            j += 1
        out.append((j, p // f**j))
        p += f**j
    return out


async def put_run(io: ObjectIO, prefix: str, name: str, files: list[bytes]) -> list[FileInfo]:
    infos = []
    for i, d in enumerate(files):
        nm = f"{name}.{i:04d}"
        await io.write(f"{prefix}{nm}.kx", d)
        infos.append(FileInfo.describe(nm, 0, d))
    return infos


async def page_all(io: ObjectIO, prefix: str, runs: list[list[FileInfo]]) -> tuple[int, int]:
    """Page every key of `runs` (newest first) 100K at a time; returns (keys, pages)."""

    state = IndexState(files=tuple(f for r in runs for f in r), prefix=prefix)
    idx = KeyIndex(io, prefix, state, Options())
    after, keys, pages = None, 0, 0
    while True:
        got, _, _, _, after = await idx._scan(runs, after, PAGE, drop_deleted=False)
        keys += len(got)
        pages += 1
        if after is None:
            return keys, pages


async def measure(io: ObjectIO, fn) -> dict:
    io.metrics.reset()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t = time.perf_counter()
    out = await fn()
    m = io.metrics.snapshot()
    return {
        "wall": time.perf_counter() - t,
        "gets": m["gets"],
        "mb": m["bytes_in"] / 1e6,
        "rss_growth_mb": (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - rss) / 2**20,
        "out": out,
    }


async def size_run(n: int, commits: int, root: Path) -> list[dict]:
    shutil.rmtree(root, ignore_errors=True)
    t = time.perf_counter()
    deltas = write_log(n, commits)
    print(f"  {n:,}: log of {commits:,} commits in {time.perf_counter() - t:.0f} s", flush=True)
    head = commits - 1
    positions = {b: head - b + 1 for b in BEHIND}
    io = cold(root, latency=0, bandwidth=None)
    prefix = "x/"

    # Spans, built commit by commit from the oldest endpoint on.
    t = time.perf_counter()
    oldest = min(positions.values())
    sp = Spans(set(positions.values()))
    for c in range(oldest, commits):
        sp.add(c, deltas[c])
    build_s = time.perf_counter() - t
    span_files = []
    for a, b, files in sp.spans:
        span_files.append((a, b, await put_run(io, prefix, f"s{a:06d}-{b:06d}", files)))
    print(
        f"  spans: {len(sp.spans)} after {commits - oldest:,} commits, written {sp.written / sp.committed:.1f}x, "
        f"built in {build_s:.0f} s",
        flush=True,
    )

    # Aligned blocks, fanout 8, built bottom up over the whole log.
    t = time.perf_counter()
    level = {(0, c): [d] for c, d in enumerate(deltas)}
    lv = 0
    while 8 ** (lv + 1) <= commits:
        for i in range(commits // 8 ** (lv + 1)):
            level[(lv + 1, i)] = merge([level[(lv, 8 * i + j)] for j in range(7, -1, -1)])
        lv += 1
    aligned_written = sum(entries(fs) for (lv_, _), fs in level.items() if lv_ > 0) / sp.committed
    print(f"  aligned: {lv} levels built in {time.perf_counter() - t:.0f} s", flush=True)

    rows = []
    for b in BEHIND:
        p = positions[b]
        # Truth: the per-commit merge's classes.
        runs = [blocks_in(deltas[c]) for c in range(head, p - 1, -1)]
        truth, _, _ = _native.presence([r[0] for r in runs], [r[1] for r in runs], False)

        layouts = {}
        mine = [s for s in span_files if s[1] >= p]
        assert mine[0][0] == p, "a position is a span start"
        layouts["spans"] = [s[2] for s in reversed(mine)]
        blocks = decompose(p, head)
        al = []
        for lv_, i in reversed(blocks):
            al.append(await put_run(io, prefix, f"a{lv_}-{i:06d}", level[(lv_, i)]))
        layouts["aligned"] = al

        for name, rs in layouts.items():
            got = [run_of([await io.read_whole(f"{prefix}{f.name}.kx", f.size) for f in r]) for r in rs]
            counts, _, _ = _native.presence([g[0] for g in got], [g[1] for g in got], False)
            assert counts == truth, (name, counts, truth)
            c = cold(root)
            r = await measure(c, lambda c=c, rs=rs: page_all(c, prefix, rs))
            keys, pages = r.pop("out")
            row = {
                "n": n,
                "behind": b,
                "layout": name,
                "runs": len(rs),
                "files": sum(len(x) for x in rs),
                "entries": sum(f.entries for x in rs for f in x),
                "keys": keys,
                "pages": pages,
                **r,
            }
            rows.append(row)
            print(json.dumps(row), flush=True)

        # Packed: one object, 16 MB range reads, one merge.
        packed = b"".join(deltas[c] for c in range(p, commits))
        await io.write(f"{prefix}packed-{b}.bin", packed)
        packed_size = len(packed)
        c = cold(root)

        async def packed_read(c=c, p=p, size=packed_size, b=b):
            data = await c.read_whole(f"{prefix}packed-{b}.bin", size)  # range reads of RANGE bytes
            offs, at = [], 0
            for cc in range(p, commits):
                offs.append((at, at + len(deltas[cc])))
                at += len(deltas[cc])
            rs = [blocks_in(data[a:z]) for a, z in reversed(offs)]
            return await asyncio.to_thread(_native.presence, [r[0] for r in rs], [r[1] for r in rs], False)

        r = await measure(c, packed_read)
        counts, _, _ = r.pop("out")
        assert counts == truth
        row = {"n": n, "behind": b, "layout": "packed", "runs": commits - p, "files": 1, "entries": None, **r}
        rows.append(row)
        print(json.dumps(row), flush=True)
    for row in rows:
        row["spans_written"] = sp.written / sp.committed
        row["aligned_written"] = aligned_written
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--commits", type=int, default=12_000)
    ap.add_argument("--dir", default="/tmp/catchup")
    args = ap.parse_args()
    rows = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        rows += asyncio.run(size_run(n, args.commits, Path(args.dir) / str(n)))
    print("\n| Keys | Behind | Layout | Runs read | Files | GETs | MB read | Wall | RSS growth |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(
            f"| {r['n']:,} | {r['behind']:,} | {r['layout']} | {r['runs']:,} | {r['files']:,} | {r['gets']:,} | "
            f"{r['mb']:,.1f} | {r['wall']:.2f} s | {r['rss_growth_mb']:,.0f} MB |"
        )
    w = rows[0]
    print(
        f"\nWritten per entry committed: spans {w['spans_written']:.1f}x, aligned {w['aligned_written']:.1f}x"
    )


if __name__ == "__main__":
    main()
