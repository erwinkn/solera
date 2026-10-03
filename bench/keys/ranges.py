"""Range files over the delta log (docs/presence-at-position.md, "Range
files"): what a consumer's catch-up costs read from a tree of merged ranges,
against one merge over every per-commit delta, and one packed object.

    uv run python bench/keys/ranges.py --sizes 1e6,1e8 --commits 12000

A delta log of `--commits` commits of 1K keys (90% updates, 5% removes, 5%
adds, half of them re-adds) over an index of n keys, each entry naming its
predecessor exactly (option A). The index's own files play no part: a
catch-up reads only the log. The tree: block (j, i) covers commits
[i·2^j, (i+1)·2^j - 1]; level 0 is the per-commit deltas, and each block
above merges its two children (`_native.merge_ranges`). A consumer behind
by 1, 100 and 10,000 commits reads the head's greedy decomposition into
aligned blocks, and its classes are checked key for key against the
per-commit merge's. Local object store, 30 ms per request and 80 MB/s per
connection injected (bench.py's model).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import shutil
import sys
import time
from pathlib import Path

from solera import _native
from solera import keys as K
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options

sys.path.insert(0, str(Path(__file__).parent))
from presence import blocks_of, classes, cold, measure, new_key  # noqa: E402

from bench import key_of  # noqa: E402

PER = 1000


def blocks_in(data: bytes) -> tuple[list[bytes], int]:
    footer = K.parse_footer(data[-K.FOOTER_SIZE :])
    tail = K.parse_tail(data[footer["filters_offset"] :], len(data))
    return [data[off : off + size] for _, off, size, _, _ in tail["blocks"]], tail["codec"]


def write_log(n: int, commits: int, seed: int = 11):
    """The per-commit deltas' bytes, oldest first. The index holds n keys,
    live at generation 1 unless the log changed them."""

    rng = random.Random(seed)
    gap = 10**12 // n
    state: dict[bytes, int | None] = {}  # changed keys: generation, None once removed
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
                chosen[key] = (x >= 0.90, was)  # 5% removes
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


def decompose(first: int, last: int) -> list[tuple[int, int]]:
    """[first, last] as aligned blocks (j, i), oldest first: from each point
    the largest block starting there that ends by `last`."""

    out, p = [], first
    while p <= last:
        j = 0
        while p % (1 << (j + 1)) == 0 and p + (1 << (j + 1)) - 1 <= last:
            j += 1
        out.append((j, p >> j))
        p += 1 << j
    return out


def run_of(files: list[bytes]) -> tuple[list[bytes], int]:
    """A block's files (non-overlapping, in key order) as one merge run."""

    blocks, codec = [], 1
    for data in files:
        b, codec = blocks_in(data)
        blocks += b
    return blocks, codec


async def size_run(n: int, commits: int, root: Path) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    io = cold(root, latency=0, bandwidth=None)
    t = time.perf_counter()
    deltas = write_log(n, commits)
    log_s = time.perf_counter() - t
    tree: dict[tuple[int, int], list[FileInfo]] = {}
    raw = {}  # (j, i) -> file bytes, for the level being built from
    for i, data in enumerate(deltas):
        name = f"r00-{i:06d}.0000"
        await io.write(f"log/{name}.kx", data)
        tree[(0, i)] = [FileInfo.describe(name, 0, data)]
        raw[(0, i)] = [data]
    delta_bytes = sum(len(d) for d in deltas)

    # Erwin's packed object: every delta back to back, with its offsets.
    packed = b"".join(deltas)
    offsets = [0]
    for d in deltas:
        offsets.append(offsets[-1] + len(d))
    await io.write("log/packed.bin", packed)
    del packed

    # The tree, level by level: each block merges its two children, newest first.
    levels = [
        {"level": 0, "blocks": commits, "entries": commits * PER, "mb": delta_bytes / 1e6, "build_s": 0.0}
    ]
    j = 0
    while (commits >> (j + 1)) > 0:
        j += 1
        built, entries, nbytes, cpu = {}, 0, 0, 0.0
        for i in range(commits >> j):
            left, right = raw[(j - 1, 2 * i)], raw[(j - 1, 2 * i + 1)]
            runs = [run_of(right), run_of(left)]
            t = time.process_time()
            files = await asyncio.to_thread(_native.merge_ranges, [r[0] for r in runs], [r[1] for r in runs])
            cpu += time.process_time() - t
            infos = []
            for m, data in enumerate(files):
                name = f"r{j:02d}-{i:06d}.{m:04d}"
                await io.write(f"log/{name}.kx", data)
                infos.append(FileInfo.describe(name, 0, data))
            tree[(j, i)] = infos
            built[(j, i)] = files
            entries += sum(f.entries for f in infos)
            nbytes += sum(f.size for f in infos)
        raw = {**{k: v for k, v in raw.items() if k[0] != j - 1}, **built}
        levels.append(
            {"level": j, "blocks": commits >> j, "entries": entries, "mb": nbytes / 1e6, "build_s": cpu}
        )
        print(json.dumps(levels[-1]), flush=True)
    del raw

    head = commits - 1
    rows = []
    for behind in (b for b in (1, 100, 10_000) if b <= commits):
        first = head - behind + 1
        parts = decompose(first, head)
        row = {"behind": behind, "files": len(parts)}

        # One merge over every per-commit delta (pending, fixed to merge once).
        io = cold(root)
        files = [f for c in range(head, first - 1, -1) for f in tree[(0, c)]]

        async def per_commit(io=io, files=files):
            read = await asyncio.gather(*(blocks_of(io, "log/", f) for f in files))
            return await asyncio.to_thread(_native.presence, [r[0] for r in read], [r[1] for r in read], True)

        r = await measure(io, per_commit)
        truth = r.pop("out")
        row["one merge"] = r

        # The same deltas from the packed object: one ranged read.
        io = cold(root)

        async def packed_read(io=io, first=first):
            data = await io.read("log/packed.bin", offsets[first], offsets[head + 1], offsets[-1])
            runs = [
                blocks_in(data[offsets[c] - offsets[first] : offsets[c + 1] - offsets[first]])
                for c in range(head, first - 1, -1)
            ]
            return await asyncio.to_thread(_native.presence, [x[0] for x in runs], [x[1] for x in runs], True)

        r = await measure(io, packed_read)
        assert r.pop("out")[0] == truth[0]
        row["packed"] = r

        # The range tree: the decomposition's blocks, newest first.
        io = cold(root)
        rfiles = [tree[p] for p in reversed(parts)]

        async def ranged(io=io, rfiles=rfiles):
            runs = []
            for fs in rfiles:
                read = await asyncio.gather(*(blocks_of(io, "log/", f) for f in fs))
                runs.append(([b for blocks, _ in read for b in blocks], read[0][1]))
            return await asyncio.to_thread(_native.presence, [x[0] for x in runs], [x[1] for x in runs], True)

        r = await measure(io, ranged)
        got = r.pop("out")
        assert got[0] == truth[0], (got[0], truth[0])
        assert list(got[1]) == list(truth[1]) and bytes(got[2]) == bytes(truth[2]), "classes differ"
        row["ranges"] = r
        row["classes"] = classes(truth[0])

        # A keys= read-ahead of 100 changed keys: filters and blocks of each file.
        sample = random.Random(behind).sample(list(truth[1]), min(100, len(truth[1])))
        for label, fs in (("keys, per commit", [[f] for f in files]), ("keys, ranges", rfiles)):
            io = cold(root)
            state = IndexState(files=tuple(f for group in fs for f in group), prefix="log/")
            # Level 0 is newest first by name: per-commit names sort by commit, ranges by start.
            idx = KeyIndex(io, "log/", state, Options())
            r = await measure(io, lambda idx=idx, sample=sample: idx.lookup(sample))
            r.pop("out")
            row[label] = r
        print(json.dumps(row), flush=True)
        rows.append(row)

    # Storage kept for one consumer `behind` back, the others at the head
    # (whose position is head + 1). Two rules: a block that starts before the
    # oldest position goes (no reader can use it whole); a block whose parent
    # is built goes, unless a position lies strictly inside the parent.
    storage = {}
    for behind in (b for b in (1, 100, 10_000) if b <= commits):
        pos = [head - behind + 1, head + 1]
        lo = min(pos)
        kept = set()
        for j, i in tree:
            if i << j < lo:
                continue
            parent = (j + 1, i >> 1)
            ps, pe = parent[1] << (j + 1), ((parent[1] + 1) << (j + 1)) - 1
            if parent in tree and not any(ps < p <= pe for p in pos):
                continue
            kept.add((j, i))
        for p in pos:
            if p <= head:
                assert set(decompose(p, head)) <= kept, "a decomposition lost a block"
        size = sum(f.size for b in kept for f in tree[b])
        raw_log = sum(f.size for c in range(lo, head + 1) for f in tree[(0, c)])
        storage[behind] = {"kept_mb": size / 1e6, "raw_log_mb": raw_log / 1e6, "blocks": len(kept)}
    out = {"n": n, "commits": commits, "log_s": log_s, "levels": levels, "rows": rows, "storage": storage}
    print(json.dumps(storage), flush=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e8")
    ap.add_argument("--commits", type=int, default=12_000)
    ap.add_argument("--dir", default="/tmp/ranges")
    ap.add_argument("--json", default="/tmp/ranges.json")
    args = ap.parse_args()
    results = []
    for n in [int(float(s)) for s in args.sizes.split(",")]:
        results.append(asyncio.run(size_run(n, args.commits, Path(args.dir) / f"n{n}")))
        Path(args.json).write_text(json.dumps(results, indent=1, default=str))
        shutil.rmtree(Path(args.dir) / f"n{n}", ignore_errors=True)
    print(json.dumps([{**r, "rows": None} for r in results], default=str)[:2000])


if __name__ == "__main__":
    main()
