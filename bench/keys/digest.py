"""Row digests at 1M–10M rows: Python dicts against Arrow, time and peak memory.

    uv run python bench/keys/digest.py --sizes 1e6,1e7

Each case runs in a process of its own: it builds its rows, then — measured —
turns them into a key index's content (`key_rows`) and writes it as an
initial load into an empty index on local disk, so every row's version is
computed exactly once. Peak memory is what the operation adds on top of the
rows (the kernel's peak counter reset first). Rows have six columns: a key,
an integer, a float, a string, a list of strings and a timestamp; `flat`
rows leave the timestamp out (JSON values only).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bulk import memory, reset_peak  # noqa: E402
from obstore.store import LocalStore  # noqa: E402
from solera.keys.index import IndexState, KeyIndex  # noqa: E402
from solera.keys.io import ObjectIO  # noqa: E402
from solera.sdk import Output  # noqa: E402
from solera.stores import key_rows  # noqa: E402

CASES = ("dicts", "flat", "arrow")
EPOCH = dt.datetime(2026, 1, 1)


def dicts(n: int) -> list[dict]:
    return [
        {
            "id": f"k{(i * 7919) % n:09d}",
            "n": i,
            "x": i * 0.5,
            "name": f"name-{i % 1000}",
            "tags": ["a", "b"] if i % 2 else ["c"],
            "at": EPOCH + dt.timedelta(seconds=i),
        }
        for i in range(n)
    ]


def arrow(n: int):
    import pyarrow as pa

    ids = [f"k{(i * 7919) % n:09d}" for i in range(n)]
    return pa.table(
        {
            "id": ids,
            "n": pa.array(range(n), pa.int64()),
            "x": pa.array([i * 0.5 for i in range(n)], pa.float64()),
            "name": [f"name-{i % 1000}" for i in range(n)],
            "tags": pa.array([["a", "b"] if i % 2 else ["c"] for i in range(n)], pa.list_(pa.string())),
            "at": pa.array(
                [int(EPOCH.timestamp()) * 10**6 + i * 10**6 for i in range(n)], pa.timestamp("us")
            ),
        }
    )


async def one(case: str, n: int) -> dict:
    value = arrow(n) if case == "arrow" else dicts(n)
    if case == "flat":
        for row in value:
            del row["at"]
    out = Output("bench", key="id")
    with tempfile.TemporaryDirectory() as d:
        idx = KeyIndex(ObjectIO(LocalStore(d)), "keys/", IndexState())
        base, _ = memory()
        reset_peak()
        t = time.perf_counter()
        rows = await asyncio.to_thread(key_rows, value, out)
        files, _ = await idx.replace(rows, 0, "bench")
        wall = time.perf_counter() - t
        _, peak = memory()
    assert sum(f.entries for f in files.files) == n
    return {"case": case, "n": n, "wall": wall, "peak_gb": (peak - base) / 1e9, "input_gb": base / 1e9}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7")
    ap.add_argument("--cases", default=",".join(CASES))
    ap.add_argument("--one")
    ap.add_argument("--n", type=int)
    args = ap.parse_args()
    if args.one:
        print(json.dumps(asyncio.run(one(args.one, args.n))))
        return
    results = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        for case in args.cases.split(","):
            p = subprocess.run(
                [sys.executable, __file__, "--one", case, "--n", str(n)], capture_output=True, text=True
            )
            if p.returncode:
                print(f"[{case} at {n:,} failed]\n{p.stderr[-2000:]}", flush=True)
                continue
            results.append(json.loads(p.stdout.strip().splitlines()[-1]))
            print(json.dumps(results[-1]), flush=True)
    sizes = sorted({r["n"] for r in results})
    print("\n| Rows | " + " | ".join(f"{n:,}" for n in sizes) + " |\n|---|" + "---|" * len(sizes))
    for case in CASES:
        cells = []
        for n in sizes:
            r = next((r for r in results if r["case"] == case and r["n"] == n), None)
            cells.append("—" if r is None else f"{r['wall']:.1f} s · {r['peak_gb']:.2f} GB")
        print(f"| {case} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
