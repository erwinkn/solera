"""Bulk key index operations at 1M–100M keys: time and peak memory.

    uv run python bench/keys/bulk.py --sizes 1e6,1e7,1e8 --latency 0.03

The operations that touch every key: an initial load, a full replacement
(every key written again: a derived output's keys carry no payload), a
compaction that rewrites the bottom level, and a recount. The index they run on
holds every key in level 1 and, in level 0, a patch of 1% of them. Each runs in a process of its own after its input exists, and
reports the peak resident memory it added on top of that input — the data a
worker would already hold (the kernel's peak counter is reset first).

Input shapes: `list` is a Python list of keys (`Rows.keys`); `arrow` is a pyarrow
Table with a `k` column, read in place. Rows arrive shuffled unless `sorted`.
Keys are `cust-%013d` with random gaps, as in bench.py.

Uses bench.py's server (`--s3`, default the local MinIO) and latency model.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from solera.keys import Rows, SortedEntries  # noqa: E402
from solera.keys.index import IndexState, KeyIndex, Options  # noqa: E402
from solera.keys.io import ObjectIO  # noqa: E402

import bench as B  # noqa: E402

CASES = (
    "load-list",
    "load-arrow",
    "load-arrow-sorted",
    "replace-list",
    "replace-arrow",
    "compact",
    "recount",
)


def keys_sql(n: int, shuffled: bool, where: str = "true") -> str:
    gap = max(2, 10**12 // n)
    key = f"'cust-' || lpad((i * {gap} + hash(i) % {gap})::varchar, 13, '0')"
    order = "ORDER BY hash(i * 7 + 3)" if shuffled else "ORDER BY i"
    return f"SELECT k FROM (SELECT i, {key} AS k FROM range({n}) t(i)) WHERE {where} {order}"


def arrow_table(n: int, shuffled: bool):
    import duckdb

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    return con.sql(keys_sql(n, shuffled)).fetch_arrow_table()


def key_list(n: int, shuffled: bool, where: str = "true") -> list[bytes]:
    import duckdb

    rel = duckdb.connect().sql(keys_sql(n, shuffled, where))
    out = []
    while chunk := rel.fetchmany(1_000_000):
        out += [k.encode() for (k,) in chunk]
    return out


def memory() -> tuple[int, int]:
    """Resident and peak resident bytes of this process."""

    fields = {}
    with open("/proc/self/status") as f:
        for line in f:
            name, _, value = line.partition(":")
            fields[name] = value.strip()
    return int(fields["VmRSS"].split()[0]) * 1024, int(fields["VmHWM"].split()[0]) * 1024


def reset_peak() -> None:
    with open("/proc/self/clear_refs", "w") as f:
        f.write("5")


# -- one case, in its own process -------------------------------------------------------------


async def one(case: str, n: int, prefix: str, state_file: str | None, args) -> dict:
    opts = Options()
    io = ObjectIO(B.store(), latency=args.latency, bandwidth=args.bandwidth)
    state = IndexState.from_json(json.load(open(state_file))) if state_file else IndexState()
    load = case.startswith("load")
    idx = KeyIndex(io, f"{prefix}{case}/" if load else prefix, state, opts)
    replacing = case.startswith("replace")

    # The input, before measuring.
    if case.endswith("-list"):
        keys = key_list(n, shuffled=True)
    elif "-arrow" in case:
        table = arrow_table(n, shuffled=not case.endswith("sorted"))

    async def run():
        if case.endswith("-list"):
            rows = Rows.keys(keys)
        elif "-arrow" in case:
            rows = Rows.arrow(table, "k")
        if load or replacing:
            files, _ = await idx.replace(rows, 2, case, generation=3)
            return files
        if case == "compact":
            return await idx.compact((state.level(0) + state.level(1), 1))
        return await idx.recount()

    base, _ = memory()
    reset_peak()
    io.metrics.reset()
    t = time.perf_counter()
    out = await run()
    wall = time.perf_counter() - t
    _, peak = memory()
    m = io.metrics.snapshot()
    result = {
        "case": case,
        "n": n,
        "wall": wall,
        "peak_gb": (peak - base) / 1e9,
        "input_gb": base / 1e9,
        "gets": m["gets"],
        "puts": m["puts"],
        "mb_in": m["bytes_in"] / 1e6,
        "mb_out": m["bytes_out"] / 1e6,
    }
    if case == "recount":
        assert out == n, (out, n)
    elif case == "compact":
        result["files"] = len(out[0])
    else:
        files = out.files
        result["files"] = len(files)
        result["entries"] = sum(f.entries for f in files)
        assert abs(result["entries"] - n) < max(10, n // 20), (result["entries"], n)
    return result


# -- the driver ---------------------------------------------------------------------------------


async def build(n: int, prefix: str, path: str) -> None:
    """The index the replacement, compaction and recount run on: every key at
    generation 1 in level 1, then a patch of 1% of them (ids ending in 00) at
    generation 2 in level 0."""

    io = ObjectIO(B.store())
    opts = Options()
    idx = KeyIndex(io, prefix, IndexState(), opts)
    files, _ = await idx.replace(Rows.arrow(arrow_table(n, False), "k"), 0, "build", generation=1)
    state = IndexState().committed(0, files, keep_log=False)
    idx = KeyIndex(io, prefix, state, opts)
    patch = SortedEntries.of(key_list(n, False, "k LIKE '%00'"))
    files, _ = await idx.resolve(patch, batch=1, attempt="delta", generation=2)
    state = state.committed(1, files, keep_log=False)
    with open(path, "w") as f:
        json.dump(state.to_json(), f)


def run_case(case, n, prefix, state_file, args) -> dict | None:
    cmd = [sys.executable, __file__, "--one", case, "--n", str(n), "--prefix", prefix]
    cmd += ["--latency", str(args.latency), "--bandwidth", str(args.bandwidth)]
    if args.s3:
        cmd += ["--s3", args.s3]
    if state_file:
        cmd += ["--state", state_file]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        print(f"[{case} at {n:,}: failed]\n{p.stderr[-2000:]}", flush=True)
        return None
    return json.loads(p.stdout.strip().splitlines()[-1])


def report(results) -> None:
    sizes = sorted({r["n"] for r in results})
    print("\n| Operation | " + " | ".join(f"{n:,} keys" for n in sizes) + " |")
    print("|---|" + "---|" * len(sizes))
    for case in CASES:
        cells = []
        for n in sizes:
            r = next((r for r in results if r["case"] == case and r["n"] == n), None)
            if r is None:
                cells.append("—")
                continue
            up = f" ↑{r['mb_out']:.0f} MB" if r["mb_out"] >= 1 else ""
            cells.append(
                f"{B.fmt_s(r['wall'])} · **{r['peak_gb']:.2f} GB** · {r['gets']} GET {r['puts']} PUT"
                f" · {r['mb_in']:.0f} MB{up}"
            )
        if any(c != "—" for c in cells):
            print(f"| {case} | " + " | ".join(cells) + " |")
    print(
        "\nInput held before each operation (GB): "
        + ", ".join(f"{r['case']} at {r['n']:,}: {r['input_gb']:.1f}" for r in results)
    )


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,1e7")
    ap.add_argument("--cases", default=",".join(CASES))
    ap.add_argument("--s3", default=os.environ.get("SOLERA_TEST_S3"))
    ap.add_argument("--latency", type=float, default=0.03)
    ap.add_argument("--bandwidth", type=float, default=80e6)
    ap.add_argument("--json")
    ap.add_argument("--one")
    ap.add_argument("--n", type=int)
    ap.add_argument("--prefix")
    ap.add_argument("--state")
    args = ap.parse_args()
    base = B.configure(args.s3) if args.s3 else ""
    if args.one:
        print(json.dumps(await one(args.one, args.n, args.prefix, args.state, args)))
        return
    results = []
    for n in (int(float(x)) for x in args.sizes.split(",")):
        prefix = f"{base}bench-bulk-{uuid.uuid4().hex[:8]}/n{n}/"
        state_file = f"/tmp/bench-bulk-{uuid.uuid4().hex[:8]}.json"
        try:
            cases = args.cases.split(",")
            if {"replace-list", "replace-arrow", "compact", "recount"} & set(cases):
                t = time.perf_counter()
                await build(n, prefix, state_file)
                print(f"[{n:,}: index built in {time.perf_counter() - t:.0f} s]", flush=True)
            for case in cases:
                if Rows is None and "arrow" in case:
                    continue
                needs = case.startswith(("replace", "compact", "recount"))
                r = run_case(case, n, prefix, state_file if needs else None, args)
                if r is not None:
                    results.append(r)
                    print(json.dumps(r), flush=True)
        finally:
            B.clear_prefix(prefix)
            if os.path.exists(state_file):
                os.unlink(state_file)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=1)
    report(results)


if __name__ == "__main__":
    asyncio.run(main())
