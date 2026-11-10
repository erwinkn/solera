"""Scale pass (ERW-18, SOLE-17): today's in-place rebuild against the chosen
form, a shadow build with a copy at the switch, on one table at the scale
target (100M rows). Outside query cost is measured before, after today's
rebuild, and after the switch. Branch exp/table-swap.

    python bench6.py N [today|shadow|both]

BENCH_NARROW=1 leaves column c NULL and the table without its index on a,
so that 100M rows fit a 65 GB disk: the switch needs the old rows, the
shadow and the new rows on disk at once.
"""

from __future__ import annotations

import os
import shutil
import sys
import time

import bench
import bench2
import bench5
from bench import conn, fresh, key, out, rebuild, setup_other, size, sql, vacuum
from bench2 import COLS, gen, measure

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000_000
WHAT = sys.argv[2] if len(sys.argv) > 2 else "both"
NARROW = os.environ.get("BENCH_NARROW") == "1"

if NARROW:
    bench.rows = lambda lo, hi, g: [(key(i), (i * 31) % 1000003, i * 0.25, None, "") for i in range(lo, hi)]
    for q in ("index_only_count", "range_sum"):
        bench2.Q.pop(q)

    def gen(table, n=N, part="''", version=1):  # noqa: F811
        return (
            f"INSERT INTO {table} (k, a, b, c, part, _version) SELECT 'key' || lpad(i::text, 9, '0'), "
            f"(i * 7919) % 1000003, i * 0.5, NULL, {part}, {version} FROM generate_series(0::bigint, {n - 1}) i"
        )


def disk():
    return round(shutil.disk_usage("/").free / 2**30, 1)


def load():
    with conn() as c:
        fresh(c)
        setup_other(c)
        sql(c, f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        if not NARROW:
            sql(c, "CREATE INDEX t_copy_a ON t_copy (a)")
        c.commit()
        t = time.perf_counter()
        sql(c, gen("t_copy", n=N))
        c.commit()
        out(what="load", narrow=NARROW, seconds=round(time.perf_counter() - t, 1), free_gb=disk())
    vacuum("s_other")
    out(what="vacuum after load", seconds=round(vacuum("t_copy"), 1))
    with conn() as c:
        sz = size(c, "t_copy")
        out(what="size", heap=sz[0], indexes=sz[1], total=sz[2], bytes_per_row=round(sz[2] / N, 1))
    measure("queries before", "t_copy")


def today():
    with conn() as c:
        out(what="today: rebuild in place", **rebuild(c, "t_copy", 1000), free_gb=disk())
    out(what="today: vacuum after the rebuild", seconds=round(vacuum("t_copy"), 1))
    measure("queries after today's rebuild", "t_copy")


def shadow():
    t = time.perf_counter()
    bench5.build("''", gen=2000)
    with conn() as c:
        sz = size(c, "solera.shadow")
        out(what="shadow size", total=sz[2], bytes_per_row=round(sz[2] / N, 1), free_gb=disk())
    bench5.blocked("''")
    out(what="shadow: build + switch, total", seconds=round(time.perf_counter() - t, 1), free_gb=disk())
    out(what="shadow: vacuum after the switch", seconds=round(vacuum("t_copy"), 1))
    measure("queries after the switch", "t_copy")


def main():
    load()
    if WHAT in ("today", "both"):
        today()
    if WHAT in ("shadow", "both"):
        shadow()


if __name__ == "__main__":
    main()
