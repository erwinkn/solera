"""Scale pass (ERW-18, SOLE-17): today's in-place rebuild against the chosen
form, a shadow build with a copy at the switch, on one table at the scale
target (100M rows). Outside query cost is measured before, after today's
rebuild, and after the switch. Branch exp/table-swap.

    python bench6.py N
"""

from __future__ import annotations

import shutil
import sys
import time

import bench5
from bench import conn, fresh, out, rebuild, setup_other, size, sql, vacuum
from bench2 import COLS, gen, measure

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000_000


def disk():
    return round(shutil.disk_usage("/").free / 2**30, 1)


def main():
    with conn() as c:
        fresh(c)
        setup_other(c)
        sql(c, f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        sql(c, "CREATE INDEX t_copy_a ON t_copy (a)")
        c.commit()
        t = time.perf_counter()
        sql(c, gen("t_copy", n=N))
        c.commit()
        out(what="load", seconds=round(time.perf_counter() - t, 1), free_gb=disk())
    vacuum("s_other")
    out(what="vacuum after load", seconds=round(vacuum("t_copy"), 1))
    with conn() as c:
        sz = size(c, "t_copy")
        out(what="size", heap=sz[0], indexes=sz[1], total=sz[2], bytes_per_row=round(sz[2] / N, 1))
    measure("queries before", "t_copy")

    with conn() as c:
        out(what="today: rebuild in place", **rebuild(c, "t_copy", 1000), free_gb=disk())
    out(what="today: vacuum after the rebuild", seconds=round(vacuum("t_copy"), 1))
    measure("queries after today's rebuild", "t_copy")

    t = time.perf_counter()
    bench5.build("''", gen=2000)
    with conn() as c:
        sz = size(c, "solera.shadow")
        out(what="shadow size", total=sz[2], bytes_per_row=round(sz[2] / N, 1), free_gb=disk())
    bench5.blocked("''")
    out(what="shadow: build + switch, total", seconds=round(time.perf_counter() - t, 1), free_gb=disk())
    out(what="shadow: vacuum after the switch", seconds=round(vacuum("t_copy"), 1))
    measure("queries after the switch", "t_copy")


if __name__ == "__main__":
    main()
