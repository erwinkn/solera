"""Fifth pass (ERW-18, SOLE-17): build in a shadow table, copy at the switch.
The user's table stays a plain table, read by outsiders as today. A full run
writes its batches into a shadow table (one per output partition, primary
key only); Solera reads the shadow while it exists. The switch is one
transaction on the plain table: delete the partition's rows, insert the
shadow's, then drop the shadow. MVCC keeps outsiders on the old rows until
it commits; no lock it takes conflicts with a reader. Branch exp/table-swap.

    python bench5.py N [parts]
"""

from __future__ import annotations

import sys
import threading
import time

from bench import BATCH, FENCE, conn, out, rebuild, relid, sql, vacuum, wal, wal_bytes
from bench2 import COLS, gen

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
P = int(sys.argv[2]) if len(sys.argv) > 2 else 1


def setup():
    with conn() as c:
        for q in ("DROP VIEW IF EXISTS s_copy, s2_copy, h_copy, hm_copy", "DROP SCHEMA IF EXISTS solera CASCADE",
                  "CREATE SCHEMA solera", "DROP TABLE IF EXISTS t_copy, s_other, m_plain CASCADE",
                  f"DROP TABLE IF EXISTS {FENCE}",
                  f"CREATE TABLE {FENCE} (relid oid NOT NULL, part text NOT NULL, generation bigint NOT NULL, "
                  "worker_id text NOT NULL, written bigint, live smallint, build smallint, build_gen bigint, PRIMARY KEY (relid, part))",
                  f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))",
                  "CREATE INDEX t_copy_a ON t_copy (a)"):
            sql(c, q)
        for i in range(P):
            sql(c, gen("t_copy", n=N // P, part=f"'p{i}'" if P > 1 else "''"))
        c.commit()
    vacuum("t_copy")


def build(part, gen=1000):
    with conn() as c:
        sql(c, "DROP TABLE IF EXISTS solera.shadow")
        sql(c, f"CREATE TABLE solera.shadow ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        c.commit()
        r = rebuild(c, "solera.shadow", gen, delete_keys=True, hi=N // P)
    if part != "''":  # rebuild() writes part ''; restamp for a partitioned table
        with conn() as c:
            sql(c, f"UPDATE solera.shadow SET part = {part}")
            c.commit()
    out(what=f"build in the shadow ({N // P} rows)", **r)


def switch(part):
    with conn() as c:
        w0, t = wal(c), time.perf_counter()
        sql(c, f"DELETE FROM t_copy WHERE part = {part}")
        t1 = time.perf_counter()
        sql(c, f"INSERT INTO t_copy SELECT * FROM solera.shadow WHERE part = {part}")
        t2 = time.perf_counter()
        c.commit()
        t3 = time.perf_counter()
        sql(c, "DROP TABLE solera.shadow")
        c.commit()
        return dict(delete_s=round(t1 - t, 1), insert_s=round(t2 - t1, 1), commit_s=round(t3 - t2, 2),
                    drop_s=round(time.perf_counter() - t3, 2), wal_mb=round(wal_bytes(c, w0) / 2**20))


def blocked(part):
    """The switch with a 10 s outside query open, and a point query and a
    count issued while the switch runs."""
    long_c = conn()
    sql(long_c, "SELECT k FROM t_copy LIMIT 1")
    res = {}

    def do():
        res.update(switch(part))

    th = threading.Thread(target=do)
    t0 = time.perf_counter()
    th.start()
    time.sleep(2.0)
    with conn(autocommit=True) as c:
        t = time.perf_counter()
        v = sql(c, "SELECT _version FROM t_copy WHERE k = 'key000000007' LIMIT 1")
        res["point_during_switch_s"] = round(time.perf_counter() - t, 3)
        res["point_reads_version"] = v[0][0] if v else None
        t = time.perf_counter()
        n = sql(c, f"SELECT count(*), max(_version) FROM t_copy WHERE part = {part}")[0]
        res["count_during_switch_s"] = round(time.perf_counter() - t, 3)
        res["count_reads"] = [n[0], n[1]]
    time.sleep(max(0.0, 10.0 - (time.perf_counter() - t0)))
    long_c.commit()
    long_c.close()
    th.join()
    res["switch_total_s"] = round(time.perf_counter() - t0, 1)
    out(what="switch under a 10 s outside query", **res)
    with conn(autocommit=True) as c:
        n = sql(c, f"SELECT count(*), min(_version), max(_version) FROM t_copy WHERE part = {part}")[0]
        out(what="after the switch", rows=n[0], versions=[n[1], n[2]])


def main():
    setup()
    part = "'p3'" if P > 1 else "''"
    t = time.perf_counter()
    build(part)
    blocked(part)
    out(what="build + switch, total", seconds=round(time.perf_counter() - t, 1))
    out(what="vacuum after the switch", seconds=round(vacuum("t_copy"), 1))


if __name__ == "__main__":
    main()
