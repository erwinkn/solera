"""Second pass (ERW-18, SOLE-17): what an outside query pays through the
side view, at rest and during a build, measured with nothing else running;
the view's plan when one table holds several partitions; and the rebuild's
write throughput against today's replacement write. Branch exp/table-swap.

    python bench2.py N [parts]
"""

from __future__ import annotations

import json
import statistics
import sys
import time

from bench import BATCH, FENCE, conn, out, rebuild, relid, sql, size, vacuum

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
WHAT = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else {"queries", "parts", "rebuild"}
COLS = "k text, a bigint, b double precision, c text, part text NOT NULL DEFAULT ''"

Q = {
    "point": "SELECT * FROM {v} WHERE k = 'key000123457'",
    "index_only_count": "SELECT count(*) FROM {v} WHERE a BETWEEN 1000 AND 51000",
    "range_sum": "SELECT count(*), sum(b) FROM {v} WHERE a BETWEEN 1000 AND 3000",
    "count": "SELECT count(*) FROM {v}",
    "group": "SELECT a % 100, avg(b) FROM {v} GROUP BY 1",
    "join": "SELECT count(*) FROM {v} x JOIN s_other o ON o.k = x.k",
}
REPS = {"point": 500, "index_only_count": 20, "range_sum": 100, "count": 5, "group": 5, "join": 5}


def gen(table, side=None, n=N, part="''", version=1):
    cols = "k, a, b, c, part, _version" + ("" if side is None else ", _side")
    extra = "" if side is None else f", {side}"
    return (f"INSERT INTO {table} ({cols}) SELECT 'key' || lpad(i::text, 9, '0'), (i * 7919) % 1000003, i * 0.5, "
            f"md5(i::text)::varchar(24), {part}, {version}{extra} FROM generate_series(0::bigint, {n - 1}) i")


def timed(view, q, reps):
    with conn(autocommit=True) as c:
        sql(c, q.format(v=view))
        runs = []
        for _ in range(3):
            t = time.perf_counter()
            for _ in range(reps):
                sql(c, q.format(v=view))
            runs.append((time.perf_counter() - t) / reps * 1e3)
        return round(statistics.median(runs), 3)


def measure(label, view):
    out(what=label, **{name: timed(view, q, REPS[name]) for name, q in Q.items()})


def explain(label, view, queries=Q):
    with conn(autocommit=True) as c:
        for name, q in queries.items():
            plan = [r[0] for r in sql(c, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + q.format(v=view))]
            out(what=f"plan {label}", query=name, plan=plan)


def queries():
    with conn() as c:
        for q in ("DROP VIEW IF EXISTS s_copy", "DROP SCHEMA IF EXISTS solera CASCADE", "CREATE SCHEMA solera",
                  "DROP TABLE IF EXISTS t_copy, s_other CASCADE", f"DROP TABLE IF EXISTS {FENCE}",
                  f"CREATE TABLE {FENCE} (relid oid NOT NULL, part text NOT NULL, generation bigint NOT NULL, "
                  "worker_id text NOT NULL, written bigint, live smallint, build smallint, build_gen bigint, "
                  "PRIMARY KEY (relid, part))",
                  "CREATE TABLE s_other (k text PRIMARY KEY)",
                  f"INSERT INTO s_other SELECT 'key' || lpad((i * 97)::text, 9, '0') FROM generate_series(0, {N // 100 - 1}) i",
                  f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))",
                  "CREATE INDEX t_copy_a ON t_copy (a)",
                  gen("t_copy"),
                  f"CREATE TABLE solera.s_copy ({COLS}, _side smallint NOT NULL, _version bigint, PRIMARY KEY (k, part, _side))",
                  "CREATE INDEX s_copy_a ON solera.s_copy (a)",
                  "CREATE INDEX s_copy_a_inc ON solera.s_copy (a) INCLUDE (_side)",
                  gen("solera.s_copy", side=0)):
            sql(c, q)
        rid = relid(c, "solera.s_copy")
        sql(c, f"INSERT INTO {FENCE} VALUES (%s, '', 1, 'w', 1, 0, NULL, NULL)", (rid,))
        sql(c, f"CREATE VIEW s_copy AS SELECT r.k, r.a, r.b, r.c, r.part, r._version FROM solera.s_copy r "
               f"WHERE r._side = (SELECT g.live FROM {FENCE} g WHERE g.relid = {rid} AND g.part = '')")
        c.commit()
    for t in ("s_other", "t_copy", "solera.s_copy"):
        vacuum(t)
    with conn() as c:
        for t in ("t_copy", "solera.s_copy"):
            sz = size(c, t)
            out(what="size", table=t, heap_bytes_per_row=round(sz[0] / N, 1), index_bytes_per_row=round(sz[1] / N, 1))
    # the INCLUDE index alone: does it bring back index-only scans through the view?
    with conn() as c:
        sql(c, "DROP INDEX solera.s_copy_a")
        c.commit()
    measure("today, table", "t_copy")
    measure("side, view, at rest", "s_copy")
    explain("today", "t_copy")
    explain("side at rest", "s_copy")
    with conn() as c:
        sql(c, gen("solera.s_copy", side=1, version=2))
        sql(c, f"UPDATE {FENCE} SET build = 1, build_gen = 2")
        c.commit()
    vacuum("solera.s_copy")
    measure("side, view, during a build (both sides whole)", "s_copy")
    explain("side during a build", "s_copy", {k: Q[k] for k in ("index_only_count", "count")})
    with conn() as c:
        t = time.perf_counter()
        sql(c, f"UPDATE {FENCE} SET live = 1, build = NULL WHERE build_gen = 2")
        c.commit()
        out(what="switch", seconds=round(time.perf_counter() - t, 4))
        t = time.perf_counter()
        sql(c, "DELETE FROM solera.s_copy WHERE part = '' AND _side = 0")
        c.commit()
        out(what="delete of the old side", seconds=round(time.perf_counter() - t, 1))
    out(what="vacuum after the delete", seconds=round(vacuum("solera.s_copy"), 1))
    measure("side, view, after the switch and cleanup", "s_copy")


def parts(p=10):
    """One table holding p partitions (partition_column): the view joins each
    row's partition to its fence row."""
    per = N // p
    with conn() as c:
        sql(c, "DROP VIEW IF EXISTS m_copy")
        sql(c, "DROP TABLE IF EXISTS solera.m_copy, m_plain")
        sql(c, f"CREATE TABLE solera.m_copy ({COLS}, _side smallint NOT NULL, _version bigint, PRIMARY KEY (k, part, _side))")
        sql(c, "CREATE INDEX m_copy_a ON solera.m_copy (a) INCLUDE (_side)")
        sql(c, f"CREATE TABLE m_plain ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        sql(c, "CREATE INDEX m_plain_a ON m_plain (a)")
        rid = relid(c, "solera.m_copy")
        for i in range(p):
            sql(c, gen("solera.m_copy", side=i % 2, n=per, part=f"'p{i}'"))
            sql(c, gen("m_plain", n=per, part=f"'p{i}'"))
            sql(c, f"INSERT INTO {FENCE} VALUES (%s, %s, 1, 'w', 1, %s, NULL, NULL)", (rid, f"p{i}", i % 2))
        sql(c, f"CREATE VIEW m_copy AS SELECT r.k, r.a, r.b, r.c, r.part, r._version FROM solera.m_copy r "
               f"WHERE EXISTS (SELECT 1 FROM {FENCE} g WHERE g.relid = {rid} AND g.part = r.part AND g.live = r._side)")
        c.commit()
    vacuum("solera.m_copy")
    vacuum("m_plain")
    vacuum(FENCE)
    pq = {
        "point_any_part": "SELECT * FROM {v} WHERE k = 'key000012345'",
        "point_in_part": "SELECT * FROM {v} WHERE part = 'p3' AND k = 'key000012345'",
        "count": "SELECT count(*) FROM {v}",
        "count_part": "SELECT count(*) FROM {v} WHERE part = 'p3'",
        "group_part": "SELECT part, count(*) FROM {v} GROUP BY 1",
        "index_only_count": "SELECT count(*) FROM {v} WHERE a BETWEEN 1000 AND 51000",
    }
    reps = {"point_any_part": 300, "point_in_part": 300, "count": 3, "count_part": 10, "group_part": 3,
            "index_only_count": 20}
    for label, v in (("partitions, plain table", "m_plain"), ("partitions, side view", "m_copy")):
        out(what=label, **{name: timed(v, q, reps[name]) for name, q in pq.items()})
    explain("partitions, side view", "m_copy", pq)


def rebuilds():
    """A whole full run of N rows in 10k-key batches: today's replacement write
    against the side's build, one after the other with nothing else running."""
    with conn() as c:
        sql(c, "DROP VIEW IF EXISTS s_copy")
        sql(c, "DROP TABLE IF EXISTS t_copy, solera.s_copy")
        sql(c, f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        sql(c, "CREATE INDEX t_copy_a ON t_copy (a)")
        sql(c, gen("t_copy"))
        sql(c, f"CREATE TABLE solera.s_copy ({COLS}, _side smallint NOT NULL, _version bigint, PRIMARY KEY (k, part, _side))")
        sql(c, "CREATE INDEX s_copy_a ON solera.s_copy (a) INCLUDE (_side)")
        sql(c, gen("solera.s_copy", side=0))
        sql(c, f"DELETE FROM {FENCE}")
        sql(c, f"INSERT INTO {FENCE} VALUES (%s, '', 1, 'w', 1, 0, NULL, NULL)", (relid(c, "solera.s_copy"),))
        c.commit()
    vacuum("t_copy")
    vacuum("solera.s_copy")
    with conn() as c:
        out(what="rebuild, today", **rebuild(c, "t_copy", 1000))
        out(what="rebuild, side", **rebuild(c, "solera.s_copy", 1000, side=0))


if __name__ == "__main__":
    if "queries" in WHAT:
        queries()
    if "parts" in WHAT:
        parts()
    if "rebuild" in WHAT:
        rebuilds()
