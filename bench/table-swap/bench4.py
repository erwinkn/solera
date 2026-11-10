"""Fourth pass (ERW-18, SOLE-17): a view that filters nothing at rest. The
fence row names the side outsiders must not see (hidden: the side being
built, or the old side until its delete finishes). The view is two branches:
with no hidden side anywhere in the table, the whole base table, unfiltered;
otherwise the rows whose (partition, side) is not hidden, an anti-join.
Each branch's condition is a one-time filter. Branch exp/table-swap.

    python bench4.py N
"""

from __future__ import annotations

import sys

from bench import FENCE, conn, out, relid, sql, vacuum
from bench2 import COLS, Q, explain, gen, measure, timed

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
P = 10


def view(name, base, rid):
    cols = "r.k, r.a, r.b, r.c, r.part, r._version"
    any_hidden = f"(SELECT EXISTS (SELECT 1 FROM {FENCE} g WHERE g.relid = {rid} AND g.hidden IS NOT NULL))"
    return (
        f"CREATE VIEW {name} AS SELECT {cols} FROM {base} r WHERE NOT {any_hidden} "
        f"UNION ALL SELECT {cols} FROM {base} r WHERE {any_hidden} AND NOT EXISTS "
        f"(SELECT 1 FROM {FENCE} g WHERE g.relid = {rid} AND g.part = r.part AND g.hidden = r._side)"
    )


PQ = {
    "point_any_part": "SELECT * FROM {v} WHERE k = 'key000012345'",
    "point_in_part": "SELECT * FROM {v} WHERE part = 'p3' AND k = 'key000012345'",
    "count": "SELECT count(*) FROM {v}",
    "count_part": "SELECT count(*) FROM {v} WHERE part = 'p3'",
    "group_part": "SELECT part, count(*) FROM {v} GROUP BY 1",
    "index_only_count": "SELECT count(*) FROM {v} WHERE a BETWEEN 1000 AND 51000",
}
PREPS = {"point_any_part": 300, "point_in_part": 300, "count": 3, "count_part": 10, "group_part": 3,
         "index_only_count": 20}


def main():
    with conn() as c:
        for q in ("DROP VIEW IF EXISTS s_copy, s2_copy, h_copy, hm_copy", "DROP SCHEMA IF EXISTS solera CASCADE",
                  "CREATE SCHEMA solera", "DROP TABLE IF EXISTS t_copy, s_other, m_plain CASCADE",
                  f"DROP TABLE IF EXISTS {FENCE}",
                  f"CREATE TABLE {FENCE} (relid oid NOT NULL, part text NOT NULL, generation bigint NOT NULL, "
                  "worker_id text NOT NULL, written bigint, live smallint, build smallint, build_gen bigint, "
                  "hidden smallint, PRIMARY KEY (relid, part))",
                  "CREATE TABLE s_other (k text PRIMARY KEY)",
                  f"INSERT INTO s_other SELECT 'key' || lpad((i * 97)::text, 9, '0') FROM generate_series(0, {N // 100 - 1}) i",
                  f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))",
                  "CREATE INDEX t_copy_a ON t_copy (a)",
                  gen("t_copy"),
                  f"CREATE TABLE solera.h_copy ({COLS}, _side smallint NOT NULL, _version bigint, PRIMARY KEY (k, part, _side))",
                  "CREATE INDEX h_copy_a ON solera.h_copy (a)",
                  gen("solera.h_copy", side=0)):
            sql(c, q)
        rid = relid(c, "solera.h_copy")
        sql(c, f"INSERT INTO {FENCE} VALUES (%s, '', 1, 'w', 1, 0, NULL, NULL, NULL)", (rid,))
        sql(c, view("h_copy", "solera.h_copy", rid))
        c.commit()
    for t in ("s_other", "t_copy", "solera.h_copy", FENCE):
        vacuum(t)
    measure("today, table", "t_copy")
    measure("hidden view, at rest", "h_copy")
    explain("hidden view, at rest", "h_copy")
    with conn() as c:
        sql(c, gen("solera.h_copy", side=1, version=2))
        sql(c, f"UPDATE {FENCE} SET build = 1, build_gen = 2, hidden = 1")
        c.commit()
    vacuum("solera.h_copy")
    measure("hidden view, during a build (both sides whole)", "h_copy")
    explain("hidden view, during a build", "h_copy", {k: Q[k] for k in ("point", "index_only_count", "count")})
    with conn() as c:
        sql(c, "DROP VIEW h_copy")
        sql(c, "DROP TABLE solera.h_copy")
        sql(c, f"CREATE TABLE solera.hm_copy ({COLS}, _side smallint NOT NULL, _version bigint, PRIMARY KEY (k, part, _side))")
        sql(c, "CREATE INDEX hm_copy_a ON solera.hm_copy (a)")
        sql(c, f"CREATE TABLE m_plain ({COLS}, _version bigint, PRIMARY KEY (k, part))")
        sql(c, "CREATE INDEX m_plain_a ON m_plain (a)")
        rid = relid(c, "solera.hm_copy")
        for i in range(P):
            sql(c, gen("solera.hm_copy", side=i % 2, n=N // P, part=f"'p{i}'"))
            sql(c, gen("m_plain", n=N // P, part=f"'p{i}'"))
            sql(c, f"INSERT INTO {FENCE} VALUES (%s, %s, 1, 'w', 1, %s, NULL, NULL, NULL)", (rid, f"p{i}", i % 2))
        sql(c, view("hm_copy", "solera.hm_copy", rid))
        c.commit()
    for t in ("solera.hm_copy", "m_plain", FENCE):
        vacuum(t)
    for label, v in (("partitions, plain table", "m_plain"), ("partitions, hidden view, at rest", "hm_copy")):
        out(what=label, **{n: timed(v, q, PREPS[n]) for n, q in PQ.items()})
    explain("partitions, hidden view, at rest", "hm_copy", PQ)
    with conn() as c:  # p3 rebuilds: its other side fills, and is hidden
        sql(c, gen("solera.hm_copy", side=0, n=N // P, part="'p3'", version=2))
        sql(c, f"UPDATE {FENCE} SET build = 0, build_gen = 2, hidden = 0 WHERE part = 'p3'")
        c.commit()
    vacuum("solera.hm_copy")
    out(what="partitions, hidden view, p3 building", **{n: timed("hm_copy", q, PREPS[n]) for n, q in PQ.items()})
    explain("partitions, hidden view, p3 building", "hm_copy", PQ)


if __name__ == "__main__":
    main()
