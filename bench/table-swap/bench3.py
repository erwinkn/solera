"""Third pass (ERW-18, SOLE-17): the side column first in the row and first in
every index, so an outside query through the view seeks its side instead of
filtering it. Branch exp/table-swap.

    python bench3.py N
"""

from __future__ import annotations

import sys

from bench import FENCE, conn, out, relid, sql, vacuum
from bench2 import COLS, Q, explain, gen, measure

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000


def main():
    with conn() as c:
        for q in ("DROP VIEW IF EXISTS s_copy, s2_copy", "DROP SCHEMA IF EXISTS solera CASCADE", "CREATE SCHEMA solera",
                  "DROP TABLE IF EXISTS t_copy, s_other CASCADE", f"DROP TABLE IF EXISTS {FENCE}",
                  f"CREATE TABLE {FENCE} (relid oid NOT NULL, part text NOT NULL, generation bigint NOT NULL, "
                  "worker_id text NOT NULL, written bigint, live smallint, build smallint, build_gen bigint, "
                  "PRIMARY KEY (relid, part))",
                  "CREATE TABLE s_other (k text PRIMARY KEY)",
                  f"INSERT INTO s_other SELECT 'key' || lpad((i * 97)::text, 9, '0') FROM generate_series(0, {N // 100 - 1}) i",
                  f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))",
                  "CREATE INDEX t_copy_a ON t_copy (a)",
                  gen("t_copy"),
                  f"CREATE TABLE solera.s2_copy (_side smallint NOT NULL, {COLS}, _version bigint, "
                  "PRIMARY KEY (_side, k, part))",
                  "CREATE INDEX s2_copy_a ON solera.s2_copy (_side, a)",
                  gen("solera.s2_copy", side=0)):
            sql(c, q)
        rid = relid(c, "solera.s2_copy")
        sql(c, f"INSERT INTO {FENCE} VALUES (%s, '', 1, 'w', 1, 0, NULL, NULL)", (rid,))
        sql(c, f"CREATE VIEW s2_copy AS SELECT r.k, r.a, r.b, r.c, r.part, r._version FROM solera.s2_copy r "
               f"WHERE r._side = (SELECT g.live FROM {FENCE} g WHERE g.relid = {rid} AND g.part = '')")
        c.commit()
    for t in ("s_other", "t_copy", "solera.s2_copy"):
        vacuum(t)
    with conn() as c:
        for t in ("t_copy", "solera.s2_copy"):
            heap = sql(c, "SELECT pg_relation_size(%s)", (t,))[0][0]
            idx = sql(c, "SELECT indexrelid::regclass::text, pg_relation_size(indexrelid) FROM pg_index "
                         "WHERE indrelid = %s::regclass", (t,))
            out(what="size", table=t, heap_bytes_per_row=round(heap / N, 1),
                indexes={n: round(s / N, 1) for n, s in idx})
    measure("today, table", "t_copy")
    measure("side first, view, at rest", "s2_copy")
    explain("side first, at rest", "s2_copy")
    with conn() as c:
        sql(c, gen("solera.s2_copy", side=1, version=2))
        sql(c, f"UPDATE {FENCE} SET build = 1, build_gen = 2")
        c.commit()
    vacuum("solera.s2_copy")
    measure("side first, view, during a build (both sides whole)", "s2_copy")
    explain("side first, during a build", "s2_copy", {k: Q[k] for k in ("index_only_count", "count", "group")})


if __name__ == "__main__":
    main()
