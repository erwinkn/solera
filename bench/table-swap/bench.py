"""Throwaway prototypes of four ways a full run can rebuild a Postgres table
(ERW-18, SOLE-17). Never merged: branch exp/table-swap.

  today  the table in place: the first batch deletes the partition, the
         batches write into it (PostgresStore's replacement write)
  side   a base table with a _side smallint column; the user's name is a
         view showing, per partition, only the published side, read from
         the fence row; the rebuild writes the other side and the switch
         is a one-row UPDATE of the fence row
  part   the user's name is a partitioned table (LIST on the partition
         column); the rebuild fills a standalone table; the switch is
         DETACH old + ATTACH new
  rename the rebuild fills a shadow table; the switch renames it over the
         table

Every write transaction is shaped like PostgresStore's: take the fence row,
delete the batch's keys, COPY its rows, count the keys written.

    python bench.py N [variants]   # N rows (default 10M); prints JSON lines
"""

from __future__ import annotations

import json
import os
import random
import sys
import threading
import time

import psycopg

DSN = os.environ.get("BENCH_DSN", "postgresql://postgres:pg@localhost:5432/bench")
N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
ONLY = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else {"today", "side", "part", "rename"}
BATCH = 10_000
COLS = "k text, a bigint, b double precision, c text, part text NOT NULL DEFAULT ''"
FENCE = "public.solera_generations"


def out(**kw):
    print(json.dumps(kw), flush=True)


def conn(autocommit=False):
    return psycopg.connect(DSN, autocommit=autocommit)


def sql(c, q, *a):
    with c.cursor() as cur:
        cur.execute(q, *a)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return None


def wal(c):
    return sql(c, "SELECT pg_current_wal_lsn()")[0][0]


def wal_bytes(c, lo):
    return sql(c, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (lo,))[0][0]


def size(c, rel):
    return sql(
        c,
        "SELECT pg_relation_size(%s), pg_indexes_size(%s), pg_total_relation_size(%s)",
        (rel, rel, rel),
    )[0]


def key(i):
    return f"key{i:09d}"


def fresh(c):
    for q in (
        "DROP SCHEMA IF EXISTS solera CASCADE",
        "CREATE SCHEMA solera",
        "DROP VIEW IF EXISTS s_copy, user_view CASCADE",
        "DROP TABLE IF EXISTS t_copy, s_other CASCADE",
        "DROP TABLE IF EXISTS p_copy, p_copy_old, p_copy_new, r_copy, r_copy_old, r_copy_new CASCADE",
        f"DROP TABLE IF EXISTS {FENCE}",
        f"CREATE TABLE {FENCE} (relid oid NOT NULL, part text NOT NULL, generation bigint NOT NULL, "
        "worker_id text NOT NULL, written bigint, live smallint, build smallint, build_gen bigint, "
        "PRIMARY KEY (relid, part))",
    ):
        sql(c, q)
    c.commit()


def load_old(c, table, side=None):
    """The table as the last run left it: N rows at version 1."""
    sel = (
        "SELECT 'key' || lpad(i::text, 9, '0'), (i * 7919) % 1000003, i * 0.5, "
        "md5(i::text)::varchar(24), '', 1" + ("" if side is None else f", {side}")
    )
    cols = "k, a, b, c, part, _version" + ("" if side is None else ", _side")
    t = time.perf_counter()
    sql(c, f"INSERT INTO {table} ({cols}) {sel} FROM generate_series(0::bigint, {N - 1}) i")
    c.commit()
    return time.perf_counter() - t


def vacuum(table):
    with conn(autocommit=True) as c:
        t = time.perf_counter()
        sql(c, f"VACUUM ANALYZE {table}")
        return time.perf_counter() - t


def take(cur, relid, gen, part=""):
    """PostgresStore._take, with the side columns returned (one statement)."""
    return cur.execute(
        f"INSERT INTO {FENCE} (relid, part, generation, worker_id, written) VALUES (%s, %s, %s, 'w', %s) "
        f"ON CONFLICT (relid, part) DO UPDATE SET generation = EXCLUDED.generation, written = EXCLUDED.written "
        f"WHERE {FENCE}.generation <= EXCLUDED.generation RETURNING live, build",
        (relid, part, gen, gen),
    ).fetchone()


def rows(lo, hi, gen):
    return [(key(i), (i * 31) % 1000003, i * 0.25, f"v{gen}-{i:020d}", "") for i in range(lo, hi)]


def write_batch(c, table, relid, gen, lo, hi, reset=False, side=None, delete_keys=True):
    """One batch's write transaction, as PostgresStore writes it."""
    rs = rows(lo, hi, gen)
    keys = [r[0] for r in rs]
    with c.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock_shared(hashtext(%s))", (f"solera-domain:{table}",))
        live, build = take(cur, relid, gen) or (None, None)
        where = "part = ''"
        s = None
        if side is not None:
            if reset:
                build = 0 if live is None else 1 - live
                cur.execute(
                    f"UPDATE {FENCE} SET build = %s, build_gen = %s WHERE relid = %s AND part = ''",
                    (build, gen, relid),
                )
                cur.execute(f"DELETE FROM {table} WHERE {where} AND _side = %s", (build,))
            s = build if build is not None else live
            where += f" AND _side = {s}"
        elif reset:
            cur.execute(f"DELETE FROM {table} WHERE {where}")
        if delete_keys and not reset:
            cur.execute(f"DELETE FROM {table} WHERE {where} AND k::text = ANY(%s)", (keys,))
        cols = "k, a, b, c, part, _version" + ("" if side is None else ", _side")
        stamp = [gen] + ([] if side is None else [s])
        with cur.copy(f"COPY {table} ({cols}) FROM STDIN") as cp:
            for r in rs:
                cp.write_row([*r, *stamp])
        n = cur.execute(
            f"SELECT count(DISTINCT k::text) FROM {table} WHERE {where} AND k::text = ANY(%s)", (keys,)
        ).fetchone()[0]
        assert n == len(keys), (n, len(keys))
    c.commit()


def relid(c, table):
    return sql(c, "SELECT to_regclass(%s)::oid", (table,))[0][0]


def rebuild(c, table, gen, side=None, reset=True, delete_keys=True, lo=0, hi=None):
    rid = relid(c, table)
    hi = N if hi is None else hi
    t, w0 = time.perf_counter(), wal(c)
    lat = []
    for b, x in enumerate(range(lo, hi, BATCH)):
        t1 = time.perf_counter()
        write_batch(c, table, rid, gen, x, min(hi, x + BATCH), reset=reset and b == 0 and lo == 0, side=side,
                    delete_keys=delete_keys)
        lat.append(time.perf_counter() - t1)
    el = time.perf_counter() - t
    first = lat[0]
    lat.sort()
    return dict(seconds=round(el, 1), rows_per_s=round(len(lat) * BATCH / el), wal_mb=round(wal_bytes(c, w0) / 2**20),
                batch_p50_ms=round(lat[len(lat) // 2] * 1e3, 1), batch_p99_ms=round(lat[int(len(lat) * .99)] * 1e3, 1),
                first_batch_ms=round(first * 1e3, 1))


QUERIES = {
    "point": "SELECT * FROM {v} WHERE k = 'key{i:09d}'",
    "range_a": "SELECT count(*), sum(b) FROM {v} WHERE a BETWEEN {j} AND {j} + 2000",
    "count": "SELECT count(*) FROM {v}",
    "group": "SELECT a % 100, avg(b) FROM {v} GROUP BY 1",
    "join": "SELECT count(*) FROM {v} x JOIN s_other o ON o.k = x.k",
}


def queries(view, reps=None):
    reps = reps or {"point": 2000, "range_a": 200, "count": 3, "group": 3, "join": 3}
    res = {}
    rnd = random.Random(1)
    with conn(autocommit=True) as c:
        for name, q in QUERIES.items():
            sql(c, q.format(v=view, i=1, j=1))
            t = time.perf_counter()
            for _ in range(reps[name]):
                sql(c, q.format(v=view, i=rnd.randrange(N), j=rnd.randrange(1000003)))
            res[name + "_ms"] = round((time.perf_counter() - t) / reps[name] * 1e3, 3)
    return res


def plans(view):
    with conn(autocommit=True) as c:
        return {
            name: [r[0] for r in sql(c, "EXPLAIN " + q.format(v=view, i=5, j=5))]
            for name, q in QUERIES.items()
        }


def blocking(switch, view, label, hold=10.0):
    """The switch with a long outside query open on view: how long the switch
    waits, and how long a short outside query issued while it waits takes."""
    long_c = conn()
    sql(long_c, f"SELECT k FROM {view} LIMIT 1")  # a report's transaction: holds the relation's lock
    t0 = time.perf_counter()
    res = {}

    def do_switch():
        with conn() as c:
            t = time.perf_counter()
            switch(c)
            res["switch_s"] = round(time.perf_counter() - t, 3)

    th = threading.Thread(target=do_switch)
    th.start()
    time.sleep(1.0)

    def short():
        with conn(autocommit=True) as c:
            t = time.perf_counter()
            sql(c, f"SELECT * FROM {view} WHERE k = 'key000000007'")
            res["short_query_s"] = round(time.perf_counter() - t, 3)

    q = threading.Thread(target=short)
    q.start()
    time.sleep(max(0.0, hold - (time.perf_counter() - t0)))
    long_c.commit()
    long_c.close()
    th.join()
    q.join()
    res["long_query_open_s"] = hold
    out(variant=label, what="switch under a long outside query", **res)


def setup_other(c):
    sql(c, "CREATE TABLE s_other (k text PRIMARY KEY)")
    sql(c, f"INSERT INTO s_other SELECT 'key' || lpad((i * 97)::text, 9, '0') FROM generate_series(0, {N // 100 - 1}) i")
    c.commit()


def patches(c, table, side=None, n=50):
    """Outside rebuilds: n ordinary batches of 10k keys, rewritten in place."""
    rid = relid(c, table)
    rnd = random.Random(7)
    t = time.perf_counter()
    for b in range(n):
        lo = rnd.randrange(0, N - BATCH)
        write_batch(c, table, rid, 100 + b, lo, lo + BATCH, side=side)
    return round(n * BATCH / (time.perf_counter() - t))


def main():
    with conn() as c:
        fresh(c)
        setup_other(c)
    with conn(autocommit=True) as c:
        sql(c, "VACUUM ANALYZE s_other")

    if "today" in ONLY:
        with conn() as c:
            sql(c, f"CREATE TABLE t_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))")
            sql(c, "CREATE INDEX t_copy_a ON t_copy (a)")
            c.commit()
            out(variant="today", what="load", seconds=round(load_old(c, "t_copy"), 1))
        vacuum("t_copy")
        with conn() as c:
            sz = size(c, "t_copy")
            out(variant="today", what="size", heap=sz[0], indexes=sz[1], total=sz[2], bytes_per_row=round(sz[2] / N, 1))
            out(variant="today", what="patches", rows_per_s=patches(c, "t_copy"))
        out(variant="today", what="queries", **queries("t_copy"))
        out(variant="today", what="plans", **plans("t_copy"))
        with conn() as c:
            out(variant="today", what="rebuild", **rebuild(c, "t_copy", 1000))
        out(variant="today", what="vacuum after rebuild", seconds=round(vacuum("t_copy"), 1))

    if "side" in ONLY:
        with conn() as c:
            sql(c, f"CREATE TABLE solera.s_copy ({COLS}, _side smallint NOT NULL, _version bigint, "
                   "PRIMARY KEY (k, part, _side))")
            sql(c, "CREATE INDEX s_copy_a ON solera.s_copy (a)")
            rid = relid(c, "solera.s_copy")
            sql(c, f"INSERT INTO {FENCE} VALUES (%s, '', 1, 'w', 1, 0, NULL, NULL)", (rid,))
            sql(c, f"CREATE VIEW s_copy AS SELECT r.k, r.a, r.b, r.c, r.part, r._version FROM solera.s_copy r "
                   f"WHERE r._side = (SELECT g.live FROM {FENCE} g WHERE g.relid = {rid} AND g.part = '')")
            c.commit()
            out(variant="side", what="load", seconds=round(load_old(c, "solera.s_copy", side=0), 1))
        vacuum("solera.s_copy")
        with conn() as c:
            sz = size(c, "solera.s_copy")
            out(variant="side", what="size", heap=sz[0], indexes=sz[1], total=sz[2], bytes_per_row=round(sz[2] / N, 1))
            out(variant="side", what="patches", rows_per_s=patches(c, "solera.s_copy", side=0))
        out(variant="side", what="queries", **queries("s_copy"))
        out(variant="side", what="plans", **plans("s_copy"))
        with conn() as c:
            out(variant="side", what="rebuild, first half", **rebuild(c, "solera.s_copy", 1000, side=0, hi=N // 2))
        vacuum("solera.s_copy")
        out(variant="side", what="queries mid rebuild (old side whole, new side half)", **queries("s_copy"))
        with conn() as c:
            out(variant="side", what="rebuild, second half",
                **rebuild(c, "solera.s_copy", 1001, side=0, lo=N // 2))
            vis = sql(c, "SELECT count(*), min(_version), max(_version) FROM s_copy")[0]
            out(variant="side", what="outside view before the switch", rows=vis[0], versions=[vis[1], vis[2]])

        def switch(c):
            sql(c, f"UPDATE {FENCE} SET live = build, build = NULL WHERE relid = %s AND part = '' AND build_gen = 1000",
                (relid(c, "solera.s_copy"),))
            c.commit()

        blocking(switch, "s_copy", "side")
        with conn() as c:
            vis = sql(c, "SELECT count(*), min(_version), max(_version) FROM s_copy")[0]
            out(variant="side", what="outside view after the switch", rows=vis[0], versions=[vis[1], vis[2]])
            w0, t = wal(c), time.perf_counter()
            sql(c, "DELETE FROM solera.s_copy WHERE part = '' AND _side = 0")
            c.commit()
            out(variant="side", what="delete of the old side", seconds=round(time.perf_counter() - t, 1),
                wal_mb=round(wal_bytes(c, w0) / 2**20))
        out(variant="side", what="vacuum after cleanup", seconds=round(vacuum("solera.s_copy"), 1))
        with conn() as c:
            out(variant="side", what="rebuild (a second full run, whole)", **rebuild(c, "solera.s_copy", 2000, side=0))

    if "part" in ONLY:
        with conn() as c:
            sql(c, f"CREATE TABLE p_copy ({COLS}, _version bigint, PRIMARY KEY (k, part)) PARTITION BY LIST (part)")
            sql(c, "CREATE INDEX p_copy_a ON p_copy (a)")
            sql(c, "CREATE TABLE p_copy_old PARTITION OF p_copy FOR VALUES IN ('')")
            c.commit()
            out(variant="part", what="load", seconds=round(load_old(c, "p_copy"), 1))
        vacuum("p_copy")
        out(variant="part", what="queries", **queries("p_copy"))
        with conn() as c:
            sql(c, "CREATE TABLE p_copy_new (LIKE p_copy INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
            sql(c, "ALTER TABLE p_copy_new ADD PRIMARY KEY (k, part)")
            sql(c, "CREATE INDEX p_copy_new_a ON p_copy_new (a)")
            sql(c, "ALTER TABLE p_copy_new ADD CONSTRAINT p_copy_new_part CHECK (part = '')")
            c.commit()
            out(variant="part", what="rebuild", **rebuild(c, "p_copy_new", 1000, reset=False))

        def switch(c):
            sql(c, "ALTER TABLE p_copy DETACH PARTITION p_copy_old")
            sql(c, "ALTER TABLE p_copy ATTACH PARTITION p_copy_new FOR VALUES IN ('')")
            c.commit()

        blocking(switch, "p_copy", "part")
        with conn() as c:
            t = time.perf_counter()
            sql(c, "DROP TABLE p_copy_old")
            c.commit()
            out(variant="part", what="drop of the old rows", seconds=round(time.perf_counter() - t, 2))

    if "rename" in ONLY:
        with conn() as c:
            sql(c, f"CREATE TABLE r_copy ({COLS}, _version bigint, PRIMARY KEY (k, part))")
            sql(c, "CREATE INDEX r_copy_a ON r_copy (a)")
            c.commit()
            out(variant="rename", what="load", seconds=round(load_old(c, "r_copy"), 1))
            sql(c, "CREATE VIEW user_view AS SELECT k, _version FROM r_copy")
            sql(c, "CREATE TABLE r_copy_new (LIKE r_copy INCLUDING ALL)")
            c.commit()
            out(variant="rename", what="rebuild", **rebuild(c, "r_copy_new", 1000, reset=False))

        def switch(c):
            sql(c, "ALTER TABLE r_copy RENAME TO r_copy_old")
            sql(c, "ALTER TABLE r_copy_new RENAME TO r_copy")
            c.commit()

        blocking(switch, "r_copy", "rename")
        with conn() as c:
            uv = sql(c, "SELECT max(_version) FROM user_view")[0][0]
            out(variant="rename", what="a user's view over the table after the switch", reads_version=uv,
                note="1 means it still reads the old table: dependents follow the OID")


if __name__ == "__main__":
    main()
