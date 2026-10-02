"""A fenced SQL store in one page (docs/stores.md, "Recipe: a SQL table"):
each output is a PostgreSQL table of JSON rows, one row per input row,
with its key and batch alongside. `fence()` at the top of every write
transaction, and as `acquire`, is all the fencing it needs. Each call's
transaction runs on a thread, off the worker's event loop. The
conformance kit (`solera.testing.stores`) checks it like any store."""

from __future__ import annotations

import asyncio

import psycopg
from psycopg.types.json import Jsonb
from solera.fencing import fence, fence_table
from solera.sdk import Ref, digest
from solera.stores import Batches, KeyedWrite, Keys, Written, takes_plain


class JsonTableStore:
    writes = "fenced"

    def __init__(self, dsn: str):
        self.dsn = dsn

    def can_load(self, t, selection) -> bool:
        return True

    def can_store(self, t, output) -> bool:
        return takes_plain(t)  # rows as Python: it defines no `prepare` of its own

    def _table(self, output) -> str:
        return f'"rows_{output.name}"'

    def _transaction(self, output):
        conn = psycopg.connect(self.dsn)  # `with` commits, or rolls back on an exception
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('json_table_store'))")  # creation takes turns
        fence_table(cur)
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table(output)} (part text, k text, batch bigint, row jsonb)"
        )
        return conn, cur

    async def acquire(self, scope) -> None:
        await asyncio.to_thread(self._acquire, scope)

    def _acquire(self, scope) -> None:
        conn, cur = self._transaction(scope.output)
        with conn:
            fence(cur, scope, self._table(scope.output))

    async def store(self, write, prior, scope) -> Written:
        return await asyncio.to_thread(self._store, write, prior, scope)

    def _store(self, write, prior, scope) -> Written:
        out, table = scope.output, self._table(scope.output)
        conn, cur = self._transaction(out)
        with conn:
            fence(cur, scope, table)  # before this transaction changes anything
            if out.key is None:  # an unkeyed incremental output: a batch of rows
                rows = list(write.rows)
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, %s, %s)",
                    [(scope.partition, scope.batch, Jsonb(r)) for r in rows],
                )
                version = digest([prior.version if prior else "", scope.batch, rows])
                return Written(Ref(out.name, "", {"batch": scope.batch}, version, scope.partition))
            write = KeyedWrite.of(self, write, out, prior)
            if write.whole:  # the scope's whole content: clear it first
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (scope.partition,))
            for page in write.iter_pages():  # the keys to write: (key, version, rows)
                keys = [k for k, _, _ in page]
                if not write.whole:
                    cur.execute(
                        f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)", (scope.partition, keys)
                    )
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, %s, NULL, %s)",
                    [(scope.partition, k, Jsonb(dict(r))) for k, _, group in page for r in group],
                )
            if write.removes:
                cur.execute(
                    f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)",
                    (scope.partition, sorted(write.removes)),
                )
            return Written(Ref(out.name, "", {}, write.version(prior), scope.partition))

    async def load(self, ref, t, selection) -> list[dict]:
        return await asyncio.to_thread(self._load, ref, selection)

    def _load(self, ref, selection) -> list[dict]:
        sql, params = f'SELECT row FROM "rows_{ref.output}" WHERE part = %s', [ref.partition]
        if isinstance(selection, Keys):
            sql, params = sql + " AND k = ANY(%s)", [*params, sorted(selection.revisions)]
        elif isinstance(selection, Batches):
            sql, params = sql + " AND batch BETWEEN %s AND %s", [*params, selection.lo, selection.hi]
        elif (ref.handle or {}).get("batch") is not None:
            sql, params = sql + " AND batch <= %s", [*params, ref.handle["batch"]]
        with psycopg.connect(self.dsn) as conn:
            return [r[0] for r in conn.execute(sql, params).fetchall()]
