"""A fenced SQL store in one page (docs/stores.md, "Recipe: a SQL table"):
each output is a PostgreSQL table of JSON rows, one row per input row,
with its key and batch alongside: a keyed output's rows, an unkeyed
one's (its whole content, or batches), loaded as a list — or, by key,
as `Each` reads a page. The committed head names the table, so a renamed
output keeps it. `fence()` at the top of every write transaction, and as
`acquire`, is all the fencing it needs; `reads`
reads an attempt's inputs at one moment, each with the generation that
wrote what it read (`written()`). Each call's transaction runs on a
thread, off the worker's event loop. The conformance kit
(`solera.testing.stores`) checks it like any store."""

from __future__ import annotations

import asyncio
import contextlib

import psycopg
from psycopg.types.json import Jsonb
from solera.fencing import fence, fence_table, written
from solera.sdk import Ref, digest
from solera.stores import MISSING, Batches, KeyedWrite, Keys, Patch, Written, by_key_type, takes

ROWS = (None, list, list[dict])  # what a load gives: rows, as a list


class JsonTableStore:
    writes = "fenced"

    def __init__(self, dsn: str):
        self.dsn = dsn

    def can_load(self, t, selection) -> bool:
        inner = by_key_type(t)
        if inner is not MISSING:  # dict[str, rows]: each key's rows, as `Each` reads a page
            return selection is not None and inner in ROWS
        return t in ROWS

    def can_store(self, t, output) -> bool:
        return takes(t, output, values=False)  # rows as Python: it defines no `prepare` of its own

    def _table(self, output, prior) -> str:
        """The committed head's table; the output's name only for a first write."""

        return (prior.handle or {}).get("table") if prior is not None else f'"rows_{output.name}"'

    def _transaction(self, table):
        conn = psycopg.connect(self.dsn)  # `with` commits, or rolls back on an exception
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('json_table_store'))")  # creation takes turns
        fence_table(cur)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (part text, k text, batch bigint, row jsonb)")
        return conn, cur

    async def acquire(self, scope, prior) -> None:
        await asyncio.to_thread(self._acquire, scope, prior)

    def _acquire(self, scope, prior) -> None:
        table = self._table(scope.output, prior)
        conn, cur = self._transaction(table)
        with conn:
            fence(cur, scope, table)

    async def store(self, write, prior, scope) -> Written:
        return await asyncio.to_thread(self._store, write, prior, scope)

    def _store(self, write, prior, scope) -> Written:
        out, table = scope.output, self._table(scope.output, prior)
        if scope.reset:
            prior = None  # a full run keeps nothing of the content
        conn, cur = self._transaction(table)
        handle = {"table": table}
        with conn:
            fence(cur, scope, table, write=True)  # before this transaction changes anything
            if out.key is None and not out.incremental:  # an unkeyed output: its whole content
                rows = list(write)
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (scope.partition,))
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, NULL, %s)",
                    [(scope.partition, Jsonb(r)) for r in rows],
                )
                return Written(Ref(out.name, "", handle, digest(rows), scope.partition))
            if out.key is None:  # an unkeyed incremental output: a batch of rows
                rows = list(write.rows if isinstance(write, Patch) else write)
                # The batch replaces itself (a retried call writes it again); with no
                # prior (a first write, or a reset) the batches start over.
                if prior is None:
                    cur.execute(f"DELETE FROM {table} WHERE part = %s", (scope.partition,))
                else:
                    cur.execute(
                        f"DELETE FROM {table} WHERE part = %s AND batch = %s", (scope.partition, scope.batch)
                    )
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, %s, %s)",
                    [(scope.partition, scope.batch, Jsonb(r)) for r in rows],
                )
                version = digest([prior.version if prior else "", scope.batch, rows])
                return Written(Ref(out.name, "", {**handle, "batch": scope.batch}, version, scope.partition))
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
            return Written(Ref(out.name, "", handle, write.version(prior), scope.partition))

    async def load(self, ref, t, selection):
        def work():
            with psycopg.connect(self.dsn) as conn:
                return self._load(conn, ref, t, selection)

        return await asyncio.to_thread(work)

    @contextlib.asynccontextmanager
    async def reads(self):
        """One snapshot for an attempt's reads: each load, with the generation
        that wrote what it read."""

        conn = await asyncio.to_thread(psycopg.connect, self.dsn)
        conn.isolation_level, conn.read_only = psycopg.IsolationLevel.REPEATABLE_READ, True
        lock = asyncio.Lock()

        class Reader:
            async def load(_, ref, t, selection):
                def work():
                    rows = self._load(conn, ref, t, selection)
                    return rows, written(conn.cursor(), ref.handle["table"], ref.partition)

                async with lock:  # one connection: one load at a time
                    return await asyncio.to_thread(work)

        try:
            yield Reader()
        finally:
            await asyncio.to_thread(conn.close)

    def _load(self, conn, ref, t, selection):
        sql, params = f"SELECT k, row FROM {ref.handle['table']} WHERE part = %s", [ref.partition]
        if isinstance(selection, Keys):
            sql, params = sql + " AND k = ANY(%s)", [*params, sorted(selection.revisions)]
        elif isinstance(selection, Batches):
            sql, params = sql + " AND batch BETWEEN %s AND %s", [*params, selection.lo, selection.hi]
        elif (ref.handle or {}).get("batch") is not None:
            sql, params = sql + " AND batch <= %s", [*params, ref.handle["batch"]]
        found = conn.execute(sql, params).fetchall()
        if by_key_type(t) is not MISSING:  # each key's rows; a key with none does not exist
            groups: dict[str, list] = {}
            for k, row in found:
                groups.setdefault(k, []).append(row)
            return groups
        return [row for _, row in found]
