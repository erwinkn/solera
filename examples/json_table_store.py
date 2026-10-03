"""A fenced SQL store in one page (docs/stores.md, "Recipe: a SQL table"):
each output is a PostgreSQL table of JSON rows, one row per input row,
with its key and batch alongside: a keyed output's rows, an unkeyed
one's (its whole content, or batches), loaded as a list — or, by key,
as `Each` reads a page. The committed head names the table, so a renamed
output keeps it. `fence()` at the top of every write transaction, and as
`acquire`, is all the fencing it needs; `reads`
reads an attempt's inputs at one moment, each with the generation that
wrote what it read (`written()`); `keys` says which keys a partition holds,
for the repair after a writer died. Each call's transaction runs on a
thread, off the worker's event loop. The conformance kit
(`solera.testing.stores`) checks it like any store."""

from __future__ import annotations

import asyncio
import contextlib

import psycopg
from psycopg.types.json import Jsonb
from solera.fencing import fence, fence_table, written
from solera.sdk import Ref
from solera.stores import MISSING, Commits, KeyedWrite, Keys, Patch, Written, by_key_type, takes

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

    async def acquire(self, context, prior) -> None:
        await asyncio.to_thread(self._acquire, context, prior)

    def _acquire(self, context, prior) -> None:
        table = self._table(context.output, prior)
        conn, cur = self._transaction(table)
        with conn:
            fence(cur, context, table)

    async def store(self, write, prior, context) -> Written:
        return await asyncio.to_thread(self._store, write, prior, context)

    def _store(self, write, prior, context) -> Written:
        out, table = context.output, self._table(context.output, prior)
        if context.reset:
            prior = None  # a full run keeps nothing of the content
        conn, cur = self._transaction(table)
        handle = {"table": table}
        with conn:
            fence(cur, context, table, write=True)  # before this transaction changes anything
            if out.key is None and not out.incremental:  # an unkeyed output: its whole content
                rows = list(write)
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (context.partition,))
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, NULL, %s)",
                    [(context.partition, Jsonb(r)) for r in rows],
                )
                return Written(Ref(out.name, "", handle, context.partition))
            if out.key is None:  # an unkeyed incremental output: a batch of rows
                rows = list(write.rows if isinstance(write, Patch) else write)
                # The batch replaces itself (a retried call writes it again); with no
                # prior (a first write, or a reset) the batches start over.
                if prior is None:
                    cur.execute(f"DELETE FROM {table} WHERE part = %s", (context.partition,))
                else:
                    cur.execute(
                        f"DELETE FROM {table} WHERE part = %s AND batch = %s",
                        (context.partition, context.commit_number),
                    )
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, %s, %s)",
                    [(context.partition, context.commit_number, Jsonb(r)) for r in rows],
                )
                return Written(
                    Ref(out.name, "", {**handle, "commit_number": context.commit_number}, context.partition)
                )
            write = KeyedWrite.of(self, write, out, prior)
            if write.reset:  # the partition's whole content: clear it first
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (context.partition,))
            for chunk in write.iter_chunks():  # the keys to write: (key, rows)
                keys = [k for k, _ in chunk]
                if not write.reset:
                    cur.execute(
                        f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)", (context.partition, keys)
                    )
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, %s, NULL, %s)",
                    [(context.partition, k, Jsonb(dict(r))) for k, group in chunk for r in group],
                )
            if write.removes:
                cur.execute(
                    f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)",
                    (context.partition, sorted(write.removes)),
                )
            return Written(Ref(out.name, "", handle, context.partition))

    def keys(self, ref, among=None):
        """The keys `ref`'s partition holds — among `among`, or all — sorted by
        their bytes, in chunks: never a value."""

        sql, params = (
            f"SELECT DISTINCT k FROM {ref.handle['table']} WHERE part = %s AND k IS NOT NULL",
            [ref.partition],
        )
        if among is not None:
            sql, params = sql + " AND k = ANY(%s)", [*params, sorted(among)]
        with psycopg.connect(self.dsn) as conn:
            found = [k for (k,) in conn.execute(sql, params)]
        yield sorted(found, key=str.encode)

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
            sql, params = sql + " AND k = ANY(%s)", [*params, sorted(selection.generations)]
        elif isinstance(selection, Commits):
            sql, params = sql + " AND batch BETWEEN %s AND %s", [*params, selection.lo, selection.hi]
        elif (ref.handle or {}).get("commit_number") is not None:
            sql, params = sql + " AND batch <= %s", [*params, ref.handle["commit_number"]]
        found = conn.execute(sql, params).fetchall()
        if by_key_type(t) is not MISSING:  # each key's rows; a key with none does not exist
            groups: dict[str, list] = {}
            for k, row in found:
                groups.setdefault(k, []).append(row)
            return groups
        return [row for _, row in found]
