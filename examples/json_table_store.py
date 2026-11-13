"""A fenced SQL store in one page (docs/stores.md, "Recipe: a SQL table"):
each output is a PostgreSQL table of JSON rows, one row per input row,
with its key, batch and version alongside: a keyed output's rows, an
unkeyed one's (its whole content, or batches), loaded as a list — or, by
key, as `Each` reads a page. A write says which keys it changed
(`KeyedWrite.changed`), and `read` the version each key was read at
(docs/stores.md, "What a write and a load say"). The committed head names the table.
`fence()` at the top of every write transaction is all the version check it
needs: nothing is taken before a read. `reads`
reads an attempt's inputs at one moment, each with the generation that
wrote what it read (`written()`); `written_at` says which keys a partition
holds at a dead writer's version, for the next attempt to repair. Each
call's transaction runs on a thread, off the worker's event loop. The conformance kit
(`solera.testing.stores`) checks it like any store."""

from __future__ import annotations

import asyncio
import contextlib

import psycopg
from psycopg.types.json import Jsonb
from solera.fencing import cleaned, fence, fence_table, written
from solera.sdk import Ref
from solera.stores import MISSING, Commits, KeyedWrite, Keys, Patch, Read, Written, by_key_type, takes

ROWS = (None, list, list[dict])  # what a load gives: rows, as a list


class JsonTableStore:
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
        """The committed head's table; for a first write, the output's."""

        return (prior.handle or {}).get("table") if prior is not None else f'"rows_{output.name}"'

    def _transaction(self, table):
        conn = psycopg.connect(self.dsn)  # `with` commits, or rolls back on an exception
        cur = conn.cursor()
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('json_table_store'))")  # creation takes turns
        fence_table(cur)
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (part text, k text, batch bigint, row jsonb, version bigint)"
        )
        return conn, cur

    async def written_at(self, versions, prior, context) -> list[list[str]]:
        """The keys the partition holds at any of `versions`, the attempts
        that wrote and ended without a commit, in chunks sorted by their
        bytes. It takes the version check for `context` first, so no older
        writer can add a row afterwards."""

        return await asyncio.to_thread(self._written_at, list(versions), prior, context)

    def _written_at(self, versions, prior, context) -> list[list[str]]:
        table = self._table(context.output, prior)
        conn, cur = self._transaction(table)
        with conn:
            fence(cur, context, table)
            cur.execute(
                f"SELECT DISTINCT k FROM {table} WHERE part = %s AND k IS NOT NULL AND version = ANY(%s)",
                (context.partition, versions),
            )
            return [sorted((k for (k,) in cur.fetchall()), key=str.encode)]

    async def store(self, write, prior, context) -> Written:
        return await asyncio.to_thread(self._store, write, prior, context)

    def _store(self, write, prior, context) -> Written:
        out, table = context.output, self._table(context.output, prior)
        if context.reset:
            prior = None  # a full run keeps nothing of the content
        conn, cur = self._transaction(table)
        handle, version = {"table": table}, context.generation
        with conn:
            fence(cur, context, table, write=True)  # before this transaction changes anything
            if out.key is None and not out.incremental:  # an unkeyed output: its whole content
                rows = list(write)
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (context.partition,))
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, NULL, NULL, %s, %s)",
                    [(context.partition, Jsonb(r), version) for r in rows],
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
                    f"INSERT INTO {table} VALUES (%s, NULL, %s, %s, %s)",
                    [(context.partition, context.commit_number, Jsonb(r), version) for r in rows],
                )
                return Written(
                    Ref(out.name, "", {**handle, "commit_number": context.commit_number}, context.partition)
                )
            write = KeyedWrite.of(self, write, out, prior)
            if write.reset:  # the partition's whole content: clear it first
                cur.execute(f"DELETE FROM {table} WHERE part = %s", (context.partition,))
            for chunk in write.iter_chunks():  # the keys to write, and each key's rows
                if not write.reset:
                    cur.execute(
                        f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)",
                        (context.partition, chunk.keys),
                    )
                cur.executemany(
                    f"INSERT INTO {table} VALUES (%s, %s, NULL, %s, %s)",
                    [
                        (context.partition, k, Jsonb(dict(r)), version)
                        for k, group in chunk.groups()
                        for r in group
                    ],
                )
            if write.removes:
                cur.execute(
                    f"DELETE FROM {table} WHERE part = %s AND k = ANY(%s)",
                    (context.partition, sorted(write.removes)),
                )
            ref = Ref(out.name, "", {**handle, "key": out.key}, context.partition)
            return Written(ref, changed=write.changed())

    async def cleanup(self, output, *, partition=None, key=None, generation=None, before=None):
        """Delete what the pattern matches (docs/stores.md § Cleanup): the
        partitions `cleaned` takes by the generation that last wrote them —
        or a key's rows of them — and the table once the whole output goes."""

        await asyncio.to_thread(self._cleanup, output, partition, key, generation, before)

    def _cleanup(self, output, partition, key, generation, before) -> None:
        table = self._table(output, None)
        conn, cur = self._transaction(table)
        with conn:
            parts, everything = cleaned(
                cur, table, partition=partition, generation=generation, before=before, keep=key is not None
            )
            if everything and partition is None and key is None:
                cur.execute(f"DROP TABLE {table}")
                return
            for part in parts:
                if key is None:
                    cur.execute(f"DELETE FROM {table} WHERE part = %s", (part,))
                else:
                    cur.execute(f"DELETE FROM {table} WHERE part = %s AND k = %s", (part, str(key)))

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
        return (await self.read(ref, t, selection)).value

    async def read(self, ref, t, selection) -> Read:
        """`load`, and the version each key was read at (a keyed output's), or
        the newest one read (an unkeyed output's): a key planned but gone is
        None."""

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
                    rows = self._load(conn, ref, t, selection).value
                    return rows, written(conn.cursor(), ref.handle["table"], ref.partition)

                async with lock:  # one connection: one load at a time
                    return await asyncio.to_thread(work)

        try:
            yield Reader()
        finally:
            await asyncio.to_thread(conn.close)

    def _load(self, conn, ref, t, selection) -> Read:
        sql, params = f"SELECT k, row, version FROM {ref.handle['table']} WHERE part = %s", [ref.partition]
        if isinstance(selection, Keys):
            sql, params = sql + " AND k = ANY(%s)", [*params, sorted(selection.generations)]
        elif isinstance(selection, Commits):
            sql, params = sql + " AND batch BETWEEN %s AND %s", [*params, selection.lo, selection.hi]
        elif (ref.handle or {}).get("commit_number") is not None:
            sql, params = sql + " AND batch <= %s", [*params, ref.handle["commit_number"]]
        found = conn.execute(sql, params).fetchall()
        versions = {k: v for k, _, v in found} if ref.handle.get("key") else None
        if isinstance(selection, Keys):
            versions = {k: versions.get(k) for k in selection.generations}
        newest = (
            None if versions is not None else max((v for _, _, v in found if v is not None), default=None)
        )
        if by_key_type(t) is not MISSING:  # each key's rows; a key with none does not exist
            groups: dict[str, list] = {}
            for k, row, _ in found:
                groups.setdefault(k, []).append(row)
            return Read(groups, versions)
        return Read([row for _, row, _ in found], versions, newest)
