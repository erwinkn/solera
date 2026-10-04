"""PostgresStore: shared mutable tables (§3, §4). Reads are not pinned: a
ref names a table partition, and a load reads what it holds now.

A `fenced` store (docs/lifecycle.md §9.7): each attempt takes its generation
for the partition it writes — keyed by the table's OID, which survives renames,
and the partition — before it reads anything, and every write transaction
checks it under the row's lock. A newer attempt's acquisition waits for an
older writer's open transaction, after which that writer can change nothing.

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the worker).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from solera.sdk import KEYS, Output, Ref, TableRef
from solera.stores import (
    MISSING,
    Commits,
    KeyedWrite,
    Keys,
    Opaque,
    Patch,
    Prepared,
    StoreError,
    WriteContext,
    WriteError,
    Written,
    by_key_type,
    frames,
    resolve_env,
    takes,
)

log = logging.getLogger("solera.postgres")

LEDGER_TABLE = "public.solera_migration_ledger"  # (relation, name): what ran on which table
FENCE_TABLE = "public.solera_generations"
COMMIT_COLUMN = "_commit"
SEQ_COLUMN = "_seq"
KEY_CHUNK = 100_000  # keys per chunk `keys` reads


def _assigned_commit(context: WriteContext, prior: Ref | None) -> int:
    if context.commit_number is not None:
        return context.commit_number
    last = (prior.handle or {}).get("commit_number") if prior is not None else None
    return int(last) + 1 if last is not None else 0


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
        raise StoreError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


def _qname(schema: str, table: str) -> str:
    return f"{_ident(schema)}.{_ident(table)}"


def _split(qualified: str) -> tuple[str, str]:
    """`"schema"."table"`, as `_qname` makes it: its schema and table."""

    found = re.fullmatch(r'"([^"]+)"\."([^"]+)"', qualified)
    if found is None:
        raise StoreError(f"not a table this store names: {qualified}")
    return found.group(1), found.group(2)


@dataclass(frozen=True)
class Sql(Opaque):
    """Materialize a query — a SELECT, VALUES or TABLE — into the output's
    table (§4), in the database: the worker never sees its rows. It is
    never run as a statement: UPDATE, DELETE and DDL are refused; a table
    changes through a `Migration`."""

    stmt: str


class PostgresStore:
    version = "1"
    ref_type = TableRef
    shared_table = True
    writes = "fenced"

    def __init__(self, dsn: str, grants: list[str] | tuple = (), sql_read_only: bool = False):
        """`grants`: roles given SELECT on every table the store creates.
        `sql_read_only`: a `Sql` query runs in a READ ONLY transaction of its
        own, so not even a function it calls can write; its rows stream
        through the worker, from one connection's COPY into the other's."""

        self.dsn, self.grants, self.sql_read_only = dsn, tuple(grants), sql_read_only

    # -- registration -------------------------------------------------------

    def can_load(self, t, selection) -> bool:
        if t is None:
            return selection is None
        inner = by_key_type(t)
        if inner is not MISSING:  # dict[str, T]: a keyed read, each key's group as T (per-key §5)
            return selection is not None and inner is not None and self.can_load(inner, selection)
        if getattr(t, "__module__", "").split(".")[0] in ("pandas", "geopandas") and getattr(
            t, "__name__", ""
        ) in ("DataFrame", "GeoDataFrame"):
            return True
        import typing

        if typing.get_origin(t) is list and typing.get_args(t) == (dict,):
            return True
        if t is list or t is dict:
            return True
        if isinstance(t, type) and issubclass(t, Ref):
            return issubclass(self.ref_type, t)
        return False

    def can_store(self, t, output) -> bool:
        if output.is_dynamic_partitions or output.key == KEYS:
            return False  # dynamic partitions and dict outputs live on the default store
        return t is Sql or takes(t, output, frames=True, values=False)  # rows, DataFrames, Arrow

    # -- plumbing -----------------------------------------------------------

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(resolve_env(self.dsn), row_factory=dict_row, autocommit=False)

    def _table(self, output: Output, prior: Ref | None = None) -> str:
        """Where the output's content lives: the committed head's table, so a
        renamed output keeps its table, and every ref to it stays readable
        (§2); the declaration's (`schema`, `table`, else the output's name)
        only for a first write."""

        committed = (prior.handle or {}).get("table") if prior is not None else None
        if committed:
            return (committed, *_split(committed))
        schema = output.config.get("schema", "public")
        table = output.config.get("table", output.name)
        return _qname(schema, table), schema, table

    def _declared_shape(self, output: Output) -> tuple[dict, list]:
        """The columns and primary key the declaration pins down (§4). Row-inferred
        columns are creation-only and never part of the drift check."""

        columns = dict(output.config.get("columns") or {})
        partition_col = output.config.get("partition_column")
        if partition_col:
            columns.setdefault(partition_col, "text")
        commit_mode = output.incremental and output.key is None
        if commit_mode:
            columns.setdefault(COMMIT_COLUMN, "integer")
            columns.setdefault(SEQ_COLUMN, "integer")
        # A key names a group of rows, not one: it is never a primary key by itself.
        pk = list(output.config.get("primary_key") or [])
        if partition_col and partition_col not in pk and (pk or commit_mode):
            pk = [*pk, partition_col]
        if commit_mode:
            for c in (COMMIT_COLUMN, SEQ_COLUMN):
                if c not in pk:
                    pk = [*pk, c]
        return columns, pk

    def _ensure(
        self,
        cur,
        output: Output,
        table: str,
        rows: Callable[[], Iterable[dict]] | None = None,
        context: WriteContext | None = None,
        inferred: dict | None = None,
        kinds: Mapping[str, str | None] | None = None,
    ) -> None:
        """The table, created if missing, under the transaction's fence. A
        table the write creates takes the declared columns, then those of a
        `Sql` SELECT (`inferred`), then the write's: each column's kind as
        its reader knows it (`kinds`: a DataFrame's dtypes, an Arrow schema),
        else the kind of every value not null in it (`rows()`, read only
        then). Inferred columns are logged, once. What a column does to a
        value is the database's: the declared columns are the user's
        contract (docs/versions.md §4)."""

        schema, table_name = _split(table)
        indexes = self._indexes(output)
        names = [table_name + "_" + "_".join(index) for index in indexes]
        if key_text := self._key_text(cur, output, schema, table_name):
            names.append(key_text)
        exists = "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
        indexed = cur.execute(
            "SELECT count(*) AS n FROM pg_indexes WHERE schemaname = %s AND tablename = %s AND indexname = ANY(%s)",
            (schema, table_name, names),
        ).fetchone()["n"]
        if indexed < len(names) or not cur.execute(exists, (schema, table_name)).fetchone():
            # Partitions write one table at once, and `IF NOT EXISTS` DDL collides until the
            # first creator commits: take turns, until the end of the transaction.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        declared, pk = self._declared_shape(output)
        declared = {**(inferred or {}), **declared}
        columns = dict(declared)
        existed = cur.execute(exists, (schema, table_name)).fetchone()
        if not existed and rows is not None:
            known = {c: _COLUMNS[k] for c, k in (kinds or {}).items() if k is not None and c not in declared}
            untyped = kinds is None or any(k is None for c, k in kinds.items() if c not in declared)
            found = _column_types(output.name, rows(), {**declared, **known}) if untyped else {}
            columns = {**known, **found, **declared}
            if guessed := {c: t for c, t in columns.items() if c not in declared}:
                log.warning(
                    "%s: creating %s with inferred columns %s; declare them to keep them: "
                    "Output(..., columns={...})",
                    output.name,
                    table,
                    guessed,
                )
        defs = [f"{_ident(c)} {_sql_type(t)}" for c, t in (columns or {"value": "jsonb"}).items()]
        if pk:
            defs.append(f"PRIMARY KEY ({', '.join(_ident(c) for c in pk)})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(defs)})")
        if existed:
            self._check_drift(cur, output, table, schema, table_name, columns, pk)
        if key_text := self._key_text(cur, output, schema, table_name):
            # Reads and patches name keys as text (`key::text = ANY(...)`): an index on
            # the column alone serves only a text key.
            key = _ident(output.key)
            cur.execute(f"CREATE INDEX IF NOT EXISTS {_ident(key_text)} ON {table} (({key}::text))")
        for index in indexes:
            cols = ", ".join(_ident(c) for c in index)
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {_ident(table_name + '_' + '_'.join(index))} ON {table} ({cols})"
            )
        # Grants are deployment sugar: a role this database lacks is skipped, before
        # its GRANT could fail and abort the write's transaction; any other failure is real.
        present = {
            r["rolname"]
            for r in cur.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(self.grants),))
        }
        for role in self.grants:
            if role in present:
                cur.execute(f"GRANT SELECT ON {table} TO {_ident(role)}")
        self._fence(cur, table, context)  # before this transaction changes any row

    # -- generations (docs/lifecycle.md §9.7) -------------------------------------

    def _fence_table(self, cur) -> None:
        """The fence rows: per partition, the generation that holds it, and the one
        whose write transaction last changed it (`written`), which a read
        reports (`reads`)."""

        if cur.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
            "AND column_name = 'worker_id'",
            tuple(FENCE_TABLE.split(".")),
        ).fetchone():
            return
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (FENCE_TABLE,))
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {FENCE_TABLE} (relid oid NOT NULL, part text NOT NULL, "
            "generation bigint NOT NULL, worker_id text NOT NULL, written bigint, PRIMARY KEY (relid, part))"
        )

    def _take(self, cur, relid: int, context: WriteContext, write: bool = False) -> None:
        """Take `partition`'s generation for (relid, partition), holding the row's
        lock until the transaction ends. Postgres locks the conflicting row
        even when the `WHERE` refuses the update, so a newer acquisition waits
        for an older writer's open transaction. A `write` transaction also
        marks the partition written by its generation, as it commits."""

        written = ", written = EXCLUDED.written" if write else ""
        taken = cur.execute(
            f"INSERT INTO {FENCE_TABLE} VALUES (%s, %s, %s, %s, %s) ON CONFLICT (relid, part) "
            f"DO UPDATE SET generation = EXCLUDED.generation, worker_id = EXCLUDED.worker_id{written} "
            f"WHERE {FENCE_TABLE}.generation < EXCLUDED.generation "
            f"OR ({FENCE_TABLE}.generation = EXCLUDED.generation AND {FENCE_TABLE}.worker_id = EXCLUDED.worker_id) "
            "RETURNING worker_id",
            (
                relid,
                context.partition,
                context.generation,
                context.worker_id,
                context.generation if write else None,
            ),
        ).fetchone()
        if taken is None:
            raise StoreError(
                f"{context.output.name}: a newer attempt holds this slice (generation {context.generation} "
                f"of {context.worker_id} refused)"
            )

    def _domain(self, cur, table: str, exclusive: bool = False) -> None:
        """The table's write domain, until the transaction ends: shared by
        every writer's transaction — acquisitions, a partition's first write,
        writes — and exclusive for a migration, which so runs between
        writers of every partition, never under one. Always the first lock a
        transaction takes, so the order is the same everywhere."""

        lock = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
        cur.execute(f"SELECT {lock}(hashtext(%s))", (f"solera-domain:{table}",))

    def _relid(self, cur, table: str) -> int | None:
        return cur.execute("SELECT to_regclass(%s)::oid AS relid", (table,)).fetchone()["relid"]

    def _fence(self, cur, table: str, context: WriteContext | None) -> None:
        """In a write transaction, before it changes anything: the partition must
        still be this attempt's (`_take` is a no-op for its own generation
        and worker). A partition it never acquired — a table this
        transaction created, a new partition — is acquired here."""

        if context is None or context.generation is None:
            return
        self._fence_table(cur)
        self._take(cur, self._relid(cur, table), context, write=True)

    async def acquire(self, context: WriteContext, prior: Ref | None = None) -> None:
        """Take the attempt's generation for the partition it writes — in the
        table `prior`, the committed head, names — in a transaction of its
        own, before any read of the store: from here on no older attempt can
        change it. A table that does not exist yet is acquired when the
        first write creates it."""

        if context.generation is None:
            return
        await asyncio.to_thread(self._acquire, context, prior)

    def _acquire(self, context: WriteContext, prior: Ref | None) -> None:
        table, _, _ = self._table(context.output, prior)
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table)
            relid = self._relid(cur, table)
            if relid is None:
                return
            self._fence_table(cur)
            self._take(cur, relid, context)

    def _key_text(self, cur, output: Output, schema: str, table_name: str) -> str | None:
        """The name of the index on a keyed table's key as text, which the
        key's own index cannot serve when it is not text (a bigint key):
        None when it is, or when there is no table yet."""

        if output.key is None:
            return None
        found = cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (schema, table_name, output.key),
        ).fetchone()
        if found is None or found["data_type"] in ("text", "character varying", "character"):
            return None
        return f"{table_name}_{output.key}_text"

    def _indexes(self, output: Output) -> list[list[str]]:
        """The table's indexes: the declared ones, and one on the key column."""

        _, pk = self._declared_shape(output)
        keyed = [[output.key]] if output.key and output.key not in pk[:1] else []
        return keyed + [list(i) for i in output.config.get("indexes") or []]

    def _check_drift(self, cur, output: Output, table, schema, table_name, declared, pk):
        """A pre-existing table must already match the declaration; evolving
        shape is a migration's job, never a silent ALTER (§4)."""

        if not declared and not pk:
            return
        live_columns = {
            r["column_name"]
            for r in cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table_name),
            )
        }
        live_pk = [
            r["column_name"]
            for r in cur.execute(
                "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "ON tc.constraint_name = kcu.constraint_name "
                "AND tc.constraint_schema = kcu.constraint_schema "
                "AND tc.table_schema = kcu.table_schema "
                "AND tc.table_name = kcu.table_name "
                "WHERE tc.constraint_type = 'PRIMARY KEY' "
                "AND tc.table_schema = %s AND tc.table_name = %s "
                "ORDER BY kcu.ordinal_position",
                (schema, table_name),
            )
        ]
        drift = []
        missing = [c for c in declared if c not in live_columns]
        if missing:
            drift.append(f"missing columns {missing}")
        if set(live_pk) != set(pk):
            drift.append(f"primary key {live_pk or 'none'} != declared {pk or 'none'}")
        if drift:
            raise StoreError(
                f"{output.name}: live table {table} does not match the declaration "
                f"({'; '.join(drift)}); reconcile it with a Migration"
            )

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, context: WriteContext) -> Written:
        """The write, in one transaction on a thread of its own: the worker's
        event loop goes on meanwhile. Canceled, the transaction still ends as
        it would have — under its fence, and counted as `writing` until then."""

        import psycopg

        try:
            return await asyncio.to_thread(self._store, write, prior, context)
        except psycopg.IntegrityError as e:  # the data breaks the table's constraints (`primary_key`)
            raise WriteError(f"{context.output.name}: {e}") from e

    def _store(self, write, prior: Ref | None, context: WriteContext) -> Written:
        output = context.output
        table, _, _ = self._table(output, prior)  # where it is, even when a full run starts it over
        if context.reset:
            prior = None  # a full run keeps nothing of the content
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: context.partition} if partition_col else {}

            commit_number = _assigned_commit(context, prior) if output.incremental else None
            keys = None
            if isinstance(write, Sql):
                self._apply_sql(cur, output, write, context, table, slice_where)
                if output.key:  # read once the write has committed
                    keys = self._keys(table, output.key, slice_where, None)
            elif output.key is not None:
                if isinstance(write, Patch) and not output.incremental:
                    raise WriteError(f"{output.name}: Patch requires an incremental output")
                write = KeyedWrite.of(self, write, output, prior)
                if prior is None and not (
                    write.reset or write.upserts or write.removes or len(write.prepared.rows)
                ):
                    return Written(None)  # a first write of nothing: no table to make
                self._apply_keyed(cur, output, write, context, table, slice_where)
            elif isinstance(write, Patch):
                if not self._apply_commit(
                    cur, output, write, context, table, slice_where, prior, commit_number
                ):
                    return Written(prior)
            else:
                if output.incremental:
                    raise WriteError(
                        f"{output.name}: an unkeyed incremental output only accepts Patch writes"
                    )
                self._apply_replace(cur, output, write, context, table, slice_where)
        commit_mode = output.key is None and output.incremental
        return Written(
            TableRef(
                output=output.name,
                store="",
                handle={
                    "table": table,
                    "where": slice_where,
                    "key": COMMIT_COLUMN if commit_mode else output.key,
                    "commit_number": commit_number if commit_mode else None,
                },
                partition=context.partition,
            ),
            keys,
        )

    def _apply_replace(self, cur, output, write, context, table, slice_where) -> None:
        """An unkeyed output's whole content."""

        rows = frames.rows_of(write, output.name)
        kinds = frames.frame_kinds(write) if frames.is_frame(write) else None
        self._ensure(cur, output, table, lambda: rows, context, kinds=kinds)
        self._delete_slice(cur, table, slice_where)
        self._insert(cur, output, table, rows, self._stamps(output, context))

    def _apply_commit(self, cur, output, write: Patch, context, table, slice_where, prior, commit_number):
        """An unkeyed incremental output's commit: its rows stamped with the
        commit columns, in place of this commit's (a retry's) — or, with no
        prior (a first write, or a reset), of every commit."""

        if not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        rows = frames.rows_of(write.rows, output.name)
        if not rows and prior is not None:
            return False
        rows = [{**row, SEQ_COLUMN: i} for i, row in enumerate(rows)]
        self._ensure(cur, output, table, lambda: rows, context)
        self._delete_slice(
            cur, table, slice_where if prior is None else {**slice_where, COMMIT_COLUMN: commit_number}
        )
        self._insert(
            cur, output, table, rows, {**self._stamps(output, context), COMMIT_COLUMN: commit_number}
        )
        return True

    def _apply_keyed(self, cur, output, write: KeyedWrite, context, table, slice_where) -> None:
        """Every key is the group of rows that carry it. A whole write is the
        partition's content: cleared, then written; otherwise only the keys it
        writes change — their rows replaced by their groups, a page at a
        time — and its removes go, every other row untouched. A write of
        nothing still runs, fencing the partition: the worker gives one only
        to a repair, whose generation the partition must then read as written
        (docs/versions.md §5)."""

        self._ensure(
            cur, output, table, lambda: write.prepared.take(None), context, kinds=write.prepared.kinds
        )
        stamps = self._stamps(output, context)
        if write.reset:
            self._delete_slice(cur, table, slice_where)
        for chunk in write.iter_chunks():
            keys = [key for key, _ in chunk]
            if not write.reset:
                self._delete_keys(cur, output, table, slice_where, keys)
            self._insert(cur, output, table, [row for _, group in chunk for row in group], stamps)
            self._check_keys(cur, output, table, slice_where, keys)
        if write.removes and not write.reset:
            self._delete_keys(cur, output, table, slice_where, sorted(write.removes))

    def _check_keys(self, cur, output, table, slice_where, keys: list[str]) -> None:
        """Every key written is stored as itself: the key column's text of its
        rows is the key the index holds. A column may change any other value
        — round it, pad it — but a key it changed (`"01"` in a bigint
        column, stored as `1`) would leave the index naming rows no read or
        delete finds (docs/versions.md §4)."""

        if not keys:
            return
        key = _ident(output.key)
        found = cur.execute(
            f"SELECT count(DISTINCT {key}::text) AS n FROM {table} "
            f"WHERE {self._where_sql(slice_where)} AND {key}::text = ANY(%s)",
            [slice_where[k] for k in sorted(slice_where)] + [keys],
        ).fetchone()["n"]
        if found != len(keys):
            stored = {
                r["k"]
                for r in cur.execute(
                    f"SELECT DISTINCT {key}::text AS k FROM {table} "
                    f"WHERE {self._where_sql(slice_where)} AND {key}::text = ANY(%s)",
                    [slice_where[k] for k in sorted(slice_where)] + [keys],
                )
            }
            lost = next(k for k in keys if k not in stored)
            raise WriteError(
                f"{output.name}: key {lost!r} would be stored as another key in column {output.key!r}"
            )

    def _stamps(self, output, context) -> dict:
        """The columns the store sets on every row: the partition's."""

        column = output.config.get("partition_column")
        return {column: context.partition} if column else {}

    def _delete_keys(self, cur, output, table, slice_where, keys: list[str]) -> None:
        cur.execute(
            f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND {_ident(output.key)}::text = ANY(%s)",
            [slice_where[k] for k in sorted(slice_where)] + [keys],
        )

    def _apply_sql(self, cur, output, write: Sql, context, table, slice_where) -> None:
        """Materialize a query into the partition. The query is never a statement
        of its own: the store embeds it in one, `INSERT INTO t SELECT … FROM
        (<query>) _src`, prepared (the extended protocol), so UPDATE, DELETE,
        DDL and data-modifying CTEs do not parse, and a second statement is
        refused. What remains is a function the query calls, which must not
        write; `sql_read_only` makes sure. The worker never sees these rows:
        a keyed output reports the partition's keys (§6, §9)."""

        import psycopg
        from psycopg import sql

        # On a line of its own: a trailing `--` comment ends with the query.
        query = f"(\n{write.stmt}\n) _src"
        partition_col = output.config.get("partition_column")
        with contextlib.ExitStack() as stack:
            source = cur
            if self.sql_read_only:  # the query reads at its own snapshot, writing nothing
                reader = stack.enter_context(psycopg.connect(resolve_env(self.dsn)))
                reader.read_only, reader.isolation_level = True, psycopg.IsolationLevel.REPEATABLE_READ
                source = stack.enter_context(reader.cursor())
            try:
                probe = source.execute(f"SELECT * FROM {query} LIMIT 0", prepare=True)
            except (psycopg.errors.SyntaxError, psycopg.errors.FeatureNotSupported) as e:
                raise WriteError(
                    f"{output.name}: a Sql write is one query — a SELECT, VALUES or TABLE — the store "
                    f"materializes; change rows through the output's writes, the table through a "
                    f"Migration ({str(e).splitlines()[0]})"
                ) from e
            described = [(d.name, d.type_code) for d in probe.description]
            columns = [name for name, _ in described if name != partition_col]
            # The query's own column types, for a table it creates; declared ones win.
            types = {
                r["oid"]: r["t"]
                for r in cur.execute(
                    "SELECT oid, format_type(oid, NULL) AS t FROM pg_type WHERE oid = ANY(%s)",
                    ([oid for _, oid in described],),
                )
            }
            inferred = {name: _inferred(types.get(oid)) for name, oid in described}
            self._ensure(cur, output, table, context=context, inferred=inferred)
            self._delete_slice(cur, table, slice_where)
            selected = ", ".join(_ident(c) for c in columns)
            into = ", ".join(_ident(c) for c in [*columns, *([partition_col] if partition_col else [])])
            stamp = f", {sql.Literal(context.partition).as_string(source)}" if partition_col else ""
            if not self.sql_read_only:
                cur.execute(
                    f"INSERT INTO {table} ({into}) SELECT {selected}{stamp} FROM {query}", prepare=True
                )
            else:
                try:
                    with (
                        source.copy(f"COPY (SELECT {selected}{stamp} FROM {query}) TO STDOUT") as rows,
                        cur.copy(f"COPY {table} ({into}) FROM STDIN") as copy,
                    ):
                        for data in rows:
                            copy.write(data)
                except psycopg.errors.ReadOnlySqlTransaction as e:
                    raise WriteError(
                        f"{output.name}: a Sql query must not write, nor any function it calls "
                        f"({str(e).splitlines()[0]})"
                    ) from e

    def prepare(self, write, output: Output) -> Prepared:
        """A keyed write of plain Python, DataFrames or Arrow (`frames`)."""

        return frames.prepare(write, output)

    def keys(self, ref: Ref, among: list[str] | None = None):
        """The keys `ref`'s partition holds — among `among`, or all of them —
        sorted by their bytes, a chunk at a time: never a value
        (docs/versions.md §5)."""

        handle = ref.handle or {}
        return self._keys(handle["table"], handle["key"], dict(handle.get("where") or {}), among)

    def _keys(self, table, key_column: str, where: dict, among: list[str] | None):
        """The partition's keys (as text) sorted by their bytes, a chunk at a time
        from a server-side cursor, read once the iteration starts — after a
        write has committed."""

        import psycopg

        key = _ident(key_column)
        params = [where[k] for k in sorted(where)]
        only = ""
        if among is not None:
            only, params = f" AND {key}::text = ANY(%s)", [*params, sorted(among)]
        with psycopg.connect(resolve_env(self.dsn)) as conn, conn.cursor(name="solera_keys") as cur:
            cur.execute(
                f"SELECT DISTINCT {key}::text, convert_to({key}::text, 'UTF8') FROM {table} "
                f"WHERE {self._where_sql(where)}{only} ORDER BY 2",
                params,
            )
            while chunk := cur.fetchmany(KEY_CHUNK):
                yield [k for k, _ in chunk]

    # -- migrations (§4) --------------------------------------------------------

    def _ensure_ledger(self, cur):
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
            "relation text NOT NULL, name text NOT NULL, "
            "at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (relation, name))"
        )

    async def migrate(
        self, output: Output, migrations, context: WriteContext | None = None, prior: Ref | None = None
    ) -> list[str]:
        """Apply pending migrations in declared order; each migration and its
        ledger row commit in one transaction under the table's locks, so
        concurrent attempts apply each exactly once (§4). The
        ledger and the lock are keyed by the schema-qualified table a
        migration changes, never by the output's name: two projects, or a
        staging and a production namespace, writing `orders` into schemas of
        their own each get it applied.

        A migration changes the whole table, so it holds the table's write
        domain exclusively (`_domain`): it waits for every partition's open
        write transaction, a partition's first included, and holds off new
        ones until it commits. Before it changes anything it takes the
        attempt's own partition, so an older attempt's migration is refused
        (docs/lifecycle.md §9.7). An operator's migration (no partition) only
        takes its turn. The table is the committed head's (`prior`), else the
        declaration's."""

        return await asyncio.to_thread(self._migrate, output, migrations, context, prior)

    def _migrate(
        self, output: Output, migrations, context: WriteContext | None, prior: Ref | None
    ) -> list[str]:

        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_ledger(cur)
        applied = []
        table, _, _ = self._table(output, prior)
        for migration in migrations:
            with self._connect() as conn, conn.cursor() as cur:
                # The table's write domain, then the table lock, as a writer takes them
                # (`_store`, `_ensure`): the exclusive domain also serializes migrations.
                self._domain(cur, table, exclusive=True)
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
                before = self._relid(cur, table)
                done = cur.execute(
                    f"SELECT 1 FROM {LEDGER_TABLE} WHERE relation = %s AND name = %s",
                    (table, migration.name),
                ).fetchone()
                if done:
                    applied.append(migration.name)
                    continue
                if before is not None and context is not None and context.generation is not None:
                    self._fence_table(cur)
                    self._take(cur, before, context)
                if isinstance(migration.payload, str):
                    cur.execute(migration.payload)
                elif callable(migration.payload):
                    migration.payload(cur)
                else:
                    raise StoreError(
                        f"{output.name}: migration {migration.name!r} payload must be "
                        "a SQL string or a callable taking a cursor"
                    )
                after = self._relid(cur, table)
                if before is not None and after is not None and after != before:
                    # The migration replaced the relation: its partitions keep their generations.
                    self._fence_table(cur)
                    cur.execute(f"UPDATE {FENCE_TABLE} SET relid = %s WHERE relid = %s", (after, before))
                cur.execute(
                    f"INSERT INTO {LEDGER_TABLE} (relation, name) VALUES (%s, %s)",
                    (table, migration.name),
                )
                applied.append(migration.name)
        return applied

    # -- reads ----------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Commits | None) -> Any:
        if isinstance(t, type) and issubclass(t, Ref):
            return ref
        return await asyncio.to_thread(self._load, ref, t, selection)

    @contextlib.asynccontextmanager
    async def reads(self):
        """An attempt's reads of this store, at one moment (docs/stores.md,
        "What a read sees"): every load of the reader runs in one REPEATABLE
        READ, READ ONLY transaction, and returns with the generation whose
        write last changed its partition, read in the same snapshot."""

        import psycopg

        conn = await asyncio.to_thread(self._connect)
        conn.read_only, conn.isolation_level = True, psycopg.IsolationLevel.REPEATABLE_READ
        try:
            yield _Reader(self, conn)
        finally:
            await asyncio.to_thread(conn.close)

    def _written(self, cur, ref: Ref) -> int | None:
        """The generation whose write transaction last changed `ref`'s partition,
        as the transaction's snapshot sees it; None if none was fenced."""

        if not cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (FENCE_TABLE,)).fetchone()["ok"]:
            return None
        found = cur.execute(
            f"SELECT written FROM {FENCE_TABLE} WHERE relid = to_regclass(%s) AND part = %s",
            ((ref.handle or {}).get("table"), ref.partition),
        ).fetchone()
        return None if found is None or found["written"] is None else int(found["written"])

    def _load(self, ref: Ref, t, selection: Keys | Commits | None, conn=None) -> Any:
        handle = ref.handle or {}
        with contextlib.nullcontext(conn) if conn else self._connect() as conn, conn.cursor() as cur:
            table = handle.get("table") or _qname(handle.get("schema", "public"), handle["name"])
            where = dict(handle.get("where") or {})
            sql, params = f"SELECT * FROM {table}", []
            clauses = [f"{_ident(k)} = %s" for k in sorted(where)]
            params += [where[k] for k in sorted(where)]
            if handle.get("commit_number") is not None:
                clauses.append(f"{_ident(COMMIT_COLUMN)} <= %s")
                params.append(handle["commit_number"])
            if isinstance(selection, Commits):
                clauses.append(f"{_ident(COMMIT_COLUMN)} BETWEEN %s AND %s")
                params += [selection.lo, selection.hi]
            elif selection is not None:
                key_col = handle.get("key")
                if not key_col:
                    raise StoreError(f"{ref.output}: no key column for a Keys selection")
                clauses.append(f"{_ident(key_col)}::text = ANY(%s)")
                params.append(sorted(selection.generations))
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            found = cur.execute(sql, params)
            rows = found.fetchall()
            columns = [d.name for d in found.description or ()]
            if handle.get("commit_number") is not None:
                internal = {COMMIT_COLUMN, SEQ_COLUMN}
                rows = [{k: v for k, v in r.items() if k not in internal} for r in rows]
        inner = by_key_type(t)
        if inner is not MISSING and isinstance(selection, Keys):
            # Each selected key's group: a key with no rows does not exist (per-key §6).
            groups: dict[str, list] = {}
            for row in rows:
                groups.setdefault(str(row[handle["key"]]), []).append(row)
            return {k: _materialize(g, inner, columns) for k, g in groups.items()}
        return _materialize(rows, t)

    # -- row helpers ----------------------------------------------------------

    def _where_sql(self, where: dict) -> str:
        if not where:
            return "TRUE"
        return " AND ".join(f"{_ident(k)} = %s" for k in sorted(where))

    def _delete_slice(self, cur, table, where: dict):
        params = [where[k] for k in sorted(where)]
        cur.execute(f"DELETE FROM {table} WHERE {self._where_sql(where)}", params)

    def _insert(self, cur, output, table, rows: list[dict], stamps: dict):
        """Rows, by `COPY`: every column any row has, missing ones null, and
        the `stamps` the store sets on every row."""

        if not rows:
            return
        for column, value in stamps.items():
            if any(column in row and str(row[column]) != str(value) for row in rows):
                raise WriteError(f"{output.name}: a row's {column} disagrees with {value!r}")
        columns = sorted({c for r in rows for c in r} - set(stamps))
        constant = list(stamps.values())
        names = ", ".join(_ident(c) for c in [*columns, *stamps])
        with cur.copy(f"COPY {table} ({names}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row([row.get(c) for c in columns] + constant)


# The column a table the write creates gives each kind.
_COLUMNS = {
    "boolean": "boolean",
    "integer": "bigint",
    "float": "double precision",
    "decimal": "numeric",
    "text": "text",
    "instant": "timestamptz",
    "timestamp": "timestamp",
    "date": "date",
    "time": "time",
    "interval": "interval",
    "bytes": "bytea",
}


def _column_types(name: str, rows: Iterable[dict], declared: dict) -> dict[str, str]:
    """The columns a table the write creates gets for the undeclared ones:
    each from the kind of every value not null it holds. Two kinds in one
    column, or none at all (only nulls), want a declaration."""

    kinds: dict[str, set] = {}
    for row in rows:
        for column, value in row.items():
            if column in declared:
                continue
            found = kinds.setdefault(column, set())
            if value is not None:
                found.add(frames.kind_of(value))
    columns = {}
    for column, found in kinds.items():
        if len(found) != 1 or None in found:
            what = "only nulls" if not found else " and ".join(sorted(k or "untyped" for k in found))
            raise WriteError(
                f"{name}: column {column!r} holds {what}: declare its type, "
                f"Output(..., columns={{{column!r}: ...}})"
            )
        columns[column] = _COLUMNS[found.pop()]
    return columns


def _inferred(sql_type: str | None) -> str:
    """A column a SELECT produced, as a table declares it: its own type; a
    literal Postgres never typed (`unknown`), text."""

    return "text" if sql_type in (None, "unknown") else sql_type


class _Reader:
    """`PostgresStore.reads()`: loads in one snapshot, one at a time."""

    def __init__(self, store: PostgresStore, conn):
        self.store, self.conn, self.lock = store, conn, asyncio.Lock()

    async def load(self, ref: Ref, t, selection: Keys | Commits | None) -> tuple[Any, int | None]:
        if isinstance(t, type) and issubclass(t, Ref):
            return ref, None  # nothing read: the producer reads it itself

        def work():
            with self.conn.cursor() as cur:
                written = self.store._written(cur, ref)
            return self.store._load(ref, t, selection, self.conn), written

        async with self.lock:
            return await asyncio.to_thread(work)


def _sql_type(decl: str) -> str:
    return re.sub(r"\bstring\b", "text", str(decl))


def _materialize(rows: list[dict], t, columns: list[str] | None = None):
    import typing

    if (
        t is None
        or t is list
        or t is dict
        or (typing.get_origin(t) is list and typing.get_args(t) == (dict,))
    ):
        return rows
    if getattr(t, "__module__", "").split(".")[0] in ("pandas", "geopandas") and getattr(
        t, "__name__", ""
    ) in ("DataFrame", "GeoDataFrame"):
        import pandas as pd

        return pd.DataFrame(rows, columns=columns if not rows and columns else None)
    return rows
