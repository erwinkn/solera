"""PostgresStore: shared mutable tables (§3, §4). Reads are not pinned: a
ref names a table slice, and a load reads what it holds now.

A `fenced` store (docs/lifecycle.md §9.7): each attempt takes its generation
for the slice it writes — keyed by the table's OID, which survives renames,
and the partition — before it reads anything, and every write transaction
checks it under the row's lock. A newer attempt's acquisition waits for an
older writer's open transaction, after which that writer can change nothing.

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the harness).
"""

from __future__ import annotations

import re
from typing import Any

from solera.sdk import KEYS, Output, Ref, TableRef, digest
from solera.stores import (
    MISSING,
    Batches,
    Keys,
    Patch,
    Scope,
    Sql,
    StoreError,
    WriteError,
    Written,
    by_key_type,
    prepare_for,
    resolve_env,
)

LEDGER_TABLE = "public.solera_migrations"
FENCE_TABLE = "public.solera_generations"
BATCH_COLUMN = "_batch"
SEQ_COLUMN = "_seq"
KEY_CHUNK = 100_000  # rows per chunk a keyed Sql write reports


def _assigned_batch(scope: Scope, prior: Ref | None) -> int:
    if scope.batch is not None:
        return scope.batch
    last = (prior.handle or {}).get("batch") if prior is not None else None
    return int(last) + 1 if last is not None else 0


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
        raise StoreError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


def _qname(schema: str, table: str) -> str:
    return f"{_ident(schema)}.{_ident(table)}"


def _is_select(stmt: str) -> bool:
    return stmt.lstrip().split(None, 1)[0].lower() in ("select", "with", "values")


class PostgresStore:
    version = "1"
    ref_type = TableRef
    shared_table = True
    writes = "fenced"

    def __init__(self, dsn: str, grants: list[str] | tuple = ()):
        self.dsn, self.grants = dsn, tuple(grants)

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
        if output.is_partition_set or output.key == KEYS:
            return False  # partition sets and dict outputs live on the default store
        return True  # DataFrame / list[dict] / Patch / Sql / unannotated

    # -- plumbing -----------------------------------------------------------

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(resolve_env(self.dsn), row_factory=dict_row, autocommit=False)

    def _table(self, output: Output) -> str:
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
        batch_mode = output.incremental and output.key is None
        if batch_mode:
            columns.setdefault(BATCH_COLUMN, "integer")
            columns.setdefault(SEQ_COLUMN, "integer")
        # A key names a group of rows, not one: it is never a primary key by itself.
        pk = list(output.config.get("primary_key") or [])
        if partition_col and partition_col not in pk and (pk or batch_mode):
            pk = [*pk, partition_col]
        if batch_mode:
            for c in (BATCH_COLUMN, SEQ_COLUMN):
                if c not in pk:
                    pk = [*pk, c]
        return columns, pk

    def _ensure(self, cur, output: Output, rows: list[dict] | None = None, scope: Scope | None = None):
        table, schema, table_name = self._table(output)
        indexes = self._indexes(output)
        names = [table_name + "_" + "_".join(index) for index in indexes]
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
        columns = dict(declared)
        if rows:
            for row in rows:
                for column, value in row.items():
                    columns.setdefault(column, _column_type(value))
        existed = cur.execute(exists, (schema, table_name)).fetchone()
        defs = [f"{_ident(c)} {_sql_type(t)}" for c, t in (columns or {"value": "jsonb"}).items()]
        if pk:
            defs.append(f"PRIMARY KEY ({', '.join(_ident(c) for c in pk)})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(defs)})")
        if existed:
            self._check_drift(cur, output, table, schema, table_name, columns, pk)
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
        self._fence(cur, table, scope)  # before this transaction changes any row
        return table

    # -- generations (docs/lifecycle.md §9.7) -------------------------------------

    def _fence_table(self, cur) -> None:
        if cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (FENCE_TABLE,)).fetchone()["ok"]:
            return
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (FENCE_TABLE,))
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {FENCE_TABLE} (relid oid NOT NULL, part text NOT NULL, "
            "generation bigint NOT NULL, invocation text NOT NULL, PRIMARY KEY (relid, part))"
        )

    def _take(self, cur, relid: int, scope: Scope) -> None:
        """Take `scope`'s generation for (relid, partition), holding the row's
        lock until the transaction ends. Postgres locks the conflicting row
        even when the `WHERE` refuses the update, so a newer acquisition waits
        for an older writer's open transaction."""

        taken = cur.execute(
            f"INSERT INTO {FENCE_TABLE} VALUES (%s, %s, %s, %s) ON CONFLICT (relid, part) "
            f"DO UPDATE SET generation = EXCLUDED.generation, invocation = EXCLUDED.invocation "
            f"WHERE {FENCE_TABLE}.generation < EXCLUDED.generation "
            f"OR ({FENCE_TABLE}.generation = EXCLUDED.generation AND {FENCE_TABLE}.invocation = EXCLUDED.invocation) "
            "RETURNING invocation",
            (relid, scope.partition, scope.generation, scope.invocation),
        ).fetchone()
        if taken is None:
            raise StoreError(
                f"{scope.output.name}: a newer attempt holds this slice (generation {scope.generation} "
                f"of {scope.invocation} refused)"
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

    def _fence(self, cur, table: str, scope: Scope | None) -> None:
        """In a write transaction, before it changes anything: the slice must
        still be this attempt's (`_take` is a no-op for its own generation
        and invocation). A slice it never acquired — a table this
        transaction created, a new partition — is acquired here."""

        if scope is None or scope.generation is None:
            return
        self._fence_table(cur)
        self._take(cur, self._relid(cur, table), scope)

    async def acquire(self, scope: Scope) -> None:
        """Take the attempt's generation for the slice it writes, in a
        transaction of its own, before any read of the store: from here on
        no older attempt can change it. A table that does not exist yet is
        acquired when the first write creates it."""

        if scope.generation is None:
            return
        table, _, _ = self._table(scope.output)
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table)
            self._rename(cur, scope.output, scope)
            relid = self._relid(cur, table)
            if relid is None:
                return
            self._fence_table(cur)
            self._take(cur, relid, scope)

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

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        import psycopg

        try:
            return self._store(write, prior, scope)
        except psycopg.IntegrityError as e:  # the data breaks the table's constraints (`primary_key`)
            raise WriteError(f"{scope.output.name}: {e}") from e

    def _store(self, write, prior: Ref | None, scope: Scope) -> Written:
        output = scope.output
        table, _, _ = self._table(output)
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table)
            self._rename(cur, output, scope)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: scope.partition} if partition_col else {}

            batch = _assigned_batch(scope, prior) if output.incremental else None
            keys = None
            if isinstance(write, Sql):
                version, keys = self._apply_sql(cur, output, write, scope, table, slice_where, prior)
            elif output.key is not None:
                if isinstance(write, Patch) and not output.incremental:
                    raise WriteError(f"{output.name}: Patch requires an incremental output")
                prepared = scope.prepared or prepare_for(self, write, output)
                version = self._apply_keyed(cur, output, prepared, scope, table, slice_where, prior)
                if version is None:
                    return Written(prior)
            elif isinstance(write, Patch):
                version = self._apply_batch(cur, output, write, scope, table, slice_where, prior, batch)
                if version is None:
                    return Written(prior)
            else:
                if output.incremental:
                    raise WriteError(
                        f"{output.name}: an unkeyed incremental output only accepts Patch writes"
                    )
                version = self._apply_replace(cur, output, write, scope, table, slice_where)
        batch_mode = output.key is None and output.incremental
        return Written(
            TableRef(
                output=output.name,
                store="",
                handle={
                    "table": table,
                    "where": slice_where,
                    "key": BATCH_COLUMN if batch_mode else output.key,
                    "revision": output.revision,
                    "batch": batch if batch_mode else None,
                },
                version=version,
                partition=scope.partition,
            ),
            keys,
        )

    def _rename(self, cur, output, scope):
        """An output renamed through its asset's aliases (§2) takes its table
        along, once: the first write under the new name."""

        if not scope.aliases or "table" in output.config:
            return
        schema = output.config.get("schema", "public")
        exists = "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
        if cur.execute(exists, (schema, output.name)).fetchone():
            return
        for alias in scope.aliases:
            if cur.execute(exists, (schema, alias)).fetchone():
                cur.execute(f"ALTER TABLE {_qname(schema, alias)} RENAME TO {_ident(output.name)}")
                return

    def _stamp(self, output, rows: list[dict], scope) -> list[dict]:
        """Rows with the partition column set to the scope's; a row that says
        another partition is a write error."""

        if partition_col := output.config.get("partition_column"):
            for row in rows:
                if partition_col in row and str(row[partition_col]) != scope.partition:
                    raise WriteError(
                        f"{output.name}: row {partition_col}={row[partition_col]!r} disagrees "
                        f"with scope {scope.partition!r}"
                    )
                row[partition_col] = scope.partition
        return rows

    def _apply_replace(self, cur, output, write, scope, table, slice_where):
        """An unkeyed output's whole content: its version is the multiset of
        its rows (docs/row-digest.md), before the store stamps them."""

        rows = _coerce_rows(write)
        version = _rows_version(rows, [])
        self._ensure(cur, output, self._stamp(output, rows, scope), scope)
        self._delete_slice(cur, table, slice_where)
        self._insert(cur, table, rows)
        return version

    def _apply_batch(self, cur, output, write: Patch, scope, table, slice_where, prior, batch):
        """An unkeyed incremental output's batch: its rows stamped with the
        batch columns, in place of this batch's (a retry's) — or, with no
        prior (a full run), of every batch."""

        if not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        rows = _coerce_rows(write.rows)
        if not rows and prior is not None:
            return None
        version = _rows_version(rows, [prior.version if prior else ""])
        for i, row in enumerate(self._stamp(output, rows, scope)):
            row[BATCH_COLUMN] = batch
            row[SEQ_COLUMN] = i
        self._ensure(cur, output, rows, scope)
        self._delete_slice(cur, table, slice_where if prior is None else {**slice_where, BATCH_COLUMN: batch})
        self._insert(cur, table, rows)
        return version

    def _apply_keyed(self, cur, output, prepared, scope, table, slice_where, prior):
        """Every key is the group of rows that carry it. With no selection —
        a first write, a full run, or more changes than the harness lists —
        a replacement is the slice's whole content; otherwise only the
        selected keys change: their rows replaced by their groups, removed
        keys' rows gone, every other row untouched. A patch with no selection
        changes its own keys and removes."""

        nothing = not len(prepared.rows) and not prepared.removes and not prepared.entries()
        if prepared.patch and prior is not None and nothing:
            return None  # the prior stands
        if prior is None or (scope.upserts is None and not prepared.patch):
            rows = self._stamp(output, [dict(r) for r in prepared.all_rows()], scope)
            self._ensure(cur, output, rows, scope)
            self._delete_slice(cur, table, slice_where)
        else:
            keys = sorted(scope.upserts) if scope.upserts is not None else [k for k, _ in prepared.entries()]
            groups = prepared.groups(keys)
            rows = self._stamp(output, [dict(row) for group in groups for row in group], scope)
            self._ensure(cur, output, rows, scope)
            removes = scope.removes if scope.removes is not None else prepared.removes
            gone = sorted(set(keys) | set(removes))
            if gone:
                cur.execute(
                    f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} "
                    f"AND {_ident(output.key)}::text = ANY(%s)",
                    ([slice_where[k] for k in sorted(slice_where)] + [gone]),
                )
        self._insert(cur, table, rows)
        return prepared.version(prior)

    def _apply_sql(self, cur, output, write: Sql, scope, table, slice_where, prior):
        """Materialize a SELECT into the slice, or run a statement verbatim. The
        harness never sees these rows, so a keyed output reports the slice's
        rows, sorted, for the harness to version (§6, §9)."""

        if _is_select(write.stmt):
            probe = cur.execute(f"SELECT * FROM ({write.stmt}) _probe LIMIT 0")
            columns = [d.name for d in probe.description]
            partition_col = output.config.get("partition_column")
            if partition_col and partition_col not in columns:
                columns.append(partition_col)
            declared = dict(output.config.get("columns") or {})
            for c in columns:
                declared.setdefault(c, "text")
            output.config["columns"] = declared
            self._ensure(cur, output, scope=scope)
            self._delete_slice(cur, table, slice_where)
            select_cols = ", ".join(_ident(c) for c in columns if c != partition_col)
            if partition_col:
                cur.execute(
                    f"INSERT INTO {table} ({select_cols}, {_ident(partition_col)}) "
                    f"SELECT {select_cols}, %s FROM ({write.stmt}) _src",
                    (scope.partition,),
                )
            else:
                cur.execute(
                    f"INSERT INTO {table} ({select_cols}) SELECT {select_cols} FROM ({write.stmt}) _src"
                )
        else:
            self._ensure(cur, output, scope=scope)
            relid = self._relid(cur, table)
            cur.execute(write.stmt)
            if self._relid(cur, table) != relid:  # rolled back: its fence would not follow it
                raise WriteError(
                    f"{output.name}: a Sql statement must keep {table} itself; replace it through a Migration"
                )
        keys = self._sorted_rows(table, output, slice_where) if output.key else None
        return digest([prior.version if prior else "", digest(write.stmt)]), keys

    def stamped(self, output: Output) -> tuple[str, ...]:
        """Columns the store adds to every row — the partition column — which
        a row's digest leaves out, however it is read (docs/row-digest.md)."""

        column = output.config.get("partition_column")
        return (column,) if column and column != output.key else ()

    def scan(self, ref: Ref, output: Output, skip=()):
        """A slice's rows as they are, sorted by key, a chunk at a time — as a
        `Sql` write reports them — but those of the keys in `skip`."""

        handle = ref.handle or {}
        return self._sorted_rows(handle["table"], output, dict(handle.get("where") or {}), skip)

    def _sorted_rows(self, table, output: Output, where: dict, skip=()):
        """The slice's rows sorted by the key's bytes, a chunk at a time from a
        server-side cursor once the write has committed, for the harness to
        version as it versions any rows: the key (as text) and the declared
        revision, or every column but the partition column, typed. Keys in
        `skip` are left out."""

        import psycopg
        from psycopg.rows import dict_row

        key = _ident(output.key)
        params = [where[k] for k in sorted(where)]
        with psycopg.connect(resolve_env(self.dsn), row_factory=dict_row) as conn:
            if output.revision:
                columns = [output.revision]
            else:
                probe = conn.execute(f"SELECT * FROM {table} LIMIT 0")
                columns = [d.name for d in probe.description if d.name != output.key and d.name not in where]
            select = ", ".join([f"{key}::text AS {key}", *(_ident(c) for c in columns)])
            with conn.cursor(name="solera_keys") as cur:
                cur.execute(
                    f"SELECT {select} FROM {table} WHERE {self._where_sql(where)} "
                    f"AND NOT ({key}::text = ANY(%s)) ORDER BY convert_to({key}::text, 'UTF8')",
                    [*params, sorted(skip)],
                )
                while chunk := cur.fetchmany(KEY_CHUNK):
                    yield chunk

    # -- migrations (§4) --------------------------------------------------------

    def _ensure_ledger(self, cur):
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
            "output text NOT NULL, name text NOT NULL, "
            "at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (output, name))"
        )

    async def migrate(self, output: Output, migrations, scope: Scope | None = None) -> list[str]:
        """Apply pending migrations in declared order; each migration and its
        ledger row commit in one transaction under an advisory lock keyed on
        the output, so concurrent attempts apply each exactly once (§4).

        A migration changes the whole table, so it holds the table's write
        domain exclusively (`_domain`): it waits for every partition's open
        write transaction, a partition's first included, and holds off new
        ones until it commits. Before it changes anything it takes the
        attempt's own slice, so an older attempt's migration is refused
        (docs/lifecycle.md §9.7). An operator's migration (no scope) only
        takes its turn."""

        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_ledger(cur)
        applied = []
        table, _, _ = self._table(output)
        for migration in migrations:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (output.name,))
                self._domain(cur, table, exclusive=True)
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
                before = self._relid(cur, table)
                done = cur.execute(
                    f"SELECT 1 FROM {LEDGER_TABLE} WHERE output = %s AND name = %s",
                    (output.name, migration.name),
                ).fetchone()
                if done:
                    applied.append(migration.name)
                    continue
                if before is not None and scope is not None and scope.generation is not None:
                    self._fence_table(cur)
                    self._take(cur, before, scope)
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
                    # The migration replaced the relation: its slices keep their generations.
                    self._fence_table(cur)
                    cur.execute(f"UPDATE {FENCE_TABLE} SET relid = %s WHERE relid = %s", (after, before))
                cur.execute(
                    f"INSERT INTO {LEDGER_TABLE} (output, name) VALUES (%s, %s)",
                    (output.name, migration.name),
                )
                applied.append(migration.name)
        return applied

    # -- reads ----------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any:
        if isinstance(t, type) and issubclass(t, Ref):
            return ref
        handle = ref.handle or {}
        with self._connect() as conn, conn.cursor() as cur:
            table = handle.get("table") or _qname(handle.get("schema", "public"), handle["name"])
            where = dict(handle.get("where") or {})
            sql, params = f"SELECT * FROM {table}", []
            clauses = [f"{_ident(k)} = %s" for k in sorted(where)]
            params += [where[k] for k in sorted(where)]
            if handle.get("batch") is not None:
                clauses.append(f"{_ident(BATCH_COLUMN)} <= %s")
                params.append(handle["batch"])
            if isinstance(selection, Batches):
                clauses.append(f"{_ident(BATCH_COLUMN)} BETWEEN %s AND %s")
                params += [selection.lo, selection.hi]
            elif selection is not None:
                key_col = handle.get("key")
                if not key_col:
                    raise StoreError(f"{ref.output}: no key column for a Keys selection")
                clauses.append(f"{_ident(key_col)}::text = ANY(%s)")
                params.append(sorted(selection.revisions))
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            found = cur.execute(sql, params)
            rows = found.fetchall()
            columns = [d.name for d in found.description or ()]
            if handle.get("batch") is not None:
                internal = {BATCH_COLUMN, SEQ_COLUMN}
                rows = [{k: v for k, v in r.items() if k not in internal} for r in rows]
        inner = by_key_type(t)
        if inner is not MISSING and isinstance(selection, Keys):
            # Each selected key's group, an empty one included (per-key §6).
            groups: dict[str, list] = {k: [] for k in sorted(selection.revisions)}
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

    def _insert(self, cur, table, rows: list[dict]):
        if not rows:
            return
        columns = sorted({c for r in rows for c in r})
        for row in rows:
            cur.execute(
                f"INSERT INTO {table} ({', '.join(_ident(c) for c in columns)}) "
                f"VALUES ({', '.join('%s' for _ in columns)})",
                [row.get(c) for c in columns],
            )


def _coerce_rows(write: Any) -> list[dict]:
    if write is None:
        return []
    if type(write).__name__ in ("DataFrame", "GeoDataFrame") and type(write).__module__.split(".")[0] in (
        "pandas",
        "geopandas",
    ):
        return write.to_dict(orient="records")
    if isinstance(write, list) and all(isinstance(r, dict) for r in write):
        return [dict(r) for r in write]
    raise WriteError(f"Expected rows (list[dict] or DataFrame), got {type(write).__name__}")


def _rows_version(rows: list[dict], before: list) -> str:
    """A version from rows as a multiset (`group`, docs/row-digest.md) and
    what comes before them (a prior version)."""

    from solera._native import group_digest

    try:
        content = group_digest(rows).hex()
    except ValueError as e:
        raise WriteError(str(e)) from e
    return digest([*before, content])


def _column_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    return "text"


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
