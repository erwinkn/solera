"""PostgresStore: shared mutable tables (§3, §4). Reads are not pinned: a
ref names a table slice, and a load reads what it holds now; the engine's
write fence (§8) keeps a dead attempt from writing over a live one.

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the harness).
"""

from __future__ import annotations

import re
from typing import Any

from solera.sdk import KEYS, Output, Ref, TableRef, digest
from solera.stores import (
    Batches,
    Keys,
    Patch,
    Scope,
    Sql,
    StoreError,
    WriteError,
    Written,
    key_map,
    resolve_env,
)

LEDGER_TABLE = "public.solera_migrations"
BATCH_COLUMN = "_batch"
SEQ_COLUMN = "_seq"
KEY_CHUNK = 100_000  # (key, version) pairs per chunk a keyed Sql write reports


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

    def __init__(self, dsn: str, grants: list[str] | tuple = ()):
        self.dsn, self.grants = dsn, tuple(grants)

    # -- registration -------------------------------------------------------

    def can_load(self, t, selection) -> bool:
        if t is None:
            return selection is None
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
        pk = list(output.config.get("primary_key") or ([output.key] if output.key else []))
        if partition_col and partition_col not in pk and (pk or batch_mode):
            pk = [*pk, partition_col]
        if batch_mode:
            for c in (BATCH_COLUMN, SEQ_COLUMN):
                if c not in pk:
                    pk = [*pk, c]
        return columns, pk

    def _ensure(self, cur, output: Output, rows: list[dict] | None = None):
        table, schema, table_name = self._table(output)
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        declared, pk = self._declared_shape(output)
        columns = dict(declared)
        if rows:
            for row in rows:
                for column, value in row.items():
                    columns.setdefault(column, _column_type(value))
        existed = cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (schema, table_name),
        ).fetchone()
        defs = [f"{_ident(c)} {_sql_type(t)}" for c, t in (columns or {"value": "jsonb"}).items()]
        if pk:
            defs.append(f"PRIMARY KEY ({', '.join(_ident(c) for c in pk)})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(defs)})")
        if existed:
            self._check_drift(cur, output, table, schema, table_name, columns, pk)
        for index in output.config.get("indexes") or []:
            cols = ", ".join(_ident(c) for c in index)
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {_ident(table_name + '_' + '_'.join(index))} ON {table} ({cols})"
            )
        for role in self.grants:
            try:
                cur.execute(f"GRANT SELECT ON {table} TO {_ident(role)}")
            except Exception:
                pass  # grants are deployment sugar; a missing role is not fatal
        return table

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
        output = scope.output
        with self._connect() as conn, conn.cursor() as cur:
            self._rename(cur, output, scope)
            table, _, _ = self._table(output)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: scope.partition} if partition_col else {}

            batch = _assigned_batch(scope, prior) if output.incremental else None
            keys = None
            if isinstance(write, Sql):
                version, keys = self._apply_sql(cur, output, write, scope, table, slice_where, prior)
            elif isinstance(write, Patch):
                version = self._apply_patch(cur, output, write, scope, table, slice_where, prior, batch)
                if version is None:
                    return Written(prior)
            else:
                if output.incremental and output.key is None:
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

    def _apply_replace(self, cur, output, write, scope, table, slice_where):
        rows = _coerce_rows(write)
        if partition_col := output.config.get("partition_column"):
            for row in rows:
                if partition_col in row and str(row[partition_col]) != scope.partition:
                    raise WriteError(
                        f"{output.name}: row {partition_col}={row[partition_col]!r} disagrees "
                        f"with scope {scope.partition!r}"
                    )
                row[partition_col] = scope.partition
        if output.key is not None:
            key_map(output, rows)  # validates the key column and duplicate keys
        self._ensure(cur, output, rows)
        self._delete_slice(cur, table, slice_where)
        self._insert(cur, table, rows)
        return digest(rows)

    def _apply_patch(self, cur, output, write: Patch, scope, table, slice_where, prior, batch):
        if not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        remove = {str(k) for k in write.remove}
        rows = _coerce_rows(write.rows)
        if not rows and not remove and prior is not None:
            return None
        self._ensure(cur, output, rows)
        partition_col = output.config.get("partition_column")
        if output.key is None:
            # Batch mode: stamp the batch columns, replace this batch's rows.
            if remove:
                raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
            for i, row in enumerate(rows):
                row[BATCH_COLUMN] = batch
                row[SEQ_COLUMN] = i
                if partition_col:
                    row[partition_col] = scope.partition
            if prior is None:
                # A full run replaces the slice: supersede every prior batch.
                self._delete_slice(cur, table, slice_where)
            else:
                self._delete_slice(cur, table, {**slice_where, BATCH_COLUMN: batch})
            self._insert(cur, table, rows)
            return digest([prior.version if prior else "", digest({"rows": _canon(rows), "remove": []})])

        for row in rows:
            if partition_col:
                if partition_col in row and str(row[partition_col]) != scope.partition:
                    raise WriteError(
                        f"{output.name}: row {partition_col}={row[partition_col]!r} disagrees with scope"
                    )
                row[partition_col] = scope.partition
        key_map(output, rows)  # validates the key column and duplicate keys
        if prior is None:
            # No prior (a first write or a full run): the patch is the whole state.
            self._delete_slice(cur, table, slice_where)
        self._upsert(cur, table, output, rows)
        if remove:
            cur.execute(
                f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND {_ident(output.key)}::text = ANY(%s)",
                ([slice_where[k] for k in sorted(slice_where)] + [sorted(remove)]),
            )
        return digest(
            [prior.version if prior else "", digest({"rows": _canon(rows), "remove": sorted(remove)})]
        )

    def _apply_sql(self, cur, output, write: Sql, scope, table, slice_where, prior):
        """Materialize a SELECT into the slice, or run a statement verbatim. The
        harness never sees these rows, so a keyed output reports the slice's
        complete content, sorted, as the harness reads it (§6, §9)."""

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
            self._ensure(cur, output)
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
            self._ensure(cur, output)
            cur.execute(write.stmt)
            _, schema, table_name = self._table(output)
            exists = cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                (schema, table_name),
            ).fetchone()
            if not exists:
                raise WriteError(f"{output.name}: Sql statement must leave {table} in place")
        keys = self._sorted_keys(table, output, slice_where) if output.key else None
        return digest([prior.version if prior else "", digest(write.stmt)]), keys

    def _sorted_keys(self, table, output: Output, where: dict):
        """The slice's `(key, version)` pairs sorted by the key's bytes, a chunk
        at a time from a server-side cursor once the write has committed: the
        declared revision's text, else an MD5 digest of the row's text."""

        import psycopg

        key = f"convert_to({_ident(output.key)}::text, 'UTF8')"
        if output.revision:
            version = f"convert_to({_ident(output.revision)}::text, 'UTF8')"
        else:
            version = "decode(md5(_row::text), 'hex')"
        sql = f"SELECT {key}, {version} FROM {table} _row WHERE {self._where_sql(where)} ORDER BY 1"
        with psycopg.connect(resolve_env(self.dsn)) as conn, conn.cursor(name="solera_keys") as cur:
            cur.execute(sql, [where[k] for k in sorted(where)])
            while chunk := cur.fetchmany(KEY_CHUNK):
                yield chunk

    # -- migrations (§4) --------------------------------------------------------

    def _ensure_ledger(self, cur):
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {LEDGER_TABLE} ("
            "output text NOT NULL, name text NOT NULL, "
            "at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (output, name))"
        )

    async def migrate(self, output: Output, migrations) -> list[str]:
        """Apply pending migrations in declared order; each migration and its
        ledger row commit in one transaction under an advisory lock keyed on
        the output, so concurrent attempts apply each exactly once (§4)."""

        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_ledger(cur)
        applied = []
        for migration in migrations:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (output.name,))
                done = cur.execute(
                    f"SELECT 1 FROM {LEDGER_TABLE} WHERE output = %s AND name = %s",
                    (output.name, migration.name),
                ).fetchone()
                if done:
                    applied.append(migration.name)
                    continue
                if isinstance(migration.payload, str):
                    cur.execute(migration.payload)
                elif callable(migration.payload):
                    migration.payload(cur)
                else:
                    raise StoreError(
                        f"{output.name}: migration {migration.name!r} payload must be "
                        "a SQL string or a callable taking a cursor"
                    )
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
            rows = cur.execute(sql, params).fetchall()
            if handle.get("batch") is not None:
                internal = {BATCH_COLUMN, SEQ_COLUMN}
                rows = [{k: v for k, v in r.items() if k not in internal} for r in rows]
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

    def _upsert(self, cur, table, output, rows: list[dict]):
        if not rows:
            return
        pk = list(output.config.get("primary_key") or [output.key])
        partition_col = output.config.get("partition_column")
        if partition_col and partition_col not in pk:
            pk = [*pk, partition_col]
        columns = sorted({c for r in rows for c in r})
        update = [c for c in columns if c not in pk]
        conflict = (
            f"ON CONFLICT ({', '.join(_ident(c) for c in pk)}) DO UPDATE SET "
            + ", ".join(f"{_ident(c)} = EXCLUDED.{_ident(c)}" for c in update)
            if update
            else "ON CONFLICT DO NOTHING"
        )
        for row in rows:
            cur.execute(
                f"INSERT INTO {table} ({', '.join(_ident(c) for c in columns)}) "
                f"VALUES ({', '.join('%s' for _ in columns)}) {conflict}",
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


def _canon(value):
    if type(value).__name__ in ("DataFrame", "GeoDataFrame") and type(value).__module__.split(".")[0] in (
        "pandas",
        "geopandas",
    ):
        return value.to_dict(orient="records")
    return value


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


def _materialize(rows: list[dict], t):
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

        return pd.DataFrame(rows)
    return rows
