"""PostgresStore: shared mutable tables behind version markers (§3, §4).

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the harness).
"""

from __future__ import annotations

import re
from typing import Any

from cursus.sdk import Output, Ref, TableRef, digest
from cursus.stores import (
    Batches,
    Delta,
    Keys,
    Patch,
    Scope,
    Sql,
    StaleRead,
    StoreConflict,
    StoreError,
    WriteError,
    Written,
    key_map,
    next_batch,
    resolve_env,
)

MARKER_TABLE = "public.cursus_markers"
LEDGER_TABLE = "public.cursus_migrations"
KEYMAP_TABLE = "public.cursus_keys"
BATCH_COLUMN = "_batch"
SEQ_COLUMN = "_seq"


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
        if output.is_partition_set:
            return False  # partition sets live on the default store
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
        self._ensure_markers(cur)
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

    def _ensure_markers(self, cur):
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {MARKER_TABLE} ("
            "output text NOT NULL, partition text NOT NULL, version text NOT NULL, "
            "batch integer, PRIMARY KEY (output, partition))"
        )

    def _marker(self, cur, output_name: str, partition: str) -> str | None:
        row = cur.execute(
            f"SELECT version FROM {MARKER_TABLE} WHERE output = %s AND partition = %s",
            (output_name, partition),
        ).fetchone()
        return row["version"] if row else None

    def _set_marker(self, cur, output_name: str, partition: str, version: str, batch: int | None):
        cur.execute(
            f"INSERT INTO {MARKER_TABLE} (output, partition, version, batch) VALUES (%s,%s,%s,%s) "
            "ON CONFLICT (output, partition) DO UPDATE SET version = EXCLUDED.version, batch = EXCLUDED.batch",
            (output_name, partition, version, batch),
        )

    def _marker_batch(self, cur, output_name: str, partition: str) -> int | None:
        row = cur.execute(
            f"SELECT batch FROM {MARKER_TABLE} WHERE output = %s AND partition = %s",
            (output_name, partition),
        ).fetchone()
        return row["batch"] if row else None

    def _ensure_keymap(self, cur):
        """The committed `key -> revision` map for keyed incremental outputs —
        kept transactionally beside the data so delta diffs and the orphan
        sweep never need a staged object (§2.1)."""

        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {KEYMAP_TABLE} ("
            "output text NOT NULL, partition text NOT NULL, "
            "key text NOT NULL, rev text NOT NULL, "
            "PRIMARY KEY (output, partition, key))"
        )

    def _key_revs(self, cur, output_name: str, partition: str, keys=None) -> dict:
        sql = f"SELECT key, rev FROM {KEYMAP_TABLE} WHERE output = %s AND partition = %s"
        params = [output_name, partition]
        if keys is not None:
            sql += " AND key = ANY(%s)"
            params.append([str(k) for k in keys])
        return {r["key"]: r["rev"] for r in cur.execute(sql, params).fetchall()}

    def _put_key_revs(self, cur, output_name: str, partition: str, revs: dict):
        if not revs:
            return
        pairs = [(str(k), str(v)) for k, v in revs.items()]
        cur.execute(
            f"INSERT INTO {KEYMAP_TABLE} (output, partition, key, rev) "
            "SELECT %s, %s, k, r FROM unnest(%s::text[], %s::text[]) AS u(k, r) "
            "ON CONFLICT (output, partition, key) DO UPDATE SET rev = EXCLUDED.rev",
            (output_name, partition, [p[0] for p in pairs], [p[1] for p in pairs]),
        )

    def _del_key_revs(self, cur, output_name: str, partition: str, keys=None):
        if keys is None:
            cur.execute(
                f"DELETE FROM {KEYMAP_TABLE} WHERE output = %s AND partition = %s",
                (output_name, partition),
            )
        elif keys:
            cur.execute(
                f"DELETE FROM {KEYMAP_TABLE} WHERE output = %s AND partition = %s AND key = ANY(%s)",
                (output_name, partition, [str(k) for k in keys]),
            )

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        output = scope.output
        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_markers(cur)
            self._ensure_keymap(cur)
            table, _, _ = self._table(output)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: scope.partition} if partition_col else {}

            if prior is not None:  # a full run (prior=None) skips the marker check
                live = self._marker(cur, output.name, scope.partition)
                if live != prior.version:
                    raise StoreConflict(
                        f"{output.name}/{scope.partition}: live marker {live} != pinned {prior.version}"
                    )

            batch = None
            if isinstance(write, Sql):
                version, delta = self._apply_sql(cur, output, write, scope, table, slice_where, prior)
                if version is None:
                    return Written(prior or scope.baseline)
            elif isinstance(write, Patch):
                applied = self._apply_patch(cur, output, write, scope, table, slice_where, prior)
                if applied is None:
                    return Written(prior or scope.baseline)
                version, delta, batch = applied
            else:
                if output.incremental and output.key is None:
                    raise WriteError(
                        f"{output.name}: an unkeyed incremental output only accepts Patch writes"
                    )
                version, delta = self._apply_replace(cur, output, write, scope, table, slice_where)
                if version is None:
                    return Written(prior or scope.baseline)
            if output.incremental and delta is not None:
                batch = delta.batch
            self._set_marker(cur, output.name, scope.partition, version, batch)
        return Written(
            TableRef(
                output=output.name,
                store="",
                handle={
                    "table": table,
                    "where": slice_where,
                    "key": BATCH_COLUMN if output.key is None and output.incremental else output.key,
                    "revision": output.revision,
                    "batch": batch if output.key is None and output.incremental else None,
                },
                version=version,
                partition=scope.partition,
            ),
            delta,
        )

    def _assigned_batch(self, cur, output, scope) -> int:
        if scope.batch is not None:
            return scope.batch
        prior_marker = self._marker_batch(cur, output.name, scope.partition)
        if prior_marker is not None:
            return int(prior_marker) + 1
        return next_batch(scope.baseline)

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
        self._ensure(cur, output, rows)
        delta = None
        if output.key is not None:
            # Keyed incremental: the delta diffs the new map against the
            # committed logical map in cursus_keys (§2.1).
            live = self._key_revs(cur, output.name, scope.partition)
            new_map = key_map(output, rows)
            upserted = {k: r for k, r in new_map.items() if live.get(k) != r}
            deleted = sorted(set(live) - set(new_map))
            if not upserted and not deleted:
                return None, None  # identical content: same version, empty delta
            delta = Delta(
                batch=self._assigned_batch(cur, output, scope),
                rows=len(upserted) + len(deleted),
                upserted=upserted,
                deleted=tuple(deleted),
                reset=True,
            )
            self._del_key_revs(cur, output.name, scope.partition)
            self._put_key_revs(cur, output.name, scope.partition, new_map)
        self._delete_slice(cur, table, slice_where)
        self._insert(cur, table, rows)
        version = digest(rows)
        return version, delta

    def _apply_patch(self, cur, output, write: Patch, scope, table, slice_where, prior):
        if not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        remove = {str(k) for k in write.remove}
        rows = _coerce_rows(write.rows)
        if not rows and not remove:
            if prior is None and scope.baseline is None:
                raise WriteError(f"{output.name}: empty Patch with no prior head")
            return None
        self._ensure(cur, output, rows)
        if output.key is None:
            # Batch mode: stamp the batch columns, replace this batch's rows.
            if remove:
                raise WriteError(
                    f"{output.name}: remove is not allowed on an unkeyed incremental output"
                )
            batch = self._assigned_batch(cur, output, scope)
            partition_col = output.config.get("partition_column")
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
            version = digest(
                [prior.version if prior else "", digest({"rows": _canon(rows), "remove": []})]
            )
            return version, Delta(batch=batch, rows=len(rows), reset=prior is None), batch

        partition_col = output.config.get("partition_column")
        for row in rows:
            if partition_col:
                if partition_col in row and str(row[partition_col]) != scope.partition:
                    raise WriteError(
                        f"{output.name}: row {partition_col}={row[partition_col]!r} disagrees with scope"
                    )
                row[partition_col] = scope.partition
        patch_map = key_map(output, rows)
        reset = prior is None
        if reset:
            # No prior: the patch is the whole state — diff against the full
            # committed map, not just the touched keys.
            live = self._key_revs(cur, output.name, scope.partition)
            upserted = {k: r for k, r in patch_map.items() if live.get(k) != r}
            deleted = sorted(set(live) - set(patch_map))
        else:
            live = self._key_revs(
                cur, output.name, scope.partition, list(set(patch_map) | remove)
            )
            upserted = {k: r for k, r in patch_map.items() if live.get(k) != r}
            deleted = sorted(k for k in remove if k in live)
        if not upserted and not deleted:
            return None
        self._upsert(cur, table, output, rows)
        if remove:
            cur.execute(
                f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND {_ident(output.key)}::text = ANY(%s)",
                ([slice_where[k] for k in sorted(slice_where)] + [sorted(remove)]),
            )
        if reset:
            self._del_key_revs(cur, output.name, scope.partition)
            self._put_key_revs(cur, output.name, scope.partition, patch_map)
        else:
            self._del_key_revs(cur, output.name, scope.partition, remove)
            self._put_key_revs(cur, output.name, scope.partition, patch_map)
        # Orphan sweep: rows left by an attempt that never committed are absent
        # from the committed key map by construction (§3).
        cur.execute(
            f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} "
            f"AND {_ident(output.key)}::text NOT IN "
            f"(SELECT key FROM {KEYMAP_TABLE} WHERE output = %s AND partition = %s)",
            ([slice_where[k] for k in sorted(slice_where)] + [output.name, scope.partition]),
        )
        version = digest(
            [prior.version if prior else "", digest({"rows": _canon(rows), "remove": sorted(remove)})]
        )
        delta = Delta(
            batch=self._assigned_batch(cur, output, scope),
            rows=len(upserted) + len(deleted),
            upserted=upserted,
            deleted=tuple(deleted),
            reset=reset,
        )
        return version, delta, None

    def _apply_sql(self, cur, output, write: Sql, scope, table, slice_where, prior):
        live = self._key_revs(cur, output.name, scope.partition) if output.key else {}
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
        delta = None
        if output.key:
            rows = cur.execute(
                f"SELECT * FROM {table} WHERE {self._where_sql(slice_where)}",
                [slice_where[k] for k in sorted(slice_where)],
            ).fetchall()
            new_map = key_map(output, rows)
            upserted = {k: r for k, r in new_map.items() if live.get(k) != r}
            deleted = sorted(set(live) - set(new_map))
            if not upserted and not deleted:
                return None, None  # identical content: same version, empty delta
            delta = Delta(
                batch=self._assigned_batch(cur, output, scope),
                rows=len(upserted) + len(deleted),
                upserted=upserted,
                deleted=tuple(deleted),
                reset=True,
            )
            self._del_key_revs(cur, output.name, scope.partition)
            self._put_key_revs(cur, output.name, scope.partition, new_map)
        version = digest([prior.version if prior else "", digest(write.stmt)])
        return version, delta

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
            self._ensure_markers(cur)
            # Batch-mode refs are exempt: the `_batch <=` filter is a true
            # snapshot at the pinned version, always readable after later
            # writes (§3).
            if not ref.meta.get("external") and handle.get("batch") is None:
                live = self._marker(cur, ref.output, ref.partition)
                if live != ref.version:
                    raise StaleRead(
                        f"{ref.output}/{ref.partition}: live marker {live} != pinned {ref.version}"
                    )
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
