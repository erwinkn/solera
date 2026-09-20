"""PostgresStore: shared mutable tables behind version markers (§3, §4).

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the harness).
"""

from __future__ import annotations

import re
from typing import Any

from data_orchestrator.sdk import Output, Ref, TableRef, digest
from data_orchestrator.stores import (
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
    resolve_env,
)

MARKER_TABLE = "public.dorc_markers"
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

    def _ensure(self, cur, output: Output, rows: list[dict] | None = None):
        table, schema, table_name = self._table(output)
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
        columns = dict(output.config.get("columns") or {})
        partition_col = output.config.get("partition_column")
        if rows:
            for row in rows:
                for column, value in row.items():
                    columns.setdefault(column, _column_type(value))
        if partition_col:
            columns.setdefault(partition_col, "text")
        if output.mode == "append":
            columns.setdefault(BATCH_COLUMN, "integer")
            columns.setdefault(SEQ_COLUMN, "integer")
        if not columns:
            columns = {"value": "jsonb"}
        pk = list(output.config.get("primary_key") or ([output.key] if output.key else []))
        if partition_col and partition_col not in pk and (pk or output.mode == "append"):
            pk = [*pk, partition_col]
        if output.mode == "append":
            for c in (BATCH_COLUMN, SEQ_COLUMN):
                if c not in pk:
                    pk = [*pk, c]
        defs = [f"{_ident(c)} {_sql_type(t)}" for c, t in columns.items()]
        if pk:
            defs.append(f"PRIMARY KEY ({', '.join(_ident(c) for c in pk)})")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(defs)})")
        existing = {
            r["column_name"]
            for r in cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table_name),
            )
        }
        for column, decl in columns.items():
            if column not in existing:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {_ident(column)} {_sql_type(decl)}")
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

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        output = scope.output
        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_markers(cur)
            table, _, _ = self._table(output)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: scope.partition} if partition_col else {}

            if prior is not None:  # recompute (prior=None) skips the marker check
                live = self._marker(cur, output.name, scope.partition)
                if live != prior.version:
                    raise StoreConflict(
                        f"{output.name}/{scope.partition}: live marker {live} != pinned {prior.version}"
                    )

            prior_keys = dict(scope.prior_keys or {})
            batch = None
            if isinstance(write, Sql):
                version, keys, batch = self._apply_sql(cur, output, write, scope, table, slice_where, prior)
            elif isinstance(write, Patch):
                version, keys, batch = self._apply_patch(
                    cur, output, write, scope, table, slice_where, prior, prior_keys
                )
                if version is None:
                    return Written(prior, prior_keys)
            else:
                if output.mode == "append":
                    raise WriteError(f"{output.name}: an append output only accepts Patch writes")
                version, keys = self._apply_replace(cur, output, write, scope, table, slice_where)

            self._set_marker(cur, output.name, scope.partition, version, batch)
        return Written(
            TableRef(
                output=output.name,
                store="",
                handle={
                    "table": table,
                    "where": slice_where,
                    "key": BATCH_COLUMN if output.mode == "append" else output.key,
                    "revision": output.revision,
                    "batch": batch,
                },
                version=version,
                partition=scope.partition,
            ),
            keys,
        )

    def _apply_replace(self, cur, output, write, scope, table, slice_where) -> tuple[str, dict | None]:
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
        self._delete_slice(cur, table, slice_where)
        self._insert(cur, table, rows)
        version = digest(rows)
        keys = key_map(output, rows) if output.key or output.is_partition_set else None
        return version, keys

    def _apply_patch(self, cur, output, write: Patch, scope, table, slice_where, prior, prior_keys):
        if output.key is None and not output.is_partition_set and output.mode != "append":
            raise WriteError(f"{output.name}: Patch requires a keyed or append output")
        remove = {str(k) for k in write.remove}
        rows = _coerce_rows(write.rows)
        if not rows and not remove:
            if prior is None:
                raise WriteError(f"{output.name}: empty Patch with no prior head")
            return None, None, None
        self._ensure(cur, output, rows)
        batch = None
        if output.mode == "append":
            if remove:
                raise WriteError(f"{output.name}: remove is not allowed on an append output")
            batch = max([int(b) for b in prior_keys], default=-1) + 1
            partition_col = output.config.get("partition_column")
            for i, row in enumerate(rows):
                row[BATCH_COLUMN] = batch
                row[SEQ_COLUMN] = i
                if partition_col:
                    row[partition_col] = scope.partition
            # Idempotent on (scope, batch): replace this batch's rows.
            where = {**slice_where, BATCH_COLUMN: batch}
            self._delete_slice(cur, table, where)
            self._insert(cur, table, rows)
            keys = {**prior_keys, str(batch): digest(rows)}
            version = digest([prior.version if prior else "", digest({"rows": _canon(rows), "remove": []})])
            return version, keys, batch

        partition_col = output.config.get("partition_column")
        for row in rows:
            if partition_col:
                if partition_col in row and str(row[partition_col]) != scope.partition:
                    raise WriteError(
                        f"{output.name}: row {partition_col}={row[partition_col]!r} disagrees with scope"
                    )
                row[partition_col] = scope.partition
        self._upsert(cur, table, output, rows)
        if remove:
            key_col = output.key or "key"
            cur.execute(
                f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND {_ident(key_col)}::text = ANY(%s)",
                ([slice_where[k] for k in sorted(slice_where)] + [sorted(remove)]),
            )
        patch_map = key_map(output, rows)
        keys = {k: v for k, v in prior_keys.items() if k not in remove}
        keys.update(patch_map)
        # Orphan sweep: rows left by an attempt that never committed are absent
        # from the resulting map by construction (§3).
        if keys:
            cur.execute(
                f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND NOT ({_ident(output.key)}::text = ANY(%s))",
                ([slice_where[k] for k in sorted(slice_where)] + [sorted(keys)]),
            )
        else:
            self._delete_slice(cur, table, slice_where)
        version = digest(
            [prior.version if prior else "", digest({"rows": _canon(rows), "remove": sorted(remove)})]
        )
        return version, keys, None

    def _apply_sql(self, cur, output, write: Sql, scope, table, slice_where, prior):
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
        keys = None
        if output.key:
            rows = cur.execute(
                f"SELECT * FROM {table} WHERE {self._where_sql(slice_where)}",
                [slice_where[k] for k in sorted(slice_where)],
            ).fetchall()
            keys = key_map(output, rows)
        version = digest([prior.version if prior else "", digest(write.stmt)])
        return version, keys, None

    # -- reads ----------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | None) -> Any:
        if isinstance(t, type) and issubclass(t, Ref):
            return ref
        handle = ref.handle or {}
        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_markers(cur)
            # Append refs are exempt: the `_batch <=` filter is a true snapshot
            # at the pinned version, always readable after later writes (§3).
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
            if selection is not None:
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
