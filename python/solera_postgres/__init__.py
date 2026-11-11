"""PostgresStore: shared mutable tables (§3, §4). Reads are not pinned: a
ref names a table partition, and a load reads what it holds now.

The version check (D196, docs/tracking-design.md §8): one row per table and
partition (`VERSIONS_TABLE`) holds the newest attempt version that has
written it. Every write transaction raises it to its own version first, or is
refused whole, so a writer the engine gave up on can change nothing once a
newer attempt has written. Today's `fenced` path also takes it before an
attempt reads (`acquire`).

A full run builds beside the published rows (D211): its writes go to a build
table in `solera_build`, which the same row registers, and
`full_run_ended` publishes it in one transaction.

`psycopg` is imported lazily so project files can declare the store without a
driver installed; only `store`/`load` need it (in the worker).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from solera.sdk import KEYS, Output, Ref, TableRef
from solera.stores import (
    MISSING,
    Changed,
    Commits,
    KeyedWrite,
    Keys,
    Opaque,
    Patch,
    Prepared,
    PublicationRefused,
    Read,
    RunLost,
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
# Per table (by name) and partition: the version check (`generation`, `worker_id`), the
# version whose write last changed it (`written`), the newest full run published there
# (`published`) and the full runs registered there: with a build (`builds`), or with none made
# yet, for their writes so far had no rows and no shape to make one from (`pending`).
VERSIONS_TABLE = "public.solera_versions"
VERSIONS_DDL = (
    f"CREATE TABLE IF NOT EXISTS {VERSIONS_TABLE} (rel text NOT NULL, part text NOT NULL, "
    "generation bigint NOT NULL, worker_id text NOT NULL, written bigint, published bigint, "
    "builds bigint[] NOT NULL DEFAULT '{}', pending bigint[] NOT NULL DEFAULT '{}', PRIMARY KEY (rel, part))"
)
BUILD_SCHEMA = "solera_build"  # a full run's build: "solera_build"."b_<table and partition>_<run>"
COMMIT_COLUMN = "_commit"
SEQ_COLUMN = "_seq"
VERSION_COLUMN = "_version"  # every row's version: the generation of the attempt that wrote it
KEY_CHUNK = 100_000  # keys per chunk `keys` reads


class _Spool:
    """Keys spooled to a temporary file, NUL after each (text holds no NUL):
    replayed from the start a chunk at a time, by every iteration."""

    def __init__(self):
        import tempfile

        self.file = tempfile.TemporaryFile()

    def add(self, keys: Iterable[str]) -> None:
        self.file.write(b"".join(k.encode() + b"\0" for k in keys))

    def __iter__(self):
        import os

        self.file.flush()
        offset, rest, chunk = 0, b"", []
        while data := os.pread(self.file.fileno(), 1 << 20, offset):
            offset += len(data)
            *done, rest = (rest + data).split(b"\0")
            for key in done:
                chunk.append(key.decode())
                if len(chunk) == KEY_CHUNK:
                    yield chunk
                    chunk = []
        if chunk:
            yield chunk


class BuildInUse(StoreError):
    """A migration that met a load of one of the table's builds, which it
    drops first (`_drop_builds`): it changed nothing; run it again."""

    retryable = True


class BuildGone(StoreError):
    """A load in one snapshot (`reads()`) of a ref to a full run's build that
    a switch published and dropped after the snapshot began: in that
    snapshot the user's table still holds the old rows. Read again."""

    retryable = True


@dataclass
class _Target:
    """Where a write goes (`PostgresStore._aim`): the user's `table`, or the
    build of full run `run` of `partition`, which the write `opens`; the
    partition's registered runs after it, with a build (`builds`) or with
    none made yet (`pending`), and whether the catalog holds a build of the
    partition it does not keep (`retired`)."""

    table: str
    partition: str
    run: int | None = None
    opens: bool = False
    builds: list = field(default_factory=list)
    pending: list = field(default_factory=list)
    retired: bool = False

    def made_nothing(self) -> None:
        """The write that opens the run's build had no rows, and no shape to
        make one from: the run stays registered, pending, and its first
        write with rows opens it."""

        if self.opens:
            self.builds.remove(self.run)
            self.pending.append(self.run)
            self.run, self.opens = None, False


def _build_name(table: str, partition: str, run: int) -> str:
    """A build's table in `BUILD_SCHEMA`: one per table, partition and run (the
    version of the attempt that wrote the run's clear). It starts with a
    letter, as `_ident` asks (probe P10)."""

    import hashlib

    digest = hashlib.md5(f"{table}\0{partition}".encode()).hexdigest()[:16]
    return f"b_{digest}_{run}"


# A trigger or rule, in any state, on the table or any table that inherits from it (a partition
# among them): a switch's DELETE and INSERT could run one on the rows Solera read in the build,
# which writing them there did not. Which one would run depends on its events, its level, the
# session's replication role and the statement's target, so the store does not guess: any refuses.
_TRIGGERED = """EXISTS (WITH RECURSIVE tree(oid) AS (SELECT {rel}::oid
    UNION SELECT i.inhrelid FROM pg_inherits i JOIN tree ON i.inhparent = tree.oid)
  SELECT 1 FROM tree WHERE EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgrelid = tree.oid AND NOT t.tgisinternal)
    OR EXISTS (SELECT 1 FROM pg_rewrite w WHERE w.ev_class = tree.oid AND w.rulename <> '_RETURN'))"""

_STATE = (
    """
SELECT r.oid IS NOT NULL AS present,
  coalesce((SELECT jsonb_object_agg(a.attname, format_type(CASE WHEN t.typtype = 'd' THEN t.typbasetype
      ELSE a.atttypid END, NULL)) FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped), '{}') AS columns,
  coalesce((SELECT array_agg(a.attname::text ORDER BY k.i) FROM pg_constraint c
    CROSS JOIN unnest(c.conkey) WITH ORDINALITY AS k(n, i)
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n
    WHERE c.conrelid = r.oid AND c.contype = 'p'), '{}') AS pk,
  coalesce((SELECT array_agg(i.relname::text) FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
    WHERE x.indrelid = r.oid), '{}') AS indexes,
  coalesce((SELECT array_agg(g.rolname::text) FROM pg_roles g WHERE g.rolname = ANY(%(grants)s)
    AND NOT (r.oid IS NOT NULL AND has_table_privilege(g.oid, r.oid, 'SELECT'))), '{}') AS ungranted,
  EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %(build)s) AS build_schema,
  coalesce((SELECT array_agg(c.relname::text) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %(build)s AND c.relname >= %(first)s::name AND c.relname < %(past)s::name
    AND c.relkind = 'r'), '{}') AS built,
  r.oid IS NOT NULL AND ("""
    + _TRIGGERED.format(rel="r.oid")
    + """) AS triggered
FROM (SELECT (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = %(schema)s AND c.relname = %(name)s AND c.relkind IN ('r', 'p')) AS oid) AS r
"""
)

_TRIGGERS = "SELECT " + _TRIGGERED.format(rel="%(t)s::regclass") + " AS triggered"

# A new build's indexes it does not keep (neither unique nor exclusion), and the user's
# table's identity columns with their sequences.
_BUILD_EXTRAS = """
SELECT i.relname::text AS index, NULL::text AS identity, NULL::text AS sequence
  FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
  WHERE x.indrelid = %(build)s::regclass AND NOT x.indisunique AND NOT x.indisexclusion
UNION ALL
SELECT NULL, a.attname::text, pg_get_serial_sequence(%(table)s, a.attname)
  FROM pg_attribute a WHERE a.attrelid = %(table)s::regclass AND a.attidentity <> '' AND NOT a.attisdropped
"""


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


def _dsn_password(dsn: str) -> bool:
    """Whether a DSN — a URL or `key=value` pairs — names a password."""

    if "://" in dsn:
        from urllib.parse import urlsplit

        parts = urlsplit(dsn)
        return bool(parts.password) or "password=" in (parts.query or "")
    return any(pair.split("=", 1)[0].strip() == "password" for pair in dsn.split())


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
        self._versions_ready = False

    def describe(self) -> dict | None:
        """`{class, config}` (FileStore.describe), refusing a DSN that holds a
        password: it would sit in the manifest, journal and API. A DSN
        without one (local development) passes; `env:NAME` is resolved in the
        worker."""

        if type(self) is not PostgresStore:
            return None
        if not self.dsn.startswith("env:") and _dsn_password(self.dsn):
            raise ValueError("the dsn holds a password: pass dsn='env:NAME' and set NAME to the DSN")
        config = {"dsn": self.dsn, "grants": list(self.grants), "sql_read_only": self.sql_read_only}
        return {"class": "PostgresStore", "config": config}

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

    def _table(self, output: Output, prior: Ref | None = None, home: str | None = None) -> str:
        """Where the output's content lives: the committed head's table, so a
        renamed output keeps its table, and every ref to it stays readable
        (§2); the declaration's (`schema`, `table`, else the name its life
        began under, `home`: one table for every partition, K25) only for a
        first write."""

        committed = (prior.handle or {}).get("table") if prior is not None else None
        if committed:
            return (committed, *_split(committed))
        schema = output.config.get("schema", "public")
        table = output.config.get("table", home or output.name)
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
        at: _Target,
        state: dict,
        rows: Callable[[], Iterable[dict]] | None = None,
        inferred: dict | None = None,
        kinds: Mapping[str, str | None] | None = None,
    ) -> str:
        """The user's table, made if missing, and the table the write goes to
        (`_open`). It runs after the version check (`_begin`), from the
        catalog state `_begin` read under it, and changes the catalog only
        where that state says something is missing: `IF NOT EXISTS` DDL takes
        its lock even when the object exists, and would queue behind a
        switch of another partition (probes P1, P1c). A table the write
        creates takes the declared columns, then those of a `Sql` SELECT
        (`inferred`), then the write's: each column's kind as its reader
        knows it (`kinds`: a DataFrame's dtypes, an Arrow schema), else the
        kind of every value not null in it (`rows()`, read only then).
        Inferred columns are logged, once. What a column does to a value is
        the database's: the declared columns are the user's contract
        (docs/versions.md §4)."""

        table = at.table
        schema, table_name = _split(table)
        declared, pk = self._declared_shape(output)
        declared = {**(inferred or {}), **declared}
        if not state["present"]:
            # Partitions write one table at once: its first writers take turns.
            state = self._locked_state(cur, table)
        if state["present"]:
            self._check_drift(output, table, state, declared, pk)
        else:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")
            columns = dict(declared)
            if rows is not None:
                known = {
                    c: _COLUMNS[k] for c, k in (kinds or {}).items() if k is not None and c not in declared
                }
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
            columns.pop(VERSION_COLUMN, None)
            defs = [f"{_ident(c)} {_sql_type(t)}" for c, t in (columns or {"value": "jsonb"}).items()]
            defs.append(f"{_ident(VERSION_COLUMN)} bigint")
            if pk:
                defs.append(f"PRIMARY KEY ({', '.join(_ident(c) for c in pk)})")
            cur.execute(f"CREATE TABLE {table} ({', '.join(defs)})")
            state = self._state(cur, table)
        wanted = self._wanted(output, table_name, state)
        if any(name not in state["indexes"] for name in wanted) or state["ungranted"]:
            state = self._locked_state(cur, table)
            for name, on in wanted.items():
                if name not in state["indexes"]:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS {_ident(name)} ON {table} ({on})")
            # Grants are deployment sugar: a role this database lacks is never asked for.
            for role in state["ungranted"]:
                cur.execute(f"GRANT SELECT ON {table} TO {_ident(role)}")
        return self._open(cur, output, at, state)

    def _wanted(self, output: Output, table_name: str, state: dict) -> dict[str, str]:
        """The indexes the table must have, by name: the declared ones, one on
        the key column, and, for a key that is not text, one on the key as
        text, which reads and patches name (`key::text = ANY(...)`)."""

        wanted = {
            (table_name + "_" + "_".join(index))[:63]: ", ".join(_ident(c) for c in index)
            for index in self._indexes(output)
        }
        kind = state["columns"].get(output.key) if output.key is not None else None
        if kind is not None and kind not in ("text", "character varying", "character"):
            wanted[f"{table_name}_{output.key}_text"[:63]] = f"({_ident(output.key)}::text)"
        return wanted

    def _open(self, cur, output: Output, at: _Target, state: dict) -> str:
        """The table the write goes to: the build `at` names, made now if the
        write opens it, else the user's table. A build is the user's table's
        shape (`LIKE`) with its CHECK constraints, generated columns, unique
        and exclusion indexes, so a row that breaks one fails the batch that
        writes it, as in place; of its other indexes only Solera's on the key.

        An identity column draws from the user's table's sequence, never one
        of the build's own (probes P5, R4, R9)."""

        from psycopg import sql

        if at.run is None:
            return at.table
        build = _qname(BUILD_SCHEMA, _build_name(at.table, at.partition, at.run))
        if not at.opens:
            return build
        if not state["build_schema"]:  # two first builds making it at once collide (probe P8)
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (BUILD_SCHEMA,))
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(BUILD_SCHEMA)}")
        # A table of the same name the registry no longer lists: a sweep that has not dropped it yet.
        cur.execute(f"DROP TABLE IF EXISTS {build}")
        cur.execute(
            f"CREATE TABLE {build} (LIKE {at.table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS "
            "INCLUDING GENERATED INCLUDING INDEXES)"
        )
        for found in cur.execute(_BUILD_EXTRAS, {"build": build, "table": at.table}).fetchall():
            if found["index"] is not None:
                cur.execute(f"DROP INDEX {sql.Identifier(BUILD_SCHEMA, found['index']).as_string(cur)}")
            else:
                sequence = sql.Literal(found["sequence"]).as_string(cur)
                column = sql.Identifier(found["identity"]).as_string(cur)
                cur.execute(
                    f"ALTER TABLE {build} ALTER COLUMN {column} SET DEFAULT nextval({sequence}::regclass)"
                )
        if output.key is not None:
            key = _ident(output.key)
            if state["columns"].get(output.key) not in ("text", "character varying", "character"):
                cur.execute(f"CREATE INDEX ON {build} (({key}::text))")
            elif list(state["pk"][:1]) != [output.key]:
                cur.execute(f"CREATE INDEX ON {build} ({key})")
        return build

    def _check_drift(self, output: Output, table: str, state: dict, declared: dict, pk: list) -> None:
        """A pre-existing table must already match the declaration; evolving
        shape is a migration's job, never a silent ALTER (§4). Every row it
        holds carries its version."""

        live = state["columns"]
        drift = []
        if declared or pk:
            if missing := [c for c in declared if c not in live]:
                drift.append(f"missing columns {missing}")
            if set(state["pk"]) != set(pk):
                drift.append(f"primary key {list(state['pk']) or 'none'} != declared {pk or 'none'}")
        if drift:
            raise StoreError(
                f"{output.name}: live table {table} does not match the declaration "
                f"({'; '.join(drift)}); reconcile it with a Migration"
            )
        if live.get(VERSION_COLUMN) != "bigint":
            raise StoreError(
                f"{output.name}: table {table} has no {VERSION_COLUMN} bigint column, which every row of a "
                "table this store writes carries: the table was made before rows carried their "
                "version. Drop it, or name a new table for the output (Output(..., table=...))"
            )

    # -- the version check (D196) and the catalog ---------------------------------

    def _ready(self) -> None:
        """The versions table, made once per store: two first writers making
        it at once would collide, so they take turns."""

        if self._versions_ready:
            return
        with self._connect() as conn, conn.cursor() as cur:
            if cur.execute("SELECT to_regclass(%s) IS NULL AS missing", (VERSIONS_TABLE,)).fetchone()[
                "missing"
            ]:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (VERSIONS_TABLE,))
                cur.execute(VERSIONS_DDL)
        self._versions_ready = True

    def _check_sql(self, table: str, context: WriteContext, write: bool) -> tuple[str, tuple]:
        """The version check: raise the partition's row to the attempt's
        version, inserting it, or change nothing when a newer version (or
        another worker of this one) holds it. Postgres locks the conflicting
        row even when the `WHERE` refuses the update, so a newer check waits
        for an older writer's open transaction, after which that writer can
        change nothing. A `write` also marks the partition written by its
        version. It returns the partition's published run and registered runs."""

        written = ", written = EXCLUDED.written" if write else ""
        return (
            f"INSERT INTO {VERSIONS_TABLE} AS v (rel, part, generation, worker_id, written) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (rel, part) DO UPDATE SET "
            f"generation = EXCLUDED.generation, worker_id = EXCLUDED.worker_id{written} "
            "WHERE v.generation < EXCLUDED.generation "
            "OR (v.generation = EXCLUDED.generation AND v.worker_id = EXCLUDED.worker_id) "
            "RETURNING v.published, v.builds, v.pending",
            (
                table,
                context.partition,
                context.generation,
                context.worker_id or "",
                context.generation if write else None,
            ),
        )

    def _checked(self, taken: dict | None, context: WriteContext) -> dict:
        if taken is None:
            raise StoreError(
                f"{context.output.name}: a newer attempt has written this slice (generation "
                f"{context.generation} of {context.worker_id} refused)"
            )
        return taken

    def _check(self, cur, table: str, context: WriteContext, write: bool = True) -> dict:
        """The version check in `cur`'s transaction, held until it ends."""

        self._ready()
        return self._checked(cur.execute(*self._check_sql(table, context, write)).fetchone(), context)

    def _begin(
        self, conn, table: str, context: WriteContext | None, write: bool = True
    ) -> tuple[dict | None, dict]:
        """A write transaction's first round trip, before it names a build or
        changes the catalog: the table's write domain, the version check (its
        row: `published`, `builds`; None outside an attempt) and what the
        catalog holds of the table (`_STATE`), read after the check's lock is
        held, never before: a lookup made before a wait does not see what
        the transaction it waited for did (probes P3, P4, R10, R11). The
        three statements go in one pipeline."""

        fenced = context is not None and context.generation is not None
        if fenced:
            self._ready()
        domain, check, state = conn.cursor(), conn.cursor(), conn.cursor()
        with conn.pipeline():
            self._domain(domain, table)
            if fenced:
                check.execute(*self._check_sql(table, context, write))
            state.execute(_STATE, self._state_params(table, context.partition if context else ""))
            taken, found = (check.fetchone() if fenced else None), state.fetchone()
        return (self._checked(taken, context) if fenced else None), found

    def _state_params(self, table: str, partition: str = "") -> dict:
        schema, name = _split(table)
        prefix = _build_name(table, partition, 0)[:-1]  # the partition's builds: names in [first, past)
        return {
            "schema": schema,
            "name": name,
            "grants": list(self.grants),
            "build": BUILD_SCHEMA,
            "first": prefix,
            "past": prefix[:-1] + chr(ord(prefix[-1]) + 1),
        }

    def _state(self, cur, table: str) -> dict:
        return cur.execute(_STATE, self._state_params(table)).fetchone()

    def _locked_state(self, cur, table: str) -> dict:
        """The catalog state under the table's DDL lock: a writer that finds
        something missing takes turns with the others, then reads again what
        the one before it made."""

        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
        return self._state(cur, table)

    def _domain(self, cur, table: str, exclusive: bool = False) -> None:
        """The table's write domain, until the transaction ends: shared by
        every writer's transaction — acquisitions, writes, switches — and
        exclusive for a migration and a cleanup, which so run between
        writers of every partition, never under one. Always the first lock a
        transaction takes, so the order is the same everywhere: the domain,
        then the partition's version check, then the catalog."""

        lock = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
        cur.execute(f"SELECT {lock}(hashtext(%s))", (f"solera-domain:{table}",))

    def _aim(self, table: str, context: WriteContext, checked: dict | None, state: dict) -> _Target:
        """Where a write goes (D211; docs/tracking-design.md §8, obligations 1
        and 2). Today's path (`context.builds` off) writes in place. A full
        run's first write (`reset`) opens the build of its run (its own
        version), unless that run was published already: the same write
        delivered again then rewrites the same rows in place. Any other write
        goes to the build of the run `build_run` names while the partition
        registers that run, else in place. Every write keeps the run
        `build_run` names and its own, and unregisters the partition's
        others: no run in the log names them, and the check has locked their
        writers out.

        A run's state is recorded in its version row, never inferred from
        the catalog: registered with a build (`builds`), or with none made
        yet (`pending`: its writes so far had no rows, and no shape to make
        one from), whose first write with rows opens it. The catalog
        (`state`, from the write's first round trip) says three things. A
        registered build it no longer holds is lost (a migration dropped it,
        or a restore lost it): a write of its run fails (`RunLost`) and
        changes nothing, for the rows it held are gone. A table with a
        trigger or rule, in any state, on it or on a table that inherits from
        it, never opens a build (`PublicationRefused`): the switch could run
        it on rows Solera has read, which writing them did not, and the store
        never alters the user's table to stop it. A
        build of the partition the write does not keep makes it sweep after
        it commits."""

        at = _Target(
            table,
            context.partition,
            builds=list(checked["builds"]) if checked else [],
            pending=list(checked["pending"]) if checked else [],
        )
        if not context.builds:
            return at
        if checked is None:
            raise StoreError(f"{context.output.name}: a write that builds needs its attempt's version")
        published, built = checked["published"], set(state["built"])
        lost = {b for b in at.builds if _build_name(table, context.partition, b) not in built}
        run = None
        if context.reset and (published is None or context.generation > published):
            run = context.generation
        elif not context.reset and context.build_run in (*at.builds, *at.pending):
            run = context.build_run
        if run in lost:
            raise RunLost(
                f"{context.output.name} {context.partition!r}: the build of full run {run} is gone "
                "(a migration dropped it, or a restore lost it), and the rows it held with it: the "
                "partition's full run must start over"
            )
        if run is not None:
            at.run, at.opens = run, run not in at.builds
        if at.opens and state["triggered"]:
            raise PublicationRefused(
                f"{context.output.name} {context.partition!r}: {table} has a trigger or rule, on it or "
                "on a table that inherits from it: publishing a full run built beside it could run it "
                "on rows Solera has read. Drop it, or write the output in place"
            )
        keep = {context.build_run, at.run}
        at.builds = [b for b in at.builds if b in keep] + ([at.run] if at.opens else [])
        at.pending = [b for b in at.pending if b in keep and not (at.opens and b == at.run)]
        kept = {_build_name(table, context.partition, b) for b in at.builds}
        at.retired = any(name not in kept for name in built)
        return at

    def _register(self, cur, at: _Target, checked: dict | None) -> None:
        if checked is not None and (at.builds, at.pending) != (
            list(checked["builds"]),
            list(checked["pending"]),
        ):
            cur.execute(
                f"UPDATE {VERSIONS_TABLE} SET builds = %s, pending = %s WHERE rel = %s AND part = %s",
                (at.builds, at.pending, at.table, at.partition),
            )

    def _sweep(self, table: str, partition: str) -> None:
        """Drop the partition's builds no version row registers: unregistered
        by a write, a switch or a cleanup whose transaction has committed.
        Each DROP is a short transaction of its own, after that one: it waits
        behind any load still reading the build, and must never do so while
        holding the partition's version check (probe R3). Each reads the
        registry again after its DROP, under the DROP's lock, and keeps a
        build registered meanwhile (a reset delivered again opens its build
        under the same name, and waits for that lock). Best effort: the
        caller's work has committed, so a DROP that cannot get its lock soon,
        an error, or a crash leaves the table to the partition's next write
        or call, which finds it in the catalog."""

        import psycopg

        try:
            with psycopg.connect(resolve_env(self.dsn), autocommit=True) as conn:
                registered = f"SELECT %s = ANY(builds) FROM {VERSIONS_TABLE} WHERE rel = %s AND part = %s"
                conn.execute("SET lock_timeout = '100ms'")  # a write waits on it: retried by the next
                for name in self._builds_of(conn.cursor(), table, [partition]):
                    run = int(name.rsplit("_", 1)[1])
                    if (conn.execute(registered, (run, table, partition)).fetchone() or [False])[0]:
                        continue
                    try:
                        with conn.transaction():
                            conn.execute(f"DROP TABLE IF EXISTS {_qname(BUILD_SCHEMA, name)}")
                            if (conn.execute(registered, (run, table, partition)).fetchone() or [False])[0]:
                                raise psycopg.Rollback()
                    except psycopg.errors.LockNotAvailable:
                        log.info("build %s is still being read: a later sweep drops it", name)
        except psycopg.Error as e:
            log.warning("sweeping the builds of %s %r: %s; a later sweep drops them", table, partition, e)

    def _builds_of(self, cur, table: str, partitions: list[str]) -> list[str]:
        """The names of the partitions' build tables, registered or not, in one
        lookup."""

        prefixes = [_build_name(table, p, 0)[:-1] for p in partitions]
        if not prefixes:
            return []
        found = cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relkind = 'r' AND left(c.relname, %s) = ANY(%s)",
            (BUILD_SCHEMA, len(prefixes[0]), prefixes),
        ).fetchall()
        return [r["relname"] if isinstance(r, dict) else r[0] for r in found]

    async def cleanup(
        self, output, *, home=None, partition=None, key=None, generation=None, before=None
    ) -> None:
        """Delete what the pattern matches (docs/stores.md § Cleanup). A
        partition is matched by its version row's last write, not by its rows'
        versions: its rows go only if that write matches — exactly
        `generation`, or older than `before` — and the whole output's table
        is dropped only when no partition of it is newer: a later life of the
        same name in the same table keeps what it wrote. `key` narrows it to
        that key's rows. A partition that goes takes its builds. Idempotent."""

        await asyncio.to_thread(self._cleanup, output, home, partition, key, generation, before)

    def _cleanup(self, output, home, partition, key, generation, before) -> None:
        table, _, _ = self._table(output, None, home)
        column = output.config.get("partition_column")
        self._ready()
        alone = partition is not None and bool(column)  # a partition's rows are its own
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table, exclusive=not alone)
            placed = None
            if alone:  # a row to lock before the partition's first check: its first writer waits for it
                placed = cur.execute(
                    f"INSERT INTO {VERSIONS_TABLE} (rel, part, generation, worker_id) VALUES (%s, %s, 0, '') "
                    "ON CONFLICT DO NOTHING RETURNING 1",
                    (table, partition),
                ).fetchone()
            only = " AND part = %s FOR UPDATE" if alone else ""
            written = {
                r["part"]: r["written"]
                for r in cur.execute(
                    f"SELECT part, written FROM {VERSIONS_TABLE} WHERE rel = %s{only}",
                    (table, partition) if alone else (table,),
                )
            }
            present = cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (table,)).fetchone()["ok"]

            def due(part) -> bool:
                g = written.get(part)
                return (generation is None or g == generation) and (before is None or g is None or g < before)

            parts = list(written) if partition is None else [partition]
            taken = [p for p in parts if due(p)]
            if not column and len(taken) != len(written):
                return  # unpartitioned, partitions share the rows: a newer one keeps them
            if key is None:  # before the table: a build's identity default names its sequence
                cur.execute("SET LOCAL lock_timeout = '2s'")  # a load of a build: retry the cleanup later
                for name in self._builds_of(cur, table, taken):
                    cur.execute(f"DROP TABLE IF EXISTS {_qname(BUILD_SCHEMA, name)}")
                cur.execute("SET LOCAL lock_timeout = DEFAULT")
            if partition is None and key is None and len(taken) == len(written):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
                self._forget(cur, table, list(written))
            else:
                for part in taken:
                    where = {column: part} if column else {}
                    sql, params = self._where_sql(where), [where[k] for k in sorted(where)]
                    if key is None:
                        if present:
                            cur.execute(f"DELETE FROM {table} WHERE {sql}", params)
                        self._forget(cur, table, [part])
                        continue
                    # A key's rows wherever the partition holds them: published, and in its builds.
                    keyed = _ident(COMMIT_COLUMN if output.key is None else output.key)
                    sql, params = f"{sql} AND {keyed}::text = %s", [*params, str(key)]
                    builds = [_qname(BUILD_SCHEMA, n) for n in self._builds_of(cur, table, [part])]
                    for target in ([table] if present else []) + builds:
                        cur.execute(f"DELETE FROM {target} WHERE {sql}", params)
            if placed:
                cur.execute(f"DELETE FROM {VERSIONS_TABLE} WHERE rel = %s AND part = %s", (table, partition))

    def _forget(self, cur, table: str, parts: list[str]) -> None:
        """The version rows of partitions a cleanup deleted. A row whose check
        an attempt took after the partition's last write stays, emptied: it
        fences out every older attempt, which deleting it would let write
        again (a late writer of the version cleaned up, under a repair that
        already listed what that version left)."""

        cur.execute(
            f"DELETE FROM {VERSIONS_TABLE} WHERE rel = %s AND part = ANY(%s) AND generation = written",
            (table, parts),
        )
        cur.execute(
            f"UPDATE {VERSIONS_TABLE} SET written = NULL, published = NULL, builds = '{{}}', pending = '{{}}' "
            "WHERE rel = %s AND part = ANY(%s)",
            (table, parts),
        )

    async def acquire(self, context: WriteContext, prior: Ref | None = None) -> None:
        """Today's path: take the attempt's version check for the partition it
        writes — in the table `prior`, the committed head, names — in a
        transaction of its own, before any read of the store: from here on
        no older attempt can change it."""

        if context.generation is None:
            return
        await asyncio.to_thread(self._acquire, context, prior)

    def _acquire(self, context: WriteContext, prior: Ref | None) -> None:
        table, _, _ = self._table(context.output, prior, context.home)
        with self._connect() as conn, conn.cursor() as cur:
            self._domain(cur, table)
            self._check(cur, table, context, write=False)

    def _indexes(self, output: Output) -> list[list[str]]:
        """The table's indexes: the declared ones, and one on the key column."""

        _, pk = self._declared_shape(output)
        keyed = [[output.key]] if output.key and output.key not in pk[:1] else []
        return keyed + [list(i) for i in output.config.get("indexes") or []]

    # -- full runs (D211) and dead writers (D196) --------------------------------

    async def written_at(self, versions, prior: Ref | None, context: WriteContext):
        """The keys the partition holds at any of `versions` — attempts that
        wrote and ended without a commit — where the log points (the build
        a write of `context` would go to, else the user's table),
        sorted by their bytes, a chunk at a time (D196, rule 2). It first
        takes the version check for `context`, the attempt that repairs,
        in a transaction of its own that it reads in: no dead or late writer
        can add a row after (rule 3). The engine deletes the keys the log
        lacks and redoes the others. An unkeyed output yields none: its next
        write replaces what a dead one left, a value whole and a commit by
        its number."""

        return await asyncio.to_thread(self._written_at, list(versions), prior, context)

    def _written_at(self, versions: list[int], prior: Ref | None, context: WriteContext) -> _Spool:
        output, spool = context.output, _Spool()
        table, _, _ = self._table(output, prior, context.home)
        with self._connect() as conn, conn.cursor() as cur:
            checked, state = self._begin(conn, table, context, write=False)
            if output.key is None or not state["present"]:
                return spool
            at = self._aim(table, context, checked, state)
            source = (
                table
                if at.run is None or at.opens
                else _qname(BUILD_SCHEMA, _build_name(table, context.partition, at.run))
            )
            column = output.config.get("partition_column")
            return self._held(
                cur, source, output.key, {column: context.partition} if column else {}, versions
            )

    async def full_run_ended(
        self,
        output: Output,
        partition: str,
        run: int,
        newest: int,
        *,
        prior: Ref | None = None,
        home: str | None = None,
    ) -> bool:
        """The full run of `partition` whose clear the attempt at version
        `run` wrote has ended in the log: publish its build (D211,
        docs/tracking-design.md §8, obligations 4 and 5). One transaction
        holding the partition's version check: if an attempt newer than
        `newest` has written (a call delivered late), it does nothing and
        says not published; a run published before says published; a run
        that is not registered (overtaken), or whose build is gone (a
        migration dropped it, or a restore lost it; it stays registered, as
        lost, until a new full run starts), says not published. A pending
        run (`_aim`: it made no build, for every write of it was empty)
        publishes no rows. Otherwise it deletes the partition's rows and
        inserts the build's,
        every column the database does not generate, so outside readers see
        all old rows, then all new, and wait for nothing; it never changes the
        check's version. A constraint only the published rows can check
        refuses the publication whole (`PublicationRefused`), and so does a
        trigger or rule added since the run began (`_aim`), read under the lock its DELETE takes, so none can be
        added in between, and again after the rows. The
        build's
        table is dropped after, by a sweep. Idempotent; it may take minutes,
        and sets no statement timeout. `prior` (the committed head) and
        `home` say where the table is, as for a write."""

        return await asyncio.to_thread(self._full_run_ended, output, partition, run, newest, prior, home)

    def _full_run_ended(self, output, partition, run, newest, prior, home) -> bool:
        import psycopg
        from psycopg import sql

        table, _, _ = self._table(output, prior, home)
        self._ready()
        try:
            with self._connect() as conn, conn.cursor() as cur:
                self._domain(cur, table)
                found = cur.execute(
                    f"SELECT generation, published, builds, pending FROM {VERSIONS_TABLE} "
                    "WHERE rel = %s AND part = %s FOR UPDATE",
                    (table, partition),
                ).fetchone()
                build = _qname(BUILD_SCHEMA, _build_name(table, partition, run))
                if found is None or found["generation"] > newest:
                    published = False
                elif found["published"] is not None and run <= found["published"]:
                    published = True
                elif run not in found["builds"] and run not in found["pending"]:
                    published = False  # overtaken: a newer full run's first write unregistered it
                elif (
                    run in found["builds"]
                    and not cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (build,)).fetchone()["ok"]
                ):
                    # Lost (a migration dropped it, or a restore): it stays registered, so a write
                    # naming the run fails (`RunLost`) until a new full run's first write drops it.
                    published = False
                else:
                    # A pending run made no build: every write of it was empty, and so is what it publishes.
                    column = output.config.get("partition_column")
                    present = cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (table,)).fetchone()[
                        "ok"
                    ]
                    cur.execute("SET LOCAL statement_timeout = 0")
                    if present:
                        # The lock the DELETE takes, on every partition, first: no trigger or rule can be
                        # added or enabled until this commits, so the catalog read next is the one the
                        # rows meet, before any of them runs.
                        cur.execute(f"LOCK TABLE {table} IN ROW EXCLUSIVE MODE")
                        self._refuse_triggered(cur, output, partition, table)
                        self._delete_slice(cur, table, {column: partition} if column else {})
                    if run in found["builds"]:
                        columns = ", ".join(
                            sql.Identifier(r["attname"]).as_string(cur)
                            for r in cur.execute(
                                "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnum > 0 "
                                "AND NOT attisdropped AND attgenerated = '' ORDER BY attnum",
                                (table,),
                            )
                        )
                        cur.execute(
                            f"INSERT INTO {table} ({columns}) OVERRIDING SYSTEM VALUE SELECT {columns} FROM {build}"
                        )
                    if present:  # and after them: a partition attached meanwhile takes no lock it waits for
                        self._refuse_triggered(cur, output, partition, table)
                    cur.execute(
                        f"UPDATE {VERSIONS_TABLE} SET published = %s, "
                        "builds = ARRAY(SELECT b FROM unnest(builds) AS b WHERE b > %s), "
                        "pending = ARRAY(SELECT b FROM unnest(pending) AS b WHERE b > %s) "
                        "WHERE rel = %s AND part = %s",
                        (run, run, run, table, partition),
                    )
                    published = True
        except psycopg.IntegrityError as e:
            raise PublicationRefused(
                f"{output.name} {partition!r}: the full run's rows break a constraint of {table}, "
                f"so the published rows stay as they were: {str(e).splitlines()[0]}"
            ) from e
        self._sweep(table, partition)
        return published

    def _refuse_triggered(self, cur, output, partition, table) -> None:
        if cur.execute(_TRIGGERS, {"t": table}).fetchone()["triggered"]:
            raise PublicationRefused(
                f"{output.name} {partition!r}: {table} has a trigger or rule, on it or on a table that "
                "inherits from it, added after the full run began: publishing could run it on rows Solera "
                "has read, so the published rows stay as they were. Drop it, or write the output in place"
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
        table, _, _ = self._table(
            output, prior, context.home
        )  # where it is, even when a full run starts it over
        if context.reset:
            prior = None  # a full run keeps nothing of the content
        with self._connect() as conn, conn.cursor() as cur:
            checked, state = self._begin(conn, table, context)
            at = self._aim(table, context, checked, state)
            partition_col = output.config.get("partition_column")
            slice_where = {partition_col: context.partition} if partition_col else {}

            commit_number = _assigned_commit(context, prior) if output.incremental else None
            keys = changed = None
            made = True
            if isinstance(write, Sql):
                target = self._apply_sql(cur, output, write, context, at, state, slice_where)
                if output.key:  # the keys it wrote, read in its own transaction: no later write's
                    keys = self._held(cur, target, output.key, slice_where)
                    changed = Changed(keys, whole=True)
            elif output.key is not None:
                if isinstance(write, Patch) and not output.incremental:
                    raise WriteError(f"{output.name}: Patch requires an incremental output")
                write = KeyedWrite.of(self, write, output, prior)
                changed = write.changed()
                # Nothing to write, and no table: none to make (a remove finds no row).
                made = bool(
                    state["present"]
                    or (write.reset and output.config.get("columns"))
                    or write.upserts
                    or len(write.prepared.rows)
                )
                if made:
                    self._apply_keyed(cur, output, write, context, at, state, slice_where)
            elif isinstance(write, Patch):
                made = self._apply_commit(
                    cur, output, write, context, at, state, slice_where, prior, commit_number
                )
            else:
                if output.incremental:
                    raise WriteError(
                        f"{output.name}: an unkeyed incremental output only accepts Patch writes"
                    )
                self._apply_replace(cur, output, write, context, at, state, slice_where)
            if not made:
                at.made_nothing()
            self._register(cur, at, checked)
        if at.retired:
            self._sweep(table, context.partition)
        if not made:
            return Written(prior if output.key is None else None, changed=changed)
        commit_mode = output.key is None and output.incremental
        handle = {
            "table": table,
            "where": slice_where,
            "key": COMMIT_COLUMN if commit_mode else output.key,
            "commit_number": commit_number if commit_mode else None,
        }
        if at.run is not None:
            handle["build"] = at.run  # Solera reads the build while it is open (D211, obligation 3)
        return Written(
            TableRef(output=output.name, store="", handle=handle, partition=context.partition),
            keys,
            changed,
        )

    def _apply_replace(self, cur, output, write, context, at, state, slice_where) -> None:
        """An unkeyed output's whole content."""

        rows = frames.rows_of(write, output.name)
        kinds = frames.frame_kinds(write) if frames.is_frame(write) else None
        target = self._ensure(cur, output, at, state, lambda: rows, kinds=kinds)
        self._delete_slice(cur, target, slice_where)
        self._insert(cur, output, target, rows, self._stamps(output, context))

    def _apply_commit(self, cur, output, write: Patch, context, at, state, slice_where, prior, commit_number):
        """An unkeyed incremental output's commit: its rows stamped with the
        commit columns, in place of this commit's (a retry's) — or, with no
        prior (a first write, or a reset), of every commit."""

        if not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        rows = frames.rows_of(write.rows, output.name)
        if not rows and (prior is not None or not (state["present"] or output.config.get("columns"))):
            return False  # nothing to write; with no table and no declared columns, no shape to make one
        rows = [{**row, SEQ_COLUMN: i} for i, row in enumerate(rows)]
        target = self._ensure(cur, output, at, state, lambda: rows)
        self._delete_slice(
            cur, target, slice_where if prior is None else {**slice_where, COMMIT_COLUMN: commit_number}
        )
        self._insert(
            cur, output, target, rows, {**self._stamps(output, context), COMMIT_COLUMN: commit_number}
        )
        return True

    def _apply_keyed(self, cur, output, write: KeyedWrite, context, at, state, slice_where) -> None:
        """Every key is the group of rows that carry it. A whole write is the
        partition's content: cleared, then written; otherwise only the keys it
        writes change — their rows replaced by their groups, a page at a
        time — and its removes go, every other row untouched. A write of
        nothing still runs its version check: the first attempt after one
        that ended without a commit writes, even nothing, so the late writer
        is locked out (D196, rule 3; docs/versions.md §5)."""

        target = self._ensure(
            cur, output, at, state, lambda: write.prepared.take(None), kinds=write.prepared.kinds
        )
        stamps = self._stamps(output, context)
        if write.reset:
            self._delete_slice(cur, target, slice_where)
        for chunk in write.iter_chunks():
            keys = [key for key, _ in chunk]
            if not write.reset:
                self._delete_keys(cur, output, target, slice_where, keys)
            self._insert(cur, output, target, [row for _, group in chunk for row in group], stamps)
            self._check_keys(cur, output, target, slice_where, keys)
        if write.removes and not write.reset:
            self._delete_keys(cur, output, target, slice_where, sorted(write.removes))

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
        """The columns the store sets on every row: the partition's, and the
        writing attempt's version (None outside an attempt)."""

        column = output.config.get("partition_column")
        return {**({column: context.partition} if column else {}), VERSION_COLUMN: context.generation}

    def _delete_keys(self, cur, output, table, slice_where, keys: list[str]) -> None:
        cur.execute(
            f"DELETE FROM {table} WHERE {self._where_sql(slice_where)} AND {_ident(output.key)}::text = ANY(%s)",
            [slice_where[k] for k in sorted(slice_where)] + [keys],
        )

    def _apply_sql(self, cur, output, write: Sql, context, at, state, slice_where) -> str:
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
            columns = [name for name, _ in described if name not in (partition_col, VERSION_COLUMN)]
            # The query's own column types, for a table it creates; declared ones win.
            types = {
                r["oid"]: r["t"]
                for r in cur.execute(
                    "SELECT oid, format_type(oid, NULL) AS t FROM pg_type WHERE oid = ANY(%s)",
                    ([oid for _, oid in described],),
                )
            }
            inferred = {name: _inferred(types.get(oid)) for name, oid in described}
            table = self._ensure(cur, output, at, state, inferred=inferred)
            self._delete_slice(cur, table, slice_where)
            selected = ", ".join(_ident(c) for c in columns)
            stamped = [*([partition_col] if partition_col else []), VERSION_COLUMN]
            into = ", ".join(_ident(c) for c in [*columns, *stamped])
            values = [*([context.partition] if partition_col else []), context.generation]
            stamp = "".join(f", {sql.Literal(v).as_string(source)}" for v in values)
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
        return table

    def prepare(self, write, output: Output) -> Prepared:
        """A keyed write of plain Python, DataFrames or Arrow (`frames`)."""

        return frames.prepare(write, output)

    def keys(self, ref: Ref, among: list[str] | None = None):
        """The keys `ref`'s partition holds — among `among`, or all of them —
        sorted by their bytes, a chunk at a time: never a value
        (docs/versions.md §5)."""

        handle = ref.handle or {}
        return self._keys(ref, handle["key"], dict(handle.get("where") or {}), among)

    def _held(self, cur, table, key_column: str, where: dict, versions: list[int] | None = None) -> _Spool:
        """The keys (as text) the partition holds as `cur`'s transaction sees
        it — those of rows at one of `versions`, if given — sorted by their
        bytes: read through a server-side cursor and spooled to a file, so a
        write of 100 million keys holds a chunk of them in memory at a time."""

        key, spool = _ident(key_column), _Spool()
        params, only = [where[k] for k in sorted(where)], ""
        if versions is not None:
            params, only = [*params, versions], f" AND {_ident(VERSION_COLUMN)} = ANY(%s)"
        with cur.connection.cursor(name="solera_held") as held:
            held.execute(
                f"SELECT DISTINCT {key}::text AS k, convert_to({key}::text, 'UTF8') AS b FROM {table} "
                f"WHERE {self._where_sql(where)}{only} ORDER BY 2",
                params,
            )
            while chunk := held.fetchmany(KEY_CHUNK):
                spool.add(r["k"] for r in chunk)
        return spool

    def _source(self, cur, ref: Ref, handed: bool = False) -> str:
        """The table a ref's rows are in: the build of the run its handle names
        while the partition registers it, else the user's table (D211,
        obligation 3). Asked in the reader's transaction; a build no commit
        names is read by no one. A registered build the catalog no longer
        holds is lost (`RunLost`): its rows are nowhere. Inside a snapshot
        (`handed`) the registry is the snapshot's and the catalog is now's,
        so a build a switch published and dropped since looks the same: read
        again (`BuildGone`), in a snapshot that tells them apart."""

        handle = ref.handle or {}
        table = handle.get("table") or _qname(handle.get("schema", "public"), handle["name"])
        run = handle.get("build")
        if run is None:
            return table
        build = _qname(BUILD_SCHEMA, _build_name(table, ref.partition, run))
        found = cur.execute(
            f"SELECT %s = ANY(builds) AS open, to_regclass(%s) IS NULL AS gone FROM {VERSIONS_TABLE} "
            "WHERE rel = %s AND part = %s",
            (run, build, table, ref.partition),
        ).fetchone()
        if not (found and found["open"]):
            return table
        if found["gone"] and handed:
            raise BuildGone(
                f"{ref.output}: run {run}'s build was published and dropped after this read's snapshot "
                "began, or is lost; read again"
            )
        if found["gone"]:
            raise RunLost(
                f"{ref.output} {ref.partition!r}: the build of full run {run} is gone (a migration "
                "dropped it, or a restore lost it): the rows this commit names are nowhere"
            )
        return build

    def _keys(self, ref: Ref, key_column: str, where: dict, among: list[str] | None):
        """The partition's keys (as text) sorted by their bytes, a chunk at a time
        from a server-side cursor, read once the iteration starts — after a
        write has committed."""

        import psycopg
        from psycopg.rows import tuple_row

        key = _ident(key_column)
        params = [where[k] for k in sorted(where)]
        only = ""
        if among is not None:
            only, params = f" AND {key}::text = ANY(%s)", [*params, sorted(among)]
        query = (
            f"SELECT DISTINCT {key}::text, convert_to({key}::text, 'UTF8') FROM {{}} "
            f"WHERE {self._where_sql(where)}{only} ORDER BY 2"
        )
        handle = ref.handle or {}
        table = handle.get("table") or _qname(handle.get("schema", "public"), handle["name"])
        with self._connect() as conn, conn.cursor(name="solera_keys", row_factory=tuple_row) as cur:
            source = self._source(conn.cursor(), ref)
            if source == table:
                cur.execute(query.format(table), params)
            else:
                try:
                    with conn.transaction():  # a savepoint: the build may be gone by the time it is read
                        cur.execute(query.format(source), params)
                except psycopg.errors.UndefinedTable:  # dropped after the lookup: look again, as `_read`
                    cur.execute(query.format(self._source(conn.cursor(), ref)), params)
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

        applied = []
        table, _, _ = self._table(output, prior, context.home if context is not None else None)
        self._ready()
        # What already ran, read with no lock: an attempt that finds nothing pending takes no
        # exclusive domain, so it never waits out a switch (probe R7).
        with self._connect() as conn, conn.cursor() as cur:
            self._ensure_ledger(cur)
            ran = {
                r["name"]
                for r in cur.execute(f"SELECT name FROM {LEDGER_TABLE} WHERE relation = %s", (table,))
            }
        for migration in migrations:
            if migration.name in ran:
                applied.append(migration.name)
                continue
            with self._connect() as conn, conn.cursor() as cur:
                # The table's write domain, then the table lock, as a writer takes them
                # (`_store`, `_ensure`): the exclusive domain also serializes migrations.
                self._domain(cur, table, exclusive=True)
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (table,))
                done = cur.execute(
                    f"SELECT 1 FROM {LEDGER_TABLE} WHERE relation = %s AND name = %s",
                    (table, migration.name),
                ).fetchone()
                if done:
                    applied.append(migration.name)
                    continue
                if context is not None and context.generation is not None:
                    self._check(cur, table, context, write=False)
                self._drop_builds(cur, table)
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
                    f"INSERT INTO {LEDGER_TABLE} (relation, name) VALUES (%s, %s)",
                    (table, migration.name),
                )
                applied.append(migration.name)
        return applied

    def _drop_builds(self, cur, table: str) -> None:
        """Before a migration changes the table: drop every build of it, and
        leave each registered, as lost. A build is a second copy of the
        table's shape and rows that the migration would not change, and it
        draws from the table's sequences, so it would publish rows from
        before the migration, refuse the run's next batches, or block a
        migration that replaces the table. A full run in progress then fails
        its writes (`RunLost`) and is not published: it starts over, after
        the migration. Its writers wait on the exclusive domain this
        transaction holds; a load of a build makes the migration fail and
        retry (`BuildInUse`), rather than hold every writer while it waits.
        A pending run (`_aim`) has no build: it goes on after the migration."""

        import psycopg

        parts = [
            r["part"] for r in cur.execute(f"SELECT part FROM {VERSIONS_TABLE} WHERE rel = %s", (table,))
        ]
        names = self._builds_of(cur, table, parts)
        if not names:
            return
        cur.execute("SET LOCAL lock_timeout = '2s'")
        try:
            for name in names:
                cur.execute(f"DROP TABLE IF EXISTS {_qname(BUILD_SCHEMA, name)}")
        except psycopg.errors.LockNotAvailable as e:
            raise BuildInUse(f"{table}: a load is reading a build of it; the migration runs again") from e
        cur.execute("SET LOCAL lock_timeout = DEFAULT")

    # -- reads ----------------------------------------------------------------

    def can_serve(self, source) -> bool:
        """A source this store can read: a `table`, and for a keyed one the
        `version_column` that says each row's version."""

        return bool(source.handle.get("table")) and (
            source.key is None or bool(source.handle.get("version_column"))
        )

    async def serve(self, source, keys, ctx):
        """A source read from a table this store does not write: `Source("orders",
        store="pg", key="id", table="public.orders", version_column="updated_at")`
        (docs/stores.md, "Sources: how data is loaded"); the table may name
        `{partition}`. Keyed, each asked key's row at the version its
        `version_column` holds, read in one snapshot; unkeyed, every row, at
        the column's greatest value (none without the column)."""

        return await asyncio.to_thread(self._serve, source, keys, ctx)

    def _serve(self, source, keys, ctx):
        from solera.sdk import Loaded

        named = str(source.handle["table"]).format(partition=ctx.partition)
        if named.startswith('"'):  # quoted already: "schema"."table"
            table = _qname(*_split(named))
        else:
            schema, _, name = named.rpartition(".")
            table = _qname(schema or "public", name)
        version, clauses, params = source.handle.get("version_column"), [], []
        for column, value in (source.handle.get("where") or {}).items():  # a fixed filter on the table
            clauses.append(f"{_ident(column)} = %s")
            params.append(value)
        if source.key is not None and keys is not None:
            clauses.append(f"{_ident(source.key)}::text = ANY(%s)")
            params.append(list(keys))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn, conn.cursor() as cur:
            conn.read_only = True
            rows = cur.execute(f"SELECT * FROM {table}{where}", params).fetchall()
        if source.key is None:
            newest = max((str(r[version]) for r in rows if r.get(version) is not None), default=None)
            return Loaded(rows, version=newest if version else None)
        return {str(r[source.key]): Loaded(r, version=str(r[version])) for r in rows}

    async def load(self, ref: Ref, t, selection: Keys | Commits | None) -> Any:
        return (await self.read(ref, t, selection)).value

    async def read(self, ref: Ref, t, selection: Keys | Commits | None) -> Read:
        """`load`, and the versions it read, from each row's version column
        (docs/stores.md, "What a write and a load say"): a table holds its
        rows as they are now, so a key read may be at another version than
        planned, or absent (None)."""

        if isinstance(t, type) and issubclass(t, Ref):
            return Read(ref)
        return await asyncio.to_thread(self._read, ref, t, selection)

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

        if not cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (VERSIONS_TABLE,)).fetchone()["ok"]:
            return None
        found = cur.execute(
            f"SELECT written FROM {VERSIONS_TABLE} WHERE rel = %s AND part = %s",
            ((ref.handle or {}).get("table"), ref.partition),
        ).fetchone()
        return None if found is None or found["written"] is None else int(found["written"])

    def _load(self, ref: Ref, t, selection: Keys | Commits | None, conn=None) -> Any:
        return self._read(ref, t, selection, conn).value

    def _read(self, ref: Ref, t, selection: Keys | Commits | None, conn=None) -> Read:
        import psycopg

        handle, handed = ref.handle or {}, conn is not None
        with contextlib.nullcontext(conn) if conn else self._connect() as conn, conn.cursor() as cur:
            table = handle.get("table") or _qname(handle.get("schema", "public"), handle["name"])
            source = self._source(cur, ref, handed)
            where = dict(handle.get("where") or {})
            sql, params = "SELECT * FROM {}", []
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
            try:
                with conn.transaction() if source != table else contextlib.nullcontext():
                    found = cur.execute(sql.format(source), params)  # a build may be gone: a savepoint
                    rows = found.fetchall()
            except psycopg.errors.UndefinedTable:
                if source == table:
                    raise
                if handed:  # a snapshot from before the switch: the user's table holds the old rows there
                    raise BuildGone(
                        f"{ref.output}: run {handle.get('build')}'s build was published and dropped after "
                        "this read's snapshot began; read again"
                    ) from None
                # Dropped after the lookup: a switch published it, and its rows are the user's table's
                # now, which a new statement sees (probes P2, sol1/read_drop.py, sol2/load_retry.py);
                # or a migration dropped it, and it is lost. Looking again tells them apart.
                found = cur.execute(sql.format(self._source(cur, ref)), params)
                rows = found.fetchall()
            columns = [d.name for d in found.description or () if d.name != VERSION_COLUMN]
            if handle.get("commit_number") is not None:
                internal = {COMMIT_COLUMN, SEQ_COLUMN}
                rows = [{k: v for k, v in r.items() if k not in internal} for r in rows]
        # Every row's version, taken off it: a key's rows share theirs (one write wrote them).
        keyed = bool(handle.get("key")) and handle.get("commit_number") is None
        versions: dict[str, Any] | None = {} if keyed else None
        newest = None
        for row in rows:
            version = row.pop(VERSION_COLUMN, None)
            if versions is not None:
                versions[str(row[handle["key"]])] = version
            elif version is not None and (newest is None or version > newest):
                newest = version
        if versions is not None and isinstance(selection, Keys):
            versions = {k: versions.get(k) for k in selection.generations}
        inner = by_key_type(t)
        if inner is not MISSING and isinstance(selection, Keys):
            # Each selected key's group: a key with no rows does not exist (per-key §6).
            groups: dict[str, list] = {}
            for row in rows:
                groups.setdefault(str(row[handle["key"]]), []).append(row)
            return Read({k: _materialize(g, inner, columns) for k, g in groups.items()}, versions)
        return Read(_materialize(rows, t), versions, newest)

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
