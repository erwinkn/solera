"""§3/§4: PostgresStore — markers, Patch, batch snapshots, partition slices,
Sql writes. Skips unless CURSUS_TEST_DATABASE_URL points at a scratch database."""

import os
import uuid

import pytest
from cursus.sdk import Output
from cursus.stores import Keys, Patch, Sql, StaleRead, StoreConflict, StoreError, WriteError

from tests.conftest import scope

pytestmark = pytest.mark.postgres

DSN = os.environ.get("CURSUS_TEST_DATABASE_URL")


@pytest.fixture
def store():
    if not DSN:
        pytest.skip("CURSUS_TEST_DATABASE_URL is not set")
    from cursus_postgres import PostgresStore

    return PostgresStore(DSN)


def output(name=None, **config):
    return Output(name or f"t_{uuid.uuid4().hex[:12]}", store="postgres", **config)


async def test_bare_replace_and_versions(store):
    """§3: a bare write replaces the scope; equal content keeps the version."""

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    second = await store.store([{"id": "a", "v": "1"}], first.ref, scope(out, baseline=first.ref))
    assert first.ref.version == second.ref.version
    third = await store.store([{"id": "a", "v": "2"}], first.ref, scope(out))
    assert third.ref.version != first.ref.version
    assert await store.load(third.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_patch_upsert_remove_and_orphan_sweep(store):
    """§3/§4: Patch upserts keys, removes others, and deletes rows absent
    from the resulting map (orphans left by uncommitted attempts)."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    assert first.delta.upserted == {"a": "1", "b": "1"}
    second = await store.store(
        Patch([{"id": "b", "v": "2"}, {"id": "c", "v": "9"}], remove=["a"]),
        first.ref,
        scope(out, baseline=first.ref),
    )
    assert second.delta.upserted == {"b": "2", "c": "9"}
    assert second.delta.deleted == ("a",)
    assert sorted(r["id"] for r in await store.load(second.ref, list[dict], None)) == ["b", "c"]
    with pytest.raises(WriteError):
        await store.store(
            Patch([{"id": "x", "v": "1"}, {"id": "x", "v": "1"}]),
            second.ref,
            scope(out, baseline=second.ref),
        )


async def test_marker_fences_store_and_load(store):
    """§3: store() refuses when the live marker != prior.version; load()
    refuses when it != ref.version."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    moved = await store.store([{"id": "a", "v": "2"}], first.ref, scope(out, baseline=first.ref))
    with pytest.raises(StoreConflict):
        await store.store([{"id": "a", "v": "3"}], first.ref, scope(out, baseline=first.ref))
    with pytest.raises(StaleRead):
        await store.load(first.ref, list[dict], None)
    assert (await store.load(moved.ref, list[dict], None))[0]["v"] == "2"


async def test_partition_column_stamping(store):
    """§4: partition_column is stamped from the scope; disagreeing rows are rejected."""

    out = output(key="id", revision="v", primary_key=["id"], partition_column="site")
    with pytest.raises(WriteError):
        await store.store([{"id": "a", "v": "1", "site": "Perth"}], None, scope(out, partition="Richmond"))
    written = await store.store([{"id": "a", "v": "1"}], None, scope(out, partition="Richmond"))
    assert written.ref.where == {"site": "Richmond"}
    rows = await store.load(written.ref, list[dict], None)
    assert rows == [{"id": "a", "v": "1", "site": "Richmond"}]


async def test_sql_materializes_select(store):
    """§4: Sql with a SELECT materializes into {schema}.{table} and returns a
    loadable TableRef."""

    source = output(key="id", revision="v", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, scope(source))
    derived = output()
    sql_ref = await store.store(Sql(f"SELECT id, v FROM {written.ref.table}"), None, scope(derived))
    assert derived.name in sql_ref.ref.table
    rows = await store.load(sql_ref.ref, list[dict], None)
    assert sorted(r["id"] for r in rows) == ["a", "b"]


async def test_batch_snapshot_at_pinned_version(store):
    """§3/§2.1: an unkeyed incremental ref's load returns rows up to the
    pinned batch, even after later writes."""

    out = output(incremental=True, partition_column="site")
    first = await store.store(Patch([{"e": 1}]), None, scope(out, partition="s1"))
    second = await store.store(Patch([{"e": 2}]), first.ref, scope(out, partition="s1", baseline=first.ref))
    assert first.delta.batch == 0 and first.delta.reset
    assert second.delta.batch == 1
    at_first = await store.load(first.ref, list[dict], None)
    assert [r["e"] for r in at_first] == [1]
    at_second = await store.load(second.ref, list[dict], None)
    assert [r["e"] for r in at_second] == [1, 2]


async def test_keys_selection(store):
    """§4: load under Keys returns only the selected keys."""

    out = output(key="id", revision="v", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, scope(out))
    rows = await store.load(written.ref, list[dict], Keys({"b": "2"}))
    assert [r["id"] for r in rows] == ["b"]


async def test_dataframe_round_trip(store):
    """§4: PostgresStore accepts and produces DataFrames."""

    pd = pytest.importorskip("pandas")
    out = output(key="id", revision="v", primary_key=["id"])
    frame = pd.DataFrame([{"id": "a", "v": "1"}])
    written = await store.store(frame, None, scope(out))
    loaded = await store.load(written.ref, pd.DataFrame, None)
    assert list(loaded["id"]) == ["a"]


def _ledger(store, output_name):
    with store._connect() as conn, conn.cursor() as cur:
        rows = cur.execute(
            "SELECT name FROM public.cursus_migrations WHERE output = %s ORDER BY at, name",
            (output_name,),
        ).fetchall()
        return [r["name"] for r in rows]


async def test_migrations_apply_in_order_and_record(store):
    """§4: pending migrations apply in declared order and land in the
    cursus_migrations ledger."""
    from cursus.sdk import Migration

    out = output()
    table = out.name
    out.migrations = (
        Migration("m1", f'CREATE TABLE IF NOT EXISTS "{table}_m1" (id text)'),
        Migration("m2", lambda cur: cur.execute(f'CREATE TABLE "{table}_m2" (id text)')),
    )
    applied = await store.migrate(out, out.migrations)
    assert applied == ["m1", "m2"]
    assert _ledger(store, out.name) == ["m1", "m2"]
    second = await store.migrate(out, out.migrations)
    assert second == ["m1", "m2"]  # ledger names; nothing re-applied


async def test_concurrent_migrate_applies_each_once(store):
    """§4: two concurrent migrate calls apply each migration exactly once
    (advisory lock + ledger re-read inside it)."""
    import asyncio

    from cursus.sdk import Migration

    out = output()
    runs = f'"{out.name}_runs"'

    def bump(cur):
        cur.execute(f"CREATE TABLE IF NOT EXISTS {runs} (n int)")
        cur.execute(f"INSERT INTO {runs} VALUES (1)")
        cur.execute("SELECT pg_sleep(0.05)")  # widen the lock window

    migrations = [Migration("once", bump)]
    await asyncio.gather(
        asyncio.to_thread(asyncio.run, store.migrate(out, migrations)),
        asyncio.to_thread(asyncio.run, store.migrate(out, migrations)),
    )
    with store._connect() as conn, conn.cursor() as cur:
        n = cur.execute(f"SELECT count(*) AS c FROM {runs}").fetchone()["c"]
    assert n == 1
    assert _ledger(store, out.name) == ["once"]


async def test_failed_migration_leaves_no_ledger_row(store):
    """§4: a failing migration rolls back its work and writes no ledger row."""
    from cursus.sdk import Migration

    out = output()
    table = f'"{out.name}_ghost"'

    def boom(cur):
        cur.execute(f"CREATE TABLE {table} (id text)")
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError, match="nope"):
        await store.migrate(out, [Migration("bad", boom)])
    with store._connect() as conn, conn.cursor() as cur:
        ghost = cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (f"{out.name}_ghost",),
        ).fetchone()
    assert ghost is None
    assert _ledger(store, out.name) == []


async def test_schema_drift_fails_the_write(store):
    """§4: a live table whose shape differs from the declaration fails the
    write non-retryably and names the difference."""

    out = output(key="id", primary_key=["id"])
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{out.name}" (id text, v text, PRIMARY KEY (v))')
    with pytest.raises(StoreError, match="does not match the declaration"):
        await store.store([{"id": "a", "v": "1"}], None, scope(out))
    assert StoreError.retryable is False


async def test_migration_can_reconcile_drift(store):
    """§4: a migration that brings the live table in line lets the write pass."""
    from cursus.sdk import Migration

    out = output(key="id", primary_key=["id"])
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{out.name}" (id text, v text, PRIMARY KEY (v))')
    with pytest.raises(StoreError, match="does not match"):
        await store.store([{"id": "a", "v": "1"}], None, scope(out))
    out.migrations = (
        Migration(
            "fix_pk",
            f'ALTER TABLE "{out.name}" DROP CONSTRAINT "{out.name}_pkey", ADD PRIMARY KEY (id)',
        ),
    )
    assert await store.migrate(out, out.migrations) == ["fix_pk"]
    written = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    assert written.ref.version
