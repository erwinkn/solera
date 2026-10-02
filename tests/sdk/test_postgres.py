"""§3/§4: PostgresStore — Patch, batch snapshots, partition slices,
Sql writes. Skips unless SOLERA_TEST_DATABASE_URL points at a scratch database."""

import os
import uuid

import pytest
from solera.keys import Rows
from solera.sdk import Output
from solera.stores import Keys, Patch, Sql, StoreError, WriteError

from tests.conftest import scope

pytestmark = pytest.mark.postgres

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")


@pytest.fixture
def store():
    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    return PostgresStore(DSN)


def output(name=None, **config):
    return Output(name or f"t_{uuid.uuid4().hex[:12]}", store="postgres", **config)


async def test_bare_replace_and_versions(store):
    """§3: a bare write replaces the scope; equal content keeps the version."""

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    second = await store.store([{"id": "a", "v": "1"}], first.ref, scope(out))
    assert first.ref.version == second.ref.version
    third = await store.store([{"id": "a", "v": "2"}], first.ref, scope(out))
    assert third.ref.version != first.ref.version
    assert await store.load(third.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_patch_upsert_and_remove(store):
    """§3/§4: Patch upserts keys and removes others; with no prior it is the
    whole content. The store reports no keys: the harness derives them."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    assert first.keys is None
    second = await store.store(
        Patch([{"id": "b", "v": "2"}, {"id": "c", "v": "9"}], remove=["a"]),
        first.ref,
        scope(out),
    )
    rows = await store.load(second.ref, list[dict], None)
    assert sorted((r["id"], r["v"]) for r in rows) == [("b", "2"), ("c", "9")]
    with pytest.raises(WriteError):
        await store.store(
            Patch([{"id": "x", "v": "1"}, {"id": "x", "v": "1"}]),
            second.ref,
            scope(out),
        )
    reset = await store.store(Patch([{"id": "z", "v": "1"}]), None, scope(out))
    assert [r["id"] for r in await store.load(reset.ref, list[dict], None)] == ["z"]


async def test_patch_replaces_each_keys_rows(store):
    """§6: every key is the group of rows that carry it. A patch replaces all
    of a key's rows; keys it does not name keep theirs."""

    out = output(key="path", revision="v")
    first = await store.store(
        Patch(
            [
                {"path": "a.csv", "v": "1", "n": 1},
                {"path": "a.csv", "v": "1", "n": 2},
                {"path": "b.csv", "v": "1", "n": 1},
            ]
        ),
        None,
        scope(out),
    )
    second = await store.store(Patch([{"path": "a.csv", "v": "2", "n": 3}]), first.ref, scope(out))
    rows = await store.load(second.ref, list[dict], None)
    assert sorted((r["path"], r["n"]) for r in rows) == [("a.csv", 3), ("b.csv", 1)]


async def test_a_lost_commit_does_not_stick_the_slice(store):
    """§8: an attempt whose write landed but whose commit was lost leaves the
    table ahead of the head. The next write, from the older prior, goes
    through; reads get what the slice holds now."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    await store.store([{"id": "a", "v": "2"}], first.ref, scope(out))  # its commit is lost
    again = await store.store([{"id": "a", "v": "3"}], first.ref, scope(out))
    assert (await store.load(first.ref, list[dict], None))[0]["v"] == "3"
    assert (await store.load(again.ref, list[dict], None))[0]["v"] == "3"


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
    assert sql_ref.keys is None  # unkeyed: nothing to report


async def test_keyed_sql_reports_its_keys(store):
    """§6/§9: the harness never sees rows a Sql write materializes, so the
    store reports the slice's complete key map."""

    source = output(key="id", revision="v", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, scope(source))
    derived = output(key="id", revision="v")
    sql = await store.store(Sql(f"SELECT id, v FROM {written.ref.table}"), None, scope(derived))
    assert [row for chunk in sql.keys for row in chunk] == [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}]
    # Without a revision, every column: versioned as the same rows from Python would be.
    digested = await store.store(Sql(f"SELECT id, v FROM {written.ref.table}"), None, scope(output(key="id")))
    rows = [row for chunk in digested.keys for row in chunk]
    assert (
        Rows.records(rows, "id").entries()
        == Rows.records([{"id": "b", "v": "2"}, {"id": "a", "v": "1"}], "id").entries()
    )


async def test_aliases_rename_the_table(store):
    """§2: an output renamed through its asset's aliases takes its table along."""

    old = output(key="id", revision="v", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, scope(old))
    new = output(key="id", revision="v", primary_key=["id"])
    from solera.stores import Scope

    moved = await store.store(
        Patch([{"id": "b", "v": "1"}]),
        first.ref,
        Scope(output=new, partition="", batch=1, attempt="t", aliases=(old.name,)),
    )
    assert new.name in moved.ref.table
    assert sorted(r["id"] for r in await store.load(moved.ref, list[dict], None)) == ["a", "b"]


async def test_batch_snapshot_at_pinned_version(store):
    """§3/§2.1: an unkeyed incremental ref's load returns rows up to the
    pinned batch, even after later writes."""

    out = output(incremental=True, partition_column="site")
    first = await store.store(Patch([{"e": 1}]), None, scope(out, partition="s1", batch=0))
    second = await store.store(Patch([{"e": 2}]), first.ref, scope(out, partition="s1", batch=1))
    assert first.ref.handle["batch"] == 0 and second.ref.handle["batch"] == 1
    at_first = await store.load(first.ref, list[dict], None)
    assert [r["e"] for r in at_first] == [1]
    at_second = await store.load(second.ref, list[dict], None)
    assert [r["e"] for r in at_second] == [1, 2]


async def test_keys_selection(store):
    """§4: load under Keys returns only the selected keys."""

    out = output(key="id", revision="v", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, scope(out))
    rows = await store.load(written.ref, list[dict], Keys({"b": (b"2", 0)}))
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
            "SELECT name FROM public.solera_migrations WHERE output = %s ORDER BY at, name",
            (output_name,),
        ).fetchall()
        return [r["name"] for r in rows]


async def test_migrations_apply_in_order_and_record(store):
    """§4: pending migrations apply in declared order and land in the
    solera_migrations ledger."""
    from solera.sdk import Migration

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

    from solera.sdk import Migration

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
    from solera.sdk import Migration

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
    from solera.sdk import Migration

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


def fenced(out, generation, invocation="i"):
    return scope(out, generation=generation, invocation=invocation)


async def test_a_newer_generation_fences_older_writers(store):
    """docs/lifecycle.md §9.7: once a newer attempt acquired a slice, an older
    one can change nothing; another invocation of the same generation can't
    either, while the same invocation acquiring again is its own retry."""

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 5))
    await store.acquire(fenced(out, 7))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "0"}], first.ref, fenced(out, 5))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.acquire(fenced(out, 7, "a duplicate"))
    await store.acquire(fenced(out, 7))
    written = await store.store([{"id": "a", "v": "2"}], first.ref, fenced(out, 7))
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_a_takeover_waits_for_an_older_writers_transaction(store):
    """The older writer's transaction is open, holding the slice: the newer
    acquisition waits for it to end, and from then on the older writer's
    next transaction is refused."""

    import asyncio
    import threading

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 5))
    table, _, _ = store._table(out)
    conn = store._connect()
    cur = conn.cursor()
    store._fence(cur, table, fenced(out, 5))  # the older writer's open transaction
    acquired = threading.Event()

    def take():
        asyncio.run(store.acquire(fenced(out, 9)))
        acquired.set()

    thread = threading.Thread(target=take)
    thread.start()
    assert not acquired.wait(0.5)  # waiting behind it
    cur.execute(f"UPDATE {table} SET v = 'old' WHERE id = 'a'")
    conn.commit()  # its write lands before the takeover
    assert acquired.wait(5)
    thread.join()
    conn.close()
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "late"}], first.ref, fenced(out, 5))
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "old"}]


async def test_the_first_write_creates_and_acquires(store):
    """A table that does not exist yet is acquired by the write that creates
    it, in the same transaction; a stale writer arriving later is refused."""

    out = output(key="id", revision="v")
    await store.acquire(fenced(out, 3))  # no table yet: nothing to take
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 3))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "0"}], first.ref, fenced(out, 2))
    await store.store([{"id": "a", "v": "2"}], first.ref, fenced(out, 3))


async def test_a_migration_that_replaces_the_table_keeps_its_fence(store):
    """A migration that recreates the relation gives it a new OID; the fence
    row moves with it, so a stale writer is still refused."""

    from solera.sdk import Migration

    name = f"t_{uuid.uuid4().hex[:12]}"
    out = output(name, key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 4))
    swap = (
        f'CREATE TABLE public."{name}_new" AS SELECT * FROM public."{name}"; '
        f'DROP TABLE public."{name}"; ALTER TABLE public."{name}_new" RENAME TO "{name}"'
    )
    await store.migrate(out, [Migration("swap", swap)])
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "0"}], first.ref, fenced(out, 3))
    await store.store([{"id": "a", "v": "2"}], first.ref, fenced(out, 4))


async def test_a_patch_refuses_requested_keys_it_does_not_hold(store):
    """A key the harness asks the store to write must be in the write: a
    missing one is an error, never a silent skip."""

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    with pytest.raises(StoreError, match="does not hold"):
        await store.store(
            Patch([{"id": "a", "v": "2"}]), first.ref, scope(out, upserts=frozenset({"a", "zzz"}))
        )


async def test_reconciliation_streams_the_slice_and_digests_rows_as_written(store, monkeypatch):
    """After a dead `Sql` writer, a patch reconciles the whole slice: read
    back through `scan` a chunk at a time — never loaded whole — and digested
    as written rows are, without the partition column the store stamps. An
    unchanged stored row stays unchanged."""

    from obstore.store import MemoryStore
    from solera.keys import _python
    from solera.keys.index import IndexState, KeyIndex
    from solera.keys.io import ObjectIO
    from solera_worker import worker

    out = output(key="id", partition_column="site")
    rows = [{"id": "a", "x": 1}, {"id": "c", "x": 3}]
    written = await store.store(rows, None, scope(out, partition="oakland"))
    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/")
    files, _ = await KeyIndex(io, None, state).replace(store.key_rows(rows, out), 0, "w1")
    state = state.committed(0, files, keep_log=True)

    async def no_load(*args, **kwargs):
        raise AssertionError("reconciliation streams the slice, it never loads it whole")

    monkeypatch.setattr(store, "load", no_load)
    patch = [{"id": "b", "x": 2, "site": "oakland"}]  # a row may carry the stamped column, or not
    keys, versions = store.key_rows(patch, out).entries()
    new = dict(zip((k.decode() for k in keys), versions, strict=True))
    delta, _ = await worker._reconcile(
        out, store, written.ref, KeyIndex(io, None, state), patch, new, ["c"], 1, "w2", 2
    )
    got = [
        e[:3] for f in delta.files for e in _python.iter_file(await io.read_whole(state.path(f.name), f.size))
    ]
    assert got == [(b"b", new["b"], 0), (b"c", b"", 1)]  # `a` is unchanged
    assert new["b"] == store.key_rows([{"id": "b", "x": 2}], out).entries()[1][0]
