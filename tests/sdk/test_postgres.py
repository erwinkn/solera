"""§3/§4: PostgresStore — Patch, batch snapshots, partition partitions,
Sql writes. Skips unless SOLERA_TEST_DATABASE_URL points at a scratch database."""

import os
import uuid
from decimal import Decimal as D

import pytest
from solera.keys import SortedEntries
from solera.sdk import Output
from solera.stores import KeyedWrite, Keys, Patch, StoreError, WriteError, prepare_for
from solera_postgres import Sql

from tests.conftest import context

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


async def test_bare_replace(store):
    """§3: a bare write replaces the partition: the table keeps its place."""

    out = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}], None, context(out))
    second = await store.store([{"id": "a", "v": "2"}], first.ref, context(out))
    assert second.ref == first.ref  # the same table and partition: its generation is the worker's
    assert await store.load(second.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_patch_upsert_and_remove(store):
    """§3/§4: Patch upserts keys and removes others; with no prior it is the
    whole content. The store reports no keys: the worker derives them."""

    out = output(key="id", primary_key=["id"])
    first = await store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, context(out))
    assert first.keys is None
    second = await store.store(
        Patch([{"id": "b", "v": "2"}, {"id": "c", "v": "9"}], remove=["a"]),
        first.ref,
        context(out),
    )
    rows = await store.load(second.ref, list[dict], None)
    assert sorted((r["id"], r["v"]) for r in rows) == [("b", "2"), ("c", "9")]
    with pytest.raises(WriteError):
        await store.store(
            Patch([{"id": "x", "v": "1"}, {"id": "x", "v": "1"}]),
            second.ref,
            context(out),
        )
    reset = await store.store(Patch([{"id": "z", "v": "1"}]), None, context(out))
    assert [r["id"] for r in await store.load(reset.ref, list[dict], None)] == ["z"]


async def test_patch_replaces_each_keys_rows(store):
    """§6: every key is the group of rows that carry it. A patch replaces all
    of a key's rows; keys it does not name keep theirs."""

    out = output(key="path")
    first = await store.store(
        Patch(
            [
                {"path": "a.csv", "v": "1", "n": 1},
                {"path": "a.csv", "v": "1", "n": 2},
                {"path": "b.csv", "v": "1", "n": 1},
            ]
        ),
        None,
        context(out),
    )
    second = await store.store(Patch([{"path": "a.csv", "v": "2", "n": 3}]), first.ref, context(out))
    rows = await store.load(second.ref, list[dict], None)
    assert sorted((r["path"], r["n"]) for r in rows) == [("a.csv", 3), ("b.csv", 1)]


async def test_a_lost_commit_does_not_stick_the_slice(store):
    """§8: an attempt whose write landed but whose commit was lost leaves the
    table ahead of the head. The next write, from the older prior, goes
    through; reads get what the partition holds now."""

    out = output(key="id", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, context(out))
    await store.store([{"id": "a", "v": "2"}], first.ref, context(out))  # its commit is lost
    again = await store.store([{"id": "a", "v": "3"}], first.ref, context(out))
    assert (await store.load(first.ref, list[dict], None))[0]["v"] == "3"
    assert (await store.load(again.ref, list[dict], None))[0]["v"] == "3"


async def test_partition_column_stamping(store):
    """§4: partition_column is stamped from the partition; disagreeing rows are rejected."""

    out = output(key="id", primary_key=["id"], partition_column="site")
    with pytest.raises(WriteError):
        await store.store([{"id": "a", "v": "1", "site": "Perth"}], None, context(out, partition="Richmond"))
    written = await store.store([{"id": "a", "v": "1"}], None, context(out, partition="Richmond"))
    assert written.ref.where == {"site": "Richmond"}
    rows = await store.load(written.ref, list[dict], None)
    assert rows == [{"id": "a", "v": "1", "site": "Richmond"}]


async def test_sql_materializes_select(store):
    """§4: Sql with a SELECT materializes into {schema}.{table} and returns a
    loadable TableRef."""

    source = output(key="id", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, context(source))
    derived = output()
    sql_ref = await store.store(Sql(f"SELECT id, v FROM {written.ref.table}"), None, context(derived))
    assert derived.name in sql_ref.ref.table
    rows = await store.load(sql_ref.ref, list[dict], None)
    assert sorted(r["id"] for r in rows) == ["a", "b"]
    assert sql_ref.keys is None  # unkeyed: nothing to report


async def test_keyed_sql_reports_its_keys(store):
    """§6/§9: the worker never sees rows a Sql write materializes, so the
    store reports the keys the partition holds, sorted, each once."""

    source = output(key="id")
    rows = [{"id": "b", "v": "2"}, {"id": "a", "v": "1"}, {"id": "b", "v": "3"}]
    written = await store.store(rows, None, context(source))
    sql = await store.store(Sql(f"SELECT id, v FROM {written.ref.table}"), None, context(output(key="id")))
    assert [key for chunk in sql.keys for key in chunk] == ["a", "b"]


async def test_a_renamed_output_keeps_its_table(store):
    """§2: the committed head says where an output's content is, so an
    output renamed through its asset's aliases keeps writing — acquiring,
    migrating — its table, and every ref to it, the old ones included,
    stays readable. The declaration names a table only for a first write."""

    from solera.sdk import Migration

    old = output(key="id", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(old, 1))
    new = output(key="id", primary_key=["id"])
    await store.acquire(fenced(new, 2), first.ref)
    moved = await store.store(Patch([{"id": "b", "v": "1"}]), first.ref, fenced(new, 2))
    assert moved.ref.table == first.ref.table and new.name not in moved.ref.table
    assert sorted(r["id"] for r in await store.load(moved.ref, list[dict], None)) == ["a", "b"]
    # An older ref still reads its table — as it is now: a fenced store keeps one copy.
    assert sorted(r["id"] for r in await store.load(first.ref, list[dict], None)) == ["a", "b"]
    migrated = Output(
        new.name,
        store="postgres",
        key="id",
        primary_key=["id"],
        migrations=[
            Migration("add_n", lambda cur: cur.execute(f"ALTER TABLE {first.ref.table} ADD COLUMN n bigint"))
        ],
    )
    assert await store.migrate(migrated, migrated.migrations, fenced(migrated, 3), prior=moved.ref) == [
        "add_n"
    ]
    with store._connect() as conn:
        missing = conn.execute("SELECT to_regclass(%s) AS t", (f'public."{new.name}"',)).fetchone()["t"]
    assert missing is None  # nothing was created, or renamed, under the new name


async def test_batch_snapshot_at_pinned_version(store):
    """§3/§2.1: an unkeyed incremental ref's load returns rows up to the
    pinned batch, even after later writes."""

    out = output(incremental=True, partition_column="site")
    first = await store.store(Patch([{"e": 1}]), None, context(out, partition="s1", commit_number=0))
    second = await store.store(Patch([{"e": 2}]), first.ref, context(out, partition="s1", commit_number=1))
    assert first.ref.handle["commit_number"] == 0 and second.ref.handle["commit_number"] == 1
    at_first = await store.load(first.ref, list[dict], None)
    assert [r["e"] for r in at_first] == [1]
    at_second = await store.load(second.ref, list[dict], None)
    assert [r["e"] for r in at_second] == [1, 2]


async def test_keys_selection(store):
    """§4: load under Keys returns only the selected keys."""

    out = output(key="id", primary_key=["id"])
    written = await store.store([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}], None, context(out))
    rows = await store.load(written.ref, list[dict], Keys({"b": 1}))
    assert [r["id"] for r in rows] == ["b"]


async def test_dataframe_round_trip(store):
    """§4: PostgresStore accepts and produces DataFrames."""

    pd = pytest.importorskip("pandas")
    out = output(key="id", primary_key=["id"])
    frame = pd.DataFrame([{"id": "a", "v": "1"}])
    written = await store.store(frame, None, context(out))
    loaded = await store.load(written.ref, pd.DataFrame, None)
    assert list(loaded["id"]) == ["a"]


def _ledger(store, output):
    """The migrations the ledger says ran on `output`'s table."""

    with store._connect() as conn, conn.cursor() as cur:
        rows = cur.execute(
            "SELECT name FROM public.solera_migration_ledger WHERE relation = %s ORDER BY at, name",
            (store._table(output)[0],),
        ).fetchall()
        return [r["name"] for r in rows]


async def test_migrations_apply_in_order_and_record(store):
    """§4: pending migrations apply in declared order and land in the
    solera_migration_ledger."""
    from solera.sdk import Migration

    out = output()
    table = out.name
    out.migrations = (
        Migration("m1", f'CREATE TABLE IF NOT EXISTS "{table}_m1" (id text)'),
        Migration("m2", lambda cur: cur.execute(f'CREATE TABLE "{table}_m2" (id text)')),
    )
    applied = await store.migrate(out, out.migrations)
    assert applied == ["m1", "m2"]
    assert _ledger(store, out) == ["m1", "m2"]
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
    assert _ledger(store, out) == ["once"]


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
    assert _ledger(store, out) == []


async def test_schema_drift_fails_the_write(store):
    """§4: a live table whose shape differs from the declaration fails the
    write non-retryably and names the difference."""

    out = output(key="id", primary_key=["id"])
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{out.name}" (id text, v text, PRIMARY KEY (v))')
    with pytest.raises(StoreError, match="does not match the declaration"):
        await store.store([{"id": "a", "v": "1"}], None, context(out))
    assert StoreError.retryable is False


async def test_migration_can_reconcile_drift(store):
    """§4: a migration that brings the live table in line lets the write pass."""
    from solera.sdk import Migration

    out = output(key="id", primary_key=["id"])
    with store._connect() as conn, conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{out.name}" (id text, v text, PRIMARY KEY (v))')
    with pytest.raises(StoreError, match="does not match"):
        await store.store([{"id": "a", "v": "1"}], None, context(out))
    out.migrations = (
        Migration(
            "fix_pk",
            f'ALTER TABLE "{out.name}" DROP CONSTRAINT "{out.name}_pkey", ADD PRIMARY KEY (id)',
        ),
    )
    assert await store.migrate(out, out.migrations) == ["fix_pk"]
    written = await store.store([{"id": "a", "v": "1"}], None, context(out))
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "v": "1"}]


def fenced(out, generation, worker_id="i", partition=""):
    return context(out, partition, generation=generation, worker_id=worker_id)


async def test_a_newer_generation_fences_older_writers(store):
    """docs/lifecycle.md §9.7: once a newer attempt acquired a partition, an older
    one can change nothing; another worker of the same generation can't
    either, while the same worker acquiring again is its own retry."""

    out = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 5))
    await store.acquire(fenced(out, 7))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "0"}], first.ref, fenced(out, 5))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.acquire(fenced(out, 7, "a duplicate"))
    await store.acquire(fenced(out, 7))
    written = await store.store([{"id": "a", "v": "2"}], first.ref, fenced(out, 7))
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_a_repair_writing_no_rows_still_marks_the_slice_written(store):
    """docs/versions.md §5, break 1: the store is given an empty write only by
    a repair, which must still run its transaction — the partition then reads as
    written by the repairing generation, not by the dead attempt it repaired,
    which no commit has."""

    out = output(key="id", incremental=True)
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 12))
    await store.acquire(fenced(out, 15), first.ref)
    nothing = KeyedWrite(prepare_for(store, Patch([]), out), None, frozenset())
    repaired = await store.store(nothing, first.ref, fenced(out, 15))
    async with store.reads() as reader:
        rows, generation = await reader.load(repaired.ref, list[dict], None)
    assert (rows, generation) == ([{"id": "a", "v": "1"}], 15)


async def test_a_takeover_waits_for_an_older_writers_transaction(store):
    """The older writer's transaction is open, holding the partition: the newer
    acquisition waits for it to end, and from then on the older writer's
    next transaction is refused."""

    import asyncio
    import threading

    out = output(key="id")
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

    out = output(key="id")
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
    out = output(name, key="id")
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
    """A key the worker asks the store to write must be in the write: a
    missing one is an error, never a silent skip."""

    out = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}], None, context(out))
    with pytest.raises(StoreError, match="does not hold"):
        patch = prepare_for(store, Patch([{"id": "a", "v": "2"}]), out)
        await store.store(KeyedWrite(patch, frozenset(["a", "zzz"])), first.ref, context(out))


async def test_reconciliation_streams_the_slice_s_keys(store, monkeypatch):
    """docs/versions.md §5: after a dead `Sql` writer, a patch reconciles the
    whole partition — the keys it holds read back through `keys` a chunk at a
    time, never a value — at the patch's generation, with its own keys and
    removes laid over them; a live key the store lacks is removed. A by-key
    patch reconciles like any: a key it gives no rows is removed."""

    from obstore.store import MemoryStore
    from solera.keys.index import IndexState, KeyIndex
    from solera.keys.io import ObjectIO
    from solera_worker import worker

    from . import keys_reference as _python

    out = output(key="id", partition_column="site")
    rows = [{"id": "a", "x": 1}, {"id": "c", "x": 3}, {"id": "e", "x": 5}]
    written = await store.store(rows, None, context(out, partition="oakland"))
    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/")
    files, _ = await KeyIndex(io, None, state).replace(
        prepare_for(store, rows + [{"id": "f", "x": 6}], out).rows, 0, "w1", generation=1
    )
    state = state.committed(0, files, keep_log=True)

    async def no_load(*args, **kwargs):
        raise AssertionError("reconciliation reads keys, never values")

    monkeypatch.setattr(store, "load", no_load)
    monkeypatch.setattr("solera_postgres.KEY_CHUNK", 1)  # every key in a chunk of its own
    # A row may carry the stamped column, or not.
    for attempt, patch in enumerate(
        (
            Patch([{"id": "b", "x": 2, "site": "oakland"}], remove=["c"]),
            Patch({"b": [{"x": 2}], "d": []}, remove=["c"]),
        )
    ):
        o = worker._Out(
            "t",
            out,
            store,
            {"commit_number": 1, "repairs": [{"unknown": True}], "before": written.ref.to_json()},
            patch,
        )
        o.index = KeyIndex(io, None, state)
        o.prepared = prepare_for(store, patch, out)
        o.run = SortedEntries.from_rows(o.prepared.rows, [k.encode() for k in o.prepared.removes])
        g = attempt + 2
        delta, _ = await worker._reconcile(o, {"attempt": f"w{g}", "generation": g})
        got = [
            e[:3]
            for f in delta.files
            for e in _python.iter_file(await io.read_whole(state.path(f.name), f.size))
        ]
        # What the store holds is written again at g; `f`, which it lacks, removed.
        assert got == [(b"a", g, 0), (b"b", g, 0), (b"c", g, 1), (b"e", g, 0), (b"f", g, 1)]


def in_thread(coroutine) -> tuple:
    """Run `coroutine` on a thread of its own; returns (thread, done, errors)."""

    import asyncio
    import threading

    done, errors = threading.Event(), []

    def run():
        try:
            asyncio.run(coroutine)
        except Exception as error:
            errors.append(error)
        done.set()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, done, errors


async def test_a_migration_runs_between_acquisitions_never_under_one(store):
    """Review P1-1: attempt 5 is inside its migration when attempt 9
    acquires. The acquisition waits for the migration to commit, so 9's
    repair reads never precede 5's last change; then 5 is refused."""

    import threading

    from solera.sdk import Migration

    out = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 5))
    table, _, _ = store._table(out)
    inside, go = threading.Event(), threading.Event()

    def backfill(cur):
        inside.set()
        assert go.wait(10)
        cur.execute(f"UPDATE {table} SET v = 'migrated'")

    migrating, migrated, errors = in_thread(
        store.migrate(out, [Migration("backfill", backfill)], fenced(out, 5))
    )
    assert inside.wait(10)
    acquiring, acquired, _ = in_thread(store.acquire(fenced(out, 9)))
    assert not acquired.wait(0.5)  # waits behind the migration
    go.set()
    assert migrated.wait(10) and acquired.wait(10) and not errors
    migrating.join()
    acquiring.join()
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "migrated"}]
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store([{"id": "a", "v": "late"}], first.ref, fenced(out, 5))


async def test_an_older_attempts_migration_is_refused(store):
    """Once attempt 9 holds the partition, attempt 5's migration changes nothing
    and records nothing."""

    from solera.sdk import Migration

    out = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 5))
    table, _, _ = store._table(out)
    await store.acquire(fenced(out, 9))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.migrate(out, [Migration("stale", f"UPDATE {table} SET v = 'stale'")], fenced(out, 5))
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "1"}]
    assert await store.migrate(
        out, [Migration("stale", f"UPDATE {table} SET v = 'new'")], fenced(out, 9)
    ) == ["stale"]


async def test_a_migration_waits_for_every_slices_open_writer(store):
    """A migration changes every partition's rows: it waits for an open
    write transaction of another partition's partition, not just its own."""

    from solera.sdk import Migration

    out = output(key="id", partition_column="site")
    first = await store.store(
        [{"id": "a", "v": "1"}], None, context(out, partition="p1", generation=4, worker_id="i")
    )
    await store.store(
        [{"id": "b", "v": "1"}], None, context(out, partition="p2", generation=3, worker_id="i")
    )
    table, _, _ = store._table(out)
    conn = store._connect()
    cur = conn.cursor()
    store._domain(cur, table)  # p2's writer, open: as every write transaction begins
    store._fence(cur, table, context(out, partition="p2", generation=3, worker_id="i"))
    p1 = context(out, partition="p1", generation=4, worker_id="i")
    migrating, migrated, errors = in_thread(
        store.migrate(out, [Migration("all", f"UPDATE {table} SET v = 'm'")], p1)
    )
    assert not migrated.wait(0.5)
    cur.execute(f"UPDATE {table} SET v = '2' WHERE id = 'b'")
    conn.commit()
    assert migrated.wait(10) and not errors
    migrating.join()
    conn.close()
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "m", "site": "p1"}]


async def test_a_sql_write_is_a_query_never_a_statement(store):
    """A `Sql` write is embedded in the store's own statement, prepared, so
    UPDATE, DELETE, DDL, a data-modifying CTE and a second statement are all
    refused before anything changes; another partition's rows, and the
    table itself, stay as they were. A query of any shape — a CTE, VALUES,
    a trailing comment — is materialized."""

    name = f"t_{uuid.uuid4().hex[:12]}"
    out = output(name, key="id", partition_column="part")
    other = await store.store([{"id": "b", "v": "9"}], None, fenced(out, 9, partition="b"))
    first = await store.store([{"id": "a", "v": "1"}], None, fenced(out, 4, partition="a"))
    table, _, _ = store._table(out)
    with store._connect() as conn, conn.cursor() as cur:
        relid = store._relid(cur, table)
    refused = [
        f"UPDATE {table} SET v = 'stale'",
        f"DELETE FROM {table}",
        f"DROP TABLE {table}",
        f"WITH gone AS (DELETE FROM {table} RETURNING *) SELECT id, v FROM gone",
        f"SELECT 'a' AS id, '2' AS v) q; UPDATE {table} SET v = 'stale'; SELECT * FROM (SELECT 1",
        f"SELECT 'a' AS id, '2' AS v; UPDATE {table} SET v = 'stale'",
    ]
    for stmt in refused:
        with pytest.raises(WriteError, match="one query"):
            await store.store(Sql(stmt), first.ref, fenced(out, 5, partition="a"))
    with store._connect() as conn, conn.cursor() as cur:
        assert store._relid(cur, table) == relid
    assert await store.load(other.ref, list[dict], None) == [{"id": "b", "v": "9", "part": "b"}]
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "1", "part": "a"}]
    for query in [
        "WITH x AS (SELECT 'a'::text AS id, '2'::text AS v) SELECT * FROM x",
        "VALUES ('a'::text, '2'::text)",
        "SELECT 'a'::text AS id, '2'::text AS v -- the newest",
    ]:
        if query.startswith("VALUES"):
            query = f"SELECT column1 AS id, column2 AS v FROM ({query}) t"
        written = await store.store(Sql(query), first.ref, fenced(out, 5, partition="a"))
        assert await store.load(written.ref, list[dict], None) == [{"id": "a", "v": "2", "part": "a"}]


async def test_a_read_only_sql_store_refuses_a_query_whose_function_writes():
    """A function the query calls is the one way left for it to write;
    `sql_read_only` reads the query in a READ ONLY transaction of its own,
    which no function can turn back (not even through `SET ROLE`), and
    streams its rows into the partition."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    store = PostgresStore(DSN, sql_read_only=True)
    out = output(key="id", partition_column="part")
    victim = output(key="id")
    first = await store.store([{"id": "a", "v": "1"}], None, context(victim))
    table = first.ref.table
    fn = f"writes_{uuid.uuid4().hex[:8]}"
    with store._connect() as conn:
        conn.execute(
            f"CREATE FUNCTION {fn}() RETURNS text LANGUAGE plpgsql AS "
            f"$$ BEGIN UPDATE {table} SET v = 'stale'; RETURN 'x'; END $$"
        )
    with pytest.raises(WriteError, match="must not write"):
        await store.store(Sql(f"SELECT 'k'::text AS id, {fn}() AS v"), None, context(out))
    assert await store.load(first.ref, list[dict], None) == [{"id": "a", "v": "1"}]
    written = await store.store(Sql(f"SELECT id, v FROM {table}"), None, context(out, partition="p"))
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "v": "1", "part": "p"}]


async def test_by_key_patch_stamps_keys_and_removes_keys_given_no_rows(store):
    """docs/per-key-processing.md §6: `Patch({key: rows})` stamps the key
    column; a key given no rows does not exist — it is removed; a keyed load
    hands each selected key that has rows its group."""

    import pandas as pd

    out = output(key="path")
    first = await store.store(
        Patch({"a.csv": pd.DataFrame({"n": [1, 2]}), "b.csv": pd.DataFrame({"n": [3]})}), None, context(out)
    )
    patch = Patch({"b.csv": pd.DataFrame({"n": []}), "c.csv": [{"n": 4}]}, remove=["a.csv"])
    second = await store.store(
        KeyedWrite(prepare_for(store, patch, out), {"c.csv": b""}, frozenset({"a.csv", "b.csv"})),
        first.ref,
        context(out),
    )
    rows = await store.load(second.ref, list[dict], None)
    assert sorted((r["path"], r["n"]) for r in rows) == [("c.csv", 4)]
    selection = Keys({k: (b"", 0) for k in ("b.csv", "c.csv")})
    groups = await store.load(second.ref, dict[str, pd.DataFrame], selection)
    assert list(groups) == ["c.csv"]
    assert groups["c.csv"]["n"].tolist() == [4]
    assert patch_removes(store, patch, out) == ("a.csv", "b.csv")
    with pytest.raises(WriteError, match="carries"):
        await store.store(Patch({"d.csv": [{"path": "other", "n": 1}]}), second.ref, context(out))
    assert store.can_load(dict[str, pd.DataFrame], Keys) and not store.can_load(dict[str, pd.DataFrame], None)


async def test_a_partitions_first_write_waits_for_a_migration(store):
    """Astra review 2, P1-1: a migration has copied the table and is about
    to swap it in when a partition no attempt has written yet acquires and
    writes. The write waits for the migration, then lands in the new table:
    the swap cannot drop it."""

    import threading

    from solera.sdk import Migration

    name = f"t_{uuid.uuid4().hex[:12]}"
    out = output(name, key="id", partition_column="site")
    await store.store(
        [{"id": "a", "v": "1"}], None, context(out, partition="p1", generation=4, worker_id="i")
    )
    copied, go = threading.Event(), threading.Event()

    def swap(cur):
        cur.execute(f'CREATE TABLE public."{name}_new" AS SELECT * FROM public."{name}"')
        copied.set()
        assert go.wait(10)
        cur.execute(f'DROP TABLE public."{name}"; ALTER TABLE public."{name}_new" RENAME TO "{name}"')

    p1 = context(out, partition="p1", generation=4, worker_id="i")
    migrating, migrated, errors = in_thread(store.migrate(out, [Migration("swap", swap)], p1))
    assert copied.wait(10)
    p2 = context(out, partition="p2", generation=5, worker_id="i")

    async def first_write():
        await store.acquire(p2)
        return await store.store([{"id": "b", "v": "1"}], None, p2)

    writing, written, write_errors = in_thread(first_write())
    assert not written.wait(0.5)  # waits for the migration
    go.set()
    assert migrated.wait(10) and written.wait(10) and not errors and not write_errors
    migrating.join()
    writing.join()
    table, _, _ = store._table(out)
    with store._connect() as conn, conn.cursor() as cur:
        rows = cur.execute(f"SELECT id, site FROM {table} ORDER BY id").fetchall()
    assert [(r["id"], r["site"]) for r in rows] == [("a", "p1"), ("b", "p2")]


async def test_a_missing_grant_role_is_skipped_without_aborting_the_write(store):
    """§4: grants are deployment sugar. A role this database lacks is skipped
    before its GRANT could abort the write's transaction; one it has is granted."""

    import psycopg

    role = f"r_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f'CREATE ROLE "{role}"')
    try:
        from solera_postgres import PostgresStore

        granting = PostgresStore(DSN, grants=[f"missing_{role}", role])
        out = output(key="id")
        written = await granting.store([{"id": "a", "n": 1}], None, context(out))
        assert await granting.load(written.ref, list[dict], None) == [{"id": "a", "n": 1}]
        with psycopg.connect(DSN) as conn:
            allowed = conn.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')", (role, out.name)
            ).fetchone()
        assert allowed == (True,)
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(f'DROP OWNED BY "{role}"')
            conn.execute(f'DROP ROLE "{role}"')


async def test_a_replacement_writes_only_the_keys_it_is_asked_to(store):
    """§4: with a selection, a replacement changes only the selected keys —
    content and key events never disagree; with none, it is the whole partition."""

    out = output(key="id")
    first = await store.store(
        [{"id": "a", "r": "1", "v": 10}, {"id": "b", "r": "1", "v": 20}, {"id": "c", "r": "1", "v": 30}],
        None,
        context(out),
    )
    later = [{"id": "a", "r": "1", "v": 999}, {"id": "b", "r": "2", "v": 21}]
    selected = KeyedWrite(prepare_for(store, later, out), frozenset({"b"}), frozenset({"c"}))
    written = await store.store(selected, first.ref, context(out))
    got = sorted((r["id"], r["v"]) for r in await store.load(written.ref, list[dict], None))
    assert got == [("a", 10), ("b", 21)]  # `a` was not selected: its row stays
    whole = await store.store(later, written.ref, context(out))
    assert sorted((r["id"], r["v"]) for r in await store.load(whole.ref, list[dict], None)) == [
        ("a", 999),
        ("b", 21),
    ]


async def test_a_sql_table_keeps_the_select_s_column_types(store):
    """§4: a table a Sql SELECT creates takes the SELECT's column types, so
    its rows read back as the values they were: 42, not "42". The
    declaration is not changed by it."""

    out = output(key="id")
    written = await store.store(
        Sql("SELECT 'a'::text AS id, 42::bigint AS n, 1.5::numeric AS x, 'w' AS w"), None, context(out)
    )
    assert [key for chunk in written.keys for key in chunk] == ["a"]
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "n": 42, "x": D("1.5"), "w": "w"}]
    assert "columns" not in out.config


async def test_rows_first_written_keep_their_types(store):
    """A table a Python write creates takes column types that read its
    values back as they were: decimals, timestamps, dates, bytes."""

    import datetime as dt

    out = output(key="id")
    row = {
        "id": "a",
        "d": D("1.25"),
        "at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        "on": dt.date(2026, 1, 2),
        "b": b"\x00\x01",
    }
    written = await store.store([row], None, context(out))
    [back] = await store.load(written.ref, list[dict], None)
    assert {k: back[k] for k in row} == {**row, "b": back["b"]} and bytes(back["b"]) == row["b"]


def patch_removes(store, patch, out):
    return prepare_for(store, patch, out).removes


async def test_a_column_s_type_is_every_value_s_not_the_first(store):
    """§4: a table the write creates types each column by every value not
    null in it, so a null then 42 is a bigint holding 42. Two kinds in one
    column, or only nulls, want a declaration. What a declared column does
    with a value is the database's: 42 in a text column is "42"."""

    out = output(key="id")
    rows = [{"id": "a", "n": None}, {"id": "b", "n": 42}]
    written = await store.store(rows, None, context(out))
    back = sorted(await store.load(written.ref, list[dict], None), key=lambda r: r["id"])
    assert back == rows
    with pytest.raises(WriteError, match="declare its type"):
        await store.store([{"id": "a", "n": 1}, {"id": "b", "n": "x"}], None, context(output(key="id")))
    with pytest.raises(WriteError, match="only nulls"):
        await store.store([{"id": "a", "n": None}], None, context(output(key="id")))
    declared = output(key="id", columns={"n": "text"})
    written = await store.store([{"id": "a", "n": 42}], None, context(declared))
    assert await store.load(written.ref, list[dict], None) == [{"id": "a", "n": "42"}]


async def test_a_write_waits_on_a_thread_not_on_the_event_loop(store):
    """A store call's transaction runs on a thread of its own: while the
    database works, the worker's event loop goes on (cancellation, logs,
    async producers)."""

    import asyncio
    import time

    out = output(key="id")
    late = []

    async def tick():
        start = time.perf_counter()
        await asyncio.sleep(0.02)
        late.append(time.perf_counter() - start)

    ticking = asyncio.create_task(tick())
    await asyncio.sleep(0)  # the tick is asleep before the write starts
    await store.store(Sql("SELECT 'a'::text AS id, pg_sleep(0.4)::text AS slept"), None, context(out))
    await ticking
    assert late[0] < 0.2, late


async def test_a_repair_read_back_waits_off_the_event_loop(store, monkeypatch):
    """After a failed Sql write, the store's keys are read back (`keys`) a
    chunk at a time on a thread: a slow read leaves the event loop free."""

    import asyncio
    import time

    from obstore.store import MemoryStore
    from solera.keys.index import IndexState, KeyIndex
    from solera.keys.io import ObjectIO
    from solera_worker import worker

    out = output(key="id")
    rows = [{"id": "a", "x": 1}]
    written = await store.store(rows, None, context(out))
    io = ObjectIO(MemoryStore())
    state = IndexState(prefix="keys/")
    files, _ = await KeyIndex(io, None, state).replace(prepare_for(store, rows, out).rows, 0, "w1")
    state = state.committed(0, files, keep_log=True)
    keys = store.keys

    def slow(ref, among=None):
        time.sleep(0.4)  # a database that takes its time
        yield from keys(ref, among)

    monkeypatch.setattr(store, "keys", slow)
    patch = Patch([{"id": "b", "x": 2}])
    o = worker._Out(
        "t",
        out,
        store,
        {"commit_number": 1, "repairs": [{"unknown": True}], "before": written.ref.to_json()},
        patch,
    )
    o.index = KeyIndex(io, None, state)
    o.prepared = prepare_for(store, patch, out)
    o.run = SortedEntries.from_rows(o.prepared.rows)
    late = []

    async def tick():
        start = time.perf_counter()
        await asyncio.sleep(0.02)
        late.append(time.perf_counter() - start)

    ticking = asyncio.create_task(tick())
    await asyncio.sleep(0)
    await worker._reconcile(o, {"attempt": "w2", "generation": 2})
    await ticking
    assert late[0] < 0.2, late


async def test_a_dataframe_s_table_is_typed_by_its_dtypes_and_logged(store, caplog):
    """A table a DataFrame creates takes its columns' types from its schema
    (pyarrow's, when installed, else pandas' dtypes): a float column of NaN
    alone is still a float column. The inferred columns are logged once,
    with the hint to declare them."""

    import logging
    import math

    import pandas as pd

    out = output(key="id")
    frame = pd.DataFrame({"id": ["a", "b"], "x": [math.nan, math.nan], "n": [1, 2]})
    with caplog.at_level(logging.WARNING, logger="solera.postgres"):
        written = await store.store(frame, None, context(out))
    [warning] = [r for r in caplog.records if r.name == "solera.postgres"]
    assert "inferred columns" in warning.getMessage() and "columns=" in warning.getMessage()
    later = pd.DataFrame({"id": ["c"], "x": [1.5], "n": [3]})
    written = await store.store(later, written.ref, context(out))
    assert await store.load(written.ref, list[dict], None) == [{"id": "c", "x": 1.5, "n": 3}]


async def test_a_key_is_stored_as_itself(store):
    """docs/versions.md §4: a column may change a value — round a decimal,
    narrow a real, cut a timestamp's precision — and the store takes it as
    the database does. But a key its column changes (`"01"` in a bigint
    column, stored as `1`) is refused: the index would name a key no read,
    delete or repair finds. A key the column keeps as itself is taken."""

    import datetime as dt

    out = output(key="id", columns={"d": "numeric(6,2)", "r": "real", "at": "timestamp(3)"})
    lossy = [
        {"id": "a", "d": D("1.234"), "r": 1.234567890123, "at": dt.datetime(2026, 1, 1, 0, 0, 0, 123400)}
    ]
    written = await store.store(lossy, None, context(out))
    [back] = await store.load(written.ref, list[dict], None)
    assert back["d"] == D("1.23")  # the database's to round
    numbered = output(key="id", columns={"id": "bigint", "v": "text"})
    written = await store.store([{"id": 1, "v": "x"}, {"id": "2", "v": "y"}], None, context(numbered))
    assert sorted(r["id"] for r in await store.load(written.ref, list[dict], None)) == [1, 2]
    for bad in ("01", " 3", "+4"):
        with pytest.raises(WriteError, match="stored as another key"):
            await store.store([{"id": bad, "v": "x"}], None, context(numbered))
    with pytest.raises(WriteError, match="stored as another key"):  # in a patch too
        await store.store(Patch([{"id": "05", "v": "x"}]), written.ref, context(numbered))
    assert sorted(r["id"] for r in await store.load(written.ref, list[dict], None)) == [1, 2]


async def test_an_integer_key_is_read_through_an_index(store):
    """Review round 5 #6: reads and patches name keys as text; an integer
    key's table gets an index on its key as text, so reading one key is an
    index scan, not a scan of the table."""

    out = output(key="id", columns={"id": "bigint", "v": "text"})
    written = await store.store([{"id": i, "v": "x"} for i in range(5000)], None, context(out))
    with store._connect() as conn:
        conn.execute(f"ANALYZE {written.ref.table}")
        plan = "\n".join(
            r["QUERY PLAN"]
            for r in conn.execute(
                f"EXPLAIN SELECT * FROM {written.ref.table} WHERE id::text = ANY(%s)", (["42"],)
            )
        )
    assert "Index" in plan and "Seq Scan" not in plan, plan
    assert await store.load(written.ref, list[dict], Keys({"42": 0})) == [{"id": 42, "v": "x"}]


async def test_a_migration_it_cannot_run_is_refused_when_it_runs(store):
    """A migration's payload is the store's business: PostgresStore runs SQL
    text or a callable taking a cursor, and refuses anything else as it
    runs — registration does not type it."""

    from solera.sdk import Migration

    out = output(key="id", migrations=[Migration("odd", 42)])
    with pytest.raises(StoreError, match="SQL string or a callable"):
        await store.migrate(out, out.migrations)


async def test_a_migration_applies_to_each_schemas_table_of_one_name(store):
    """F18: two projects (or a staging and a production namespace) on one
    database each write an output `orders`, in schemas of their own. A
    migration applied to the first table is applied to the second too: the
    ledger knows which table it changed."""

    from solera.sdk import Migration

    name, tables = f"t_{uuid.uuid4().hex[:12]}", []
    for side in ("a", "b"):
        schema = f"{name}_{side}"
        with store._connect() as conn, conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
        out = Output(name, key="id", store="postgres", schema=schema, columns={"id": "text", "v": "text"})
        written = await store.store([{"id": "x", "v": "1"}], None, context(out))
        out.migrations = (Migration("note", f'ALTER TABLE "{schema}"."{name}" ADD COLUMN note text'),)
        assert await store.migrate(out, out.migrations, prior=written.ref) == ["note"]
        tables.append(schema)
    with store._connect() as conn, conn.cursor() as cur:
        for schema in tables:
            columns = cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (schema, name),
            ).fetchall()
            assert "note" in {r["column_name"] for r in columns}, schema
