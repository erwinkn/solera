"""PostgresStore's exact tables (D196) and full runs built beside the published
rows (D211), against a real PostgreSQL: the version check that locks out late
and dead writers, repair by version, the build and its switch, and every probe
the reviews of the design raised (handoffs/SOLE-17/table-swap.md, "B8's
PostgreSQL tests", in the Captain's drive). Skips unless
SOLERA_TEST_DATABASE_URL points at a scratch database."""

import asyncio
import os
import random
import re
import tempfile
import threading
import time
import uuid

import pytest
from solera.sdk import Migration, Output
from solera.stores import Patch, PublicationRefused, StoreError, WriteContext, WriteError

pytestmark = pytest.mark.postgres

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")


@pytest.fixture
def store():
    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    return PostgresStore(DSN)


def output(name=None, **config) -> Output:
    config.setdefault("key", "id")
    return Output(name or f"t_{uuid.uuid4().hex[:12]}", store="postgres", **config)


def at(out, generation, *, reset=False, run=None, owed=None, partition="", worker="w") -> WriteContext:
    """The new core's write context: the attempt's version, and the full run
    whose build the write targets (`build_run`)."""

    return WriteContext(
        output=out,
        partition=partition,
        attempt=f"a{generation}",
        generation=generation,
        worker_id=worker,
        reset=reset,
        build_run=run,
        owed_run=owed,
        builds=True,
    )


def rows(**values) -> list[dict]:
    return [{"id": k, "v": v} for k, v in values.items()]


def sql(statement, params=()):
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as conn:
        found = conn.execute(statement, params)
        return found.fetchall() if found.description else None


def outside(store, out, partition=None) -> dict:
    """What a program reading the table outside Solera sees: the user's table."""

    table, _, _ = store._table(out)
    column = out.config.get("partition_column")
    where, params = (f" WHERE {column} = %s", (partition,)) if partition is not None else ("", ())
    return {r[0]: r[1] for r in sql(f"SELECT id, v FROM {table}{where}", params)}


async def solera(store, ref) -> dict:
    """What Solera reads through a commit's ref."""

    return {r["id"]: r["v"] for r in await store.load(ref, list[dict], None)}


def builds(store, out, partition="") -> list[int]:
    """The runs whose build tables exist for the partition."""

    from solera_postgres import BUILD_SCHEMA, _build_name

    table, _, _ = store._table(out)
    prefix = _build_name(table, partition, 0)[:-1]
    found = sql(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind = 'r' AND starts_with(c.relname, %s)",
        (BUILD_SCHEMA, prefix),
    )
    return sorted(int(name[len(prefix) :]) for (name,) in found)


def registry(store, out, partition="") -> tuple:
    """The partition's version row: (generation, published, open builds)."""

    from solera_postgres import VERSIONS_TABLE

    table, _, _ = store._table(out)
    found = sql(
        f"SELECT generation, published, builds FROM {VERSIONS_TABLE} WHERE rel = %s AND part = %s",
        (table, partition),
    )
    return tuple(found[0]) if found else None


def in_thread(coroutine) -> tuple:
    done, errors, results = threading.Event(), [], []

    def run():
        try:
            results.append(asyncio.run(coroutine))
        except Exception as error:
            errors.append(error)
        done.set()

    thread = threading.Thread(target=run)
    thread.start()
    return thread, done, errors, results


def open_switch(store, out, partition):
    """A switch of the partition in progress, held open: the write domain, the
    partition's version row, and its rows deleted (ROW EXCLUSIVE on the
    user's table), as full_run_ended holds them for minutes at 100M rows."""

    from solera_postgres import VERSIONS_TABLE

    table, _, _ = store._table(out)
    conn = store._connect()
    cur = conn.cursor()
    store._domain(cur, table)
    cur.execute(f"SELECT 1 FROM {VERSIONS_TABLE} WHERE rel = %s AND part = %s FOR UPDATE", (table, partition))
    column = out.config.get("partition_column")
    cur.execute(
        f"DELETE FROM {table} WHERE {column} = %s" if column else f"DELETE FROM {table}",
        (partition,) if column else (),
    )
    return conn


# -- late and dead writers (D196; review F1) -----------------------------------------------


async def test_a_late_insert_of_a_removed_key_is_refused(store):
    """F1's first reproduction. The table holds k2 at 2. Attempt 6 plans k2 and
    stalls; the engine gives up on it. Its retry 7 removes k2 and commits.
    Attempt 6's write lands late: there is no row to compare with, but the
    partition's version check is 7, so the write is refused whole."""

    out = output(incremental=True)
    first = await store.store(rows(k1="1", k2="2"), None, at(out, 2))
    retry = await store.store(Patch([], remove=["k2"]), first.ref, at(out, 7))
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store(Patch(rows(k2="late")), first.ref, at(out, 6))
    assert outside(store, out) == {"k1": "1"}
    assert await solera(store, retry.ref) == {"k1": "1"}


async def test_a_late_insert_of_a_key_never_held_is_refused(store):
    """F1's second reproduction. feed adds k6; attempt 6 plans it and stalls;
    feed removes k6 again, so the retry 7 has nothing to write for it. The
    first attempt after one that ended without committing writes all the
    same, even nothing (D196, rule 3): attempt 6's insert of k6 is refused."""

    out = output(incremental=True)
    first = await store.store(rows(k1="1"), None, at(out, 2))
    await store.store(Patch([]), first.ref, at(out, 7))  # the retry's write of nothing
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store(Patch(rows(k6="late")), first.ref, at(out, 6))
    assert outside(store, out) == {"k1": "1"}


async def test_a_first_write_of_nothing_takes_the_check_before_any_table_exists(store):
    """The check is keyed by the table's name, so the first attempt after a
    dead one locks it out even when no table exists yet, and creates none."""

    out = output(incremental=True)
    await store.store(Patch([]), None, at(out, 7))
    assert registry(store, out)[0] == 7
    assert sql("SELECT to_regclass(%s)", (store._table(out)[0],)) == [(None,)]
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store(rows(k6="late"), None, at(out, 6))


async def test_a_repair_lists_a_dead_attempts_keys_by_version(store):
    """Repair by version (D196, rule 2): attempt 5 wrote b and c, then attempt
    6 rewrote c, and both died without a commit. Attempt 9 asks which keys
    the partition holds at version 5: b alone. Asking takes the version
    check first (rule 3), so attempt 5 can add nothing after the list."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(Patch(rows(b="1", c="1")), first.ref, at(out, 5))
    await store.store(Patch(rows(c="2")), first.ref, at(out, 6))
    listed = [k for chunk in await store.written_at([5], first.ref, at(out, 9)) for k in chunk]
    assert listed == ["b"]
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store(Patch(rows(d="late")), first.ref, at(out, 5))
    assert sorted(k for chunk in await store.written_at([5, 6], first.ref, at(out, 9)) for k in chunk) == [
        "b",
        "c",
    ]


async def test_a_repair_lists_where_the_log_points(store):
    """During a full run the dead attempt wrote into the build: the repair
    lists the build's rows, not the published ones."""

    out = output(incremental=True)
    first = await store.store(rows(a="1", b="1"), None, at(out, 1))
    run = await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    await store.store(Patch(rows(b="dead")), run.ref, at(out, 6, run=5))
    listed = await store.written_at([6], run.ref, at(out, 7, run=5))
    assert [k for chunk in listed for k in chunk] == ["b"]


async def test_an_unkeyed_output_lists_no_keys(store):
    out = output(key=None)
    first = await store.store([{"v": "1"}], None, at(out, 1))
    assert [k for chunk in await store.written_at([1], first.ref, at(out, 2)) for k in chunk] == []


# -- the build and the switch (D211) -----------------------------------------------------


async def test_a_full_run_builds_beside_and_switches_once(store):
    """The table holds a, b, c, d at version 1. A full run (clear by attempt 5)
    writes a and b, then attempt 6 writes d and e. Outside Solera the table
    shows the old rows throughout; Solera reads the build its commits name.
    full_run_ended publishes the run in one step: outsiders see the new
    rows, the build is gone, and a ref to the build reads the published rows."""

    out = output(incremental=True)
    first = await store.store(rows(a="1", b="1", c="1", d="1"), None, at(out, 1))
    one = await store.store(rows(a="3", b="3"), first.ref, at(out, 5, reset=True))
    assert one.ref.handle["build"] == 5 and one.ref.handle["table"] == first.ref.handle["table"]
    assert outside(store, out) == {"a": "1", "b": "1", "c": "1", "d": "1"}
    assert await solera(store, one.ref) == {"a": "3", "b": "3"}
    two = await store.store(Patch(rows(d="4", e="4")), one.ref, at(out, 6, run=5))
    assert outside(store, out) == {"a": "1", "b": "1", "c": "1", "d": "1"}
    assert await solera(store, two.ref) == {"a": "3", "b": "3", "d": "4", "e": "4"}
    assert registry(store, out) == (6, None, [5]) and builds(store, out) == [5]

    assert await store.full_run_ended(out, "", 5, 6) is True
    assert outside(store, out) == {"a": "3", "b": "3", "d": "4", "e": "4"}
    assert registry(store, out) == (6, 5, []) and builds(store, out) == []
    assert await solera(store, two.ref) == {"a": "3", "b": "3", "d": "4", "e": "4"}
    versions = sql(f"SELECT id, _version FROM {store._table(out)[0]} ORDER BY id")
    assert versions == [("a", 5), ("b", 5), ("d", 6), ("e", 6)]

    # A repeat says published and changes nothing; a later write goes in place.
    assert await store.full_run_ended(out, "", 5, 6) is True
    three = await store.store(Patch(rows(f="7")), two.ref, at(out, 7))
    assert "build" not in three.ref.handle and outside(store, out)["f"] == "7"


async def test_a_late_switch_call_does_nothing_and_says_not_published(store):
    """Rule 15 (switch_newest): the engine called the switch when attempt 6
    was its newest, but the call reached the store after attempt 7 wrote.
    It changes nothing and says not published; the engine keeps the debt and
    calls again once the partition is quiet."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    run = await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    await store.store(Patch(rows(b="7")), run.ref, at(out, 7, run=5))  # not committed yet
    assert await store.full_run_ended(out, "", 5, 6) is False
    assert outside(store, out) == {"a": "1"} and registry(store, out) == (7, None, [5])
    assert await store.full_run_ended(out, "", 5, 7) is True
    assert outside(store, out) == {"a": "2", "b": "7"}


async def test_a_switch_never_lowers_the_check(store):
    """After the switch, a late writer of the run is still refused, in the
    build and in place."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    run = await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    await store.store(Patch(rows(b="6")), run.ref, at(out, 6, run=5))
    assert await store.full_run_ended(out, "", 5, 6)
    for late in (at(out, 5, run=5), at(out, 5, reset=True), at(out, 5)):
        with pytest.raises(StoreError, match="newer attempt"):
            await store.store(Patch(rows(z="late")), run.ref, late)
    assert outside(store, out) == {"a": "2", "b": "6"}


async def test_a_reset_delivered_again_after_its_switch_rewrites_in_place(store):
    """Rule 12 (reset_replay): the clear's write reaches the store a second
    time after its run was published. It rewrites the same rows in place,
    and opens no build that a repeated switch could publish twice."""

    out = output(incremental=True)
    first = await store.store(rows(a="1", b="1"), None, at(out, 1))
    run = await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    assert await store.full_run_ended(out, "", 5, 5)
    again = await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    assert "build" not in again.ref.handle
    assert outside(store, out) == {"a": "2"} and builds(store, out) == []
    assert registry(store, out) == (5, 5, [])
    assert await solera(store, run.ref) == {"a": "2"}


async def test_a_reset_delivered_again_before_its_switch_rewrites_its_build(store):
    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(rows(a="2", b="2"), first.ref, at(out, 5, reset=True))
    again = await store.store(rows(a="2", b="2"), first.ref, at(out, 5, reset=True))
    assert await solera(store, again.ref) == {"a": "2", "b": "2"}
    assert builds(store, out) == [5] and outside(store, out) == {"a": "1"}


async def test_every_write_keeps_the_builds_the_log_names(store):
    """Rule 11 (build_keep). Run 5's clear committed: the log names run 5 in
    progress. A second definition change starts run 8, whose first write
    keeps run 5's build: until 8's clear is journaled, Solera still reads
    5's. The next write after the clear names run 8 alone (run 5 never
    ended) and drops 5's build."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    five = await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    eight = await store.store(rows(a="8", b="8"), five.ref, at(out, 8, reset=True, run=5))
    assert builds(store, out) == [5, 8]
    assert await solera(store, five.ref) == {"a": "5"}
    assert await solera(store, eight.ref) == {"a": "8", "b": "8"}
    nine = await store.store(Patch(rows(c="9")), eight.ref, at(out, 9, run=8))
    assert builds(store, out) == [8] and registry(store, out)[2] == [8]
    assert await solera(store, nine.ref) == {"a": "8", "b": "8", "c": "9"}
    assert await store.full_run_ended(out, "", 5, 9) is False  # overtaken, never published
    assert await store.full_run_ended(out, "", 8, 9) is True
    assert outside(store, out) == {"a": "8", "b": "8", "c": "9"}


async def test_a_run_owed_its_switch_keeps_its_build_while_the_next_run_builds(store):
    """Run 5 ended in the log and its switch is owed (switch_owed) when run 8
    starts: every write of run 8 names 5 as owed and keeps its build, Solera
    reads 8's, and the owed switch still publishes 5, then 8's publishes 8."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    five = await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    eight = await store.store(rows(a="8"), five.ref, at(out, 8, reset=True, owed=5))
    nine = await store.store(Patch(rows(b="9")), eight.ref, at(out, 9, run=8, owed=5))
    assert builds(store, out) == [5, 8] and registry(store, out)[2] == [5, 8]
    assert await solera(store, nine.ref) == {"a": "8", "b": "9"}
    assert await store.full_run_ended(out, "", 5, 9) is True
    assert outside(store, out) == {"a": "5"} and builds(store, out) == [8]
    assert await solera(store, nine.ref) == {"a": "8", "b": "9"}
    ten = await store.store(Patch(rows(c="10")), nine.ref, at(out, 10, run=8))
    assert await solera(store, ten.ref) == {"a": "8", "b": "9", "c": "10"}
    assert await store.full_run_ended(out, "", 8, 10) is True
    assert outside(store, out) == {"a": "8", "b": "9", "c": "10"} and registry(store, out) == (10, 8, [])


async def test_a_dead_first_writes_build_is_dropped_by_the_next_write(store):
    """A full run's first attempt (5) opened its build and died before its
    clear was journaled: no commit names the build, nobody reads it, and the
    next write (an ordinary one: the log names no run) drops it."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(rows(a="dead"), first.ref, at(out, 5, reset=True))
    after = await store.store(Patch(rows(b="6")), first.ref, at(out, 6))
    assert builds(store, out) == [] and registry(store, out)[2] == []
    assert outside(store, out) == {"a": "1", "b": "6"}
    assert await solera(store, after.ref) == {"a": "1", "b": "6"}
    with pytest.raises(StoreError, match="newer attempt"):
        await store.store(Patch(rows(c="late")), first.ref, at(out, 5, run=5))


async def test_a_switch_publishes_only_its_partition(store):
    """Two partitions of one table mid-run: each has its own build, and a
    switch of one leaves the other's build and published rows alone."""

    out = output(incremental=True, partition_column="site")
    p1 = await store.store(rows(a="1"), None, at(out, 1, partition="p1"))
    p2 = await store.store(rows(b="1"), None, at(out, 2, partition="p2"))
    r1 = await store.store(rows(a="5"), p1.ref, at(out, 5, reset=True, partition="p1"))
    r2 = await store.store(rows(b="6"), p2.ref, at(out, 6, reset=True, partition="p2"))
    assert await store.full_run_ended(out, "p1", 5, 5) is True
    assert outside(store, out, "p1") == {"a": "5"} and outside(store, out, "p2") == {"b": "1"}
    assert builds(store, out, "p1") == [] and builds(store, out, "p2") == [6]
    assert await solera(store, r2.ref) == {"b": "6"}
    assert await store.full_run_ended(out, "p2", 6, 6) is True
    assert outside(store, out) == {"a": "5", "b": "6"}
    assert await solera(store, r1.ref) == {"a": "5"}


async def test_an_output_with_no_partition_column_switches_every_row(store):
    out = output(incremental=True)
    first = await store.store(rows(a="1", b="1"), None, at(out, 1))
    await store.store(rows(c="5"), first.ref, at(out, 5, reset=True))
    assert await store.full_run_ended(out, "", 5, 5)
    assert outside(store, out) == {"c": "5"}


async def test_an_empty_full_run_publishes_an_empty_partition(store):
    """A run whose first write holds nothing still opens its build, so its
    switch removes every old row."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store([], first.ref, at(out, 5, reset=True))
    assert outside(store, out) == {"a": "1"}
    assert await store.full_run_ended(out, "", 5, 5)
    assert outside(store, out) == {}


async def test_a_first_run_builds_beside_an_empty_table(store):
    """An ordinary output's first run is a full run: its rows appear whole."""

    out = output(incremental=True)
    one = await store.store(rows(a="1"), None, at(out, 5, reset=True))
    assert outside(store, out) == {}
    await store.store(Patch(rows(b="1")), one.ref, at(out, 6, run=5))
    assert outside(store, out) == {}
    assert await store.full_run_ended(out, "", 5, 6)
    assert outside(store, out) == {"a": "1", "b": "1"}


async def test_unkeyed_sql_and_commit_writes_build_beside(store):
    """Every full run builds beside, whatever the output's shape: a value
    replaced whole, commits appended, a Sql query materialized."""

    from solera_postgres import Sql

    value = output(key=None)
    first = await store.store([{"v": "1"}], None, at(value, 1))
    await store.store([{"v": "2"}], first.ref, at(value, 5, reset=True))
    assert sql(f"SELECT v FROM {store._table(value)[0]}") == [("1",)]
    assert await store.full_run_ended(value, "", 5, 5)
    assert sql(f"SELECT v FROM {store._table(value)[0]}") == [("2",)]

    log = output(key=None, incremental=True)
    first = await store.store(
        Patch([{"v": "1"}]),
        None,
        WriteContext(log, "", commit_number=0, attempt="a1", generation=1, worker_id="w", builds=True),
    )
    clear = WriteContext(
        log, "", commit_number=1, attempt="a5", generation=5, worker_id="w", reset=True, builds=True
    )
    run = await store.store(Patch([{"v": "2"}]), first.ref, clear)
    assert await store.load(run.ref, list[dict], None) == [{"v": "2"}]
    assert sql(f"SELECT v FROM {store._table(log)[0]}") == [("1",)]
    assert await store.full_run_ended(log, "", 5, 5)
    assert sql(f"SELECT v, _commit FROM {store._table(log)[0]}") == [("2", 1)]

    keyed = output()
    first = await store.store(Sql("SELECT 'a'::text AS id, '1'::text AS v"), None, at(keyed, 1))
    run = await store.store(
        Sql("SELECT 'b'::text AS id, '2'::text AS v"), first.ref, at(keyed, 5, reset=True)
    )
    assert outside(store, keyed) == {"a": "1"} and await solera(store, run.ref) == {"b": "2"}
    assert await store.full_run_ended(keyed, "", 5, 5)
    assert outside(store, keyed) == {"b": "2"}


async def test_a_cleanup_takes_the_outputs_builds(store):
    out = output(incremental=True, partition_column="site")
    first = await store.store(rows(a="1"), None, at(out, 1, partition="p1"))
    await store.store(rows(a="5"), first.ref, at(out, 5, reset=True, partition="p1"))
    await store.store(rows(b="6"), None, at(out, 6, reset=True, partition="p2"))
    await store.cleanup(out, partition="p2")
    assert builds(store, out, "p2") == [] and builds(store, out, "p1") == [5]
    await store.cleanup(out)
    assert builds(store, out, "p1") == [] and registry(store, out, "p1") is None


# -- the build's columns and constraints (probes P5, R4, R5, R9) ----------------------------


def user_table(store, out, extra: str) -> str:
    table, _, _ = store._table(out)
    sql(f"CREATE TABLE {table} (id text PRIMARY KEY, v text, {extra}, _version bigint)")
    return table


async def test_identity_serial_and_generated_columns_publish_what_solera_read(store):
    """The build draws identity and serial values from the user's table's
    sequences (never sequences of its own, P5/R4), computes generated
    columns, and the switch copies every column the database does not
    generate (OVERRIDING SYSTEM VALUE): the rows published are the rows
    Solera loaded from the build."""

    out = output(incremental=True)
    table = user_table(
        store,
        out,
        "n bigint GENERATED ALWAYS AS IDENTITY, s serial, g text GENERATED ALWAYS AS (v || '!') STORED",
    )
    first = await store.store(rows(a="1"), None, at(out, 1))
    run = await store.store(rows(a="2", b="2"), first.ref, at(out, 5, reset=True))
    loaded = sorted((r["id"], r["n"], r["s"], r["g"]) for r in await store.load(run.ref, list[dict], None))
    assert loaded == [("a", 2, 2, "2!"), ("b", 3, 3, "2!")]
    assert await store.full_run_ended(out, "", 5, 5)
    assert sql(f"SELECT id, n, s, g FROM {table} ORDER BY id") == loaded
    fresh = await store.store(Patch(rows(c="3")), run.ref, at(out, 6))
    assert sql(f"SELECT n, s FROM {table} WHERE id = 'c'") == [(4, 4)]
    assert "build" not in fresh.ref.handle


async def test_a_check_constraint_fails_the_batch_that_breaks_it(store):
    out = output(incremental=True)
    user_table(store, out, "CHECK (v <> 'bad')")
    first = await store.store(rows(a="1"), None, at(out, 1))
    with pytest.raises(WriteError):
        await store.store(rows(a="bad"), first.ref, at(out, 5, reset=True))
    assert outside(store, out) == {"a": "1"} and builds(store, out) == []


async def test_partial_unique_and_exclusion_indexes_fail_the_batch(store):
    """R9: INCLUDING INDEXES brings partial and exclusion indexes to the
    build, so it refuses what the user's table would refuse, in the batch."""

    out = output(incremental=True)
    sql("CREATE EXTENSION IF NOT EXISTS btree_gist")
    table = user_table(store, out, "r int4range, EXCLUDE USING gist (r WITH &&)")
    sql(f"CREATE UNIQUE INDEX ON {table} (v) WHERE v <> 'x'")
    first = await store.store([{"id": "a", "v": "1", "r": "[1,5)"}], None, at(out, 1))
    with pytest.raises(WriteError):
        await store.store(
            [{"id": "a", "v": "1", "r": "[1,5)"}, {"id": "b", "v": "1", "r": "[7,9)"}],
            first.ref,
            at(out, 5, reset=True),
        )
    with pytest.raises(WriteError):
        await store.store(
            [{"id": "a", "v": "1", "r": "[1,5)"}, {"id": "b", "v": "2", "r": "[3,9)"}],
            first.ref,
            at(out, 6, reset=True),
        )
    run = await store.store(
        [{"id": "a", "v": "x", "r": "[1,5)"}, {"id": "b", "v": "x", "r": "[5,9)"}],
        first.ref,
        at(out, 7, reset=True),
    )
    assert await store.full_run_ended(out, "", 7, 7)
    assert await solera(store, run.ref) == {"a": "x", "b": "x"}


async def test_a_build_keeps_only_its_unique_and_key_indexes(store):
    """The build skips the output's non-unique indexes (they cost every
    insert and nothing reads them before the switch)."""

    from solera_postgres import BUILD_SCHEMA, _build_name

    out = output(incremental=True, indexes=[["v"]])
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(rows(a="2"), first.ref, at(out, 5, reset=True))
    build = _build_name(store._table(out)[0], "", 5)
    found = sql(
        "SELECT pg_get_indexdef(x.indexrelid) FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s",
        (BUILD_SCHEMA, build),
    )
    assert found and all("(v)" not in d for (d,) in found)


async def test_a_unique_index_across_partitions_refuses_the_publication(store):
    """R5: a unique index that does not hold the partition column can only
    fail when the build meets the other partitions' rows, at the switch. The
    publication is refused whole (PublicationRefused): the old rows stay,
    the build stays registered for the next call, and a later call
    publishes once the data changes."""

    out = output(incremental=True, partition_column="site")
    p1 = await store.store(rows(a="1"), None, at(out, 1, partition="p1"))
    await store.store(rows(b="1"), None, at(out, 2, partition="p2"))
    sql(f"CREATE UNIQUE INDEX ON {store._table(out)[0]} (id)")
    run = await store.store(rows(b="5"), p1.ref, at(out, 5, reset=True, partition="p1"))
    with pytest.raises(PublicationRefused):
        await store.full_run_ended(out, "p1", 5, 5)
    assert outside(store, out, "p1") == {"a": "1"} and registry(store, out, "p1") == (5, None, [5])
    await store.store(Patch([], remove=["b"]), None, at(out, 6, partition="p2"))
    assert await store.full_run_ended(out, "p1", 5, 5)
    assert outside(store, out) == {"b": "5"}
    assert await solera(store, run.ref) == {"b": "5"}


# -- lock order and the catalog (probes P1, P3, P4, R1, R2, R7, R8, R10, R11) ------------------


async def test_a_write_does_not_wait_for_a_switch_of_another_partition(store):
    """P1, P1c: a switch holds ROW EXCLUSIVE on the user's table for minutes.
    A write of another partition changes the catalog only when it lacks
    something (CREATE INDEX IF NOT EXISTS takes SHARE even when the index
    exists, and would queue behind the switch): it does not wait."""

    out = output(incremental=True, partition_column="site", indexes=[["v"]])
    await store.store(rows(a="1"), None, at(out, 1, partition="p1"))
    p2 = await store.store(rows(b="1"), None, at(out, 2, partition="p2"))
    switch = open_switch(store, out, "p1")
    try:
        started = time.monotonic()
        await asyncio.wait_for(store.store(Patch(rows(b="3")), p2.ref, at(out, 3, partition="p2")), 5)
        assert time.monotonic() - started < 2
    finally:
        switch.rollback()
        switch.close()


async def test_a_write_of_the_switching_partition_waits_then_writes_where_the_log_points(store):
    """P3, P4: a write of the partition being switched waits on its version
    row, and once the switch commits it writes the published rows (the build
    is no longer registered): it never names the dropped build."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    run = await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    holder = open_switch(store, out, "")
    holder.rollback()
    holder.close()
    thread, done, errors, results = in_thread(store.full_run_ended(out, "", 5, 5))
    writing = asyncio.create_task(store.store(Patch(rows(b="6")), run.ref, at(out, 6, run=5)))
    written = await asyncio.wait_for(writing, 10)
    thread.join(10)
    assert not errors and results in ([True], [False])
    if results == [False]:  # the write came first: the switch saw a newer attempt
        assert written.ref.handle.get("build") == 5
        assert await store.full_run_ended(out, "", 5, 6)
    assert outside(store, out) == {"a": "5", "b": "6"} and builds(store, out) == []


async def test_a_build_made_while_a_writer_waited_is_found(store):
    """R1, R2, R10, R11: the write that opens run 6's build holds the version
    row; a writer of run 6 waits on it, and once the first commits it finds
    the build: the registry is read in the row's lock, never before it
    (to_regclass before a wait does not see the build)."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    started = threading.Event()
    real = store._register

    def slow(cur, target, checked):
        real(cur, target, checked)
        started.set()
        time.sleep(1.0)

    store._register = slow
    opening = asyncio.create_task(store.store(rows(a="6"), first.ref, at(out, 6, reset=True)))
    await asyncio.to_thread(started.wait, 10)
    store._register = real
    waiting = asyncio.create_task(store.store(Patch(rows(b="6")), first.ref, at(out, 6, run=6)))
    await asyncio.sleep(0.3)
    assert not waiting.done()
    await asyncio.wait_for(opening, 10)
    written = await asyncio.wait_for(waiting, 10)
    assert written.ref.handle["build"] == 6
    assert await solera(store, written.ref) == {"a": "6", "b": "6"}
    assert outside(store, out) == {"a": "1"}


async def test_a_migration_with_nothing_pending_does_not_wait_out_a_switch(store):
    """R7: an attempt of an output with migrations reads the ledger with no
    lock; only a pending migration takes the write domain exclusively."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    applied = Migration("noop", "SELECT 1")
    await store.migrate(out, [applied], at(out, 2), first.ref)
    switch = open_switch(store, out, "")
    try:
        await asyncio.wait_for(store.migrate(out, [applied], at(out, 3), first.ref), 3)
    finally:
        switch.rollback()
        switch.close()


async def test_first_writers_with_grants_do_not_collide(store):
    """R8: every writer used to GRANT; two at once collided ("tuple
    concurrently updated"). A grant is given only when missing."""

    role = "solera_b8_reader"
    if not sql("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)):
        sql(f"CREATE ROLE {role}")
    from solera_postgres import PostgresStore

    granting = PostgresStore(DSN, grants=[role])
    out = output(incremental=True, partition_column="site")
    await granting.store(rows(a="0"), None, at(out, 1, partition="p0"))
    await asyncio.gather(
        *(granting.store(rows(a=str(i)), None, at(out, 10 + i, partition=f"p{i}")) for i in range(1, 9))
    )
    table, _, _ = granting._table(out)
    assert sql("SELECT has_table_privilege(%s, %s, 'SELECT')", (role, table)) == [(True,)]


# -- loads and the DROP (probes P2, R3) ----------------------------------------------------


async def test_a_load_racing_the_switchs_drop_reads_the_published_rows(store, monkeypatch):
    """P2: a load looked the build up, then a switch published and dropped it
    before the load's query ran. The query finds no table; the load reads
    the user's table again, which now holds the build's rows."""

    from solera_postgres import BUILD_SCHEMA, _build_name, _qname

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    run = await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    assert await store.full_run_ended(out, "", 5, 5)
    gone = _qname(BUILD_SCHEMA, _build_name(store._table(out)[0], "", 5))
    monkeypatch.setattr(store, "_source", lambda cur, ref: gone)  # the lookup made before the switch
    assert await solera(store, run.ref) == {"a": "5"}


async def test_loads_racing_switches_always_read_the_runs_rows(store):
    """Loads of a ref to the build run in a loop while the run is switched:
    each reads the build or, after the switch, the published rows (the same
    rows), and none fails."""

    out = output(incremental=True)
    first = await store.store(rows(**{f"k{i}": "1" for i in range(200)}), None, at(out, 1))
    run = await store.store(rows(**{f"k{i}": "5" for i in range(300)}), first.ref, at(out, 5, reset=True))
    expected = {f"k{i}": "5" for i in range(300)}
    stop, seen, errors = threading.Event(), [], []

    def reader():
        try:
            while not stop.is_set():
                seen.append(asyncio.run(solera(store, run.ref)) == expected)
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    await asyncio.sleep(0.3)
    assert await store.full_run_ended(out, "", 5, 5)
    await asyncio.sleep(0.3)
    stop.set()
    for t in threads:
        t.join()
    assert not errors and len(seen) > 10 and all(seen)


async def test_a_switch_never_waits_for_a_load_of_its_build(store):
    """R3: a load of the build is open. The switch publishes and unregisters
    the build without waiting for it; the DROP comes after, in its own short
    transaction that gives up at once, and is left to the next sweep while
    the load holds the table."""

    from solera_postgres import BUILD_SCHEMA, _build_name, _qname

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    reader = store._connect()
    reader.execute(
        f"SELECT * FROM {_qname(BUILD_SCHEMA, _build_name(store._table(out)[0], '', 5))}"
    ).fetchall()
    try:
        assert await asyncio.wait_for(store.full_run_ended(out, "", 5, 5), 8) is True
        assert outside(store, out) == {"a": "5"} and registry(store, out)[2] == []
        assert builds(store, out) == [5]  # still being read
    finally:
        reader.commit()
        reader.close()
    assert await store.full_run_ended(out, "", 5, 5) is True  # a repeat sweeps
    assert builds(store, out) == []


async def test_a_crash_after_the_switch_leaves_its_build_to_the_next_sweep(store, monkeypatch):
    """The switch committed and the process died before its DROP. The build
    is unregistered; the next call (the engine repeats it after a restart)
    or the partition's next write drops it."""

    out = output(incremental=True)
    first = await store.store(rows(a="1"), None, at(out, 1))
    await store.store(rows(a="5"), first.ref, at(out, 5, reset=True))
    sweep = store._sweep
    monkeypatch.setattr(store, "_sweep", lambda table, partition: None)
    assert await store.full_run_ended(out, "", 5, 5)
    assert builds(store, out) == [5]
    monkeypatch.setattr(store, "_sweep", sweep)
    assert await store.full_run_ended(out, "", 5, 5)
    assert builds(store, out) == []


# -- what outsiders see (probe P6) ---------------------------------------------------------


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "REPEATABLE READ"])
async def test_outside_readers_see_all_old_then_all_new_and_never_wait(store, isolation):
    """P6: readers outside Solera, under READ COMMITTED and REPEATABLE READ,
    query the table while a switch of 50,000 rows runs: every answer holds
    only old rows or only new ones, and none waits for the switch."""

    import psycopg

    out = output(incremental=True)
    table, _, _ = store._table(out)
    first = await store.store([{"id": f"k{i}", "v": "1"} for i in range(50_000)], None, at(out, 1))
    await store.store([{"id": f"k{i}", "v": "5"} for i in range(50_000)], first.ref, at(out, 5, reset=True))
    stop, answers, slowest, errors = threading.Event(), [], [0.0], []

    def reader():
        try:
            with psycopg.connect(DSN, autocommit=True) as conn:
                while not stop.is_set():
                    started = time.monotonic()
                    with conn.transaction():
                        conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}")
                        one = conn.execute(
                            f"SELECT count(*), min(_version), max(_version) FROM {table}"
                        ).fetchone()
                        two = conn.execute(
                            f"SELECT count(*), min(_version), max(_version) FROM {table}"
                        ).fetchone()
                    slowest[0] = max(slowest[0], time.monotonic() - started)
                    answers.append(one)
                    if isolation == "REPEATABLE READ":
                        assert two == one
                    answers.append(two)
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=reader) for _ in range(2)]
    for t in threads:
        t.start()
    await asyncio.sleep(0.2)
    assert await store.full_run_ended(out, "", 5, 5)
    await asyncio.sleep(0.2)
    stop.set()
    for t in threads:
        t.join()
    assert not errors
    assert set(answers) <= {(50_000, 1, 1), (50_000, 5, 5)}
    assert (50_000, 1, 1) in answers and (50_000, 5, 5) in answers
    assert slowest[0] < 2


# -- the schema of builds (probe P8) -------------------------------------------------------


async def test_two_first_builds_make_the_build_schema_once():
    """P8: the first builds of a database, at once, all create solera_build:
    they take turns, and none fails."""

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    name = f"b8_{uuid.uuid4().hex[:10]}"
    sql(f"CREATE DATABASE {name}")
    try:
        dsn = re.sub(r"/[^/?]+(\?|$)", f"/{name}\\1", DSN, count=1)
        store = PostgresStore(dsn)
        outs = [output(incremental=True) for _ in range(4)]
        await asyncio.gather(*(store.store(rows(a="1"), None, at(o, 5, reset=True)) for o in outs))
        with psycopg.connect(dsn) as conn:
            assert conn.execute(
                "SELECT count(*) FROM pg_namespace WHERE nspname = 'solera_build'"
            ).fetchone() == (1,)
    finally:
        sql(f"DROP DATABASE {name} WITH (FORCE)")


# -- the cost: one statement, no extra round trip ------------------------------------------


def round_trips(store, monkeypatch) -> list:
    """Record what the next write sends, as the statements of each round trip."""

    import psycopg

    connect, traces = store._connect, []

    def traced():
        conn = connect()
        trace = tempfile.TemporaryFile("w+")
        conn.pgconn.trace(trace.fileno())
        conn.pgconn.set_trace_flags(psycopg.pq.Trace.SUPPRESS_TIMESTAMPS)
        traces.append(trace)
        return conn

    monkeypatch.setattr(store, "_connect", traced)
    return traces


def trips_of(trace) -> list[list[str]]:
    trace.seek(0)
    trips, current = [], []
    for line in trace.read().splitlines():
        parts = line.split("\t")
        if len(parts) < 3 or parts[0] != "F":
            continue
        kind = parts[2].split(" ")[0]
        if kind in ("Parse", "Query"):
            current.append(" ".join(parts[2:]))
        if kind in ("Sync", "Query", "CopyDone", "CopyFail"):
            trips.append(current)
            current = []
    return [t for t in trips if t]


async def test_the_version_check_is_one_statement_in_the_writes_first_round_trip(store, monkeypatch):
    """The check is one statement per write transaction, sent in one pipeline
    with the write domain's lock and the catalog lookup, so it adds no round
    trip to the write."""

    out = output(incremental=True)
    first = await store.store(rows(a="1", b="1"), None, at(out, 1))
    ref = (await store.store(Patch(rows(a="2")), first.ref, at(out, 2))).ref
    traces = round_trips(store, monkeypatch)
    await store.store(Patch(rows(b="3")), ref, at(out, 3))
    assert len(traces) == 1
    trips = trips_of(traces[0])
    versions = [s for trip in trips for s in trip if "solera_versions" in s]
    assert len(versions) == 1
    first_trip = next(trip for trip in trips if any("solera_versions" in s for s in trip))
    assert len(first_trip) == 3 and "pg_advisory_xact_lock_shared" in first_trip[0]
    assert "solera_versions" in first_trip[1]  # the third is the catalog lookup (_STATE), on lines of its own
    assert len(trips) <= 6  # BEGIN, that pipeline, the DELETE, the COPY, the key check, COMMIT


# -- a random history against the decided model -------------------------------------------


def _random_history(store, out, seed: int, steps: int) -> None:
    """Writes, late writes, replays and switches on two partitions of one
    table, each partition against the model the design was checked with
    (tests/tracking/fullrun.py, a table with every rule on): after every step
    the published rows, the registered builds and each build's rows, with
    their versions, are the model's, and no build is left that the registry
    does not list. Each call's outcome (refused, published) is the model's."""

    from solera.sdk import TableRef
    from solera_postgres import BUILD_SCHEMA, _build_name, _qname

    from tests.tracking.core import ABSENT
    from tests.tracking.fullrun import Output as Model

    rng = random.Random(seed)
    table, _, _ = store._table(out)
    models = {p: Model(p, "table") for p in ("p1", "p2")}
    clears = {p: [] for p in models}
    held = {p: [] for p in models}  # attempts that planned and stall: written late, if ever
    last = {p: None for p in models}
    version = 0

    def prior(p):
        return TableRef(
            output=out.name, store="", handle={"table": table, "where": {"site": p}, "key": "id"}, partition=p
        )

    def write(p, attempt):
        v, writes, clear, ctx = attempt
        model = models[p]
        refused = model.fence > v
        if not refused:
            model.fence = max(model.fence, v)
            model.write(v, writes, clear, None, ctx)
        context = at(out, v, reset=clear, run=ctx[0], owed=ctx[1], partition=p)
        upserts = [{"id": k, "v": f"{v}{k}"} for k, value in sorted(writes.items()) if value is not ABSENT]
        removes = sorted(k for k, value in writes.items() if value is ABSENT)
        data = upserts if clear else Patch(upserts, remove=removes)
        try:
            written = asyncio.run(store.store(data, prior(p), context))
        except StoreError as error:
            assert refused and "newer attempt" in str(error), (seed, attempt, error)
            return
        assert not refused, (seed, attempt, "the store took a write the model refuses")
        last[p] = attempt
        expected = model.builds.get(written.ref.handle.get("build"), model.published)
        loaded = {r["id"]: r["v"] for r in asyncio.run(store.load(written.ref, list[dict], None))}
        assert loaded == {k: f"{row.version}{k}" for k, row in expected.items()}, (seed, attempt)

    def check(p):
        model = models[p]
        found = sql(f"SELECT id, v, _version FROM {table} WHERE site = %s", (p,)) or []
        assert {k: (v, g) for k, v, g in found} == {
            k: (f"{r.version}{k}", r.version) for k, r in model.published.items()
        }, (seed, p, "published")
        registered = (registry(store, out, p) or (0, None, []))[2]
        assert sorted(registered) == sorted(model.builds) == builds(store, out, p), (seed, p, "builds")
        for run, rows_ in model.builds.items():
            found = sql(f"SELECT id, v, _version FROM {_qname(BUILD_SCHEMA, _build_name(table, p, run))}")
            assert {k: (v, g) for k, v, g in found} == {
                k: (f"{r.version}{k}", r.version) for k, r in rows_.items()
            }, (seed, p, run)

    sql(f"CREATE TABLE {table} (id text, v text, site text, _version bigint)")
    for _ in range(steps):
        p = rng.choice(sorted(models))
        model, roll = models[p], rng.random()
        if roll < 0.55:
            version += 1
            clear = rng.random() < 0.3
            keys = rng.sample("abcde", rng.randint(0 if clear else 1, 3))
            writes = {k: ABSENT if not clear and rng.random() < 0.3 else version for k in keys}
            named = [None, *clears[p][-3:]]
            run = rng.choice(named)
            owed = rng.choice([g for g in named if g != run or g is None])
            attempt = (version, writes, clear, (run, owed))
            if clear:
                clears[p].append(version)
            if rng.random() < 0.2:
                held[p].append(attempt)
            else:
                write(p, attempt)
        elif roll < 0.67 and held[p]:
            write(p, held[p].pop(rng.randrange(len(held[p]))))
        elif roll < 0.75 and last[p] is not None:
            write(p, last[p])
        elif clears[p]:
            run = rng.choice(clears[p][-3:])
            newest = model.fence - (1 if rng.random() < 0.2 else 0)
            expected = model.switch(run, newest)
            assert asyncio.run(store.full_run_ended(out, p, run, newest)) is expected, (seed, p, run, newest)
        check(p)
    for p in models:
        check(p)


@pytest.mark.parametrize("seed", range(int(os.environ.get("SOLERA_RANDOM_HISTORIES", "12"))))
def test_a_random_history_matches_the_model(store, seed):
    _random_history(store, output(incremental=True, partition_column="site"), seed, steps=40)
