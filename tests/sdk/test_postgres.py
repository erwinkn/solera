"""§3/§4: PostgresStore — markers, Patch, append snapshots, partition slices,
Sql writes. Skips unless DORC_TEST_DATABASE_URL points at a scratch database."""

import os
import uuid

import pytest
from data_orchestrator.sdk import Output, digest
from data_orchestrator.stores import Keys, Patch, Sql, StaleRead, StoreConflict, WriteError

from tests.conftest import scope

pytestmark = pytest.mark.postgres

DSN = os.environ.get("DORC_TEST_DATABASE_URL")


@pytest.fixture
def store():
    if not DSN:
        pytest.skip("DORC_TEST_DATABASE_URL is not set")
    from dorc_postgres import PostgresStore

    return PostgresStore(DSN)


def output(name=None, **config):
    return Output(name or f"t_{uuid.uuid4().hex[:12]}", store="postgres", **config)


async def test_bare_replace_and_versions(store):
    """§3: a bare write replaces the scope; equal content keeps the version."""

    out = output(key="id", revision="v")
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    second = await store.store([{"id": "a", "v": "1"}], first.ref, scope(out, prior_keys=first.keys))
    assert first.ref.version == second.ref.version
    third = await store.store([{"id": "a", "v": "2"}], first.ref, scope(out))
    assert third.ref.version != first.ref.version
    assert await store.load(third.ref, list[dict], None) == [{"id": "a", "v": "2"}]


async def test_patch_upsert_remove_and_orphan_sweep(store):
    """§3/§4: Patch upserts keys, removes others, and deletes rows absent
    from the resulting map (orphans left by uncommitted attempts)."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    assert first.keys == {"a": "1", "b": "1"}
    second = await store.store(
        Patch([{"id": "b", "v": "2"}, {"id": "c", "v": "9"}], remove=["a"]),
        first.ref,
        scope(out, prior_keys=first.keys),
    )
    assert second.keys == {"b": "2", "c": "9"}
    assert sorted(r["id"] for r in await store.load(second.ref, list[dict], None)) == ["b", "c"]
    with pytest.raises(WriteError):
        await store.store(
            Patch([{"id": "x", "v": "1"}, {"id": "x", "v": "1"}]),
            second.ref,
            scope(out, prior_keys=second.keys),
        )


async def test_marker_fences_store_and_load(store):
    """§3: store() refuses when the live marker != prior.version; load()
    refuses when it != ref.version."""

    out = output(key="id", revision="v", primary_key=["id"])
    first = await store.store([{"id": "a", "v": "1"}], None, scope(out))
    moved = await store.store([{"id": "a", "v": "2"}], first.ref, scope(out, prior_keys=first.keys))
    with pytest.raises(StoreConflict):
        await store.store([{"id": "a", "v": "3"}], first.ref, scope(out, prior_keys=first.keys))
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


async def test_append_snapshot_at_pinned_version(store):
    """§3: an append ref's load returns batches <= the pinned newest batch,
    even after later writes."""

    out = output(mode="append", partition_column="site")
    first = await store.store(Patch([{"e": 1}]), None, scope(out, partition="s1"))
    second = await store.store(
        Patch([{"e": 2}]), first.ref, scope(out, partition="s1", prior_keys=first.keys)
    )
    assert first.keys == {"0": digest([{"e": 1, "_batch": 0, "site": "s1"}])}
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
