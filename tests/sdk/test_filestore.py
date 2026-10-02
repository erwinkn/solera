"""§4: FileStore and S3Store — one object per value, key or batch, each
written once under a name that carries the writing attempt's generation
(docs/lifecycle.md §9.8, docs/versions.md); JSON when it round-trips,
pickle otherwise. The store never works out what changed; the harness
does, against the key index (tests/worker), and says so in the write's
scope. A keyed read names its objects from the generations the index
holds."""

import os
import uuid
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pandas as pd
import pytest
from solera.keys.index import key_str
from solera.sdk import KEYS, Output, PartitionSet, Ref, RegistrationError
from solera.stores import (
    Batches,
    FileStore,
    KeyedWrite,
    Keys,
    Patch,
    S3Store,
    StoreError,
    WriteError,
    prepare,
)

from tests.conftest import scope

# An S3-compatible server to also run against, e.g. http://user:secret@127.0.0.1:9100/bucket
S3_URL = os.getenv("SOLERA_TEST_S3")


@pytest.fixture(params=["file", "s3"])
def store(request, data):
    if request.param == "file":
        return FileStore(data)
    if not S3_URL:
        pytest.skip("set SOLERA_TEST_S3 to run against an S3-compatible server")
    u = urlsplit(S3_URL)
    return S3Store(
        f"s3://{u.path.strip('/')}/filestore-test-{uuid.uuid4().hex}",
        endpoint=f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
        access_key_id=unquote(u.username),
        secret_access_key=unquote(u.password),
        region="us-east-1",
        client_options={"allow_http": True},
    )


async def paths(store) -> list[str]:
    import obstore

    found = []
    async for chunk in obstore.list(store._objects()):
        found += [m["path"] for m in chunk]
    return sorted(found)


def at(out, content, generation, keys=None) -> Keys:
    """The `Keys` a key index would hold after writing `content` at `generation`."""

    written = [key_str(k) for k in prepare(content, out).rows.entries()[0]]
    return Keys({k: generation for k in written if keys is None or k in keys})


async def test_a_value_is_one_object_per_generation(store):
    out = Output("rollup")
    first = await store.store({"sites": 3}, None, scope(out, generation=7))
    assert first.ref.handle == {"mode": "value", "path": "rollup@7", "base": "rollup"}
    assert await store.load(first.ref, None, None) == {"sites": 3}
    second = await store.store({"sites": 4}, first.ref, scope(out, generation=9))
    assert second.ref.handle["path"] != first.ref.handle["path"]
    assert await paths(store) == ["rollup@7.json", "rollup@9.json"]
    assert await store.load(first.ref, None, None) == {"sites": 3}  # a pinned reader reads its version
    assert await store.load(second.ref, None, None) == {"sites": 4}
    again = await store.store({"sites": 4}, second.ref, scope(out, generation=9))  # the same attempt again
    assert again.ref == second.ref


async def test_what_json_cannot_hold_is_pickled(store):
    out = Output("frame")
    frame = pd.DataFrame({"a": [1, 2]})
    written = await store.store(frame, None, scope(out, generation=1))
    assert await paths(store) == ["frame@1.pkl"]
    assert (await store.load(written.ref, None, None)).equals(frame)
    written = await store.store({1: "int keys"}, written.ref, scope(out, generation=2))
    assert await store.load(written.ref, None, None) == {1: "int keys"}


async def test_partitions_and_keys_are_escaped_path_segments(store):
    out = Output("status", keyed=True)
    content = {"a/b": 1, "..": 2, "ü": 3}
    written = await store.store(content, None, scope(out, partition="site|x", generation=4))
    assert written.ref.handle == {"mode": "keyed", "path": "status/site%7Cx", "key": KEYS}
    assert await store.load(written.ref, None, at(out, content, 4)) == content
    assert all(p.startswith("status/site%7Cx/") for p in await paths(store))


async def test_a_keyed_output_is_one_object_per_key_and_generation(store):
    out = Output("uploads", keyed=True)
    first = {"u-1": {"bytes": 3}, "u-2": {"bytes": 5}}
    written = await store.store(first, None, scope(out, generation=5))
    names = await paths(store)
    assert names == ["uploads/u-1/5.json", "uploads/u-2/5.json"]
    assert {n.split("/")[1] for n in names} == {"u-1", "u-2"}
    with pytest.raises(StoreError, match="takes Keys"):
        await store.load(written.ref, None, None)  # a whole read goes through the index
    patch = {"u-2": {"bytes": 6}}
    patched = await store.store(Patch(patch, remove=["u-1"]), written.ref, scope(out, generation=8))
    assert len(await paths(store)) == 3  # nothing overwritten, nothing deleted
    current = at(out, patch, 8)
    assert await store.load(patched.ref, None, current) == {"u-2": {"bytes": 6}}
    assert await store.load(written.ref, None, at(out, first, 5)) == first  # the old version, intact
    old = [("key", k, g) for k, g in at(out, first, 5).generations.items()]
    await store.discard(scope(out, generation=9), patched.ref, old)
    await store.discard(
        scope(out, generation=9), patched.ref, old
    )  # names are never reused: twice is no harm
    assert await store.load(patched.ref, None, current) == {"u-2": {"bytes": 6}}
    assert len(await paths(store)) == 1


async def test_a_keyed_write_touches_only_what_the_harness_says(store):
    out = Output("uploads", keyed=True)
    content = {"a": 1, "b": 20, "c": 3}
    prepared = prepare(content, out)
    only = KeyedWrite(prepared, frozenset({"b"}), frozenset({"a"}))
    await store.store(only, None, scope(out, generation=3))
    # `c` is in the write but not in upserts: the harness knows it is there already.
    assert await paths(store) == ["uploads/b/3.json"]
    # A requested key the write does not hold is an error, never a silent skip.
    with pytest.raises(StoreError, match="zzz"):
        await store.store(KeyedWrite(prepared, frozenset({"b", "zzz"})), None, scope(out, generation=4))


async def test_a_keyed_output_takes_a_dict_of_str(store):
    out = Output("uploads", keyed=True)
    for bad in ([{"id": 1}], {1: "x"}, "text"):
        with pytest.raises(WriteError, match="dict"):
            await store.store(bad, None, scope(out))


def test_keyed_registration():
    assert Output("u", keyed=True).key == KEYS and Output("u", keyed=True).incremental
    with pytest.raises(RegistrationError):
        Output("u", keyed=True, key="id")
    fs = FileStore()
    assert fs.can_store(dict[str, int], Output("u", keyed=True))
    assert not fs.can_store(list[dict], Output("u", keyed=True))
    assert not fs.can_store(dict[int, int], Output("u", keyed=True))
    assert not fs.can_store(pd.DataFrame, Output("u", keyed=True))


async def test_rows_by_key_column(store):
    out = Output("files", key="id")
    rows = [{"id": 1, "v": "1"}, {"id": 2, "v": "1"}]
    first = await store.store(rows, None, scope(out, generation=1))
    assert await paths(store) == ["files/1/1.json", "files/2/1.json"]
    patch = [{"id": 3, "v": "1"}]
    patched = await store.store(Patch(patch, remove=[1]), first.ref, scope(out, generation=2))
    current = Keys({**at(out, rows, 1, keys={"2"}).generations, **at(out, patch, 2).generations})
    assert await store.load(patched.ref, list[dict], current) == [{"id": 2, "v": "1"}, {"id": 3, "v": "1"}]
    frame = await store.load(patched.ref, pd.DataFrame, at(out, patch, 2))
    assert list(frame["id"]) == [3]
    # Every key holds all its rows: a patch of the key replaces the whole group.
    group = [{"id": 3, "v": "2"}, {"id": 3, "v": "2", "n": 1}]
    grouped = await store.store(Patch(group), patched.ref, scope(out, generation=3))
    assert await store.load(grouped.ref, list[dict], at(out, group, 3)) == group
    with pytest.raises(WriteError, match="key column"):
        await store.store([{"v": 1}], None, scope(out, generation=4))


async def test_unkeyed_incremental_is_one_object_per_batch(store):
    out = Output("events", incremental=True)
    with pytest.raises(WriteError, match="Patch"):
        await store.store([{"e": 0}], None, scope(out, batch=0))
    first = await store.store(Patch([{"e": 1}]), None, scope(out, batch=3, generation=10))
    second = await store.store(Patch([{"e": 2}, {"e": 3}]), first.ref, scope(out, batch=4, generation=11))
    assert second.ref.handle == {"mode": "batches", "path": "events", "batches": [3, 4]}
    assert await store.load(second.ref, None, None) == [{"e": 1}, {"e": 2}, {"e": 3}]
    assert await store.load(second.ref, None, Batches(4, 4)) == [{"e": 2}, {"e": 3}]
    assert (await store.store(Patch([]), second.ref, scope(out, batch=5, generation=12))).ref is second.ref


async def test_a_batchs_committed_object_is_its_highest_generation(store):
    """Retries reuse a batch number: a dead attempt's batch 4 (generation 11)
    and its retry's (generation 12) are both there, and the retry, which
    committed, is the one read. A full run starts over at a later batch."""

    out = Output("events", incremental=True)
    first = await store.store(Patch([{"e": 1}]), None, scope(out, batch=3, generation=10))
    await store.store(Patch([{"e": "dead"}]), first.ref, scope(out, batch=4, generation=11))
    retry = await store.store(Patch([{"e": 2}]), first.ref, scope(out, batch=4, generation=12))
    assert await store.load(retry.ref, None, None) == [{"e": 1}, {"e": 2}]
    reset = await store.store(Patch([{"e": 9}]), None, scope(out, batch=6, generation=13))
    assert await store.load(reset.ref, None, None) == [{"e": 9}]


async def test_a_partition_set_is_its_element_list(store):
    out = PartitionSet("sites")
    written = await store.store(["Richmond", "Perth"], None, scope(out, generation=1))
    assert await store.load(written.ref, list, None) == ["Richmond", "Perth"]
    assert await store.load(written.ref, list, Keys({"Perth": 1})) == ["Perth"]
    patched = await store.store(Patch(["Hobart"], remove=["Perth"]), written.ref, scope(out, generation=2))
    assert await store.load(patched.ref, list, None) == ["Richmond", "Hobart"]


async def test_a_renamed_output_keeps_its_objects(store):
    old = Output("old", keyed=True)
    written = await store.store({"a": 1}, None, scope(old, generation=1))
    renamed = await store.store(Patch({"b": 2}), written.ref, scope(Output("new", keyed=True), generation=2))
    assert renamed.ref.handle["path"] == "old"
    both = Keys({**at(old, {"a": 1}, 1).generations, **at(old, {"b": 2}, 2).generations})
    assert await store.load(renamed.ref, None, both) == {"a": 1, "b": 2}
    value = await store.store(1, None, scope(Output("v1"), generation=3))
    moved = await store.store(2, value.ref, scope(Output("v2"), generation=4))
    assert moved.ref.handle == {"mode": "value", "path": "v1@4", "base": "v1"}


async def test_refs_round_trip_and_gone_values_fail(store):
    written = await store.store("x", None, scope(Output("v"), generation=1))
    back = Ref.from_json(written.ref.to_json())
    assert type(back).__name__ == "ObjectRef" and back == written.ref
    await store.discard(scope(Output("v"), generation=2), written.ref, [("value", 1)])
    with pytest.raises(StoreError, match="gone"):
        await store.load(back, None, None)


def test_the_default_path(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_DATA")
    store = FileStore()
    store.home = str(tmp_path)
    assert Path(store._objects().prefix) == tmp_path / ".solera" / "data"
    monkeypatch.setenv("SOLERA_DATA", str(tmp_path / "elsewhere"))
    assert Path(store._objects().prefix) == tmp_path / "elsewhere"


async def test_one_batch_is_read_without_listing_the_history(store, monkeypatch):
    """A consumer reading one batch — the newest, or the first of a long
    history — lists that batch's objects, not the output's whole history
    (§6: hot-path work does not grow with history). A range past the ref's
    last batch stops at it: a batch no commit wrote yet is not read."""

    import obstore

    out = Output("events", incremental=True)
    ref = None
    for batch in range(200):
        ref = (
            await store.store(Patch([{"e": batch}]), ref, scope(out, batch=batch, generation=batch + 1))
        ).ref
    listed = []
    real = obstore.list

    def counting(*args, **kwargs):
        async def chunks():
            async for chunk in real(*args, **kwargs):
                listed.append(len(chunk))  # a page: one request to a bucket
                yield chunk

        return chunks()

    await store.store(Patch([{"e": "uncommitted"}]), ref, scope(out, batch=200, generation=300))
    monkeypatch.setattr(obstore, "list", counting)
    for lo, hi, read in ((199, 199, [{"e": 199}]), (0, 0, [{"e": 0}]), (199, 205, [{"e": 199}])):
        listed.clear()
        assert await store.load(ref, None, Batches(lo, hi)) == read
        assert len(listed) <= 1, (lo, hi, listed)


async def test_many_objects_are_never_many_tasks_at_once(store):
    """`PARALLEL` requests in flight, from as many workers: a large write or
    read never holds a task per object."""

    import asyncio

    from solera.stores import PARALLEL

    seen = []

    async def one(i):
        seen.append(len(asyncio.all_tasks()))
        await asyncio.sleep(0)
        return i

    assert await store._many(one, range(5000)) == list(range(5000))
    assert max(seen) <= PARALLEL + 5, max(seen)


async def test_a_prepared_write_is_read_once_and_only_its_selection_taken(store, monkeypatch):
    """§4, §6: the harness reads a keyed write once (`Prepared`); the store
    reads no more of it than the groups of the keys it is asked to write —
    a DataFrame of many keys with one changed is one group, not every row."""

    import dataclasses

    import solera.stores as stores

    out = Output("frame", key="id")
    frame = pd.DataFrame({"id": [f"k{i}" for i in range(1000)], "n": range(1000)})
    prepared = store.prepare(frame, out)  # FileStore takes DataFrames (`solera.stores.frames`)
    taken = []

    def counting(rows):
        taken.append(None if rows is None else len(rows))
        return prepared.take(rows)

    monkeypatch.setattr(stores, "prepare_for", lambda *a, **k: pytest.fail("the store read the write again"))
    write = KeyedWrite(dataclasses.replace(prepared, take=counting), frozenset({"k7"}))
    written = await store.store(write, None, scope(out, generation=2))
    assert taken == [1]
    assert await store.load(written.ref, None, Keys({"k7": 2})) == [{"id": "k7", "n": 7}]


async def test_a_missing_object_a_commit_names_fails_the_read(store, data):
    """An immutable store's objects are named by the key index or the ref: a
    missing one is an error, never a silently shorter read."""

    import obstore

    out = Output("uploads", keyed=True)
    written = await store.store({"a": 1, "b": 2}, None, scope(out, generation=1))
    keys = at(out, {"a": 1, "b": 2}, 1)
    gone = FileStore.key_name(written.ref.handle["path"], "a", keys.generations["a"])
    await obstore.delete_async(store._objects(), f"{gone}.json")
    with pytest.raises(StoreError, match="'a'.* is gone"):
        await store.load(written.ref, None, keys)
    events = Output("events", incremental=True)
    batch = await store.store(Patch([{"e": 1}]), None, scope(events, batch=0, generation=1))
    names = await store._batches(batch.ref.handle["path"], 0, 0)
    await obstore.delete_async(store._objects(), f"{names[0]}.json")
    with pytest.raises(StoreError, match="is gone"):
        await store.load(batch.ref, None, None)


async def test_a_store_reads_its_own_types(store):
    """A store reads writes of a type of its own (`Store.prepare`): the key
    index and its writes see that type's rows, nothing else does."""

    import dataclasses

    from solera.keys import Rows
    from solera.stores import Prepared

    @dataclasses.dataclass
    class Sheet:  # a type of the store's own: rows as (key, amount) pairs
        lines: list

    def prepare(write, output):
        rows = [{"id": k, "amount": a} for k, a in write.lines]
        return Prepared(
            output, Rows.records(rows, "id"), lambda at: rows if at is None else [rows[r] for r in at]
        )

    store.prepare = prepare
    out = Output("sheet", key="id")
    written = await store.store(Sheet([("a", 1), ("b", 2)]), None, scope(out, generation=1))
    assert await store.load(written.ref, None, Keys({"a": 1, "b": 1})) == [
        {"id": "a", "amount": 1},
        {"id": "b", "amount": 2},
    ]


def test_a_write_with_no_selection_is_paged_natively():
    """`KeyedWrite.iter_pages`: every key of the write, a page at a time, with
    its group — never every entry at once."""

    out = Output("t", key="id")
    rows = [{"id": f"k{i}", "n": i} for i in range(5)]
    write = KeyedWrite(prepare(rows, out), whole=True)
    pages = list(write.iter_pages(2))
    assert [len(p) for p in pages] == [2, 2, 1]
    assert [(k, g) for p in pages for k, g in p] == [(r["id"], [r]) for r in rows]
