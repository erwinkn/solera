"""§4: FileStore and S3Store — one object per value, partition, key or
batch, overwritten in place; JSON when it round-trips, pickle otherwise. The
store never works out what changed; the harness does, against the key index
(tests/worker), and says so in the write's scope."""

import os
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import pytest
from solera.sdk import KEYS, Output, PartitionSet, Ref, RegistrationError
from solera.stores import Batches, FileStore, Keys, Patch, S3Store, StoreError, WriteError, revision

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
        endpoint=f"{u.scheme}://{u.hostname}:{u.port}",
        access_key_id=u.username,
        secret_access_key=u.password,
        region="us-east-1",
        client_options={"allow_http": True},
    )


async def paths(store) -> list[str]:
    import obstore

    found = []
    async for chunk in obstore.list(store._objects()):
        found += [m["path"] for m in chunk]
    return sorted(found)


async def test_a_value_is_one_object_overwritten_in_place(store):
    out = Output("rollup")
    first = await store.store({"sites": 3}, None, scope(out))
    assert first.ref.handle == {"mode": "value", "path": "rollup"}
    assert await store.load(first.ref, None, None) == {"sites": 3}
    again = await store.store({"sites": 3}, first.ref, scope(out))
    assert again.ref.version == first.ref.version  # same content: the commit is unchanged
    second = await store.store({"sites": 4}, first.ref, scope(out))
    assert second.ref.version != first.ref.version
    assert await paths(store) == ["rollup.json"]
    assert await store.load(first.ref, None, None) == {"sites": 4}  # no history: reads get what is there


async def test_what_json_cannot_hold_is_pickled(store):
    out = Output("frame")
    frame = pd.DataFrame({"a": [1, 2]})
    written = await store.store(frame, None, scope(out))
    assert await paths(store) == ["frame.pkl"]
    assert (await store.load(written.ref, None, None)).equals(frame)
    # Back to JSON: the JSON object shadows the pickle; a pickle write clears JSON.
    written = await store.store([1, 2], written.ref, scope(out))
    assert await store.load(written.ref, None, None) == [1, 2]
    written = await store.store({1: "int keys"}, written.ref, scope(out))
    assert await paths(store) == ["frame.pkl"]
    assert await store.load(written.ref, None, None) == {1: "int keys"}


async def test_partitions_and_keys_are_escaped_path_segments(store):
    out = Output("status", keyed=True)
    written = await store.store({"a/b": 1, "..": 2, "ü": 3}, None, scope(out, partition="site|x"))
    assert written.ref.handle == {"mode": "keyed", "path": "status/site%7Cx", "key": KEYS}
    assert await store.load(written.ref, None, None) == {"..": 2, "a/b": 1, "ü": 3}


async def test_a_keyed_output_is_one_object_per_key(store):
    out = Output("uploads", keyed=True)
    first = await store.store({"u-1": {"bytes": 3}, "u-2": {"bytes": 5}}, None, scope(out))
    assert await paths(store) == ["uploads/u-1.json", "uploads/u-2.json"]
    patched = await store.store(Patch({"u-3": {"bytes": 8}}, remove=["u-1"]), first.ref, scope(out))
    assert await store.load(patched.ref, None, None) == {"u-2": {"bytes": 5}, "u-3": {"bytes": 8}}
    assert await store.load(patched.ref, None, Keys({"u-3": ""})) == {"u-3": {"bytes": 8}}
    # A replacement with no word from the harness writes it all and drops the rest.
    replaced = await store.store({"u-9": 1}, patched.ref, scope(out))
    assert await paths(store) == ["uploads/u-9.json"]
    assert replaced.ref.version != patched.ref.version


async def test_a_keyed_write_touches_only_what_the_harness_says(store):
    out = Output("uploads", keyed=True)
    first = await store.store({"a": 1, "b": 2}, None, scope(out))
    content = {"a": 1, "b": 20, "c": 3}
    only = scope(out, upserts=frozenset({"b", "zzz"}), removes=frozenset({"a"}))
    written = await store.store(content, first.ref, only)
    # `c` is in the write but not in upserts: the harness knows it is there already.
    assert await store.load(written.ref, None, None) == {"b": 20}


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
    out = Output("files", key="id", revision="v")
    first = await store.store([{"id": 1, "v": "1"}, {"id": 2, "v": "1"}], None, scope(out))
    assert await paths(store) == ["files/1.json", "files/2.json"]
    patched = await store.store(Patch([{"id": 3, "v": "1"}], remove=[1]), first.ref, scope(out))
    assert await store.load(patched.ref, list[dict], None) == [{"id": 2, "v": "1"}, {"id": 3, "v": "1"}]
    frame = await store.load(patched.ref, pd.DataFrame, Keys({"3": "1"}))
    assert list(frame["id"]) == [3]
    with pytest.raises(WriteError, match="duplicate"):
        await store.store([{"id": 1}, {"id": 1}], None, scope(out))


async def test_unkeyed_incremental_is_one_object_per_batch(store):
    out = Output("events", incremental=True)
    with pytest.raises(WriteError, match="Patch"):
        await store.store([{"e": 0}], None, scope(out, batch=0))
    first = await store.store(Patch([{"e": 1}]), None, scope(out, batch=3))
    second = await store.store(Patch([{"e": 2}, {"e": 3}]), first.ref, scope(out, batch=4))
    assert second.ref.handle == {"mode": "batches", "path": "events", "batches": [3, 4]}
    assert await store.load(second.ref, None, None) == [{"e": 1}, {"e": 2}, {"e": 3}]
    assert await store.load(second.ref, None, Batches(4, 4)) == [{"e": 2}, {"e": 3}]
    assert (await store.store(Patch([]), second.ref, scope(out, batch=5))).ref is second.ref
    # A full run starts over: earlier batches go.
    reset = await store.store(Patch([{"e": 9}]), None, scope(out, batch=6))
    assert await paths(store) == ["events/000000000006.json"]
    assert await store.load(reset.ref, None, None) == [{"e": 9}]


async def test_a_partition_set_is_its_element_list(store):
    out = PartitionSet("sites")
    written = await store.store(["Richmond", "Perth"], None, scope(out))
    assert await store.load(written.ref, list, None) == ["Richmond", "Perth"]
    assert await store.load(written.ref, list, Keys({"Perth": "1"})) == ["Perth"]
    patched = await store.store(Patch(["Hobart"], remove=["Perth"]), written.ref, scope(out))
    assert await store.load(patched.ref, list, None) == ["Richmond", "Hobart"]


async def test_a_renamed_output_keeps_its_objects(store):
    written = await store.store({"a": 1}, None, scope(Output("old", keyed=True)))
    renamed = await store.store(Patch({"b": 2}), written.ref, scope(Output("new", keyed=True)))
    assert renamed.ref.handle["path"] == "old"
    assert await store.load(renamed.ref, None, None) == {"a": 1, "b": 2}


async def test_refs_round_trip_and_gone_values_fail(store):
    written = await store.store("x", None, scope(Output("v")))
    back = Ref.from_json(written.ref.to_json())
    assert type(back).__name__ == "ObjectRef" and back == written.ref
    import obstore

    await obstore.delete_async(store._objects(), "v.json")
    with pytest.raises(StoreError, match="gone"):
        await store.load(back, None, None)


def test_revision_is_a_16_byte_digest_of_the_encoding():
    import hashlib

    row = {"id": "a", "n": [1, 2]}
    assert revision(row) == hashlib.blake2b(b'{"id":"a","n":[1,2]}', digest_size=16).digest()
    assert revision(row) != revision({**row, "n": [2, 1]})


def test_the_default_path(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_DATA")
    store = FileStore()
    store.home = str(tmp_path)
    assert Path(store._objects().prefix) == tmp_path / ".solera" / "data"
    monkeypatch.setenv("SOLERA_DATA", str(tmp_path / "elsewhere"))
    assert Path(store._objects().prefix) == tmp_path / "elsewhere"
