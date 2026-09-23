"""§4/§9: JsonStore — content-addressed values, per-batch incremental writes,
Patch, Keys and Batches loads. The store never works out what changed; the
harness does, against the key index (tests/worker)."""

import pytest
from cursus.sdk import Output, PartitionSet, Ref, digest
from cursus.stores import Batches, JsonStore, Keys, Patch, WriteError

from tests.conftest import scope


async def test_bare_replace_version_is_content_addressed(json_store):
    """§3: a bare value's version is H(payload); equal content is equal version."""

    out = Output("t")
    first = await json_store.store([{"a": 1}], None, scope(out))
    second = await json_store.store([{"a": 1}], first.ref, scope(out))
    assert first.ref.version == second.ref.version
    assert second.ref.version == digest([{"a": 1}])
    third = await json_store.store([{"a": 2}], first.ref, scope(out))
    assert third.ref.version != first.ref.version


async def test_patch_extends_the_batch_window(json_store):
    """§4: a keyed Patch writes one batch object; loads fold the window."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(
        Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out, batch=0)
    )
    assert first.ref.handle["batches"] == [0, 0]
    rows = [{"id": "b", "v": "2"}, {"id": "c", "v": "1"}]
    second = await json_store.store(Patch(rows), first.ref, scope(out, batch=1))
    assert second.ref.handle["batches"] == [0, 1]
    loaded = await json_store.load(second.ref, list[dict], None)
    assert {r["id"]: r["v"] for r in loaded} == {"a": "1", "b": "2", "c": "1"}
    assert second.ref.version == digest([first.ref.version, digest({"rows": rows, "remove": []})])
    assert second.keys is None  # the harness derives keys from the rows


async def test_patch_remove_drops_keys(json_store):
    """§4: keys in remove leave the loaded content."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    second = await json_store.store(Patch([], remove=["a"]), first.ref, scope(out))
    loaded = await json_store.load(second.ref, list[dict], None)
    assert [r["id"] for r in loaded] == ["b"]


async def test_replacement_starts_the_window_over(json_store):
    """§4: a replacement (or a Patch with no prior) is a reset batch."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out, batch=0))
    second = await json_store.store(Patch([{"id": "b", "v": "1"}]), first.ref, scope(out, batch=1))
    third = await json_store.store([{"id": "c", "v": "1"}], second.ref, scope(out, batch=2))
    assert third.ref.handle["batches"] == [2, 2]
    assert [r["id"] for r in await json_store.load(third.ref, list[dict], None)] == ["c"]


async def test_snapshots_bound_the_fold(json_store):
    """§4: every `snapshot_every` batches a snapshot lets loads fold a tail."""

    store = JsonStore(snapshot_every=4)
    store.bind_objects(json_store._objects)
    out = Output("t", key="id", revision="v")
    ref = None
    for b in range(10):
        ref = (await store.store(Patch([{"id": f"k{b}", "v": "1"}]), ref, scope(out, batch=b))).ref
    assert ref.handle["snapshot"] == 7  # snapshots at batches 3 and 7
    assert len(await store.load(ref, list[dict], None)) == 10


async def test_patch_duplicate_keys_are_a_write_error(json_store):
    """§4: duplicate keys in one write are an error."""

    out = Output("t", key="id")
    with pytest.raises(WriteError):
        await json_store.store(Patch([{"id": "a"}, {"id": "a"}]), None, scope(out))


async def test_empty_patch_keeps_prior_version(json_store):
    """§3: an empty batch-mode Patch leaves the prior ref unchanged."""

    out = Output("t", incremental=True)
    first = await json_store.store(Patch([{"e": 1}]), None, scope(out, batch=0))
    same = await json_store.store(Patch([]), first.ref, scope(out, batch=1))
    assert same.ref is first.ref


async def test_unkeyed_incremental_batches(json_store):
    """§2.1: an unkeyed incremental output writes one object per engine
    batch; loads fold a batch range."""

    out = Output("t", incremental=True)
    first = await json_store.store(Patch([{"e": 1}]), None, scope(out, batch=0))
    second = await json_store.store(Patch([{"e": 2}]), first.ref, scope(out, batch=1))
    assert second.ref.handle["batches"] == [0, 1]
    # A load at the first ref returns only its batch (snapshot at the version).
    at_first = await json_store.load(first.ref, list[dict], None)
    assert at_first == [{"e": 1}]
    at_second = await json_store.load(second.ref, list[dict], None)
    assert at_second == [{"e": 1}, {"e": 2}]
    # Batches selections bound the fold.
    only_second = await json_store.load(second.ref, list[dict], Batches(1, 1))
    assert only_second == [{"e": 2}]


async def test_load_with_keys_selection(json_store):
    """§4/§6: a Keys selection returns only those keys' rows."""

    out = Output("t", key="id", revision="v")
    written = await json_store.store(
        Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "2"}, {"id": "c", "v": "3"}]),
        None,
        scope(out),
    )
    selected = await json_store.load(written.ref, list[dict], Keys({"b": "2", "c": "3"}))
    assert [r["id"] for r in selected] == ["b", "c"]


async def test_partition_set_store_and_load(json_store):
    """§2/§7: a PartitionSet's payload is its element list; Keys selects elements."""

    out = PartitionSet("sites")
    written = await json_store.store(["Richmond", "Perth"], None, scope(out))
    assert await json_store.load(written.ref, list, None) == ["Richmond", "Perth"]
    selected = await json_store.load(written.ref, list, Keys({"Perth": "1"}))
    assert selected == ["Perth"]
    patched = await json_store.store(Patch(["Hobart"], remove=["Perth"]), written.ref, scope(out))
    assert await json_store.load(patched.ref, list, None) == ["Richmond", "Hobart"]


async def test_jsonref_round_trip():
    """§3: refs are JSON round-trippable with their subclass restored."""

    ref = Ref(output="o", store="json", handle={"object": "x"}, version="v", partition="p")
    back = Ref.from_json(ref.to_json())
    assert back == ref and type(back) is Ref

    out = Output("t", key="id")
    import tempfile

    from obstore.store import LocalStore

    store = JsonStore()
    store.bind_objects(LocalStore(tempfile.mkdtemp()))
    written = await store.store([{"id": "a"}], None, scope(out))
    restored = Ref.from_json(written.ref.to_json())
    assert type(restored).__name__ == "JsonRef"
