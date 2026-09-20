"""§3/§4: JsonStore — content-addressed writes, Patch, batches, Keys loads."""

import pytest
from cursus.sdk import Output, PartitionSet, Ref, digest
from cursus.stores import JsonStore, Keys, Patch, WriteError

from tests.conftest import scope


async def test_bare_replace_version_is_content_addressed(json_store):
    """§3: a bare value's version is H(payload); equal content is equal version."""

    out = Output("t")
    first = await json_store.store([{"a": 1}], None, scope(out))
    second = await json_store.store([{"a": 1}], first.ref, scope(out, prior_keys=None))
    assert first.ref.version == second.ref.version
    assert second.ref.version == digest([{"a": 1}])
    third = await json_store.store([{"a": 2}], first.ref, scope(out))
    assert third.ref.version != first.ref.version


async def test_patch_merges_into_complete_key_map(json_store):
    """§4: Patch returns the complete map merged with prior_keys."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    assert first.keys == {"a": "1", "b": "1"}
    second = await json_store.store(
        Patch([{"id": "b", "v": "2"}, {"id": "c", "v": "1"}]),
        first.ref,
        scope(out, prior_keys=first.keys),
    )
    assert second.keys == {"a": "1", "b": "2", "c": "1"}
    assert second.ref.version == digest([first.ref.version, digest({"rows": second_rows(), "remove": []})])


def second_rows():
    return [{"id": "b", "v": "2"}, {"id": "c", "v": "1"}]


async def test_patch_remove_drops_keys(json_store):
    """§4: keys in remove leave the map; rows stay consistent."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(Patch([{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]), None, scope(out))
    second = await json_store.store(Patch([], remove=["a"]), first.ref, scope(out, prior_keys=first.keys))
    assert second.keys == {"b": "1"}
    loaded = await json_store.load(second.ref, list[dict], None)
    assert [r["id"] for r in loaded] == ["b"]


async def test_patch_duplicate_keys_are_a_write_error(json_store):
    """§4: duplicate keys in one write are an error."""

    out = Output("t", key="id")
    with pytest.raises(WriteError):
        await json_store.store(Patch([{"id": "a"}, {"id": "a"}]), None, scope(out))


async def test_empty_patch_keeps_prior_version(json_store):
    """§3: a no-op write leaves the prior ref unchanged."""

    out = Output("t", key="id", revision="v")
    first = await json_store.store(Patch([{"id": "a", "v": "1"}]), None, scope(out))
    same = await json_store.store(Patch([], remove=[]), first.ref, scope(out, prior_keys=first.keys))
    assert same.ref is first.ref
    assert same.keys == first.keys


async def test_append_batches_number_from_prior_keys(json_store):
    """§4: an append output's keys are engine-numbered batches from prior_keys."""

    out = Output("t", mode="append")
    first = await json_store.store(Patch([{"e": 1}]), None, scope(out))
    assert first.keys == {"0": digest([{"e": 1}])}
    second = await json_store.store(Patch([{"e": 2}]), first.ref, scope(out, prior_keys=first.keys))
    assert second.keys == {"0": digest([{"e": 1}]), "1": digest([{"e": 2}])}
    # A load at the first ref returns only its batch (snapshot at the version).
    at_first = await json_store.load(first.ref, list[dict], None)
    assert at_first == [{"e": 1}]
    at_second = await json_store.load(second.ref, list[dict], None)
    assert at_second == [{"e": 1}, {"e": 2}]


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
    """§2/§7: a PartitionSet's elements are its own keys, revision = presence."""

    out = PartitionSet("sites")
    written = await json_store.store(["Richmond", "Perth"], None, scope(out))
    assert written.keys == {"Richmond": "1", "Perth": "1"}
    assert await json_store.load(written.ref, list, None) == ["Richmond", "Perth"]
    selected = await json_store.load(written.ref, list, Keys({"Perth": "1"}))
    assert selected == ["Perth"]


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
