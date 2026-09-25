"""§4/§9: JsonStore — content-addressed values, per-batch incremental writes,
Patch, Keys and Batches loads. The store never works out what changed; the
harness does, against the key index (tests/worker)."""

import pytest
from solera.sdk import Output, PartitionSet, Ref, digest
from solera.stores import Batches, JsonStore, Keys, Patch, WriteError

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


def at(output, t, batch=None, partition=""):
    """A write scope whose attempt id dates its objects at time `t`."""

    from solera.ids import ulid
    from solera.stores import Scope

    return Scope(output=output, partition=partition, batch=batch, attempt=ulid(t))


async def _paths(objects, prefix):
    import obstore

    out = []
    async for chunk in obstore.list(objects, prefix=prefix):
        out += [m["path"] for m in chunk]
    return sorted(out)


async def test_expire_keeps_every_version_after_the_horizon(json_store, objects):
    """§9/§11: keyed batches below the snapshot the oldest retained version
    folds from are deleted; every version after `before` still loads."""

    store = JsonStore(snapshot_every=4)
    store.bind_objects(objects)
    out = Output("t", key="id", revision="v")
    refs = []
    for b in range(12):
        prior = refs[-1] if refs else None
        refs.append((await store.store(Patch([{"id": f"k{b}", "v": "1"}]), prior, at(out, 1000 + b, b))).ref)
    await store.expire(refs[-1], before=1009)  # versions 9, 10, 11 must still load
    names = [p.rsplit("/", 1)[-1][:13] for p in await _paths(objects, "data/t/")]
    assert names == [f"b{b:012d}" for b in range(7, 12)] + ["s000000000007", "s000000000011"]
    for b in (9, 10, 11):
        assert len(await store.load(refs[b], list[dict], None)) == b + 1


async def test_expire_drops_old_batches_of_an_event_log(json_store, objects):
    out = Output("t", incremental=True)
    ref = None
    for b in range(5):
        ref = (await json_store.store(Patch([{"e": b}]), ref, at(out, 1000 + b, b))).ref
    await json_store.expire(ref, before=1003)
    assert await json_store.load(ref, list[dict], None) == [{"e": 3}, {"e": 4}]


async def test_expire_values_and_uncommitted_attempts(json_store, objects):
    out = Output("t")
    first = (await json_store.store({"v": 1}, None, at(out, 1000))).ref
    second = (await json_store.store({"v": 2}, first, at(out, 2000))).ref
    await json_store.expire(second, before=1500)
    assert await _paths(objects, "data/t/") == [second.handle["object"]]
    # Two attempts wrote batch 0; the newer one is committed. The older is
    # dropped once it is past the horizon.
    log = Output("log", incremental=True)
    await json_store.store(Patch([{"e": "lost"}]), None, at(log, 1000, 0))
    kept = (await json_store.store(Patch([{"e": "won"}]), None, at(log, 1001, 0))).ref
    assert await json_store.load(kept, list[dict], None) == [{"e": "won"}]
    await json_store.expire(kept, before=1000.5)
    assert len(await _paths(objects, "data/log/")) == 1
