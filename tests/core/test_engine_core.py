"""The engine on the new tracking core (build plan B5): the seams a contract
test cannot see — what the journal holds, what a restart rebuilds, and the
commit check — driven through the contract's harness on the core's path."""

import json
from base64 import b64decode

import pytest
from solera_server.state import Conflict

from tests.contract.example import Example
from tests.contract.harness import Solera

KEYS = {f"unique-key-{n:03d}": "1" for n in range(40)}


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, "core")
    yield namespace
    await namespace.close()


def _recording(solera) -> list[dict]:
    """Every event the engine records from now on."""

    recorded, record = [], solera.state.record

    def spy(*events, **kw):
        recorded.extend(events)
        return record(*events, **kw)

    solera.state.record = spy
    return recorded


def _keys_named(event: dict) -> set[str]:
    """The keys of `KEYS` an event names: in its JSON, or in a core event's bytes."""

    text = json.dumps(event)
    core = (event.get("commit") or {}).get("core") or {}
    if core.get("event"):
        text += b64decode(core["event"]).decode("latin-1")
    return {k for k in KEYS if k in text}


async def test_a_batch_commits_journal_event_names_no_key_but_bounds(solera):
    ex = await Example(solera).start(batch_size=16)
    await ex.feed(KEYS, items=False)
    recorded = _recording(solera)
    batches = await ex.run("items")
    assert len(batches) == 3
    commits = [e for e in recorded if e["type"] == "AttemptFinished" and e.get("commit")]
    assert len(commits) == 3 and all("core" in e["commit"] for e in commits)
    for event in commits:
        # Of a batch's 16 keys, only bounds: its range's, and its output layer's first and last.
        assert len(_keys_named(event)) <= 4, _keys_named(event)


async def test_a_restart_rebuilds_the_cores_state_from_the_journal(solera):
    ex = await Example(solera).start()
    await ex.feed(KEYS)
    before = (
        solera.engine.m.core.snapshot(),
        dict(solera.engine.m.core_outputs),
        dict(solera.engine.m.core_edges),
    )
    await solera.restart()
    after = (
        solera.engine.m.core.snapshot(),
        dict(solera.engine.m.core_outputs),
        dict(solera.engine.m.core_edges),
    )
    assert after == before
    await ex.feed({"unique-key-000": "2"}, items=False)
    [batch] = await ex.run("items")
    assert batch.classes == ([], ["unique-key-000"], [])


async def test_the_commit_check_refuses_a_batch_its_inputs_progress_moved_past(solera):
    ex = await Example(solera).start(batch_size=16)
    await ex.feed(KEYS, items=False)
    recorded = _recording(solera)
    await ex.run("items")
    first = next(e for e in recorded if e["type"] == "AttemptFinished" and e.get("commit"))
    event = b64decode(first["commit"]["core"]["event"])
    with pytest.raises(Conflict):
        solera.engine.core.check(event)


async def test_a_today_namespace_and_a_core_one_refuse_each_other(tmp_path):
    from solera_server.journal import OldNamespace

    today = Solera(tmp_path, "today")
    ex = await Example(today).start()
    await ex.feed({"k1": "1"})
    await today.close()
    core = Solera(tmp_path, "core")
    with pytest.raises(OldNamespace):
        await Example(core).start()
    await core.close()


async def _attempt_requests(tmp_path, path: str) -> dict:
    """The state store's requests, by method, while one keyed incremental
    attempt of 40 keys runs and commits, on a bucket."""

    solera = Solera(tmp_path / path, path)
    storage = solera.storage_server()
    try:
        ex = await Example(solera).start(batch_size=100)
        await ex.feed(KEYS, items=False)
        await solera.settle(0.2)
        before = dict(storage.methods)
        [batch] = await ex.run("items")
        assert len(batch.added) == len(KEYS)
        return {m: n - before.get(m, 0) for m, n in storage.methods.items()}
    finally:
        await solera.close()
        storage.close()


async def test_n13_an_attempt_costs_todays_requests_and_at_most_its_layer_more(tmp_path):
    """N13: outside the index, an attempt's requests are today's, plus at
    most the commit's layer (its changed keys fit the result)."""

    today = await _attempt_requests(tmp_path, "today")
    core = await _attempt_requests(tmp_path, "core")
    assert core.get("PUT", 0) <= today.get("PUT", 0) + 1, (today, core)
    assert core.get("GET", 0) <= today.get("GET", 0), (today, core)


async def test_changed_keys_too_big_for_the_result_travel_as_a_file(solera, monkeypatch):
    """N13's other PUT: a write whose changed keys pass `CHANGES_INLINE` puts
    them in one file beside the attempt's control file, which the engine
    reads to build the commit. The worker encodes it a chunk of keys at a
    time, removals joining the chunk whose keys they fall among."""

    from solera_worker import batch

    monkeypatch.setattr(batch, "CHANGES_INLINE", 0)
    monkeypatch.setattr(batch, "CHANGES_CHUNK", 7)
    ex = await Example(solera).start(batch_size=100)
    recorded = _recording(solera)
    await ex.feed(KEYS)
    assert await solera.rows("items") == KEYS
    [finished] = [e for e in recorded if e["type"] == "AttemptFinished" and e.get("commit")]
    assert finished["commit"]["core"]["counts"]["items"]["added"] == len(KEYS)
    found = await solera.state.list_objects("")
    assert any(path.endswith("changes-items") for path in found)
    names = sorted(KEYS)
    updated, removed = {k: "2" for k in names[::3]}, names[1::3]
    await ex.feed(updated, removes=removed)
    last = [e for e in recorded if e["type"] == "AttemptFinished" and e.get("commit")][-1]
    counts = last["commit"]["core"]["counts"]["items"]
    assert (counts["updated"], counts["removed"]) == (len(updated), len(removed))
    assert await solera.rows("items") == {k: updated.get(k, v) for k, v in KEYS.items() if k not in removed}


def test_a_removal_heavy_write_is_encoded_a_chunk_of_changes_at_a_time(monkeypatch):
    import tempfile
    from types import SimpleNamespace

    from solera.sdk import Output
    from solera.stores import Patch, prepare
    from solera_worker import batch

    output = Output("items", key="id")
    prepared = prepare(Patch([{"id": "k050"}], remove=[f"k{i:03d}" for i in range(100)]), output)
    calls, encode = [], batch.encode_changes

    def counted(upserts, removes=()):
        calls.append(len(upserts) + len(removes))
        return encode(upserts, removes)

    monkeypatch.setattr(batch, "CHANGES_CHUNK", 7)
    monkeypatch.setattr(batch, "encode_changes", counted)
    with tempfile.TemporaryFile() as f:
        batch._encode(SimpleNamespace(output=output, prepared=prepared, replace=False), f)
    assert max(calls) <= 7 and sum(calls) == 100, calls


def test_a_commit_takes_its_changes_as_bytes_or_a_read_only_map_only(tmp_path):
    import mmap

    from solera._native import ChangeIndex, Tracking, encode_changes

    tracking = Tracking()
    tracking.attach(b"items", ChangeIndex(tmp_path.as_uri()))
    data = encode_changes(["k"])
    with pytest.raises(TypeError):  # read-only, but over memory Python may still change
        tracking.build_commit(b"items", memoryview(bytearray(data)).toreadonly(), version=1, path="a.lay")
    path = tmp_path / "changes"
    path.write_bytes(data)
    with path.open("rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        assert tracking.build_commit(b"items", mapped, version=1, path="b.lay")["added"] == 1

    class Exporting(mmap.mmap):  # a subclass may export any buffer, a mutable one's too
        def __buffer__(self, flags):
            return memoryview(bytearray(data)).toreadonly()

    with path.open("rb") as f, Exporting(f.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        with pytest.raises(TypeError):
            tracking.build_commit(b"items", mapped, version=2, path="c.lay")


async def test_a_launch_journals_no_layer_of_its_outputs(solera):
    ex = await Example(solera).start(batch_size=1)
    recorded = _recording(solera)
    await ex.feed({f"k{i:03d}": "1" for i in range(4)})
    launches = [e for e in recorded if e["type"] == "AttemptLaunched"]
    assert launches and not any(
        "core" in info for e in launches for info in e["prepared"]["outputs"].values()
    )
