"""Index files nothing names (docs/key-index-design.md § Lifecycles): what
the orphan collector judges a commit's delta by, and the event that makes
an orphan garbage."""

from __future__ import annotations

from solera_server.model import Model
from solera_server.upkeep import _attempt_of


def test_a_delta_names_its_attempt():
    assert _attempt_of("000000000012-01J8ZD3Q-0.lay") == "01J8ZD3Q"
    assert _attempt_of("000000000012-01J8ZD3Q-17.lay") == "01J8ZD3Q"
    assert _attempt_of("000000000012-01J8ZD3Q.lix") == "01J8ZD3Q"
    for other in (
        "l000000000000-000000000056-e3-01J8ZE2-m0.lay",  # a merge output: its epoch decides
        "000000000012-01J8ZD3Q.0000",  # a span-era delta: not a layer file
        "12-01J8ZD3Q-0.lay",  # not a commit number
        "000000000012-01J8ZD3Q-x.lay",
        "000000000012--0.lay",
    ):
        assert _attempt_of(other) is None, other


def test_orphans_become_garbage_once():
    m = Model()
    m.garbage = [["keys/o/_/a.lay", 3]]
    m.apply({"type": "OrphansFound", "paths": ["keys/o/_/a.lay", "keys/o/_/b.lay"]})
    paths = [p for p, _ in m.garbage]
    assert paths == ["keys/o/_/a.lay", "keys/o/_/b.lay"]  # a.lay kept its own event counter
    assert m.garbage[0][1] == 3


async def test_an_abandoned_attempts_delta_is_collected_by_the_engine(tmp_path, data, monkeypatch):
    """An attempt uploads its delta, then dies writing its store. No cleanup
    task is involved (Q3): its store cleanup is not even due yet (its
    asset's timeout is long), while the engine's orphan collector finds the
    delta — its attempt holds no claim, nothing names it — journals it as
    garbage, and deletes it once no pin predates that."""

    import math

    from solera.sdk import Output, Project, Retry, asset
    from solera.stores import FileStore
    from solera_server.state import State
    from tests.server.test_collection import engine_for, run

    values = [{"a": 1, "b": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}]

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0), timeout=3600)
    def scores():
        return values.pop(0)

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, cancel_grace=0)
    await engine.initialize()
    await run(engine, ["scores"])
    put = FileStore._put

    async def dying(self, base, value):
        raise OSError("the worker died")  # its delta is uploaded; no object lands

    monkeypatch.setattr(FileStore, "_put", dying)
    detail = await engine.run_until((await engine.submit(["scores"]))["id"], 20)
    monkeypatch.setattr(FileStore, "_put", put)
    dead = [a for a in detail["attempts"][detail["tasks"][0]["id"]] if a["outcome"] == "failed"]
    assert dead, detail["attempts"]
    prefix = state.model.indexes[("scores", "")].prefix

    async def deltas_of(attempt):
        return [p for p in await state.list_objects(prefix) if f"-{attempt}-" in p or f"-{attempt}." in p]

    attempt = dead[0]["id"]
    assert await deltas_of(attempt), "the dead attempt uploaded no delta"
    pending = [d for ds in state.model.cleanups.values() for d in ds if d.get("attempt") == attempt]
    assert all(d["kind"] == "abandoned" and d["after"] > state.clock() for d in pending)  # not due
    engine.upkeep._orphans_at = -math.inf
    await engine.upkeep.collect_orphans()
    await engine.upkeep.collect()
    assert not await deltas_of(attempt)
    assert [d for ds in state.model.cleanups.values() for d in ds if d.get("attempt") == attempt] == pending
    await engine.stop()
    await state.close()
