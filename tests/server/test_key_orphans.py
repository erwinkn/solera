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


async def test_upkeep_merges_layers_and_reads_stay_exact(tmp_path, data):
    """Commits pile up deltas; upkeep merges them under the rule (lanes,
    attempts, publication), raises the cut to the head (no reader), and
    collects the inputs: the index still lists exactly the live keys."""

    import asyncio
    import random

    from solera.sdk import Output, Project, asset
    from solera.stores import Patch
    from solera_server.state import State

    from tests.server.test_collection import engine_for, run

    rng = random.Random(5)
    live: dict[str, int] = {}
    pending = {"rows": {}, "removes": []}

    @asset(outputs=Output("items", keyed=True))
    def items():
        return Patch(pending["rows"], remove=pending["removes"])

    project = Project(assets=[items])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    for _ in range(30):
        rows = {f"k{rng.randrange(40):02d}": rng.randrange(9) for _ in range(5)}
        removes = [k for k in rng.sample(sorted(live), min(2, len(live))) if k not in rows]
        pending["rows"], pending["removes"] = rows, removes
        await run(engine, ["items"])
        live.update(rows)
        for k in removes:
            live.pop(k)
    key = ("items", "")
    before = len(state.model.indexes[key].layers)
    for _ in range(50):
        engine.upkeep.maintain()
        if not len(engine.upkeep.jobs):
            break
        await asyncio.gather(*engine.upkeep.jobs.values(), return_exceptions=True)
    index = state.model.indexes[key]
    assert len(index.layers) < before, (before, len(index.layers))
    assert index.cut == index.head
    assert not engine.upkeep.failing, engine.upkeep.failing
    listed = await engine.list_keys("items", "", limit=100)
    assert sorted(listed["keys"]) == sorted(live) and listed["total"] == len(live)
    await engine.upkeep.collect()
    stored = set(await state.list_objects(index.prefix))
    assert {index.path(n) for n in index.referenced()} <= stored
    await engine.stop()
    await state.close()


async def test_a_delta_whose_index_is_read_or_intended_is_no_orphan(tmp_path, data):
    """A delta no claim holds is still kept while a reader holds its index (a
    source commit resolving) or a repair intent names it."""

    import math

    from solera.sdk import Output, Project, asset
    from solera_server.state import State

    from tests.server.test_collection import engine_for, run

    @asset(outputs=Output("items", keyed=True))
    def items():
        return {"a": 1}

    project = Project(assets=[items])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["items"])
    index = state.model.indexes[("items", "")]
    held, intended = index.path("000000000009-01HELD-0.lay"), index.path("000000000009-01INTENT-0.lay")
    import obstore

    for path in (held, intended):
        await obstore.put_async(state.objects, path, b"x")
    state.model.repairs[("items", "")] = [
        {
            "part": {
                "files": [
                    {"name": "000000000009-01INTENT-0.lay", "size": 1, "entries": 0, "first": "", "last": ""}
                ]
            }
        }
    ]

    async def orphans():
        engine.upkeep._orphans_at = -math.inf
        n = len(state.model.garbage)
        await engine.upkeep.collect_orphans()
        return {p for p, _ in state.model.garbage[n:]}

    with state.model.reading(index.prefix):
        assert held not in await orphans() and intended not in await orphans()
    assert held in await orphans()
    assert intended not in await orphans()
    state.model.repairs.pop(("items", ""))
    await engine.stop()
    await state.close()
