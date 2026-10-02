"""Collection of immutable stores' data (docs/lifecycle.md §9.8): what a
commit, a compaction or an abandoned attempt let go of is discarded by the
scope's next attempt, once no reader can still need it."""

import random

from solera.keys.index import Options
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole


def engine_for(state, project, **kw):
    placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


async def run(engine, targets):
    detail = await engine.run_until((await engine.submit(targets))["id"], 20)
    assert detail["request"]["status"] == "succeeded", [t.get("error") for t in detail["tasks"]]
    return detail


def objects(data, output) -> set[str]:
    return {str(p.relative_to(data)).rsplit(".", 1)[0] for p in (data / output).rglob("*") if p.is_file()}


async def named(state, output) -> set[str]:
    """The objects the key index names: exactly what a keyed output should hold."""

    keys = await whole(state, output)
    return {FileStore.key_name(output, k, v, loc) for k, (v, loc) in keys.revisions.items()}


async def test_superseded_versions_go_with_the_next_attempt(tmp_path, data):
    """A commit lets go of each changed key's predecessor and of a value's
    previous object; the next attempt on the scope deletes them. A value is
    rewritten every run, so one superseded object always awaits the next."""

    values = iter([{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 2, "b": 1}])
    totals = iter([1, 2, 2])

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return next(values)

    @asset
    def total():
        return next(totals)

    project = Project(assets=[scores, total])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    for _ in range(2):
        await run(engine, ["scores", "total"])
    assert len(objects(data, "scores")) == 3  # a@1, a@2, b: the old `a` not yet discarded
    first, second = sorted(data.glob("total@*"), key=lambda p: int(p.stem.split("@")[1]))
    await run(engine, ["scores", "total"])  # unchanged, and it collects
    assert objects(data, "scores") == await named(state, "scores")
    assert not first.exists() and second.exists()
    assert len(list(data.glob("total@*"))) == 2
    assert ("scores", "") not in state.model.discards
    await engine.stop()
    await state.close()


async def test_an_abandoned_attempts_objects_go(tmp_path, data, monkeypatch):
    """An attempt dies after its first object landed: everything it wrote
    carries its generation, and its delta file names it all. The retry, the
    scope's next attempt, discards it and its delta file."""

    values = [{"a": 1, "b": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}, {"a": 2, "c": 1}]

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0))
    def scores():
        return values.pop(0)

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    await run(engine, ["scores"])
    put, puts = FileStore._put, []

    async def dying(self, base, value):
        await put(self, base, value)
        puts.append(base)
        if len(puts) == 1:
            raise OSError("the worker died")  # once one object landed

    monkeypatch.setattr(FileStore, "_put", dying)
    detail = await run(engine, ["scores"])
    first, _ = detail["attempts"][detail["tasks"][0]["id"]]
    assert first["status"] == "failed"
    assert not (data / f"{puts[0]}.json").exists()  # the retry collected it
    await run(engine, ["scores"])  # and what the retry's commit superseded goes next
    assert objects(data, "scores") == await named(state, "scores")
    prefix = state.model.indexes[("scores", "")].prefix
    await engine.upkeep.collect()
    left = {p.rsplit("/", 1)[-1] for p in await state.list_objects(prefix)}
    assert not any(first["id"] in name for name in left)  # its delta file went too
    await engine.stop()
    await state.close()


async def test_compaction_garbage_is_collected(tmp_path, data):
    """Compaction drops shadowed entries — some never named as predecessors
    (a key the filters cleared) — and lists them; with everything collected,
    the store holds exactly what the index names."""

    rng = random.Random(11)
    pending = {"rows": {}}

    @asset(outputs=Output("items", keyed=True))
    def items():
        return Patch(pending["rows"])

    @asset(inputs={"items": Incremental(batch_size=5)})
    def mirror(items: list):
        return []

    project = Project(assets=[items, mirror])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, key_options=Options(l0_max_files=3))
    await engine.initialize()
    compacted = 0
    for step in range(24):
        pending["rows"] = {f"k{rng.randrange(12)}": rng.randrange(4) for _ in range(4)}
        await run(engine, ["items"])
        if step % 6 == 5:
            await run(engine, ["mirror"])
        engine.upkeep.truncate()
        engine.upkeep.maintain()
        for job in list(engine.upkeep.jobs.values()):
            await job
        compacted += sum(1 for d in state.model.discards.get(("items", ""), []) if d["kind"] == "sidecar")
    assert compacted, "no compaction listed garbage"
    pending["rows"] = {}
    for _ in range(4):  # unchanged runs: each collects what is due
        await run(engine, ["items"])
    assert objects(data, "items") == await named(state, "items")
    await engine.upkeep.collect()
    sidecars = [
        p for p in await state.list_objects(state.model.indexes[("items", "")].prefix) if p.endswith(".kg")
    ]
    assert sidecars == []  # each went once its entries were discarded
    await engine.stop()
    await state.close()


async def test_a_reader_pin_holds_collection_back(tmp_path):
    """Data garbage is due only once every reader pin has passed it: the
    claims of attempts in flight, and a delta window paged over attempts."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    m = state.model
    m.discards[("scores", "")] = [{"n": 10, "kind": "items", "items": [["path", "x"]]}]
    m.claims["reader"] = {"attempt": "r", "pin": 9, "started_at": 0, "status": "running"}
    assert engine._due_discards("scores", "", "me") == []
    m.claims["reader"]["pin"] = 10
    m.watermarks[("c", "e", "")] = {"pin": 8}
    assert engine._due_discards("scores", "", "me") == []
    del m.watermarks[("c", "e", "")]
    assert [d["n"] for d in engine._due_discards("scores", "", "me")] == [10]
    assert [d["n"] for d in engine._due_discards("scores", "", "r")] == [10]  # its own claim reads none of it
    await state.close()
