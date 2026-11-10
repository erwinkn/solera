"""The engine on the new tracking core (build plan B5) where commits race:
two source commits, a load's report and a newer commit, a sensor tick and
the commits made while it is validated, a redeploy while a batch's commit
is built. Each interleaving is forced by wrapping the call it races."""

import asyncio

import pytest
from solera.sdk import Commit, Incremental, Output, Project, Retry, Source, Tick, asset, sensor
from solera.stores import FileStore, Patch
from solera_server.state import Conflict
from solera_server.tracking import sources
from solera_server.tracking.sources import record_loads

from tests.contract.harness import Outside, Refused, Solera, SourceStore
from tests.contract.test_eng import sensed_project


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, "core")
    yield namespace
    await namespace.close()


async def _state(engine, name: str) -> dict:
    output = engine.m.core_output(name, "")
    rows, _ = await engine.core.run(engine.core.index.scan, output["layers"], output["head"])
    return {row[0]: row[2] for row in rows}


async def test_a_refused_source_commit_leaves_the_accepted_commits_layer_as_written(solera, monkeypatch):
    await solera.deploy(Project(sources=[Source("feed", key="id")]))
    engine = solera.engine
    await engine.commit_source("feed", upsert={"k": "v1"})
    run = engine.core.run
    first_built, second_built, first_recorded = (asyncio.Event() for _ in range(3))
    calls = 0

    async def interleaved(fn, *args, **kwargs):
        nonlocal calls
        if fn != engine.core.tracking.build_commit:
            return await run(fn, *args, **kwargs)
        calls += 1
        mine = calls
        built = await run(fn, *args, **kwargs)
        if mine == 1:  # both build at the same head before either records
            first_built.set()
            await asyncio.wait_for(second_built.wait(), 10)
        elif mine == 2:
            second_built.set()
            await first_recorded.wait()
        return built

    monkeypatch.setattr(engine.core, "run", interleaved)
    first = asyncio.create_task(engine.commit_source("feed", upsert={"k": "v2"}))
    await asyncio.wait_for(first_built.wait(), 10)
    second = asyncio.create_task(engine.commit_source("feed", upsert={"k": "v3"}))
    try:
        accepted = await asyncio.wait_for(asyncio.shield(first), 10)
    finally:
        first_recorded.set()
    [other] = await asyncio.wait_for(asyncio.gather(second, return_exceptions=True), 10)
    assert accepted["changed"]
    assert not isinstance(other, BaseException) or isinstance(other, Conflict)
    rows, _ = await run(engine.core.index.scan, engine.m.core_output("feed", "")["layers"], 2)
    assert {row[0]: row[2] for row in rows} == {"k": "v2"}


async def test_a_loads_report_never_overwrites_a_source_commit_made_since_the_batchs_head(
    solera, monkeypatch
):
    await solera.deploy(Project(sources=[Source("feed", key="id")]))
    engine = solera.engine
    await engine.commit_source("feed", upsert={"k": "v1"})  # the batch's head: commit 1
    commit, newer = engine.commit_source, False

    async def interleaved(name, **kwargs):
        nonlocal newer
        if not newer:  # the load's report is about to be committed
            newer = True
            await commit("feed", upsert={"k": "v3"}, by="newer client")
        return await commit(name, **kwargs)

    monkeypatch.setattr(engine, "commit_source", interleaved)
    plans = {"plans": {"feed": {"kind": "core", "output": "feed", "head": 1}}}
    await record_loads(engine, "old-attempt", plans, {"delivered": {"feed": {"loaded": [["k", "v2"]]}}})
    assert newer
    assert await _state(engine, "feed") == {"k": "v3"}


async def test_a_finished_keyed_input_is_delivered_empty_while_unkeyed_inputs_go_on(solera, tmp_path):
    outside, seen = Outside(), []
    outside.feed.update({"k": "v1"})

    @asset(outputs=Output("log", incremental=True))
    def log():
        return Patch([{"v": 1}])

    @asset(inputs={"feed": Incremental(batch_size=1), "log": Incremental(batch_size=1)}, retries=Retry(n=0))
    def consumer(ctx, feed: list, log: list):
        seen.append((len(feed), len(log)))
        return len(log)

    await solera.deploy(
        Project(
            assets=[log, consumer],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(tmp_path / "ext", outside)},
            default_store=FileStore(tmp_path / "data"),
        )
    )
    await solera.commit_source("feed", upsert={"k": "v1"})
    for _ in range(3):
        await solera.run("log")
    detail = await solera.run("consumer", expect=None)
    errors = [attempt.get("error") for attempts in detail["attempts"].values() for attempt in attempts]
    assert detail["request"]["status"] == "succeeded", errors
    assert seen == [(1, 1), (0, 1), (0, 1)]


async def test_a_tick_is_refused_when_a_key_it_reports_moves_while_it_is_validated(solera, monkeypatch):
    await solera.deploy(sensed_project())
    await solera.commit_source("uploads", upsert={"a": "1", "z": "1"})
    [tick] = await solera.sensor_ticks()
    await solera.commit_source("uploads", upsert={"z": "2"})
    moved, committed = sources.moved, False

    async def interleaved(*args, **kwargs):
        nonlocal committed
        result = await moved(*args, **kwargs)
        if not committed:
            committed = True
            await solera.engine.commit_source("uploads", upsert={"a": "3"})
        return result

    monkeypatch.setattr(sources, "moved", interleaved)
    with pytest.raises(Refused) as refused:
        await solera.sensor_post(tick, Tick(cursor="c1", commits=[Commit("uploads", upsert={"a": "2"})]))
    assert refused.value.status == 409
    assert await _state(solera.engine, "uploads") == {"a": "3", "z": "2"}


async def test_a_tick_is_refused_when_an_unkeyed_source_it_commits_moved_since_dispatch(solera):
    @sensor(every=1, commits=["knob"])
    def watch(ctx):
        return None

    await solera.deploy(Project(sources=[Source("knob")], sensors=[watch]))
    await solera.commit_source("knob", version="v1")
    [tick] = await solera.sensor_ticks()
    await solera.commit_source("knob", version="v3")
    with pytest.raises(Refused) as refused:
        await solera.sensor_post(tick, Tick(cursor="c1", commits=[Commit("knob", version="v2")]))
    assert refused.value.status == 409
    assert (await solera.head("knob"))["version"] == "v3"


async def test_a_tick_is_refused_when_a_key_it_reports_was_added_and_removed_since_dispatch(solera):
    await solera.deploy(sensed_project())
    await solera.commit_source("uploads", upsert={"z": "1"})
    [tick] = await solera.sensor_ticks()
    await solera.commit_source("uploads", upsert={"a": "new"})
    await solera.commit_source("uploads", remove=["a"])
    with pytest.raises(Refused) as refused:
        await solera.sensor_post(tick, Tick(cursor="c1", commits=[Commit("uploads", upsert={"a": "old"})]))
    assert refused.value.status == 409
    assert await solera.keys("uploads") == {"z": "1"}


async def test_a_redeploy_while_a_batchs_commit_is_built_refuses_the_old_definitions_commit(
    solera, tmp_path, monkeypatch
):
    def project(version):
        @asset(version=version, outputs=Output("copy", key="id"))
        def copy():
            return [{"id": "k", "v": "1"}]

        return Project(assets=[copy], default_store=FileStore(tmp_path / "data"))

    await solera.deploy(project("1"))
    engine = solera.engine
    run, record, redeployed, installed = engine.core.run, solera.state.record, False, []

    async def interleaved(fn, *args, **kwargs):
        nonlocal redeployed
        built = await run(fn, *args, **kwargs)
        if fn == engine.core.tracking.build_commit and not redeployed:
            redeployed = True
            engine.redeploy(project("2").manifest)
        return built

    def capture(*events, **kwargs):
        for event in events:
            head = ((event.get("commit") or {}).get("heads") or {}).get("copy")
            if redeployed and event["type"] == "AttemptFinished" and head is not None:
                installed.append(head["version"])
        return record(*events, **kwargs)

    monkeypatch.setattr(engine.core, "run", interleaved)
    monkeypatch.setattr(solera.state, "record", capture)
    await solera.run("copy", expect=None)
    assert redeployed
    assert "1" not in installed, installed
