"""Full runs on the engine's new tracking core (B5, D205): the gate on every
reader of an incomplete upstream, how a full run ends, what a batch holds
back, and the operator's `proceed`."""

import asyncio

import pytest
from solera.sdk import In, Incremental, Output, Project, Source, StaticPartitions, asset, canonical_partition
from solera.stores import FileStore, Patch
from solera_server.planning import Planner
from solera_server.state import State
from solera_server.tracking import full

from tests.contract.example import Example
from tests.contract.harness import Outside, Refused, Solera, SourceStore
from tests.contract.test_run import hold
from tests.contract.test_sta import INCOMPLETE, Files, decided


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, "core")
    yield namespace
    await namespace.close()


async def test_a_whole_reader_waits_for_an_unkeyed_output_mid_full_run(solera):
    ex = Example(solera)
    project = ex.project(batch_size=1)
    seen = []

    @asset(inputs={"tally": In()})
    def report(tally: dict):
        seen.append(tally["rows"])
        return tally

    await solera.deploy(
        Project(
            assets=[*project.assets.values(), report],
            sources=list(project.sources.values()),
            stores=project.stores,
        )
    )
    await ex.factor("w1")
    await ex.feed({"a": "1", "b": "1", "c": "1"})
    await ex.run("tally")
    begun, release = hold(ex, batch=1)
    rebuild = await solera.submit("tally", mode="full")
    await solera.until(begun.is_set)
    try:
        reader = await solera.submit("report")
        task = await decided(solera, reader, "report")
        assert task.get("held") == [INCOMPLETE, "tally"] and seen == [], (task, seen)
    finally:
        release.set()
        await solera.wait(rebuild)
    await solera.wait(reader)
    assert seen == [3]


async def test_a_reader_waits_for_a_first_keyed_run_not_walked_to_the_end(solera):
    ex = await Example(solera).start(batch_size=1)
    await ex.feed({"a": "1", "b": "1", "c": "1"})
    eng = solera.engine
    run, entered, release, calls = eng.core.run, asyncio.Event(), asyncio.Event(), [0]

    async def second_build_waits(fn, *args, **kwargs):
        result = await run(fn, *args, **kwargs)
        if fn == eng.core.tracking.build_commit:
            calls[0] += 1
            if calls[0] == 2:
                entered.set()
                await release.wait()
        return result

    eng.core.run = second_build_waits
    first = await solera.submit("copy")
    await solera.until(entered.is_set)
    try:
        assert await solera.rows("copy") == {"a": "1"}
        await eng._cover_tick()
        assert ("copy", "") in full.incomplete(eng)
    finally:
        release.set()
        await solera.wait(first)
    await eng._cover_tick()
    assert full.incomplete(eng) == set()


@pytest.mark.parametrize("lists", [[["a", "b"]], [["a"], ["b"]]])
async def test_key_lists_that_take_every_key_end_a_full_run(solera, lists):
    files = await Files(solera, {"a": 1, "b": 1}).deploy()
    await solera.run("files")
    await solera.run("copy")
    await files.deploy(version="2")
    for keys in lists:
        await solera.run("copy", keys={"files": keys})
    await solera.settle(0.1)
    assert await solera.engine.complete("copy", "")
    assert full.incomplete(solera.engine) == set() and full.running(solera.engine.core) == set()


def test_a_fan_in_waits_only_for_the_partitions_its_shared_keys_name(tmp_path):
    days = {"site": StaticPartitions(["east", "west"]), "day": StaticPartitions(["one", "two"])}

    @asset(partitions=days, outputs=Output("items", key="id"))
    def items():
        return []

    @asset(partitions={"site": StaticPartitions(["east", "west"])}, inputs={"items": In()})
    def report(items: dict[str, list]):
        return len(items)

    project = Project(assets=[items, report], default_store=FileStore(tmp_path / "data"))
    east, west = (canonical_partition(["site", "day"], {"site": s, "day": "one"}) for s in ("east", "west"))
    heads = {east: {"commit": 1}, west: {"commit": 1}}
    planner = Planner(project.manifest, lambda o, p: heads.get(p), lambda o: heads, 0)
    incomplete = {("items", east)}
    assert full.gate(planner, {"asset": "report", "partition": "west"}, incomplete) is None
    assert full.gate(planner, {"asset": "report", "partition": "east"}, incomplete) == [INCOMPLETE, "items"]


async def test_a_key_held_back_keeps_its_prior_output(solera, tmp_path):
    entered, release, calls = asyncio.Event(), asyncio.Event(), [0]

    @asset(inputs={"feed": Incremental()}, outputs=Output("copy", key="id"))
    def copy(feed: list):
        return [{"id": r["id"], "v": r["v"]} for r in feed]

    @asset(inputs={"feed": Incremental(), "copy": Incremental()}, outputs=Output("join", key="id"))
    async def join(feed: list, copy: list):
        calls[0] += 1
        if calls[0] == 2:
            entered.set()
            await release.wait()
        rows = {r["id"]: r["v"] for r in copy} | {r["id"]: r["v"] for r in feed}
        return Patch([{"id": k, "v": v} for k, v in rows.items()])

    outside = Outside()
    stores = {"ext": SourceStore(tmp_path / "ext", outside)}
    sources = [Source("feed", key="id", store="ext")]
    await solera.deploy(
        Project(
            assets=[copy, join], sources=sources, stores=stores, default_store=FileStore(tmp_path / "data")
        )
    )
    outside.feed.update({"k1": "1", "k2": "1"})
    await solera.commit_source("feed", upsert=outside.feed)
    await solera.run("copy")
    await solera.run("join")
    outside.feed["k1"] = "2"
    await solera.commit_source("feed", upsert={"k1": "2"})
    joining = await solera.submit("join")
    await solera.until(entered.is_set)
    await solera.run("copy", mode="full")  # a clear on the input k1's batch did not take
    release.set()
    await solera.wait(joining)
    eng = solera.engine
    edge = eng.m.core_edges[("join", "", "feed")][0]
    context = {"definition": eng.m.partition("join", "")["definition"].encode(), "context": b"{}"}
    assert eng.core.tracking.owed(edge.encode(), context)[0] == [("k1", "updated")]
    assert await solera.rows("join") == {"k1": "1", "k2": "1"}, "held back on every input: its output stays"


async def test_a_rebound_input_closes_its_edge_at_deploy(solera, tmp_path):
    def project(up):
        @asset(outputs=Output("left", key="id"))
        def left():
            return []

        @asset(outputs=Output("right", key="id"))
        def right():
            return []

        @asset(inputs={"data": Incremental(up)}, outputs=Output("copy", key="id"))
        def copy(data: list):
            return data

        return Project(assets=[left, right, copy], default_store=FileStore(tmp_path / "data"))

    await solera.deploy(project("left"))
    await solera.run("left")
    await solera.run("copy")
    assert ("copy", "", "data") in solera.engine.m.core_edges
    await solera.deploy(project("right"))
    assert ("copy", "", "data") not in solera.engine.m.core_edges


async def test_proceed_lets_readers_go_on_over_a_partial_output_until_it_starts_over(solera, tmp_path):
    files, seen = Files(solera, {"a": 1, "b": 1, "c": 1}), []

    @asset(inputs={"copy": In()})
    def report(copy: list):
        seen.append(len(copy))
        return len(copy)

    project = files.project()
    await solera.deploy(
        Project(
            assets=[*project.assets.values(), report],
            stores=project.stores,
            default_store=FileStore(solera.root / "data"),
        ),
        cancel_grace=0.2,
        heartbeat_seconds=0.1,
    )
    await solera.run("files")
    await solera.run("copy")
    with pytest.raises(Refused):  # nothing to let through
        await solera.post("/assets/copy/proceed", {"partition": ""})
    await files.cancel_midway(mode="full")
    assert await solera.alarmed("copy")
    reader = await solera.submit("report")
    task = await decided(solera, reader, "report")
    assert task.get("held") == [INCOMPLETE, "copy"] and seen == [], task
    done = await solera.post("/assets/copy/proceed", {"partition": "", "by": "test"})
    assert done["outputs"] == ["copy"]
    await solera.wait(reader)
    assert seen == [1] and not await solera.alarmed("copy"), "over the partial output: a, the first batch's"
    assert full.running(solera.engine.core) == set(), "its full run ended"
    await solera.state.durable()
    replay = await State.open((tmp_path / "state").as_uri(), "contract", writer=False)
    try:
        assert (
            replay.model.proceeded
            == solera.engine.m.proceeded
            == {("copy", ""): pytest.approx(replay.model.proceeded[("copy", "")])}
        )
    finally:
        await replay.close()
    files.waiting = False  # the canceled batch never got past its hold
    files.hold.clear()  # the next full run starts over: its readers wait again
    rebuild = await solera.submit("copy", mode="full")
    await solera.until(lambda: files.waiting)
    assert solera.engine.m.proceeded == {}
    reader = await solera.submit("report")
    task = await decided(solera, reader, "report")
    assert task.get("held") == [INCOMPLETE, "copy"], task
    files.hold.set()
    await solera.wait(rebuild)
    await solera.wait(reader)
    assert seen == [1, 3]
