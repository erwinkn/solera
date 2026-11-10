"""Full runs on the engine's new tracking core (B5, D205): the gate on every
reader of an incomplete upstream, how a full run ends, what a batch holds
back, and the operator's `proceed`."""

import asyncio

import pytest
from solera._native import ChangeIndex, Tracking, encode_changes
from solera.sdk import (
    In,
    Incremental,
    Output,
    Project,
    Retry,
    Source,
    StaticPartitions,
    asset,
    canonical_partition,
)
from solera.stores import FileStore, Patch
from solera_server.planning import Planner
from solera_server.state import Conflict, State
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


def _attempts(detail: dict, asset_name: str) -> list[dict]:
    [task] = [t for t in detail["tasks"] if t["asset"] == asset_name]
    return detail["attempts"][task["id"]]


async def test_a_batch_that_would_hold_a_key_back_publishes_nothing_and_is_planned_again(solera, tmp_path):
    entered, release, calls = asyncio.Event(), asyncio.Event(), [0]

    @asset(inputs={"feed": Incremental()}, outputs=Output("copy", key="id"))
    def copy(feed: list):
        return [{"id": r["id"], "v": r["v"]} for r in feed]

    @asset(
        inputs={"feed": Incremental(), "copy": Incremental()},
        outputs=[Output("mapped", key="id"), Output("total")],
        retries=Retry(n=0),
    )
    async def join(feed: list, copy: list):
        calls[0] += 1
        if calls[0] == 2:
            entered.set()
            await release.wait()
        mapped = Patch([{"id": "mapped-" + r["id"], "v": r["v"]} for r in feed])
        return {"mapped": mapped, "total": {"n": sum(int(r["v"]) for r in feed)}}

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
    eng = solera.engine
    before = {name: eng.m.heads[(name, "")]["ref"] for name in ("mapped", "total")}
    outside.feed["k1"] = "9"
    await solera.commit_source("feed", upsert={"k1": "9"})
    joining = await solera.submit("join")
    await solera.until(entered.is_set)
    await solera.run("copy", mode="full")  # a clear on the input k1's batch did not take
    replanned = []
    record = solera.state.record

    def spy(*events, **kw):
        for e in events:
            if e.get("type") == "AttemptFinished" and e.get("outcome") == "replanned":
                replanned.append({name: eng.m.heads[(name, "")]["ref"] for name in before})
        return record(*events, **kw)

    solera.state.record = spy
    release.set()
    detail = await solera.wait(joining)
    assert detail["request"]["status"] == "succeeded", "no try spent: retries=0 still succeeds"
    outcomes = [a["outcome"] for a in _attempts(detail, "join")]
    assert outcomes == ["replanned", "succeeded"], outcomes
    assert replanned == [before], "the held batch published neither its keyed nor its unkeyed output"
    assert await solera.rows("mapped") == {"mapped-k1": "9", "mapped-k2": "1"}
    assert await solera.read("total") == {"n": 9}


async def test_a_key_list_that_would_hold_its_key_back_is_planned_again_and_completes(solera, tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    release.set()

    @asset(inputs={"feed": Incremental()}, outputs=Output("empty", key="id"))
    def empty(feed: list):
        return []

    async def join(feed: list, empty: list):
        entered.set()
        await release.wait()
        return [{"id": r["id"], "v": r["v"]} for r in feed]

    outside = Outside()

    def project(version):
        joining = asset(
            join,
            inputs={"feed": Incremental(), "empty": Incremental()},
            outputs=Output("join", key="id"),
            version=version,
        )
        stores = {"ext": SourceStore(tmp_path / "ext", outside)}
        sources = [Source("feed", key="id", store="ext")]
        return Project(
            assets=[empty, joining],
            sources=sources,
            stores=stores,
            default_store=FileStore(tmp_path / "data"),
        )

    await solera.deploy(project("1"))
    outside.feed["k"] = "1"
    await solera.commit_source("feed", upsert=outside.feed)
    await solera.run("empty")
    await solera.run("join")
    await solera.deploy(project("2"))  # join owes a full run
    entered.clear()
    release.clear()
    listed = await solera.submit("join", keys={"feed": ["k"]})
    await solera.until(entered.is_set)
    await solera.run("empty", mode="full")
    release.set()
    detail = await solera.wait(listed)
    assert [a["outcome"] for a in _attempts(detail, "join")] == ["replanned", "succeeded"]
    await solera.settle(0.1)
    assert await solera.rows("join") == {"k": "1"}
    assert await solera.engine.complete("join", "") and full.incomplete(solera.engine) == set()


async def test_a_whole_reader_waits_for_a_full_run_over_unkeyed_inputs(solera, tmp_path):
    entered, release, holding, seen = asyncio.Event(), asyncio.Event(), [False], []

    @asset(outputs=Output("events", incremental=True))
    def events():
        return Patch([{"n": 1}])

    @asset(inputs={"events": Incremental(batch_size=1)})
    async def digest(ctx, events: list):
        b = ctx.batch["events"]
        if holding[0] and b.index == 1:
            entered.set()
            await release.wait()
        prior = 0 if b.reset else (await ctx.load() or {"n": 0})["n"]
        return {"n": prior + len(events)}

    @asset(inputs={"digest": In()})
    def report(digest: dict):
        seen.append(digest["n"])
        return digest

    await solera.deploy(Project(assets=[events, digest, report], default_store=FileStore(tmp_path / "data")))
    for _ in range(3):
        await solera.run("events")
    await solera.run("digest")
    holding[0] = True
    rebuild = await solera.submit("digest", mode="full")
    await solera.until(entered.is_set)
    try:
        reader = await solera.submit("report")
        task = await decided(solera, reader, "report")
        assert task.get("held") == [INCOMPLETE, "digest"] and seen == [], (task, seen)
    finally:
        release.set()
        await solera.wait(rebuild)
    await solera.wait(reader)
    assert seen == [3] and full.incomplete(solera.engine) == set()


async def test_proceed_and_a_commit_of_several_outputs_take_their_locks_in_one_order(solera, tmp_path):
    @asset(outputs=[Output("z", key="id"), Output("a", key="id")])
    def two():
        return {"z": [{"id": "k"}], "a": [{"id": "k"}]}

    await solera.deploy(Project(assets=[two], default_store=FileStore(tmp_path / "data")))
    eng = solera.engine
    run, entered, release = eng.core.run, asyncio.Event(), asyncio.Event()

    async def first_build_waits(fn, *args, **kwargs):
        result = await run(fn, *args, **kwargs)
        if fn == eng.core.tracking.build_commit and not entered.is_set():
            entered.set()
            await release.wait()
        return result

    eng.core.run = first_build_waits
    request = await solera.submit("two")
    await solera.until(entered.is_set)
    ids = eng.m.core_outputs
    assert ids[("z", "")] < ids[("a", "")], ids  # id order and name order differ
    proceeding = asyncio.create_task(eng.proceed("two"))
    release.set()
    detail = await solera.wait(request, timeout=5)
    assert detail["request"]["status"] == "succeeded"
    with pytest.raises(Conflict):  # nothing to let through: refused, not stuck
        await asyncio.wait_for(proceeding, 5)


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


def test_the_engines_own_tasks_pass_the_gate(tmp_path):
    @asset(outputs=Output("items", key="id"))
    def items():
        return []

    project = Project(assets=[items], default_store=FileStore(tmp_path / "data"))
    planner = Planner(project.manifest, lambda o, p: None, lambda o: {}, 0)
    assert full.gate(planner, {"asset": "@cleanup", "partition": ""}, {("items", "")}) is None


async def test_an_input_cleared_while_the_batch_builds_replans_it_without_spending_a_try(solera, tmp_path):
    @asset(inputs={"feed": Incremental()}, outputs=Output("copy", key="id"))
    def copy(feed: list):
        return [{"id": r["id"], "v": r["v"]} for r in feed]

    @asset(
        inputs={"feed": Incremental(), "copy": Incremental()},
        outputs=Output("join", key="id"),
        retries=Retry(n=0),
    )
    def join(feed: list, copy: list):
        return Patch([{"id": r["id"], "v": r["v"]} for r in feed])

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
    await solera.commit_source("feed", upsert={"k1": "2"})  # join takes k1 from feed, not from copy
    eng = solera.engine
    run, entered, release = eng.core.run, asyncio.Event(), asyncio.Event()

    async def join_build_waits(fn, *args, **kwargs):
        if fn == eng.core.tracking.build_commit and args[0] == eng.m.core_outputs[("join", "")].encode():
            if not entered.is_set():
                entered.set()
                await release.wait()
        return await run(fn, *args, **kwargs)

    eng.core.run = join_build_waits
    joining = await solera.submit("join")
    await solera.until(entered.is_set)
    await solera.run("copy", mode="full")  # copy clears after the batch's progress was built
    release.set()
    detail = await solera.wait(joining)
    assert detail["request"]["status"] == "succeeded", detail
    assert [a["outcome"] for a in _attempts(detail, "join")] == ["replanned", "succeeded"]
    assert await solera.rows("join") == {"k1": "2", "k2": "1"}


async def test_finishing_one_unkeyed_walk_does_not_revoke_proceed(solera, tmp_path):
    phase = ["fail"]
    entered, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("left", incremental=True))
    def left():
        return Patch([{"n": 1}])

    @asset(outputs=Output("right", incremental=True))
    def right():
        return Patch([{"n": 1}])

    @asset(inputs={"left": Incremental(batch_size=1), "right": Incremental(batch_size=1)}, retries=Retry(n=0))
    async def digest(ctx, left: list, right: list):
        if ctx.batch["right"].index == 1:
            if phase[0] == "fail":
                raise RuntimeError("stop after one committed batch")
            entered.set()
            await release.wait()
        return {"n": len(left) + len(right)}

    @asset(inputs={"digest": In()})
    def report(digest: dict):
        return digest

    await solera.deploy(
        Project(assets=[left, right, digest, report], default_store=FileStore(tmp_path / "data"))
    )
    for _ in range(2):
        await solera.run("left")
    for _ in range(4):
        await solera.run("right")
    await solera.run("digest", expect="failed")
    assert full.incomplete(solera.engine) == {("digest", "")}
    await solera.post("/assets/digest/proceed", {"partition": ""})
    assert ("digest", "") in solera.engine.m.proceeded
    phase[0] = "resume"
    resumed = await solera.submit("digest")
    await solera.until(entered.is_set)
    try:
        reader = await solera.submit("report")
        task = await decided(solera, reader, "report")
        assert task.get("held") != ["upstream incomplete", "digest"], task
        assert ("digest", "") in solera.engine.m.proceeded
    finally:
        release.set()
        await solera.wait(resumed)


async def test_time_waiting_for_incomplete_upstream_does_not_expire_replans(solera, tmp_path):
    joins = [0]
    join_entered = [asyncio.Event(), asyncio.Event()]
    join_release = [asyncio.Event(), asyncio.Event()]
    empty_entered, empty_release = asyncio.Event(), asyncio.Event()
    stall_empty = [False]

    @asset(inputs={"feed": Incremental(batch_size=1)}, outputs=Output("empty", key="id"))
    async def empty(ctx, feed: list):
        if stall_empty[0] and ctx.batch["feed"].index == 1:
            empty_entered.set()
            await empty_release.wait()
        return []

    @asset(
        inputs={"feed": Incremental(), "empty": Incremental()},
        outputs=Output("join", key="id"),
        retries=Retry(n=0),
    )
    async def join(feed: list, empty: list):
        joins[0] += 1
        if joins[0] in (2, 3):
            n = joins[0] - 2
            join_entered[n].set()
            await join_release[n].wait()
        return [{"id": r["id"], "v": r["v"]} for r in feed]

    outside = Outside()
    await solera.deploy(
        Project(
            assets=[empty, join],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(tmp_path / "ext", outside)},
            default_store=FileStore(tmp_path / "data"),
        )
    )
    outside.feed.update(k1="1", k2="1")
    await solera.commit_source("feed", upsert=outside.feed)
    await solera.run("empty")
    await solera.run("join")
    outside.feed["k1"] = "9"
    await solera.commit_source("feed", upsert={"k1": "9"})
    request = await solera.submit("join")
    await solera.until(join_entered[0].is_set)
    stall_empty[0] = True
    rebuild = await solera.submit("empty", mode="full")
    await solera.until(empty_entered.is_set)
    join_release[0].set()
    task = next(iter(solera.engine.m.runs[request["id"]]["tasks"].values()))
    await solera.until(lambda: task.get("held") == ["upstream incomplete", "empty"])
    assert task.get("outcomes") == {"replanned": 1}
    solera.advance(901)
    stall_empty[0] = False
    empty_release.set()
    await solera.wait(rebuild)
    await solera.until(join_entered[1].is_set)
    await solera.run("empty", mode="full")
    join_release[1].set()
    detail = await solera.wait(request)
    assert detail["request"]["status"] == "succeeded", detail["tasks"]


def test_failed_retry_does_not_erase_a_successful_key_list_removals_coverage(tmp_path):
    t = Tracking()
    t.attach(b"up", ChangeIndex(tmp_path.as_uri()))
    t.apply(t.build_commit(b"up", encode_changes(["k"]), version=1, path="one")["event"])
    t.apply(t.event_edge_open(b"e", b"up", True))

    def plan():
        return t.plan(
            {"mode": "keys", "keys": ["k"], "limit": 10, "inputs": [{"edge": b"e", "conditions": {}}]}
        )["plan"]

    t.apply(t.progress_ops(plan(), [([], [])])[0])
    t.apply(t.build_commit(b"up", encode_changes([], ["k"]), version=2, path="two")["event"])
    t.apply(t.progress_ops(plan(), [([], [])])[0])
    t.apply(t.build_commit(b"up", encode_changes(["k"]), version=3, path="three")["event"])
    assert not t.taken(b"up", edge=b"e"), "the key list successfully processed k's removal"
    t.apply(t.progress_ops(plan(), [(["k"], [])])[0])
    assert not t.taken(b"up", edge=b"e"), (
        "a failed retry must keep k's earlier coverage, as well as its holding"
    )
