"""The event-sourced model (docs/object-store-state.md §4, §5) and the engine's
use of it: commits, fencing, durable launches, restarts, and replay."""

import asyncio
import copy
import json

import pytest
from solera.sdk import Automation, Every, Incremental, OnChange, Output, Project, asset
from solera.stores import Patch
from solera_server.engine import Engine
from solera_server.model import Model
from solera_server.placements.inline import InlinePlacement
from solera_server.state import Conflict, LostOwnership, State


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


class Hold:
    """A placement that runs nothing and never exits: attempts stay claimed."""

    max_concurrent = None

    def __init__(self, ctx):
        self.ctx = ctx

    async def launch(self, stage):
        return {"id": stage["attempt"]}

    async def wait(self, handle, timeout):
        await asyncio.sleep(min(timeout, 0.02))
        return None

    async def cancel(self, handle):
        return None


@asset(outputs=Output("files", key="id"))
def files():
    return [{"id": "a"}, {"id": "b"}]


@asset(inputs={"files": Incremental()}, automations=OnChange())
def consumer(files: list):
    return [{"n": len(files)}]


@asset(automations=Automation(trigger=Every(60)))
def polled():
    return [1]


PROJECT = Project(assets=[files, consumer, polled])


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
async def state(tmp_path, clock):
    opened = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    yield opened
    await opened.close()


def engine_on(state, clock, placement="inline", **kw):
    if placement == "inline":
        placements = {"Local": lambda s, c: InlinePlacement(c, PROJECT)}
    else:
        placements = {"Local": lambda s, c: Hold(c)}
    return Engine(state, PROJECT.manifest, placements=placements, clock=clock, eval_interval=0.01, **kw)


async def settle(engine, run_id):
    return await engine.run_until(run_id, 30)


async def held(engine, targets, **kw):
    """Submit and dispatch without finishing: the attempt stays claimed."""

    run = await engine.submit(targets, **kw)
    await engine.tick()
    await asyncio.sleep(0.05)
    task_id = next(t for t in engine.m.runs[run["id"]]["tasks"])
    return run, task_id, engine.m.claims[task_id]["attempt"]


async def quiet(engine):
    """No automation may queue competing work while a test holds a claim."""

    await engine.initialize()
    for name in list(engine.m.automations):
        await engine.set_automation(name, False)


def durable(model: Model) -> dict:
    return json.loads(json.dumps(model.snapshot(), sort_keys=True))


async def test_commit_installs_heads_cursor_watermarks_and_pends_onchange(state, clock):
    engine = engine_on(state, clock)
    await engine.initialize()
    detail = await settle(engine, (await engine.submit(["consumer"], upstream=True))["id"])
    assert detail["request"]["status"] == "succeeded"
    m = state.model
    assert m.heads[("files", "")]["run"] == detail["request"]["id"]
    assert m.scope("consumer", "")["drained"] is True
    assert m.watermark("consumer", "files", "")["next"] == 1
    # files changed and consumer watches it: the change pended, and the next tick
    # (run_until ticks) fired the OnChange automation and consumed it — without a
    # new run, since this run's consumer task was still pending (§9).
    auto = m.automations["consumer.onchange.0"]
    assert auto["last_at"] is not None and auto["pending"] == []


async def test_content_written_again_is_a_change(state, clock):
    """docs/versions.md §1: writing a key changes it, identical or not — the
    second run's head and keys are its own generation, and its delta lists
    them for the consumer."""

    engine = engine_on(state, clock)
    await quiet(engine)
    await settle(engine, (await engine.submit(["files"]))["id"])
    first = state.model.heads[("files", "")]["ref"]["generation"]
    await settle(engine, (await engine.submit(["files"]))["id"])
    second = state.model.heads[("files", "")]
    assert second["ref"]["generation"] > first
    assert (await engine.list_keys("files", ""))["keys"] == {
        "a": second["ref"]["generation"],
        "b": second["ref"]["generation"],
    }


async def test_failed_precondition_changes_nothing(state, clock):
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    await settle(engine_on(state, clock), (await engine.submit(["files"]))["id"])
    _, task_id, attempt = await held(engine, ["files"])
    before = durable(state.model)
    contract = {"store": "default", "writes": "immutable", "key": None, "incremental": False}
    prepared = {
        "inputs": {},
        "outputs": {"files": {"head": None, "reset": True, "contract": contract}},
    }  # stale
    ref = {"output": "files", "store": "default", "handle": {}, "partition": "", "generation": 2, "meta": {}}
    with pytest.raises(Conflict):
        engine.commit_attempt(attempt, prepared, {"outputs": {"files": ref}})
    assert durable(state.model) == before


async def test_an_aborted_attempt_can_no_longer_commit(state, clock):
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    run, task_id, attempt = await held(engine, ["polled"])
    await engine.cancel(run["id"])
    for _ in range(20):
        await asyncio.sleep(0.02)
        if state.model.claimed(attempt) is None:
            break
    assert state.model.task(task_id)["last"]["outcome"] == "canceled"
    with pytest.raises(LostOwnership):
        engine.commit_attempt(attempt, {"inputs": {}, "outputs": {}}, {"outputs": {}})


async def test_a_moved_input_still_commits(state, clock):
    """An input that moves while an attempt runs does not void the attempt:
    it delivered what it pinned, and the next run picks up the change."""

    engine = engine_on(state, clock)
    await quiet(engine)
    rows = {"v": [{"id": "a"}, {"id": "b"}]}
    delivered = []

    @asset(outputs=Output("items", key="id"))
    def items():
        return rows["v"]

    @asset(inputs={"items": Incremental()})
    async def reader(ctx, items: list):
        delivered.append(sorted(r["id"] for r in items))
        if len(delivered) == 1:
            # The upstream moves (and commits) while this attempt is still running.
            rows["v"] = Patch([{"id": "c"}])
            await settle(engine2, (await engine2.submit(["items"]))["id"])
            assert state.model.heads[("items", "")]["commit_number"] == 1
        return []

    project = Project(assets=[items, reader])
    engine2 = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=clock,
        eval_interval=0.01,
    )
    await engine2.initialize()
    await settle(engine2, (await engine2.submit(["items"]))["id"])
    first = await settle(engine2, (await engine2.submit(["reader"]))["id"])
    assert first["request"]["status"] == "succeeded"
    await settle(engine2, (await engine2.submit(["reader"]))["id"])
    assert delivered == [["a", "b"], ["c"]]


async def test_restart_keeps_launched_claims(tmp_path, clock):
    """A launched attempt is durable (§8): after a restart its task still
    holds its claim and scope lock, and is not queued again."""

    state = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    _, task_id, attempt = await held(engine, ["polled"])
    await engine.stop()
    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    task = again.model.task(task_id)
    assert again.model.claimed(attempt)["launched"] and again.model.locks == {("polled", ""): attempt}
    assert task["status"] == "running" and task_id not in again.model.queue
    await again.close()


def test_finishing_a_task_touches_only_its_dependents():
    """§4.2: completing one task of a 10,000-task run never scans the run."""

    class Counting(dict):
        scans = 0

        def items(self):
            Counting.scans += 1
            return super().items()

        def values(self):
            Counting.scans += 1
            return super().values()

    m = Model()
    tasks = {}
    for i in range(10_000):
        tid = f"r/a:{i}"
        tasks[tid] = {
            "id": tid,
            "run": "r",
            "asset": "a",
            "scope": str(i),
            "status": "queued",
            "deps": [],
            "max_attempts": 1,
            "retry": None,
            "ready_at": 0,
            "queued_at": 0,
            "wait": 0.0,
            "attempts": [],
        }
    tasks["r/b:"] = {
        "id": "r/b:",
        "run": "r",
        "asset": "b",
        "scope": "",
        "status": "waiting",
        "deps": ["r/a:0"],
        "max_attempts": 1,
        "retry": None,
        "ready_at": 0,
        "queued_at": None,
        "wait": 0.0,
        "attempts": [],
    }
    m.apply(
        {
            "type": "RunSubmitted",
            "run": {
                "id": "r",
                "status": "running",
                "created_at": 0,
                "updated_at": 0,
                "paused": False,
                "events": 0,
                "tasks": tasks,
            },
        }
    )
    m.runs["r"]["tasks"] = Counting(m.runs["r"]["tasks"])
    Counting.scans = 0
    m.apply(
        {
            "type": "AttemptFinished",
            "run": "r",
            "task": "r/a:0",
            "attempt": "x",
            "outcome": "succeeded",
            "started_at": 1,
            "finished_at": 2,
            "commit": {"heads": {}, "watermarks": {}},
        }
    )
    assert Counting.scans == 0
    assert m.task("r/b:")["status"] == "queued"  # its one dependency finished
    assert m.run_left["r"] == [10_000, 0]  # 10,001 tasks, one finished


async def test_automation_state_survives_reregistration(state, clock):
    engine = engine_on(state, clock)
    await engine.initialize()
    await engine.set_automation("polled.every.0", False)
    state.model.automations["consumer.onchange.0"]["pending"] = [["files", ""]]
    await engine_on(state, clock).initialize()  # same manifest: nothing to do
    changed = Project(assets=[files, consumer, asset(automations=Automation(trigger=Every(5)))(polled.fn)])
    await Engine(state, changed.manifest, clock=clock).initialize()
    autos = state.model.automations
    assert autos["polled.every.0"]["enabled"] is False  # toggles survive
    assert autos["consumer.onchange.0"]["pending"] == [["files", ""]]  # same trigger keeps its state
    assert autos["polled.every.0"]["trigger"]["seconds"] == 5


async def test_replay_reproduces_the_live_model(tmp_path, clock):
    """Checkpoint + journal tail, replayed on restart, equals the live model."""

    state = await State.open(
        tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001, min_checkpoint=2000
    )
    engine = engine_on(state, clock)
    await engine.initialize()
    for i in range(6):
        await settle(engine, (await engine.submit(["consumer"], upstream=True))["id"])
        await engine.set_automation("polled.every.0", i % 2 == 0)
        clock.now += 61
        await engine.tick()
    await engine.tick()  # archive what finished
    await engine.stop()  # an OnChange run it fired may still be going: nothing may record past here
    live = durable(state.model)
    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    history = again.model
    finished = len(history.history.rows.get("runs", ())) + sum(
        f["rows"] for f in history.history.files.get("runs", ())
    )
    assert finished >= 6
    replayed = durable(again.model)
    live.pop("engine"), replayed.pop("engine")
    assert replayed == live


async def test_the_journal_alone_reproduces_the_live_model(tmp_path, clock):
    """Engine review #1: no final checkpoint — the namespace is read while
    its writer still runs, from the journal its flushes wrote. Later
    transitions (a pause, an attempt, archiving) must not have rewritten
    what earlier events recorded."""

    state = await State.open(
        tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001, min_checkpoint=1 << 30
    )
    engine = engine_on(state, clock)
    await quiet(engine)
    for _ in range(3):
        await settle(engine, (await engine.submit(["consumer"], upstream=True))["id"])
        clock.now += 61
    await engine.tick()  # archive what finished
    run = await engine.submit(["polled"])
    await state.durable()
    await engine.pause(run["id"])  # after RunSubmitted was flushed: it must not change it
    await state.durable()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    live, replayed = durable(state.model), durable(again.model)
    live.pop("engine"), replayed.pop("engine")
    assert replayed == live
    await state.close()


async def test_a_value_the_journal_cannot_hold_changes_nothing(state, clock):
    """Engine review #1: `config={"x": 1e400}` is refused before the run
    reaches the model; the journal and its checkpoints go on."""

    engine = engine_on(state, clock)
    await engine.initialize()
    runs = dict(state.model.runs)
    with pytest.raises(ValueError):
        await engine.submit(["polled"], config={"x": float("inf")})
    assert state.model.runs == runs
    await state.durable()
    json.dumps(state.model.snapshot(), allow_nan=False)


async def test_a_paged_task_keeps_no_list_of_its_pages(state, clock):
    """Engine review #6 (D4): a task paging through a backlog holds counts
    and its last attempt, not one summary per page; every attempt's row is
    in the history as it ends, and the run's detail lists them all."""

    from solera.stores import Patch

    commit_number = {}

    @asset(outputs=Output("uploads", keyed=True))
    def uploads():
        return Patch(commit_number)

    @asset(inputs={"uploads": Incremental(page_size=1)})
    def each_page(uploads: dict):
        return [{"n": len(uploads)}]

    project = Project(assets=[uploads, each_page])
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=clock,
        eval_interval=0.01,
    )
    await engine.initialize()
    sizes = []
    for n in range(3):
        commit_number.clear()
        commit_number.update({f"u{n}.{i}": i for i in range(6)})
        await settle(engine, (await engine.submit(["uploads"]))["id"])
        run = await engine.submit(["each_page"])
        task_id = next(iter(state.model.runs[run["id"]]["tasks"]))
        while state.model.runs.get(run["id"], {}).get("status") not in (None, "succeeded"):
            await engine.tick()
            await asyncio.sleep(0.01)
            task = state.model.task(task_id)
            if task is not None:
                assert "attempts" not in task
                sizes.append(len(json.dumps({k: v for k, v in task.items() if k != "launched"})))
        detail = await engine.run_detail(run["id"])
        assert len(detail["attempts"][task_id]) == 6  # one page a key, all in the history
    assert max(sizes) < min(sizes) + 400  # flat, however many pages


async def test_a_recount_meanwhile_does_not_refuse_a_commit(state, clock):
    """Review round 2, engine #3: a recount corrects the head's count while
    an attempt runs. No writer came in between: its commit stands."""

    engine = engine_on(state, clock)
    await quiet(engine)
    await settle(engine, (await engine.submit(["files"]))["id"])
    held_engine = engine_on(state, clock, placement="hold")
    run, task_id, attempt = await held(held_engine, ["files"])
    for _ in range(500):  # launched: its spec written and its launch durable, however busy the host
        if "launched" in state.model.task(task_id):
            break
        await asyncio.sleep(0.01)
    prepared = state.model.task(task_id)["launched"]["prepared"]
    count = state.model.heads[("files", "")]["count"]
    state.record(
        {
            "type": "IndexRecounted",
            "output": "files",
            "scope": "",
            "live": count + 1,
            "pinned_count": state.model.indexes[("files", "")].count,
            "pinned_inexact": 0,
        }
    )
    assert state.model.heads[("files", "")]["count"] != count
    held_engine.commit_attempt(attempt, prepared, {"outputs": {"files": {"unchanged": True}}})
    assert (
        state.model.claimed(attempt) is None and state.model.task(task_id)["last"]["outcome"] == "succeeded"
    )


def test_one_outputs_heads_are_found_without_looking_at_the_others():
    """Review round 5, engine #7: `heads_of` scanned every output's heads,
    so preparing a fan-in grew with the whole namespace. Heads are held by
    output as well; they stay so across a restore and a move."""

    m = Model()
    for i in range(1000):
        m.heads[("other", f"p{i}")] = {"ref": {"generation": i}}
    m.heads[("mine", "b")] = {"ref": {"generation": 2}}
    m.heads[("mine", "a")] = {"ref": {"generation": 1}}

    def scanned():
        raise AssertionError("heads_of looked at every head")

    m.heads.items = scanned
    assert [s for s, _ in m.heads_of("mine")] == ["a", "b"]
    m.heads[("moved", "a")] = m.heads.pop(("mine", "a"))
    assert [s for s, _ in m.heads_of("mine")] == ["b"] and [s for s, _ in m.heads_of("moved")] == ["a"]
    del m.heads.items
    again = Model()
    again.restore(m.snapshot())
    assert len(again.heads_of("other")) == 1000 and again.heads_of("nothing") == []


def test_a_rename_moves_a_scopes_record_whole():
    """§2, §5: a scope's cursor, outcome, completeness, watermarks and
    failing keys are one record, and `aliases=` moves it as one. A name
    that already has a record keeps its own: two assets' states never mix.
    A watermark of an edge the project no longer declares goes."""

    wm = {"kind": "keys", "output": "feed", "up": "", "next": 3}
    whole = {
        "cursor": "c1",
        "last": {"outcome": "failed", "run": "r", "attempt": "a", "at": 1.0},
        "drained": True,
        "watermarks": {"feed": wm, "gone": {**wm, "output": "elsewhere"}},
        "failures": {"commit_number": 0, "forced": {}, "counts": {"failed": 1}},
    }
    m = Model()
    m.scopes[("old", "x")] = copy.deepcopy(whole)
    m.scopes[("old", "y")] = {"cursor": "old's"}
    m.scopes[("new", "y")] = {"last": {"outcome": "succeeded", "run": "r", "attempt": "b", "at": 2.0}}
    edge = {"kind": "incremental", "output": "feed"}
    manifest = {
        "assets": {"new": {"aliases": ["old"], "inputs": {"feed": edge}, "outputs": []}},
        "outputs": {},
        "sources": {},
        "automations": {},
    }
    m.apply({"type": "ProjectRegistered", "deploy": "r2", "manifest": manifest, "at": 3.0})
    assert m.scope("new", "x") == {**whole, "watermarks": {"feed": wm}}
    assert m.scope("new", "y") == {"last": {"outcome": "succeeded", "run": "r", "attempt": "b", "at": 2.0}}
    assert m.scope("old", "x") == {} and sorted(m.scopes.of("new")) == ["x", "y"]


def test_a_discard_entrys_delta_outlives_the_attempt_holding_it():
    """docs/lifecycle.md §9.8, simulation finding F11: an attempt's spec hands
    it a discard entry; the previous attempt's own discards (D8) acknowledge
    that entry meanwhile. The delta file the entry reads stays readable until
    the attempt holding it ends, not only while the entry is pending."""

    m = Model()
    entry = {"n": 1, "id": "1.0", "kind": "delta", "prefix": "keys/out/_/", "files": ["000000000003-a"]}
    path = "keys/out/_/000000000003-a.kx"
    m.discards[("out", "")] = [entry]
    task = {
        "id": "t1",
        "asset": "out",
        "scope": "",
        "launched": {
            "attempt": "A2",
            "started_at": 0.0,
            "pin": 5,
            "prepared": {"outputs": {"out": {"discard": [entry]}}},
        },
    }
    m._hold(task)
    assert path in m.discard_reads()
    m._drop_discards("out", "", ["1.0"])  # acknowledged by the attempt before
    assert path in m.discard_reads()  # A2 still reads it
    del m.claims["t1"]  # A2 ended
    assert path not in m.discard_reads()
