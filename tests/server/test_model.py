"""The event-sourced model (docs/object-store-state.md §4, §5) and the engine's
use of it: commits, fencing, durable launches, restarts, and replay."""

import asyncio
import copy
import json

import orjson
import pytest
from solera.sdk import Every, Incremental, OnChange, Output, Project, asset
from solera.stores import Patch
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.model import Model
from solera_server.state import Conflict, LostOwnership, State

from tests.conftest import worker_finished


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


@asset(automations=Every(60))
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
    task_id = next(t for t in engine.m.runs[run["id"]]["tasks"])
    deadline = asyncio.get_running_loop().time() + 60
    while task_id not in engine.m.claims:  # claimed: dispatched, however busy the host
        assert asyncio.get_running_loop().time() < deadline, "never claimed"
        await engine.tick()
        await asyncio.sleep(0.02)
    return run, task_id, engine.m.claims[task_id]["attempt"]


async def quiet(engine):
    """No automation may queue competing work while a test holds a claim."""

    await engine.initialize()
    for name in list(engine.m.automations):
        await engine.set_automation(name, False)


def durable(model: Model) -> dict:
    return json.loads(json.dumps(model.snapshot(), sort_keys=True))


async def test_commit_installs_heads_cursor_positions_and_pends_onchange(state, clock):
    engine = engine_on(state, clock)
    await engine.initialize()
    detail = await settle(engine, (await engine.submit(["consumer"], upstream=True))["id"])
    assert detail["request"]["status"] == "succeeded"
    m = state.model
    assert m.heads[("files", "")]["run"] == detail["request"]["id"]
    assert m.partition("consumer", "")["caught_up"] is True
    assert m.position("consumer", "files", "")["next"] == 1
    # files changed and consumer watches it: the change pended, and the next tick
    # (run_until ticks) fired the OnChange automation and consumed it — without a
    # new run, since this run's consumer task was still pending (§9).
    auto = m.automations["consumer.onchange.0"]
    assert auto["last_fired"] is not None and auto["pending"] == []


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
    for _ in range(3000):
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
    holds its claim and claim, and is not queued again."""

    state = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    _, task_id, attempt = await held(engine, ["polled"])
    await engine.stop()
    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    task = again.model.task(task_id)
    assert again.model.claimed(attempt)["launched"] and again.model.claimed_partitions == {
        ("polled", ""): attempt
    }
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
            "partition": str(i),
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
        "partition": "",
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
            "commit": {"heads": {}, "positions": {}},
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
    changed = Project(assets=[files, consumer, asset(automations=Every(5))(polled.fn)])
    await Engine(state, changed.manifest, clock=clock).initialize()
    autos = state.model.automations
    assert autos["polled.every.0"]["enabled"] is False  # toggles survive
    assert autos["consumer.onchange.0"]["pending"] == [["files", ""]]  # same trigger keeps its state
    assert autos["polled.every.0"]["trigger"]["seconds"] == 5


async def test_replay_reproduces_the_live_model(tmp_path, clock):
    """A checkpoint and the journal tail after it, replayed on restart,
    equal the live model."""

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
    await engine.stop()
    # Workers outlive the engine's stop: each records its cleanups after its commit (D8).
    # Snapshot only once they are done, or replay holds events the snapshot missed.
    await worker_finished()
    live = durable(state.model)
    await state.journal.close(checkpoint=False)  # no final checkpoint: replay applies the tail
    assert durable(state.model) == live, "something recorded after the snapshot"
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    assert again.journal.checkpoint is not None and again.journal._events, "a checkpoint and a tail"
    history = again.model
    finished = len(history.history.rows.get("runs", ())) + sum(
        f["rows"] for f in history.history.files.get("runs", ())
    )
    assert finished >= 6
    replayed = durable(again.model)
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
    await worker_finished()  # its cleanups recorded too: nothing lands after the snapshot
    await engine.tick()  # archive what finished
    run = await engine.submit(["polled"])
    await state.durable()
    await engine.pause(run["id"])  # after RunSubmitted was flushed: it must not change it
    await state.durable()
    live = durable(state.model)
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    replayed = durable(again.model)
    assert replayed == live
    await state.close()


@pytest.mark.parametrize("value", [float("inf"), 2**70, -(2**63) - 1])
async def test_a_value_the_journal_cannot_hold_changes_nothing(state, clock, value):
    """Engine review #1, F27: `config={"x": 1e400}`, or an integer past 64
    bits, is refused before the run reaches the model; the journal and its
    checkpoints go on."""

    engine = engine_on(state, clock)
    await engine.initialize()
    runs = dict(state.model.runs)
    with pytest.raises(ValueError):
        await engine.submit(["polled"], config={"x": value})
    assert state.model.runs == runs
    await state.durable()
    orjson.dumps(state.model.snapshot(), option=orjson.OPT_SORT_KEYS)


async def test_a_task_over_many_batches_keeps_no_list_of_them(state, clock):
    """Engine review #6 (D4): a task paging through a backlog holds counts
    and its last attempt, not one summary per batch; every attempt's row is
    in the history as it ends, and the run's detail lists them all."""

    from solera.stores import Patch

    commit_number = {}

    @asset(outputs=Output("uploads", keyed=True))
    def uploads():
        return Patch(commit_number)

    @asset(inputs={"uploads": Incremental(batch_size=1)})
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
        assert len(detail["attempts"][task_id]) == 6  # one batch a key, all in the history
    assert max(sizes) < min(sizes) + 400  # flat, however many batches


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
    """§2, §5: a partition's cursor, outcome, completeness, positions and
    failing keys are one record, and `aliases=` moves it as one. A name
    that already has a record keeps its own: two assets' states never mix.
    A position of an input the project no longer declares goes."""

    position = {"kind": "keys", "output": "feed", "upstream_partition": "", "next": 3}
    whole = {
        "cursor": "c1",
        "last": {"outcome": "failed", "run": "r", "attempt": "a", "at": 1.0},
        "caught_up": True,
        "positions": {"feed": position, "gone": {**position, "output": "elsewhere"}},
        "failures": {"commit_number": 0, "forced": {}, "counts": {"failed": 1}},
    }
    m = Model()
    m.partitions[("old", "x")] = copy.deepcopy(whole)
    m.partitions[("old", "y")] = {"cursor": "old's"}
    m.partitions[("new", "y")] = {"last": {"outcome": "succeeded", "run": "r", "attempt": "b", "at": 2.0}}
    input = {"kind": "incremental", "output": "feed"}
    manifest = {
        "assets": {"new": {"aliases": ["old"], "inputs": {"feed": input}, "outputs": []}},
        "outputs": {},
        "sources": {},
        "automations": {},
    }
    m.apply({"type": "ProjectRegistered", "deploy": "r2", "manifest": manifest, "at": 3.0})
    assert m.partition("new", "x") == {**whole, "positions": {"feed": position}}
    assert m.partition("new", "y") == {
        "last": {"outcome": "succeeded", "run": "r", "attempt": "b", "at": 2.0}
    }
    assert m.partition("old", "x") == {} and sorted(m.partitions.of("new")) == ["x", "y"]


def test_a_discard_entrys_delta_outlives_the_attempt_holding_it():
    """docs/lifecycle.md §9.8, simulation finding F11: an attempt's spec hands
    it a clean up entry; the previous attempt's own cleanups (D8) acknowledge
    that entry meanwhile. The delta file the entry reads stays readable until
    the attempt holding it ends, not only while the entry is pending."""

    m = Model()
    entry = {"n": 1, "id": "1.0", "kind": "delta", "prefix": "keys/out/_/", "files": ["000000000003-a"]}
    path = "keys/out/_/000000000003-a.kx"
    m.cleanups[("out", "")] = [entry]
    task = {
        "id": "t1",
        "asset": "out",
        "partition": "",
        "launched": {
            "attempt": "A2",
            "started_at": 0.0,
            "generation": 5,
            "prepared": {"outputs": {"out": {"cleanup": [entry]}}},
        },
    }
    m._hold(task)
    assert path in m.cleanup_reads()
    m._drop_cleanups("out", "", ["1.0"])  # acknowledged by the attempt before
    assert path in m.cleanup_reads()  # A2 still reads it
    del m.claims["t1"]  # A2 ended
    assert path not in m.cleanup_reads()
