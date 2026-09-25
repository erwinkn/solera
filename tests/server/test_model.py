"""The event-sourced model (docs/object-store-state.md §4, §5) and the engine's
use of it: commits, fencing, leases, pool claims, restarts, and replay."""

import asyncio
import json

import pytest
from solera.sdk import Automation, Every, Incremental, OnChange, Output, Project, asset
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

    async def wait(self, run, timeout):
        await asyncio.sleep(min(timeout, 0.02))
        return None

    async def cancel(self, run):
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
        placements = {"Local": lambda e, o, c: InlinePlacement(c, PROJECT)}
    else:
        placements = {"Local": lambda e, o, c: Hold(c)}
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
    assert m.heads[("consumer", "")]["complete"] is True
    assert m.watermarks[("consumer", "files", "")]["batch"] == 1
    # files changed and consumer watches it: the change pended, and the next tick
    # (run_until ticks) fired the OnChange automation and consumed it — without a
    # new run, since this run's consumer task was still pending (§9).
    auto = m.automations["consumer.onchange.0"]
    assert auto["last_at"] is not None and auto["pending"] == []


async def test_identical_content_is_not_a_change(state, clock):
    engine = engine_on(state, clock)
    await engine.initialize()
    await settle(engine, (await engine.submit(["files"]))["id"])
    state.model.automations["consumer.onchange.0"]["pending"] = []
    await settle(engine, (await engine.submit(["files"]))["id"])
    assert state.model.automations["consumer.onchange.0"]["pending"] == []


async def test_failed_precondition_changes_nothing(state, clock):
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    await settle(engine_on(state, clock), (await engine.submit(["files"]))["id"])
    _, task_id, attempt = await held(engine, ["files"])
    before = durable(state.model)
    prepared = {"inputs": {}, "baseline": {"files": None}, "scope_complete": True}  # a stale baseline
    ref = {"output": "files", "store": "json", "handle": {}, "version": "v2", "partition": "", "meta": {}}
    with pytest.raises(Conflict):
        await engine.commit_attempt(attempt, prepared, {"outputs": {"files": ref}})
    assert durable(state.model) == before


async def test_an_expired_lease_requeues_and_fences_the_old_attempt(state, clock):
    engine = engine_on(state, clock, placement="hold", lease_seconds=10)
    await quiet(engine)
    _, task_id, attempt = await held(engine, ["polled"])
    clock.now += 11
    await engine._sweep_leases()
    task = state.model.task(task_id)
    assert task["status"] == "queued" and task["attempts"][-1]["outcome"] == "expired"
    assert state.model.claimed(attempt) is None
    with pytest.raises(LostOwnership):
        await engine.commit_attempt(attempt, {"inputs": {}, "baseline": {}}, {"outputs": {}})
    await engine.tick()  # redispatched under a new attempt
    assert state.model.claims[task_id]["attempt"] != attempt


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
            rows["v"] = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
            await settle(engine2, (await engine2.submit(["items"]))["id"])
            assert state.model.heads[("items", "")]["batch"] == 1
        return []

    project = Project(assets=[items, reader])
    engine2 = Engine(
        state,
        project.manifest,
        placements={"Local": lambda e, o, c: InlinePlacement(c, project)},
        clock=clock,
        eval_interval=0.01,
    )
    await engine2.initialize()
    await settle(engine2, (await engine2.submit(["items"]))["id"])
    first = await settle(engine2, (await engine2.submit(["reader"]))["id"])
    assert first["request"]["status"] == "succeeded"
    await settle(engine2, (await engine2.submit(["reader"]))["id"])
    assert delivered == [["a", "b"], ["c"]]


async def test_restart_forgets_claims_and_pool_claims(tmp_path, clock):
    state = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    engine = engine_on(state, clock, placement="hold")
    await quiet(engine)
    _, task_id, attempt = await held(engine, ["polled"])
    engine.m.pool[attempt] = {
        "attempt": attempt,
        "task": task_id,
        "status": "claimed",
        "claimed_by": "w",
        "lease_until": clock.now + 30,
        "pool": "p",
        "created_at": clock.now,
    }
    for _, job in engine.inflight.values():
        job.cancel()
    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, flush_interval=0.001)
    assert again.model.claims == {} and again.model.pool == {} and again.model.locks == {}
    assert again.model.task(task_id)["status"] == "queued" and task_id in again.model.queue
    await again.close()


async def test_pool_claims_lease_and_expire(state, clock):
    engine = engine_on(state, clock)
    engine.m.pool["a1"] = {
        "attempt": "a1",
        "task": "t",
        "status": "queued",
        "claimed_by": None,
        "lease_until": None,
        "pool": "p",
        "needs": {"cpu": 2},
        "created_at": 1,
    }
    assert engine.claim_pool_task("w", ["p"], {"cpu": 1}, 30) is None  # too small
    record = engine.claim_pool_task("w", ["p"], {"cpu": 4}, 30)
    assert record["claimed_by"] == "w"
    assert engine.heartbeat_pool_task("w", "a1", 30) == clock.now + 30
    with pytest.raises(LostOwnership):
        engine.heartbeat_pool_task("other", "a1", 30)
    clock.now += 31
    engine._sweep_pool()
    assert engine.m.pool["a1"]["status"] == "queued"
    assert engine.claim_pool_task("w2", ["p"], {"cpu": 4}, 30)["claimed_by"] == "w2"


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
    live = durable(state.model)
    await state.close()
    assert len(await (await State.open(tmp_path.as_uri(), "test", writer=False)).archived_ids()) >= 6
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    replayed = durable(again.model)
    live.pop("writer"), replayed.pop("writer")
    assert replayed == live
