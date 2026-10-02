"""The event-sourced model (docs/object-store-state.md §4, §5) and the engine's
use of it: commits, fencing, durable launches, restarts, and replay."""

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
    ref = {"output": "files", "store": "default", "handle": {}, "version": "v2", "partition": "", "meta": {}}
    with pytest.raises(Conflict):
        await engine.commit_attempt(attempt, prepared, {"outputs": {"files": ref}})
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
        await engine.commit_attempt(attempt, {"inputs": {}, "baseline": {}}, {"outputs": {}})


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
    live = durable(state.model)
    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", clock=clock, writer=False)
    history = again.model
    finished = len(history.history.rows.get("runs", ())) + sum(
        f["rows"] for f in history.history.files.get("runs", ())
    )
    assert finished >= 6
    replayed = durable(again.model)
    live.pop("writer"), replayed.pop("writer")
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
    live.pop("writer"), replayed.pop("writer")
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

    batch = {}

    @asset(outputs=Output("uploads", keyed=True))
    def uploads():
        return Patch(batch)

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
        batch.clear()
        batch.update({f"u{n}.{i}": i for i in range(6)})
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
