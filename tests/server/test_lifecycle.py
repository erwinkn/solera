"""The attempt lifecycle's races (docs/lifecycle.md §2–§7): duplicate
invocations, a duplicate's exit, the two-phase cancel and its record, the
binding rebuilt after a restart, and logs as chunks plus a tail."""

import asyncio
import json

from solera import lifecycle
from solera.lifecycle import Ended
from solera.sdk import Output, Project, Retry, asset
from solera.stores import Patch
from solera_server.state import State
from solera_worker.channel import LocalChannel
from solera_worker.worker import run_attempt

from .test_fence import REMOTE, Fake, Remote, engine_for, finish_as_worker, launched, until


async def test_a_duplicate_invocation_waits_for_the_owner_and_writes_nothing(tmp_path):
    """Two invocations of one attempt: the first claim wins. The loser
    touches nothing and exits only once the owner's result exists, so its
    exit never reads as the attempt's (§4)."""

    @asset(executor=Fake("fake")(), outputs=Output("items", keyed=True))
    async def items():
        await asyncio.sleep(0.3)
        return {"a": 1}

    project = Project(assets=[items], executors=[Fake("fake")])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    run, attempt = await launched(engine, ["items"])

    def worker_id():
        return run_attempt(
            state.objects_url,
            attempt,
            project,
            run=run["id"],
            channel=LocalChannel(engine, attempt),
            loser_poll=0.05,
        )

    owner = asyncio.create_task(worker_id())
    await asyncio.sleep(0.05)
    loser = asyncio.create_task(worker_id())
    await asyncio.sleep(0.1)
    assert not loser.done()  # waiting for the owner, not exiting
    assert await asyncio.wait_for(asyncio.gather(owner, loser), 10) == [0, 0]
    claim = json.loads(await state.get_object(f"{lifecycle.base(run['id'], attempt)}.worker"))
    result = await state.attempt_result(run["id"], attempt)
    assert result["worker_id"] == claim["worker_id"]
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert state.model.heads[("items", "")]["attempt"] == attempt
    await engine.stop()
    await state.close()


async def test_a_loser_exits_once_the_engine_says_the_attempt_ended(tmp_path):
    """Review P2-8: the owner died; no result will come. While the attempt
    is live the engine answers the loser `not_owner` and it waits; once the
    engine ends the attempt it answers `ended`, and the loser exits."""

    @asset(executor=Fake("fake")(), outputs=Output("items", keyed=True), retries=Retry(0))
    def items():
        return {"a": 1}

    project = Project(assets=[items], executors=[Fake("fake")])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, cancel_grace=0.1)
    await engine.initialize()
    run, attempt = await launched(engine, ["items"])
    await state.create_object(f"{lifecycle.base(run['id'], attempt)}.worker", b'{"worker_id": "dead"}')
    loser = asyncio.create_task(
        run_attempt(
            state.objects_url,
            attempt,
            project,
            run=run["id"],
            channel=LocalChannel(engine, attempt),
            loser_poll=0.05,
        )
    )
    for _ in range(10):
        await engine.tick()
        await asyncio.sleep(0.02)
    assert not loser.done()  # `not_owner`: the attempt is live
    await engine.cancel(run["id"])
    await until(engine, loser.done)
    assert await loser == 0 and await state.attempt_result(run["id"], attempt) is None
    await engine.stop()
    await state.close()


async def test_a_loser_without_a_channel_exits_on_a_terminal_gate(tmp_path):
    """With no channel, the objects tell it: the engine aborted the gate."""

    from .test_fence import Gated

    @asset(executor=Fake("fake")(), retries=Retry(0))
    def items():
        return [{"a": 1}]

    project = Project(assets=[items], executors=[Fake("fake")], default_store=Gated())
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, cancel_grace=0.1)
    await engine.initialize()
    run, attempt = await launched(engine, ["items"])
    base = lifecycle.base(run["id"], attempt)
    await state.create_object(f"{base}.worker", b'{"worker_id": "dead"}')
    loser = asyncio.create_task(
        run_attempt(state.objects_url, attempt, project, run=run["id"], loser_poll=0.05)
    )
    await engine.cancel(run["id"])
    await until(engine, loser.done)
    assert json.loads(await state.get_object(f"{base}.writing"))["state"] == "aborted"
    await engine.stop()
    await state.close()


class Duplicate(Remote):
    """The relaunched handle names a duplicate, which exits at once."""

    async def wait(self, handle, timeout):
        return {"code": 0, "reason": None, "meta": {}}


async def test_a_duplicates_exit_does_not_end_the_owners_attempt(tmp_path):
    """The provider reports an exit while the claim's owner still beats over
    the channel: the exit was another invocation's. The engine drops the
    handle, follows the owner, and commits its result (§4)."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, worker=Duplicate, heartbeat_seconds=0.3)
    await engine.initialize()
    Remote.launches.clear()
    run = await engine.submit(["remote"])
    await until(engine, lambda: Remote.launches)
    attempt = Remote.launches[0]
    base = lifecycle.base(run["id"], attempt)
    await state.create_object(f"{base}.worker", json.dumps({"worker_id": "owner"}).encode())
    await engine.attempt_start(attempt, {"worker_id": "owner"})
    for seq in range(1, 6):
        await engine.attempt_beat(attempt, {"worker_id": "owner", "seq": seq})
        await engine.tick()
        await asyncio.sleep(0.05)
    assert state.model.claimed(attempt) is not None  # the exit did not end it
    await finish_as_worker(state, run["id"], attempt, "remote", worker_id="owner")
    await engine.attempt_finished(attempt, {"worker_id": "owner"})
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert state.model.heads[("remote", "")]["attempt"] == attempt
    await engine.stop()
    await state.close()


async def test_a_requested_cancel_drains_into_a_canceled_result(tmp_path):
    """A user cancel is requested over the channel: the worker stops before
    its gate and publishes `canceled`, carrying the record it acted on; the
    engine accepts it while the phase is `requested` (§2.2, §7)."""

    started = asyncio.Event()

    @asset(outputs=Output("slow", keyed=True), retries=Retry(0))
    async def slow():
        started.set()
        await asyncio.sleep(30)
        return {"a": 1}

    project = Project(assets=[slow])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline", heartbeat_seconds=0.2, cancel_grace=30)
    await engine.initialize()
    run = await engine.submit(["slow"])
    await until(engine, started.is_set)
    await engine.cancel(run["id"])
    detail = await engine.run_until(run["id"], 15)
    assert detail["request"]["status"] == "canceled"
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    result = await state.attempt_result(run["id"], attempt["id"])
    assert result["status"] == "canceled" and result["write"] == "none"
    assert result["cancel"]["phase"] == "requested" and result["cancel"]["reason"] == "user"
    assert (
        await state.get_object(f"{lifecycle.base(run['id'], attempt['id'])}.writing") is None
    )  # immutable: no gate
    await engine.stop()
    await state.close()


async def test_a_timeout_drain_is_retryable(tmp_path):
    """A timeout follows the same phases with reason `timeout`: the drained
    attempt fails retryably, and the retry runs."""

    calls = []

    @asset(outputs=Output("late", keyed=True), timeout=0.5, retries=Retry(1, delay=0))
    async def late():
        calls.append(1)
        if len(calls) == 1:
            await asyncio.sleep(30)
        return {"a": 1}

    project = Project(assets=[late])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline", heartbeat_seconds=0.2, cancel_grace=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["late"]))["id"], 20)
    assert detail["request"]["status"] == "succeeded"
    first, second = detail["attempts"][detail["tasks"][0]["id"]]
    assert first["status"] == "failed" and first["error"] == "timeout"
    result = await state.attempt_result(detail["request"]["id"], first["id"])
    assert result["cancel"]["reason"] == "timeout"
    await engine.stop()
    await state.close()


async def test_a_restarted_engine_binds_the_claims_owner(tmp_path):
    """After a restart the binding lives in `.worker`, never in a request:
    the owner's next beat binds; any other invocation gets `not_owner`."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    await state.create_object(
        f"{lifecycle.base(run['id'], attempt)}.worker", json.dumps({"worker_id": "own"}).encode()
    )
    await engine.attempt_start(attempt, {"worker_id": "own"})
    await engine.stop()
    await state.close()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    try:
        await engine.attempt_beat(attempt, {"worker_id": "other", "seq": 1})
        raise AssertionError("another invocation was accepted")
    except Ended as error:
        assert error.reason == "not_owner"
    assert await engine.attempt_beat(attempt, {"worker_id": "own", "seq": 7}) == {"cancel": None}
    await engine.stop()
    await state.close()


async def test_a_long_log_is_chunks_and_a_tail(tmp_path, monkeypatch):
    """Lines go out as create-only chunks while the attempt runs, never
    joined; the end that fits travels in the result (§2.1)."""

    from solera_worker import reporting

    monkeypatch.setattr(reporting, "LOG_CHUNK_SECONDS", 0.2)
    monkeypatch.setattr(reporting, "LOG_LIVE_SECONDS", 0.05)

    @asset(outputs=Output("logged", keyed=True))
    async def logged(ctx):
        for n in range(6):
            ctx.log(f"line {n}")
            await asyncio.sleep(0.15)
        return {"a": 1}

    project = Project(assets=[logged])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["logged"]))["id"], 15)
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    result = await state.attempt_result(detail["request"]["id"], attempt["id"])
    log = result["log"]
    assert log["lines"] == 6 and log["chunks"] and sum(c[1] for c in log["chunks"]) <= 6
    text = await state.attempt_log(detail["request"]["id"], attempt["id"])
    assert [json.loads(line)["message"] for line in text.splitlines()] == [f"line {n}" for n in range(6)]
    last = await state.attempt_log(detail["request"]["id"], attempt["id"], tail=2)
    assert [json.loads(line)["message"] for line in last.splitlines()] == ["line 4", "line 5"]
    await engine.stop()
    await state.close()


async def test_a_fenced_store_runs_its_retry_at_once(tmp_path):
    """A dead writer on a fenced store (§9.6): the retry runs at once, its
    acquisition fencing the dead writer out, and each attempt acquires a
    higher generation than the last, before it reads anything."""

    from .test_fence import LiveStore

    class Fenced(LiveStore):
        writes = "fenced"

        def __init__(self):
            super().__init__()
            self.acquired = []

        async def acquire(self, context, prior=None):
            self.acquired.append((context.generation, context.worker_id))

    live = Fenced()
    writes = [Patch([{"id": "a", "v": 1}, {"id": "b", "v": 1}]), Patch([{"id": "a", "v": 2}])]

    @asset(outputs=Output("items", key="id", store="live"), retries=Retry(1, delay=0))
    def items():
        return writes[0] if len(writes) == 1 else writes.pop(0)

    project = Project(assets=[items], stores={"live": live})
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    live.die = 1
    detail = await engine.run_until((await engine.submit(["items"]))["id"], 15)
    assert detail["request"]["status"] == "succeeded"
    assert [a["status"] for a in detail["attempts"][detail["tasks"][0]["id"]]] == ["failed", "succeeded"]
    (first, one), (second, other) = live.acquired
    assert second > first and one != other
    await engine.stop()
    await state.close()


async def test_a_renamed_asset_keeps_what_its_scope_owes(tmp_path):
    """Review P1-3: renaming an asset (`aliases=`) moves its unsettled
    intents and its pending discards with it: a new name never lets a dead
    writer's keys go unrepaired, nor its garbage go uncollected."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "a", "v": 1}]

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, Project(assets=[items]), placement="inline")
    await engine.initialize()
    m = state.model
    m.unsettled[("items", "")] = [{"files": [], "run": "r", "attempt": "dead"}]
    m.discards[("items", "")] = [{"n": 1, "id": "1.0", "kind": "items", "items": [["path", "x"]]}]

    @asset(outputs=Output(key="id"), aliases=["items"])
    def catalog():
        return [{"id": "a", "v": 3}]

    engine = engine_for(state, Project(assets=[catalog]), placement="inline")
    await engine.initialize()
    assert m.unsettled[("catalog", "")][0]["attempt"] == "dead" and ("items", "") not in m.unsettled
    assert [d["n"] for d in m.discards[("catalog", "")]] == [1] and ("items", "") not in m.discards
    await state.close()
