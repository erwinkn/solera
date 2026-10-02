"""The attempt lifecycle's races (docs/lifecycle.md §2–§7): duplicate
invocations, a duplicate's exit, the two-phase cancel and its record, the
binding rebuilt after a restart, and logs as chunks plus a tail."""

import asyncio
import json

from solera import lifecycle
from solera.lifecycle import Ended
from solera.sdk import Output, Project, Retry, asset
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

    def invocation():
        return run_attempt(
            state.objects_url,
            attempt,
            project,
            run=run["id"],
            channel=LocalChannel(engine, attempt),
            loser_poll=0.05,
        )

    owner = asyncio.create_task(invocation())
    await asyncio.sleep(0.05)
    loser = asyncio.create_task(invocation())
    await asyncio.sleep(0.1)
    assert not loser.done()  # waiting for the owner, not exiting
    assert await asyncio.wait_for(asyncio.gather(owner, loser), 10) == [0, 0]
    claim = json.loads(await state.get_object(f"{lifecycle.base(run['id'], attempt)}.worker"))
    result = await state.attempt_result(run["id"], attempt)
    assert result["invocation"] == claim["invocation"]
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert state.model.heads[("items", "")]["attempt"] == attempt
    await engine.stop()
    await state.close()


class Duplicate(Remote):
    """The relaunched handle names a duplicate, which exits at once."""

    async def wait(self, run, timeout):
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
    await state.create_object(f"{base}.worker", json.dumps({"invocation": "owner"}).encode())
    await engine.attempt_start(attempt, {"invocation": "owner"})
    for seq in range(1, 6):
        await engine.attempt_beat(attempt, {"invocation": "owner", "seq": seq})
        await engine.tick()
        await asyncio.sleep(0.05)
    assert state.model.claimed(attempt) is not None  # the exit did not end it
    await finish_as_worker(state, run["id"], attempt, "remote", invocation="owner")
    await engine.attempt_finished(attempt, {"invocation": "owner"})
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
    assert result["status"] == "canceled" and result["writes"] == "none"
    assert result["cancel"]["phase"] == "requested" and result["cancel"]["reason"] == "user"
    assert await state.get_object(f"{lifecycle.base(run['id'], attempt['id'])}.writing") is not None
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
        f"{lifecycle.base(run['id'], attempt)}.worker", json.dumps({"invocation": "own"}).encode()
    )
    await engine.attempt_start(attempt, {"invocation": "own"})
    await engine.stop()
    await state.close()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    try:
        await engine.attempt_beat(attempt, {"invocation": "other", "seq": 1})
        raise AssertionError("another invocation was accepted")
    except Ended as error:
        assert error.reason == "not_owner"
    assert await engine.attempt_beat(attempt, {"invocation": "own", "seq": 7}) == {"cancel": None}
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


async def strict_hold(tmp_path):
    """A strict overwrite store whose writer dies mid-write: the attempt ends
    `uncertain` and its scope is held, the retry with it."""

    from .test_fence import LiveStore

    class Strict(LiveStore):
        strict = True

    live = Strict()
    writes = [[{"id": "a", "v": 1}], [{"id": "a", "v": 2}, {"id": "b", "v": 2}], [{"id": "a", "v": 2}]]

    @asset(outputs=Output("items", key="id", revision="v", store="live"), retries=Retry(1, delay=0))
    def items():
        return writes.pop(0)

    project = Project(assets=[items], stores={"live": live})
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline", heartbeat_seconds=0.1)
    await engine.initialize()
    await engine.run_until((await engine.submit(["items"]))["id"], 10)
    live.die = 1
    run = await engine.submit(["items"])
    await until(engine, lambda: ("items", "") in state.model.holds)
    hold = state.model.holds[("items", "")]
    assert hold["mode"] == "strict"
    task = state.model.task(next(iter(state.model.runs[run["id"]]["tasks"])))
    await until(engine, lambda: task.get("held") == ["uncertain", hold["attempt"]])
    return engine, state, run, hold, live


async def test_a_strict_scope_waits_for_an_operator(tmp_path):
    """Strict: no grace. The scope stays held however long; an operator's
    release lets the retry run, and its write settles what the dead writer
    left."""

    engine, state, run, hold, live = await strict_hold(tmp_path)
    for _ in range(20):
        await engine.tick()
        await asyncio.sleep(0.05)
    assert ("items", "") in state.model.holds
    engine.release_scope("items", "", "ops@example.com")
    detail = await engine.run_until(run["id"], 15)
    assert detail["request"]["status"] == "succeeded"
    events = await engine.history.events(run["id"])
    assert [e["reason"] for e in events if e["type"] == "released"] == ["operator:ops@example.com"]
    assert live.rows == {"a": {"id": "a", "v": 2}}  # the retry's replacement, whatever the dead one left
    await engine.stop()
    await state.close()


async def test_a_strict_scope_is_released_by_the_writers_late_result(tmp_path):
    """The writer was not dead after all: its result arrives, sealed after
    its store calls returned (`complete`). That establishes completion, and
    the scope is released."""

    engine, state, run, hold, live = await strict_hold(tmp_path)
    late = {"invocation": "late", "status": "succeeded", "writes": "complete", "outputs": {}}
    await state.create_object(
        f"{lifecycle.base(hold['run'], hold['attempt'])}.result", json.dumps(late).encode()
    )
    detail = await engine.run_until(run["id"], 15)
    assert detail["request"]["status"] == "succeeded"
    events = await engine.history.events(run["id"])
    assert [e["reason"] for e in events if e["type"] == "released"] == ["result"]
    await engine.stop()
    await state.close()
