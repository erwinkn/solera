"""Launched attempts outlive the engine (docs/object-store-state.md §8): the
write fence, adoption after a restart, heartbeats, and unsettled outputs."""

import asyncio
import contextlib
import json

import pytest
from obstore.exceptions import AlreadyExistsError
from solera import lifecycle
from solera.executors import Environment
from solera.sdk import Output, Project, Ref, Result, Retry, asset
from solera.stores import FileStore, Keys, Patch, Written
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State

from tests.conftest import whole


class Fake(Environment):
    kind = "Fake"


class Overwriting(FileStore):
    """FileStore's layout, declared `overwrite`: its attempts take a gate, so
    the tests below can watch it (docs/lifecycle.md §9.6)."""

    writes = "overwrite"


class Remote:
    """A placement whose worker the test plays by writing its objects."""

    max_concurrent = None
    launches: list[str] = []

    def __init__(self, ctx):
        self.ctx = ctx

    async def launch(self, stage):
        self.launches.append(stage["attempt"])
        return {"id": stage["attempt"], "run": stage["run"]}

    async def wait(self, run, timeout):
        if await self.ctx.state.attempt_finished(run["run"], run["id"]):
            return {"code": 0, "reason": None, "meta": {}}  # the worker exits once it wrote its result
        await asyncio.sleep(min(timeout, 0.05))
        return None

    async def cancel(self, run):
        return None


def engine_for(state, project, placement="remote", worker=None, **kw):
    if placement == "remote":
        placements = {"Fake": lambda s, c: (worker or Remote)(c)}
    else:
        placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    kw.setdefault("heartbeat_seconds", 0.3)
    return Engine(state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw)


async def until(engine, done, timeout=10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not done():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await engine.tick()
        await asyncio.sleep(0.02)


async def finish_as_worker(state, run_id, attempt, output, invocation="w"):
    """What a worker does (docs/lifecycle.md §3): claim, take the gate, write,
    then seal its result."""

    base = state.attempt_path(run_id, attempt)
    with contextlib.suppress(AlreadyExistsError):
        await state.create_object(f"{base}.worker", json.dumps({"invocation": invocation}).encode())
    await state.create_object(f"{base}.writing", lifecycle.gate("writing", invocation, {}))
    ref = {
        "output": output,
        "store": "default",
        "handle": {"path": "x"},
        "version": "v1",
        "partition": "",
    }
    result = {
        "invocation": invocation,
        "status": "succeeded",
        "writes": "complete",
        "outputs": {output: {"ref": {**ref, "meta": {}}}},
    }
    await state.create_object(f"{base}.result", json.dumps(result).encode())


async def fence(state, run_id, attempt):
    data = await state.get_object(f"{state.attempt_path(run_id, attempt)}.writing")
    return json.loads(data) if data is not None else None


async def launched(engine, targets):
    Remote.launches.clear()
    run = await engine.submit(targets)
    await until(engine, lambda: Remote.launches)
    attempt = Remote.launches[0]
    await until(engine, lambda: engine.m.claimed(attempt) and engine.m.claimed(attempt).get("launched"))
    return run, attempt


async def restart(state, engine, url, project, **kw):
    await engine.stop()
    await state.close()
    again = await State.open(url, "test", flush_interval=0.001)
    return again, engine_for(again, project, **kw)


@asset(executor=Fake("fake")())
def remote():
    return [{"ok": True}]


REMOTE = Project(assets=[remote], executors=[Fake("fake")], default_store=Overwriting())


async def test_a_restarted_engine_adopts_and_commits_a_launched_attempt(tmp_path):
    """The launch is durable: after a restart the attempt keeps its claim, the
    new engine follows its heartbeat, and commits what it wrote — no relaunch."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    state, engine = await restart(state, engine, url, REMOTE)
    task = state.model.task(state.model.attempts[attempt])
    assert task["status"] == "running" and task["launched"]["attempt"] == attempt
    assert state.model.claimed(attempt)["launched"]
    await engine.initialize()
    await engine.tick()
    assert attempt in engine.inflight  # adopted
    await finish_as_worker(state, run["id"], attempt, "remote")
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert Remote.launches == [attempt]
    assert state.model.heads[("remote", "")]["attempt"] == attempt
    await engine.stop()
    await state.close()


class Blind(Remote):
    """A placement whose provider cannot be reached: it cannot tell."""

    async def wait(self, run, timeout):
        raise ConnectionError("the provider's API is down")


async def test_an_attempt_its_placement_cannot_see_is_followed_by_its_heartbeat(tmp_path):
    """After a restart the provider's API is down: the engine keeps the
    handle and follows the worker's beats. Its worker beat once, then went
    quiet: three missed beats and it is dead, aborted and failed retryably."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    await state.put_object(
        f"{state.attempt_path(run['id'], attempt)}.worker", json.dumps({"invocation": "w"}).encode()
    )
    state, engine = await restart(state, engine, url, REMOTE, worker=Blind, heartbeat_seconds=0.1)
    assert state.model.task(state.model.attempts[attempt])["launched"]["handle"] == {
        "id": attempt,
        "run": run["id"],
    }
    await engine.initialize()
    await until(engine, lambda: state.model.claimed(attempt) is None)
    task = state.model.task(next(iter(state.model.runs[run["id"]]["tasks"])))
    first = task["attempts"][0]
    assert first["id"] == attempt and first["outcome"] == "failed"
    assert "no heartbeat" in first["error"]
    assert (await fence(state, run["id"], attempt)) == {"state": "aborted"}
    await engine.stop()
    await state.close()


class Flaky(Remote):
    """A provider whose API fails now and then; the run is there throughout."""

    errors = 0
    seen: list = []

    async def wait(self, run, timeout):
        self.seen.append(run)
        if Flaky.errors:
            Flaky.errors -= 1
            raise TimeoutError("DescribeTasks timed out")
        return await super().wait(run, timeout)


async def test_a_placement_that_cannot_tell_keeps_its_handle(tmp_path):
    """Errors from the provider are not news of the worker: the engine keeps
    asking through the same handle, and settles once it says the run exited."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, worker=Flaky)
    await engine.initialize()
    Flaky.errors, Flaky.seen = 5, []
    run, attempt = await launched(engine, ["remote"])
    await until(engine, lambda: Flaky.errors == 0)
    await finish_as_worker(state, run["id"], attempt, "remote")
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert all(h == {"id": attempt, "run": run["id"]} for h in Flaky.seen)
    await engine.stop()
    await state.close()


class Named(Remote):
    """A provider that names each run after its attempt: launching one twice
    starts it once. Its first launch hangs, as if the engine died in it."""

    started: set = set()
    hang = True

    async def launch(self, stage):
        if Named.hang:
            Named.hang = False
            await asyncio.Event().wait()
        Named.started.add(stage["attempt"])
        return await super().launch(stage)

    resume = launch


async def test_an_attempt_whose_launch_was_cut_short_is_resumed(tmp_path):
    """The engine stops after the launch is durable but before the placement
    answers: no handle was recorded. The next engine resumes it through the
    placement's own name for it, records the handle, and commits it — the
    attempt is not failed, and no retry is spent."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, worker=Named)
    await engine.initialize()
    Named.hang, Named.started = True, set()
    run = await engine.submit(["remote"])
    await until(engine, lambda: any(c.get("launched") for c in state.model.claims.values()))
    [attempt] = [c["attempt"] for c in state.model.claims.values()]
    state, engine = await restart(state, engine, url, REMOTE, worker=Named)
    assert "handle" not in state.model.task(state.model.attempts[attempt])["launched"]
    await engine.initialize()
    await until(engine, lambda: Named.started)
    assert state.model.task(state.model.attempts[attempt])["launched"]["handle"]["id"] == attempt
    await finish_as_worker(state, run["id"], attempt, "remote")
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert [a["status"] for a in detail["attempts"][detail["tasks"][0]["id"]]] == ["succeeded"]
    await engine.stop()
    await state.close()


async def test_a_worker_that_never_reports_is_given_up_on(tmp_path):
    """Provisioning has a deadline of its own: a worker that has not said a
    word by then never started. It is aborted, its run canceled at the
    provider, and the attempt fails retryably."""

    class Stuck(Quiet):
        canceled = []

        async def cancel(self, run):
            Stuck.canceled.append(run["id"])
            await super().cancel(run)

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, worker=Stuck, provision_seconds=0.3)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    task = state.model.task(next(iter(state.model.runs[run["id"]]["tasks"])))
    assert "did not report" in task["attempts"][0]["error"] and Stuck.canceled == [attempt]
    assert (await fence(state, run["id"], attempt)) == {"state": "aborted"}
    await engine.stop()
    await state.close()


async def test_an_adopted_deadline_trusts_the_launching_clock_within_bounds(tmp_path):
    """An adopted attempt keeps what the launching engine's clock says is
    left of its budget, but never less than three heartbeats — that clock
    may have run fast — nor more than the whole budget."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, heartbeat_seconds=30)
    now = state.clock()
    assert abs(engine._left({"at": now - 600}, 3600) - 3000) < 5
    assert engine._left({"at": now - 86400}, 3600) == 90  # three heartbeats
    assert engine._left({"at": now + 86400}, 3600) == 3600
    await state.close()


async def test_a_cancel_waits_for_a_worker_that_is_writing(tmp_path):
    """A cancel is requested first (docs/lifecycle.md §7): a worker that took
    the gate drains — completes its writes and publishes — within the
    grace, and its commit stands though the run is canceled."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, cancel_grace=5)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    base = state.attempt_path(run["id"], attempt)
    await state.create_object(f"{base}.worker", json.dumps({"invocation": "w"}).encode())
    await until(engine, lambda: engine.live[attempt].started)
    await engine.cancel(run["id"])
    await until(
        engine,
        lambda: (engine.live[attempt].cancel or lifecycle.Cancel("forced", "user", 0)).phase == "requested",
    )
    assert engine.live[attempt].cancel.reason == "user"
    await finish_as_worker(state, run["id"], attempt, "remote")
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert state.model.runs[run["id"]]["status"] == "canceled"
    assert state.model.heads[("remote", "")]["attempt"] == attempt
    await engine.stop()
    await state.close()


async def test_a_drain_that_outlives_its_grace_is_forced_and_uncertain(tmp_path):
    """A worker that took the gate and does not finish within the grace is
    forced: its gate is found `writing`, so its writes are uncertain (§2.3)
    and its intents stay unsettled for the next attempt to repair."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, cancel_grace=0.3)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    base = state.attempt_path(run["id"], attempt)
    await state.create_object(f"{base}.worker", json.dumps({"invocation": "w"}).encode())
    intents = {"remote": {"files": [], "added": 0, "removed": 0, "exact": True}}
    await state.create_object(f"{base}.writing", lifecycle.gate("writing", "w", intents))
    await until(engine, lambda: engine.live[attempt].started)
    await engine.cancel(run["id"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    events = [e for e in await engine.history.events(run["id"]) if e["attempt"] == attempt]
    assert events[-1]["type"] == "aborted"
    assert state.model.unsettled[("remote", "")][0]["attempt"] == attempt
    await engine.stop()
    await state.close()


async def test_a_cancel_aborts_a_worker_that_is_not_writing(tmp_path):
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    await engine.cancel(run["id"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert (await fence(state, run["id"], attempt)) == {"state": "aborted"}
    assert ("remote", "") not in state.model.heads
    await engine.stop()
    await state.close()


class Quiet(Remote):
    """A worker that says nothing until it exits, like a real process."""

    def __init__(self, ctx):
        super().__init__(ctx)
        self.stopped = asyncio.Event()

    async def wait(self, run, timeout):
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stopped.wait(), timeout)
            return {"code": -15, "reason": "stopped", "meta": {}}
        return None

    async def cancel(self, run):
        self.stopped.set()


async def test_a_cancel_lands_at_once_however_long_the_wait(tmp_path):
    """The watcher is woken by the cancel, not by its next look at the worker."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE, worker=Quiet, heartbeat_seconds=30)  # looks every 10 s
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    started = asyncio.get_running_loop().time()
    await engine.cancel(run["id"])
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert asyncio.get_running_loop().time() - started < 1.0
    assert (await fence(state, run["id"], attempt)) == {"state": "aborted"}
    await engine.stop()
    await state.close()


class LiveStore(FileStore):
    """A row store read in place, like a database table: what a dead attempt
    wrote is visible. `die` makes the next write land its first n rows, then
    kills the worker."""

    writes = "overwrite"

    late_write_grace = 0.2  # a dead writer's scope is released this soon (docs/lifecycle.md §9.9)

    def __init__(self):
        super().__init__()
        self.rows: dict[str, dict] = {}
        self.die: int | None = None

    def can_load(self, t, selection):
        return True

    async def store(self, write, prior, scope):
        patch = isinstance(write, Patch)
        rows = write.rows if patch else write
        if not patch:
            self.rows.clear()
        for n, row in enumerate(rows):
            if self.die is not None and n == self.die:
                self.die = None
                raise asyncio.CancelledError  # the worker dies mid-write
            self.rows[row["id"]] = dict(row)
        for key in write.remove if patch else ():
            self.rows.pop(key, None)
        return Written(Ref(scope.output.name, "live", {}, scope.attempt, scope.partition))

    async def load(self, ref, t, selection):
        if isinstance(selection, Keys):
            return [dict(r) for k, r in self.rows.items() if k in selection.revisions]
        return [dict(r) for r in self.rows.values()]


async def test_a_worker_that_dies_writing_leaves_its_output_unsettled_and_the_retry_repairs_it(tmp_path):
    """The dead attempt meant to change `a` and add `c`, and only `c` landed.
    Its retry writes `a` only, so it reads `c` back from the store: the
    index — and every reader of it — learns about `c` in the retry's commit."""

    live = LiveStore()
    writes = [
        [{"id": "a", "v": 1}, {"id": "b", "v": 1}],
        Patch([{"id": "c", "v": 1}, {"id": "a", "v": 2}]),  # dies once `c` landed
        Patch([{"id": "a", "v": 2}]),  # the retry
    ]

    @asset(outputs=Output("items", key="id", revision="v", store="live"), retries=Retry(1, delay=0))
    def items():
        return writes.pop(0)

    project = Project(assets=[items], stores={"live": live})
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    assert (await engine.run_until((await engine.submit(["items"]))["id"], 10))["request"][
        "status"
    ] == "succeeded"
    seen = []

    async def watch():
        while not state.model.unsettled:
            await asyncio.sleep(0.002)
        seen.append({k: len(v) for k, v in state.model.unsettled.items()})

    watcher = asyncio.create_task(watch())
    live.die = 1
    detail = await engine.run_until((await engine.submit(["items"]))["id"], 20)
    watcher.cancel()
    assert detail["request"]["status"] == "succeeded", detail
    assert seen == [{("items", ""): 1}]  # the dead attempt left `items` unsettled
    events = await engine.history.events(detail["request"]["id"])
    assert [e["reason"] for e in events if e["type"] == "released"] == ["grace"]  # held, then released
    assert state.model.unsettled == {}  # the retry's commit settled it
    assert live.rows == {"a": {"id": "a", "v": 2}, "b": {"id": "b", "v": 1}, "c": {"id": "c", "v": 1}}
    assert state.model.heads[("items", "")]["count"] == 3
    await engine.stop()
    await state.close()


async def test_an_aborted_worker_writes_nothing(tmp_path):
    """A worker the engine cannot reach learns of the end from its gate,
    which its reports read while its channel fails (§6), and stops while it
    computes; one past its producer by then finds the gate taken and writes
    nothing (§2.4). Neither publishes a result."""

    from solera_worker.worker import ENDED, run_attempt

    @asset(executor=Fake("fake")(), outputs=Output("slow", key="id"))
    async def slow():
        await asyncio.sleep(10)
        return [{"id": "a"}]

    @asset(executor=Fake("fake")(), outputs=Output("quick", key="id"))
    def quick():
        return [{"id": "a"}]

    project = Project(assets=[slow, quick], executors=[Fake("fake")], default_store=Overwriting())
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    for name in ("slow", "quick"):
        run, attempt = await launched(engine, [name])
        assert await engine._gate(run["id"], attempt, "aborted") == ("none", None)
        code = await asyncio.wait_for(run_attempt(state.objects_url, attempt, project, run=run["id"]), 5)
        assert code == ENDED
        assert await state.attempt_result(run["id"], attempt) is None
        claim = json.loads(await state.get_object(f"{state.attempt_path(run['id'], attempt)}.worker"))
        assert "writing" not in [e["type"] for e in claim.get("events", [])]
    await engine.stop()
    await state.close()


async def test_a_dead_immutable_write_leaves_nothing_to_repair(tmp_path, data, monkeypatch):
    """FileStore is immutable (§9.8): a worker that dies after its first
    object landed leaves an object nothing references, under its own
    generation. There is no gate, no unsettled intent and no hold: the
    retry runs at once, and reads see exactly the committed versions."""

    values = [{"a": 1, "b": 1}, {"a": 2, "c": 1}, {"a": 1, "b": 1}]

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0))
    def scores():
        return values.pop(0)

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    assert (await engine.run_until((await engine.submit(["scores"]))["id"], 10))["request"][
        "status"
    ] == "succeeded"
    put, puts = FileStore._put, []

    async def dying(self, base, value):
        puts.append(base)
        first = len(puts) == 1
        await put(self, base, value)
        if first:
            raise asyncio.CancelledError  # the worker dies once one object landed

    monkeypatch.setattr(FileStore, "_put", dying)
    detail = await engine.run_until((await engine.submit(["scores"]))["id"], 20)
    assert detail["request"]["status"] == "succeeded", detail
    first, second = detail["attempts"][detail["tasks"][0]["id"]]
    assert first["status"] == "failed" and second["status"] == "succeeded"
    assert state.model.unsettled == {} and state.model.holds == {}
    ref = Ref.from_json(state.model.heads[("scores", "")]["ref"])
    assert await project.stores["default"].load(ref, None, await whole(state, "scores")) == {"a": 1, "b": 1}
    await engine.stop()
    await state.close()


async def test_a_create_whose_response_was_lost_is_its_own(tmp_path, monkeypatch):
    """Every create-only write lands but loses its response, so its retry
    finds the object there — the spec, the delta file, the write fence,
    the journal. Each holds the writer's own bytes, so each is a success:
    the worker writes and commits rather than taking itself for aborted."""

    import obstore
    from obstore.exceptions import AlreadyExistsError
    from solera.objects import create

    put = obstore.put_async
    retried = []

    async def unheard(store, path, data, **kw):
        await put(store, path, data, **kw)
        if kw.get("mode") == "create":
            retried.append(path.rsplit(".", 1)[-1])
            await put(store, path, data, **kw)  # the retry: AlreadyExists

    monkeypatch.setattr(obstore, "put_async", unheard)

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1, "b": 2}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["scores"]))["id"], 10)
    assert detail["request"]["status"] == "succeeded", detail
    assert {"json", "kx", "writing"} <= set(retried)
    assert state.model.heads[("scores", "")]["count"] == 2 and state.model.unsettled == {}
    with pytest.raises(AlreadyExistsError):  # another writer's object is still a collision
        await create(state.objects, f"{state.attempt_path(detail['request']['id'], 'x')}.writing", b"a")
        await create(state.objects, f"{state.attempt_path(detail['request']['id'], 'x')}.writing", b"b")
    await engine.stop()
    await state.close()


async def test_a_result_that_fails_to_publish_stays_what_it_was(tmp_path, monkeypatch):
    """Publishing is not executing. The result's create lands but loses its
    response: the retry finds the very same bytes, so the success stays a
    success. A worker that cannot publish at all leaves no result — never a
    failure it did not have — and the engine retries it as dead."""

    from solera_worker import worker

    create, puts, broken = worker.create, [], {"on": False}

    async def flaky(objects, key, value):
        if key.endswith(".result"):
            puts.append(value)
            if broken["on"]:
                raise OSError("store unreachable")
            if len(puts) == 1:
                await create(objects, key, value)
                raise OSError("the response was lost")
        await create(objects, key, value)

    monkeypatch.setattr(worker, "create", flaky)
    monkeypatch.setattr(worker, "PUBLISH_TRIES", 2)
    calls = []

    @asset(outputs=Output("scores", keyed=True), retries=Retry(1, delay=0))
    def scores(ctx):
        calls.append(1)
        broken["on"] = len(calls) == 2  # the next run's first attempt cannot publish
        ctx.log("scoring")
        return {"a": len(calls)}

    project = Project(assets=[scores])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    engine.late_write_grace["scores"] = 0  # a dead writer's scope is held, briefly (§9.9)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["scores"]))["id"], 10)
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    assert attempt["status"] == "succeeded" and len(puts) == 2 and puts[0] == puts[1]
    result = await state.attempt_result(detail["request"]["id"], attempt["id"])
    assert result["status"] == "succeeded" and result["log"]["tail"]
    assert b"scoring" in await state.attempt_log(detail["request"]["id"], attempt["id"])

    detail = await engine.run_until((await engine.submit(["scores"]))["id"], 20)
    first, second = detail["attempts"][detail["tasks"][0]["id"]]
    assert "without a result" in first["error"] and second["status"] == "succeeded"
    await engine.stop()
    await state.close()


async def test_garbage_waits_for_attempts_claimed_before_it_whatever_the_clocks(tmp_path):
    """The engine that launched this attempt ran an hour fast; the one that
    adopted it is right. A file let go of after the claim is kept until the
    attempt ends: both are placed by their position in the journal, which
    every engine replays alike, not by either engine's clock."""

    import time

    url = tmp_path.as_uri()
    state = await State.open(url, "test", clock=lambda: time.time() + 3600, flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    state, engine = await restart(state, engine, url, REMOTE)
    await engine.initialize()
    path = "history/runs/merged-away.parquet"
    await state.put_object(path, b"rows")
    removed = {"table": "runs", "removed": [path], "added": None}
    state.record({"type": "HistoryCompacted", "changes": [removed], "at": time.time()})
    await engine.upkeep.collect()
    assert await state.get_object(path) is not None  # the attempt may still read it
    await finish_as_worker(state, run["id"], attempt, "remote")
    await engine.run_until(run["id"], 10)
    await engine.upkeep.collect()
    assert await state.get_object(path) is None and state.model.garbage == []
    await engine.stop()
    await state.close()


async def test_the_timeout_runs_from_the_first_report(tmp_path):
    """Provisioning is not running: an asset's timeout starts when its
    worker first reports, however long it took to get there. After a
    restart, a running attempt gets its whole timeout again, from when the
    new engine first hears from it — never what an old clock says is left."""

    @asset(executor=Fake("fake")(), timeout=0.5, retries=Retry(0))
    def brief():
        return [{"ok": True}]

    project = Project(assets=[brief], executors=[Fake("fake")])
    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, project, worker=Quiet, heartbeat_seconds=0.3, cancel_grace=0.1)
    await engine.initialize()
    run, attempt = await launched(engine, ["brief"])
    beat = f"{state.attempt_path(run['id'], attempt)}.worker"
    for _ in range(50):  # a second of provisioning, twice the timeout
        await engine.tick()
        await asyncio.sleep(0.02)
    assert state.model.claimed(attempt) is not None
    await state.put_object(beat, json.dumps({"invocation": "w"}).encode())
    reported = asyncio.get_running_loop().time()
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert asyncio.get_running_loop().time() - reported >= 0.5
    task = state.model.task(next(iter(state.model.runs[run["id"]]["tasks"])))
    assert task["attempts"][0]["error"] == "timeout"

    run, attempt = await launched(engine, ["brief"])
    await state.put_object(
        f"{state.attempt_path(run['id'], attempt)}.worker", json.dumps({"invocation": "w"}).encode()
    )
    state, engine = await restart(
        state, engine, url, project, worker=Quiet, heartbeat_seconds=0.1, cancel_grace=0.1
    )
    await asyncio.sleep(0.6)  # no engine for longer than its timeout
    await engine.initialize()
    adopted = asyncio.get_running_loop().time()
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert asyncio.get_running_loop().time() - adopted >= 0.5
    await engine.stop()
    await state.close()


async def test_an_attempt_that_wrote_nothing_still_leaves_a_gate(tmp_path):
    """An attempt that commits without a store call (its output unchanged)
    took no gate; the engine closes it, so no delayed worker can take it
    later (§2.4). One that wrote keeps the worker's `writing` gate."""

    calls = []

    @asset(outputs=Output("same", keyed=True))
    def same():
        calls.append(1)
        return {"a": 1}

    project = Project(assets=[same], default_store=Overwriting())
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    gates = []
    for _ in range(2):
        detail = await engine.run_until((await engine.submit(["same"]))["id"], 10)
        [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
        result = await state.attempt_result(detail["request"]["id"], attempt["id"])
        gate = json.loads(
            await state.get_object(f"{state.attempt_path(detail['request']['id'], attempt['id'])}.writing")
        )
        gates.append((result["writes"], gate["state"]))
    assert gates == [("complete", "writing"), ("none", "closed")]
    await engine.stop()
    await state.close()


async def test_only_stores_that_take_a_gate_get_one(tmp_path):
    """Review P3-9: an attempt writing only immutable outputs creates no
    `.writing` (§9.6: no gate, no intents); one writing an overwrite store
    does, listing only that store's intents."""

    @asset(outputs=Output("scores", keyed=True))
    def scores():
        return {"a": 1}

    @asset(outputs=[Output("plain", keyed=True), Output("legacy", keyed=True, store="old")])
    def both():
        return Result({"plain": {"a": 1}, "legacy": {"a": 1}})

    project = Project(assets=[scores, both], stores={"old": Overwriting()})
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project, placement="inline")
    await engine.initialize()
    for target, gated in (("scores", None), ("both", ["legacy"])):
        detail = await engine.run_until((await engine.submit([target]))["id"], 10)
        assert detail["request"]["status"] == "succeeded"
        attempt = detail["attempts"][detail["tasks"][0]["id"]][0]["id"]
        gate = await state.get_object(f"{state.attempt_path(detail['request']['id'], attempt)}.writing")
        assert (sorted(json.loads(gate)["intents"]) if gate else None) == gated
    await engine.stop()
    await state.close()
