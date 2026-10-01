"""Launched attempts outlive the engine (docs/object-store-state.md §8): the
write fence, adoption after a restart, heartbeats, and unsettled outputs."""

import asyncio
import contextlib
import json

import pytest
from solera.executors import Environment
from solera.sdk import Output, Project, Ref, Retry, asset
from solera.stores import FileStore, Keys, Patch, Written
from solera_server.engine import Engine
from solera_server.placements.inline import InlinePlacement
from solera_server.state import State


class Fake(Environment):
    kind = "Fake"


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


async def finish_as_worker(state, run_id, attempt, output):
    """What a worker does at the end: its result, then a done beat."""

    base = state.attempt_path(run_id, attempt)
    await state.create_object(f"{base}.writing", json.dumps({"state": "writing", "intents": {}}).encode())
    ref = {
        "output": output,
        "store": "default",
        "handle": {"path": "x"},
        "version": "v1",
        "partition": "",
    }
    result = {"status": "succeeded", "outputs": {output: {"ref": {**ref, "meta": {}}}}}
    await state.put_object(f"{base}.json", json.dumps({"spec": {}, "result": result}).encode())
    await state.put_object(f"{base}.beat", json.dumps({"done": True}).encode())


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


REMOTE = Project(assets=[remote], executors=[Fake("fake")])


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


async def test_an_adopted_attempt_without_heartbeats_is_dead(tmp_path):
    """After a restart the engine has no placement handle: three missed beats
    and the attempt is aborted and fails retryably."""

    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    state, engine = await restart(state, engine, url, REMOTE, heartbeat_seconds=0.1)
    await engine.initialize()
    await until(engine, lambda: state.model.claimed(attempt) is None)
    task = state.model.task(next(iter(state.model.runs[run["id"]]["tasks"])))
    first = task["attempts"][0]
    assert first["id"] == attempt and first["outcome"] == "failed"
    assert "no heartbeat" in first["error"]
    assert (await fence(state, run["id"], attempt)) == {"state": "aborted"}
    await engine.stop()
    await state.close()


async def test_a_cancel_waits_for_a_worker_that_is_writing(tmp_path):
    """A worker that took the fence can't be stopped halfway: a cancel waits
    for it, and its commit lands even though the run is canceled."""

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, REMOTE)
    await engine.initialize()
    run, attempt = await launched(engine, ["remote"])
    base = state.attempt_path(run["id"], attempt)
    await state.create_object(f"{base}.writing", json.dumps({"state": "writing", "intents": {}}).encode())
    await engine.cancel(run["id"])
    for _ in range(10):
        await engine.tick()
        await asyncio.sleep(0.02)
    assert state.model.claimed(attempt) is not None  # still waiting for the writer
    await state.delete_objects([f"{base}.writing"])  # let the helper take it again
    await finish_as_worker(state, run["id"], attempt, "remote")
    await until(engine, lambda: state.model.claimed(attempt) is None)
    assert state.model.runs[run["id"]]["status"] == "canceled"
    assert state.model.heads[("remote", "")]["attempt"] == attempt
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
    assert state.model.unsettled == {}  # the retry's commit settled it
    assert live.rows == {"a": {"id": "a", "v": 2}, "b": {"id": "b", "v": 1}, "c": {"id": "c", "v": 1}}
    assert state.model.heads[("items", "")]["count"] == 3
    await engine.stop()
    await state.close()


async def test_an_aborted_worker_writes_nothing(tmp_path):
    """The worker learns of an abort from its heartbeat, which stops it — or,
    if it is past its producer by then, from the fence, which it can't take."""

    from solera_worker.worker import ABORTED, run_attempt

    calls = []

    @asset(executor=Fake("fake")(), outputs=Output("slow", key="id"))
    async def slow():
        calls.append(1)
        await asyncio.sleep(0 if len(calls) > 1 else 10)
        return [{"id": "a"}]

    project = Project(assets=[slow], executors=[Fake("fake")])
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project)
    await engine.initialize()
    run, attempt = await launched(engine, ["slow"])
    await engine._abort(run["id"], attempt)
    # the heartbeat stops it while it computes; the fence, once it has computed
    for on_abort, reached in ((None, "computing"), (lambda: None, "computed")):
        code = await asyncio.wait_for(
            run_attempt(state.objects_url, attempt, project, run=run["id"], on_abort=on_abort), 5
        )
        assert code == ABORTED
        assert "result" not in await state.attempt_record(run["id"], attempt)
        beat = json.loads(await state.get_object(f"{state.attempt_path(run['id'], attempt)}.beat"))
        assert beat["done"] is True
        assert [e["type"] for e in beat["events"]][-1] == reached  # never "writing"
    assert len(calls) == 2
    prefix = state.model.index("slow", "").prefix
    assert await state.list_objects(prefix) == []  # its delta file was deleted
    await engine.stop()
    await state.close()


async def test_a_retry_puts_back_what_a_dead_keyed_write_half_did(tmp_path, data, monkeypatch):
    """The dead attempt meant to change `a`, add `c` and drop `b`, and died
    after its first object landed. The retry's content matches the index, so
    its delta is empty — yet it rewrites the keys the dead one touched and
    deletes the one it may have added."""

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
    assert len(detail["attempts"][detail["tasks"][0]["id"]]) == 2  # it died, and the retry committed
    assert state.model.unsettled == {}
    assert {p.name: p.read_text() for p in (data / "scores").iterdir()} == {"a.json": "1", "b.json": "1"}
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
