"""ENG: engine crashes, restarts, zombies and fencing (docs/behaviors.md)."""

import asyncio
import json
import os
import signal
import time

import pytest
from solera.executors import Pool
from solera.sdk import Commit, Output, Project, Retry, Source, Tick, asset, sensor
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Refused


class Gate:
    """A producer that waits to be let go, counting its calls."""

    def __init__(self):
        self.calls = 0
        self.started = asyncio.Event()
        self.go = asyncio.Event()

    async def __call__(self, value):
        self.calls += 1
        self.started.set()
        await self.go.wait()
        return value


def held_project(gate: Gate, executor=None, **decl) -> Project:
    @asset(outputs=Output("copy", key="id"), executor=executor, **decl)
    async def copy():
        return await gate([{"id": "k1", "v": "1"}, {"id": "k2", "v": "1"}])

    return Project(assets=[copy])


def pooled_project(calls: list) -> Project:
    @asset(outputs=Output("trained"), executor=Pool("gpu")())
    def trained():
        calls.append(1)
        return {"w": len(calls)}

    return Project(assets=[trained])


async def test_eng_1_a_launched_attempt_survives_a_restart_and_commits_once(solera):
    gate = Gate()
    await solera.deploy(held_project(gate))
    run = await solera.submit("copy")
    while not gate.started.is_set():
        await solera.settle(0.05)
    await solera.restart()
    gate.go.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert gate.calls == 1
    [task] = detail["tasks"]
    assert [a["outcome"] for a in detail["attempts"][task["id"]]] == ["succeeded"]
    assert await solera.rows("copy") == {"k1": "1", "k2": "1"}


async def test_eng_3_a_pool_worker_finishes_while_the_engine_is_down(solera):
    calls = []
    await solera.deploy(pooled_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    assert await solera.worker(stage) == 0
    await solera.restart()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert calls == [1] and await solera.read("trained") == {"w": 1}


async def test_eng_6_a_worker_started_twice_runs_once(solera):
    gate = Gate()
    project = held_project(gate, executor=Pool("gpu")())
    await solera.deploy(project)
    run = await solera.submit("copy")
    [stage] = await solera.offered("gpu")
    first = asyncio.create_task(solera.worker(stage, pool=False))
    await gate.started.wait()
    second = asyncio.create_task(solera.worker(stage, pool=False))
    await asyncio.sleep(0.3)
    assert not second.done()  # waits for the owner's end, writing nothing
    gate.go.set()
    assert await first == 0
    detail = await solera.wait(run)
    assert await asyncio.wait_for(second, 30) == 0  # once the attempt is over
    assert detail["request"]["status"] == "succeeded" and gate.calls == 1
    [task] = detail["tasks"]
    assert [a["outcome"] for a in detail["attempts"][task["id"]]] == ["succeeded"]
    assert await solera.rows("copy") == {"k1": "1", "k2": "1"}


async def test_eng_6_pool_workers_racing_for_one_attempt_run_it_once(solera):
    calls = []
    await solera.deploy(pooled_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    assert await asyncio.gather(solera.worker(stage), solera.worker(stage)) == [0, 0]
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert calls == [1] and await solera.read("trained") == {"w": 1}
    assert await solera.offered("gpu", timeout=0) == []


def counted_project(calls: list, rows: dict) -> Project:
    """`items` writes `rows` as they are, counting its calls; `trained` runs on
    the `gpu` pool, whose workers the test plays."""

    @asset(outputs=Output("items", key="id"))
    def items():
        calls.append("items")
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(outputs=Output("trained"), executor=Pool("gpu")())
    def trained():
        calls.append("trained")
        return {"w": 1}

    return Project(assets=[items, trained])


async def test_eng_4_a_new_engine_fences_the_old_one_and_loses_nothing_acknowledged(solera):
    calls, rows = [], {"k1": "1"}
    project = counted_project(calls, rows)
    await solera.deploy(project)
    first = await solera.run("items")
    pooled = await solera.submit("trained")  # acknowledged: queued on the pool
    later = await solera.submit("items")  # acknowledged, not yet dispatched
    successor = solera.successor()
    try:
        await successor.deploy(project)  # a rolling deploy: B takes the namespace while A runs
        with pytest.raises(Refused) as refused:
            await solera.submit("items")  # A's next write finds itself replaced
        assert refused.value.status == 503
        assert (await solera.client.get("/healthz")).status_code == 503
        await solera.start()
        await asyncio.sleep(0.2)  # served on, A acts no more: it runs nothing it had accepted
        assert calls == ["items"]

        assert {r["id"] for r in await successor.runs()} >= {
            first["request"]["id"],
            pooled["id"],
            later["id"],
        }
        assert (await successor.wait(later))["request"]["status"] == "succeeded"
        [stage] = await successor.offered("gpu")
        assert await successor.worker(stage) == 0
        assert (await successor.wait(pooled))["request"]["status"] == "succeeded"
        assert calls == ["items", "items", "trained"]

        rows["k2"] = "2"
        await successor.run("items")
        await asyncio.sleep(0.2)  # whatever A still holds, it deletes nothing of B's
        assert await successor.rows("items") == {"k1": "1", "k2": "2"}
        assert await successor.read("trained") == {"w": 1}
    finally:
        await successor.close()


async def answers(solera, ex) -> dict:
    """What every read of the API answers about the namespace."""

    names = sorted(solera.project.manifest["assets"])
    outputs = sorted(solera.project.manifest["outputs"])
    paths = [
        "/runs",
        "/tasks",
        "/assets",
        "/assets:status",
        "/automations",
        "/executors",
        "/repairs",
        "/cleanups",
    ]
    paths += [f"/runs/{r['id']}" for r in await solera.runs()]
    paths += [f"/runs/{r['id']}/events" for r in await solera.runs()]
    for name in names:
        paths += [f"/assets/{name}", f"/assets/{name}/stale-keys", f"/assets/{name}/outcomes"]
        paths += [f"/assets/{name}/history", f"/assets/{name}/inputs", f"/partitions/{name}"]
    for name in outputs:
        paths += [f"/outputs/{name}/heads", f"/outputs/{name}/keys"]
    out = {}
    for path in paths:
        response = await solera.request("GET", path, ok=False)
        body = response.json() if response.content else None
        if isinstance(body, dict):
            body.pop("now", None)  # when it was asked
        out[path] = (response.status_code, body)
    out["content"] = {
        name: await solera.read(name) for name in ("items", "tally", "copy", "checks", "filtered")
    }
    return out


async def test_eng_5_a_restarted_engine_answers_every_read_as_before(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally")
    ex.failing["k2"] = ValueError("bad row")
    await solera.run("checks", expect=None)  # k2's key fails
    await solera.run("filtered")
    await ex.feed({"k1": "2"}, removes=["k3"])
    await ex.factor("w2")
    await solera.cancel((await solera.submit("copy"))["id"])
    await solera.settle(0.1)
    before = await answers(solera, ex)
    await solera.restart()
    after = await answers(solera, ex)
    assert after.keys() == before.keys()
    assert [p for p in before if after[p] != before[p]] == []


def pool_project(calls: list) -> Project:
    @asset(outputs=Output("trained"), executor=Pool("gpu")(), retries=Retry(1, delay=0))
    def trained():
        calls.append(1)
        return {"w": len(calls)}

    return Project(assets=[trained])


async def another_offer(solera, pool: str, stage: dict) -> list[dict]:
    """What the pool offers next, once the attempt `stage` names is no longer offered."""

    for _ in range(250):
        if work := [s for s in await solera.offered(pool) if s["attempt"] != stage["attempt"]]:
            return work
        await solera.settle(0.02)
    raise AssertionError("nothing else offered")


async def test_eng_9_a_config_value_past_64_bits_is_a_400_and_the_engine_goes_on(solera):
    calls, rows = [], {"k1": "1"}
    await solera.deploy(counted_project(calls, rows))
    with pytest.raises(Refused) as refused:
        await solera.submit("items", config={"n": 2**70})
    assert refused.value.status == 400 and "64-bit" in refused.value.detail
    await solera.run("items", config={"n": 2**63})
    await solera.restart()  # what it took, it holds
    await solera.run("items")
    assert calls == ["items", "items"] and await solera.rows("items") == {"k1": "1"}


@pytest.mark.parametrize(
    "route,body",
    [
        ("/runs", "x"),
        ("/runs", {"targets": 5}),
        ("/assets/items/keys:retry", None),
        ("/assets/items/keys:retry", []),
        ("/cleanups:clear", {"output": []}),
        ("/sources/feed/commit", 1),
    ],
)
async def test_eng_9_a_malformed_body_is_a_422_and_the_engine_goes_on(solera, route, body):
    calls, rows = [], {"k1": "1"}
    await solera.deploy(counted_project(calls, rows))
    response = await solera.request("POST", route, ok=False, json=body)
    assert response.status_code == 422, response.text
    await solera.run("items")
    assert (await solera.client.get("/healthz")).status_code == 200


@pytest.mark.parametrize(
    "body",
    [
        b'{"engine": "e"}',
        b'{"state": "sealed", "worker_id": "w"}',
        b"",
        b"not json",
        b'{"state": "open", "worker_id": "w"}',
    ],
)
async def test_eng_9_a_malformed_control_file_fails_its_attempt_retryably(solera, body):
    calls = []
    await solera.deploy(pool_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    await solera.tamper(stage["run"], stage["attempt"], lambda _: body)
    await solera.restart()
    [retry] = await another_offer(solera, "gpu", stage)
    assert await solera.worker(retry) == 0
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [task] = detail["tasks"]
    first, second = detail["attempts"][task["id"]]
    assert first["outcome"] == "failed" and "malformed control file" in first["error"]
    assert second["outcome"] == "succeeded" and calls == [1]
    assert await solera.read("trained") == {"w": 1}


async def test_eng_9_a_malformed_worker_result_is_settled_without_its_bad_parts(solera):
    calls = []
    await solera.deploy(pool_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    assert await solera.worker(stage) == 0

    def garbled(body):
        result = body["result"]
        result["events"] = [*result.get("events", []), {"type": "garbled", "at": "not-a-number"}]
        result["usage"] = {"cpu_seconds": "x"}
        return json.dumps(body).encode()

    await solera.tamper(stage["run"], stage["attempt"], garbled)
    await solera.restart()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert await solera.read("trained") == {"w": 1}
    timeline = [e["type"] for e in await solera.get(f"/runs/{run['id']}/events")]
    assert "garbled" not in timeline and {"booted", "computing", "stored"} <= set(timeline), timeline
    assert (await solera.client.get("/healthz")).status_code == 200


async def test_eng_11_a_restart_never_cuts_an_attempts_timeout_short_and_stretches_it_by_one_at_most(solera):
    gate, limit = Gate(), 1.5
    project = held_project(gate, retries=Retry(0), timeout=limit)
    await solera.deploy(project, heartbeat_seconds=0.1, cancel_grace=0.1)
    run = await solera.submit("copy")
    while not gate.started.is_set():
        await solera.settle(0.02)
    began = time.monotonic()
    solera.advance(3600)  # the new engine's clock says the launch was an hour ago
    await solera.restart()
    await solera.settle(0.1)
    detail = await solera.get(f"/runs/{run['id']}")
    assert time.monotonic() - began < limit and detail["request"]["status"] == "running"
    detail = await solera.wait(run)
    ended = time.monotonic() - began
    [task] = detail["tasks"]
    assert detail["request"]["status"] == "failed" and task["error"] == "timeout"
    assert limit <= ended < 2 * limit + 1.0, ended
    assert gate.calls == 1


def sensed_project() -> Project:
    @asset(deps=["uploads"])
    def ingest() -> int:
        return 1

    @sensor(every=1, commits=["uploads"])
    def watch(ctx) -> Tick | None:
        return None

    return Project(assets=[ingest], sources=[Source("uploads", key="id")], sensors=[watch])


async def test_eng_12_a_tick_in_flight_at_a_restart_is_forgotten_and_the_sensor_is_due_from_its_cursor(
    solera,
):
    await solera.deploy(sensed_project())
    [tick] = await solera.sensor_ticks()
    assert tick["sensor"] == "watch" and tick["cursor"] is None
    await solera.sensor_post(tick, Tick(cursor="c1", commits=[Commit("uploads", upsert={"a": "1"})]))
    assert await solera.sensor_ticks() == []  # due again in a second
    [tick] = await solera.sensor_ticks(wait=5)
    assert tick["cursor"] == "c1"
    await solera.restart()
    with pytest.raises(Refused) as refused:
        await solera.sensor_post(tick, Tick(cursor="c2"))
    assert refused.value.status == 409
    [again] = await solera.sensor_ticks()  # due at once, from its durable cursor
    assert again["sensor"] == "watch" and again["cursor"] == "c1" and again["tick"] != tick["tick"]
    await solera.sensor_post(again, Tick(cursor="c2"))
    assert (await solera.keys("uploads")).keys() == {"a"}


async def test_eng_15_a_sigterm_stops_solera_serve_cleanly(solera):
    """systemd, Railway and the rest stop a service with SIGTERM on every
    redeploy: a graceful stop is a success, not a failed unit."""

    process = await solera.serve_process()
    try:
        process.send_signal(signal.SIGTERM)  # the process itself, as a platform signals it
        _, err = await asyncio.to_thread(process.communicate, timeout=60)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
    assert process.returncode == 0, err[-2000:]
    assert "Application shutdown complete" in err


class CountingStore(FileStore):
    """A user's store that counts what the engine and its workers ask of it."""

    def __init__(self, path):
        super().__init__(path)
        self.asked = 0

    async def load(self, *args, **kw):
        self.asked += 1
        return await super().load(*args, **kw)

    async def store(self, *args, **kw):
        self.asked += 1
        return await super().store(*args, **kw)


async def test_eng_10_an_idle_engine_does_not_spin(solera):
    """Between external inputs the engine's work stays bounded: served with
    a run waiting on a pool no worker polls and another waiting an hour for
    its retry, it barely uses the CPU and asks its store nothing."""

    store = CountingStore(solera.root / "data")

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "k1", "v": "1"}]

    @asset(outputs=Output("trained"), executor=Pool("gpu")())
    def trained(items: list):
        return {"w": len(items)}

    @asset(retries=Retry(1, delay=3600))
    def flaky(items: list) -> int:
        raise ValueError("down")

    await solera.deploy(Project(assets=[items, trained, flaky], default_store=store))
    await solera.run("items")
    waiting = [await solera.submit("trained"), await solera.submit("flaky")]
    await solera.offered("gpu")
    await solera.start()
    await asyncio.sleep(0.3)  # its first passes: dispatch, adoption, upkeep, history
    asked, cpu, wall = store.asked, time.process_time(), time.monotonic()
    await asyncio.sleep(1.0)
    used = (time.process_time() - cpu) / (time.monotonic() - wall)
    assert used < 0.1, f"{used:.0%} of a CPU while idle"
    assert store.asked == asked
    for run in waiting:
        assert (await solera.get(f"/runs/{run['id']}"))["request"]["status"] == "running"


async def test_eng_13_a_source_commit_waits_while_its_upkeep_is_far_behind(solera):
    await Example(solera).start()
    refused, n = None, 0
    while refused is None and n < 200:
        n += 1
        try:
            await solera.commit_source("feed", upsert={f"k{n:03}": "1"})
        except Refused as error:
            refused = error
    assert refused is not None and refused.status == 409 and "retry" in refused.detail
    assert (await solera.get("/outputs/feed/merges"))["backlogged"]
    await solera.start()  # upkeep runs: merges bring it down

    async def caught_up():
        return not (await solera.get("/outputs/feed/merges"))["backlogged"]

    for _ in range(200):
        if await caught_up():
            break
        await asyncio.sleep(0.05)
    await solera.commit_source("feed", upsert={f"k{n:03}": "1"})  # taken now
    assert len(await solera.keys("feed")) == n


async def test_eng_13_attempts_writing_an_output_wait_while_its_upkeep_is_far_behind(solera):
    ex = await Example(solera).start(batch_size=1)
    await ex.feed({f"k{i:03}": "1" for i in range(70)}, items=False)
    run = await solera.submit("items")  # 70 batches: a commit of items each
    for _ in range(600):
        await solera.settle(0.02)
        [task] = (await solera.get(f"/runs/{run['id']}"))["tasks"]
        if task.get("held"):
            break
    assert task["status"] == "queued" and task["held"][0] == "merges"
    assert (await solera.get("/outputs/items/merges"))["backlogged"]
    await solera.start()  # upkeep merges; the writer goes on
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert len(await solera.keys("items")) == 70


async def merges(solera, output: str, done, seconds: float = 10) -> dict:
    """The output's upkeep as `GET /outputs/{output}/merges` shows it, once `done(view)`."""

    for _ in range(int(seconds / 0.1)):
        view = await solera.get(f"/outputs/{output}/merges")
        if done(view):
            return view
        await asyncio.sleep(0.1)
    raise AssertionError(view)


def tried(n: int):
    return lambda view: bool(view["failed"]) and view["failed"][0]["uploads"] == n


async def failing_upkeep(solera) -> None:
    await Example(solera).start()
    for n in range(1, 64):
        await solera.commit_source("feed", upsert={f"k{n:03}": "1"})
    solera.refuse_upkeep("feed")
    await solera.start()


async def test_eng_14_upkeep_that_keeps_failing_backs_off_alarms_and_tries_again(solera):
    try:
        await failing_upkeep(solera)
        [failed] = (await merges(solera, "feed", tried(3)))["failed"]
        assert failed["due"] - failed["at"] == 600  # three tries, then ten minutes
        assert "key index feed" in (await solera.get("/api/diagnostics"))["last_error"]
        await asyncio.sleep(1.2)  # upkeep looks again, and waits
        assert (await solera.get("/outputs/feed/merges"))["failed"][0]["uploads"] == 3
        solera.advance(600)
        [failed] = (await merges(solera, "feed", tried(4)))["failed"]
        assert failed["due"] - failed["at"] == 1200  # once more, then twice the wait
    finally:
        solera.refuse_upkeep("feed", False)
    solera.advance(1200)  # it never stops for good: the store is back, a merge publishes
    view = await merges(solera, "feed", lambda view: not view["failed"] and view["layers"] < 63)
    assert not view["backlogged"]
    for _ in range(100):  # and the alarm goes
        if "key index feed" not in ((await solera.get("/api/diagnostics"))["last_error"] or ""):
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("the alarm stays")


async def test_eng_14_an_operator_clear_merges_at_once(solera):
    try:
        await failing_upkeep(solera)
        await merges(solera, "feed", tried(3))
    finally:
        solera.refuse_upkeep("feed", False)
    cleared = await solera.post("/merges:clear", {"output": "feed", "partition": ""})
    assert cleared["cleared"]
    await merges(solera, "feed", lambda view: not view["failed"] and view["layers"] < 63)
