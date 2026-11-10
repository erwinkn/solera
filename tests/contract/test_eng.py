"""ENG: engine crashes, restarts, zombies and fencing (docs/behaviors.md)."""

import asyncio
import json
import os
import signal
import time

import obstore
import pytest
from solera.executors import Pool
from solera.sdk import Commit, Incremental, Output, Project, Retry, Source, Tick, asset, sensor
from solera.stores import FileStore, Patch

from tests.contract import test_sto as sto
from tests.contract.example import Example
from tests.contract.harness import Refused, Storage
from tests.contract.test_sto import (
    Files,
    attempts_of,
    in_table,
    items_on,
    killed_after_writing,
    on,
    rows,
    served,
)


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
    await solera.until(gate.started.is_set)
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
    await asyncio.wait_for(gate.started.wait(), 30)
    second = asyncio.create_task(solera.worker(stage, pool=False))
    await asyncio.sleep(0.3)
    assert not second.done()  # waits for the owner's end, writing nothing
    gate.go.set()
    assert await asyncio.wait_for(first, 30) == 0
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

    work = []

    async def other():
        work[:] = [s for s in await solera.offered(pool, timeout=0) if s["attempt"] != stage["attempt"]]
        return work

    await solera.until(other, 30)
    return work


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


@pytest.mark.parametrize(
    "outputs",
    [
        pytest.param("garbage", id="outputs-not-an-object"),
        pytest.param({"trained": {"ref": 5}}, id="a-ref-not-an-object"),
    ],
)
async def test_eng_9_a_result_the_engine_cannot_use_fails_its_attempt_retryably(solera, outputs):
    calls = []
    await solera.deploy(pool_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    assert await solera.worker(stage) == 0

    def malformed(body):
        body["result"]["outputs"] = outputs
        return json.dumps(body).encode()

    await solera.tamper(stage["run"], stage["attempt"], malformed)
    await solera.restart()
    [retry] = await another_offer(solera, "gpu", stage)
    assert await solera.worker(retry) == 0
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [task] = detail["tasks"]
    first, second = detail["attempts"][task["id"]]
    assert first["outcome"] == "failed" and "commit" not in first
    assert second["outcome"] == "succeeded" and calls == [1, 1]
    assert await solera.read("trained") == {"w": 2}  # the retry's: nothing of the malformed result


@pytest.mark.xfail(
    strict=True, reason="today's engine accepts a result with an unknown status (gap, behaviors.md ENG-9)"
)
async def test_eng_9_a_result_with_an_unknown_status_fails_its_attempt_retryably(solera):
    calls = []
    await solera.deploy(pool_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    assert await solera.worker(stage) == 0

    def unknown(body):
        body["result"]["status"] = "nonsense"
        return json.dumps(body).encode()

    await solera.tamper(stage["run"], stage["attempt"], unknown)
    await solera.restart()

    async def attempts():
        detail = await solera.get(f"/runs/{run['id']}")
        attempts = detail["attempts"].get(detail["tasks"][0]["id"], [])
        return [a for a in attempts if a["outcome"] in ("succeeded", "failed")]  # settled

    await solera.until(attempts)
    [first] = await attempts()
    assert first["outcome"] == "failed" and "commit" not in first, first
    [retry] = await another_offer(solera, "gpu", stage)
    assert await solera.worker(retry) == 0
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert calls == [1, 1]
    assert await solera.read("trained") == {"w": 2}  # the retry's: nothing of the bad result


async def test_eng_9_malformed_telemetry_in_a_result_is_dropped_and_the_attempt_settles(solera):
    calls, sent = [], []
    await solera.deploy(pool_project(calls))
    run = await solera.submit("trained")
    [stage] = await solera.offered("gpu")
    await solera.down()
    assert await solera.worker(stage) == 0

    def garbled(body):
        result = body["result"]
        sent.extend(e["type"] for e in result.get("events", []))
        result["events"] = [*result.get("events", []), {"type": "garbled", "at": "not-a-number"}]
        result["usage"] = {"cpu_seconds": "x"}
        return json.dumps(body).encode()

    await solera.tamper(stage["run"], stage["attempt"], garbled)
    await solera.restart()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert await solera.read("trained") == {"w": 1}
    timeline = [e["type"] for e in await solera.get(f"/runs/{run['id']}/events")]
    assert sent and set(sent) <= set(timeline), (sent, timeline)  # what the worker reported well stands
    assert "garbled" not in timeline
    assert (await solera.client.get("/healthz")).status_code == 200


async def test_eng_11_a_restart_never_cuts_an_attempts_timeout_short_and_stretches_it_by_one_at_most(solera):
    gate, limit = Gate(), 1.5
    project = held_project(gate, retries=Retry(0), timeout=limit)
    await solera.deploy(project, heartbeat_seconds=0.1, cancel_grace=0.1)
    run = await solera.submit("copy")
    await solera.until(gate.started.is_set)
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


BURST, RATE = 20, 2.0  # what an idle engine may ask of its storage: at once, then per second


async def test_eng_10_an_idle_engine_does_not_spin(solera):
    """Between external inputs the engine's work stays within `BURST + RATE ×
    elapsed`: served with a run waiting on a pool no worker polls and another
    waiting an hour for its retry, it asks the namespace's storage and its
    output store next to nothing, and barely uses the CPU."""

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

    storage = solera.storage_server()
    try:
        await solera.deploy(Project(assets=[items, trained, flaky], default_store=store))
        await solera.run("items")
        waiting, flaky_run = await solera.submit("trained"), await solera.submit("flaky")
        await solera.offered("gpu")
        await solera.start()

        async def retry_pending():  # flaky failed once, and waits an hour for its retry
            detail = await solera.get(f"/runs/{flaky_run['id']}")
            return [a["outcome"] for a in detail["attempts"][detail["tasks"][0]["id"]]] == ["failed"]

        await solera.until(retry_pending)
        requests, asked, cpu, wall = storage.requests, store.asked, time.process_time(), time.monotonic()
        await asyncio.sleep(1.0)  # idle: what it does meanwhile is measured
        elapsed = time.monotonic() - wall
        requests = storage.requests - requests
        used = (time.process_time() - cpu) / elapsed
        for run in (waiting, flaky_run):
            assert (await solera.get(f"/runs/{run['id']}"))["request"]["status"] == "running"
    finally:
        await solera.close()
        storage.close()
    assert requests <= BURST + RATE * elapsed, f"{requests} storage requests in {elapsed:.1f} s while idle"
    assert store.asked == asked
    assert used < 0.1, f"{used:.0%} of a CPU while idle"


def test_eng_10_each_storage_server_keeps_its_own_state():
    """What one test's storage server holds, another's never shows, though
    moto keeps one store for the whole process."""

    first = Storage()
    try:
        obstore.put(obstore.store.from_url(f"{first.url}/state"), "contract/control/journal.json", b"{}")
    finally:
        first.close()
    second = Storage()
    try:
        assert second.url != first.url
        assert list(obstore.list(obstore.store.from_url(f"{second.url}/state"))) == []
    finally:
        second.close()


async def test_eng_10_the_storage_count_sees_every_request(solera):
    """What ENG-10 bounds is counted request by request, never coalesced:
    1,000 reads of one object of the namespace's state are 1,000 requests."""

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": "k1", "v": "1"}]

    storage = solera.storage_server()
    try:
        await solera.deploy(Project(assets=[items]))
        await solera.run("items")
        await solera.close()  # nobody else asks it anything
        state = obstore.store.from_url(f"{storage.url}/state")
        path = next(iter(obstore.list(state)))[0]["path"]
        before = storage.requests
        for _ in range(1000):
            await (await obstore.get_async(state, path)).bytes_async()
        assert storage.requests - before == 1000
    finally:
        await solera.close()
        storage.close()


async def backlogged(solera, keys: int = 1) -> dict:
    """Source commits to `feed`, `keys` keys each, until one is refused as
    its upkeep is far behind: the keys taken."""

    taken = {}
    for n in range(1000):
        upsert = {f"k{n:03}-{i:05}": "1" for i in range(keys)}
        try:
            await solera.commit_source("feed", upsert=upsert)
        except Refused as refused:
            assert refused.status == 409 and "retry" in refused.detail, refused
            return taken
        taken.update(upsert)
    raise AssertionError("no commit was ever held back")


def caught_up(view: dict) -> bool:
    return not view["failed"] and not view["backlogged"]


async def test_eng_13_a_source_commit_waits_while_its_upkeep_is_far_behind(solera):
    await Example(solera).start()
    taken = await backlogged(solera)
    assert (await solera.get("/outputs/feed/merges"))["backlogged"]
    await solera.start()  # upkeep runs: merges bring it down
    await merges(solera, "feed", caught_up)
    await solera.commit_source("feed", upsert={"later": "1"})  # taken now
    assert len(await solera.keys("feed")) == len(taken) + 1


async def test_eng_13_attempts_writing_an_output_wait_while_its_upkeep_is_far_behind(solera):
    ex = await Example(solera).start(batch_size=1)
    taken = await backlogged(solera, keys=2)  # as many commits of feed as it takes
    ex.outside.feed.update(taken)
    run = await solera.submit("items")  # twice as many of items: a batch, a commit each
    task = {}

    async def held():
        [shown] = (await solera.get(f"/runs/{run['id']}"))["tasks"]
        task.update(shown)
        return bool(task.get("held"))

    await solera.until(held, 12)
    assert task["status"] == "queued" and task["held"][0] == "merges"
    assert (await solera.get("/outputs/items/merges"))["backlogged"]
    await solera.start()  # upkeep merges; the writer goes on
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    assert len(await solera.keys("items")) == len(taken)


async def merges(solera, output: str, done, seconds: float = 10) -> dict:
    """The output's upkeep as `GET /outputs/{output}/merges` shows it, once `done(view)`."""

    view = {}

    async def shown():
        view.update(await solera.get(f"/outputs/{output}/merges"))
        return done(view)

    await solera.until(shown, seconds)
    return view


def tried(n: int):
    return lambda view: bool(view["failed"]) and view["failed"][0]["uploads"] == n


async def failing_upkeep(solera) -> None:
    await Example(solera).start()
    await backlogged(solera)
    solera.refuse_upkeep("feed")
    await solera.start()


async def alarm(solera) -> tuple[int, str | None]:
    """What the engine shows of an alarm: its health, and the error it gives."""

    return (await solera.client.get("/healthz")).status_code, (await solera.get("/api/diagnostics"))[
        "last_error"
    ]


async def test_eng_14_upkeep_that_keeps_failing_backs_off_alarms_and_tries_again(solera):
    """An output's merges fail three times: it waits 10 minutes, doubling,
    alarmed, and tries again until a merge publishes."""

    try:
        await failing_upkeep(solera)
        [failed] = (await merges(solera, "feed", tried(3)))["failed"]
        assert failed["due"] - failed["at"] == 600  # three tries, then ten minutes
        health, error = await alarm(solera)
        assert health == 503 and error is not None, (health, error)  # alarmed, while it waits
        await asyncio.sleep(1.2)  # upkeep looks again, and waits
        assert (await solera.get("/outputs/feed/merges"))["failed"][0]["uploads"] == 3
        solera.advance(600)
        [failed] = (await merges(solera, "feed", tried(4)))["failed"]
        assert failed["due"] - failed["at"] == 1200  # once more, then twice the wait
    finally:
        solera.refuse_upkeep("feed", False)
    solera.advance(1200)  # it never stops for good: the store is back, a merge publishes
    await merges(solera, "feed", caught_up)

    async def alarm_gone():
        return await alarm(solera) == (200, None)

    await solera.until(alarm_gone)
    await solera.commit_source("feed", upsert={"later": "1"})  # and writes are taken again


async def test_eng_14_merges_cut_short_by_the_engines_stop_count_for_nothing(solera):
    """Three stops, each while a merge's upload is under way, leave the output
    merging at once, where three failed merges would have it wait ten minutes."""

    storage = solera.storage_server()
    try:
        await Example(solera).start()
        await backlogged(solera)
        for _ in range(3):
            hold = storage.hold_upkeep("feed")
            await solera.start()
            await solera.until(hold.entered.is_set)  # a merge's upload under way, stalled
            await solera.down()
            await solera.restart()
            hold.release()  # the cut upload ends; nothing awaits it any more
            view = await solera.get("/outputs/feed/merges")
            assert view["backlogged"] and view["failed"] == []  # counted for nothing
        await solera.start()
        await merges(solera, "feed", caught_up)  # no clock moved
    finally:
        await solera.close()
        storage.close()


async def test_eng_14_an_operator_clear_merges_at_once(solera):
    try:
        await failing_upkeep(solera)
        await merges(solera, "feed", tried(3))
    finally:
        solera.refuse_upkeep("feed", False)
    cleared = await solera.post("/merges:clear", {"output": "feed", "partition": ""})
    assert cleared["cleared"]
    await merges(solera, "feed", caught_up)  # no clock moved: at once
    await solera.commit_source("feed", upsert={"later": "1"})


# -- target (PR 2) --

table = sto.table  # the fixture: a Postgres store a test makes misbehave


# -- ENG-7 ------------------------------------------------------------------------


async def late_write(solera, store) -> asyncio.Event:
    """`items` writes a=1; its next attempt sends a=2, b=2, and its worker is
    killed before the write lands; the retry commits a=3. The returned
    event lets the late write land."""

    writes = [rows("a=1"), rows("a=2", "b=2"), rows("a=3")]
    await served(solera, items_on(store, writes, retries=Retry(1, delay=0)))
    await solera.run("items")
    store.late = late = asyncio.Event()
    run = await solera.submit("items")
    await solera.until(store.sent.is_set)
    assert await solera.kill_workers() == 1
    assert (await solera.wait(run))["request"]["status"] == "succeeded"
    return late


@pytest.mark.postgres
async def test_eng_7_a_late_write_to_a_table_is_refused_by_its_version_check(solera, table):
    late = await late_write(solera, table)
    late.set()
    await table.landing
    [refused] = table.outcomes
    assert isinstance(refused, Exception), refused
    assert in_table(await solera.loaded("items")) == {"a": ["3"]}
    assert await solera.rows("items") == {"a": "3"}


async def test_eng_7_a_late_write_to_files_lands_under_names_nothing_reads(solera):
    files = Files(solera.root / "files")
    late = await late_write(solera, files)
    late.set()
    await files.landing
    assert files.outcomes == ["written"]
    assert await solera.rows("items") == {"a": "3"}
    await solera.run("copy")
    assert await solera.rows("copy") == {"a": "3"}


# -- ENG-8 ------------------------------------------------------------------------


@pytest.mark.postgres
async def test_eng_8_a_dead_writers_rows_are_repaired_by_the_next_attempt_and_nothing_waits_for_it(
    solera, table
):
    """`items`' attempt writes c and dies, out of retries. Nothing runs on
    its own and `copy` reads `items` at once; `items`' next attempt,
    though it writes nothing, deletes c."""

    writes = [rows("a=1", "b=1"), Patch(rows("c=1")), Patch([])]
    await served(solera, items_on(table, writes, retries=Retry(0)))
    first = (await solera.run("items"))["request"]["id"]
    table.hang_after_write()
    run = await solera.submit("items")
    await killed_after_writing(solera, table)
    assert (await solera.wait(run))["request"]["status"] == "failed"
    marked = (await solera.run("marker"))["request"]["id"]  # a barrier: the engine has worked on since
    assert [
        r for r in await solera.runs() if r["id"] not in (first, run["id"], marked)
    ] == []  # no repair clock
    await solera.run("copy", timeout=5)  # its reader does not wait
    assert await solera.rows("copy") == {"a": "1", "b": "1"}
    assert in_table(await solera.loaded("items")) == {"a": ["1"], "b": ["1"], "c": ["1"]}
    await solera.run("items")
    assert in_table(await solera.loaded("items")) == {"a": ["1"], "b": ["1"]}
    assert await solera.rows("items") == {"a": "1", "b": "1"}


# -- ENG-17 -----------------------------------------------------------------------


@pytest.mark.postgres
async def test_eng_17_a_dead_writers_version_is_an_anomaly_never_a_change(solera, table):
    """`items`' attempt A writes k2@6 and dies. `copy`, which owes k2,
    loads it: an alarm names `items`, k2 and A; `copy` does not process
    it, nor read it again, until `items` has redone k2; `items`' log never
    records it."""

    received = []
    writes = [rows("k1=1", "k2=1"), Patch(rows("k2=6")), Patch(rows("k2=7"))]
    await served(solera, items_on(table, writes, received, retries=Retry(0)))
    first = (await solera.run("items"))["request"]["id"]
    table.hang_after_write()
    run = await solera.submit("items")
    await killed_after_writing(solera, table)
    [dead] = attempts_of(await solera.wait(run), "items")
    await solera.run("copy", timeout=5)
    assert await solera.alarmed("items", "k2", dead["id"])
    assert all("k2" not in seen for _, seen in received)  # never processed as a change
    await solera.run("copy")
    assert all("k2" not in seen for _, seen in received)  # nor read again
    assert [c["run"] for c in await solera.commits("items")] == [first]
    await solera.run("items")
    await solera.run("copy")
    assert received[-1][1] == {"k2": "7"}
    assert await solera.rows("copy") == {"k1": "1", "k2": "7"}


@pytest.mark.postgres
async def test_eng_17_a_version_of_an_attempt_still_live_is_no_anomaly(solera, table):
    """`items`' attempt writes k2@6 and is still live when `copy`, which
    owes k2@2, loads k2@6: `copy` processes it as loaded, no alarm, and k2
    settles once that attempt commits."""

    received, go = [], asyncio.Event()
    writes = [rows("k1=1", "k2=1"), Patch(rows("k2=2")), Patch(rows("k2=6"))]
    await solera.deploy(items_on(table, writes, received))
    await solera.run("items")
    await solera.run("copy")
    await solera.run("items")
    table.landed.clear()
    table.after_write = go.wait
    run = await solera.submit("items")
    try:
        await solera.until(table.landed.is_set)
        reader = await solera.submit("copy")

        async def decided():  # it ran, or it says why it waits
            detail = await solera.get(f"/runs/{reader['id']}")
            return detail["request"]["status"] in ("succeeded", "failed") or any(
                t.get("held") for t in detail["tasks"]
            )

        await solera.until(decided)
        detail = await solera.get(f"/runs/{reader['id']}")
        assert detail["request"]["status"] == "succeeded", detail["tasks"]  # it does not wait for the writer
    finally:
        go.set()
    assert received[-1] == (([], ["k2"], []), {"k2": "6"})
    assert not await solera.alarmed("k2")
    assert (await solera.wait(run))["request"]["status"] == "succeeded"
    assert await solera.stale("copy") == []
    await solera.run("copy")
    assert len(received) == 2 and await solera.rows("copy") == {"k1": "1", "k2": "6"}


# -- ENG-18 -----------------------------------------------------------------------


@pytest.mark.postgres
async def test_eng_18_an_asset_never_builds_on_a_dead_write_to_its_own_output(solera, table):
    """`tally`, on Postgres, holds 4. Its attempt reads 4, writes 5 and dies
    before its commit: its retry's `ctx.load()` would return 5, so `tally`
    owes a full run instead of adding k5 to it."""

    seen, feed = [], {f"k{i}": "1" for i in range(1, 5)}

    @asset(outputs=Output("items", key="id"))
    def items():
        return [{"id": k, "v": v} for k, v in feed.items()]

    @asset(
        inputs={"items": Incremental()},
        outputs=Output("tally", key="id", **on(table, id="text", n="bigint")),
        retries=Retry(1, delay=0),
    )
    async def tally(ctx, items: list):
        batch, held = ctx.batch["items"], await ctx.load()
        before = 0 if batch.reset or not held else held[0]["n"]
        seen.append((batch.reset, sorted(batch.added), before))
        return [{"id": "total", "n": before + len(batch.added) - len(batch.removed)}]

    await served(solera, Project(assets=[items, tally], stores={"s": table}))
    await solera.run("items")
    await solera.run("tally")
    feed["k5"] = "1"
    await solera.run("items")
    table.hang_after_write()
    run = await solera.submit("tally")
    await killed_after_writing(solera, table)
    await solera.wait(run)
    for _ in range(2):
        if not await solera.stale("tally"):
            break
        await solera.run("tally", expect=None)
    assert seen[:2] == [(False, ["k1", "k2", "k3", "k4"], 0), (False, ["k5"], 4)]  # the second died
    after = seen[2:]
    assert all(before != 5 for _, _, before in after), after  # nothing built on the dead write
    assert any(reset and added == sorted(feed) for reset, added, _ in after), after  # a full run instead
    assert [r["n"] for r in await solera.read("tally")] == [5]
