"""ENG: engine crashes, restarts, zombies and fencing (docs/behaviors.md)."""

import asyncio
import json
import os
import signal
import time

import pytest
from solera.executors import Pool
from solera.sdk import Commit, Incremental, Output, Project, Retry, Source, Tick, asset, sensor
from solera.stores import FileStore, Patch

from tests.contract import test_sto as sto
from tests.contract.example import Example
from tests.contract.harness import Refused
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
    assert in_table(table) == {"a": ["3"]}
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
    await solera.settle(1.0)
    assert [r for r in await solera.runs() if r["id"] not in (first, run["id"])] == []  # no repair clock
    await solera.run("copy", timeout=5)  # its reader does not wait
    assert await solera.rows("copy") == {"a": "1", "b": "1"}
    assert in_table(table) == {"a": ["1"], "b": ["1"], "c": ["1"]}
    await solera.run("items")
    assert in_table(table) == {"a": ["1"], "b": ["1"]}
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

        async def read():
            return (await solera.get(f"/runs/{reader['id']}"))["request"]["status"] == "succeeded"

        await solera.until(read, timeout=5)  # it does not wait for the writer
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
