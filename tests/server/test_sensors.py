"""Sensors (docs/lifecycle.md §11): a tick is dispatched with its sensor's
cursor and a snapshot of the sources it may commit to, and its outcome is
applied all or nothing — refused when a source moved, answered again when
retried, recorded not at all when nothing changed."""

import asyncio
import os
import socket
import threading
import time

import httpx
import pytest
from solera import lifecycle
from solera.executors import AWSECS, Pool
from solera.sdk import (
    Commit,
    DynamicPartitions,
    Every,
    Observed,
    Project,
    RegistrationError,
    RunRequest,
    Source,
    Tick,
    asset,
    sensor,
)
from solera_server import sensors as engine_sensors
from solera_server.api import create_app
from solera_server.engine import Conflict, Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.state import State
from solera_worker.sensors import ORPHANED, OVERRAN, LocalSensorChannel, run_sensor_host


async def open_engine(tmp_path, project, *, host=False, **kw) -> tuple[State, Engine]:
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    placements = {"Local": lambda s, c: InlinePlacement(c, project)}
    if host:
        kw["sensor_host"] = lambda engine: run_sensor_host(LocalSensorChannel(engine), project, "local")
    engine = Engine(
        state, project.manifest, placements=placements, clock=state.clock, eval_interval=0.02, **kw
    )
    await engine.initialize()
    return state, engine


async def dispatch(engine, name) -> dict:
    engine.sensor_due.pop(name, None)
    ticks = (await engine.sensor_next("local", engine.manifest["deploy"], "test", 8, 0))["ticks"]
    return next(t for t in ticks if t["sensor"] == name)


def recording(state) -> list[list[dict]]:
    """Each `record()` call's events, from here on."""

    calls, record = [], state.record

    def spy(*events, **kw):
        calls.append([dict(e) for e in events])
        record(*events, **kw)

    state.record = spy
    return calls


def rows(engine, sensor=None) -> list[dict]:
    return [r for r in engine.history.lake.unwritten("ticks") if sensor is None or r["sensor"] == sensor]


async def until(condition, timeout=10.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        assert loop.time() < deadline, "timed out"
        await asyncio.sleep(0.01)


def feed_project(**decl):
    @asset(deps=["uploads"])
    def ingest() -> int:
        return 1

    @sensor(every=60, commits=["uploads", "feed"], **decl)
    def watch(ctx) -> Tick | None:
        return None

    sources = [Source("uploads", key="id"), Source("feed")]
    return Project(assets=[ingest], sources=sources, sensors=[watch])


async def test_a_tick_commits_and_requests_runs_in_one_record(tmp_path):
    """Source commits, run submissions, the cursor and the accepted outcome
    go in one `record()`, so one journal segment holds all or none."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    assert tick["cursor"] is None and set(tick["snapshot"]) == {"uploads", "feed"}
    calls = recording(state)
    outcome = Tick(
        cursor="page-2",
        commits=[Commit("uploads", upsert={"a": "1", "b": "1"}), Commit("feed", version="v7")],
        runs=[RunRequest("ingest")],
    ).to_json()
    answer = await engine.sensor_post("watch", tick["tick"], outcome)
    assert answer["accepted"] and len(answer["runs"]) == 1 and set(answer["commits"]) == {"uploads", "feed"}
    assert len(calls) == 1
    assert [e["type"] for e in calls[0]] == [
        "SourceCommitted",
        "SourceCommitted",
        "RunSubmitted",
        "SensorAdvanced",
    ]
    m = state.model
    assert m.sensors["watch"]["cursor"] == "page-2"
    assert m.heads[("feed", "")]["version"] == "v7"
    for source, head in answer["commits"].items():
        assert engine._head_id(source) == head
    run = m.runs[answer["runs"][0]]
    assert run["tags"] == {"sensor": "watch", "tick": tick["tick"]} and run["by"] == "sensor watch"
    detail = await engine.run_until(run["id"], 20)
    assert detail["request"]["status"] == "succeeded"
    assert rows(engine, "watch")[-1]["outcome"] == "requested"
    await state.close()


async def test_nothing_changed_records_nothing_and_a_cursor_alone_only_advances(tmp_path):
    """ "Unchanged" is no version, no key change and no new cursor: nothing
    is recorded. A new cursor alone is `SensorAdvanced`: no commit, no
    consumer woken."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    await engine.commit_source("feed", version="v1")
    tick = await dispatch(engine, "watch")
    before = state.recorded
    same = Tick(commits=[Commit("feed", version="v1")]).to_json()  # the version it has
    assert await engine.sensor_post("watch", tick["tick"], same) == {
        "accepted": True,
        "runs": [],
        "commits": {},
    }
    assert state.recorded == before and rows(engine)[-1]["outcome"] == "skipped"
    tick = await dispatch(engine, "watch")
    calls = recording(state)
    await engine.sensor_post("watch", tick["tick"], Tick(cursor=17).to_json())
    assert [[e["type"] for e in c] for c in calls] == [["SensorAdvanced"]]
    assert state.model.sensors["watch"]["cursor"] == 17 and rows(engine)[-1]["outcome"] == "advanced"
    tick = await dispatch(engine, "watch")
    assert tick["cursor"] == 17
    before = state.recorded
    await engine.sensor_post("watch", tick["tick"], Tick(cursor=17).to_json())  # the same cursor
    assert state.recorded == before
    await state.close()


async def test_a_tick_that_saw_a_source_since_moved_is_refused_whole(tmp_path):
    """An API client commits `v2` while a tick that saw `v1` runs. The tick
    is refused whole, whatever its version says — even the commit to the
    other source and its run request — and its delta files go."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    await engine.commit_source("feed", version="v1")
    tick = await dispatch(engine, "watch")
    await engine.commit_source("feed", version="v2")
    before, files = state.recorded, set(await state.list_objects("keys/"))
    outcome = Tick(
        commits=[Commit("uploads", keys={"a": "1"}), Commit("feed", version="v1")],
        runs=[RunRequest("ingest")],
    ).to_json()
    with pytest.raises(Conflict, match="moved"):
        await engine.sensor_post("watch", tick["tick"], outcome)
    assert state.recorded == before and set(await state.list_objects("keys/")) == files
    assert state.model.heads[("feed", "")]["version"] == "v2"
    assert rows(engine)[-1]["outcome"] == "refused"
    with pytest.raises(Conflict, match="not current"):  # decided: its claim is consumed
        await engine.sensor_post("watch", tick["tick"], outcome)
    tick = await dispatch(engine, "watch")  # the next tick observes again
    await engine.sensor_post("watch", tick["tick"], Tick(commits=[Commit("feed", version="v3")]).to_json())
    assert state.model.heads[("feed", "")]["version"] == "v3"
    await state.close()


async def test_a_source_moving_while_the_tick_is_prepared_refuses_it(tmp_path, monkeypatch):
    """Preparing reads the key index (an await): a commit landing meanwhile
    refuses the tick, and the delta file it wrote goes."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    prepare, raced = engine._prepare_commit, []

    async def racing(*args, **kw):
        prepared = await prepare(*args, **kw)
        if not raced:
            raced.append(True)
            await engine.commit_source("uploads", upsert={"z": "1"})
        return prepared

    monkeypatch.setattr(engine, "_prepare_commit", racing)
    files = set(await state.list_objects("keys/"))
    with pytest.raises(Conflict, match="moved while"):
        await engine.sensor_post(
            "watch", tick["tick"], Tick(commits=[Commit("uploads", keys={"a": "1"})]).to_json()
        )
    assert list((await engine.list_keys("uploads"))["keys"]) == ["z"]  # the API's commit, not the tick's
    assert len(set(await state.list_objects("keys/")) - files) == 1  # the API commit's delta alone
    await state.close()


async def test_a_retried_post_is_answered_again_and_applied_once(tmp_path):
    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    outcome = Tick(commits=[Commit("feed", version="v1")], runs=[RunRequest("ingest")]).to_json()
    first = await engine.sensor_post("watch", tick["tick"], outcome)
    before, runs = state.recorded, set(state.model.runs)
    assert await engine.sensor_post("watch", tick["tick"], outcome) == first
    assert state.recorded == before and set(state.model.runs) == runs
    await state.close()


async def test_late_and_pre_restart_ticks_get_409(tmp_path, monkeypatch):
    """A tick over its timeout is dropped; its late post is refused. Claims
    are memory only: after a restart, a tick from before is refused too,
    and the sensor is due again at once, from its durable cursor."""

    monkeypatch.setattr(engine_sensors, "POST_GRACE", 0.0)
    project = feed_project(timeout=0.05)
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    await asyncio.sleep(0.1)
    engine._sensor_sweep()
    assert "watch" not in state.model.ticks and rows(engine)[-1]["error"] == "timed out"
    with pytest.raises(Conflict):
        await engine.sensor_post("watch", tick["tick"], Tick(cursor="late").to_json())
    tick = await dispatch(engine, "watch")
    await engine.sensor_post("watch", tick["tick"], Tick(cursor="c1").to_json())
    tick = await dispatch(engine, "watch")
    await state.close()

    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = Engine(state, project.manifest, clock=state.clock)
    await engine.initialize()
    with pytest.raises(Conflict):
        await engine.sensor_post("watch", tick["tick"], Tick(cursor="c2").to_json())
    again = (await engine.sensor_next("local", engine.manifest["deploy"], "test", 8, 0))["ticks"]
    assert [(t["sensor"], t["cursor"]) for t in again] == [("watch", "c1")]
    await state.close()


async def test_what_a_sensor_may_not_do_is_refused_whole(tmp_path):
    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    await engine.commit_source("feed", version="v1")
    for outcome in (
        Tick(commits=[Commit("elsewhere", version="x")]),  # not in its commits=
        Tick(commits=[Commit("feed", version="a"), Commit("feed", version="b")]),
        Tick(commits=[Commit("feed", version="v2")], runs=[RunRequest("nope")]),
        Tick(commits=[Commit("feed", keys={"a": "1"})]),  # an unkeyed source takes a version
    ):
        tick = await dispatch(engine, "watch")
        before = state.recorded
        with pytest.raises(ValueError):
            await engine.sensor_post("watch", tick["tick"], outcome.to_json())
        assert state.recorded == before and rows(engine)[-1]["outcome"] == "failed"
    assert state.model.heads[("feed", "")]["version"] == "v1"
    await state.close()


async def test_a_tick_pins_what_it_reads_until_decided(tmp_path):
    """The snapshot names a keyed source's pinned index; the tick's reader
    pin keeps those files from collection until it is decided (§11.3)."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    await engine.commit_source("uploads", keys={"a": "1"})
    tick = await dispatch(engine, "watch")
    assert tick["snapshot"]["uploads"]["index"]["files"]
    assert state.model.pin_floor() == state.model.ticks["watch"]["pin"]
    await engine.sensor_post("watch", tick["tick"], {})
    assert state.model.pin_floor() == float("inf")
    await state.close()


async def test_a_host_on_another_revision_gets_no_ticks(tmp_path):
    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    answer = await engine.sensor_next("local", "old-revision", "test", 8, 0)
    assert answer == {"deploy": project.manifest["deploy"], "ticks": []}
    assert (await engine.sensor_next("sensors", project.manifest["deploy"], "test", 8, 0))["ticks"] == []
    await state.close()


async def test_observable_sources_commit_on_their_schedule(tmp_path):
    """`Source(observe=Every(…))`: a version, a full map, or a patch with a
    cursor, each its source's commit — run by the engine's local host."""

    feed_pages = [
        Observed(upsert={"x": "1"}, cursor="p1"),
        Observed(cursor="p2"),
        Observed(remove=["x"], cursor="p3"),
    ]

    class Versioned(Source):
        def observe(self, ctx):
            return "2026-10-02"

    class Landing(Source):
        def observe(self, ctx, bucket):
            return dict(bucket)

    class Feed(Source):
        def observe(self, ctx):
            return feed_pages.pop(0) if feed_pages else None

    project = Project(
        sources=[
            Versioned("table", observe=Every(1)),
            Landing("landing", key="id", observe=Every(1)),
            Feed("events", key="id", observe=Every(1)),
        ],
        resources={"bucket": {"a": "e1", "b": "e2"}},
    )
    assert set(project.manifest["sensors"]) == {"table.observe", "landing.observe", "events.observe"}
    state, engine = await open_engine(tmp_path, project, host=True)
    await engine.start()
    m = state.model
    await until(lambda: (m.sensors.get("events.observe") or {}).get("cursor") == "p3")
    await until(lambda: m.heads[("table", "")].get("version") == "2026-10-02")
    await until(lambda: m.heads[("landing", "")].get("commit_number") is not None)
    assert list((await engine.list_keys("landing"))["keys"]) == ["a", "b"]
    assert (await engine.list_keys("events"))["keys"] == {}  # x came, then went
    assert {r["outcome"] for r in rows(engine, "events.observe")} >= {"committed", "advanced"}
    await engine.stop()
    await state.close()


async def test_a_raising_body_fails_its_tick_and_keeps_the_cursor(tmp_path):
    calls = []

    @sensor(every=1)
    def flaky(ctx):
        calls.append(ctx.cursor)
        if len(calls) == 1:
            return Tick(cursor="one")
        raise RuntimeError("the API is down")

    project = Project(sensors=[flaky])
    state, engine = await open_engine(tmp_path, project, host=True)
    await engine.start()
    await until(lambda: any(r["outcome"] == "failed" for r in rows(engine)))
    failed = next(r for r in rows(engine) if r["outcome"] == "failed")
    assert "RuntimeError: the API is down" in failed["error"]
    assert state.model.sensors["flaky"]["cursor"] == "one" and calls[1] == "one"
    await engine.stop()
    await state.close()


async def test_a_host_whose_tick_overran_exits_and_the_tick_is_dropped(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_sensors, "POST_GRACE", 0.0)
    release = asyncio.Event()

    @sensor(every=1, timeout=0.1)
    def stuck(ctx):
        asyncio.run_coroutine_threadsafe(release.wait(), loop).result()
        return Tick(cursor="never")

    loop = asyncio.get_running_loop()
    project = Project(sensors=[stuck])
    state, engine = await open_engine(tmp_path, project)
    code = await run_sensor_host(LocalSensorChannel(engine), project, "local")
    assert code == OVERRAN
    await asyncio.sleep(0.05)
    engine._sensor_sweep()
    assert rows(engine)[-1]["error"] == "timed out" and "stuck" not in state.model.sensors
    release.set()
    await state.close()


async def test_a_sensors_own_timeout_is_its_outcome_not_an_overrun(tmp_path):
    """Review round 5 #4: a body that raises TimeoutError — a remote call
    that timed out — failed its tick, well within its own timeout: the
    error is posted, and the host carries on."""

    @sensor(every=1, timeout=60)
    def remote(ctx):
        raise TimeoutError("remote request timed out")

    project = Project(sensors=[remote])
    state, engine = await open_engine(tmp_path, project)
    code = await asyncio.wait_for(
        run_sensor_host(LocalSensorChannel(engine), project, "local", max_ticks=1), 5
    )
    assert code == 0
    assert "remote request timed out" in rows(engine)[-1]["error"]
    await state.close()


async def test_tick_rows_are_written_with_the_history_and_expire_after_a_day(tmp_path):
    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    for cursor in ("a", "b"):
        tick = await dispatch(engine, "watch")
        await engine.sensor_post("watch", tick["tick"], Tick(cursor=cursor).to_json())
    assert [r["outcome"] for r in await engine.history.ticks("watch")] == ["advanced", "advanced"]
    await engine.history.lake.flush(force=True)
    assert rows(engine) == [] and len(state.model.history.files["ticks"]) == 1
    assert len(await engine.history.ticks("watch")) == 2  # now from the file
    engine.upkeep.expire_ticks()
    assert len(state.model.history.files["ticks"]) == 1
    clock = state.clock
    engine.upkeep.clock = lambda: clock() + 86400 + 60
    engine.upkeep.expire_ticks()
    assert state.model.history.files["ticks"] == [] and await engine.history.ticks("watch") == []
    await state.close()


async def test_hosts_reach_the_engine_over_https(tmp_path):
    """A pool host takes ticks with the pool token; the engine's own host
    with a token signed by the engine secret; nothing else gets in."""

    project = feed_project(executor=Pool("sensors"))
    state, engine = await open_engine(tmp_path, project)
    p, deploy = project.manifest["name"], project.manifest["deploy"]
    params = {"executor": "sensors", "deploy": deploy, "wait": 0, "host": "h1"}
    host_token = lifecycle.token(engine.secret, engine_sensors.HOST_TOKEN)
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("SOLERA_POOL_TOKEN", "pool")
        app = create_app(engine=engine, token="admin")
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        url = f"/api/projects/{p}/sensors/next"
        assert (
            await client.get(url, params=params, headers={"Authorization": "Bearer nope"})
        ).status_code == 401
        ticks = (await client.get(url, params=params, headers={"Authorization": "Bearer pool"})).json()[
            "ticks"
        ]
        assert [t["sensor"] for t in ticks] == ["watch"]
        post = f"/api/projects/{p}/sensors/watch/ticks/{ticks[0]['tick']}"
        auth = {"Authorization": f"Bearer {host_token}"}
        answer = await client.post(post, json=Tick(cursor="c").to_json(), headers=auth)
        assert answer.status_code == 200 and answer.json()["accepted"]
        assert (
            await client.post(post.replace(ticks[0]["tick"], "other"), json={}, headers=auth)
        ).status_code == 409
        listed = (
            await client.get(f"/api/projects/{p}/sensors", headers={"Authorization": "Bearer admin"})
        ).json()
        assert listed["sensors"][0]["cursor"] == "c" and listed["workers"][0]["id"] == "h1"
        history = await client.get(
            f"/api/projects/{p}/sensors/watch/ticks", headers={"Authorization": "Bearer admin"}
        )
        assert history.json()["ticks"][0]["outcome"] == "advanced"
    await state.close()


def test_sensor_registration():
    def body(ctx):
        return None

    with pytest.raises(RegistrationError, match="not a source"):
        Project(sensors=[sensor(every=60, commits=["nope"])(body)])
    with pytest.raises(RegistrationError, match="Local or a Pool"):
        Project(sensors=[sensor(every=60, executor=AWSECS("etl", cluster="c", region="r"))(body)])
    with pytest.raises(RegistrationError, match="not a resource"):

        @sensor(every=60)
        def needs(ctx, s3):
            return None

        Project(sensors=[needs])
    with pytest.raises(RegistrationError, match="subclass"):
        Source("plain", observe=Every(60))
    with pytest.raises(RegistrationError, match="Duplicate sensor"):
        Project(sensors=[sensor(every=60, name="s")(body), sensor(every=60, name="s")(body)])


SENSING = """
from solera.sdk import Every, Project, Source


class Clock(Source):
    def observe(self, ctx):
        return "seen"


project = Project(sources=[Clock("clock", observe=Every(1))], name="sensing")
"""


def test_a_served_engine_keeps_its_own_sensor_host(tmp_path, monkeypatch):
    """`solera serve`: the engine starts a `solera_worker sensors` process
    beside it, which reaches it over HTTP with a token the engine signed."""

    import uvicorn

    (tmp_path / "sensing.py").write_text(SENSING)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    base = f"http://127.0.0.1:{port}"
    monkeypatch.setenv("SOLERA_DATA", str(tmp_path / "data"))
    app = create_app(
        state_url=(tmp_path / "state").as_uri(),
        namespace="sensing",
        project=str(tmp_path / "sensing.py"),
        token="admin",
        engine_url=base,
    )
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    admin = {"Authorization": "Bearer admin"}
    try:
        deadline = time.monotonic() + 60
        while True:
            assert time.monotonic() < deadline, "the local host never ticked"
            try:
                listed = httpx.get(f"{base}/api/projects/sensing/sensors", headers=admin, timeout=5).json()
                if (listed["sensors"][0]["accepted"] or {}).get("commits"):
                    break
            except (httpx.HTTPError, KeyError):
                pass
            time.sleep(0.2)
        assert listed["workers"][0]["executor"] == "local"
    finally:
        server.should_exit = True
        thread.join(timeout=20)


async def test_a_host_on_old_code_waits_then_starts_afresh(tmp_path):
    """The engine serves another revision: the host takes no ticks, waits,
    and returns, for its process to start again with the code on disk."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    engine.manifest = {**engine.manifest, "deploy": "newer"}
    code = await asyncio.wait_for(
        run_sensor_host(LocalSensorChannel(engine), project, "local", stale_wait=0.05), 5
    )
    assert code == 0 and "watch" not in state.model.ticks
    with pytest.raises(ValueError, match="JSON object"):
        await engine.sensor_post("watch", "t", ["not", "an", "object"])
    await state.close()


async def test_a_decision_under_way_keeps_its_claim_and_pin(tmp_path, monkeypatch):
    """Astra review 2, P1-2: the first post pauses while preparing its
    commit (planning itself is synchronous: it never yields). Meanwhile its
    claim and reader pin hold: no other tick is dispatched (which could roll
    the cursor back), expiry leaves it, and a duplicate post waits for the
    decision and gets its answer."""

    monkeypatch.setattr(engine_sensors, "POST_GRACE", 0.0)
    project = feed_project(timeout=0.05)
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    paused, go = asyncio.Event(), asyncio.Event()
    prepare = engine._prepare_commit

    async def slow(*args, **kw):
        paused.set()
        await go.wait()
        return await prepare(*args, **kw)

    monkeypatch.setattr(engine, "_prepare_commit", slow)
    outcome = Tick(
        cursor="older", commits=[Commit("feed", version="v9")], runs=[RunRequest("ingest")]
    ).to_json()
    first = asyncio.create_task(engine.sensor_post("watch", tick["tick"], outcome))
    await paused.wait()
    pin = state.model.ticks["watch"]["pin"]
    await asyncio.sleep(0.1)
    engine._sensor_sweep()  # past its timeout, but deciding
    engine.sensor_due.pop("watch", None)
    assert (await engine.sensor_next("local", engine.manifest["deploy"], "t", 8, 0))["ticks"] == []
    assert state.model.pin_floor() == pin
    duplicate = asyncio.create_task(engine.sensor_post("watch", tick["tick"], outcome))
    await asyncio.sleep(0.05)
    assert not duplicate.done()
    go.set()
    answer = await first
    assert await duplicate == answer and len(answer["runs"]) == 1
    assert state.model.sensors["watch"]["cursor"] == "older" and "watch" not in state.model.ticks
    assert state.model.pin_floor() == float("inf")
    await state.close()


async def test_an_answer_waits_for_its_decision_to_be_durable(tmp_path, monkeypatch):
    """Astra review 2, P1-3: the journal's upload is blocked after the
    decision was recorded. Neither the post nor its retry is answered
    until the decision is durable."""

    project = feed_project()
    state, engine = await open_engine(tmp_path, project)
    tick = await dispatch(engine, "watch")
    blocked = asyncio.Event()
    durable = state.durable

    async def held():
        await blocked.wait()
        await durable()

    monkeypatch.setattr(state, "durable", held)
    outcome = Tick(cursor="c").to_json()
    first = asyncio.create_task(engine.sensor_post("watch", tick["tick"], outcome))
    await until(lambda: "watch" in state.model.sensors)  # recorded, not yet durable
    retry = asyncio.create_task(engine.sensor_post("watch", tick["tick"], outcome))
    await asyncio.sleep(0.1)
    assert not first.done() and not retry.done()
    blocked.set()
    assert (await retry)["accepted"] and (await first)["accepted"]
    await state.close()


async def test_requested_runs_see_the_ticks_own_commits(tmp_path):
    """Astra review 2, P1-4: a tick replaces a partition set `[old]` with
    `[new]` and requests a run over all partitions: the run's tasks are for
    `new`, planned against the set the tick installs."""

    sites = DynamicPartitions("sites")

    @asset(partitions=sites)
    def per_site(ctx) -> int:
        return 1

    @sensor(every=60, commits=["sites"])
    def discover(ctx):
        return None

    project = Project(assets=[per_site], sources=[sites], sensors=[discover])
    state, engine = await open_engine(tmp_path, project)
    await engine.commit_source("sites", keys=["old"])
    tick = await dispatch(engine, "discover")
    outcome = Tick(commits=[Commit("sites", keys=["new"])], runs=[RunRequest("per_site", partitions="all")])
    answer = await engine.sensor_post("discover", tick["tick"], outcome.to_json())
    [run] = answer["runs"]
    assert [t["partition"] for t in state.model.runs[run]["tasks"].values()] == ["new"]
    assert state.model.heads[("sites", "")]["partitions"] == ["new"]
    await state.close()


async def test_an_async_observe_is_awaited(tmp_path):
    """Astra review 2, P2-6."""

    class Feed(Source):
        async def observe(self, ctx):
            await asyncio.sleep(0)
            return "v1"

    project = Project(sources=[Feed("feed", observe=Every(1))])
    state, engine = await open_engine(tmp_path, project, host=True)
    await engine.start()
    await until(lambda: state.model.heads[("feed", "")].get("version") == "v1")
    await engine.stop()
    await state.close()


async def test_an_overrun_does_not_wait_for_other_sensors(tmp_path, monkeypatch):
    """Astra review 2, P2-7: one tick overruns while another sensor's,
    with an hour's timeout, is blocked. The host drains briefly and
    returns; the blocked tick's claim expires on its own."""

    release = threading.Event()

    @sensor(every=1, timeout=0.1)
    def stuck(ctx):
        release.wait(10)

    @sensor(every=1, timeout=3600)
    def slow(ctx):
        release.wait(10)

    project = Project(sensors=[stuck, slow])
    state, engine = await open_engine(tmp_path, project)
    started = asyncio.get_running_loop().time()
    code = await run_sensor_host(LocalSensorChannel(engine), project, "local", drain=0.2)
    assert code == OVERRAN and asyncio.get_running_loop().time() - started < 3
    assert "slow" in state.model.ticks  # left to expire
    release.set()
    await state.close()


async def test_an_engines_own_host_stops_once_its_engine_is_gone(tmp_path):
    """An engine killed outright never stops the host it started in its own
    session: the host notices its parent is gone and exits, rather than
    polling a dead engine forever."""

    @sensor(every=1)
    def quiet(ctx):
        return None

    project = Project(sensors=[quiet])
    state, engine = await open_engine(tmp_path, project)
    gone = os.getppid() + 1_000_000  # no process's parent: as if reparented
    code = await asyncio.wait_for(
        run_sensor_host(LocalSensorChannel(engine), project, "local", parent=gone), 5
    )
    assert code == ORPHANED
    await state.close()


async def test_a_host_with_every_slot_taken_still_stops_once_its_engine_is_gone(tmp_path, monkeypatch):
    """Both slots run a sensor with an hour's timeout when the engine dies:
    the host notices within its `watch`, drains briefly and returns, not
    once a sensor ends."""

    release, entered = threading.Event(), []

    @sensor(every=1, timeout=3600)
    def first(ctx):
        entered.append("first")
        release.wait(10)

    @sensor(every=1, timeout=3600)
    def second(ctx):
        entered.append("second")
        release.wait(10)

    project = Project(sensors=[first, second])
    state, engine = await open_engine(tmp_path, project)
    host = asyncio.create_task(
        run_sensor_host(
            LocalSensorChannel(engine),
            project,
            "local",
            concurrency=2,
            parent=os.getppid(),
            watch=0.01,
            drain=0.1,
        )
    )
    await until(lambda: len(entered) == 2)
    await asyncio.sleep(0.05)  # the host is waiting on its sensors
    assert not host.done()
    monkeypatch.setattr(os, "getppid", lambda: 1)  # reparented: the engine is gone
    assert await asyncio.wait_for(host, 2) == ORPHANED
    release.set()
    await state.close()
