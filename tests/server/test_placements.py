"""§10 placements: the pool pull path (discovery, claims, a dead claim,
cancel) and the remote kinds against stubbed SDK clients."""

import asyncio
import json
import os
import re
import sys
import types
import uuid

import pytest
from solera.executors import Pool
from solera.sdk import Project, Retry, asset
from solera_server.engine import Engine
from solera_server.executors import PlacementContext
from solera_worker.worker import run_attempt

from tests.server.test_fence import Gated


def make_engine(state, project, placements=None, **kw):
    kw.setdefault("eval_interval", 0.05)
    return Engine(state, project.manifest, placements=placements or {}, clock=state.clock, **kw)


async def pool_attempt(engine, state, count=1):
    """Wait for `count` pool attempts to be launched and discoverable."""

    for _ in range(200):
        await engine.tick()
        if len(state.model.pool) >= count:
            return sorted(state.model.pool, key=lambda a: state.model.pool[a]["created_at"])
        await asyncio.sleep(0.02)
    raise AssertionError("no pool attempt")


async def test_pool_workers_race_for_a_claim(state):
    """docs/lifecycle.md §10: discovery offers a launched pool attempt to the
    workers it fits; they race for its claim. The winner runs it and the
    engine commits it; the loser writes nothing and goes back to polling."""

    from solera_worker.channel import LocalChannel

    @asset(executor=Pool("ingest")(cpu=2))
    def job():
        return [{"ok": True}]

    project = Project(assets=[job])
    engine = make_engine(state, project, heartbeat_seconds=0.3)
    await engine.initialize()
    run = await engine.submit(["job"])
    [attempt] = await pool_attempt(engine, state)
    assert state.model.pool[attempt]["needs"] == {"cpu": 2}
    assert await engine.pool_work("ingest", {"cpu": 1}, "w-small", 0) == []  # does not fit
    [stage] = await engine.pool_work("ingest", {"cpu": 4}, "w1", 0)
    assert [s["attempt"] for s in await engine.pool_work("ingest", {"cpu": 4}, "w2", 0)] == [attempt]

    async def worker():
        channel = LocalChannel(engine, attempt)
        return await run_attempt(
            stage["objects"], attempt, project, run=stage["run"], channel=channel, pool=True
        )

    codes = await asyncio.gather(worker(), worker())
    assert sorted(codes) == [0, 0]
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert await engine.pool_work("ingest", {"cpu": 4}, "w1", 0) == []  # ended: no longer offered


async def test_a_dead_pool_claim_expires_into_a_new_attempt(state):
    """A worker claims, then dies before it says anything. The engine finds
    the claim, waits for a report, and ends the attempt lost; its writes are
    classified from its gate, never from progress (§2.3): the engine's
    `aborted` create wins, so `none`. The retry is a new attempt id, which
    another worker can claim — never the old claim with a new owner."""

    @asset(executor=Pool("ingest")(), retries=Retry(1, delay=0))
    def job():
        return [{"ok": True}]

    project = Project(assets=[job], default_store=Gated())
    engine = make_engine(state, project, heartbeat_seconds=0.1, pool_offered_grace=0.1)
    await engine.initialize()
    run = await engine.submit(["job"])
    [first] = await pool_attempt(engine, state)
    await engine.pool_work("ingest", {}, "w1", 0)
    base = state.attempt_path(run["id"], first)
    await state.create_object(f"{base}.worker", json.dumps({"worker_id": "dead"}).encode())
    for _ in range(200):
        await engine.tick()
        await asyncio.sleep(0.02)
        if first not in state.model.pool and state.model.pool:
            break
    [second] = state.model.pool
    assert second != first
    task = state.model.task(state.model.pool[second]["task"])
    ended = (await engine.history.attempts(task["run"]))[task["id"]][0]
    assert ended["id"] == first and ended["outcome"] == "failed"
    assert json.loads(await state.get_object(f"{base}.writing")) == {"state": "aborted"}
    assert [s["attempt"] for s in await engine.pool_work("ingest", {}, "w2", 0)] == [second]


async def test_a_dead_pool_claim_that_took_its_gate_is_still_writing(state):
    """The claimant took its gate and entered a store call before it could
    report again: the engine finds `writing`, so its write is `writing`
    and the intents stay owing a repair — whatever `.worker` showed."""

    from solera import lifecycle

    @asset(executor=Pool("ingest")(), retries=Retry(0))
    def job():
        return [{"ok": True}]

    project = Project(assets=[job], default_store=Gated())
    engine = make_engine(state, project, heartbeat_seconds=0.1, pool_offered_grace=0.1)
    await engine.initialize()
    run = await engine.submit(["job"])
    [first] = await pool_attempt(engine, state)
    await engine.pool_work("ingest", {}, "w1", 0)
    base = state.attempt_path(run["id"], first)
    await state.create_object(f"{base}.worker", json.dumps({"worker_id": "dead"}).encode())
    intents = {"job": {"files": [], "added": 0, "removed": 0, "exact": True}}
    await state.create_object(f"{base}.writing", lifecycle.gate("writing", "dead", intents))
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "failed"
    assert state.model.repairs[("job", "")][0]["attempt"] == first


async def test_a_pool_attempt_canceled_before_its_claim_is_withdrawn(state):
    """A cancel before any worker claimed the attempt ends it at once: no
    worker can hear a request, so it is forced, and discovery stops
    offering it."""

    @asset(executor=Pool("ingest")())
    def job():
        return [{"ok": True}]

    project = Project(assets=[job], default_store=Gated())
    engine = make_engine(state, project, heartbeat_seconds=0.3)
    await engine.initialize()
    run = await engine.submit(["job"])
    [attempt] = await pool_attempt(engine, state)
    await engine.cancel(run["id"])
    for _ in range(100):
        await engine.tick()
        await asyncio.sleep(0.02)
        if not state.model.pool:
            break
    assert await engine.pool_work("ingest", {}, "w1", 0) == []
    gate = json.loads(await state.get_object(f"{state.attempt_path(run['id'], attempt)}.writing"))
    assert gate == {"state": "aborted"}


# -- remote placements against stubbed SDKs ---------------------------------------


class FakeEcs:
    def __init__(self):
        self.tasks = {}
        self.stopped = []

    def run_task(self, **kw):
        arn = f"arn:ecs:task/{uuid.uuid4().hex}"
        self.tasks[arn] = {"kw": kw, "lastStatus": "RUNNING", "containers": [{}]}
        return {"tasks": [{"taskArn": arn}], "failures": []}

    def describe_tasks(self, cluster, tasks):
        return {"tasks": [self.tasks[t] for t in tasks if t in self.tasks]}

    def stop_task(self, cluster, task, reason):
        self.stopped.append(task)


def stub_boto3(monkeypatch, ecs):
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda service, region_name=None: ecs
    monkeypatch.setitem(sys.modules, "boto3", boto3)


async def test_awsecs_launch_wait_cancel(state, monkeypatch):
    """§10: AWSECS launch passes attempt+objects as container overrides, and
    the attempt as the client token, so a second launch starts nothing new;
    wait maps STOPPED to an exit; a task ECS does not show is unknown, not
    lost — it may not show yet."""
    from solera_server.executors.remote import AWSECS

    ecs = FakeEcs()
    stub_boto3(monkeypatch, ecs)
    placement = AWSECS({"cluster": "lab", "region": "us-east-1"}, {"cpu": 4, "memory": 30 * 10**9}, None)
    handle = await placement.launch({"attempt": "a1", "run": "r1", "objects": "s3://bkt/ns/objects"})
    task = ecs.tasks[handle["task_arn"]]
    override = task["kw"]["overrides"]["containerOverrides"][0]
    assert override["command"][-3:] == ["--attempt", "a1"] or "a1" in override["command"]
    assert "s3://bkt/ns/objects" in override["command"]
    assert override["cpu"] == "4096" and override["memory"] == "30000"
    assert task["kw"]["clientToken"] == "a1"

    ecs.tasks[handle["task_arn"]]["lastStatus"] = "STOPPED"
    ecs.tasks[handle["task_arn"]]["containers"] = [{"exitCode": 0}]
    exit_ = await placement.wait(handle, 1)
    assert exit_["code"] == 0

    del ecs.tasks[handle["task_arn"]]
    with pytest.raises(LookupError):
        await placement.wait(handle, 1)

    await placement.cancel({"task_arn": "arn:ecs:task/dead"})
    assert ecs.stopped == ["arn:ecs:task/dead"]


class ApiError(Exception):
    def __init__(self, status):
        super().__init__(status)
        self.status = status


class FakeBatchApi:
    def __init__(self):
        self.jobs = {}
        self.down = False

    def create_namespaced_job(self, namespace, body):
        name = body.metadata.name
        if name in self.jobs:
            raise ApiError(409)
        self.jobs[name] = types.SimpleNamespace(
            metadata=body.metadata, spec=body.spec, status=types.SimpleNamespace(conditions=[])
        )
        return self.jobs[name]

    def read_namespaced_job(self, name, namespace):
        if self.down:
            raise ApiError(503)
        if name not in self.jobs:
            raise ApiError(404)
        return self.jobs[name]

    def delete_namespaced_job(self, name, namespace, propagation_policy=None):
        self.jobs.pop(name, None)


def stub_kubernetes(monkeypatch, batch):
    k8s = types.ModuleType("kubernetes")
    client = types.ModuleType("kubernetes.client")

    class V:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    for name in (
        "V1Container",
        "V1Job",
        "V1JobSpec",
        "V1ObjectMeta",
        "V1PodSpec",
        "V1PodTemplateSpec",
        "V1ResourceRequirements",
    ):
        setattr(client, name, V)
    client.BatchV1Api = lambda: batch
    k8s.client = client
    monkeypatch.setitem(sys.modules, "kubernetes", k8s)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client)


async def test_k8sjob_launch_wait_lost(state, monkeypatch):
    """§10: K8sJob creates a job named after the attempt — a valid,
    lowercase DNS label — carrying the stage argv; launching it again finds
    that job. Conditions map to Exit; an API error is unknown; a deleted job
    is lost."""
    from solera_server.executors.remote import K8sJob

    batch = FakeBatchApi()
    stub_kubernetes(monkeypatch, batch)
    placement = K8sJob({"cluster": "c", "namespace": "ns"}, {"cpu": 2}, None)
    stage = {"attempt": "01J8ZB3MXQ5R2K7T9V4W6Y8Z0A", "run": "r", "objects": "s3://x"}
    handle = await placement.launch(stage)
    assert handle == {"job": "solera-01j8zb3mxq5r2k7t9v4w6y8z0a"}
    assert re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", handle["job"])
    job = batch.jobs[handle["job"]]
    args = job.spec.template.spec.containers[0].args
    assert stage["attempt"] in args and "s3://x" in args
    assert await placement.resume(stage) == handle and batch.jobs[handle["job"]] is job

    batch.down = True
    with pytest.raises(ApiError):
        await placement.wait(handle, 1)
    batch.down = False

    job.status.conditions = [
        types.SimpleNamespace(type="Failed", status="True", reason="BackoffLimitExceeded")
    ]
    exit_ = await placement.wait(handle, 1)
    assert exit_["code"] == 1 and exit_["reason"] == "BackoffLimitExceeded"

    await placement.cancel(handle)
    exit_ = await placement.wait(handle, 1)
    assert exit_["reason"] == "lost"


def modal_exceptions():
    """Modal 1.6's exception hierarchy, as far as `wait` tells its members apart."""

    class Error(Exception): ...

    class ModalTimeout(Error): ...  # modal.exception.TimeoutError: not the builtin

    names = {"Error": Error, "TimeoutError": ModalTimeout}
    for name, base in [
        ("FunctionTimeoutError", ModalTimeout),
        ("OutputExpiredError", ModalTimeout),
        ("ConnectionError", Error),
        ("ServiceError", Error),
        ("AuthError", Error),
        ("NotFoundError", Error),
        ("InternalFailure", Error),
        ("RemoteError", Error),
        ("ExecutionError", Error),
    ]:
        names[name] = type(name, (base,), {})
    return types.SimpleNamespace(**names)


async def test_modal_reports_only_what_modal_says_of_the_call(state, monkeypatch):
    """§10: Modal spawns the worker function with the stage. A call that
    returned exits with its code; one that raised or timed out fails; one
    Modal does not know is lost. Modal's own client and service errors say
    nothing of the call: wait raises, so the engine keeps the handle."""
    from solera_server.executors.remote import Modal

    errors, calls = modal_exceptions(), {}

    class FakeCall:
        def __init__(self, object_id):
            self.object_id = object_id
            self.outcome = TimeoutError()  # builtin: not done yet

        def get(self, timeout=0):
            if isinstance(self.outcome, BaseException):
                raise self.outcome
            return self.outcome

        def cancel(self):
            pass

    class Function:
        @staticmethod
        def from_name(app, name):
            def spawn(attempt, run, objects):
                call = FakeCall(uuid.uuid4().hex)
                calls[call.object_id] = (call, {"attempt": attempt, "run": run, "objects": objects})
                return call

            return types.SimpleNamespace(spawn=spawn)

    class Unknown:
        def get(self, timeout=0):
            raise errors.NotFoundError("no such call")

    def from_id(cid):
        return calls[cid][0] if cid in calls else Unknown()

    modal = types.ModuleType("modal")
    modal.Function = Function
    modal.functions = types.SimpleNamespace(FunctionCall=types.SimpleNamespace(from_id=from_id))
    modal.exception = errors
    monkeypatch.setitem(sys.modules, "modal", modal)

    placement = Modal({"app": "solera"}, {"gpu": "A10G"}, None)
    handle = await placement.launch({"attempt": "a9", "run": "r9", "objects": "s3://o"})
    call, stage = calls[handle["call_id"]]
    assert stage == {"attempt": "a9", "run": "r9", "objects": "s3://o"}
    assert await placement.wait(handle, 0.2) is None  # still running

    for transport in (errors.ConnectionError("down"), errors.ServiceError("UNAVAILABLE"), OSError("reset")):
        call.outcome = transport
        with pytest.raises(type(transport)):
            await placement.wait(handle, 1)
    for ended, code in ((3, 3), (errors.FunctionTimeoutError("2h"), 1), (ValueError("bad row"), 1)):
        call.outcome = ended
        assert (await placement.wait(handle, 1))["code"] == code
    call.outcome = errors.OutputExpiredError()
    assert (await placement.wait(handle, 1))["reason"] == "lost"
    del calls[handle["call_id"]]
    assert (await placement.wait(handle, 1))["reason"] == "lost"


@pytest.mark.skipif(
    not os.path.exists("/proc/self/stat"),
    reason="no /proc: a Local handle cannot be told from a reused pid, and is followed by heartbeats only",
)
async def test_a_local_handle_is_only_ever_its_own_process(state, monkeypatch):
    """A handle names one launch. A handle adopted from another host, or
    naming a pid since reused here, never reaches the process now holding
    that pid — not even this engine's own child: wait cannot tell, cancel
    signals nothing. A handle that provably names a live process here is
    followed and canceled."""

    import asyncio
    import os
    import socket

    from solera_server.executors import local
    from solera_server.executors.local import LocalPlacement, _start_ticks

    spawn = asyncio.create_subprocess_exec

    async def sleeper(*args, **kw):
        return await spawn("sleep", "30", **kw)

    monkeypatch.setattr(local.asyncio, "create_subprocess_exec", sleeper)
    placement = LocalPlacement(PlacementContext(state, state.objects_url, "", state.clock))
    ours = await placement.launch({"attempt": "a", "run": "r", "objects": "file:///x"})
    pid = ours["pid"]

    def alive(pid):
        try:
            os.kill(pid, 0)
            return open(f"/proc/{pid}/stat").read().split(") ")[1][0] != "Z"
        except OSError:
            return False

    elsewhere = {**ours, "launch": "from-host-a", "host": "host-a"}
    reused = {**ours, "launch": "an-old-launch", "ticks": "1"}
    with pytest.raises(LookupError):
        await placement.wait(elsewhere, 0.1)
    assert (await placement.wait(reused, 0.1))["reason"] == "lost"
    await placement.cancel(elsewhere)
    await placement.cancel(reused)
    assert alive(pid)  # our own child, untouched

    adopted = await spawn("sleep", "30", start_new_session=True)
    handle = {
        "launch": "before-the-restart",
        "pid": adopted.pid,
        "ticks": await _start_ticks(adopted.pid),
        "host": socket.gethostname(),
    }
    assert await placement.wait(handle, 0.1) is None
    await placement.cancel(handle)
    assert await asyncio.wait_for(adopted.wait(), 5) == -15
    await placement.cancel(ours)
    assert not alive(pid)


async def test_a_pool_attempt_that_ended_while_the_engine_was_down_settles(state):
    """Review P1-4: a worker claims a pool attempt, the engine stops, the
    worker finishes and exits. No worker asks for work again; the restarted
    engine still settles the attempt from its objects."""

    @asset(executor=Pool("ingest")())
    def job():
        return [{"ok": True}]

    project = Project(assets=[job])
    engine = make_engine(state, project, heartbeat_seconds=0.1)
    await engine.initialize()
    run = await engine.submit(["job"])
    [attempt] = await pool_attempt(engine, state)
    await engine.stop()
    code = await run_attempt(state.objects_url, attempt, project, run=run["id"], pool=True)
    assert code == 0
    engine = make_engine(state, project, heartbeat_seconds=0.1)
    await engine.initialize()
    detail = await engine.run_until(run["id"], 5)
    assert detail["request"]["status"] == "succeeded"
    await engine.stop()


async def test_after_a_restart_a_start_is_checked_against_the_claim(state):
    """Review P3: the first `start` binds unread only for an attempt this
    engine launched. After a restart, it must match the claim's owner."""

    from solera.lifecycle import Ended

    @asset(executor=Pool("ingest")())
    def job():
        return [{"ok": True}]

    project = Project(assets=[job])
    engine = make_engine(state, project)
    await engine.initialize()
    run = await engine.submit(["job"])
    [attempt] = await pool_attempt(engine, state)
    await state.create_object(f"{state.attempt_path(run['id'], attempt)}.worker", b'{"worker_id": "owner"}')
    await engine.stop()
    engine = make_engine(state, project)
    await engine.initialize()
    with pytest.raises(Ended, match="not_owner"):
        await engine.attempt_start(attempt, {"worker_id": "intruder"})
    await engine.attempt_start(attempt, {"worker_id": "owner"})
    assert engine.live[attempt].worker_id == "owner"
