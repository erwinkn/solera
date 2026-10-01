"""§10 placements: the pool pull path (claim fit, renew, complete, a lost
worker, cancel) and the remote kinds against stubbed SDK clients."""

import asyncio
import json
import re
import sys
import types
import uuid

import pytest
from solera.executors import Pool
from solera.sdk import Project, Retry, asset
from solera_server.engine import Engine
from solera_server.placements import PlacementContext
from solera_server.placements.pool import PoolPlacement
from solera_server.state import LostOwnership, State
from solera_worker.worker import run_attempt


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()


def make_engine(state, project, placements=None, **kw):
    kw.setdefault("eval_interval", 0.05)
    return Engine(state, project.manifest, placements=placements or {}, clock=state.clock, **kw)


async def test_pool_task_lifecycle(state):
    """§10: a Pool-placed task is claimable; a fitting worker claims, renews,
    runs the harness, completes; the engine commits the result."""

    @asset(executor=Pool("ingest")(cpu=2))
    def job():
        return [{"ok": True}]

    project = Project(assets=[job])
    engine = make_engine(state, project, heartbeat_seconds=30)
    await engine.initialize()
    run = await engine.submit(["job"])
    await engine.tick()  # dispatch: stage the pool task, wait for a claim
    await asyncio.sleep(0.1)

    [attempt] = state.model.pool
    record = state.model.pool[attempt]
    assert record["status"] == "queued" and record["needs"] == {"cpu": 2}
    assert record["task"] == f"{run['id']}/job:"

    # a worker that doesn't fit never sees the task
    assert engine.claim_pool_task("w-small", ["ingest"], {"cpu": 1}, lease_seconds=30) is None
    claimed = engine.claim_pool_task("w1", ["ingest"], {"cpu": 4}, lease_seconds=30)
    assert claimed["attempt"] == attempt and claimed["status"] == "claimed"
    # the claim is durable: a restarted engine never offers the task again
    assert state.model.task(record["task"])["launched"]["worker"] == "w1"

    # renew extends the claim; a second worker can't steal it
    engine.heartbeat_pool_task("w1", attempt, lease_seconds=30)
    assert engine.claim_pool_task("w2", ["ingest"], {"cpu": 4}, lease_seconds=30) is None

    # the worker runs the stage and completes; the engine's wait sees the result
    code = await run_attempt(state.objects_url, attempt, project, run=record["run"])
    assert code == 0
    engine.release_pool_task("w1", attempt)
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert detail["tasks"][0]["status"] == "succeeded"


async def test_a_lost_pool_worker_fails_its_attempt_and_the_retry_goes_to_another(state):
    """§10: a worker whose lease expires is lost. Its attempt is aborted and
    fails retryably; the retry is a new attempt, which another worker claims,
    and the lost worker's renew and complete are rejected."""

    @asset(executor=Pool("ingest")(), retries=Retry(1, delay=0))
    def job():
        return [{"ok": True}]

    project = Project(assets=[job])
    engine = make_engine(state, project, heartbeat_seconds=30)
    await engine.initialize()
    run = await engine.submit(["job"])
    await engine.tick()
    await asyncio.sleep(0.1)
    [first] = state.model.pool
    assert (engine.claim_pool_task("w1", ["ingest"], {}, lease_seconds=0.2))["claimed_by"] == "w1"
    for _ in range(40):
        await engine.tick()
        await asyncio.sleep(0.05)
        if state.model.pool and first not in state.model.pool:
            break
    [second] = state.model.pool
    task = state.model.task(state.model.pool[second]["task"])
    assert task["attempts"][0]["id"] == first and task["attempts"][0]["outcome"] == "failed"
    assert (
        json.loads(await state.get_object(f"{state.attempt_path(run['id'], first)}.writing"))["state"]
        == "aborted"
    )
    assert (engine.claim_pool_task("w2", ["ingest"], {}, lease_seconds=30))["attempt"] == second
    with pytest.raises(LostOwnership):
        engine.heartbeat_pool_task("w1", first, lease_seconds=30)
    engine.release_pool_task("w1", second)  # no-op: w1 doesn't own it
    assert state.model.pool[second]["claimed_by"] == "w2"


async def test_pool_cancel_stops_renewal(state):
    """§10: cancel withdraws the claimable task; a claimed worker's next renew
    fails with LostOwnership and it stops."""
    placement = PoolPlacement(PlacementContext(state, state.objects_url, "", state.clock), "ingest")
    record = {
        "attempt": "t/1",
        "task": "t",
        "run": "r",
        "asset": "a",
        "scope": "",
        "pool": "ingest",
        "needs": {},
        "spec": {},
        "prepared": {},
        "status": "claimed",
        "claimed_by": "w1",
        "lease_until": state.clock() + 60,
        "created_at": 0,
    }
    engine = make_engine(state, Project(assets=[]))
    state.model.pool["t/1"] = record
    await placement.cancel({"task": "t/1"})
    assert "t/1" not in state.model.pool
    with pytest.raises(LostOwnership):
        engine.heartbeat_pool_task("w1", "t/1", lease_seconds=30)


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
    from solera_server.placements.remote import AWSECS

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
    from solera_server.placements.remote import K8sJob

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


async def test_modal_launch_wait_lost(state, monkeypatch):
    """§10: Modal spawns the harness function with the stage; a finished call
    exits 0; a missing call is lost."""
    from solera_server.placements.remote import Modal

    calls = {}

    class FakeCall:
        def __init__(self, object_id):
            self.object_id = object_id
            self.finished = False
            self.canceled = False

        def get(self, timeout=0):
            if not self.finished:
                raise TimeoutError
            return 0

        def cancel(self):
            self.canceled = True

    class Function:
        @staticmethod
        def from_name(app, name):
            def spawn(attempt, run, objects):
                call = FakeCall(uuid.uuid4().hex)
                calls[call.object_id] = (call, {"attempt": attempt, "run": run, "objects": objects})
                return call

            return types.SimpleNamespace(spawn=spawn)

    FunctionNotFoundError = type("FunctionNotFoundError", (Exception,), {})

    def from_id(cid):
        if cid not in calls:
            raise FunctionNotFoundError(cid)
        return calls[cid][0]

    modal = types.ModuleType("modal")
    modal.Function = Function
    modal.functions = types.SimpleNamespace(FunctionCall=types.SimpleNamespace(from_id=from_id))
    modal.exception = types.SimpleNamespace(FunctionNotFoundError=FunctionNotFoundError)
    monkeypatch.setitem(sys.modules, "modal", modal)

    placement = Modal({"app": "solera"}, {"gpu": "A10G"}, None)
    handle = await placement.launch({"attempt": "a9", "run": "r9", "objects": "s3://o"})
    assert calls[handle["call_id"]][1] == {"attempt": "a9", "run": "r9", "objects": "s3://o"}

    assert await placement.wait(handle, 0.2) is None  # still running
    calls[handle["call_id"]][0].finished = True
    assert (await placement.wait(handle, 1))["code"] == 0

    del calls[handle["call_id"]]
    assert (await placement.wait(handle, 1))["reason"] == "lost"


async def test_a_local_handle_is_only_ever_its_own_process(state, monkeypatch):
    """A handle names one launch. A handle adopted from another host, or
    naming a pid since reused here, never reaches the process now holding
    that pid — not even this engine's own child: wait cannot tell, cancel
    signals nothing. A handle that provably names a live process here is
    followed and canceled."""

    import asyncio
    import os
    import socket

    from solera_server.placements import local
    from solera_server.placements.local import LocalPlacement, _start_ticks

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
