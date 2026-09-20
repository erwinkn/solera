"""§10 placements: the pool pull path (claim fit, renew, complete, expiry,
cancel) and the remote kinds against stubbed SDK clients."""

import asyncio
import sys
import types
import uuid

import pytest
from cursus.executors import Pool
from cursus.sdk import Project, asset
from cursus_server.engine import Engine
from cursus_server.placements import PlacementContext
from cursus_server.placements.pool import PoolPlacement
from cursus_server.state import LostOwnership, State
from cursus_server.storage import SlateState
from cursus_worker.worker import run_attempt


@pytest.fixture
async def state(tmp_path):
    opened = State(await SlateState.open(tmp_path.as_uri(), "test"))
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
    engine = make_engine(state, project, lease_seconds=30)
    await engine.initialize()
    run = await engine.submit(["job"])
    await engine.tick()  # dispatch: stage the pool task, wait for a claim
    await asyncio.sleep(0.1)

    attempt = f"{run['id']}/job:/1"
    record = await state.get_pool_task(attempt)
    assert record["status"] == "queued" and record["needs"] == {"cpu": 2}

    # a worker that doesn't fit never sees the task
    assert await state.claim_pool_task("w-small", ["ingest"], {"cpu": 1}, lease_seconds=30) is None
    claimed = await state.claim_pool_task("w1", ["ingest"], {"cpu": 4}, lease_seconds=30)
    assert claimed["attempt"] == attempt and claimed["status"] == "claimed"

    # renew extends the claim; a second worker can't steal it
    await state.heartbeat_pool_task("w1", attempt, lease_seconds=30)
    assert await state.claim_pool_task("w2", ["ingest"], {"cpu": 4}, lease_seconds=30) is None

    # the worker runs the stage and completes; the engine's wait sees the result
    code = await run_attempt(state.objects_url, attempt, project)
    assert code == 0
    await state.release_pool_task("w1", attempt)
    detail = await engine.run_until(run["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert detail["tasks"][0]["status"] == "succeeded"


async def test_pool_expired_claim_requeues_and_late_complete_rejected(state):
    """§10: an expired claim is swept back to queued; a late complete is
    rejected because the claim moved on."""

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
        "status": "queued",
        "claimed_by": None,
        "lease_until": None,
        "created_at": 0,
    }
    async with state.transaction() as tx:
        await tx.put_pool_task("t/1", record)
    claimed = await state.claim_pool_task("w1", ["ingest"], {}, lease_seconds=1)
    assert claimed["status"] == "claimed"
    await asyncio.sleep(1.05)
    await state.sweep_pool_leases()
    reclaimed = await state.claim_pool_task("w2", ["ingest"], {}, lease_seconds=30)
    assert reclaimed["claimed_by"] == "w2"
    # the stale worker's renew and complete are rejected
    with pytest.raises(LostOwnership):
        await state.heartbeat_pool_task("w1", "t/1", lease_seconds=30)
    await state.release_pool_task("w1", "t/1")  # no-op: w1 doesn't own it
    assert (await state.get_pool_task("t/1"))["claimed_by"] == "w2"
    await state.release_pool_task("w2", "t/1")
    assert await state.get_pool_task("t/1") is None


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
    async with state.transaction() as tx:
        await tx.put_pool_task("t/1", record)
    await placement.cancel({"task": "t/1"})
    assert await state.get_pool_task("t/1") is None
    with pytest.raises(LostOwnership):
        await state.heartbeat_pool_task("w1", "t/1", lease_seconds=30)


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
    """§10: AWSECS launch passes attempt+objects as container overrides; wait
    maps STOPPED to an exit; a vanished task is lost."""
    from cursus_server.placements.remote import AWSECS

    ecs = FakeEcs()
    stub_boto3(monkeypatch, ecs)
    placement = AWSECS({"cluster": "lab", "region": "us-east-1"}, {"cpu": 4, "memory": 30 * 10**9}, None)
    handle = await placement.launch({"attempt": "a1", "objects": "s3://bkt/ns/objects"})
    task = ecs.tasks[handle["task_arn"]]
    override = task["kw"]["overrides"]["containerOverrides"][0]
    assert override["command"][-3:] == ["--attempt", "a1"] or "a1" in override["command"]
    assert "s3://bkt/ns/objects" in override["command"]
    assert override["cpu"] == "4096" and override["memory"] == "30000"

    ecs.tasks[handle["task_arn"]]["lastStatus"] = "STOPPED"
    ecs.tasks[handle["task_arn"]]["containers"] = [{"exitCode": 0}]
    exit_ = await placement.wait(handle, 1)
    assert exit_["code"] == 0

    del ecs.tasks[handle["task_arn"]]
    exit_ = await placement.wait(handle, 1)
    assert exit_ == {"code": None, "reason": "lost", "meta": {}}

    await placement.cancel({"task_arn": "arn:ecs:task/dead"})
    assert ecs.stopped == ["arn:ecs:task/dead"]


class FakeBatchApi:
    def __init__(self):
        self.jobs = {}

    def create_namespaced_job(self, namespace, body):
        name = body.metadata.name
        self.jobs[name] = types.SimpleNamespace(
            metadata=body.metadata, spec=body.spec, status=types.SimpleNamespace(conditions=[])
        )
        return self.jobs[name]

    def read_namespaced_job(self, name, namespace):
        if name not in self.jobs:
            raise KeyError(name)
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
    """§10: K8sJob creates a job carrying the stage argv; conditions map to
    Exit; a deleted job is lost."""
    from cursus_server.placements.remote import K8sJob

    batch = FakeBatchApi()
    stub_kubernetes(monkeypatch, batch)
    placement = K8sJob({"cluster": "c", "namespace": "ns"}, {"cpu": 2}, None)
    handle = await placement.launch({"attempt": "a/1", "objects": "s3://x"})
    job = batch.jobs[handle["job"]]
    args = job.spec.template.spec.containers[0].args
    assert "a/1" in args and "s3://x" in args

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
    from cursus_server.placements.remote import Modal

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
            def spawn(attempt, objects):
                call = FakeCall(uuid.uuid4().hex)
                calls[call.object_id] = (call, {"attempt": attempt, "objects": objects})
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

    placement = Modal({"app": "cursus"}, {"gpu": "A10G"}, None)
    handle = await placement.launch({"attempt": "a9", "objects": "s3://o"})
    assert calls[handle["call_id"]][1] == {"attempt": "a9", "objects": "s3://o"}

    assert await placement.wait(handle, 0.2) is None  # still running
    calls[handle["call_id"]][0].finished = True
    assert (await placement.wait(handle, 1))["code"] == 0

    del calls[handle["call_id"]]
    assert (await placement.wait(handle, 1))["reason"] == "lost"
