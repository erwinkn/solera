from __future__ import annotations

import time

import pytest

from data_orchestrator import (
    AssetContext,
    ByKey,
    CommitBatch,
    Definitions,
    Inventory,
    Output,
    asset,
)
from data_orchestrator.service import Service
from data_orchestrator.stores import JsonStore
from data_orchestrator.worker import Worker
from tests.test_engine import drain


def test_lease_loss_after_staging_cannot_publish_or_acknowledge(db):
    class ExpiringStore(JsonStore):
        def stage(self, value):
            reference = super().stage(value)
            with db.connect() as conn:
                conn.execute(
                    "UPDATE tasks SET lease_expires=now()-interval '1 second' WHERE producer='destination' AND status='running'"
                )
            return reference

    @asset
    def source():
        return Inventory([{"id": "one", "revision": "v1"}])

    @asset(
        outputs={"destination": Output(store="expiring")}, incremental=ByKey("source"), retries=0
    )
    def destination(ctx: AssetContext, source):
        return CommitBatch({"destination": [1]}, acknowledge=ctx.changes("source").token)

    definitions = Definitions([source, destination], stores={"expiring": ExpiringStore(db)})
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["destination"])
    drain(db, definitions)
    assert Service(db).asset("destination")["commits"] == []
    assert Service(db).asset("destination")["checkpoint"] is None
    assert db.recover() == 1
    assert Service(db).run(request)["request"]["status"] == "failed"


def test_retry_retains_bound_inputs_and_records_each_attempt(db):
    calls = []

    @asset(retries=1)
    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("Temporary source outage")
        return ["recovered"]

    definitions = Definitions([flaky])
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["flaky"])
    drain(db, definitions)
    assert Service(db).run(request)["tasks"][0]["status"] == "queued"
    with db.connect() as conn:
        conn.execute("UPDATE tasks SET retry_at=now() WHERE request_id=%s", (request,))
    drain(db, definitions)
    result = Service(db).run(request)
    assert result["request"]["status"] == "succeeded"
    assert [attempt["status"] for attempt in result["attempts"]] == ["failed", "succeeded"]


def test_async_asset_execution(db):
    @asset
    async def asynchronous():
        return {"awaited": True}

    definitions = Definitions([asynchronous])
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["asynchronous"])
    drain(db, definitions)
    assert Service(db).run(request)["request"]["status"] == "succeeded"


def test_idempotency_distinguishes_dependency_policy(db):
    @asset
    def source():
        return 1

    definitions = Definitions([source])
    db.register("tests.pipeline:definitions", definitions)
    db.submit(["source"], idempotency_key="request", include_upstream=True)
    with pytest.raises(ValueError, match="different request"):
        db.submit(["source"], idempotency_key="request", include_upstream=False)


def test_runtime_contract_changes_invalidate_transformation_version():
    @asset(inputs={"input": "source"}, incremental=ByKey("input"))
    def transform(input):
        return input

    previous = transform.manifest()["version"]
    transform.incremental = ByKey("input", revision="etag")
    assert transform.manifest()["version"] != previous


def test_real_subprocess_worker_finishes_example_pipeline(db):
    db.register("examples.lab:definitions")
    request = db.submit(["sample_quality", "daily_report"])
    worker = Worker(db, concurrency=3)
    deadline = time.monotonic() + 25
    try:
        while time.monotonic() < deadline:
            worker.step()
            result = Service(db).run(request)
            if result["request"]["status"] in {"failed", "succeeded"} and not worker.running:
                break
            time.sleep(0.1)
        assert result["request"]["status"] == "succeeded", result["tasks"]
        assert all(task["status"] == "succeeded" for task in result["tasks"])
    finally:
        for handle in list(worker.running):
            worker.backend.cancel(handle)
            worker.backend.release(handle)
