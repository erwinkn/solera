from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from data_orchestrator import (
    Append,
    AssetContext,
    Automation,
    ByKey,
    CommitBatch,
    Cursor,
    DailyPartitions,
    Definitions,
    Every,
    Inventory,
    OnCommit,
    Output,
    ReplaceKeys,
    asset,
)
from data_orchestrator.api import create_app
from data_orchestrator.engine import Engine
from data_orchestrator.service import Service


def drain(db, definitions, limit=30):
    for _ in range(limit):
        task = db.claim()
        if task is None:
            break
        Engine(db).execute(str(task["id"]), str(task["owner"]), definitions)
    else:
        raise AssertionError("Queue did not drain")


def materialize(db, definitions, targets, **kwargs):
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(targets, **kwargs)
    drain(db, definitions)
    return Service(db).run(request)


def rows(db, key):
    return Service(db).asset(key)["preview"]


def pipeline(*, batch_size=2):
    source = {
        "items": [
            {"id": "a", "revision": 1, "values": [1, 2]},
            {"id": "b", "revision": 1, "values": [3]},
            {"id": "c", "revision": 1, "values": [4]},
        ],
        "complete": True,
        "fail": set(),
        "calls": [],
    }

    @asset(retries=0)
    def inventory():
        return Inventory(source["items"], source["complete"])

    @asset(
        inputs={"files": "inventory"},
        outputs={"records": Output(), "counts": Output()},
        incremental=ByKey("files", batch_size=batch_size),
        retries=0,
    )
    def parse(ctx: AssetContext, files: Inventory):
        changes = ctx.changes("files")
        source["calls"].append(changes.upserted_keys)
        if set(changes.upserted_keys) & source["fail"]:
            raise RuntimeError("Injected parser failure")
        records = [
            {"file": row["id"], "value": value}
            for row in changes.upserted
            for value in row["values"]
        ]
        counts = [{"file": row["id"], "count": len(row["values"])} for row in changes.upserted]
        keys = (*changes.upserted_keys, *changes.deleted_keys)
        return CommitBatch(
            {
                "records": ReplaceKeys("file", keys, records),
                "counts": ReplaceKeys("file", keys, counts),
            },
            acknowledge=changes.token,
        )

    return Definitions([inventory, parse]), source


def test_multioutput_keyed_incrementality_and_deletions(db):
    definitions, state = pipeline()
    first = materialize(db, definitions, ["records"])
    assert first["request"]["status"] == "succeeded"
    assert len(rows(db, "records")) == 4
    assert rows(db, "counts") == [
        {"file": "a", "count": 2},
        {"file": "b", "count": 1},
        {"file": "c", "count": 1},
    ]
    before = len(Service(db).asset("records")["commits"])
    again = materialize(db, definitions, ["records"])
    assert next(t for t in again["tasks"] if t["producer"] == "parse")["status"] == "skipped"
    assert len(Service(db).asset("records")["commits"]) == before
    state["items"] = [
        {"id": "a", "revision": 2, "values": []},
        {"id": "b", "revision": 1, "values": [3]},
    ]
    materialize(db, definitions, ["records"])
    assert rows(db, "records") == [{"file": "b", "value": 3}]
    assert Service(db).asset("records")["checkpoint"]["tracked_items"] == 2
    with db.connect() as conn:
        heads = conn.execute(
            "SELECT commit_id FROM asset_heads WHERE asset_key IN ('records','counts')"
        ).fetchall()
        assert len({row["commit_id"] for row in heads}) == 1


def test_incomplete_inventory_does_not_delete(db):
    definitions, state = pipeline()
    materialize(db, definitions, ["records"])
    state["items"] = state["items"][:1]
    state["complete"] = False
    materialize(db, definitions, ["records"])
    assert len(rows(db, "records")) == 4
    assert Service(db).asset("records")["checkpoint"]["tracked_items"] == 3


def test_failed_batch_retains_prior_progress_and_repair_pins_inputs(db):
    definitions, state = pipeline()
    state["fail"] = {"c"}
    failed = materialize(db, definitions, ["records"])
    assert failed["request"]["status"] == "failed"
    assert Service(db).asset("records")["checkpoint"]["tracked_items"] == 2
    assert len(rows(db, "records")) == 3
    # A newer source run must not replace the successful input commit retained by repair.
    state["items"] = [{"id": "z", "revision": 1, "values": [99]}]
    materialize(db, definitions, ["inventory"])
    state["fail"] = set()
    state["calls"].clear()
    repair = Service(db).repair(str(failed["request"]["id"]))
    drain(db, definitions)
    assert Service(db).run(repair)["request"]["status"] == "succeeded"
    assert state["calls"] == [("c",)]
    assert len(rows(db, "records")) == 4
    assert all(row["file"] != "z" for row in rows(db, "records"))


def test_recompute_is_bounded_and_transform_change_reprocesses_items(db):
    definitions, state = pipeline(batch_size=1)
    materialize(db, definitions, ["records"])
    state["calls"].clear()
    materialize(db, definitions, ["records"], mode="recompute")
    assert state["calls"] == [("a",), ("b",), ("c",)]
    state["calls"].clear()
    definitions.assets[1].version = "2"
    materialize(db, definitions, ["records"])
    assert state["calls"] == [("a",), ("b",), ("c",)]


def test_atomic_bundle_on_invalid_output(db):
    @asset(outputs={"first": Output(), "second": Output()}, retries=0)
    def broken():
        return {"first": [1], "second": float("nan")}

    result = materialize(db, Definitions([broken]), ["first"])
    assert result["request"]["status"] == "failed"
    with db.connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM asset_heads").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM commits").fetchone()["n"] == 0


def test_append_receipts_and_cursor_state(db):
    @asset(incremental=Cursor(initial=0), retries=0)
    def events(ctx: AssetContext):
        return CommitBatch(
            {"events": Append(f"batch-{ctx.cursor}", [{"n": ctx.cursor + 1}])},
            cursor=ctx.cursor + 1,
        )

    definitions = Definitions([events])
    materialize(db, definitions, ["events"])
    materialize(db, definitions, ["events"])
    assert rows(db, "events") == [{"n": 1}, {"n": 2}]
    assert Service(db).asset("events")["checkpoint"]["cursor_value"] == 2
    with pytest.raises(ValueError, match="live cursor"):
        db.submit(["events"], mode="recompute")
    events.incremental = Cursor(initial=0, state_version="2")
    result = materialize(db, definitions, ["events"])
    assert result["request"]["status"] == "failed"
    assert Service(db).asset("events")["checkpoint"]["cursor_value"] == 2


def test_duplicate_append_is_idempotent_and_payload_collision_rejected(db):
    data = [{"id": 1}]

    @asset(retries=0)
    def records():
        return Append("stable", data)

    definitions = Definitions([records])
    materialize(db, definitions, ["records"])
    materialize(db, definitions, ["records"])
    assert rows(db, "records") == [{"id": 1}]
    data.append({"id": 2})
    result = materialize(db, definitions, ["records"])
    assert result["request"]["status"] == "failed"
    assert rows(db, "records") == [{"id": 1}]


def test_bad_ack_does_not_advance_state(db):
    @asset
    def source():
        return Inventory([{"id": "a", "revision": 1}])

    @asset(incremental=ByKey("source"), retries=0)
    def destination(ctx, source):
        return CommitBatch({"destination": []}, acknowledge="wrong")

    result = materialize(db, Definitions([source, destination]), ["destination"])
    assert result["request"]["status"] == "failed"
    assert Service(db).asset("destination")["checkpoint"] is None


def test_expired_worker_and_cancellation_are_fenced(db):
    @asset(retries=0)
    def source():
        return 1

    definitions = Definitions([source])
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["source"])
    task = db.claim()
    with db.connect() as conn:
        conn.execute(
            "UPDATE tasks SET lease_expires=now()-interval '1 second' WHERE id=%s", (task["id"],)
        )
    assert not db.heartbeat(str(task["id"]), str(task["owner"]))
    Engine(db).execute(str(task["id"]), str(task["owner"]), definitions)
    assert Service(db).asset("source")["commits"] == []
    assert db.recover() == 1
    assert Service(db).run(request)["request"]["status"] == "failed"
    request = db.submit(["source"])
    task = db.claim()
    Service(db).cancel(request)
    Engine(db).execute(str(task["id"]), str(task["owner"]), definitions)
    assert Service(db).asset("source")["commits"] == []
    assert Service(db).run(request)["request"]["status"] == "canceled"


def test_only_one_writer_claims_a_shared_scope(db):
    @asset
    def source():
        return 1

    definitions = Definitions([source])
    db.register("tests.pipeline:definitions", definitions)
    for _ in range(8):
        db.submit(["source"])
    with ThreadPoolExecutor(max_workers=8) as pool:
        claimed = list(pool.map(lambda _: db.claim(), range(8)))
    assert sum(task is not None for task in claimed) == 1


def test_partition_backfills_pause_and_fill_missing(db):
    @asset(partitions=DailyPartitions("2026-01-01"))
    def daily(ctx):
        return [{"day": ctx.partition}]

    definitions = Definitions([daily])
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["daily"], partitions=["2026-02-01", "2026-02-02"], reason="backfill")
    Service(db).pause(request, True)
    assert db.claim() is None
    Service(db).pause(request, False)
    drain(db, definitions)
    assert len(Service(db).asset("daily")["partitions"]) == 2
    result = materialize(db, definitions, ["daily"], partitions=["2026-02-01"], mode="fill_missing")
    assert result["tasks"][0]["status"] == "skipped"


def test_durable_automations_and_deduplication(db):
    @asset
    def source():
        return 1

    @asset
    def destination(source):
        return source + 1

    definitions = Definitions(
        [source, destination],
        automations=[
            Automation("timer", ("source",), Every(3600), enabled=True),
            Automation("consumer", ("destination",), OnCommit(("source",)), enabled=True),
        ],
    )
    db.register("tests.pipeline:definitions", definitions)
    assert Service(db).tick() == 1
    assert Service(db).tick() == 0
    drain(db, definitions)
    assert Service(db).tick() == 1
    assert Service(db).tick() == 0
    drain(db, definitions)
    assert rows(db, "destination") == 2
    materialize(db, definitions, ["source"])
    assert Service(db).tick() == 0  # Identical content does not invalidate consumers.
    first = db.submit(["source"], idempotency_key="one")
    assert db.submit(["source"], idempotency_key="one") == first
    with pytest.raises(ValueError, match="different request"):
        db.submit(["destination"], idempotency_key="one")


def test_manifest_mismatch_fails_instead_of_running_different_code(db):
    @asset(retries=0)
    def source():
        return 1

    definitions = Definitions([source])
    db.register("tests.pipeline:definitions", definitions)
    request = db.submit(["source"])
    source.version = "changed"
    drain(db, definitions)
    result = Service(db).run(request)
    assert result["request"]["status"] == "failed"
    assert "pinned manifest" in result["tasks"][0]["error"]


def test_api_auth_origin_validation_and_live_reads(db):
    definitions, _ = pipeline()
    db.register("tests.pipeline:definitions", definitions)
    with TestClient(create_app(db, api_token="test-secret")) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/api/catalog").status_code == 401
        client.headers["Authorization"] = "Bearer test-secret"
        assert len(client.get("/api/catalog").json()["assets"]) == 3
        assert (
            client.post(
                "/api/runs",
                json={"targets": ["records"]},
                headers={"Origin": "https://other.example"},
            ).status_code
            == 403
        )
        assert client.post("/api/runs", json={"targets": ["unknown"]}).status_code == 422
        response = client.post("/api/runs", json={"targets": ["records"]})
        assert response.status_code == 202
        drain(db, definitions)
        assert (
            client.get(f"/api/runs/{response.json()['id']}").json()["request"]["status"]
            == "succeeded"
        )
        assert client.get("/api/runs/not-a-uuid").status_code == 422
