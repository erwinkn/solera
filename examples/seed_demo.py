"""Seed synthetic demonstration runs, idempotently, before serving the console."""
from __future__ import annotations

import time
from datetime import date, timedelta

from data_orchestrator.database import Database
from data_orchestrator.service import Service
from data_orchestrator.worker import Worker


def complete(database: Database, request_ids: list[str]) -> None:
    worker = Worker(database, concurrency=2)
    service = Service(database)
    deadline = time.monotonic() + 180
    try:
        while time.monotonic() < deadline:
            worker.step()
            states = [service.run(request_id)["request"]["status"] for request_id in request_ids]
            if all(state in {"succeeded", "failed", "canceled"} for state in states) and not worker.running:
                if any(state != "succeeded" for state in states):
                    raise RuntimeError(f"Demo seed did not succeed: {states}")
                return
            time.sleep(0.2)
        raise TimeoutError("Demo initialization exceeded its execution budget")
    finally:
        for handle, (task, _) in list(worker.running.items()):
            worker.backend.cancel(handle)
            worker.backend.release(handle)
            database.fail(str(task["id"]), str(task["owner"]), "Demo initialization stopped")
        worker.running.clear()


def main() -> None:
    database = Database()
    database.migrate()
    manifest_id = database.register("examples.lab:definitions")
    first = database.submit(
        ["sample_quality", "daily_report"],
        reason="demo:initial",
        idempotency_key=f"demo:initial:{manifest_id}",
    )
    start = date(2026, 9, 1)
    historical = database.submit(
        ["daily_report"],
        partitions=[(start + timedelta(days=index)).isoformat() for index in range(7)],
        mode="fill_missing",
        reason="backfill",
        idempotency_key=f"demo:backfill:{manifest_id}",
    )
    complete(database, [first, historical])
    unchanged = database.submit(
        ["sample_quality"],
        reason="demo:unchanged-inputs",
        idempotency_key=f"demo:unchanged:{manifest_id}",
    )
    complete(database, [unchanged])
    catalog = Service(database).catalog()
    assert all(asset["status"] == "materialized" for asset in catalog["assets"]), catalog
    print(
        f"Demo ready: {len(catalog['assets'])} assets, seeded backfill and no-change run; "
        "all requests succeeded. Synthetic data only.",
        flush=True,
    )


if __name__ == "__main__":
    main()
