"""A batch reading a fenced store reads exactly the head it was planned at
(docs/observed-set.md, "A fenced store names the commit it read"): its
read names the write it saw, and one newer than its head plans the batch
again — never a failure. While an upstream attempt holds the partition's
writer, its consumers wait instead."""

import asyncio

from solera.sdk import Incremental, Output, Project, asset
from solera.stores import Patch
from solera_server import model
from solera_server.state import State

from .remote import LiveStore, engine_for, until


def project(live: LiveStore, rows: dict, seen: list) -> Project:
    @asset(outputs=Output("items", key="id", store="live"))
    def items():
        return rows["v"]

    @asset(inputs={"items": Incremental()}, outputs=Output("copy", key="id"))
    def copy(items: list):
        seen.append(sorted((r["id"], r["v"]) for r in items))
        return [{"id": r["id"], "v": r["v"]} for r in items]

    return Project(assets=[items, copy], stores={"live": live})


async def started(tmp_path, live, rows, seen):
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(state, project(live, rows, seen), placement="inline")
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["items"]))["id"], 30)
    assert detail["request"]["status"] == "succeeded"
    return state, engine


def outcomes(detail) -> list[str]:
    [attempts] = [a for t, a in detail["attempts"].items() if t.endswith("/copy:")]
    return [a["outcome"] for a in attempts]


async def test_a_commit_installed_between_plan_and_read_replans_once(tmp_path):
    live, rows, seen = LiveStore(), {"v": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}, []
    state, engine = await started(tmp_path, live, rows, seen)

    async def commit_meanwhile():  # after `copy` was planned at items' commit 0, before it reads
        rows["v"] = Patch([{"id": "a", "v": 2}])
        detail = await engine.run_until((await engine.submit(["items"]))["id"], 30)
        assert detail["request"]["status"] == "succeeded"

    live.before_read = commit_meanwhile
    detail = await engine.run_until((await engine.submit(["copy"]))["id"], 30)
    assert detail["request"]["status"] == "succeeded", detail
    assert outcomes(detail) == ["replanned", "succeeded"]  # no failure, its budget untouched
    assert seen == [[("a", 2), ("b", 1)]]  # called once, at the newer head
    await engine.stop()
    await state.close()


async def test_a_long_upstream_write_holds_its_consumers_until_it_commits(tmp_path):
    live, rows, seen = LiveStore(), {"v": [{"id": "a", "v": 1}]}, []
    state, engine = await started(tmp_path, live, rows, seen)
    live.hold, rows["v"] = asyncio.Event(), Patch([{"id": "a", "v": 2}])
    await engine.submit(["items"])
    await until(engine, lambda: live.rows["a"]["v"] == 2)  # its rows visible, its commit not installed
    run = await engine.submit(["copy"])
    [task] = state.model.runs[run["id"]]["tasks"].values()
    await until(engine, lambda: task.get("held") == ["writing", "items/"])
    for _ in range(25):  # it waits: no attempt reads the half-committed partition
        await engine.tick()
        await asyncio.sleep(0.02)
    assert task["status"] == "queued" and not task.get("tries") and task["held"] == ["writing", "items/"]
    live.hold.set()
    detail = await engine.run_until(run["id"], 30)
    assert detail["request"]["status"] == "succeeded", detail
    assert outcomes(detail) == ["succeeded"]
    assert seen == [[("a", 2)]]
    await engine.stop()
    await state.close()


async def test_a_store_that_keeps_moving_fails_after_its_replan_window(tmp_path, monkeypatch):
    """Bounded by time, not by the task's retries: a fenced store whose reads
    never name the head (a write outside every attempt) fails the task once
    `REPLAN_FOR` has passed since its first replan."""

    monkeypatch.setattr(model, "REPLAN_FOR", 0.5)
    live, rows, seen = LiveStore(), {"v": [{"id": "a", "v": 1}]}, []
    state, engine = await started(tmp_path, live, rows, seen)
    live.written[""] = 10**9  # a generation no commit has
    detail = await engine.run_until((await engine.submit(["copy"]))["id"], 30)
    assert detail["request"]["status"] == "failed"
    found = outcomes(detail)
    assert len(found) >= 2 and set(found) == {"replanned"}
    assert seen == []
    await engine.stop()
    await state.close()
