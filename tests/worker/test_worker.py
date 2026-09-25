"""§10 end to end: the real `python -m solera_worker` subprocess under a real
Local placement against real file:// object storage."""

import asyncio
import json
import os
import signal

import pytest
from solera_server.engine import Engine
from solera_server.state import State
from solera_worker.worker import load_project


@pytest.fixture
async def state(tmp_path):
    opened = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()


def write_project(tmp_path, source):
    path = tmp_path / "proj.py"
    path.write_text(source)
    return str(path)


def make_engine(state, entrypoint, **kw):
    kw.setdefault("eval_interval", 0.05)
    manifest = load_project(entrypoint).manifest  # same code path as the worker
    return Engine(state, manifest, project=entrypoint, clock=state.clock, **kw)


async def test_local_subprocess_end_to_end(state, tmp_path):
    """§10: submit → spec object → real subprocess → store → result → commit;
    the delta file lands in the key index; ctx.log chunks are readable."""
    entrypoint = write_project(
        tmp_path,
        """
from solera.sdk import Incremental, Output, Project, asset

@asset(outputs=Output("feed", key="id"))
def feed():
    return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

@asset(inputs={"feed": Incremental()})
def consumer(ctx, feed: list):
    ctx.log("consumed", n=len(feed))
    return [{"n": len(feed), "keys": sorted(r["id"] for r in feed)}]

project = Project(assets=[feed, consumer])
""",
    )
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["consumer"], upstream=True))["id"], 60)
    assert detail["request"]["status"] == "succeeded"

    task = [t for t in detail["tasks"] if t["asset"] == "consumer"][0]
    attempt = detail["attempts"][task["id"]][0]["id"]
    # the harness wrote the batch's delta file into the output's key index
    head = state.model.heads[("feed", "")]
    assert head["count"] == 2 and head["batch"] == 0
    index = state.model.indexes[("feed", "")]
    [delta] = index.files
    assert await state.get_object(index.path(delta.name)) is not None
    assert delta.name.startswith("000000000000-") and delta.entries == 2
    # the attempt file holds the spec, the result (the commit request, with
    # what was delivered) and the index of its gzip log
    record = await state.attempt_record(detail["request"]["id"], attempt)
    assert record["spec"]["asset"] == "consumer"
    result = record["result"]
    assert result["status"] == "succeeded"
    assert result["delivered"]["feed"] == {"after": None, "upserted": ["a", "b"], "deleted": []}
    assert record["log"]["lines"] == 1 and len(record["log"]["blocks"]) == 1
    log = await state.attempt_log(detail["request"]["id"], attempt)
    assert json.loads(log.splitlines()[0])["message"] == "consumed"
    # the chunks shipped while it ran were joined into one log
    names = [p.rsplit("/", 1)[-1] for p in await state.list_objects(f"runs/{detail['request']['id']}/")]
    assert f"{attempt}.log" in names and not any(".log." in n for n in names)


async def test_revision_mismatch_writes_failed_result(state, tmp_path):
    """§10: a spec pinned to a different revision fails as a result, not a crash."""
    entrypoint = write_project(
        tmp_path,
        """
from solera.sdk import Project, asset

@asset
def job():
    return []

project = Project(assets=[job])
""",
    )
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    # Rewrite the project so the subprocess computes a different revision.
    (tmp_path / "proj.py").write_text(
        """
from solera.sdk import Project, asset

@asset(version="changed")
def job():
    return []

project = Project(assets=[job])
"""
    )
    detail = await engine.run_until((await engine.submit(["job"]))["id"], 60)
    assert detail["request"]["status"] == "failed"
    attempt = detail["attempts"][detail["tasks"][0]["id"]][0]["id"]
    result = (await state.attempt_record(detail["request"]["id"], attempt))["result"]
    assert result["status"] == "failed" and "revision mismatch" in result["error"]["message"]


async def test_killed_harness_retries(state, tmp_path, monkeypatch):
    """§10: a harness that dies leaves no result; the engine fails the attempt
    retryably and the retry commits."""
    flag = tmp_path / "slept.flag"
    monkeypatch.setenv("KILL_FLAG", str(flag))
    entrypoint = write_project(
        tmp_path,
        """
import os, pathlib, time
from solera.sdk import Project, asset

@asset
def slow():
    flag = pathlib.Path(os.environ["KILL_FLAG"])
    if not flag.exists():
        flag.write_text("x")
        time.sleep(30)          # first attempt is killed here
    return [{"ok": True}]

project = Project(assets=[slow])
""",
    )
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    run = await engine.submit(["slow"])
    await engine.tick()  # dispatch: launch the subprocess
    # Wait for the subprocess to be mid-flight, then SIGKILL it — a real crash.
    pid = None
    for _ in range(100):
        if engine.handles:
            pid = next(iter(engine.handles.values()))["pid"]
            break
        await asyncio.sleep(0.05)
    assert pid
    flag_ready = False
    for _ in range(100):
        if flag.exists():
            flag_ready = True
            break
        await asyncio.sleep(0.05)
    assert flag_ready
    os.kill(pid, signal.SIGKILL)
    detail = await engine.run_until(run["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    attempts = detail["attempts"][detail["tasks"][0]["id"]]
    assert len(attempts) == 2  # crashed once, retry committed
    assert "without a result" in (attempts[0].get("error") or "")


async def test_env_indirection_resolves_in_harness(state, tmp_path, monkeypatch):
    """§5: `env:` strings in resource config resolve in the worker process."""
    monkeypatch.setenv("TEST_SECRET", "s3cr3t")
    entrypoint = write_project(
        tmp_path,
        """
from solera.sdk import Project, asset

@asset
def whoami(vault):
    return [{"secret": vault["token"]}]

project = Project(assets=[whoami], resources={"vault": {"token": "env:TEST_SECRET"}})
""",
    )
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["whoami"]))["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    head = state.model.heads[("whoami", "")]
    body = await state.get_object(head["ref"]["handle"]["object"])
    assert json.loads(body) == [{"secret": "s3cr3t"}]


MIGRATING_PROJECT = """
import os
from collections.abc import Callable
from solera.sdk import Migration, Output, Project, asset
from solera.stores import JsonStore

class MigStore(JsonStore):
    def can_store(self, t, output):
        return t is Callable or super().can_store(t, output)

    async def migrate(self, output, migrations):
        with open(os.environ["MIGRATE_LOG"], "a") as f:
            f.write("migrate\\n")
        for m in migrations:
            m.payload(None, "")
        return [m.name for m in migrations]

    async def store(self, write, prior, scope):
        with open(os.environ["MIGRATE_LOG"], "a") as f:
            f.write("store\\n")
        return await super().store(write, prior, scope)

@asset(outputs=Output("migrated", store="mig",
                      migrations=[Migration("m1", lambda objects, prefix: None)]))
def producer():
    return {"v": 1}

project = Project(assets=[producer], stores={"mig": MigStore()})
"""


async def test_migrate_runs_before_first_write(state, tmp_path, monkeypatch):
    """§4/§10: in a real subprocess attempt, migrate() precedes the first
    store() and the head's handle carries the last applied name as `schema`."""
    log = tmp_path / "calls.log"
    monkeypatch.setenv("MIGRATE_LOG", str(log))
    entrypoint = write_project(tmp_path, MIGRATING_PROJECT)
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["producer"]))["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    assert log.read_text().splitlines() == ["migrate", "store"]
    assert state.model.heads[("migrated", "")]["ref"]["handle"]["schema"] == "m1"


async def test_failed_migration_is_not_retryable(state, tmp_path, monkeypatch):
    """§4/§10: a migration failure is a failed result with retryable=false."""
    monkeypatch.setenv("MIGRATE_LOG", str(tmp_path / "calls.log"))
    entrypoint = write_project(
        tmp_path,
        MIGRATING_PROJECT.replace("lambda objects, prefix: None", "lambda objects, prefix: 1 / 0"),
    )
    engine = make_engine(state, entrypoint, lease_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["producer"]))["id"], 60)
    assert detail["request"]["status"] == "failed"
    attempt = detail["attempts"][detail["tasks"][0]["id"]][0]["id"]
    result = (await state.attempt_record(detail["request"]["id"], attempt))["result"]
    assert result["status"] == "failed"
    assert result["error"]["retryable"] is False
    assert "migration failed" in result["error"]["message"]
