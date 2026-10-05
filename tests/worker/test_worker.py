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
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["consumer"], upstream=True))["id"], 60)
    assert detail["request"]["status"] == "succeeded"

    task = [t for t in detail["tasks"] if t["asset"] == "consumer"][0]
    attempt = detail["attempts"][task["id"]][0]["id"]
    # the worker wrote the batch's delta file into the output's key index
    head = state.model.heads[("feed", "")]
    assert head["count"] == 2 and head["commit_number"] == 0
    index = state.model.indexes[("feed", "")]
    [delta] = index.files
    assert await state.get_object(index.path(delta.name)) is not None
    assert delta.name.startswith("000000000000-") and delta.entries == 2
    # the spec, and the control file with the sealed result (the commit request,
    # with what was delivered, and the log: short enough to travel inside it)
    assert (await state.attempt_spec(detail["request"]["id"], attempt))["asset"] == "consumer"
    result = await state.attempt_result(detail["request"]["id"], attempt)
    assert result["status"] == "succeeded" and result["write"] == "complete"
    generation = head["ref"]["generation"]  # each key's version: the generation that wrote it
    assert result["delivered"]["feed"] == {"observed": {"a": generation, "b": generation}}
    assert result["log"]["lines"] == 1 and result["log"]["chunks"] == [] and result["log"]["tail"]
    log = await state.attempt_log(detail["request"]["id"], attempt)
    assert json.loads(log.splitlines()[0])["message"] == "consumed"
    paths = await state.list_objects(f"runs/{detail['request']['id']}/")
    names = {p.rsplit("/", 1)[-1] for p in paths if p.rsplit("/", 1)[-1].startswith(attempt)}
    # its reports through `.beat` (it has no channel), no log object, and no gate
    # taken: a FileStore is immutable (docs/lifecycle.md §9.6)
    assert names <= {f"{attempt}{s}" for s in (".spec", ".control", ".beat")}
    assert {f"{attempt}.spec", f"{attempt}.control"} <= names and "intents" not in result


async def test_revision_mismatch_writes_failed_result(state, tmp_path):
    """§10: a spec pinned to a different deploy fails as a result, not a crash."""
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
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    # Rewrite the project so the subprocess computes a different deploy.
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
    result = await state.attempt_result(detail["request"]["id"], attempt)
    assert result["status"] == "failed" and "deploy mismatch" in result["error"]["message"]


async def test_killed_harness_retries(state, tmp_path, monkeypatch):
    """§10: a worker that dies leaves no result; the engine fails the attempt
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
        flag.write_text(str(os.getpid()))
        time.sleep(30)          # first attempt is killed here
    return [{"ok": True}]

project = Project(assets=[slow])
""",
    )
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    run = await engine.submit(["slow"])
    await engine.tick()  # dispatch: launch the subprocess
    # Wait for the subprocess to be mid-flight, then SIGKILL it — a real crash.
    for _ in range(1200):
        if flag.exists() and flag.read_text():
            break
        await asyncio.sleep(0.05)
    os.kill(int(flag.read_text()), signal.SIGKILL)
    detail = await engine.run_until(run["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    attempts = detail["attempts"][detail["tasks"][0]["id"]]
    assert len(attempts) == 2  # crashed once, retry committed
    assert "without a result" in (attempts[0].get("error") or "")


async def test_env_indirection_resolves_in_harness(state, tmp_path, monkeypatch, data):
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
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["whoami"]))["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    head = state.model.heads[("whoami", "")]
    body = (data / f"{head['ref']['handle']['path']}.json").read_text()
    assert json.loads(body) == [{"secret": "s3cr3t"}]


MIGRATING_PROJECT = """
import os
from collections.abc import Callable
from solera.sdk import Migration, Output, Project, asset
from solera.stores import FileStore

class MigStore(FileStore):
    def can_store(self, t, output):
        return t is Callable or super().can_store(t, output)

    async def migrate(self, output, migrations, context=None, prior=None):
        with open(os.environ["MIGRATE_LOG"], "a") as f:
            f.write("migrate\\n")
        for m in migrations:
            m.payload(None, "")
        return [m.name for m in migrations]

    async def store(self, write, prior, context):
        with open(os.environ["MIGRATE_LOG"], "a") as f:
            f.write("store\\n")
        return await super().store(write, prior, context)

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
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
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
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["producer"]))["id"], 60)
    assert detail["request"]["status"] == "failed"
    attempt = detail["attempts"][detail["tasks"][0]["id"]][0]["id"]
    result = await state.attempt_result(detail["request"]["id"], attempt)
    assert result["status"] == "failed"
    assert result["error"]["retryable"] is False
    assert "migration failed" in result["error"]["message"]


LINGERING = """
import threading, time
from solera.executors import Pool
from solera.sdk import Project, asset

@asset{executor}
def lingering() -> int:
    # A call the attempt gave up on — a canceled synchronous Each call — still running.
    threading.Thread(target=time.sleep, args=(60,)).start()
    return 1

project = Project(assets=[lingering])
"""


async def test_a_local_attempt_ends_its_process_once_published(state, tmp_path, monkeypatch):
    """D5: a thread the attempt left running does not keep its process
    alive: the worker exits as soon as the result is published."""

    from solera_server.executors import local

    pids, launch = [], local.LocalPlacement.launch

    async def recorded(self, stage):
        handle = await launch(self, stage)
        pids.append(handle["pid"])
        return handle

    monkeypatch.setattr(local.LocalPlacement, "launch", recorded)
    entrypoint = write_project(tmp_path, LINGERING.format(executor=""))
    engine = make_engine(state, entrypoint, heartbeat_seconds=30)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["lingering"]))["id"], 60)
    assert detail["request"]["status"] == "succeeded"
    for _ in range(1200):
        try:
            os.kill(pids[0], 0)
        except ProcessLookupError:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("the worker outlived its result")


ABANDONED_CALL = """
import asyncio, time
from solera.executors import Pool
from solera.sdk import Project, asset

@asset(executor=Pool("ingest")())
async def lingering() -> int:
    # A blocking call the attempt gave up on, in the event loop's executor.
    asyncio.get_running_loop().run_in_executor(None, time.sleep, 60)
    return 1

project = Project(assets=[lingering])
"""


LOCKED_AT_IMPORT = """
import threading, time
from solera.executors import Pool
from solera.sdk import Project, asset

lock = threading.Lock()

def busy():  # a client's background thread, holding its lock for a while after import
    with lock:
        time.sleep(2)

threading.Thread(target=busy, daemon=True).start()

@asset(executor=Pool("ingest")())
def lingering() -> int:
    if not lock.acquire(timeout=30):  # forked mid-hold from a process that imported this: never released
        raise RuntimeError("the project's lock was inherited held")
    lock.release()
    return 1

project = Project(assets=[lingering])
"""


@pytest.mark.parametrize(
    "source",
    [LINGERING.format(executor='(executor=Pool("ingest")())'), ABANDONED_CALL, LOCKED_AT_IMPORT],
    ids=["thread", "executor-call", "locked-at-import"],
)
async def test_a_pool_attempt_runs_in_a_child_of_the_warm_worker(state, tmp_path, monkeypatch, source):
    """D5: the pool worker forks a child per attempt from its imported
    project; the child ends with its attempt, threads it left included — a
    call left in the loop's executor too (review round 3, #3)."""

    import subprocess
    import sys

    imports = tmp_path / "imports.txt"
    recorded = (
        f"open({str(imports)!r}, 'a').write('imported\\n')\n"  # each import of the project leaves a line
    )
    entrypoint = write_project(tmp_path, recorded + source)
    monkeypatch.setenv("SOLERA_PROJECT", entrypoint)
    # A short grace: the engine reads the claim and the result without the worker's
    # channel, which this worker has none of, a moment after the offer, not 10 s.
    engine = make_engine(state, entrypoint, heartbeat_seconds=0.2, pool_offered_grace=0.5)
    await engine.initialize()
    run = await engine.submit(["lingering"])
    for _ in range(3000):  # offered once its launch is durable (F26): a loaded machine flushes late
        await engine.tick()
        if offered := await engine.pool_work("ingest", {}, "w1", 0):
            break
        await asyncio.sleep(0.02)
    [stage] = offered
    # A pool worker's own process; its attempts start from a forkserver with the project imported.
    worker = (
        "import asyncio, json, sys\n"
        "from solera_worker.worker import _forked\n"
        "print(asyncio.run(_forked(json.loads(sys.argv[1]), None)))\n"
    )
    started = asyncio.get_running_loop().time()
    done = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", worker, json.dumps(stage), entrypoint],
        capture_output=True,
        text=True,
        timeout=55,
    )
    assert done.stdout.strip().splitlines()[-1] == "0", done.stderr
    assert asyncio.get_running_loop().time() - started < 45  # not the lingering minute, under load too
    # By this test's engine, and by the attempt's child: never in the forkserver it was forked from.
    assert imports.read_text().count("imported") == 2
    detail = await engine.run_until(run["id"], 60)
    assert detail["request"]["status"] == "succeeded"


def test_a_worker_of_plain_rows_imports_no_dataframe_library(tmp_path):
    """A project whose outputs are lists of dicts — keyed, batched, a value —
    runs in workers that never import pandas, pyarrow or duckdb, nor the
    engine's libraries: a library is imported only for a value of its own
    type, and `pip install solera` (no `[server]`) is a worker's install."""

    import subprocess
    import sys
    import textwrap

    project = tmp_path / "plain.py"
    project.write_text(
        textwrap.dedent(
            """
            import atexit, os, sys
            from solera import Output, Patch, Project, asset

            @asset(outputs=Output("items", key="id"))
            def items():
                return [{"id": "a", "n": 1}, {"id": "b", "n": 2}]

            @asset(outputs=Output("events", incremental=True))
            def events(items: list[dict]):
                return Patch([{"e": len(items)}])

            @asset
            def total(items: list[dict], events: list[dict]) -> int:
                return sum(r["n"] for r in items) + len(events)

            def record():
                libraries = ("pandas", "pyarrow", "duckdb", "numpy", "fastapi", "starlette", "uvicorn")
                seen = [m for m in libraries if m in sys.modules]
                name = os.path.join(os.path.dirname(__file__), f"modules-{os.getpid()}.txt")
                with open(name, "w") as f:
                    f.write(",".join(seen))

            # An attempt's process ends at once (`_exit`), skipping `atexit`: record first.
            import solera_worker.worker as worker

            end = worker._exit
            worker._exit = lambda code: (record(), end(code))
            atexit.register(record)
            project = Project(name="plain", assets=[items, events, total])
            """
        )
    )
    done = subprocess.run(
        [
            sys.executable,
            "-c",
            "from solera_server.cli import main; main()",
            "--state-url",
            (tmp_path / "state").as_uri(),
            "run",
            "--project",
            f"{project}:project",
            "total",
            "--upstream",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        env={k: v for k, v in os.environ.items() if k != "SOLERA_SERVER_URL"},
    )
    assert done.returncode == 0, done.stderr[-3000:]
    assert '"succeeded"' in done.stdout, done.stdout[-3000:]
    records = sorted(tmp_path.glob("modules-*.txt"))
    assert len(records) >= 4  # the manifest, and an attempt of each asset
    assert {r.name: r.read_text() for r in records if r.read_text()} == {}


async def test_a_local_worker_reaches_state_on_a_private_object_store(tmp_path, monkeypatch):
    """Review round 2, system #2: the engine's state on a private S3
    (MinIO here), configured by `AWS_*` in its environment. A Local worker
    gets those back — and its first read, the spec, succeeds — while other
    credentials stay out."""

    import uuid
    from urllib.parse import unquote, urlsplit

    url = os.environ.get("SOLERA_TEST_S3")
    if not url:
        pytest.skip("set SOLERA_TEST_S3 to run against an S3-compatible server")
    u = urlsplit(url)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", unquote(u.username))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", unquote(u.password))
    monkeypatch.setenv("AWS_ENDPOINT", f"{u.scheme}://{u.netloc.rpartition('@')[2]}")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("GH_TOKEN", "not for workers")
    entrypoint = write_project(
        tmp_path,
        """
import os
from solera.sdk import Project, asset

@asset
def leak() -> dict:
    return {"gh": os.environ.get("GH_TOKEN")}

project = Project(assets=[leak])
""",
    )
    state = await State.open(
        f"s3://{u.path.strip('/')}/local-{uuid.uuid4().hex}", "test", flush_interval=0.001
    )
    try:
        engine = make_engine(state, entrypoint, heartbeat_seconds=30)
        await engine.initialize()
        detail = await engine.run_until((await engine.submit(["leak"]))["id"], 60)
        assert detail["request"]["status"] == "succeeded", detail
        [written] = (tmp_path / "data").glob("leak@*.json")
        assert json.loads(written.read_text()) == {"gh": None}
    finally:
        await state.close()


def test_the_sdk_and_worker_import_graph_holds_no_server_or_dataframe_library():
    """`solera` (the SDK), `solera_worker` and the stores import only the
    base install's libraries: the engine's — fastapi, uvicorn, duckdb — and
    pandas or pyarrow never come in through them."""

    import subprocess
    import sys

    code = (
        "import sys, solera, solera.stores, solera.keys, solera_worker.worker, solera_worker.channel, "
        "solera_worker.sensors, solera_worker.each, solera_postgres\n"
        "banned = ('pandas', 'pyarrow', 'duckdb', 'numpy', 'fastapi', 'starlette', 'uvicorn', 'psycopg')\n"
        "print(','.join(m for m in banned if m in sys.modules))"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == ""


SERVED = """
from solera.sdk import Project, asset

@asset
def tiny() -> int:
    return 1

project = Project(assets=[tiny], name="served")
"""


def test_a_served_engine_keeps_nothing_of_finished_local_workers(tmp_path, monkeypatch):
    """Review round 3, B3: workers that report `finished` over HTTP are
    settled before their process exits; the placement still reaps each one,
    and keeps nothing of it once settled."""

    import contextlib
    import socket
    import threading
    import time

    import httpx
    import uvicorn
    from solera_server.api import create_app
    from solera_server.executors import local

    (tmp_path / "served.py").write_text(SERVED)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    base = f"http://127.0.0.1:{port}"
    app = create_app(
        state_url=(tmp_path / "state").as_uri(),
        project=str(tmp_path / "served.py"),
        insecure=True,
        engine_url=base,
    )
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(1200):
            with contextlib.suppress(httpx.HTTPError):
                if httpx.get(f"{base}/healthz", timeout=1).status_code == 200:
                    break
            time.sleep(0.05)
        before = set(local._children)  # other tests' launches
        for _ in range(3):
            run = httpx.post(
                f"{base}/api/projects/served/runs", json={"targets": ["tiny"]}, timeout=10
            ).json()
            deadline = time.monotonic() + 60
            while (
                httpx.get(f"{base}/api/projects/served/runs/{run['id']}", timeout=10).json()["request"][
                    "status"
                ]
                != "succeeded"
            ):
                assert time.monotonic() < deadline
                time.sleep(0.1)
        deadline = time.monotonic() + 20
        while left := set(local._children) - before:
            assert time.monotonic() < deadline, left
            time.sleep(0.1)
    finally:
        server.should_exit = True
        thread.join(timeout=20)
