"""The CLI. Commands run local (in-process engine on SOLERA_STATE_URL)
and remote (SOLERA_SERVER_URL + SOLERA_API_TOKEN against a live server)."""

import json
import socket
import sys
import threading
import time

import pytest

PROJECT_SRC = """
from solera.sdk import Output, Project, Source, asset

@asset(outputs=Output("feed", key="k"))
def feed():
    return [{"k": "a", "v": 1}, {"k": "b", "v": 2}]

@asset(inputs={"feed": "feed"})
def total(feed: list):
    return {"n": sum(v["v"] for v in feed)}

uploads = Source("uploads", key="id")
project = Project(assets=[feed, total], sources=[uploads], name="clidemo")
"""


@pytest.fixture
def project_file(tmp_path):
    path = tmp_path / "clidemo.py"
    path.write_text(PROJECT_SRC)
    return str(path)


@pytest.fixture
def state_url(tmp_path):
    return (tmp_path / "state").as_uri()


def cli(monkeypatch, capsys, *argv):
    """Run the CLI as a subprocess-free worker; returns parsed stdout."""

    from solera_server.cli import main

    monkeypatch.setattr(sys, "argv", ["solera", *argv])
    try:
        main()
    except SystemExit as error:
        if error.code not in (None, 0):
            raise
    out = capsys.readouterr().out
    if not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return out


def test_manifest(project_file, capsys, monkeypatch):
    """`solera manifest --project` prints the manifest without a server."""

    out = cli(monkeypatch, capsys, "manifest", "--project", project_file)
    assert out["name"] == "clidemo" and "feed" in out["assets"]


def test_local_run_and_reads(project_file, state_url, capsys, monkeypatch):
    """Without SOLERA_SERVER_URL every command drives a local engine."""

    monkeypatch.delenv("SOLERA_SERVER_URL", raising=False)
    detail = cli(
        monkeypatch,
        capsys,
        "--state-url",
        state_url,
        "run",
        "--project",
        project_file,
        "feed",
    )
    assert detail["request"]["status"] == "succeeded"
    run_id = detail["request"]["id"]

    runs = cli(monkeypatch, capsys, "--state-url", state_url, "runs")
    assert run_id in {r["id"] for r in runs["runs"]}

    shown = cli(monkeypatch, capsys, "--state-url", state_url, "run-show", run_id)
    assert shown["request"]["id"] == run_id and shown["tasks"]

    task = shown["tasks"][0]
    attempt = shown["attempts"][task["id"]][-1]
    cli(monkeypatch, capsys, "--state-url", state_url, "logs", run_id, attempt["id"], "--tail", "5")

    dry = cli(monkeypatch, capsys, "--state-url", state_url, "runs", "prune", "--asset", "feed", "--dry-run")
    assert dry == {"deleted": [run_id], "dry_run": True}
    deleted = cli(monkeypatch, capsys, "--state-url", state_url, "runs", "delete", run_id)
    assert deleted == {"deleted": [run_id]}
    assert run_id not in {r["id"] for r in cli(monkeypatch, capsys, "--state-url", state_url, "runs")["runs"]}

    autos = cli(monkeypatch, capsys, "--state-url", state_url, "automations")
    assert isinstance(autos, list)

    fresh = cli(monkeypatch, capsys, "--state-url", state_url, "stale", "feed")
    assert fresh == {"stale": False, "reasons": [], "tracked": True, "keys": []}

    committed = cli(
        monkeypatch,
        capsys,
        "--state-url",
        state_url,
        "commit",
        "uploads",
        "--keys",
        '{"u-1": "v1"}',
    )
    assert committed["ref"]["generation"]


def test_local_full_and_partitions(project_file, state_url, capsys, monkeypatch):
    """--full and --partition feed the §8 run vocabulary."""

    monkeypatch.delenv("SOLERA_SERVER_URL", raising=False)
    detail = cli(
        monkeypatch,
        capsys,
        "--state-url",
        state_url,
        "run",
        "--project",
        project_file,
        "feed",
        "--full",
    )
    assert detail["request"]["status"] == "succeeded"
    assert detail["request"]["mode"] == "full"


@pytest.fixture
def server(project_file, state_url, monkeypatch):
    """A real uvicorn server on a loopback port for remote-mode CLI tests."""

    import uvicorn
    from solera_server.api import create_app

    monkeypatch.setenv("SOLERA_PROJECT", project_file)
    app = create_app(
        state_url=state_url,
        namespace="test",
        project=project_file,
        insecure=True,
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    import httpx

    for _ in range(200):
        try:
            if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=1).status_code == 200:
                break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def test_remote_run_and_reads(server, project_file, capsys, monkeypatch):
    """With SOLERA_SERVER_URL set, commands hit the API (§8 run vocabulary)."""

    monkeypatch.setenv("SOLERA_SERVER_URL", server)
    monkeypatch.delenv("SOLERA_API_TOKEN", raising=False)
    detail = cli(monkeypatch, capsys, "run", "--project", project_file, "feed")
    assert detail["request"]["status"] == "succeeded"
    run_id = detail["request"]["id"]

    runs = cli(monkeypatch, capsys, "runs")
    assert run_id in {r["id"] for r in runs["runs"]}

    shown = cli(monkeypatch, capsys, "run-show", run_id)
    assert shown["request"]["id"] == run_id

    committed = cli(monkeypatch, capsys, "commit", "uploads", "--keys", '{"u-1": "v1"}')
    assert committed["ref"]["generation"]

    autos = cli(monkeypatch, capsys, "automations")
    assert isinstance(autos, list)

    unkeyed = cli(monkeypatch, capsys, "stale", "total")  # never run: missing, and it has no keys
    assert unkeyed == {"stale": False, "reasons": [], "tracked": False, "keys": []}


def test_serve_insecure_guard(capsys, monkeypatch):
    """--insecure is a loopback-only escape hatch."""

    from solera_server.cli import main

    monkeypatch.setattr(sys, "argv", ["solera", "serve", "--host", "0.0.0.0", "--insecure"])
    with pytest.raises(SystemExit):
        main()


MIGRATE_PROJECT = """
import json
import os

from solera.sdk import Migration, Output, Project, asset
from solera.stores import FileStore


class Ledgered(FileStore):
    \"\"\"Runs callable migrations once each, noting them in a ledger file.\"\"\"

    async def migrate(self, output, migrations, context=None, prior=None):
        path = os.path.join(os.environ["SOLERA_DATA"], "ledger.json")
        applied = json.load(open(path)) if os.path.exists(path) else []
        for m in migrations:
            if m.name not in applied:
                m.payload()
                applied.append(m.name)
        json.dump(applied, open(path, "w"))
        return applied


def seed():
    pass


@asset(outputs=Output("docs", store="ledgered", migrations=[Migration("seed", seed)]))
def docs():
    return b"x"


project = Project(assets=[docs], stores={"ledgered": Ledgered()}, name="migdemo")
"""


def test_migrate_command_applies_and_is_idempotent(
    project_file, state_url, capsys, monkeypatch, tmp_path, data
):
    """§4: `solera migrate` applies pending migrations for all migrating
    outputs through the local path, prints the applied names, and a second
    run applies nothing."""
    import json as jsonlib

    path = tmp_path / "migdemo.py"
    path.write_text(MIGRATE_PROJECT)
    monkeypatch.delenv("SOLERA_SERVER_URL", raising=False)
    data.mkdir()

    out = cli(monkeypatch, capsys, "--state-url", state_url, "migrate", "--project", str(path))
    assert "docs: applied seed" in out
    assert jsonlib.loads((data / "ledger.json").read_text()) == ["seed"]

    out = cli(monkeypatch, capsys, "--state-url", state_url, "migrate", "--project", str(path))
    assert "docs: applied seed" in out  # ledger names; nothing re-applied
    assert jsonlib.loads((data / "ledger.json").read_text()) == ["seed"]


async def test_local_reads_leave_the_running_writer_alone(project_file, state_url, capsys, monkeypatch):
    """System review #1: a coordinator owns the namespace; local read
    commands open it as readers, so it goes on recording."""

    import asyncio

    from solera_server.state import State

    monkeypatch.delenv("SOLERA_SERVER_URL", raising=False)

    def run(*argv):
        return cli(monkeypatch, capsys, "--state-url", state_url, *argv)

    detail = await asyncio.to_thread(run, "run", "--project", project_file, "feed")
    run_id = detail["request"]["id"]
    attempt = detail["attempts"][detail["tasks"][0]["id"]][-1]["id"]
    coordinator = await State.open(state_url, "default", flush_interval=0.001)
    for argv in (
        ("runs",),
        ("run-show", run_id),
        ("logs", run_id, attempt),
        ("automations",),
        ("runs", "prune", "--dry-run"),
        ("cleanups", "feed"),
    ):
        await asyncio.to_thread(run, *argv)
    coordinator.record({"type": "AutomationChanged", "name": "none", "enabled": True})
    await coordinator.durable()  # still the writer: nothing fenced it
    await coordinator.close()
