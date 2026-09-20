"""Phase 5 — the CLI. Commands run local (in-process engine on CURSUS_STATE_URL)
and remote (CURSUS_SERVER_URL + CURSUS_API_TOKEN against a live server)."""

import json
import socket
import sys
import threading
import time

import pytest

PROJECT_SRC = """
from cursus.sdk import Output, Project, Source, asset

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
    """Run the CLI as a subprocess-free invocation; returns parsed stdout."""

    from cursus_server.cli import main

    monkeypatch.setattr(sys, "argv", ["cursus", *argv])
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
    """`cursus manifest --project` prints the manifest without a server."""

    out = cli(monkeypatch, capsys, "manifest", "--project", project_file)
    assert out["name"] == "clidemo" and "feed" in out["assets"]


def test_local_run_and_reads(project_file, state_url, capsys, monkeypatch):
    """Without CURSUS_SERVER_URL every command drives a local engine."""

    monkeypatch.delenv("CURSUS_SERVER_URL", raising=False)
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
    assert run_id in {r["id"] for r in runs}

    shown = cli(monkeypatch, capsys, "--state-url", state_url, "run-show", run_id)
    assert shown["request"]["id"] == run_id and shown["tasks"]

    task = shown["tasks"][0]
    attempt = shown["attempts"][task["id"]][-1]
    cli(monkeypatch, capsys, "--state-url", state_url, "logs", attempt["id"])

    autos = cli(monkeypatch, capsys, "--state-url", state_url, "automations")
    assert isinstance(autos, list)

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
    assert committed["ref"]["version"]


def test_local_recompute_and_partitions(project_file, state_url, capsys, monkeypatch):
    """--recompute and --partition feed the §8 run vocabulary."""

    monkeypatch.delenv("CURSUS_SERVER_URL", raising=False)
    detail = cli(
        monkeypatch,
        capsys,
        "--state-url",
        state_url,
        "run",
        "--project",
        project_file,
        "feed",
        "--recompute",
    )
    assert detail["request"]["status"] == "succeeded"
    assert detail["request"]["mode"] == "recompute"


@pytest.fixture
def server(project_file, state_url, monkeypatch):
    """A real uvicorn server on a loopback port for remote-mode CLI tests."""

    import uvicorn
    from cursus_server.api import create_app

    monkeypatch.setenv("CURSUS_PROJECT", project_file)
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
    """With CURSUS_SERVER_URL set, commands hit the API (§8 run vocabulary)."""

    monkeypatch.setenv("CURSUS_SERVER_URL", server)
    monkeypatch.delenv("CURSUS_API_TOKEN", raising=False)
    detail = cli(monkeypatch, capsys, "run", "--project", project_file, "feed")
    assert detail["request"]["status"] == "succeeded"
    run_id = detail["request"]["id"]

    runs = cli(monkeypatch, capsys, "runs")
    assert run_id in {r["id"] for r in runs}

    shown = cli(monkeypatch, capsys, "run-show", run_id)
    assert shown["request"]["id"] == run_id

    committed = cli(monkeypatch, capsys, "commit", "uploads", "--keys", '{"u-1": "v1"}')
    assert committed["ref"]["version"]

    autos = cli(monkeypatch, capsys, "automations")
    assert isinstance(autos, list)


def test_serve_insecure_guard(capsys, monkeypatch):
    """--insecure is a loopback-only escape hatch."""

    from cursus_server.cli import main

    monkeypatch.setattr(sys, "argv", ["cursus", "serve", "--host", "0.0.0.0", "--insecure"])
    with pytest.raises(SystemExit):
        main()
