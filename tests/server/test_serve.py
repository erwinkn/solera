"""`solera serve` as a process. With SOLERA_SELFTEST=1 it probes the
storage before it listens (docs/railway.md): a failed probe exits
non-zero and never answers /healthz, so the deployment fails; a passed
one serves. A SIGTERM — how every platform stops it — exits 0. A local
serve reloads its project's code in place; one whose engine was replaced
exits rather than answer 503."""

import asyncio
import hashlib
import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request

SERVE = "from solera_server.cli import main; main()"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def listening(port) -> bool:
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def serve(state_url, port, tmp_path, **env):
    return subprocess.Popen(
        [sys.executable, "-c", SERVE, "--state-url", state_url, "serve", "--insecure", "--port", str(port)],
        env={**os.environ, "SOLERA_SELFTEST": "1", "SOLERA_DATA_URL": (tmp_path / "data").as_uri(), **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
    return process.communicate(timeout=30)


def careless_bucket(asked: threading.Event, answer: threading.Event):
    """An S3 endpoint that serves reads and writes but ignores
    `If-None-Match: *`, so the journal could not fence a stale writer:
    the engine would run on it, the probe must not. Its answer to the
    probe's second create-only write waits on `answer`."""

    objects: dict[str, bytes] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def reply(self, status, body=b"", etag=None):
            self.send_response(status)
            if etag:
                self.send_header("ETag", f'"{etag}"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_PUT(self):
            path, body = self.path.split("?")[0], self.rfile.read(int(self.headers["Content-Length"] or 0))
            if "/conformance/" in path and path in objects:
                asked.set()
                answer.wait(60)
            objects[path] = body
            self.reply(200, etag=hashlib.md5(body).hexdigest())

        def do_GET(self):
            path = self.path.split("?")[0]
            if path not in objects:
                return self.reply(404, b"<Error><Code>NoSuchKey</Code></Error>")
            self.reply(200, objects[path], etag=hashlib.md5(objects[path]).hexdigest())

        def do_DELETE(self):
            objects.pop(self.path.split("?")[0], None)
            self.reply(204)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_a_failed_probe_exits_non_zero_without_listening(tmp_path):
    asked, answer = threading.Event(), threading.Event()
    s3, port = careless_bucket(asked, answer), free_port()
    endpoint = f"http://127.0.0.1:{s3.server_address[1]}"
    aws = {"AWS_ENDPOINT": endpoint, "AWS_ALLOW_HTTP": "true", "AWS_REGION": "auto"}
    aws |= {"AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "y"}
    process = serve("s3://probe/orchestrator", port, tmp_path, **aws)
    try:
        assert asked.wait(120), "the probe never wrote twice"
        assert not listening(port)  # mid-probe: no /healthz to pass
        answer.set()
        out, err = process.communicate(timeout=60)
    finally:
        answer.set()
        stop(process)
        s3.shutdown()
    assert process.returncode not in (0, None)
    assert "Backend ignored create-if-absent" in err and "passed" not in out
    assert not listening(port)


def test_a_passed_probe_serves(tmp_path):
    port = free_port()
    process = serve((tmp_path / "state").as_uri(), port, tmp_path)
    try:
        deadline = time.monotonic() + 120
        while not listening(port):
            assert process.poll() is None, stop(process)
            assert time.monotonic() < deadline, "serve never listened"
            time.sleep(0.2)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=10) as response:
            assert response.status == 200
    finally:
        out, _ = stop(process)
    assert '"status": "passed"' in out


def test_a_sigterm_stops_it_cleanly(tmp_path):
    """systemd, Railway and the rest stop a service with SIGTERM on every
    redeploy: a graceful stop is a success, not a failed unit."""

    port = free_port()
    process = serve((tmp_path / "state").as_uri(), port, tmp_path, SOLERA_SELFTEST="0")
    try:
        deadline = time.monotonic() + 120
        while not listening(port):
            assert process.poll() is None, stop(process)
            assert time.monotonic() < deadline, "serve never listened"
            time.sleep(0.2)
        process.send_signal(signal.SIGTERM)  # the process itself, as a platform signals it
        _, err = process.communicate(timeout=60)
    finally:
        stop(process)
    assert process.returncode == 0, err[-2000:]
    assert "Application shutdown complete" in err


PROJECT_SRC = """
from solera.sdk import Output, Project, asset

@asset(outputs=Output("feed", key="k"), version="1")
def feed():
    return [{"k": "a", "v": 1}]

project = Project(assets=[feed], name="reloady")
"""


def get(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
        return json.loads(response.read())


def project_serve(tmp_path):
    (tmp_path / "proj.py").write_text(PROJECT_SRC)
    port = free_port()
    project = f"{tmp_path / 'proj.py'}:project"
    process = serve(
        (tmp_path / "state").as_uri(), port, tmp_path, SOLERA_SELFTEST="0", SOLERA_PROJECT=project
    )
    deadline = time.monotonic() + 120
    while not listening(port):
        assert process.poll() is None, stop(process)
        assert time.monotonic() < deadline, "serve never listened"
        time.sleep(0.2)
    return process, port


def test_a_reload_serves_the_new_code_in_the_same_process(tmp_path):
    """--insecure reloads by default: an edit to the project's module is a
    new deploy in the same engine, no successor; a file beside it that no
    module of the project is (a console build's assets) is none."""

    process, port = project_serve(tmp_path)
    try:
        first = get(port, "/api/diagnostics")["deploy"]
        (tmp_path / "web").mkdir()
        (tmp_path / "web" / "app.js").write_text("console.log(1)")
        time.sleep(3)  # past the reload's poll and quiet second
        assert get(port, "/api/diagnostics")["deploy"] == first
        (tmp_path / "proj.py").write_text(PROJECT_SRC.replace('version="1"', 'version="2"'))
        deadline = time.monotonic() + 60
        while get(port, "/api/diagnostics")["deploy"] == first:
            assert time.monotonic() < deadline, "the edit was never served"
            time.sleep(0.2)
        [feed] = get(port, "/api/projects/reloady/assets")["assets"]
        assert feed["version"] == "2" and process.poll() is None
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=10) as response:
            assert response.status == 200
    finally:
        _, err = stop(process)
    assert "writes no more" not in err and "was replaced" not in err


def test_a_serve_whose_engine_was_replaced_exits(tmp_path):
    """Another engine takes the namespace: at its next write the serve's
    engine finds itself fenced, and the process ends (0: its successor owns
    everything) rather than stay up answering 503."""

    from solera_server.state import State

    process, port = project_serve(tmp_path)

    async def takeover():
        successor = await State.open((tmp_path / "state").as_uri(), "default")
        try:
            body = json.dumps({"targets": ["feed"]}).encode()
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/projects/reloady/runs",
                body,
                {"content-type": "application/json"},
            )
            try:  # its write is refused: the request may fail as the serve stops
                await asyncio.to_thread(urllib.request.urlopen, request, timeout=10)
            except OSError:
                pass
            return await asyncio.to_thread(process.wait, 60)
        finally:
            await successor.close()

    try:
        code = asyncio.run(takeover())
    finally:
        _, err = stop(process)
    assert code == 0, err[-2000:]
    assert "writes no more" in err
