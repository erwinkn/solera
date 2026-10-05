"""`solera serve` as a process. With SOLERA_SELFTEST=1 it probes the
storage before it listens (docs/railway.md): a failed probe exits
non-zero and never answers /healthz, so the deployment fails; a passed
one serves. A SIGTERM — how every platform stops it — exits 0."""

import hashlib
import http.server
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
