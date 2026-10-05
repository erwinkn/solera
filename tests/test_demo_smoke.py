"""The demo as W59 runs it: a real `solera serve` on fresh state, several
runs done — keyed incremental, per-key, whole inputs, a full run whose
rewrites leave a cleanup, a `keys="all"` run — then every GET route the
console reads answers 200, through a history flush and again after a
restart that restores the buffered history from its checkpoint. The
serve's log holds no failed flush and no traceback, and each stop exits 0.

914 unit tests passed while every run detail answered 500 on a real serve
(a checkpoint's history rows of an older shape, 763c64d): this is the end
to end check the gate lacked."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SERVE = "from solera_server.cli import main; main()"
BASE = "/api/projects/demo"
TERMINAL = {"succeeded", "failed", "canceled", "skipped"}
# Routes no console page reads: a sensor host's long poll, a pool worker's discovery.
WORKERS_ONLY = {"/sensors/next", "/pools/{pool}/work"}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Serve:
    """`solera serve` on the demo project, its output in a log file."""

    def __init__(self, tmp_path: Path, name: str):
        self.port, self.log = free_port(), tmp_path / f"{name}.log"
        env = {k: v for k, v in os.environ.items() if not k.startswith("SOLERA_")}
        env |= {"SOLERA_SELFTEST": "0", "SOLERA_DATA_URL": (tmp_path / "data").as_uri()}
        argv = [sys.executable, "-c", SERVE, "--state-url", (tmp_path / "state").as_uri()]
        with open(self.log, "w") as out:
            self.process = subprocess.Popen(
                [*argv, "serve", "--insecure", "--no-reload", "--port", str(self.port)],
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.monotonic() + 120
        while self.status("/healthz") != 200:
            assert self.process.poll() is None, self.text()
            assert time.monotonic() < deadline, "serve never answered /healthz"
            time.sleep(0.3)

    def request(self, path: str, body: dict | None = None, **params):
        query = f"?{urllib.parse.urlencode(params)}" if params else ""
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}{query}", data, {"content-type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw, kind = response.read(), response.headers.get("content-type") or ""
                return response.status, (json.loads(raw) if kind.startswith("application/json") else raw)
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode(errors="replace")

    def status(self, path: str) -> int:
        try:
            return self.request(path)[0]
        except OSError:
            return 0

    def get(self, path: str, **params):
        status, body = self.request(path, **params)
        assert status == 200, f"GET {path} {params}: {status} {str(body)[:500]}"
        return body

    def run(self, **body) -> str:
        status, found = self.request(f"{BASE}/runs", body)
        assert status in (200, 201), f"POST runs {body}: {status} {found}"
        return found["id"]

    def settled(self, run: str, timeout: float = 180) -> dict:
        deadline = time.monotonic() + timeout
        while (detail := self.get(f"{BASE}/runs/{run}"))["request"]["status"] not in TERMINAL:
            assert time.monotonic() < deadline, f"run {run} still {detail['request']['status']}"
            time.sleep(0.5)
        return detail

    def stop(self) -> None:
        self.process.send_signal(signal.SIGTERM)
        assert self.process.wait(60) == 0, self.text()

    def text(self) -> str:
        return self.log.read_text()


def every_route(serve: Serve) -> int:
    """GET every route the console reads, with real names: each must answer 200."""

    manifest = serve.get(f"{BASE}/manifest")
    assets, outputs = sorted(manifest["assets"]), sorted(manifest["outputs"])
    count = 0

    def get(path, **params):
        nonlocal count
        count += 1
        return serve.get(path, **params)

    for path in ("/healthz", "/api/diagnostics", "/", f"{BASE}/assets", f"{BASE}/assets:status"):
        get(path)
    for path in ("runs:facets", "runs:histogram", "tasks", "stats", "repairs", "cleanups"):
        get(f"{BASE}/{path}")
    for path in ("sensors", "automations", "executors", "workers"):
        get(f"{BASE}/{path}")
    for sensor in [s["name"] for s in get(f"{BASE}/sensors")["sensors"]]:
        get(f"{BASE}/sensors/{sensor}/ticks")
    for name in assets:
        get(f"{BASE}/assets/{name}")
        get(f"{BASE}/assets/{name}/inputs")
        get(f"{BASE}/assets/{name}/history")
        partition = ""
        if manifest["assets"][name].get("partitions"):  # the console asks only of partitioned ones
            built = [
                p for p in get(f"{BASE}/partitions/{name}")["partitions"] if p["status"] in ("materialized", "stale")
            ]
            if not built:  # nothing to read per partition: a page shows it missing
                continue
            partition = built[0]["partition"]
        keyed = [o for o in manifest["assets"][name]["outputs"] if o.get("key") is not None]
        if keyed:
            get(f"{BASE}/assets/{name}/stale-keys", partition=partition)
        if any(i.get("each") for i in manifest["assets"][name]["inputs"].values()):  # per-key
            get(f"{BASE}/assets/{name}/outcomes", partition=partition)
            get(f"{BASE}/assets/{name}/outcomes/history", partition=partition)
        for output in keyed:
            keys = get(f"{BASE}/outputs/{output['name']}/keys", partition=partition)["keys"]
            if keys and manifest["assets"][name]["inputs"]:
                get(f"{BASE}/assets/{name}/explain", key=next(iter(keys)), partition=partition)
    for name in outputs:
        heads = get(f"{BASE}/outputs/{name}/heads")
        get(f"{BASE}/outputs/{name}/merges")
        for head in _heads(heads)[:1]:
            get(f"{BASE}/outputs/{name}/lineage", partition=head.get("partition") or "")
    runs = get(f"{BASE}/runs", limit=200)["runs"]
    for found in runs:
        detail = get(f"{BASE}/runs/{found['id']}")
        get(f"{BASE}/runs/{found['id']}/events")
        for attempt in [a for attempts in detail["attempts"].values() for a in attempts]:
            for part in ("logs", "spec"):
                get(f"{BASE}/runs/{found['id']}/attempts/{attempt['id']}/{part}")
            if attempt["outcome"] == "succeeded":
                get(f"{BASE}/runs/{found['id']}/attempts/{attempt['id']}/result")
    return count


def _heads(heads) -> list[dict]:
    if isinstance(heads, dict):
        heads = heads.get("heads") or []
    return [h for h in heads if isinstance(h, dict)]


def clean(text: str) -> None:
    for marker in ("Traceback", "flush failed"):
        assert marker not in text, text[max(0, text.find(marker) - 2000) : text.find(marker) + 3000]


def test_the_demo_serves_every_page_through_flushes_and_restarts(tmp_path):
    serve = Serve(tmp_path, "first")
    try:
        runs = [
            serve.run(targets=["file_index"], upstream=True, partitions="all"),  # keyed incremental
            serve.run(targets=["file_checks"], partitions="all"),  # per-key
            serve.run(
                targets=["site_status", "site_digest", "fleet_index"], partitions="all"
            ),  # whole inputs
        ]
        for run in runs:
            assert serve.settled(run)["request"]["status"] == "succeeded"
        later = [
            serve.run(targets=["file_index"], partitions="all", mode="full"),  # rewrites: a cleanup
            serve.run(targets=["file_index"], partitions="all", keys={"site_files": "all"}),
        ]
        for run in later:
            assert serve.settled(run)["request"]["status"] == "succeeded"
        deadline = time.monotonic() + 180
        while not [
            t for t in serve.get(f"{BASE}/tasks", asset="@cleanup")["tasks"] if t["status"] == "succeeded"
        ]:
            assert time.monotonic() < deadline, "no cleanup ran"
            time.sleep(1)
        history = tmp_path / "state" / "default" / "history" / "attempts"
        while not list(history.glob("*.parquet")):  # a history flush: up to a minute after the first row
            assert time.monotonic() < deadline + 120, "the history never flushed"
            time.sleep(1)
        assert every_route(serve) > 50
    finally:
        serve.stop()
    clean(serve.text())

    again = Serve(tmp_path, "restarted")  # the buffered history restored from the checkpoint
    try:
        assert every_route(again) > 50
    finally:
        again.stop()
    clean(again.text())
