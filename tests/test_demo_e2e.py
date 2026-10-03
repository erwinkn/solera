"""The demo project end to end: a real server on a temp file://
store, the external `uploads` set committed through the API, and a pool
worker claiming `ingest` work. No external services — `SiteRegistry`,
`FeedClient`, `UploadReader`, and `Mailer` are in-process fakes."""

import socket
import threading
import time

import httpx
import pytest

PROJECT = "solera_server.demo:project"
OUTPUTS = [
    "sites",
    "site_events",
    "site_files",
    "file_index",
    "site_digest",
    "fleet_index",
    "site_status",
    "upload_record",
]


@pytest.fixture
def demo(tmp_path, monkeypatch):
    """A real uvicorn server on the demo project + its base URL."""

    import uvicorn
    from solera_server.api import create_app

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SOLERA_PROJECT", PROJECT)
    app = create_app(
        state_url=(tmp_path / "state").as_uri(),
        namespace="e2e",
        project=PROJECT,
        insecure=True,
    )
    # The server is handed its socket, bound here: no other process can take
    # the port between choosing it and listening on it.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 120
    while not server.started:  # the engine initialized, and the socket listening
        # A startup that failed ends the thread, its error logged above.
        assert thread.is_alive(), "the server exited while starting"
        assert time.monotonic() < deadline, "the server is still starting after 120 s"
        time.sleep(0.01)
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    server.should_exit = True
    thread.join(timeout=10)


def wait(predicate, timeout=120, interval=0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@pytest.fixture
def pool_worker(demo):
    """`solera worker pool ingest`, as the README runs it: a process of its
    own, which forks a child per attempt."""

    import os
    import subprocess
    import sys

    env = {**os.environ, "SOLERA_PROJECT": PROJECT}
    worker = subprocess.Popen(
        [sys.executable, "-m", "solera_worker", "pool", "--pool", "ingest", "--server", demo], env=env
    )
    yield worker
    worker.terminate()
    worker.wait(10)


def test_demo_end_to_end(demo, pool_worker):
    client = httpx.Client(base_url=demo, timeout=15)
    base = "/api/projects/demo"

    def heads(output):
        return client.get(f"{base}/outputs/{output}/heads").json()["heads"]

    def run_status(run_id):
        return client.get(f"{base}/runs/{run_id}").json()["request"]["status"]

    def run_done(run_id):
        return run_status(run_id) in {"succeeded", "failed", "canceled"}

    def submit(targets, partitions="latest", upstream=True, **extra):
        response = client.post(
            f"{base}/runs",
            json={"targets": targets, "partitions": partitions, "upstream": upstream, **extra},
        )
        assert response.status_code in (200, 201), response.text
        return response.json()["id"]

    # Deterministic ordering: automations stay off so only the runs below
    # move state (the cron/every ticks would race the assertions).
    for auto in client.get(f"{base}/automations").json()["automations"]:
        client.post(f"{base}/automations/{auto['name']}/disable")

    # The external `uploads` partition set is fed from outside (§5) — this
    # commit is what the README does with `solera commit uploads`.
    committed = client.post(f"{base}/sources/uploads/commit", json={"keys": {"u-1": "v1", "u-2": "v1"}})
    assert committed.status_code == 200 and committed.json()["ref"]["generation"]

    # `manual_ingest` is placed on Pool("ingest"): only an external worker
    # can complete it — `pool_worker`, `solera worker pool ingest` in the README.

    # The partition set grows one site per run and caps at four. Saturate it
    # first so every downstream run plans the same site scopes.
    last = set()
    for _ in range(5):
        response = client.get(f"{base}/outputs/sites/keys")
        keys = set(response.json().get("keys") or {}) if response.status_code == 200 else set()
        if keys and keys == last:
            break
        last = keys
        sites_run = submit(["sites"], upstream=False)
        assert wait(lambda r=sites_run: run_done(r)) and run_status(sites_run) == "succeeded"
    sites = last
    assert len(sites) == 4

    # Submit the whole graph. upstream=True pulls producers (and the sites
    # partition set) into each run; the rollup fans in over all sites.
    submitted = [
        submit(["site_digest"]),  # site × day — needs sites + site_feed
        submit(["fleet_index"]),  # AllPartitions rollup over file_index
        submit(["site_status"], partitions="all"),
        submit(["manual_ingest"], partitions="all"),
        submit(["weekly_digest"]),  # job: no outputs, just a clean run
    ]
    assert wait(lambda: all(run_done(r) for r in submitted))
    assert all(run_status(r) == "succeeded" for r in submitted)

    # Every output has a complete head for every planned scope.
    def all_complete():
        all_heads = {o: heads(o) for o in OUTPUTS}
        return all(hs and all(h["complete"] for h in hs) for hs in all_heads.values())

    assert wait(all_complete), {o: heads(o) for o in OUTPUTS}
    for h in heads("upload_record"):
        assert h["complete"], "pool task staged, claimed, and completed"

    # The pool worker registered itself (§10 pull protocol).
    workers = client.get(f"{base}/workers").json()["workers"]
    assert any("ingest" in (w.get("pools") or []) for w in workers)

    # -- the no-change corollary (§6) --------------------------------------
    # FeedClient emits one batch per site per five-second tick; a poll whose
    # run config stretches the tick far past the stored cursor ticks is a
    # guaranteed no-change pass — the producer writes nothing, and the head
    # keeps its generation. (`ref.generation` is the version of the
    # committed data; `version` on the head record is the declared asset
    # version and never changes.)
    #
    # The concurrent runs above commit site_files deltas that can land after
    # a sibling run's last file_index drain — real work the watermark must
    # not skip. Drain the log first so every watermark sits at head.
    drain = submit(["file_index"], partitions="all", upstream=False)
    assert wait(lambda: run_done(drain)) and run_status(drain) == "succeeded"
    site_files = {h["partition"]: h["ref"]["generation"] for h in heads("site_files")}
    file_index_heads = {h["partition"]: h["commit"] for h in heads("file_index")}
    poll_run = submit(
        ["site_feed"],
        partitions="all",
        upstream=False,
        config={"feed_tick_seconds": 3600},
    )
    assert wait(lambda: run_done(poll_run)) and run_status(poll_run) == "succeeded"
    assert {h["partition"]: h["ref"]["generation"] for h in heads("site_files")} == site_files

    # The Incremental consumer over unchanged upstream state skips every
    # scope — the delta log holds nothing past its watermark.
    again = submit(["file_index"], partitions=sorted(site_files), upstream=False)
    assert wait(lambda: run_done(again)) and run_status(again) == "succeeded"
    detail = client.get(f"{base}/runs/{again}").json()
    status = {f"{t['asset']}:{t['partition']}": t["status"] for t in detail["tasks"]}
    for site in site_files:
        assert status.get(f"file_index:{site}") == "skipped", detail["tasks"]
    assert {
        h["partition"]: h["commit"] for h in heads("file_index") if h["partition"] in file_index_heads
    } == file_index_heads

    # -- the changed-keys pass (§6, Incremental) -----------------------------
    # Once the tick advances every file's revision bumps; the consumer must
    # process exactly the changed keys (and any deletions), in batches of
    # page_size=2 — four files per site means `more` continuation.
    def site_file_keys():
        out = {}
        for h in heads("site_files"):
            out[h["partition"]] = set(
                client.get(f"{base}/outputs/site_files/keys", params={"partition": h["partition"]}).json()[
                    "keys"
                ]
            )
        return out

    prior_keys = site_file_keys()
    prior_tick = int(time.time() // 5)
    wait(lambda: int(time.time() // 5) > prior_tick, timeout=10, interval=0.05)
    delta = submit(["file_index"], partitions="all")
    assert wait(lambda: run_done(delta))
    assert run_status(delta) == "succeeded"
    detail = client.get(f"{base}/runs/{delta}").json()
    current_keys = site_file_keys()
    delivered = {}
    for task in detail["tasks"]:
        if task["asset"] != "file_index":
            continue
        assert task["attempt_count"] >= 2, "page_size=2 over 4 files must continue with more"
        for attempt in detail["attempts"][task["id"]]:
            if attempt["status"] != "succeeded":
                continue
            result = client.get(f"{base}/runs/{delta}/attempts/{attempt['id']}/result").json()
            page = result["delivered"]["site_files"]
            seen = delivered.setdefault(task["partition"], [set(), set()])
            seen[0] |= set(page["upserted"])
            seen[1] |= set(page["deleted"])
    assert delivered, "file_index should have run on the new tick"
    for partition, (upserted, deleted) in delivered.items():
        assert upserted <= current_keys[partition]
        assert deleted <= prior_keys.get(partition, set()) - current_keys[partition]
        assert upserted or deleted


@pytest.mark.postgres
def test_demo_postgres_migrations_and_ondeploy(tmp_path):
    """§8 gate: with DATABASE_URL the demo's relational outputs land in
    PostgresStore — each declares one migration, `solera migrate` applies
    them into the solera_migrations ledger, written heads carry the applied
    migration as `schema`, and the OnDeploy job fires once on boot.

    The server runs as a real `solera serve` subprocess so the module-level
    DATABASE flag in the demo project evaluates against the test DSN.
    """

    import os
    import shutil
    import subprocess

    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg

    solera = shutil.which("solera")
    assert solera, "the solera console script is not on PATH"

    # Clean slate: drop every table the demo owns plus the store ledgers.
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS ops CASCADE")
        for name in (
            "site_events",
            "site_files",
            "file_index",
            "demo_migrations",
            "solera_migrations",
        ):
            conn.execute(f'DROP TABLE IF EXISTS "{name}"')

    state_url = (tmp_path / "state").as_uri()
    env = {
        **os.environ,
        "DATABASE_URL": dsn,
        "SOLERA_STATE_URL": state_url,
        "SOLERA_NAMESPACE": "pg-e2e",
        "SOLERA_PROJECT": PROJECT,
    }

    # `solera migrate` applies every declared migration before anything runs.
    migrated = subprocess.run([solera, "migrate"], env=env, capture_output=True, text=True, timeout=60)
    assert migrated.returncode == 0, migrated.stderr
    with psycopg.connect(dsn) as conn:
        applied = set(conn.execute("SELECT output, name FROM solera_migrations").fetchall())
    expected = {
        o for o in ("site_events", "site_files", "file_index", "file_checks", "fleet_status", "site_status")
    }
    assert {o for o, name in applied if name == "baseline"} == expected
    with psycopg.connect(dsn) as conn:
        logged = {r[0] for r in conn.execute("SELECT output FROM demo_migrations").fetchall()}
    assert {"site_events", "site_files", "file_index"} <= logged

    # Boot the real server; the OnDeploy job fires once for the revision.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    proc = subprocess.Popen(
        [solera, "serve", "--insecure", "--port", str(port)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(400):
            try:
                if httpx.get(f"{base_url}/healthz", timeout=1).status_code == 200:
                    break
            except Exception:
                time.sleep(0.05)
        else:
            raise RuntimeError("server did not start")
        client = httpx.Client(base_url=base_url, timeout=15)
        base = "/api/projects/demo"

        def deploy_fired():
            autos = client.get(f"{base}/automations").json()["automations"]
            auto = next((a for a in autos if a.get("targets") == ["deploy_notice"]), None)
            return bool(auto and auto.get("last_deploy"))

        assert wait(deploy_fired, timeout=30), "the OnDeploy job did not fire on boot"

        # Populate the sites partition set first so downstream runs plan
        # real scopes, then drive file_index (pulls sites + site_feed via
        # upstream). Each head's ref carries the last applied migration as
        # schema (§4).
        def run_done(run_id):
            status = client.get(f"{base}/runs/{run_id}").json()["request"]["status"]
            return status in {"succeeded", "failed", "canceled"}

        def submit(targets, partitions="latest", upstream=True):
            response = client.post(
                f"{base}/runs",
                json={"targets": targets, "partitions": partitions, "upstream": upstream},
            )
            assert response.status_code in (200, 201), response.text
            return response.json()["id"]

        sites_run = submit(["sites"], upstream=False)
        assert wait(lambda: run_done(sites_run))
        assert client.get(f"{base}/runs/{sites_run}").json()["request"]["status"] == "succeeded"

        run_id = submit(["file_index"], partitions="all")
        assert wait(lambda: run_done(run_id))
        status = client.get(f"{base}/runs/{run_id}").json()["request"]["status"]
        assert status == "succeeded"
        for output in ("site_events", "site_files", "file_index"):
            heads = client.get(f"{base}/outputs/{output}/heads").json()["heads"]
            assert heads, output
            assert all(h["ref"]["handle"].get("schema") == "baseline" for h in heads)
    finally:
        proc.terminate()
        proc.wait(timeout=15)
