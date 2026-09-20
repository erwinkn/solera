"""Phase 7 — the demo project end to end: a real server on a temp file://
store, the external `uploads` set committed through the API, and a pool
worker claiming `ingest` work. No external services — `SiteRegistry`,
`FeedClient`, `UploadReader`, and `Mailer` are in-process fakes."""

import asyncio
import socket
import threading
import time

import httpx
import pytest

PROJECT = "dorc.demo:project"
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
    from dorc.api import create_app

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DORC_PROJECT", PROJECT)
    app = create_app(
        state_url=(tmp_path / "state").as_uri(),
        namespace="e2e",
        project=PROJECT,
        insecure=True,
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(400):
        try:
            if httpx.get(f"{base}/healthz", timeout=1).status_code == 200:
                break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("server did not start")
    yield base
    server.should_exit = True
    thread.join(timeout=10)


def wait(predicate, timeout=120, interval=0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_demo_end_to_end(demo):
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
    # commit is what the README does with `dorc commit uploads`.
    committed = client.post(f"{base}/sources/uploads/commit", json={"keys": {"u-1": "v1", "u-2": "v1"}})
    assert committed.status_code == 200 and committed.json()["ref"]["version"]

    # `manual_ingest` is placed on Pool("ingest"): only an external worker
    # can complete it — `dorc worker pool ingest` in the README.
    from dorc_worker.worker import run_pool

    threading.Thread(target=lambda: asyncio.run(run_pool("ingest", demo)), daemon=True).start()

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
    # guaranteed no-change pass — the delta is empty, the recommit is
    # byte-identical, `changed` comes back empty. (`ref.version` is the
    # committed data version; `version` on the head record is the declared
    # asset version and never changes.)
    site_files = {h["scope"]: h["ref"]["version"] for h in heads("site_files")}
    file_index_heads = {h["scope"]: h["commit"] for h in heads("file_index")}
    poll_run = submit(
        ["site_feed"],
        partitions="all",
        upstream=False,
        config={"feed_tick_seconds": 3600},
    )
    assert wait(lambda: run_done(poll_run)) and run_status(poll_run) == "succeeded"
    assert {h["scope"]: h["ref"]["version"] for h in heads("site_files")} == site_files

    # The ByKey consumer over unchanged upstream state skips every scope —
    # it processed only the (empty) change set.
    again = submit(["file_index"], partitions=sorted(site_files), upstream=False)
    assert wait(lambda: run_done(again)) and run_status(again) == "succeeded"
    detail = client.get(f"{base}/runs/{again}").json()
    status = {f"{t['asset']}:{t['scope']}": t["status"] for t in detail["tasks"]}
    for site in site_files:
        assert status.get(f"file_index:{site}") == "skipped", detail["tasks"]
    assert {
        h["scope"]: h["commit"] for h in heads("file_index") if h["scope"] in file_index_heads
    } == file_index_heads

    # -- the changed-keys pass (§6, ByKey) ----------------------------------
    # Once the tick advances every file's revision bumps; the consumer must
    # process exactly the changed keys (and any deletions), in batches of
    # batch_size=2 — four files per site means `more` continuation.
    def site_file_keys():
        out = {}
        for h in heads("site_files"):
            out[h["scope"]] = set(
                client.get(f"{base}/outputs/site_files/keys", params={"scope": h["scope"]}).json()["keys"]
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
    spec_changes = {}
    for task in detail["tasks"]:
        if task["asset"] != "file_index":
            continue
        assert task["attempt_count"] >= 2, "batch_size=2 over 4 files must continue with more"
        attempt_id = detail["attempts"][task["id"]][-1]["id"]
        spec = client.get(f"{base}/attempts/{attempt_id}/spec").json()
        spec_changes[task["scope"]] = spec["inputs"]["site_files"]["changes"]
    assert spec_changes, "file_index should have run on the new tick"
    for scope, changes in spec_changes.items():
        upserted, deleted = set(changes["upserted"]), set(changes["deleted"])
        assert upserted <= current_keys[scope]
        assert deleted <= prior_keys.get(scope, set()) - current_keys[scope]
        assert upserted or deleted
