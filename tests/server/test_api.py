"""Phase 5 — the API (§5, §8-§10). httpx over ASGI against a real engine
backed by file:// objects; auth, validation, and every endpoint."""

import asyncio

import httpx
import pytest
from cursus.executors import Pool
from cursus.sdk import (
    Automation,
    Every,
    Incremental,
    OnChange,
    Output,
    PartitionSet,
    Project,
    Retry,
    Source,
    StaticPartitions,
    asset,
)
from cursus_server.api import create_app
from cursus_server.engine import Engine
from cursus_server.placements.inline import InlinePlacement
from cursus_server.state import State
from cursus_server.storage import SlateState


def build_project():
    feed_keys = {"rows": [{"k": "a", "v": 1}, {"k": "b", "v": 2}]}

    @asset(outputs=Output("feed", key="k"))
    def feed():
        return feed_keys["rows"]

    @asset(inputs={"feed": Incremental()}, automations=Automation(trigger=OnChange("feed")))
    def total(feed: list):
        return {"n": sum(v["v"] for v in feed)}

    @asset(partitions={"days": StaticPartitions(["2026-09-18", "2026-09-19"])})
    def daily(ctx):
        return {"day": ctx.partition}

    sites = PartitionSet("sites")

    @asset(partitions={"site": sites})
    def by_site(ctx):
        return {"site": ctx.partition}

    @asset(partitions={"days": StaticPartitions(["2026-09-18", "2026-09-19"])}, retries=Retry(n=0))
    def flaky(ctx):
        if ctx.config.get("fail"):
            raise RuntimeError("boom")
        return {"day": ctx.partition}

    @asset(executor=Pool("gpu")(cpu=1), automations=Every(3600))
    def trained():
        return {"weights": 1}

    upload = Source("uploads", key="id")
    return Project(
        assets=[feed, total, daily, by_site, flaky, trained],
        sources=[upload, sites],
        name="example",
    )


@pytest.fixture
async def engine(tmp_path):
    project = build_project()
    slate = await SlateState.open(tmp_path.as_uri(), "test")
    state = State(slate)
    runtime = Engine(
        state,
        project.manifest,
        placements={"Local": lambda e, o, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.05,
    )
    await runtime.initialize()
    yield runtime
    await state.close()


@pytest.fixture
async def client(engine):
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
def base(engine):
    return f"/api/projects/{engine.manifest['name']}"


async def test_health_and_diagnostics(client, base):
    """Health reports backend status; diagnostics reports state locations."""

    assert (await client.get("/healthz")).json()["status"] == "ok"
    diag = (await client.get("/api/diagnostics")).json()
    assert diag["backend"] == "slatedb" and diag["project"] == "example"
    assert diag["state"].startswith("file://")


async def test_auth_rejected_and_accepted(engine):
    """With a token configured every /api route requires the bearer header."""

    app = create_app(engine=engine, token="s3cret")
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        p = engine.manifest["name"]
        assert (await client.get(f"/api/projects/{p}/runs")).status_code == 401
        assert (
            await client.get(f"/api/projects/{p}/runs", headers={"Authorization": "Bearer wrong"})
        ).status_code == 401
        ok = await client.get(f"/api/projects/{p}/runs", headers={"Authorization": "Bearer s3cret"})
        assert ok.status_code == 200
        # The console shell is unauthenticated; the API is not.
        assert (await client.get("/healthz")).status_code == 200


async def test_project_mismatch_is_404(client):
    assert (await client.get("/api/projects/other/runs")).status_code == 404


async def test_manifest_assets_and_detail(client, base):
    """The catalog carries manifest shape plus per-output heads (§2)."""

    manifest = (await client.get(f"{base}/manifest")).json()
    assert manifest["name"] == "example"
    assets = (await client.get(f"{base}/assets")).json()["assets"]
    assert {a["name"] for a in assets} == {
        "feed",
        "total",
        "daily",
        "by_site",
        "flaky",
        "trained",
    }

    detail = (await client.get(f"{base}/assets/daily")).json()
    assert detail["asset"]["partitions"]["dims"]["days"]["kind"] == "static"
    assert set(detail["current_keys"][0]) == {"2026-09-18", "2026-09-19"}
    missing = await client.get(f"{base}/assets/nope")
    assert missing.status_code == 404


async def test_run_submit_list_detail_cancel(client, base, engine):
    """POST runs submits; runs list; detail shows tasks+attempts; cancel works (§8)."""

    response = await client.post(f"{base}/runs", json={"targets": ["daily"], "partitions": "all"})
    assert response.status_code == 201
    run = response.json()
    detail = await engine.run_until(run["id"])
    assert detail["request"]["status"] == "succeeded"

    runs = (await client.get(f"{base}/runs")).json()["runs"]
    assert run["id"] in {r["id"] for r in runs}
    shown = (await client.get(f"{base}/runs/{run['id']}")).json()
    assert {t["status"] for t in shown["tasks"]} == {"succeeded"}

    again = await client.post(f"{base}/runs/{run['id']}/cancel")
    assert again.status_code == 200
    assert (await client.get(f"{base}/runs/nope")).status_code == 404
    bad = await client.post(f"{base}/runs", json={"targets": []})
    assert bad.status_code == 422


async def test_run_validation_conflict(client, base):
    """Unknown targets and bad partition selectors are 400s."""

    unknown = await client.post(f"{base}/runs", json={"targets": ["ghost"]})
    assert unknown.status_code == 400
    bad_keys = await client.post(f"{base}/runs", json={"targets": ["total"], "keys": {"strangers": "full"}})
    assert bad_keys.status_code == 400


async def test_heads_keys_and_partitions(client, base, engine):
    """Heads expose ref/version/key_count/complete; keys pages the folded
    delta map; partitions report current/missing/retired status (§2, §7)."""

    await engine.run_until((await engine.submit(["feed"]))["id"])
    await engine.run_until((await engine.submit(["daily"], partitions=["2026-09-18"]))["id"])

    heads = (await client.get(f"{base}/outputs/feed/heads")).json()["heads"]
    assert heads[0]["version"] and heads[0]["key_count"] == 2 and heads[0]["complete"]

    keys = (await client.get(f"{base}/outputs/feed/keys")).json()
    assert keys["total"] == 2 and set(keys["keys"]) == {"a", "b"}
    paged = (await client.get(f"{base}/outputs/feed/keys?limit=1")).json()
    assert len(paged["keys"]) == 1

    parts = (await client.get(f"{base}/partitions/daily")).json()["partitions"]
    by_scope = {p["scope"]: p["status"] for p in parts}
    assert by_scope == {"2026-09-18": "complete", "2026-09-19": "missing"}
    assert (await client.get(f"{base}/outputs/ghost/heads")).status_code == 404
    assert (await client.get(f"{base}/partitions/feed")).status_code == 400


async def test_attempt_logs_spec_result(client, base, engine):
    """Attempt artifacts are readable through the API (§9)."""

    run = await engine.submit(["feed"])
    detail = await engine.run_until(run["id"])
    task = detail["tasks"][0]
    attempt = detail["attempts"][task["id"]][-1]
    attempt_id = f"{task['id']}/{attempt['generation']}"

    spec = (await client.get(f"{base}/attempts/{attempt_id}/spec")).json()
    assert spec["asset"] == "feed"
    result = (await client.get(f"{base}/attempts/{attempt_id}/result")).json()
    assert "outputs" in result
    logs = await client.get(f"{base}/attempts/{attempt_id}/logs")
    assert logs.status_code == 200
    assert (await client.get(f"{base}/attempts/nope/spec")).status_code == 404


async def test_automations_enable_disable_run_now(client, base, engine):
    """Automations list, toggle, and run-now (§9)."""

    autos = (await client.get(f"{base}/automations")).json()["automations"]
    assert any("feed" in a["name"] or a["trigger"]["kind"] == "onchange" for a in autos)

    name = autos[0]["name"]
    off = (await client.post(f"{base}/automations/{name}/disable")).json()
    assert off["enabled"] is False
    on = (await client.post(f"{base}/automations/{name}/enable")).json()
    assert on["enabled"] is True
    missing = await client.post(f"{base}/automations/ghost/disable")
    assert missing.status_code == 404

    hourly = next(a for a in autos if a["trigger"]["kind"] == "every")
    fired = await client.post(f"{base}/automations/{hourly['name']}/run-now")
    assert fired.status_code == 202
    # `trained` is pool-placed: the run queues until a worker claims it, so
    # assert submission rather than waiting on completion.
    run_id = fired.json()["last_run"]
    detail = (await client.get(f"{base}/runs/{run_id}")).json()
    assert detail["request"]["status"] in {"queued", "running"}


async def test_source_commit_endpoints(client, base, engine):
    """POST sources/{name}/commit advances the external output (§5)."""

    committed = await client.post(f"{base}/sources/uploads/commit", json={"keys": {"u-1": "v1"}})
    assert committed.status_code == 200
    assert committed.json()["ref"]["version"]

    patched = await client.post(
        f"{base}/sources/uploads/commit",
        json={"upsert": {"u-2": "v1"}, "remove": ["u-1"]},
    )
    assert patched.status_code == 200
    keys = (await client.get(f"{base}/outputs/uploads/keys")).json()["keys"]
    assert set(keys) == {"u-2"}
    missing = await client.post(f"{base}/sources/ghost/commit", json={"version": "1"})
    assert missing.status_code == 404


async def test_environments_and_workers(client, base):
    envs = (await client.get(f"{base}/environments")).json()["environments"]
    kinds = {e["kind"] for e in envs}
    assert "Local" in kinds and "Pool" in kinds
    assert (await client.get(f"{base}/workers")).json() == {"workers": []}


async def test_worker_pull_path(client, base, engine):
    """§10: register → claim → renew → complete; claim-fit respects capacity."""

    registered = await client.post(
        "/api/workers/register",
        json={"pools": ["gpu"], "capacity": {"cpu": 8, "memory": None, "gpu": 1}},
    )
    assert registered.status_code == 201
    worker = registered.json()["worker"]

    empty = await client.post("/api/tasks/claim", json={"worker": worker, "capacity": {"cpu": 8, "gpu": 1}})
    assert empty.status_code == 204

    await engine.submit(["trained"])
    await engine.tick()
    for _ in range(200):  # staging happens inside the dispatched attempt task
        async with engine.state.transaction() as tx:
            staged = await tx.pool_tasks()
        if staged:
            break
        await asyncio.sleep(0.02)
    assert staged

    claimed = await client.post("/api/tasks/claim", json={"worker": worker, "capacity": {"cpu": 8, "gpu": 1}})
    assert claimed.status_code == 200
    body = claimed.json()
    task_id = body["task"]
    assert body["stage"]["attempt"] == task_id and body["stage"]["objects"]

    renewed = await client.post(f"/api/tasks/{task_id}/renew", json={"worker": worker})
    assert renewed.status_code == 200

    stranger = await client.post(f"/api/tasks/{task_id}/complete", json={"worker": "other"})
    assert stranger.status_code == 409
    done = await client.post(f"/api/tasks/{task_id}/complete", json={"worker": worker})
    assert done.status_code == 200

    assert (await client.post("/api/tasks/claim", json={"worker": "ghost"})).status_code == 404


async def test_console_shell_served(client):
    """The console shell serves at / and client-side routes fall through."""

    index = await client.get("/")
    assert index.status_code == 200 and "text/html" in index.headers["content-type"]
    route = await client.get("/runs/abc")
    assert route.status_code == 200


async def test_partitions_read_scope_records_not_task_history(client, base, engine, monkeypatch):
    """§8: the partitions endpoint answers complete, missing, running,
    failed and retired from heads + scope records + the pending index —
    and never scans task/."""
    from cursus_server.storage import Transaction

    scanned = []
    original_scan = Transaction.scan

    async def spy(self, prefix, limit=None, after=None):
        scanned.append(prefix)
        return await original_scan(self, prefix, limit, after=after)

    # complete (a succeeded scope) and missing (a key never run)
    await engine.run_until((await engine.submit(["daily"], partitions=["2026-09-18"]))["id"])
    # failed: a scope whose task finished failed
    detail = await engine.run_until(
        (await engine.submit(["flaky"], partitions=["2026-09-18"], config={"fail": True}))["id"]
    )
    assert detail["request"]["status"] == "failed"
    # retired: commit sites a+b, complete a, then retract a
    await engine.commit_source("sites", upsert=["a", "b"])
    await engine.run_until((await engine.submit(["by_site"], partitions=["a"]))["id"])
    await engine.commit_source("sites", remove=["a"])
    # running: submitted and pending but never dispatched — submit after the
    # last run_until, which would dispatch everything queued
    await engine.submit(["daily"], partitions=["2026-09-19"], mode="full")
    await engine.submit(["by_site"], partitions=["b"])

    monkeypatch.setattr(Transaction, "scan", spy)
    daily = (await client.get(f"{base}/partitions/daily")).json()["partitions"]
    flaky = (await client.get(f"{base}/partitions/flaky")).json()["partitions"]
    by_site = (await client.get(f"{base}/partitions/by_site")).json()["partitions"]
    monkeypatch.undo()

    assert {p["scope"]: p["status"] for p in daily} == {
        "2026-09-18": "complete",
        "2026-09-19": "running",
    }
    assert {p["scope"]: p["status"] for p in flaky} == {
        "2026-09-18": "failed",
        "2026-09-19": "missing",
    }
    flaky_done = next(p for p in flaky if p["scope"] == "2026-09-18")
    assert flaky_done["last_outcome"] == "failed" and flaky_done["last_attempt"]
    complete = next(p for p in daily if p["scope"] == "2026-09-18")
    assert complete["last_outcome"] == "succeeded" and complete["last_attempt"]
    assert {p["scope"]: p["status"] for p in by_site} == {"a": "retired", "b": "running"}
    assert not [p for p in scanned if p.startswith("task/")]


async def test_failed_scope_reports_complete_after_success(client, base, engine):
    """§8: a scope that failed and later succeeded reports complete — the
    scope record is the last outcome, not a task-history scan."""

    await engine.run_until(
        (await engine.submit(["flaky"], partitions=["2026-09-19"], config={"fail": True}))["id"]
    )
    parts = (await client.get(f"{base}/partitions/flaky")).json()["partitions"]
    assert {p["scope"]: p["status"] for p in parts}["2026-09-19"] == "failed"

    await engine.run_until((await engine.submit(["flaky"], partitions=["2026-09-19"]))["id"])
    parts = (await client.get(f"{base}/partitions/flaky")).json()["partitions"]
    done = next(p for p in parts if p["scope"] == "2026-09-19")
    assert done["status"] == "complete" and done["last_outcome"] == "succeeded"
