"""Phase 3 — the engine (architecture §2, §5–§10). Small inline projects run
through InlinePlacement against the real file:// object store."""

import asyncio
import json

import pytest
from cursus.executors import Environment
from cursus.sdk import (
    AllPartitions,
    Automation,
    Cron,
    Every,
    In,
    Incremental,
    JsonRef,
    OnChange,
    OnDeploy,
    Output,
    PartitionSet,
    Project,
    Ref,
    Result,
    Retry,
    StaticPartitions,
    asset,
    job,
)
from cursus.stores import JsonStore
from cursus_server.engine import Engine
from cursus_server.placements.inline import InlinePlacement
from cursus_server.state import State


class Fake(Environment):
    """A test-only placement kind registered through `executors=` (§10)."""

    kind = "Fake"


class FakePlacement:
    """Waits forever unless scripted; records cancels."""

    max_concurrent = None
    script = {}  # attempt -> {"wait": [...], "on_cancel": fn}

    def __init__(self, ctx):
        self.ctx = ctx

    async def launch(self, stage):
        self.script.setdefault(stage["attempt"], {})["launched"] = True
        return {"id": stage["attempt"]}

    async def wait(self, run, timeout):
        script = self.script.get(run["id"], {})
        waits = script.setdefault("waits", 0)
        script["waits"] = waits + 1
        if script.get("exit_after") is not None and waits > script["exit_after"]:
            return script.get("exit", {"code": 0, "reason": None, "meta": {}})
        if script.get("exit"):
            return script["exit"]
        await asyncio.sleep(min(timeout, 0.05))
        return None

    async def cancel(self, run):
        self.script.setdefault(run["id"], {})["canceled"] = True


def inline(project, **env_kw):
    return {"Local": lambda e, o, c: InlinePlacement(c, project)}


def fake(project, **env_kw):
    return {"Fake": lambda e, o, c: FakePlacement(c)}


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()


def head(state, output, scope=""):
    return state.model.heads.get((output, scope))


async def spec_of(state, output, scope=""):
    """The spec of the attempt behind a head: what it read (lineage)."""

    attempt = head(state, output, scope)["attempt"]
    return json.loads(await state.get_object(f"specs/{attempt}.json"))


def make_engine(state, project, placements=None, **kw):
    kw.setdefault("eval_interval", 0.05)
    kw.setdefault("clock", state.clock)
    return Engine(
        state,
        project.manifest,
        placements=placements or inline(project),
        **kw,
    )


async def drive(engine, run, timeout=30):
    detail = await engine.run_until(run["id"], timeout)
    return detail


def status_of(detail):
    return detail["request"]["status"]


def task_statuses(detail):
    return {t["asset"]: t["status"] for t in detail["tasks"]}


async def test_bare_return_and_commit(state):
    """§2: a single-output asset returns the bare value; the commit installs
    the head and records the ref."""

    @asset
    def numbers():
        return [{"n": 1}, {"n": 2}]

    project = Project(assets=[numbers])
    engine = make_engine(state, project)
    await engine.initialize()
    run = await engine.submit(["numbers"])
    detail = await drive(engine, run)
    assert status_of(detail) == "succeeded"
    installed = head(state, "numbers")
    assert installed["ref"]["output"] == "numbers" and installed["ref"]["version"]
    assert installed["complete"] is True
    assert installed["run"] == run["id"] and installed["attempt"]


async def test_result_cursor_and_omitted_output(state):
    """§2: Result(outputs, cursor) — an omitted output keeps its prior head;
    cursor persists and clears on a `full` run."""
    calls = {"n": 0}

    @asset(outputs=(Output("a"), Output("b")))
    def pair(ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            return Result(outputs={"a": [1], "b": [2]}, cursor="c1")
        if calls["n"] == 2:
            return Result(outputs={"a": [3]}, cursor="c2")
        return Result(outputs={"a": [4]})  # full run: no cursor set

    project = Project(assets=[pair])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["pair"]))
    await drive(engine, await engine.submit(["pair"]))
    assert head(state, "a")["ref"]["version"] != ""
    assert head(state, "b") is not None  # kept from the first commit
    assert state.model.cursors.get(("pair", "")) == "c2"
    await drive(engine, await engine.submit(["pair"], mode="full"))
    assert state.model.cursors.get(("pair", "")) is None  # full clears the cursor


async def test_omitted_output_without_head_fails(state):
    """§2/§8: an omitted output with no prior head is a commit error."""

    @asset(outputs=(Output("a"), Output("b")))
    def pair():
        return Result(outputs={"a": [1]})

    project = Project(assets=[pair])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["pair"]))
    assert status_of(detail) == "failed"


async def test_rename_and_meta_edges(state):
    """§5: a str edge renames the bound output; In(meta=) is recorded on the
    edge in the manifest."""
    seen = {}

    @asset(outputs=Output("raw", key="id"))
    def producer():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 2}]

    @asset(inputs={"feed": "raw"})
    def consumer(feed: list):
        seen["feed"] = feed
        return [{"n": len(feed)}]

    @asset(inputs={"feed": In("raw", meta={"owner": "t"})})
    def with_meta(feed: list):
        return []

    project = Project(assets=[producer, consumer, with_meta])
    engine = make_engine(state, project)
    await engine.initialize()
    assert engine.manifest["assets"]["with_meta"]["inputs"]["feed"]["meta"] == {"owner": "t"}
    detail = await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert seen["feed"] == [{"id": "a", "v": 1}, {"id": "b", "v": 2}]


async def test_incremental_filters_input_and_changes(state):
    """§5/§6: under Incremental the parameter arrives filtered to upserted
    keys and ctx.changes carries upserted + deleted."""
    seen = {}
    content = {"rows": [{"id": "a", "v": 1}, {"id": "b", "v": 1}, {"id": "c", "v": 1}]}

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["rows"]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen["rows"] = list(files)
        seen["upserted"] = list(ctx.changes["files"].upserted)
        seen["deleted"] = list(ctx.changes["files"].deleted)
        return [{"n": len(files)}]

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["upserted"] == ["a", "b", "c"]  # §6: first delivery upserts everything
    assert {r["id"] for r in seen["rows"]} == {"a", "b", "c"}

    # Second run, no changes → skipped, nothing loaded.
    seen.clear()
    detail = await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert task_statuses(detail)["consumer"] == "skipped"
    assert seen == {}

    # One changed revision reprocesses only that key (§6).
    content["rows"] = [{"id": "a", "v": 1}, {"id": "b", "v": 2}, {"id": "c", "v": 1}]
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["upserted"] == ["b"]
    assert [r["id"] for r in seen["rows"]] == ["b"]

    # A deletion arrives via ctx.changes (§5: what a selection cannot carry).
    seen.clear()
    content["rows"] = [{"id": "a", "v": 1}, {"id": "c", "v": 1}]
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["deleted"] == ["b"] and seen["upserted"] == []


async def test_config_change_reprocesses_everything(state):
    """§6/§2.2: run config is part of the interpretation fingerprint —
    changing it forces full=True on the edge and reprocesses every key."""
    seen = {}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.setdefault("batches", []).append([r["id"] for r in files])
        seen.setdefault("full", []).append(ctx.changes["files"].full)
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    await drive(engine, await engine.submit(["consumer"], upstream=True, config={"threshold": 2}))
    assert seen["batches"] == [["a"], ["a"]]
    assert seen["full"] == [True, True]  # first delivery + fingerprint reset


async def test_full_run_resets_watermark(state):
    """§2.2: a `full` run resets the edge watermark — the consumer re-reads
    the whole head (not a diff) and the watermark lands past the head batch."""
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.append((sorted(r["id"] for r in files), ctx.changes["files"].full))
        return [{"n": len(files)}]

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    first = state.model.watermarks[("consumer", "files", "")]
    assert first == {
        "batch": 1,
        "after": None,
        "full": False,
        "fingerprint": first["fingerprint"],
        "output": "files",
        "up": "",
    }
    detail = await drive(engine, await engine.submit(["consumer"], mode="full"))
    assert task_statuses(detail)["consumer"] == "succeeded"  # never skipped on full
    second = state.model.watermarks[("consumer", "files", "")]
    assert second == first  # back at head+1, nothing left mid-way
    # Both deliveries were full-head reads.
    assert seen == [(["a", "b"], True), (["a", "b"], True)]


async def test_version_bump_fails_then_full_recovers(state):
    """§6: a version mismatch between committed and declared fails an
    incremental attempt non-retryably; on_version_change='full' turns the
    next attempt of the scope into a full run."""
    count = {"n": 0}

    @asset(version="1")
    def versioned():
        count["n"] += 1
        return [count["n"]]

    project = Project(assets=[versioned])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["versioned"]))

    @asset(version="2")
    def versioned():  # noqa: F811 — same asset name, bumped version
        count["n"] += 1
        return [count["n"]]

    project2 = Project(assets=[versioned])
    engine2 = make_engine(state, project2)
    await engine2.initialize()
    detail = await drive(engine2, await engine2.submit(["versioned"]))
    assert status_of(detail) == "failed"
    task = detail["tasks"][0]
    assert "full run is required" in task["error"]

    @asset(version="2", on_version_change="full")
    def versioned():  # noqa: F811
        count["n"] += 1
        return [count["n"]]

    project3 = Project(assets=[versioned])
    engine3 = make_engine(state, project3)
    await engine3.initialize()
    detail = await drive(engine3, await engine3.submit(["versioned"]))
    assert status_of(detail) == "succeeded"


async def test_incremental_batching_and_more(state):
    """§6: work is batched by batch_size; `more` re-queues the task;
    scope_complete lands on the head only with the last batch."""
    batches = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i}", "v": 1} for i in range(5)]

    @asset(inputs={"files": Incremental(batch_size=2)})
    def consumer(ctx, files: list):
        batches.append([r["id"] for r in files])
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert batches == [["k0", "k1"], ["k2", "k3"], ["k4"]]
    assert head(state, "consumer")["complete"] is True
    task = [t for t in detail["tasks"] if t["asset"] == "consumer"][0]
    assert len(detail["attempts"][task["id"]]) == 3  # three batches, three attempts


async def test_run_keys_override(state):
    """§8: `keys=` explicit list is a one-off selection that never moves the
    watermark; 'full' drains the folded key map as a reset."""
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.append(sorted(r["id"] for r in files))
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    await drive(engine, await engine.submit(["consumer"], keys={"files": {"keys": ["b"]}}))
    assert seen[-1] == ["b"]
    await drive(engine, await engine.submit(["consumer"], keys={"files": "full"}))
    assert seen[-1] == ["a", "b"]


async def test_deps_are_pinned_but_unbound(state):
    """§5: deps are pinned into lineage and the fingerprint but bind no
    parameter."""
    called = []

    @asset
    def dim():
        return [{"x": 1}]

    @asset(deps=["dim"])
    def dependent():
        called.append(True)
        return []

    project = Project(assets=[dim, dependent])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["dependent"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert called == [True]
    assert (await spec_of(state, "dependent"))["inputs"]["dim"]["refs"][""]["output"] == "dim"


async def test_ref_annotated_input_receives_ref(state):
    """§5: a Ref-annotated input hands the producer the pinned ref, not a load."""
    seen = {}

    @asset
    def upstream():
        return [1, 2, 3]

    @asset(inputs={"upstream": "upstream"})
    def by_ref(upstream: JsonRef):
        seen["ref"] = upstream
        return [{"v": upstream.version[:8]}]

    project = Project(assets=[upstream, by_ref])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["by_ref"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert isinstance(seen["ref"], Ref) and seen["ref"].output == "upstream"


async def test_all_partitions_values(state):
    """§7: AllPartitions yields dict[key, value] over upstream-only dimensions,
    resolved to keys with complete heads at pin time."""

    @asset(outputs=PartitionSet("sites"))
    def sites():
        return ["east", "west"]

    @asset(partitions="sites")
    def per_site(ctx):
        return [{"site": ctx.partition}]

    @asset(inputs={"per_site": AllPartitions()})
    def rollup(per_site: dict[str, list]):
        return [{"n": len(per_site), "sites": sorted(per_site)}]

    project = Project(assets=[sites, per_site, rollup])
    engine = make_engine(state, project)
    await engine.initialize()
    # The key set must be committed before fan-out planning can see it (§7).
    await drive(engine, await engine.submit(["sites"]))
    detail = await drive(engine, await engine.submit(["rollup"], upstream=True))
    assert status_of(detail) == "succeeded"
    result = await state.get_object(head(state, "rollup")["ref"]["handle"]["object"])
    rows = json.loads(result)
    assert rows == [{"n": 2, "sites": ["east", "west"]}]


async def test_partition_selections(state):
    """§7/§8: 'latest', 'missing', 'all' and explicit lists select from the
    current key set."""
    ran = []

    @asset(partitions=StaticPartitions(["a", "b", "c"]))
    def stat(ctx):
        ran.append(ctx.partition)
        return [{"p": ctx.partition}]

    project = Project(assets=[stat])
    engine = make_engine(state, project)
    await engine.initialize()
    run = await engine.submit(["stat"], partitions=["a", "c"])
    await drive(engine, run)
    assert sorted(ran) == ["a", "c"]
    ran.clear()
    run = await engine.submit(["stat"], partitions="missing")
    await drive(engine, run)
    assert ran == ["b"]  # a and c have complete heads
    ran.clear()
    run = await engine.submit(["stat"], partitions="all")
    await drive(engine, run)
    assert sorted(ran) == ["a", "b", "c"]


async def test_external_partition_set_via_commit(state):
    """§5/§7: an external PartitionSet is patched through the commit API; new
    keys surface through partitions='missing'."""
    ran = []

    @asset(partitions="uploads")
    def per_upload(ctx):
        ran.append(ctx.partition)
        return [{"u": ctx.partition}]

    project = Project(assets=[per_upload], sources=[PartitionSet("uploads")])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.commit_source("uploads", upsert=["u-1", "u-2"])
    detail = await drive(engine, await engine.submit(["per_upload"], partitions="missing"))
    assert status_of(detail) == "succeeded"
    assert sorted(ran) == ["u-1", "u-2"]
    ran.clear()
    await engine.commit_source("uploads", upsert=["u-3"])
    detail = await drive(engine, await engine.submit(["per_upload"], partitions="missing"))
    assert ran == ["u-3"]
    # An identical map is not a change (§5).
    result = await engine.commit_source("uploads", keys={"u-1": "1", "u-2": "1", "u-3": "1"})
    assert result["changed"] is False


async def test_two_dimension_broadcast_and_collapse(state):
    """§7: a consumer-only dimension broadcasts; an upstream-only dimension
    collapses via AllPartitions."""
    seen = []

    @asset(outputs=PartitionSet("sites"))
    def sites():
        return ["s1", "s2"]

    @asset(partitions={"site": "sites"})
    def site_data(ctx):
        return [{"site": ctx.partition, "v": 1}]

    # broadcast: unpartitioned upstream read by a partitioned consumer
    @asset
    def config():
        return {"threshold": 5}

    @asset(partitions={"site": "sites"}, inputs={"cfg": "config"})
    def broadcast(ctx, cfg: list):
        seen.append((ctx.partition, cfg["threshold"]))
        return [{"site": ctx.partition}]

    # collapse: partitioned upstream read by an unpartitioned consumer
    @asset(inputs={"site_data": AllPartitions()})
    def collapsed(site_data: dict[str, list]):
        return [{"n": len(site_data)}]

    project = Project(assets=[sites, config, site_data, broadcast, collapsed])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["sites"]))  # commit the key set
    detail = await drive(engine, await engine.submit(["broadcast"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert sorted(seen) == [("s1", 5), ("s2", 5)]
    detail = await drive(engine, await engine.submit(["collapsed"], upstream=True))
    assert status_of(detail) == "succeeded"


async def test_retired_keys_leave_fanout(state):
    """§7: retired keys leave fan-out but their heads persist read-only."""
    members = {"keys": ["a", "b"]}

    @asset(outputs=PartitionSet("things"))
    def things():
        return members["keys"]

    @asset(partitions="things")
    def per_thing(ctx):
        return [{"t": ctx.partition}]

    project = Project(assets=[things, per_thing])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["things"]))
    await drive(engine, await engine.submit(["per_thing"], upstream=True, partitions="all"))
    members["keys"] = ["a"]  # 'b' retires
    await drive(engine, await engine.submit(["things"]))
    run = await engine.submit(["per_thing"], partitions="all")
    detail = await drive(engine, run)
    assert [t["scope"] for t in detail["tasks"]] == ["a"]
    assert head(state, "per_thing", "b") is not None  # head persists


async def test_upstream_false_never_replans(state):
    """§8: upstream=False plans targets only; inputs pin current heads and a
    producer is never re-run."""
    calls = {"n": 0}

    @asset(outputs=Output("feed", key="id"))
    def producer():
        calls["n"] += 1
        return [{"id": "a", "v": calls["n"]}]

    @asset(inputs={"feed": Incremental()})
    def consumer(feed: list):
        return [{"n": len(feed)}]

    project = Project(assets=[producer, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["producer"]))
    detail = await drive(engine, await engine.submit(["consumer"], upstream=False))
    assert status_of(detail) == "succeeded"
    assert calls["n"] == 1  # producer never re-polled
    assert [t["asset"] for t in detail["tasks"]] == ["consumer"]


async def test_fencing_concurrent_claim(state):
    """§8: a second task for a claimed scope requeues instead of dispatching;
    only one attempt owns the scope."""

    class Hold(InlinePlacement):
        async def launch(self, stage):
            task = asyncio.create_task(self._slow(stage))
            self._tasks[stage["attempt"]] = task
            return {"id": stage["attempt"]}

        async def _slow(self, stage):
            await asyncio.sleep(0.5)
            from cursus_worker.worker import run_attempt

            return await run_attempt(stage["objects"], stage["attempt"], self.project)

    @asset
    def slow():
        return [1]

    project = Project(assets=[slow])
    engine = make_engine(state, project, placements={"Local": lambda e, o, c: Hold(c, project)})
    await engine.initialize()
    run1 = await engine.submit(["slow"])
    run2 = await engine.submit(["slow"])
    t1 = asyncio.create_task(engine.run_until(run1["id"], 10))
    t2 = asyncio.create_task(engine.run_until(run2["id"], 10))
    d1, d2 = await asyncio.gather(t1, t2)
    statuses = sorted(task_statuses(d)["slow"] for d in (d1, d2))
    assert statuses == ["skipped", "succeeded"] or statuses == ["succeeded"] * 2


async def test_retry_with_backoff_and_nonretryable(state):
    """§8: retryable failures re-queue with Retry backoff; non-retryable
    failures end the task."""
    calls = {"n": 0}

    @asset(retries=Retry(2, delay=0.01))
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return ["ok"]

    project = Project(assets=[flaky])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["flaky"]))
    assert status_of(detail) == "succeeded"
    assert calls["n"] == 3
    attempts = detail["attempts"][detail["tasks"][0]["id"]]
    assert [a["status"] for a in attempts] == ["failed", "failed", "succeeded"]


async def test_cancel_run(state):
    """§8: cancel fences running attempts (LostOwnership on next renew) and
    drops queued tasks."""

    @asset(partitions=StaticPartitions(["a", "b"]))
    def slowish(ctx):
        return [{"p": ctx.partition}]

    project = Project(assets=[slowish])
    engine = make_engine(state, project, placements=fake(project))
    FakePlacement.script.clear()
    await engine.initialize()
    run = await engine.submit(["slowish"], partitions="all")
    await engine.tick()  # dispatch into the forever-waiting fake
    await asyncio.sleep(0.1)
    await engine.cancel(run["id"])
    for _ in range(50):
        await engine.tick()
        detail = await engine.run_detail(run["id"])
        if all(t["status"] in {"canceled", "succeeded", "skipped"} for t in detail["tasks"]):
            break
        await asyncio.sleep(0.05)
    assert status_of(await engine.run_detail(run["id"])) == "canceled"


async def test_every_and_cron_fire(state):
    """§9: Every fires on its interval; Cron fires when a tick passes; both
    submit runs in the run vocabulary."""
    calls = {"n": 0}

    @asset(automations=Every(1))
    def polled():
        calls["n"] += 1
        return [calls["n"]]

    @asset(automations=Cron("* * * * *"))
    def cronned():
        return []

    project = Project(assets=[polled, cronned])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.tick()
    auto = state.model.automations["polled.every.0"]
    assert auto["last_at"] is not None and auto["last_run"]
    await engine.run_until(auto["last_run"], 10)


async def test_onchange_fans_out_by_projection(state):
    """§9: OnChange pends in the commit transaction and fans out to the target
    scopes by the projection rule."""
    seen = []

    @asset(outputs=PartitionSet("sites"))
    def sites():
        return ["s1", "s2"]

    @asset(partitions="sites", deps=["sites"], automations=OnChange())
    def per_site(ctx):
        seen.append(ctx.partition)
        return [{"site": ctx.partition}]

    project = Project(assets=[sites, per_site])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["sites"]))
    await engine.tick()  # automation eval consumes the pending change
    auto = state.model.automations["per_site.onchange.0"]
    assert auto["last_run"] and auto["pending"] == []
    await engine.run_until(auto["last_run"], 10)
    assert sorted(seen) == ["s1", "s2"]


async def test_automation_toggle_and_run_now(state):
    """§9: toggles are keyed by name; run-now submits immediately."""
    calls = {"n": 0}

    @asset(automations=Automation(trigger=Every(1), enabled=True))
    def polled():
        calls["n"] += 1
        return [calls["n"]]

    project = Project(assets=[polled])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.set_automation("polled.every.0", False)
    await engine.tick()
    assert state.model.automations["polled.every.0"]["last_at"] is None
    await engine.run_automation("polled.every.0")
    run_id = state.model.automations["polled.every.0"]["last_run"]
    await engine.run_until(run_id, 10)
    assert calls["n"] == 1


async def test_every_skips_active_scope(state):
    """§9: a tick is skipped for any scope still running."""
    calls = {"n": 0}

    @asset(automations=Every(1))
    def polled():
        calls["n"] += 1
        return []

    project = Project(assets=[polled])
    engine = make_engine(state, project, placements=fake(project))
    FakePlacement.script.clear()
    await engine.initialize()
    await engine.tick()  # fires, dispatches into forever-wait
    await asyncio.sleep(0.1)
    state.model.automations["polled.every.0"]["last_at"] = 0  # make the interval due on the next tick
    await engine.tick()  # would fire again but the scope is active
    runs = await engine.list_runs(10)
    assert len([r for r in runs if r.get("automation") == "polled.every.0"]) == 1


async def test_every_skips_queued_scope(state):
    """§9: a tick is skipped for a scope that is queued but not yet running —
    otherwise repeated fires pile duplicate tasks onto the dispatch queue."""

    @asset(automations=Every(1))
    def polled():
        return []

    project = Project(assets=[polled])
    engine = make_engine(state, project)
    await engine.initialize()
    # A paused run leaves its task queued: pending work without a live lock.
    run = await engine.submit(["polled"])
    await engine.pause(run["id"])
    before = await engine.run_automation("polled.every.0")
    assert before["last_run"] is None  # the fire submitted nothing
    await engine.pause(run["id"], False)
    await engine.run_until(run["id"], 10)
    after = await engine.run_automation("polled.every.0")
    assert after["last_run"] is not None


async def test_missing_on_schedule_picks_up_new_keys(state):
    """§9: partitions='missing' on a schedule picks up new partition-set keys
    and failed first runs without an operator."""
    ran = []
    members = {"keys": ["a"]}

    @asset(outputs=PartitionSet("things"))
    def things():
        return members["keys"]

    @asset(partitions="things", automations=Automation(trigger=Every(1), partitions="missing"))
    def per_thing(ctx):
        ran.append(ctx.partition)
        return [{"t": ctx.partition}]

    project = Project(assets=[things, per_thing])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["things"]))
    auto = await engine.run_automation("per_thing.every.0")
    await engine.run_until(auto["last_run"], 10)
    assert ran == ["a"]
    members["keys"] = ["a", "b"]
    await drive(engine, await engine.submit(["things"]))
    auto = await engine.run_automation("per_thing.every.0")
    await engine.run_until(auto["last_run"], 10)
    assert ran == ["a", "b"]  # only the new key ran


async def test_timeout_fails_retryably(state):
    """§10: a wait past the attempt timeout cancels the run and fails the
    attempt retryably."""

    @asset(executor=Fake()(), timeout=1, retries=Retry(0))
    def never():
        return []

    project = Project(assets=[never], executors=[Fake()])
    engine = make_engine(state, project, placements=fake(project), lease_seconds=1)
    FakePlacement.script.clear()
    await engine.initialize()
    run = await engine.submit(["never"])
    deadline = asyncio.get_event_loop().time() + 15
    while asyncio.get_event_loop().time() < deadline:
        await engine.tick()
        detail = await engine.run_detail(run["id"])
        if status_of(detail) in {"failed", "succeeded"}:
            break
        await asyncio.sleep(0.05)
    detail = await engine.run_detail(run["id"])
    assert status_of(detail) == "failed"
    assert "timeout" in detail["tasks"][0]["error"]


async def test_harness_exit_without_result_fails_retryably(state):
    """§10: a harness that exits without writing a result is a retryable
    failure."""

    class NoResult(FakePlacement):
        async def wait(self, run, timeout):
            return {"code": 0, "reason": None, "meta": {}}

    @asset(executor=Fake()(), retries=Retry(0))
    def ghost():
        return []

    project = Project(assets=[ghost], executors=[Fake()])
    engine = make_engine(state, project, placements={"Fake": lambda e, o, c: NoResult(c)}, lease_seconds=1)
    await engine.initialize()
    detail = await engine.run_until((await engine.submit(["ghost"]))["id"], 15)
    assert status_of(detail) == "failed"
    assert "without a result" in detail["tasks"][0]["error"]


async def test_restart_requeues_and_relaunches_inflight(tmp_path):
    """§4.3/§10: claims are never journaled, so after a restart an in-flight
    attempt is simply gone: its task is queued exactly once, and dispatch
    relaunches it under a fresh attempt id."""
    launches = []
    release = {"go": False}

    class Slow(FakePlacement):
        async def launch(self, stage):
            launches.append(stage["attempt"])
            return {"id": stage["attempt"]}

        async def wait(self, run, timeout):
            if not release["go"]:
                return None
            # the harness finishes by writing its result object
            await self.ctx.state.put_object(
                f"results/{run['id']}.json",
                json.dumps(
                    {
                        "attempt": run["id"],
                        "status": "succeeded",
                        "outputs": {
                            "resumable": {
                                "ref": {
                                    "output": "resumable",
                                    "store": "json",
                                    "handle": {"object": "x.json"},
                                    "version": "v1",
                                    "partition": "",
                                    "meta": {},
                                }
                            }
                        },
                    }
                ).encode(),
            )
            return {"code": 0, "reason": None, "meta": {}}

    @asset(executor=Fake()())
    def resumable():
        return [{"ok": True}]

    project = Project(assets=[resumable], executors=[Fake()])
    url = tmp_path.as_uri()
    state = await State.open(url, "test", flush_interval=0.001)
    engine = make_engine(state, project, placements={"Fake": lambda e, o, c: Slow(c)}, lease_seconds=30)
    await engine.initialize()
    run = await engine.submit(["resumable"])
    await engine.tick()
    await asyncio.sleep(0.3)
    assert launches  # attempt is mid-flight
    task_id = state.model.attempts[launches[0]]
    # Crash the engine: cancel in-flight asyncio tasks and reopen the state,
    # like a process restart would.
    for _, t in engine.inflight.values():
        t.cancel()
    await asyncio.gather(*(t for _, t in engine.inflight.values()), return_exceptions=True)
    engine.inflight.clear()
    await state.close()

    state2 = await State.open(url, "test", flush_interval=0.001)
    task = state2.model.task(task_id)
    assert task["status"] == "queued" and task["attempts"] == []  # the lost attempt left no trace
    assert task_id in state2.model.queue and not state2.model.claims
    engine2 = make_engine(state2, project, placements={"Fake": lambda e, o, c: Slow(c)}, lease_seconds=30)
    await engine2.initialize()
    await engine2.start()
    release["go"] = True
    detail = await engine2.run_until(run["id"], 10)
    await engine2.stop()
    await state2.close()
    assert status_of(detail) == "succeeded"
    assert len(launches) == 2 and launches[1] != launches[0]  # relaunched under a fresh attempt id


async def test_max_concurrent(state):
    """§10: in-flight attempts per environment are capped at max_concurrent."""
    in_flight = {"now": 0, "peak": 0}

    class Tracked(FakePlacement):
        max_concurrent = 1

        async def launch(self, stage):
            in_flight["now"] += 1
            in_flight["peak"] = max(in_flight["peak"], in_flight["now"])
            return await super().launch(stage)

        async def wait(self, run, timeout):
            await asyncio.sleep(0.05)
            in_flight["now"] -= 1
            return {"code": None, "reason": "lost", "meta": {}}

    @asset(executor=Fake()(), partitions=StaticPartitions(["a", "b", "c"]))
    def work(ctx):
        return [{"p": ctx.partition}]

    project = Project(assets=[work], executors=[Fake()])
    engine = make_engine(state, project, placements={"Fake": lambda e, o, c: Tracked(c)}, concurrency=10)
    await engine.initialize()
    await engine.run_until((await engine.submit(["work"], partitions="all"))["id"], 15)
    assert in_flight["peak"] == 1


async def test_identical_poll_wakes_nothing(state):
    """§1 corollary: a poll producing identical content yields no `changed`
    and pends no automation."""
    fired = []

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a", "v": 1}]

    @asset(inputs={"feed": Incremental()}, automations=OnChange())
    def consumer(feed: list):
        fired.append(True)
        return []

    project = Project(assets=[feed, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert fired == [True]
    await engine.tick()  # drain the first pending event
    await drive(engine, await engine.submit(["feed"]))  # identical content
    assert state.model.automations["consumer.onchange.0"]["pending"] == []
    fired.clear()
    await engine.tick()
    assert fired == []  # nothing woke: identical content is not a change


async def test_job_commits_lineage_only(state):
    """§2: a job has inputs, partitions, cursor and placement like an asset;
    its commit records lineage and fires nothing."""

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a"}]

    @job(inputs={"feed": "feed"})
    def vacuum(feed: list):
        assert len(feed) == 1

    project = Project(assets=[feed, vacuum])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["vacuum"], upstream=True))
    assert status_of(detail) == "succeeded"
    task = next(t for t in detail["tasks"] if t["asset"] == "vacuum")
    [attempt] = detail["attempts"][task["id"]]
    assert attempt["status"] == "succeeded" and not attempt.get("result")  # no outputs, no heads
    spec = json.loads(await state.get_object(f"specs/{attempt['id']}.json"))
    assert spec["inputs"]["feed"]["ref"]["output"] == "feed"  # lineage is the spec


class MigratingJsonStore(JsonStore):
    """A JsonStore with a migration ledger: lets keyed outputs declare
    migrations on the file:// test store (§4)."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def can_store(self, t, output):
        from collections.abc import Callable

        return t is Callable or super().can_store(t, output)

    async def migrate(self, output, migrations):
        self.calls.append(output.name)
        return [m.name for m in migrations]


async def test_migration_changes_fingerprint_and_marks_handle(state):
    """§6/§4: adding a migration to an asset's output changes the
    interpretation fingerprint so every key reprocesses, and the new head's
    handle carries the last applied migration as `schema`."""
    from cursus.sdk import Migration

    seen = []
    store = MigratingJsonStore()

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental()}, outputs=Output("rolled", store="mig"))
    def consumer(ctx, files: list):
        seen.append(sorted(r["id"] for r in files))
        return {"n": len(files)}

    project = Project(assets=[files, consumer], stores={"mig": store})
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen == [["a", "b"]]
    assert store.calls == []  # no migrations declared yet

    detail = await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert task_statuses(detail)["consumer"] == "skipped"

    @asset(outputs=Output("files", key="id"))
    def files():  # noqa: F811 — unchanged upstream
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(
        inputs={"files": Incremental()},
        outputs=Output("rolled", store="mig", migrations=[Migration("m1", lambda o, p: None)]),
    )
    def consumer(ctx, files: list):  # noqa: F811 — same asset, one migration added
        seen.append(sorted(r["id"] for r in files))
        return {"n": len(files)}

    project2 = Project(assets=[files, consumer], stores={"mig": store})
    engine2 = make_engine(state, project2)
    await engine2.initialize()
    await drive(engine2, await engine2.submit(["consumer"], upstream=True))
    assert seen == [["a", "b"], ["a", "b"]]  # fingerprint changed: all keys again
    assert store.calls == ["rolled"]  # migrate ran before the write

    rolled_head = head(state, "rolled")
    assert rolled_head["ref"]["handle"]["schema"] == "m1"


async def test_ondeploy_fires_once_per_revision(state):
    """§9: OnDeploy fires when the served revision differs from
    last_revision, then records it; further ticks stay quiet."""
    calls = []

    @job(automations=Automation(trigger=OnDeploy()))
    def deployed():
        calls.append(1)

    project = Project(assets=[deployed])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    await engine.run_until(auto["last_run"], 30)
    assert calls == [1]
    auto = state.model.automations["deployed.ondeploy.0"]
    assert auto["last_revision"] == project.manifest["revision"]

    for _ in range(3):
        await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    assert calls == [1]
    assert auto["last_revision"] == project.manifest["revision"]


async def test_ondeploy_silent_on_restart_same_revision(state):
    """§9: a re-registration of the same revision does not refire."""
    calls = []

    @job(automations=Automation(trigger=OnDeploy()))
    def deployed():
        calls.append(1)

    project = Project(assets=[deployed])
    engine = make_engine(state, project)
    await engine.initialize()
    await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    await engine.run_until(auto["last_run"], 30)
    assert calls == [1]

    engine2 = make_engine(state, project)  # same manifest, same revision
    await engine2.initialize()
    await engine2.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    assert auto["last_revision"] == project.manifest["revision"]
    assert calls == [1]


async def test_ondeploy_two_registrations_fire_latest_once(state):
    """§9: two registrations before a tick fire once, for the latest
    revision only."""
    calls = []

    @job(automations=Automation(trigger=OnDeploy()))
    def deployed():
        calls.append(1)

    project_a = Project(assets=[deployed])
    await make_engine(state, project_a).initialize()

    @job(automations=Automation(trigger=OnDeploy()), version="2")
    def deployed():  # noqa: F811 — redeployed with a new revision
        calls.append(1)

    project_b = Project(assets=[deployed])
    assert project_b.manifest["revision"] != project_a.manifest["revision"]
    engine = make_engine(state, project_b)
    await engine.initialize()
    await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    await engine.run_until(auto["last_run"], 30)
    assert calls == [1]
    auto = state.model.automations["deployed.ondeploy.0"]
    assert auto["last_revision"] == project_b.manifest["revision"]
