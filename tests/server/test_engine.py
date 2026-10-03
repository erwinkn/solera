"""The engine (architecture §2, §5–§10). Small inline projects run
through InlinePlacement against the real file:// object store."""

import asyncio
import json

import pytest
from solera.executors import Executor
from solera.sdk import (
    AllPartitions,
    Automation,
    Cron,
    DynamicPartitions,
    Every,
    In,
    Incremental,
    ObjectRef,
    OnChange,
    OnDeploy,
    Output,
    Project,
    Ref,
    Result,
    Retry,
    StaticPartitions,
    asset,
    job,
)
from solera.stores import FileStore, Patch
from solera_server.executors.inline import InlinePlacement

from .engines import drive, make_engine, status_of, task_statuses
from .test_fence import own


class Fake(Executor):
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

    async def wait(self, handle, timeout):
        script = self.script.get(handle["id"], {})
        waits = script.setdefault("waits", 0)
        script["waits"] = waits + 1
        if script.get("exit_after") is not None and waits > script["exit_after"]:
            return script.get("exit", {"code": 0, "reason": None, "meta": {}})
        if script.get("exit"):
            return script["exit"]
        await asyncio.sleep(min(timeout, 0.05))
        return None

    async def cancel(self, handle):
        self.script.setdefault(handle["id"], {})["canceled"] = True


def fake(project, **env_kw):
    return {"Fake": lambda s, c: FakePlacement(c)}


def head(state, output, partition=""):
    return state.model.heads.get((output, partition))


async def spec_of(state, output, partition=""):
    """The spec of the attempt behind a head: what it read (lineage)."""

    h = head(state, output, partition)
    return await state.attempt_spec(h["run"], h["attempt"])


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
    assert installed["ref"]["output"] == "numbers" and installed["ref"]["generation"]
    assert state.model.partition("numbers", "")["caught_up"] is True
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
    assert head(state, "a")["ref"]["generation"] > head(state, "b")["ref"]["generation"]
    assert head(state, "b") is not None  # kept from the first commit
    assert state.model.partition("pair", "").get("cursor") == "c2"
    await drive(engine, await engine.submit(["pair"], mode="full"))
    assert state.model.partition("pair", "").get("cursor") is None  # full clears the cursor


async def test_a_cursor_the_journal_cannot_hold_fails_its_attempt(state):
    """F27: a worker's result is recorded as any event is: an integer past
    64 bits fails the attempt that returned it, and the engine goes on."""

    @asset
    def huge():
        return Result(outputs={"huge": {"a": 1}}, cursor={"n": 2**70})

    engine = make_engine(state, Project(assets=[huge]))
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["huge"]))
    assert status_of(detail) == "failed" and "64-bit" in detail["tasks"][0]["error"]
    assert not state.poisoned and state.model.partition("huge", "").get("cursor") is None


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
    """§5: a str input renames the bound output; In(meta=) is recorded on the
    input in the manifest."""
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
    assert status_of(detail) == "succeeded", [t.get("error") for t in detail["tasks"]]
    assert seen["feed"] == [{"id": "a", "v": 1}, {"id": "b", "v": 2}]


async def test_incremental_filters_input_and_changes(state):
    """§5/§6: under Incremental the parameter arrives filtered to upserted
    keys and ctx.batch carries upserted + deleted."""
    seen = {}
    content = {"rows": [{"id": "a", "v": 1}, {"id": "b", "v": 1}, {"id": "c", "v": 1}]}

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["rows"]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen["rows"] = list(files)
        seen["upserted"] = list(ctx.batch["files"].upserted)
        seen["deleted"] = list(ctx.batch["files"].removed)
        return [{"n": len(files)}]

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["upserted"] == ["a", "b", "c"]  # §6: first pass upserts everything
    assert {r["id"] for r in seen["rows"]} == {"a", "b", "c"}

    # Second run, nothing written since → skipped, nothing loaded.
    seen.clear()
    detail = await drive(engine, await engine.submit(["consumer"]))
    assert task_statuses(detail)["consumer"] == "skipped"
    assert seen == {}

    # One key written again reprocesses only that key (§6).
    content["rows"] = Patch([{"id": "b", "v": 2}])
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["upserted"] == ["b"]
    assert [r["id"] for r in seen["rows"]] == ["b"]

    # A deletion arrives via ctx.batch (§5: what a selection cannot carry).
    seen.clear()
    content["rows"] = Patch([], remove=["b"])
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert seen["deleted"] == ["b"] and seen["upserted"] == []


async def test_config_change_reprocesses_everything(state):
    """§6/§2.2: run config is part of the fingerprint —
    changing it forces full=True on the input and reprocesses every key."""
    seen = {}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.setdefault("commits", []).append([r["id"] for r in files])
        seen.setdefault("full", []).append(ctx.batch["files"].full)
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    await drive(engine, await engine.submit(["consumer"], upstream=True, config={"threshold": 2}))
    assert seen["commits"] == [["a"], ["a"]]
    assert seen["full"] == [True, True]  # first pass + fingerprint reset


async def test_full_run_resets_position(state):
    """§2.2: a `full` run resets the input position — the consumer re-reads
    the whole head (not a diff) and the position lands past the head commit."""
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": 1}, {"id": "b", "v": 1}]

    @asset(inputs={"files": Incremental()})
    def consumer(ctx, files: list):
        seen.append((sorted(r["id"] for r in files), ctx.batch["files"].full))
        return [{"n": len(files)}]

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    first = state.model.position("consumer", "files", "")
    assert first == {
        "kind": "keys",
        "next": 1,  # the head's next commit: nothing under way
        "fingerprint": first["fingerprint"],
        "output": "files",
        "upstream_partition": "",
        "reset_by": first["reset_by"],  # the run whose reset began the pass
    }
    detail = await drive(engine, await engine.submit(["consumer"], mode="full"))
    assert task_statuses(detail)["consumer"] == "succeeded"  # never skipped on full
    second = state.model.position("consumer", "files", "")
    assert second == {**first, "reset_by": detail["request"]["id"]}  # back at head+1, nothing left mid-way
    # Both passes were full-head reads.
    assert seen == [(["a", "b"], True), (["a", "b"], True)]


async def test_a_version_bump_starts_the_next_run_over(state):
    """§6, Erwin's (a): a version bump is an asset change like any other. The
    next run, a plain one, starts over: no operator, no failure, and the
    partition is stale until then (definition changed)."""
    count = {"n": 0}

    def project(version):
        @asset(version=version)
        def versioned():
            count["n"] += 1
            return [count["n"]]

        return Project(assets=[versioned])

    engine = make_engine(state, project("1"))
    await engine.initialize()
    await drive(engine, await engine.submit(["versioned"]))
    await engine.stop()
    engine = make_engine(state, project("2"))
    await engine.initialize()
    assert await engine.stale_reasons("versioned", "") == ["definition changed"]
    detail = await drive(engine, await engine.submit(["versioned"]))
    assert status_of(detail) == "succeeded" and count["n"] == 2
    assert state.model.heads[("versioned", "")]["version"] == "2"
    assert await engine.stale_reasons("versioned", "") == []


async def test_incremental_batching_and_more(state):
    """§6: work is batched by batch_size; `more` re-queues the task;
    scope_complete lands on the head only with the last batch."""
    commits = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i}", "v": 1} for i in range(5)]

    @asset(inputs={"files": Incremental(batch_size=2)})
    def consumer(ctx, files: list):
        commits.append([r["id"] for r in files])
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert commits == [["k0", "k1"], ["k2", "k3"], ["k4"]]
    assert state.model.partition("consumer", "")["caught_up"] is True
    task = [t for t in detail["tasks"] if t["asset"] == "consumer"][0]
    assert len(detail["attempts"][task["id"]]) == 3  # three batches, three attempts


async def test_run_keys_override(state):
    """§8: a `keys=` list is a selection that moves no pass; on a plain input
    it is delivered only the named keys' changes past the position (K45), so
    with nothing changed it is skipped. 'full' drains the folded key map as
    a reset."""
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
    detail = await drive(engine, await engine.submit(["consumer"], keys={"files": {"keys": ["b"]}}))
    assert status_of(detail) == "succeeded", [t.get("error") for t in detail["tasks"]]
    assert seen == [["a", "b"]]  # b did not change past the position: nothing to deliver
    await drive(engine, await engine.submit(["consumer"], keys={"files": "full"}))
    assert seen[-1] == ["a", "b"]


async def test_a_selection_on_a_full_pass_due_starts_it_or_continues_it(state):
    """Review round 5 (system #3, engine #2), under K45 and Erwin's
    correction: on a consumer never run, a full pass is due, and keys=(b)
    starts it over with b alone. A default run continues it, never
    delivering b again, and stops half-way (after a); keys=(c) then
    delivers the last key, and the pass is done: the partition is caught
    up and its record collapses."""
    calls, broken = [], {"batch": 2}

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": 1} for k in "abc"]

    @asset(inputs={"files": Incremental(batch_size=1)}, retries=Retry(n=0))
    def consumer(ctx, files: list):
        if ctx.batch["files"].index == broken["batch"]:
            raise RuntimeError("stopped half-way")
        calls.append((sorted(r["id"] for r in files), ctx.batch["files"].first))
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    detail = await drive(engine, await engine.submit(["consumer"], keys={"files": {"keys": ["b"]}}))
    assert status_of(detail) == "succeeded" and calls == [(["b"], True)]  # the start-over
    assert state.model.position("consumer", "files", "")["pass"]["mode"] == "full"
    assert not state.model.partition("consumer", "").get("caught_up")
    calls.clear()
    await drive(engine, await engine.submit(["consumer"]))  # continues; stops before `c`
    assert calls == [(["a"], False)], "b is not delivered again, and nothing starts over"
    assert state.model.partition("consumer", "")["caught_up"] is False
    calls.clear()
    broken["batch"] = None
    await drive(engine, await engine.submit(["consumer"], keys={"files": {"keys": ["c"]}}))
    assert calls == [(["c"], False)]
    position = state.model.position("consumer", "files", "")
    assert "pass" not in position and "ahead" not in position and position["next"] == 1
    assert state.model.partition("consumer", "")["caught_up"] is True
    with pytest.raises(ValueError, match="cannot be a full run"):
        await engine.submit(["consumer"], mode="full", keys={"files": {"keys": ["a"]}})


async def test_a_full_override_resumes_its_pass_batch_by_batch(state):
    """Engine review round 2 #2: `keys={"files": "full"}` starts one pass per
    run and its later batches resume it — the first batch is not served again."""
    seen = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": 1} for k in "abc"]

    @asset(inputs={"files": Incremental(batch_size=2)})
    def consumer(ctx, files: list):
        changes = ctx.batch["files"]
        seen.append((sorted(r["id"] for r in files), changes.full, changes.index))
        return []

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    for _ in range(2):  # a second override starts a pass of its own
        seen.clear()
        detail = await drive(engine, await engine.submit(["consumer"], keys={"files": "full"}), timeout=10)
        assert status_of(detail) == "succeeded"
        assert seen == [(["a", "b"], True, 0), (["c"], True, 1)]


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
    def by_ref(upstream: ObjectRef):
        seen["ref"] = upstream
        return [{"v": upstream.generation}]

    project = Project(assets=[upstream, by_ref])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["by_ref"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert isinstance(seen["ref"], Ref) and seen["ref"].output == "upstream"


async def test_all_partitions_values(state, data):
    """§7: AllPartitions yields dict[key, value] over upstream-only dimensions,
    resolved to keys with complete heads at pin time."""

    @asset(outputs=DynamicPartitions("sites"))
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
    # The dynamic partitions must be committed before fan-out planning can see it (§7).
    await drive(engine, await engine.submit(["sites"]))
    detail = await drive(engine, await engine.submit(["rollup"], upstream=True))
    assert status_of(detail) == "succeeded"
    rows = json.loads((data / f"{head(state, 'rollup')['ref']['handle']['path']}.json").read_text())
    assert rows == [{"n": 2, "sites": ["east", "west"]}]


async def test_partition_selections(state):
    """§7/§8: 'latest', 'missing', 'all' and explicit lists select from the
    current dynamic partitions."""
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
    """§5/§7: an external DynamicPartitions is patched through the commit API; new
    keys surface through partitions='missing'."""
    ran = []

    @asset(partitions="uploads")
    def per_upload(ctx):
        ran.append(ctx.partition)
        return [{"u": ctx.partition}]

    project = Project(assets=[per_upload], sources=[DynamicPartitions("uploads")])
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
    # The same elements listed again are not a change (§5, docs/versions.md §2).
    result = await engine.commit_source("uploads", keys=["u-1", "u-2", "u-3"])
    assert result["changed"] is False


async def test_two_dimension_broadcast_and_collapse(state):
    """§7: a consumer-only dimension broadcasts; an upstream-only dimension
    collapses via AllPartitions."""
    seen = []

    @asset(outputs=DynamicPartitions("sites"))
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
    await drive(engine, await engine.submit(["sites"]))  # commit the dynamic partitions
    detail = await drive(engine, await engine.submit(["broadcast"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert sorted(seen) == [("s1", 5), ("s2", 5)]
    detail = await drive(engine, await engine.submit(["collapsed"], upstream=True))
    assert status_of(detail) == "succeeded"


async def test_retired_keys_leave_fanout(state):
    """§7: retired keys leave fan-out but their heads persist read-only."""
    members = {"keys": ["a", "b"]}

    @asset(outputs=DynamicPartitions("things"))
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
    assert [t["partition"] for t in detail["tasks"]] == ["a"]
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
    """§8: a second task for a claimed partition requeues instead of dispatching;
    only one attempt owns the partition."""

    class Hold(InlinePlacement):
        async def launch(self, stage):
            task = asyncio.create_task(self._slow(stage))
            self._tasks[stage["attempt"]] = task
            return {"id": stage["attempt"]}

        async def _slow(self, stage):
            await asyncio.sleep(0.5)
            from solera_worker.worker import run_attempt

            return await run_attempt(stage["objects"], stage["attempt"], self.project, run=stage["run"])

    @asset
    def slow():
        return [1]

    project = Project(assets=[slow])
    engine = make_engine(state, project, placements={"Local": lambda s, c: Hold(c, project)})
    await engine.initialize()
    run1 = await engine.submit(["slow"])
    run2 = await engine.submit(["slow"])
    t1 = asyncio.create_task(engine.run_until(run1["id"], 60))
    t2 = asyncio.create_task(engine.run_until(run2["id"], 60))
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
    assert [a["outcome"] for a in attempts] == ["failed", "failed", "succeeded"]


async def test_cancel_run(state):
    """§8: cancel fences running attempts (LostOwnership on next renew) and
    drops queued tasks."""

    @asset(partitions=StaticPartitions(["a", "b"]), executor=Fake("fake")())
    def slowish(ctx):
        return [{"p": ctx.partition}]

    project = Project(assets=[slowish], executors=[Fake("fake")])
    engine = make_engine(state, project, placements=fake(project))
    FakePlacement.script.clear()
    await engine.initialize()
    run = await engine.submit(["slowish"], partitions="all")
    await engine.tick()  # dispatch into the forever-waiting fake
    await asyncio.sleep(0.1)
    await engine.cancel(run["id"])
    for _ in range(1200):
        await engine.tick()
        detail = await engine.run_detail(run["id"])
        if all(t["status"] in {"canceled", "succeeded", "skipped"} for t in detail["tasks"]):
            break
        await asyncio.sleep(0.05)
    assert status_of(await engine.run_detail(run["id"])) == "canceled"


async def test_every_and_cron_fire(state):
    """§9: Every fires on its interval; Cron fires when a tick passes; both
    submit runs in the run vocabulary. A schedule waits for its next time,
    a new one too: nothing fires at declaration."""
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
    assert auto["last_fired"] is None  # not at once: its first time is a second away
    for _ in range(600):
        await asyncio.sleep(0.1)
        await engine.tick()
        auto = state.model.automations["polled.every.0"]
        if auto["last_fired"] is not None:
            break
    assert auto["last_fired"] is not None and auto["last_run"]
    await engine.run_until(auto["last_run"], 60)


async def test_onchange_fans_out_by_projection(state):
    """§9: OnChange pends in the commit transaction and fans out to the target
    partitions by the projection rule."""
    seen = []

    @asset(outputs=DynamicPartitions("sites"))
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
    await engine.run_until(auto["last_run"], 60)
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
    assert state.model.automations["polled.every.0"]["last_fired"] is None
    await engine.run_automation("polled.every.0")
    run_id = state.model.automations["polled.every.0"]["last_run"]
    await engine.run_until(run_id, 60)
    assert calls["n"] == 1


async def test_an_automation_can_skip_until_its_inputs_are_written(state):
    """§9: with skip_missing_inputs, a tick over an input never written is
    skipped rather than run to fail; one that builds the input is not."""

    @asset
    def index():
        return [1]

    @asset(inputs={"index": "index"}, automations=Automation(trigger=Every(1), skip_missing_inputs=True))
    def digest(index: list):
        return index

    project = Project(assets=[index, digest])
    engine = make_engine(state, project)
    await engine.initialize()
    for _ in range(600):  # its first time is a second away
        await asyncio.sleep(0.1)
        await engine.tick()
        auto = state.model.automations["digest.every.0"]
        if auto["last_fired"] is not None:
            break
    assert auto["last_fired"] is not None and auto["last_run"] is None and not state.model.runs
    run = await engine.submit(["digest"], upstream=True, skip_missing_inputs=True)
    assert run is not None and sorted(run["targets"]) == ["digest", "index"]
    await engine.run_until(run["id"], 60)
    assert await engine.submit(["digest"], skip_missing_inputs=True) is not None


async def test_every_skips_active_scope(state):
    """§9: a tick is skipped for any partition still running."""
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
    state.model.automations["polled.every.0"]["last_fired"] = 0  # make the interval due on the next tick
    await engine.tick()  # would fire again but the partition is active
    runs = (await engine.list_runs(limit=10))["runs"]
    assert len([r for r in runs if r.get("automation") == "polled.every.0"]) == 1


async def test_every_skips_queued_scope(state):
    """§9: a tick is skipped for a partition that is queued but not yet running —
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
    await engine.run_until(run["id"], 60)
    after = await engine.run_automation("polled.every.0")
    assert after["last_run"] is not None


async def test_missing_on_schedule_picks_up_new_keys(state):
    """§9: partitions='missing' on a schedule picks up new partition-set keys
    and failed first runs without an operator."""
    ran = []
    members = {"keys": ["a"]}

    @asset(outputs=DynamicPartitions("things"))
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
    await engine.run_until(auto["last_run"], 60)
    assert ran == ["a"]
    members["keys"] = ["a", "b"]
    await drive(engine, await engine.submit(["things"]))
    auto = await engine.run_automation("per_thing.every.0")
    await engine.run_until(auto["last_run"], 60)
    assert ran == ["a", "b"]  # only the new key ran


async def test_timeout_fails_retryably(state):
    """§10: a worker running past the attempt timeout is canceled, and the
    attempt fails retryably."""

    class Running(FakePlacement):
        async def launch(self, stage):  # the worker starts: owning is its first report
            await own(self.ctx.state, stage["run"], stage["attempt"])
            return await super().launch(stage)

    @asset(executor=Fake("fake")(), timeout=1, retries=Retry(0))
    def never():
        return []

    project = Project(assets=[never], executors=[Fake("fake")])
    engine = make_engine(
        state, project, placements={"Fake": lambda s, c: Running(c)}, heartbeat_seconds=1, cancel_grace=0.3
    )
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
    closed = [e for e in await engine.history.events(run["id"]) if e["type"] == "aborted"]
    assert [e["reason"] for e in closed] == ["timeout"]


async def test_harness_exit_without_result_fails_retryably(state):
    """§10: a worker that exits without writing a result is a retryable
    failure."""

    class NoResult(FakePlacement):
        async def launch(self, stage):
            # What the worker said before it went silent, by a clock far off.
            beat = {
                "worker_id": "w",
                "seq": 1,
                "events": [{"type": "booted", "at": 0}, {"type": "computing", "at": 1e12}],
            }
            await own(self.ctx.state, stage["run"], stage["attempt"])
            path = f"{self.ctx.state.attempt_path(stage['run'], stage['attempt'])}.beat"
            await self.ctx.state.put_object(path, json.dumps(beat).encode())
            return await super().launch(stage)

        async def wait(self, handle, timeout):
            return {"code": 0, "reason": None, "meta": {}}

    @asset(executor=Fake("fake")(), retries=Retry(0))
    def ghost():
        return []

    project = Project(assets=[ghost], executors=[Fake("fake")])
    engine = make_engine(state, project, placements={"Fake": lambda s, c: NoResult(c)}, heartbeat_seconds=1)
    await engine.initialize()
    run = await engine.submit(["ghost"])
    detail = await engine.run_until(run["id"], 60)
    assert status_of(detail) == "failed"
    assert "without a result" in detail["tasks"][0]["error"]
    events = [e for e in await engine.history.events(run["id"]) if e["attempt"]]
    assert [(e["type"], e["reason"]) for e in events] == [
        ("claimed", None),
        ("launched", None),
        ("booted", None),
        ("computing", None),
        ("lost", "exit code 0"),
    ]
    launched, booted, computing, lost = events[1:]
    assert booted["at"] >= launched["at"] and computing["at"] == lost["at"]  # within the attempt


async def test_max_concurrent(state):
    """§10: in-flight attempts per environment are capped at max_concurrent."""
    in_flight = {"now": 0, "peak": 0}

    class Tracked(FakePlacement):
        max_concurrent = 1

        async def launch(self, stage):
            in_flight["now"] += 1
            in_flight["peak"] = max(in_flight["peak"], in_flight["now"])
            return await super().launch(stage)

        async def wait(self, handle, timeout):
            await asyncio.sleep(0.05)
            in_flight["now"] -= 1
            return {"code": None, "reason": "lost", "meta": {}}

    @asset(executor=Fake("fake")(), partitions=StaticPartitions(["a", "b", "c"]))
    def work(ctx):
        return [{"p": ctx.partition}]

    project = Project(assets=[work], executors=[Fake("fake")])
    engine = make_engine(state, project, placements={"Fake": lambda s, c: Tracked(c)}, concurrency=10)
    await engine.initialize()
    run = await engine.submit(["work"], partitions="all")
    await engine.run_until(run["id"], 60)
    assert in_flight["peak"] == 1
    held = [e for e in await engine.history.events(run["id"]) if e["type"] == "held"]
    assert held and {(e["reason"], e["name"]) for e in held} == {("executor", "fake")}


async def test_a_poll_that_writes_nothing_wakes_nothing(state):
    """§1 corollary: a poll that finds nothing new writes nothing — an empty
    `Patch` — which yields no `changed` and pends no automation. (Writing
    the same rows again would be a change: docs/versions.md §1.)"""
    fired, polls = [], []

    @asset(outputs=Output("feed", key="id"))
    def feed():
        polls.append(True)
        return [{"id": "a", "v": 1}] if len(polls) == 1 else Patch([])

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
    await drive(engine, await engine.submit(["feed"]))  # nothing new
    assert polls == [True, True]
    assert state.model.automations["consumer.onchange.0"]["pending"] == []
    fired.clear()
    await engine.tick()
    assert fired == []  # nothing woke: nothing was written


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
    assert attempt["outcome"] == "succeeded" and not attempt.get("outputs")  # no outputs, no heads
    spec = await state.attempt_spec(detail["request"]["id"], attempt["id"])
    assert spec["inputs"]["feed"]["ref"]["output"] == "feed"  # lineage is the spec


class MigratingStore(FileStore):
    """A FileStore with a migration ledger: lets outputs declare
    migrations on the default test store (§4)."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def can_store(self, t, output):
        from collections.abc import Callable

        return t is Callable or super().can_store(t, output)

    async def migrate(self, output, migrations, context=None, prior=None):
        self.calls.append(output.name)
        return [m.name for m in migrations]


async def test_migration_changes_fingerprint_and_marks_handle(state):
    """§6/§4: adding a migration to an asset's output changes the
    fingerprint so every key reprocesses, and the new head's
    handle carries the last applied migration as `schema`."""
    from solera.sdk import Migration

    seen = []
    store = MigratingStore()

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

    detail = await drive(engine, await engine.submit(["consumer"]))
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
    await drive(engine2, await engine2.submit(["consumer"]))
    assert seen == [["a", "b"], ["a", "b"]]  # fingerprint changed: all keys again
    assert store.calls == ["rolled"]  # migrate ran before the write

    rolled_head = head(state, "rolled")
    assert rolled_head["ref"]["handle"]["schema"] == "m1"


async def test_ondeploy_fires_once_per_revision(state):
    """§9: OnDeploy fires when the served deploy differs from
    last_deploy, then records it; further ticks stay quiet."""
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
    assert auto["last_deploy"] == project.manifest["deploy"]

    for _ in range(3):
        await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    assert calls == [1]
    assert auto["last_deploy"] == project.manifest["deploy"]


async def test_ondeploy_silent_on_restart_same_revision(state):
    """§9: a re-registration of the same deploy does not refire."""
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

    engine2 = make_engine(state, project)  # same manifest, same deploy
    await engine2.initialize()
    await engine2.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    assert auto["last_deploy"] == project.manifest["deploy"]
    assert calls == [1]


async def test_ondeploy_two_registrations_fire_latest_once(state):
    """§9: two registrations before a tick fire once, for the latest
    deploy only."""
    calls = []

    @job(automations=Automation(trigger=OnDeploy()))
    def deployed():
        calls.append(1)

    project_a = Project(assets=[deployed])
    await make_engine(state, project_a).initialize()

    @job(automations=Automation(trigger=OnDeploy()), version="2")
    def deployed():  # noqa: F811 — redeployed: a new deploy
        calls.append(1)

    project_b = Project(assets=[deployed])
    assert project_b.manifest["deploy"] != project_a.manifest["deploy"]
    engine = make_engine(state, project_b)
    await engine.initialize()
    await engine.tick()
    auto = state.model.automations["deployed.ondeploy.0"]
    await engine.run_until(auto["last_run"], 30)
    assert calls == [1]
    auto = state.model.automations["deployed.ondeploy.0"]
    assert auto["last_deploy"] == project_b.manifest["deploy"]


async def test_a_batch_says_where_it_sits_in_its_pass(state):
    """A pass spans batches of `batch_size`: `index` is the batch's index,
    `count` the plan, `first` is batch 0, `final` the pass running out,
    and `full` holds on every batch of a full pass — for keyed full
    passes, delta passes and unkeyed upstreams (§5; review round 3,
    system B5)."""

    from solera.stores import Patch

    pages, rebuilt = [], {"keys": []}
    content = {f"k{i}": 1 for i in range(7)}
    written = {"keys": list(content)}

    @asset(outputs=Output("files", key="id"))
    def files():
        return Patch([{"id": k, "v": content[k]} for k in written["keys"]])

    @asset(inputs={"files": Incremental(batch_size=3)})
    def consumer(ctx, files: list):
        ch = ctx.batch["files"]
        pages.append((ch.index, ch.count, ch.first, ch.final, ch.full))
        if ch.full and ch.first:
            rebuilt["keys"] = []
        rebuilt["keys"] += [r["id"] for r in files]
        return [{"n": len(files)}]

    @asset(outputs=Output("log", incremental=True))
    def log(ctx):
        return Patch([{"n": int(ctx.cursor or 0)}])

    batch_pages = []

    @asset(inputs={"log": Incremental(batch_size=1)})
    def tail(ctx, log: list):
        ch = ctx.batch["log"]
        batch_pages.append((ch.index, ch.count, ch.first, ch.final, list(ch.upstream.commits), ch.full))
        return [{"n": len(log)}]

    project = Project(assets=[files, consumer, log, tail])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert pages == [(0, 3, True, False, True), (1, 3, False, False, True), (2, 3, False, True, True)]
    assert sorted(rebuilt["keys"]) == [f"k{i}" for i in range(7)]
    # A delta pass of four changed keys: two batches.
    pages.clear()
    for key in ("k0", "k2", "k4", "k6"):
        content[key] = 2
    written["keys"] = ["k0", "k2", "k4", "k6"]
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert pages == [(0, 2, True, False, False), (1, 2, False, True, False)]
    for _ in range(3):
        await drive(engine, await engine.submit(["log"]))
    await drive(engine, await engine.submit(["tail"]))
    assert batch_pages == [
        (0, 3, True, False, [0], True),
        (1, 3, False, False, [1], True),
        (2, 3, False, True, [2], True),
    ]
    batch_pages.clear()
    await drive(engine, await engine.submit(["log"]))
    await drive(engine, await engine.submit(["tail"]))
    assert batch_pages == [(0, 1, True, True, [3], False)]  # then a delta


async def test_the_batch_plan_is_an_estimate_but_final_is_not(state):
    """Patterns filter keys after the plan is made: batches are formed from the
    keys they take, read ahead past the rest, so the pass takes the batches
    it takes, none is empty, and `final` is on the last real one (§5)."""

    pages = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i}", "v": 1} for i in range(7)]

    @asset(inputs={"files": Incremental(batch_size=3, include=["k0", "k1", "k2", "k3"])})
    def consumer(ctx, files: list):
        ch = ctx.batch["files"]
        pages.append((ch.index, ch.count, ch.final, sorted(r["id"] for r in files)))
        return [{"n": len(files)}]

    project = Project(assets=[files, consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["consumer"], upstream=True))
    assert pages == [
        (0, 3, False, ["k0", "k1", "k2"]),
        (1, 3, True, ["k3"]),  # k4..k6 read past: nothing follows, so this batch is final
    ]


async def test_batches_read_ahead_past_keys_the_patterns_leave_out(state):
    """Every batch holds `batch_size` taken keys, however sparse they are in the
    upstream; a delta pass that takes none never calls the producer. A full
    pass does, once, with an empty batch: it starts its consumer over."""

    calls = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i:02d}", "v": 1} for i in range(30)]

    @asset(inputs={"files": Incremental(batch_size=2, include=["k03", "k04", "k17", "k18", "k29"])})
    def sparse(ctx, files: list):
        calls.append((ctx.batch["files"].index, sorted(r["id"] for r in files), ctx.batch["files"].final))
        return [{"n": len(files)}]

    @asset(inputs={"files": Incremental(batch_size=2, include="nothing/**")})
    def none(files: list):
        calls.append("none")
        return []

    project = Project(assets=[files, sparse, none])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["sparse"], upstream=True))
    assert calls == [(0, ["k03", "k04"], False), (1, ["k17", "k18"], False), (2, ["k29"], True)]
    await drive(engine, await engine.submit(["none"]))  # its first pass: a full one
    assert calls.count("none") == 1
    await drive(engine, await engine.submit(["files"], mode="full"))  # every key changes: v=1 rewritten
    detail = await drive(engine, await engine.submit(["none"]))  # a delta pass taking none
    assert calls.count("none") == 1 and task_statuses(detail)["none"] == "skipped"


async def test_a_batch_looks_ahead_a_bounded_way(state, monkeypatch):
    """Review round 5 (system #5): a batch reads the index in chunks, whatever
    it still lacks, and examines at most `LOOKAHEAD` entries — past them it
    goes as it is, not final; a batch left with nothing is skipped without
    calling the producer, and the pass still completes."""
    from solera.keys.index import key_bytes
    from solera_worker import each

    # The reviewer's case: 100 matches at the head of 10,000 keys, a batch of 100.
    keys = [key_bytes(f"a/{i:05d}") for i in range(10_000)]
    scans = []

    async def chunk(after, n):
        scans.append(n)
        lo = 0 if after is None else keys.index(after) + 1
        part = keys[lo : lo + n]
        return [(k, b"v", 0, 0) for k in part], (part[-1] if lo + n < len(keys) else None)

    def kind(entry):
        return "upsert" if entry[0].startswith(b"a/000") else None

    page, after, read = await each._fill(chunk, None, 100, kind)
    assert len(page) == 100 and after is None and read == 10_000 and len(scans) == 100  # was 9,900

    calls = []

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": f"k{i:02d}", "v": 1} for i in range(20)]

    @asset(inputs={"files": Incremental(batch_size=100, include="k0*")})
    def sparse(ctx, files: list):
        calls.append((sorted(r["id"] for r in files), ctx.batch["files"].final))
        return [{"n": len(files)}]

    monkeypatch.setattr(each, "LOOKAHEAD", 5)
    project = Project(assets=[files, sparse])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["sparse"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert calls == [([f"k0{i}" for i in range(5)], False), ([f"k0{i}" for i in range(5, 10)], False)]
    assert state.model.partition("sparse", "")["caught_up"] is True  # the empty rest, skipped


async def test_an_unchanged_keyed_write_still_applies_its_migrations(state):
    """§4: schema is not data. A migration added to a keyed output a run
    writes nothing to is applied all the same, and the head's handle says
    so: the content, its version, stays."""
    from solera.sdk import Migration

    store = MigratingStore()
    rows = {"v": [{"id": "a", "v": 1}]}

    def project_with(migrations):
        @asset(outputs=Output("files", key="id", store="mig", migrations=migrations))
        def files():
            return rows["v"]

        return Project(assets=[files], stores={"mig": store})

    engine = make_engine(state, project_with([]))
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    before = head(state, "files")
    rows["v"] = Patch([])  # nothing written
    engine2 = make_engine(state, project_with([Migration("m1", lambda o, p: None)]))
    await engine2.initialize()
    detail = await drive(engine2, await engine2.submit(["files"]))
    assert task_statuses(detail)["files"] == "succeeded"
    assert store.calls == ["files"]
    after = head(state, "files")
    assert after["ref"]["handle"]["schema"] == "m1"
    assert (
        after["ref"]["generation"] == before["ref"]["generation"]
        and after["commit_number"] == before["commit_number"]
    )
    await drive(engine2, await engine2.submit(["files"]))
    assert store.calls == ["files"]  # applied, and known to be: not asked again
