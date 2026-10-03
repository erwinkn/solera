"""Regression tests for the per-key v1 review (thr_9ezn6cyar5): the
boundaries between Each, cancellation, full passes, retries,
configuration, patterns and aliases."""

import asyncio

from solera import Failed, Rejected, Transient
from solera.failed_keys import CANCELED
from solera.sdk import Each, Incremental, Output, Project, asset
from solera_server.state import State

from .engines import drive, make_engine, task_statuses
from .test_each import files_project, records, rows_of
from .test_fence import engine_for, until


class Bad(Rejected):
    pass


async def test_1_a_cancel_interrupts_keys_still_waiting_for_a_slot(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2, "c": 3, "d": 4}

    async def parse(ctx, file: int):
        if ctx.key != "a":
            started.set()
            await release.wait()
        return [{"n": file}]

    parse = asset(parse, inputs={"file": Each("files", concurrency=1)}, outputs=Output("rows", key="path"))
    project = Project(assets=[files, parse])
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(opened, project, placement="inline", heartbeat_seconds=0.2, cancel_grace=30)
    await engine.initialize()
    await engine.run_until((await engine.submit(["files"]))["id"], 10)
    run = await engine.submit(["parse"])
    await until(engine, started.is_set)
    await engine.cancel(run["id"])
    detail = await engine.run_until(run["id"], 15)
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    assert attempt["keys"] == {"ok": 1, "canceled": 3}  # c and d too, never started
    assert {k: r.outcome for k, r in (await records(engine, "parse")).items()} == dict.fromkeys(
        "bcd", CANCELED
    )
    release.set()
    engine.retry_keys("parse", ["canceled"])
    detail = await engine.run_until((await engine.submit(["parse"]))["id"], 10)
    assert set(await rows_of(engine, project, "rows")) == set("abcd")
    await engine.stop()
    await opened.close()


async def test_2_a_full_run_keeps_a_failing_keys_last_good_output(state):
    broken = {"b": False}

    def parse(ctx, file: dict):
        if broken.get(ctx.key):
            raise ValueError("broken now")
        return [{"n": file["n"]}]

    project = files_project({"a": {"n": 1}, "b": {"n": 2}}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    broken["b"] = True
    detail = await drive(engine, await engine.submit(["parse"], mode="full"))
    assert task_statuses(detail)["parse"] == "succeeded"
    got = await rows_of(engine, project, "samples")
    assert {k: [r["n"] for r in v] for k, v in got.items()} == {"a": [1], "b": [2]}


async def test_2_a_full_run_removes_what_upstream_no_longer_has(state):
    content = {"a": {"n": 1}, "b": {"n": 2}}

    def parse(file: dict):
        return [{"n": file["n"]}]

    project = files_project(content, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    # The delta that deleted b is lost (the log truncated past it): a full run finds out.
    del content["b"]
    await drive(engine, await engine.submit(["files"]))
    index = engine.m.indexes[("files", "")]
    engine.m.indexes[("files", "")] = index.truncated(index.log[-1][0] + 1)
    await drive(engine, await engine.submit(["parse"], mode="full"))
    assert set(await rows_of(engine, project, "samples")) == {"a"}


async def test_3_retries_run_under_the_scopes_configuration(state):
    from solera.sdk import Automation, Every

    calls = []

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2}

    @asset(
        inputs={"file": Each("files")},
        outputs=Output("rows", key="path"),
        automations=Automation(trigger=Every(3600), config={"factor": 10}),
    )
    def parse(ctx, file: int):
        calls.append((ctx.key, ctx.config.get("factor")))
        if ctx.key == "b" and len([c for c in calls if c[0] == "b"]) == 1:
            raise Transient("busy", retry_after=0.2)
        return [{"n": file * ctx.config.get("factor", 1)}]

    project = Project(assets=[files, parse])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    await drive(engine, await engine.submit(["parse"], config={"factor": 10}))
    for _ in range(100):  # the retry clock, nothing upstream changes
        await engine.tick()
        if len(calls) >= 3 and not engine.m.partition("parse", "")["failures"].get("counts"):
            break
        await asyncio.sleep(0.05)
    assert calls == [("a", 10), ("b", 10), ("b", 10)]  # a not redelivered, b under factor 10
    got = await rows_of(engine, project, "rows")
    assert {k: v[0]["n"] for k, v in got.items()} == {"a": 10, "b": 20}


async def test_4_a_rescope_without_its_log_still_removes_left_out_and_deleted_keys(state):
    content = {"a/1": {"n": 1}, "old/2": {"n": 2}, "gone/3": {"n": 3}}

    def parse(file: dict):
        return [{"n": file["n"]}]

    written = {}
    old = files_project(content, parse, written=written)
    engine = make_engine(state, old)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    del content["gone/3"]
    await drive(engine, await engine.submit(["files"]))
    index = engine.m.indexes[("files", "")]
    engine.m.indexes[("files", "")] = index.truncated(index.log[-1][0] + 1)  # the log is lost
    new = files_project(content, parse, include="a/**", written=written)
    engine = make_engine(state, new)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"]))
    assert detail["request"]["status"] == "succeeded"
    assert set(await rows_of(engine, new, "samples")) == {"a/1"}
    assert "reconcile" not in engine.m.bookmark("parse", "file", "")


async def test_5_a_user_cancel_during_a_timeout_drain_makes_its_keys_canceled(tmp_path, monkeypatch):
    from solera.stores import FileStore

    stored, release = asyncio.Event(), asyncio.Event()
    real = FileStore.store

    async def blocked(self, write, prior, partition):
        if partition.output.name == "rows":
            stored.set()
            await release.wait()
        return await real(self, write, prior, partition)

    monkeypatch.setattr(FileStore, "store", blocked)

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2}

    async def parse(ctx, file: int):
        if ctx.key == "b":
            await asyncio.sleep(60)
        return [{"n": file}]

    parse = asset(parse, inputs={"file": Each("files")}, outputs=Output("rows", key="path"), timeout=0.5)
    project = Project(assets=[files, parse])
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(opened, project, placement="inline", heartbeat_seconds=0.1, cancel_grace=30)
    await engine.initialize()
    await engine.run_until((await engine.submit(["files"]))["id"], 10)
    run = await engine.submit(["parse"])
    await until(engine, stored.is_set)  # the timeout drain is storing `a`
    await engine.cancel(run["id"])
    for _ in range(10):  # the upgraded record reaches the worker with its next beat
        await engine.tick()
        await asyncio.sleep(0.1)
    release.set()
    detail = await engine.run_until(run["id"], 15)
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    result = await opened.attempt_result(run["id"], attempt["id"])
    assert result["cancel"]["reason"] == "user"
    record = (await records(engine, "parse"))["b"]
    assert record.outcome == CANCELED and record.tries == 0 and record.next_at == 0
    await engine.stop()
    await opened.close()


async def test_6_a_forced_retry_during_the_last_retry_page_is_taken(state):
    calls, gate = [], {"event": None}

    async def parse(ctx, file: dict):
        calls.append(ctx.key)
        if gate["event"] is not None and len(calls) == 2:
            await gate["event"].wait()
        raise Bad("still bad")

    project = files_project({"a": {"n": 1}}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert calls == ["a"]
    gate["event"] = asyncio.Event()
    engine.retry_keys("parse", ["rejected"])
    [run] = await engine.submit_retries("parse", [""], "test")
    while len(calls) < 2:
        await engine.tick()
        await asyncio.sleep(0.02)
    engine.retry_keys("parse", ["rejected"])  # arrives while the pass's last batch runs
    assert await engine.submit_retries("parse", [""], "test") == []  # the partition is active
    gate["event"].set()
    await engine.run_until(run["id"], 10)
    assert calls == ["a", "a", "a"]  # the newer request was taken in the same run


async def test_7_a_keys_override_obeys_the_patterns(state):
    seen = []

    def parse(ctx, file: dict):
        seen.append(ctx.key)
        return [{"n": file["n"]}]

    project = files_project({"a/1": {"n": 1}, "excluded/2": {"n": 2}}, parse, include="a/**")
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    seen.clear()
    await drive(engine, await engine.submit(["parse"], keys={"files": {"keys": ["excluded/2", "a/1"]}}))
    assert seen == ["a/1"] and set(await rows_of(engine, project, "samples")) == {"a/1"}


async def test_8_a_renamed_asset_keeps_its_failures(state):
    calls = []

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1}

    def parse(file: int):
        calls.append(file)
        raise Failed("bug")

    one = Project(
        assets=[files, asset(parse, inputs={"file": Each("files")}, outputs=Output("rows", key="path"))]
    )
    engine = make_engine(state, one)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))

    def parsed(file: int):
        calls.append(file)
        return [{"n": file}]

    renamed = asset(
        parsed, inputs={"file": Each("files")}, outputs=Output("rows", key="path"), aliases=["parse"]
    )
    two = Project(assets=[files, renamed], build="a fix")
    engine = make_engine(state, two)
    await engine.initialize()
    assert "failures" in engine.m.partition("parsed", "") and ("@parsed", "") in engine.m.indexes
    await drive(engine, await engine.submit(["parsed"]))
    assert calls == [1, 1] and engine.m.partition("parsed", "")["failures"]["counts"] == {}


async def test_a_full_run_reads_every_key_once_in_batches(state):
    """The inherited blocker: a `full` run resets once, then resumes its pass."""

    seen, plain = [], []

    def parse(ctx, file: dict):
        seen.append(ctx.key)
        return [{"n": file["n"]}]

    project = files_project({"a": {"n": 1}, "b": {"n": 2}, "c": {"n": 3}}, parse, batch_size=1)

    @asset(inputs={"f": Incremental("files", batch_size=1)})
    def consumer(f: dict):
        plain.extend(f)
        return [{"n": len(f)}]

    project = Project(assets=[*project.assets.values(), consumer])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["files"]))
    for target in ("parse", "consumer"):
        await drive(engine, await engine.submit([target]))
    seen.clear()
    plain.clear()
    for target in ("parse", "consumer"):
        detail = await drive(engine, await engine.submit([target], mode="full"))
        assert detail["request"]["status"] == "succeeded"
    assert sorted(seen) == ["a", "b", "c"] and sorted(plain) == ["a", "b", "c"]
