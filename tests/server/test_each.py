"""`Each` edges (docs/per-key-processing.md §5–§10): one call per key, by-key
writes, the failure index, retry passes, forced retries, key outcomes."""

import asyncio

import pytest
from solera import Abort, Rejected, Transient
from solera.failures import FAILED, REJECTED, RETRYING, Record
from solera.keys.index import KeyIndex, key_str
from solera.keys.io import ObjectIO
from solera.sdk import Each, Output, Project, Ref, RegistrationError, Result, Retry, asset

from ..conftest import whole
from .test_engine import drive, make_engine, state, status_of, task_statuses  # noqa: F401


class Unprocessable(Rejected):
    pass


async def rows_of(engine, project, output, scope=""):
    head = engine.m.heads[(output, scope)]
    store = project.stores["default"]
    return await store.load(
        Ref.from_json(head["ref"]), dict[str, list], await whole(engine.state, output, scope)
    )


async def records(engine, asset_name, scope=""):
    index = KeyIndex(ObjectIO(engine.state.objects), None, engine.m.index(f"@{asset_name}", scope).pinned())
    keys, versions, _, _ = await index.page(None, 10_000)
    return {key_str(k): Record.decode(v) for k, v in zip(keys, versions, strict=True)}


def files_project(content, fn, **edge):
    @asset(outputs=Output("files", keyed=True))
    def files():
        return dict(content)

    parse = asset(fn, inputs={"file": Each("files", **edge)}, outputs=Output("samples", key="path"))
    return Project(assets=[files, parse])


async def test_one_call_per_key_many_rows_one_write(state):  # noqa: F811
    """Each key's call returns its rows; the store takes every key's group
    in one write; a file that parses to nothing is a live, empty key."""

    content = {"a.csv": "1,2", "b.csv": "3", "c.csv": ""}
    calls = []

    def parse(ctx, file: dict):
        calls.append(ctx.key)
        return [{"value": int(v)} for v in file["text"].split(",") if v]

    project = files_project({k: {"text": v} for k, v in content.items()}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert status_of(detail) == "succeeded" and sorted(calls) == ["a.csv", "b.csv", "c.csv"]
    got = await rows_of(engine, project, "samples")
    assert {k: sorted(r["value"] for r in v) for k, v in got.items()} == {
        "a.csv": [1, 2],
        "b.csv": [3],
        "c.csv": [],
    }
    assert all(r["path"] == k for k, v in got.items() for r in v)  # the store stamps the key
    attempt = detail["attempts"][next(t["id"] for t in detail["tasks"] if t["asset"] == "parse")][-1]
    assert attempt["keys"] == {"ok": 3}


async def test_failures_are_recorded_and_never_block(state):  # noqa: F811
    content = {
        "good.csv": {"text": "1"},
        "empty.csv": {"text": "reject"},
        "bug.csv": {"text": "bug"},
        "slow.csv": {"text": "slow"},
    }

    def parse(file: dict):
        if file["text"] == "reject":
            raise Unprocessable("empty file")
        if file["text"] == "bug":
            raise ValueError("unexpected header")
        if file["text"] == "slow":
            raise Transient("429", retry_after=3600)
        return [{"value": int(file["text"])}]

    project = files_project(content, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert status_of(detail) == "succeeded"
    assert set(await rows_of(engine, project, "samples")) == {"good.csv"}
    record = engine.m.failures[("parse", "")]
    assert record["counts"] == {"rejected": 1, "failed": 1, "retrying": 1}
    found = await records(engine, "parse")
    assert {k: r.outcome for k, r in found.items()} == {
        "empty.csv": REJECTED,
        "bug.csv": FAILED,
        "slow.csv": RETRYING,
    }
    assert found["bug.csv"].message == "ValueError: unexpected header" and found["bug.csv"].tries == 1
    assert record["due"] == found["slow.csv"].next_at and record["epoch_min"] == engine.m.epoch
    outcomes = await engine.history.query(
        lambda con: con.execute("SELECT key, outcome FROM key_outcomes ORDER BY key").fetchall(),
        ("key_outcomes",),
    )
    assert outcomes == [
        ("bug.csv", "failed"),
        ("empty.csv", "rejected"),
        ("good.csv", "ok"),
        ("slow.csv", "retrying"),
    ]

    # A changed or removed key comes back through the change window: its record goes.
    content["empty.csv"] = {"text": "2"}
    del content["bug.csv"]
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert set(await rows_of(engine, project, "samples")) == {"good.csv", "empty.csv"}
    assert set(await records(engine, "parse")) == {"slow.csv"}
    assert engine.m.failures[("parse", "")]["counts"] == {"retrying": 1}


async def test_removed_keys_lose_their_rows(state):  # noqa: F811
    content = {"a": {"text": "1"}, "b": {"text": "2"}}

    def parse(file: dict):
        return [{"value": int(file["text"])}]

    project = files_project(content, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    del content["a"]
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert set(await rows_of(engine, project, "samples")) == {"b"}


async def test_transient_key_retried_when_due(state):  # noqa: F811
    tries = {"n": 0}

    def parse(file: dict):
        tries["n"] += 1
        if tries["n"] == 1:
            raise Transient("busy", retry_after=0)
        return [{"value": 1}]

    project = files_project({"a": {"text": "1"}}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    # The page left `a` due at once: the same run took it in a retry page.
    assert tries["n"] == 2 and set(await rows_of(engine, project, "samples")) == {"a"}
    record = engine.m.failures[("parse", "")]
    assert record["counts"] == {} and record["due"] is None and record["retry"] is None
    assert record["passes"] == 1


async def test_failed_keys_get_one_try_per_deploy(state):  # noqa: F811
    tries = {"n": 0}

    def parse(file: dict):
        tries["n"] += 1
        raise ValueError("still broken")

    project = files_project({"a": {"text": "1"}}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert tries["n"] == 1
    detail = await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 1 and task_statuses(detail)["parse"] == "skipped"
    # A new revision: one more try, and only one.
    manifest = {**engine.manifest, "revision": "next"}
    engine.state.record({"type": "ProjectRegistered", "revision": "next", "manifest": manifest, "at": 0.0})
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2
    assert (await records(engine, "parse"))["a"].tries == 2


async def test_forced_retry_takes_each_key_once(state):  # noqa: F811
    tries = {"n": 0}

    def parse(file: dict):
        tries["n"] += 1
        if tries["n"] < 3:
            raise Unprocessable("bad")
        return [{"value": 1}]

    project = files_project({"a": {"text": "1"}}, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    with pytest.raises(ValueError):
        engine.retry_keys("files", ["failed"])
    with pytest.raises(ValueError):
        engine.retry_keys("parse", ["broken"])
    found = engine.retry_keys("parse", ["failed"])  # no rejected key is retried for this
    assert found["scopes"] == [""]
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 1
    engine.retry_keys("parse", ["rejected"])
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2  # once per request, rejected again
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2
    engine.retry_keys("parse", ["all"])
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 3 and engine.m.failures[("parse", "")]["counts"] == {}


async def test_abort_fails_the_attempt_and_commits_nothing(state):  # noqa: F811
    def parse(file: dict):
        if file["text"] == "x":
            raise Abort("credentials expired")
        return [{"value": 1}]

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": {"text": "1"}, "b": {"text": "x"}}

    parse = asset(
        parse, inputs={"file": Each("files")}, outputs=Output("samples", key="path"), retries=Retry(0)
    )
    project = Project(assets=[files, parse])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert task_statuses(detail)["parse"] == "failed"
    assert ("samples", "") not in engine.m.heads and ("parse", "") not in engine.m.failures


async def test_concurrency_and_batches(state):  # noqa: F811
    live = {"now": 0, "max": 0}
    pages = []

    async def parse(ctx, file: dict):
        live["now"] += 1
        live["max"] = max(live["max"], live["now"])
        await asyncio.sleep(0.02)
        live["now"] -= 1
        pages.append(ctx.attempt if hasattr(ctx, "attempt") else ctx.run_id)
        return [{"value": file["n"]}]

    project = files_project({f"k{i}": {"n": i} for i in range(7)}, parse, batch_size=3, concurrency=2)
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert status_of(detail) == "succeeded" and live["max"] == 2
    task = next(t for t in detail["tasks"] if t["asset"] == "parse")
    assert len(detail["attempts"][task["id"]]) == 3  # 3 + 3 + 1 keys
    assert len(await rows_of(engine, project, "samples")) == 7


async def test_multi_output_result_and_keyed_values(state):  # noqa: F811
    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2}

    @asset(
        inputs={"file": Each("files")},
        outputs=(Output("rows", key="path"), Output("meta", keyed=True)),
    )
    def parse(ctx, file: int):
        if ctx.key == "b":
            return Result(outputs={"rows": [{"n": file}]})  # holds nothing in `meta`
        return Result(outputs={"rows": [{"n": file}], "meta": {"seen": file}})

    project = Project(assets=[files, parse])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    meta = await project.stores["default"].load(
        Ref.from_json(engine.m.heads[("meta", "")]["ref"]), dict, await whole(engine.state, "meta")
    )
    assert meta == {"a": {"seen": 1}}
    assert set(await rows_of(engine, project, "rows")) == {"a", "b"}


def test_registration():
    @asset(outputs=Output("files", keyed=True))
    def files():
        return {}

    @asset(outputs=Output("log", incremental=True))
    def log():
        return []

    def parse(file: dict):
        return []

    with pytest.raises(RegistrationError, match="must be keyed"):
        Project(assets=[files, asset(parse, inputs={"file": Each("files")}, outputs=Output("x"))])
    with pytest.raises(RegistrationError, match="keyed upstream"):
        Project(assets=[log, asset(parse, inputs={"file": Each("log")}, outputs=Output("x", key="k"))])
    with pytest.raises(RegistrationError, match="concurrency"):
        Each("files", concurrency=0)


async def test_a_user_cancel_commits_finished_keys_and_leaves_the_rest_dormant(tmp_path):
    """Cancel requested: no key starts, the calls in flight are cancelled, the
    keys that finished commit with the interrupted ones' records and the
    watermark past the whole page (§5); canceled keys never come due by
    themselves (§9)."""

    from solera.failures import CANCELED
    from solera_server.state import State

    from .test_fence import engine_for, until

    started, release = asyncio.Event(), asyncio.Event()

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2, "c": 3}

    async def parse(ctx, file: int):
        if ctx.key == "a":
            return [{"n": file}]
        started.set()
        await release.wait()  # b and c hang until canceled
        return [{"n": file}]

    parse = asset(parse, inputs={"file": Each("files", concurrency=3)}, outputs=Output("rows", key="path"))
    project = Project(assets=[files, parse])
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(opened, project, placement="inline", heartbeat_seconds=0.2, cancel_grace=30)
    await engine.initialize()
    await engine.run_until((await engine.submit(["files"]))["id"], 10)
    run = await engine.submit(["parse"])
    await until(engine, started.is_set)
    await engine.cancel(run["id"])
    detail = await engine.run_until(run["id"], 15)
    assert detail["request"]["status"] == "canceled"
    [attempt] = detail["attempts"][detail["tasks"][0]["id"]]
    assert attempt["status"] == "canceled" and attempt["keys"] == {"ok": 1, "canceled": 2}
    assert set(await rows_of(engine, project, "rows")) == {"a"}
    found = await records(engine, "parse")
    assert {k: r.outcome for k, r in found.items()} == {"b": CANCELED, "c": CANCELED}
    assert engine.m.watermarks[("parse", "file", "")]["after"] is None  # past the whole page
    # Dormant: a later run finds nothing to do.
    release.set()
    detail = await engine.run_until((await engine.submit(["parse"]))["id"], 10)
    assert detail["tasks"][0]["status"] == "skipped"
    # Until someone asks.
    engine.retry_keys("parse", ["canceled"])
    detail = await engine.run_until((await engine.submit(["parse"]))["id"], 10)
    assert detail["request"]["status"] == "succeeded"
    assert set(await rows_of(engine, project, "rows")) == {"a", "b", "c"}
    assert await records(engine, "parse") == {}
    await engine.stop()
    await opened.close()


async def test_a_retry_pass_spans_pages_and_accumulates_its_bounds(state):  # noqa: F811
    """Retry pages walk the failure index `batch_size` keys at a time,
    alternating with change pages; the pass's accumulators become the exact
    bounds when it completes (§9)."""

    content = {f"k{i}": {"n": i} for i in range(5)}
    broken = {"on": True}

    def parse(file: dict):
        if broken["on"]:
            raise ValueError("bug")
        return [{"n": file["n"]}]

    project = files_project(content, parse, batch_size=2)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    record = engine.m.failures[("parse", "")]
    assert record["counts"] == {"failed": 5} and record["epoch_min"] == engine.m.epoch

    # A deploy fixes the bug; a new file arrives at the same time.
    broken["on"] = False
    content["new"] = {"n": 9}
    engine.state.record(
        {"type": "ProjectRegistered", "revision": "fixed", "manifest": dict(engine.manifest), "at": 0.0}
    )
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    task = next(t for t in detail["tasks"] if t["asset"] == "parse")
    kinds = [a["keys"] for a in detail["attempts"][task["id"]]]
    assert sum(k.get("ok", 0) for k in kinds) == 6
    record = engine.m.failures[("parse", "")]
    assert record["counts"] == {} and record["retry"] is None
    assert record["due"] is None and record["epoch_min"] is None  # exact once the pass completed
    assert len(await rows_of(engine, project, "samples")) == 6


async def test_the_retry_clock_runs_automated_assets(state):  # noqa: F811
    from solera.sdk import Automation, Every

    tries = {"n": 0}

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1}

    @asset(
        inputs={"file": Each("files")},
        outputs=Output("rows", key="path"),
        automations=Automation(trigger=Every(3600)),
    )
    def parse(file: int):
        tries["n"] += 1
        if tries["n"] == 1:
            raise Transient("busy", retry_after=0.3)
        return [{"n": file}]

    project = Project(assets=[files, parse])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert tries["n"] == 1
    for _ in range(100):  # nothing upstream changes: the clock alone brings it back
        await engine.tick()
        if tries["n"] == 2 and not engine.m.failures[("parse", "")].get("counts"):
            break
        await asyncio.sleep(0.05)
    assert tries["n"] == 2 and engine.m.failures[("parse", "")]["counts"] == {}


async def test_a_timeout_drain_counts_a_try_and_comes_due(tmp_path):
    from solera.failures import TIMED_OUT
    from solera_server.state import State

    from .test_fence import engine_for

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a": 1, "b": 2}

    async def parse(ctx, file: int):
        if ctx.key == "b":
            await asyncio.sleep(30)
        return [{"n": file}]

    parse = asset(
        parse,
        inputs={"file": Each("files")},
        outputs=Output("rows", key="path"),
        timeout=0.5,
        retries=Retry(2, delay=0),
    )
    project = Project(assets=[files, parse])
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = engine_for(opened, project, placement="inline", heartbeat_seconds=0.2, cancel_grace=30)
    await engine.initialize()
    await engine.run_until((await engine.submit(["files"]))["id"], 10)
    detail = await engine.run_until((await engine.submit(["parse"]))["id"], 15)
    attempt, *rest = detail["attempts"][detail["tasks"][0]["id"]]
    assert attempt["status"] == "failed" and attempt["keys"] == {"ok": 1, "timed_out": 1}
    assert [a["status"] for a in rest] == ["skipped"]  # the retry finds nothing due yet
    assert set(await rows_of(engine, project, "rows")) == {"a"}
    record = (await records(engine, "parse"))["b"]
    assert record.outcome == TIMED_OUT and record.tries == 1 and record.next_at >= record.last + 60
    await engine.stop()
    await opened.close()
