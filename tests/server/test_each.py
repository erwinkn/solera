"""`Each` edges (docs/per-key-processing.md §5–§10): one call per key, by-key
writes, the failure index, retry passes, forced retries, key outcomes."""

import asyncio
import copy

import pytest
from solera import Abort, Rejected, Transient
from solera.failures import FAILED, REJECTED, RETRYING, Record
from solera.keys.index import KeyIndex, key_str
from solera.keys.io import ObjectIO
from solera.sdk import Each, Output, Project, Ref, RegistrationError, Result, Retry, asset
from solera.stores import Patch

from ..conftest import whole
from .test_engine import drive, make_engine, state, status_of, task_statuses  # noqa: F401


class Unprocessable(Rejected):
    pass


async def rows_of(engine, project, output, partition=""):
    head = engine.m.heads[(output, partition)]
    store = project.stores["default"]
    return await store.load(
        Ref.from_json(head["ref"]), dict[str, list], await whole(engine.state, output, partition)
    )


async def records(engine, asset_name, partition=""):
    index = KeyIndex(
        ObjectIO(engine.state.objects), None, engine.m.index(f"@{asset_name}", partition).pinned()
    )
    keys, _, payloads, _ = await index.page(None, 10_000)
    return {key_str(k): Record.decode(p) for k, p in zip(keys, payloads, strict=True)}


def files_project(content, fn, *, written=None, **input):
    """`files`, a producer that writes what changed in `content` since it
    last ran — a `Patch`, as one does to make its consumers reprocess no
    more (docs/versions.md §1) — and `parse`, `fn` over each of its keys.
    Projects of one `content` pass one `written`: what was written so far."""

    written = {} if written is None else written

    @asset(outputs=Output("files", keyed=True))
    def files():
        changed = {k: v for k, v in content.items() if written.get(k) != v}
        gone = [k for k in written if k not in content]
        written.clear()
        written.update(copy.deepcopy(content))
        return Patch(changed, remove=gone)

    parse = asset(fn, inputs={"file": Each("files", **input)}, outputs=Output("samples", key="path"))
    return Project(assets=[files, parse])


async def test_one_call_per_key_many_rows_one_write(state):  # noqa: F811
    """Each key's call returns its rows; the store takes every key's group
    in one write. A file that parses to nothing has no key in the output —
    a key with no rows does not exist — while its outcome says it was
    processed."""

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
    record = engine.m.partition("parse", "")["failures"]
    assert record["counts"] == {"rejected": 1, "failed": 1, "retrying": 1}
    found = await records(engine, "parse")
    assert {k: r.outcome for k, r in found.items()} == {
        "empty.csv": REJECTED,
        "bug.csv": FAILED,
        "slow.csv": RETRYING,
    }
    assert found["bug.csv"].message == "ValueError: unexpected header" and found["bug.csv"].tries == 1
    assert record["due"] == found["slow.csv"].next_at and record["deploy_min"] == engine.m.deploy_number
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
    assert engine.m.partition("parse", "")["failures"]["counts"] == {"retrying": 1}


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
    """A transient key is retried once its `retry_after` has passed — on the
    engine's clock, which the test moves instead of waiting on the wall."""

    import time

    tries, skew = {"n": 0}, {"seconds": 0.0}

    def parse(file: dict):
        tries["n"] += 1
        if tries["n"] == 1:
            raise Transient("busy", retry_after=3600)
        return [{"value": 1}]

    project = files_project({"a": {"text": "1"}}, parse)
    engine = make_engine(state, project, clock=lambda: time.time() + skew["seconds"])
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert tries["n"] == 1  # not due for an hour
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 1
    skew["seconds"] = 7200.0  # two hours on
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2 and set(await rows_of(engine, project, "samples")) == {"a"}
    record = engine.m.partition("parse", "")["failures"]
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
    manifest = {**engine.manifest, "deploy": "next"}
    engine.state.record({"type": "ProjectRegistered", "deploy": "next", "manifest": manifest, "at": 0.0})
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
    assert found["partitions"] == [""]
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 1
    engine.retry_keys("parse", ["rejected"])
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2  # once per request, rejected again
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2
    engine.retry_keys("parse", ["all"])
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 3 and engine.m.partition("parse", "")["failures"]["counts"] == {}


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
    assert ("samples", "") not in engine.m.heads and "failures" not in engine.m.partition("parse", "")


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
            return Result(outputs={"rows": [{"n": file}]})  # `meta` not returned: no change
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
    assert "pass" not in engine.m.bookmark("parse", "file", "")  # past the whole page
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
    """Retry pages walk the failure index `page_size` keys at a time,
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
    record = engine.m.partition("parse", "")["failures"]
    assert record["counts"] == {"failed": 5} and record["deploy_min"] == engine.m.deploy_number

    # A deploy fixes the bug; a new file arrives at the same time.
    broken["on"] = False
    content["new"] = {"n": 9}
    engine.state.record(
        {"type": "ProjectRegistered", "deploy": "fixed", "manifest": dict(engine.manifest), "at": 0.0}
    )
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    task = next(t for t in detail["tasks"] if t["asset"] == "parse")
    kinds = [a["keys"] for a in detail["attempts"][task["id"]]]
    assert sum(k.get("ok", 0) for k in kinds) == 6
    record = engine.m.partition("parse", "")["failures"]
    assert record["counts"] == {} and record["retry"] is None
    assert record["due"] is None and record["deploy_min"] is None  # exact once the pass completed
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
        if tries["n"] == 2 and not engine.m.partition("parse", "")["failures"].get("counts"):
            break
        await asyncio.sleep(0.05)
    assert tries["n"] == 2 and engine.m.partition("parse", "")["failures"]["counts"] == {}


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


async def test_patterns_select_keys_and_a_page_of_none_is_skipped(state):  # noqa: F811
    from solera import Incremental

    content = {"ICP/a.csv": {"n": 1}, "ICP/archive/b.csv": {"n": 2}, "XRF/c.csv": {"n": 3}}
    seen = []

    def parse(ctx, file: dict):
        seen.append(ctx.key)
        return [{"n": file["n"]}]

    project = files_project(content, parse, include="ICP/**", exclude={"archive": "**/archive/**"})
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert seen == ["ICP/a.csv"] and set(await rows_of(engine, project, "samples")) == {"ICP/a.csv"}
    # A change the patterns leave out: the page is read, nothing is called, the task skips.
    content["XRF/c.csv"] = {"n": 4}
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert seen == ["ICP/a.csv"] and task_statuses(detail)["parse"] == "skipped"

    # The same on a plain Incremental edge.
    @asset(outputs=Output("files2", keyed=True))
    def files2():
        return Patch({k: content[k] for k in written2.pop("keys", content)})

    written2 = {}

    calls = []

    @asset(inputs={"f": Incremental("files2", include="XRF/**")})
    def plain(f: dict):
        calls.append(sorted(f))
        return [{"n": len(f)}]

    project = Project(assets=[files2, plain])
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["plain"], upstream=True))
    content["ICP/a.csv"] = {"n": 9}
    written2["keys"] = ["ICP/a.csv"]  # only the key that changed
    detail = await drive(engine, await engine.submit(["plain"], upstream=True))
    assert calls == [["XRF/c.csv"]] and task_statuses(detail)["plain"] == "skipped"


async def test_a_pattern_change_cuts_over(state):  # noqa: F811
    """§11: changes up to the cutover finish under the old patterns — so a
    pending deletion of a newly excluded key still removes its rows — then
    membership is diffed against the snapshot at the cutover, then deltas
    continue under the new patterns."""

    content = {"a/1.csv": {"n": 1}, "archive/2.csv": {"n": 2}, "b/3.csv": {"n": 3}}
    seen = []

    def parse(ctx, file: dict):
        seen.append(ctx.key)
        return [{"n": file["n"]}]

    written = {}
    old = files_project(content, parse, include=["a/**", "archive/**"], batch_size=1, written=written)
    engine = make_engine(state, old)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert set(await rows_of(engine, old, "samples")) == {"a/1.csv", "archive/2.csv"}
    # Upstream deletes archive/2.csv (not consumed yet), and b/4.csv appears.
    del content["archive/2.csv"]
    content["b/4.csv"] = {"n": 4}
    await drive(engine, await engine.submit(["files"]))
    # A deploy: archive excluded, b included.
    new = files_project(
        content,
        parse,
        include=["a/**", "b/**"],
        exclude={"archive": "archive/**"},
        batch_size=1,
        written=written,
    )
    engine = make_engine(state, new)
    await engine.initialize()
    seen.clear()
    detail = await drive(engine, await engine.submit(["parse"]))
    assert status_of(detail) == "succeeded"
    assert set(await rows_of(engine, new, "samples")) == {"a/1.csv", "b/3.csv", "b/4.csv"}
    assert sorted(seen) == ["b/3.csv", "b/4.csv"]  # a/1.csv matched both times: not reprocessed
    wm = engine.m.bookmark("parse", "file", "")
    assert (
        "pattern_change" not in wm
        and wm["patterns"] == new.manifest["assets"]["parse"]["inputs"]["file"]["patterns"]
    )
    # From here on, deltas under the new patterns.
    content["b/5.csv"] = {"n": 5}
    content["archive/6.csv"] = {"n": 6}
    seen.clear()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert seen == ["b/5.csv"]


async def test_a_rescope_pins_its_snapshot_between_attempts(state):  # noqa: F811
    """The snapshot a rescope diffs is read across attempts: index files it
    names stay until the transition ends, even with no attempt running."""

    def parse(file: dict):
        return []

    engine = make_engine(state, files_project({}, parse))
    await engine.initialize()
    path = "keys/files/_/old.kx"
    await state.put_object(path, b"x")
    engine.m.garbage.append([path, engine.m.applied + 5])  # let go of after the pin below
    engine.m._partition("parse", "")["bookmarks"] = {
        "file": {
            "kind": "keys",
            "output": "files",
            "upstream_partition": "",
            "next": 3,
            "pattern_change": {"pin": engine.m.applied, "at": 2},
            "pass": {"mode": "diff", "at": "k", "page": 1, "pages": 2},
        }
    }
    await engine.upkeep.collect()
    assert await state.get_object(path) is not None
    del engine.m.partitions[("parse", "")]["bookmarks"]
    await engine.upkeep.collect()
    assert await state.get_object(path) is None


async def test_none_is_no_change_and_removal_is_explicit(state):  # noqa: F811
    """D7: an output an Each call returns as None (or omits) keeps its
    previous content for that key; `Patch(None, remove=[ctx.key])` removes
    the key; a Patch for anything else fails the call."""

    from solera.stores import Patch

    content = {"a": {"v": 1}, "b": {"v": 1}, "c": {"v": 1}}
    mode = {"a": "rows", "b": "rows", "c": "rows"}

    def parse(ctx, file: dict):
        if mode[ctx.key] == "none":
            return None
        if mode[ctx.key] == "remove":
            return Patch(None, remove=[ctx.key])
        if mode[ctx.key] == "other":
            return Patch(None, remove=["a"])
        return [{"v": file["v"]}]

    project = files_project(content, parse)
    engine = make_engine(state, project)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    assert set(await rows_of(engine, project, "samples")) == {"a", "b", "c"}
    for key in content:
        content[key] = {"v": 2}
    mode.update(a="none", b="remove", c="other")
    await drive(engine, await engine.submit(["parse"], upstream=True))
    got = await rows_of(engine, project, "samples")
    assert {k: v[0]["v"] for k, v in got.items()} == {"a": 1, "c": 1}  # a unchanged, b removed, c kept
    assert (await records(engine, "parse"))["c"].outcome == FAILED


async def test_a_last_page_that_writes_nothing_still_completes_the_scope(state):  # noqa: F811
    """Review round 3 (system B1): one key a page, `a` writes rows, `b`
    fails — so the last page writes no output. The delivery drained all the
    same: the scope is complete, kept out of `missing`, and its head is
    unchanged — the version and provenance `a`'s page installed."""

    def parse(ctx, file: dict):
        if ctx.key == "b.csv":
            raise ValueError("unparseable")
        return [{"value": 1}]

    project = files_project({"a.csv": {"text": "1"}, "b.csv": {"text": "2"}}, parse, batch_size=1)
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["parse"], upstream=True))
    assert status_of(detail) == "succeeded"
    written = engine.m.heads[("samples", "")]
    task = next(t for t in detail["tasks"] if t["asset"] == "parse")
    assert written["attempt"] == detail["attempts"][task["id"]][0]["id"]  # a's page wrote it
    assert engine.m.partition("parse", "")["drained"] is True
    planner = engine.planner()
    assert planner.complete("parse", "") and planner.partitions("parse", "missing") == []
    assert engine.head_view(written)["complete"] is True
    assert (await records(engine, "parse"))["b.csv"].outcome == FAILED
