"""A partition is complete when no key decodes from the empty base
(`Model.complete`, `observed.complete`): derived from what its inputs
observed, never stored. Its edge cases, each through the engine; and the
history's `materialized` column, which reads the same rule."""

import asyncio

from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore

from .engines import drive, make_engine, status_of


def project(root, rows: dict, *, include=None, each=False, version="1", hold=None):
    """`files` (keyed, `rows` its content) -> `copy`, plain or per-key, a
    key a batch; `hold`, when set, an event every batch past the first
    waits on — for a run canceled midway."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    async def wait(index):
        if hold is not None and index > 0:
            await hold.wait()

    if each:

        @asset(
            inputs={"files": Incremental(batch_size=1, include=include, each=True)},
            outputs=Output("copy", key="id"),
            version=version,
        )
        async def copy(ctx, files: list):
            if hold is not None and ctx.key != min(rows):
                await hold.wait()
            return [{"v": files[0]["v"]}]

    else:

        @asset(
            inputs={"files": Incremental(batch_size=1, include=include)},
            outputs=Output("copy", key="id"),
            version=version,
        )
        async def copy(ctx, files: list):
            await wait(ctx.batch["files"].index)
            return [{"id": r["id"], "v": r["v"]} for r in files]

    return Project(assets=[files, copy], default_store=FileStore(root / "data"))


async def engine_of(state, p):
    engine = make_engine(state, p, cancel_grace=0.2)
    await engine.initialize()
    return engine


async def cancel_midway(engine, state, run):
    """Cancel `run` once its task has committed a batch."""

    for _ in range(3000):
        await engine.tick()
        tasks = state.model.runs[run["id"]]["tasks"].values()
        if any(t.get("progress") for t in tasks if t["asset"] == "copy"):
            break
        await asyncio.sleep(0.01)
    await engine.cancel(run["id"])
    return await drive(engine, run)


async def test_never_run_is_not_complete_and_a_first_run_is(state, tmp_path):
    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1}))
    await drive(engine, await engine.submit(["files"]))
    assert not await engine.complete("copy", "")
    await drive(engine, await engine.submit(["copy"]))
    assert await engine.complete("copy", "")


async def test_an_empty_upstream_or_patterns_taking_no_key_complete_at_once(state, tmp_path):
    engine = await engine_of(state, project(tmp_path, {}))
    await drive(engine, await engine.submit(["copy"], upstream=True))
    assert await engine.complete("copy", "")
    await engine.stop()
    engine = await engine_of(state, project(tmp_path, {"a": 1}, include=["z*"]))
    await drive(engine, await engine.submit(["copy"], upstream=True))
    assert await engine.complete("copy", "")


async def test_deletions_keep_it_complete(state, tmp_path):
    rows = {"a": 1, "b": 1}
    engine = await engine_of(state, project(tmp_path, rows))
    await drive(engine, await engine.submit(["copy"], upstream=True))
    rows.pop("a")
    await drive(engine, await engine.submit(["copy"], upstream=True))
    assert await engine.complete("copy", "")


async def test_a_keys_run_on_a_never_run_partition_is_not_complete(state, tmp_path):
    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1}))
    await drive(engine, await engine.submit(["files"]))
    await drive(engine, await engine.submit(["copy"], keys={"files": ["a"]}))
    assert not await engine.complete("copy", "")  # points over the empty base
    await drive(engine, await engine.submit(["copy"]))
    assert await engine.complete("copy", "")


async def test_an_incremental_run_canceled_midway_is_complete_and_stale(state, tmp_path):
    hold, rows = asyncio.Event(), {"a": 1, "b": 1, "c": 1}
    engine = await engine_of(state, project(tmp_path, rows, hold=hold))
    hold.set()
    await drive(engine, await engine.submit(["copy"], upstream=True))
    hold.clear()
    rows.update({"a": 2, "b": 2, "c": 2})
    await drive(engine, await engine.submit(["files"]))
    detail = await cancel_midway(engine, state, await engine.submit(["copy"]))
    assert status_of(detail) == "canceled"
    assert await engine.complete("copy", "")  # its base is still a commit: complete, just stale
    assert await engine.stale_reasons("copy", "") == ["input changed"]
    assert engine.planner().materialized("copy", "")
    hold.set()


async def test_a_full_run_canceled_midway_is_not_complete(state, tmp_path):
    hold = asyncio.Event()
    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1, "c": 1}, hold=hold))
    hold.set()
    await drive(engine, await engine.submit(["copy"], upstream=True))
    hold.clear()
    detail = await cancel_midway(engine, state, await engine.submit(["copy"], mode="full"))
    assert status_of(detail) == "canceled"
    assert not await engine.complete("copy", "")  # started over: past its first batch, the empty base
    assert not engine.planner().materialized("copy", "")
    hold.set()


async def test_a_per_key_full_run_canceled_midway_is_complete(state, tmp_path):
    """A per-key consumer's full run compares against what it holds: its
    base is its own keys, so it is complete throughout — its outputs stay
    readable while it runs."""

    hold, rows = asyncio.Event(), {"a": 1, "b": 1, "c": 1}
    hold.set()
    engine = await engine_of(state, project(tmp_path, rows, each=True, hold=hold))
    await drive(engine, await engine.submit(["copy"], upstream=True))
    await engine.stop()
    hold = asyncio.Event()
    engine = await engine_of(state, project(tmp_path, rows, each=True, version="2", hold=hold))
    detail = await cancel_midway(engine, state, await engine.submit(["copy"]))
    assert status_of(detail) == "canceled"
    assert await engine.complete("copy", "")
    hold.set()


async def test_two_keyed_inputs_are_complete_together(state, tmp_path):
    """Every incremental input must be complete: one walked to the end and
    the other not leaves the partition incomplete."""

    @asset(outputs=Output("left", key="id"))
    def left():
        return [{"id": "a"}]

    @asset(outputs=Output("right", key="id"))
    def right():
        return [{"id": "b"}]

    @asset(
        inputs={"lefts": Incremental("left"), "rights": Incremental("right")},
        outputs=Output("both", key="id"),
    )
    def both(ctx, lefts: list, rights: list):
        return [*lefts, *rights]

    engine = await engine_of(state, Project(assets=[left, right, both], default_store=FileStore(tmp_path)))
    await drive(engine, await engine.submit(["both"], upstream=True))
    assert await engine.complete("both", "")
    record = state.model.partitions[("both", "")]["observed"]["rights"]
    record["base"] = {**record["base"], "endpoint": None}  # as a full run's first batch leaves it
    assert not await engine.complete("both", "")


async def test_a_keys_run_naming_every_upstream_key_is_complete(state, tmp_path):
    """A keys= run on a never-run partition that names every key upstream
    observes them all, as points: no key present upstream decodes from the
    empty base, so it is complete — and owes nothing, so it is not stale."""

    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1}))
    await drive(engine, await engine.submit(["files"]))
    await drive(engine, await engine.submit(["copy"], keys={"files": ["a", "b", "z"]}))
    assert await engine.complete("copy", "") and await engine.stale_reasons("copy", "") == []


async def test_a_partial_full_run_whose_rest_is_deleted_upstream_is_complete(state, tmp_path):
    hold, rows = asyncio.Event(), {"a": 1, "b": 1, "c": 1}
    engine = await engine_of(state, project(tmp_path, rows, hold=hold))
    hold.set()
    await drive(engine, await engine.submit(["copy"], upstream=True))
    hold.clear()
    await cancel_midway(engine, state, await engine.submit(["copy"], mode="full"))
    assert not await engine.complete("copy", "")  # b and c, upstream, decode from the empty base
    rows.pop("b"), rows.pop("c")
    await drive(engine, await engine.submit(["files"]))
    assert await engine.complete("copy", "")  # what is left of the gap holds nothing upstream
    hold.set()


async def test_each_commit_logs_whether_it_was_its_runs_last_batch(state, tmp_path):
    """The history's commit rows record `final` (as `ctx.batch.final`), not
    completeness, which is live: a run of three batches logs false, false,
    true; a canceled run logs no final commit."""

    hold = asyncio.Event()
    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1, "c": 1}, hold=hold))
    hold.set()
    run = await engine.submit(["copy"], upstream=True)
    await drive(engine, run)
    rows = [r for r in (await engine.history.commits(outputs=["copy"]))["commits"] if r["run"] == run["id"]]
    assert [r["final"] for r in sorted(rows, key=lambda r: r["generation"])] == [False, False, True]
    hold.clear()
    run = await engine.submit(["copy"], mode="full")
    await cancel_midway(engine, state, run)
    rows = [r for r in (await engine.history.commits(outputs=["copy"]))["commits"] if r["run"] == run["id"]]
    assert rows and not any(r["final"] for r in rows)
    hold.set()


async def test_each_commit_row_names_the_batch_that_made_it(state, tmp_path):
    """`batch` on a history commit row is `{index, count}` of the attempt
    that committed it, read from that attempt's row: null for an unbatched
    task."""

    engine = await engine_of(state, project(tmp_path, {"a": 1, "b": 1, "c": 1}))
    run = await engine.submit(["copy"], upstream=True)
    await drive(engine, run)
    rows = (await engine.history.commits(outputs=["files", "copy"]))["commits"]
    copies = sorted((r for r in rows if r["output"] == "copy"), key=lambda r: r["generation"])
    assert [r["batch"] for r in copies] == [{"index": i, "count": 3} for i in range(3)]
    assert [r["batch"] for r in rows if r["output"] == "files"] == [None]
