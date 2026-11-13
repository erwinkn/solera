"""A task's walk cursor names the edge it walked (review round 3): a cursor from before a
reset of the reader's own output starts over, even after another run committed on the new
life (Sol); and a store move of one output of a two-output asset makes the next run clear the
sibling too, since the clear decision reads the outputs' own heads (Fable; today's path still
keeps the key its upstream removed, ERW-27)."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.fixture
async def solera(tmp_path):
    s = Solera(tmp_path)
    yield s
    await s.close()


async def test_another_runs_commit_does_not_validate_a_pre_reset_task_cursor(tmp_path, monkeypatch):
    s = Solera(tmp_path)
    entered = asyncio.Event()

    def project(store):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k} for k in "abcd"]

        @asset(
            inputs={"files": Incremental(batch_size=1)},
            outputs=Output("copy", key="id", store=store),
            retries=Retry(n=2, delay=0.1),
        )
        async def copy(ctx, files: list):
            b = ctx.batch["files"]
            return rebuild(b, files)

        return Project(
            assets=[files, copy],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )

    try:
        await s.deploy(project("default"))
        await s.run("files")
        old = await s.submit("copy")
        record = s.state.record

        def pause_after_second_batch(*events, **kw):
            result = record(*events, **kw)
            if not entered.is_set() and any(
                e.get("type") == "AttemptFinished"
                and ((e.get("commit") or {}).get("progress") or {}).get("(core)", {}).get("key") == "b"
                for e in events
            ):
                s.engine._control(old["id"], "pause", "test")
                entered.set()
            return result

        monkeypatch.setattr(s.state, "record", pause_after_second_batch)
        await s.until(entered.is_set)
        moved = project("other")
        work = InlinePlacement._work

        def current_worker(placement, stage):
            if s.engine.manifest["deploy"] == moved.manifest["deploy"]:
                placement.project = moved
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        s.engine.redeploy(moved.manifest)
        s.project = moved
        await s.run("copy", keys={"files": ["a"]})
        await s.engine.pause(old["id"], paused=False)
        detail = await s.wait(old)
        state = {
            "status": detail["request"]["status"],
            "rows": sorted(await s.rows("copy")),
            "walking": sorted(s.engine.m.walking()),
            "waiting": full.waiting(s.engine),
            "attempts": detail["attempts"],
        }
        assert state["status"] == "succeeded" and state["rows"] == list("abcd") and not state["waiting"], (
            state
        )
    finally:
        await s.close()


async def test_a_sibling_output_loses_removed_keys_after_one_output_moves(solera):
    rows = {"a": 1, "b": 1, "c": 1}

    def project(x_store):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(
            inputs={"files": Incremental(batch_size=10)},
            outputs=[Output("x", key="id", store=x_store), Output("y", key="id")],
            retries=Retry(n=0),
        )
        async def both(ctx, files: list):
            b = ctx.batch["files"]
            out = [{"id": r["id"], "v": r["v"]} for r in files]
            return {"x": rebuild(b, out), "y": rebuild(b, out)}

        return Project(
            assets=[files, both],
            stores={"other": FileStore(solera.root / "other")},
            default_store=FileStore(solera.root / "data"),
        )

    await solera.deploy(project("default"))
    await solera.run("files")
    await solera.run("both")
    assert sorted(await solera.rows("y")) == ["a", "b", "c"]
    del rows["c"]
    await solera.run("files")  # "c" is gone upstream; both has not run since
    await solera.deploy(project("other"))  # x moves; y stays where it is, holding a, b, c
    await solera.run("both")
    await solera.settle(0.3)
    state = {
        "x": sorted(await solera.rows("x")),
        "y": sorted(await solera.rows("y")),
        "stale": await solera.stale("both"),
    }
    await solera.run("both", expect=None)  # anything owed is taken now
    state["y after another run"] = sorted(await solera.rows("y"))
    assert state["y after another run"] == ["a", "b"], f"y keeps a key removed upstream: {state}"
