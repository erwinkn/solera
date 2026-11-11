"""Round 4 probe (from the extended random test, seed 801; older code): an attempt of a
producer whose output has no head yet is in flight while a deploy resets the output (a store
move) and a second deploy renames the asset by an alias. `_commit_guards` looks the reset up
under the output's current name, and `_apply_aliases` does not move `reset_at` to it, so the
guard misses the reset: the old life's attempt is not refused."""

import asyncio
import os

import pytest
from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement

from tests.contract.harness import Solera, rebuild

PATH = os.environ.get("TRACKING_PATH", "core")


@pytest.mark.parametrize("kind", ["incremental", "whole"])
@pytest.mark.parametrize(
    "order",
    [
        pytest.param(
            "reset, then rename",
            marks=pytest.mark.xfail(
                strict=True,
                reason="ERW-27: _commit_guards looks reset_at up by the current name; aliases do not move it",
            ),
        ),
        "reset only",
    ],
)
async def test_an_attempt_in_flight_across_a_reset_and_then_a_rename(tmp_path, monkeypatch, kind, order):
    s = Solera(tmp_path, PATH)
    rows = {"a": 1, "b": 1}
    hold, waiting, projects, wrote = asyncio.Event(), asyncio.Event(), {}, []

    def project(name, store):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        if kind == "incremental":

            async def body(ctx, files: list):
                wrote.append(store)
                if not hold.is_set():
                    waiting.set()
                    await hold.wait()
                return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

            inputs = {"files": Incremental(batch_size=10)}
        else:

            async def body(ctx, files: list):
                wrote.append(store)
                if not hold.is_set():
                    waiting.set()
                    await hold.wait()
                return [{"id": r["id"], "v": r["v"]} for r in files]

            inputs = {"files": In()}
        body.__name__ = body.__qualname__ = name
        copy = asset(
            inputs=inputs,
            outputs=Output(key="id", store=store),
            aliases=["copy"] if name != "copy" else [],
            retries=Retry(n=2),
        )(body)
        made = Project(
            assets=[files, copy],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )
        projects[made.manifest["deploy"]] = made
        return made

    try:
        await s.deploy(project("copy", "default"))
        work = InlinePlacement._work

        def current_worker(placement, stage):
            placement.project = projects.get(s.engine.manifest["deploy"], placement.project)
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        await s.run("files")
        run = await s.submit("copy")  # its first run: the output has no head
        await s.until(waiting.is_set)
        p = project("copy", "other")
        s.engine.redeploy(p.manifest)  # the output moves: a new life
        s.project = p
        name = "copy"
        if order == "reset, then rename":
            name = "copy2"
            p = project(name, "other")
            s.engine.redeploy(p.manifest)
            s.project = p
        hold.set()
        detail = await s.wait(run, 20)
        tries = [
            (a["outcome"], a.get("reason"), (a.get("error") or "")[:140])
            for t in detail["attempts"].values()
            for a in t
        ]
        state = {
            "tries": tries,
            "bodies ran under store": wrote,
            "reset_at": {str(k): v for k, v in s.engine.m.reset_at.items()},
        }
        first = tries[0]
        assert first[0] == "failed" and "was reset since this attempt launched" in first[2], state
    finally:
        hold.set()
        await s.close()
