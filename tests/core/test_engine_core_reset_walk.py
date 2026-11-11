"""A store move that resets an output while a run of its producer walks its keyed input: the
run's next batch starts the walk over on the new life, which would otherwise hold only the
walk's tail and stay partial with no run left to complete it (the deploys-at-every-await
random test, seed 3: the reset clears what the partition was built under, not the walk of
the run under way)."""

import asyncio

from solera.sdk import In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement

from tests.contract.harness import Solera, rebuild


async def test_a_reset_while_a_run_walks_starts_the_walk_over_on_the_new_life(tmp_path, monkeypatch):
    s = Solera(tmp_path, "core")
    rows = {k: 1 for k in "abcd"}
    hold, waiting, seen = asyncio.Event(), [], []
    hold.set()

    def project(store):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(
            inputs={"files": Incremental(batch_size=1)},
            outputs=Output(key="id", store=store),
            retries=Retry(n=2),
        )
        async def copy(ctx, files: list):
            if ctx.batch["files"].index == 1 and not hold.is_set():
                waiting.append(True)
                await hold.wait()
            return rebuild(ctx.batch["files"], [{"id": r["id"], "v": r["v"]} for r in files])

        @asset(inputs={"copy": In()}, retries=Retry(n=1))
        def report(copy: list):
            seen.append(sorted(r["id"] for r in copy))
            return len(copy)

        stores = {"other": FileStore(s.root / "other")}
        return Project(assets=[files, copy, report], stores=stores, default_store=FileStore(s.root / "data"))

    try:
        await s.deploy(project("default"))
        await s.run("files")
        moved = project("other")
        work = InlinePlacement._work

        def current_worker(placement, stage):
            if s.engine.manifest["deploy"] == moved.manifest["deploy"]:
                placement.project = moved
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        hold.clear()
        run = await s.submit("copy", mode="full")
        await s.until(lambda: waiting)
        s.engine.redeploy(moved.manifest)  # copy moves store: a new life, its edge on files closed
        s.project = moved
        hold.set()
        await s.wait(run, 30)
        assert sorted(await s.rows("copy")) == ["a", "b", "c", "d"]
        await s.run("report", timeout=10)
        assert seen == [["a", "b", "c", "d"]]
    finally:
        await s.close()
