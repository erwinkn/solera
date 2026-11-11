"""Older code than PR 2 that review round 3 (Sol) found under deploys: strict xfails until
ERW-27 fixes them, by giving each persisted source observation its life and taking the deploy
generation at the whole query or tick. A source's load report and a sensor's snapshot carry a
commit number without the source's life; the staleness views keep their inputs across an
await; a sensor tick refuses a deploy only on its success path; the sensor long-poll keeps
the deploy it started under."""

import asyncio

import pytest
from solera.sdk import Commit, Incremental, Output, Project, Source, Tick, asset, sensor
from solera.stores import FileStore
from solera_server.executors.inline import InlinePlacement
from solera_server.tracking.sources import record_loads

from tests.contract.harness import Outside, Solera, SourceStore


async def source_versions(engine):
    out = engine.m.core_output("feed", "")
    rows, _ = await engine.core.run(engine.core.index.scan, out["layers"], out["head"])
    return {r[0]: r[2] for r in rows}


@pytest.mark.xfail(
    strict=True, reason="ERW-27: a source's load report names a commit without the source's life"
)
async def test_source_report_does_not_cross_a_source_life(tmp_path, monkeypatch):
    s = Solera(tmp_path, "core")
    try:

        def project(store):
            return Project(
                sources=[Source("first", key="id"), Source("feed", key="id", store=store)],
                stores={"other": FileStore(tmp_path / "other")},
                default_store=FileStore(tmp_path / "default"),
            )

        await s.deploy(project("default"))
        engine = s.engine
        await engine.commit_source("first", upsert={"k": "v1"})
        await engine.commit_source("feed", upsert={"k": "v1"})
        life = engine.m.life("feed", "")
        native = engine.core.run
        fired = False

        async def deploy_after_first_report_build(fn, *args, **kw):
            nonlocal fired
            result = await native(fn, *args, **kw)
            if not fired and fn == engine.core.tracking.build_commit:
                fired = True
                engine.redeploy(project("other").manifest)
                await engine.commit_source("feed", upsert={"k": "v3"})
            return result

        monkeypatch.setattr(engine.core, "run", deploy_after_first_report_build)
        prepared = {
            "plans": {
                "first": {"kind": "core", "output": "first", "head": 1},
                "feed": {"kind": "core", "output": "feed", "head": 1},
            },
            "gate": [["feed", "", 1, life]],
            "deploy_number": engine.m.deploy_number,
        }
        await record_loads(
            engine,
            "old-attempt",
            prepared,
            {"delivered": {"first": {"loaded": [["k", "v2"]]}, "feed": {"loaded": [["k", "v2"]]}}},
        )
        assert await source_versions(engine) == {"k": "v3"}
    finally:
        await s.close()


@pytest.mark.xfail(strict=True, reason="ERW-27: the staleness views keep their inputs across an await")
async def test_staleness_restarts_when_its_asset_is_removed(tmp_path, monkeypatch):
    s = Solera(tmp_path, "core")
    try:

        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": "a"}]

        @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id"))
        def copy(files: list):
            return files

        def project(assets):
            return Project(assets=assets, default_store=FileStore(tmp_path / "data"))

        await s.deploy(project([files, copy]))
        await s.run("files")
        await s.run("copy")
        engine, native = s.engine, s.engine.core.run
        fired = False

        async def remove_after_plan(fn, *args, **kw):
            nonlocal fired
            result = await native(fn, *args, **kw)
            if not fired and fn == engine.core.tracking.plan:
                fired = True
                engine.redeploy(project([files]).manifest)
            return result

        monkeypatch.setattr(engine.core, "run", remove_after_plan)
        await engine.stale_reasons("copy", "")
        assert fired
    finally:
        await s.close()


@pytest.mark.xfail(strict=True, reason="ERW-27: a sensor tick checks the deploy only on its success path")
async def test_sensor_deploy_during_prepare_is_refused_not_failed(tmp_path, monkeypatch):
    s = Solera(tmp_path, "core")

    def project(two):
        @sensor(every=1, commits=["a", "b"] if two else ["a"])
        def watch(ctx):
            return None

        return Project(
            sources=[Source("a", key="id"), *([Source("b", key="id")] if two else [])], sensors=[watch]
        )

    try:
        await s.deploy(project(True))
        await s.commit_source("a", upsert={"k": "v1"})
        await s.commit_source("b", upsert={"k": "v1"})
        [tick] = await s.sensor_ticks()
        engine, native = s.engine, s.engine.core.run
        fired = False

        async def remove_second_source(fn, *args, **kw):
            nonlocal fired
            result = await native(fn, *args, **kw)
            if not fired and fn == engine.core.tracking.build_commit:
                fired = True
                engine.redeploy(project(False).manifest)
            return result

        monkeypatch.setattr(engine.core, "run", remove_second_source)
        outcome = Tick(commits=[Commit("a", upsert={"k": "v2"}), Commit("b", upsert={"k": "v2"})])
        response = await s.post(
            f"/sensors/{tick['sensor']}/ticks/{tick['tick']}", outcome.to_json(), ok=False
        )
        assert response.status_code == 409, (response.status_code, response.text)
    finally:
        await s.close()


@pytest.mark.xfail(strict=True, reason="ERW-27: a sensor snapshot names a commit without the source's life")
async def test_sensor_snapshot_does_not_cross_source_lives(tmp_path):
    s = Solera(tmp_path, "core")

    def project(store):
        @sensor(every=1, commits=["feed"])
        def watch(ctx):
            return None

        return Project(
            sources=[Source("feed", key="id", store=store)],
            sensors=[watch],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )

    try:
        await s.deploy(project("default"))
        await s.commit_source("feed", upsert={"k": "v1"})
        [tick] = await s.sensor_ticks()
        s.engine.redeploy(project("other").manifest)
        await s.engine.commit_source("feed", upsert={"k": "v3"})
        outcome = Tick(commits=[Commit("feed", upsert={"k": "v2"})])
        response = await s.post(
            f"/sensors/{tick['sensor']}/ticks/{tick['tick']}", outcome.to_json(), ok=False
        )
        out = s.engine.m.core_output("feed", "")
        rows, _ = await s.engine.core.run(s.engine.core.index.scan, out["layers"], out["head"])
        assert response.status_code == 409 and {r[0]: r[2] for r in rows} == {"k": "v3"}, (
            response.status_code,
            response.text,
            rows,
        )
    finally:
        await s.close()


@pytest.mark.xfail(strict=True, reason="ERW-27: the sensor long-poll keeps the deploy it started under")
async def test_old_deploy_sensor_long_poll_cannot_claim_a_new_deploy_tick(tmp_path):
    s = Solera(tmp_path, "core")

    def project(new):
        def body(ctx):
            return None

        body.__name__ = body.__qualname__ = "new_watch" if new else "old_watch"
        watch = sensor(every=1, commits=["feed"])(body)
        return Project(sources=[Source("feed", key="id")], sensors=[watch])

    try:
        await s.deploy(project(False))
        old_deploy = s.engine.manifest["deploy"]
        [claimed] = await s.sensor_ticks()  # the old sensor has an active tick, so the next ask waits
        pending = asyncio.create_task(s.engine.sensor_next("local", old_deploy, "old-host", 1, 0.5))
        await asyncio.sleep(0.01)
        assert not pending.done()
        s.engine.redeploy(project(True).manifest)
        s.engine._sensors_wake()
        answer = await pending
        assert not answer["ticks"] and answer["deploy"] == s.engine.manifest["deploy"], answer
    finally:
        await s.close()


@pytest.mark.xfail(
    strict=True, reason="ERW-27: a source's load report names a commit without the source's life"
)
async def test_a_finished_attempt_never_reports_an_old_load_into_a_new_source_life(tmp_path, monkeypatch):
    s = Solera(tmp_path, "core")
    first, before, after = Outside(), Outside(), Outside()
    first.feed["k"] = before.feed["k"] = "v2"
    after.feed["k"] = "v2"

    @asset(
        inputs={"a_first": Incremental("first"), "z_feed": Incremental("feed")},
        outputs=Output("copy", key="id"),
    )
    def copy(a_first: list, z_feed: list):
        return z_feed

    @asset(inputs={"feed": Incremental()}, outputs=Output("watch", key="id"))
    def watch(feed: list):
        return feed

    def project(moved=False):
        return Project(
            assets=[copy, watch],
            sources=[
                Source("first", key="id", store="first"),
                Source("feed", key="id", store="after" if moved else "before"),
            ],
            stores={
                "first": SourceStore(tmp_path / "first", first),
                "before": SourceStore(tmp_path / "before", before),
                "after": SourceStore(tmp_path / "after", after),
            },
            default_store=FileStore(tmp_path / "data"),
        )

    try:
        await s.deploy(project())
        engine = s.engine
        await engine.commit_source("first", upsert={"k": "v1"})
        await engine.commit_source("feed", upsert={"k": "v0"})
        await engine.commit_source("feed", upsert={"k": "v0.5"})
        await engine.commit_source("feed", upsert={"k": "v1"})
        native, fired = engine.core.run, False
        first_id = engine.m.core_outputs["first", ""]
        moved = project(moved=True)
        work = InlinePlacement._work

        def current_worker(placement, stage):
            if engine.manifest["deploy"] == moved.manifest["deploy"]:
                placement.project = moved
            return work(placement, stage)

        monkeypatch.setattr(InlinePlacement, "_work", current_worker)

        async def move_after_first_report(fn, *args, **kw):
            nonlocal fired
            result = await native(fn, *args, **kw)
            if not fired and fn == engine.core.tracking.build_commit and args[0] == first_id.encode():
                fired = True
                engine.redeploy(moved.manifest)
                await engine.commit_source("feed", upsert={"k": "v1"})
                await s.run("watch")  # loads v2, reports it: new source head 2
                after.feed["k"] = "v3"
                await engine.commit_source("feed", upsert={"k": "v3"})
            return result

        monkeypatch.setattr(engine.core, "run", move_after_first_report)
        detail = await s.run("copy")
        assert fired
        out = engine.m.core_output("feed", "")
        rows, _ = await native(engine.core.index.scan, out["layers"], out["head"])
        versions = {r[0]: r[2] for r in rows}
        state = {
            "source_versions": versions,
            "outside": after.feed,
            "watch_rows": await s.rows("watch"),
            "watch_stale": await engine.stale_reasons("watch", ""),
            "attempts": detail["attempts"],
        }
        assert versions == {"k": "v3"} and state["watch_stale"], state
    finally:
        await s.close()
