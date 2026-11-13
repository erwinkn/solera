"""Deploys while the engine works on the new tracking core: a deploy while a commit is built
sends the batch back (a rename and back included), a deploy during a coverage check stops no
tick, and an append-only name reused after a rename is a new life (Sol's probes of PR 2's
redesign, round 2)."""

import asyncio

from solera.sdk import DynamicPartitions, In, Incremental, Output, Project, Retry, asset
from solera.stores import FileStore, Patch
from solera_server.tracking import full, stale

from tests.contract.harness import Solera


async def test_a_rename_and_back_while_a_commit_is_built_sends_the_batch_back(tmp_path, monkeypatch):
    from solera_server.executors.inline import InlinePlacement

    s = Solera(tmp_path)
    delta = ["a"]

    def project(name, aliases=()):
        def body():
            return {name: Patch(list(delta)), "zz": Patch([{"id": x} for x in delta])}

        body.__name__ = body.__qualname__ = name
        producer = asset(outputs=[DynamicPartitions(name), Output("zz", key="id")], aliases=list(aliases))(
            body
        )

        @asset(partitions=name)
        def per(ctx):
            return ctx.partition

        @asset(inputs={"per": In()})
        def report(per: dict[str, str]):
            return sorted(per)

        return Project(assets=[producer, per, report], default_store=FileStore(tmp_path / "data"))

    try:
        await s.deploy(project("old"))
        await s.run("old")
        await s.run("per", partitions="all")
        delta[:] = ["b"]
        engine, builds = s.engine, []
        native, work = engine.core.run, InlinePlacement._work
        back = project("old", ["new"])

        async def interleave(fn, *args, **kw):
            result = await native(fn, *args, **kw)
            if fn == engine.core.tracking.build_commit:
                builds.append(args[0])
                if len(builds) == 1:
                    engine.redeploy(project("new", ["old"]).manifest)
                elif len(builds) == 2:
                    engine.redeploy(back.manifest)
            return result

        def current_worker(self, stage):
            if engine.manifest["deploy"] == back.manifest["deploy"]:
                self.project = back
            return work(self, stage)

        monkeypatch.setattr(engine.core, "run", interleave)
        monkeypatch.setattr(InlinePlacement, "_work", current_worker)
        detail = await s.run("old", expect=None)
        outcomes = [a["outcome"] for a in sum(detail["attempts"].values(), [])]
        assert outcomes == ["replanned", "succeeded"], detail["attempts"]
        assert engine.m.heads["old", ""]["partitions"] == ["a", "b"]
        await s.deploy(back)
        await s.run("per", partitions="all")
        await s.run("report")
        assert await s.read("report") == ["a", "b"]
    finally:
        await s.close()


async def test_a_deploy_during_a_coverage_check_stops_no_tick(tmp_path, monkeypatch):
    s = Solera(tmp_path)

    def project(two):
        @asset(outputs=Output("first", key="id"))
        def first():
            return [{"id": "a"}]

        @asset(outputs=Output("second", key="id"))
        def second():
            return [{"id": "a"}, {"id": "b"}]

        @asset(
            inputs={"a": Incremental("first"), **({"b": Incremental("second")} if two else {})},
            outputs=Output("join", key="id"),
        )
        def join(a: list, b: list | None = None):
            return list({r["id"]: r for r in a + (b or [])}.values())

        return Project(assets=[first, second, join], default_store=FileStore(tmp_path / "data"))

    try:
        await s.deploy(project(True))
        await s.run("first")
        await s.run("second")
        await s.run("join", keys={"first": ["a"], "second": ["a"]})
        engine = s.engine
        assert sorted(engine.m.core_empty) == [("join", "", "a"), ("join", "", "b")]
        engine._covers.clear()
        original, changed = stale.taken_after, []

        async def interleave(*args, **kw):
            found = await original(*args, **kw)
            if not changed:
                changed.append(True)
                engine.redeploy(project(False).manifest)
            return found

        monkeypatch.setattr(stale, "taken_after", interleave)
        await engine.tick()
        assert changed
    finally:
        await s.close()


async def test_an_append_output_name_reused_after_a_rename_is_a_new_life(tmp_path):
    s = Solera(tmp_path)
    report_entered, report_release = asyncio.Event(), asyncio.Event()
    copy_entered, copy_release = asyncio.Event(), asyncio.Event()
    copy_release.set()

    def project(renamed=False, reuse=False):
        @asset(outputs=Output("files", key="id"))
        def files():
            return [{"id": k} for k in "abc"]

        def producer(name, aliases=()):
            async def body(ctx, files: list):
                if ctx.batch["files"].index > 0:
                    copy_entered.set()
                    await copy_release.wait()
                return Patch(files)

            body.__name__ = body.__qualname__ = name
            return asset(
                inputs={"files": Incremental(batch_size=1)},
                outputs=Output(incremental=True),
                aliases=list(aliases),
                retries=Retry(n=0),
            )(body)

        @asset(inputs={"rows": In("new" if renamed else "old")}, retries=Retry(n=0))
        async def report(rows: list):
            report_entered.set()
            await report_release.wait()
            return len(rows)

        assets = [files, producer("new", [] if reuse else ["old"]) if renamed else producer("old"), report]
        if reuse:
            assets.append(producer("old"))
        return Project(assets=assets, default_store=FileStore(tmp_path / "data"))

    try:
        await s.deploy(project(), cancel_grace=0.1, heartbeat_seconds=0.1)
        await s.run("files")
        await s.run("old")
        reader = await s.submit("report")
        await s.until(report_entered.is_set)
        task = next(t for t in s.engine.m.runs[reader["id"]]["tasks"].values() if t["asset"] == "report")
        pin = task["launched"]["prepared"]["gate"]
        await s.deploy(project(renamed=True), cancel_grace=0.1, heartbeat_seconds=0.1)
        await s.deploy(project(renamed=True, reuse=True), cancel_grace=0.1, heartbeat_seconds=0.1)
        copy_entered.clear()
        copy_release.clear()
        partial = await s.submit("old")
        await s.until(copy_entered.is_set)
        await s.cancel(partial["id"])
        await s.wait(partial)
        copy_release.set()
        assert full.waiting(s.engine) == {("old", "")}
        assert full.held(s.engine.m, pin) is None
        report_release.set()
        detail = await s.wait(reader, 10)
        outcomes = [a["outcome"] for a in sum(detail["attempts"].values(), [])]
        assert detail["request"]["status"] == "succeeded" and outcomes == ["succeeded"], outcomes
    finally:
        report_release.set()
        copy_release.set()
        await s.close()
