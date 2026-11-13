"""Deploys while the engine works on the new tracking core: a deploy while a commit is built
sends the batch back (a rename and back included), a deploy during a coverage check stops no
tick, and an append-only name reused after a rename is a new life (Sol's probes of PR 2's
redesign, round 2)."""

from solera.sdk import DynamicPartitions, In, Incremental, Output, Project, asset
from solera.stores import FileStore, Patch
from solera_server.tracking import stale

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
