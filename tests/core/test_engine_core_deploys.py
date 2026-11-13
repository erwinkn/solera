"""Deploys while the engine works on the new tracking core: a deploy while a commit is built
sends the batch back (a rename and back included), a deploy during a coverage check stops no
tick, and an append-only name reused after a rename is a new life (Sol's probes of PR 2's
redesign, round 2)."""

from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore
from solera_server.model import edge_id
from solera_server.tracking import stale

from tests.contract.harness import Solera


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
        assert sorted(engine.m.core.walking()) == [edge_id("join", "", "a"), edge_id("join", "", "b")]
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
