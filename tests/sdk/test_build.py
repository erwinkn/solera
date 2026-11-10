"""The build identity in the deploy (docs/per-key-processing.md §13)."""


def test_revision_follows_the_build(monkeypatch):
    from solera.sdk import Project, asset

    @asset
    def a():
        return 1

    monkeypatch.setenv("SOLERA_BUILD", "one")
    one = Project(assets=[a]).manifest
    monkeypatch.setenv("SOLERA_BUILD", "two")
    two = Project(assets=[a]).manifest
    assert one["deploy"] != two["deploy"] and "code_hash" not in one["assets"]["a"]
    assert Project(assets=[a], build="two").manifest["deploy"] == two["deploy"]


def test_the_deploy_number_counts_served_deploys():
    from solera_server.model import Model

    m = Model()
    manifest = {"automations": {}, "sources": {}, "assets": {}, "outputs": {}}
    for deploy in ("r1", "r1", "r2", "r1"):
        m.apply({"type": "ProjectRegistered", "deploy": deploy, "manifest": manifest, "at": 0})
    assert m.deploy_number == 3
    restored = Model()
    restored.restore(m.snapshot())
    assert restored.deploy_number == 3


def test_a_method_mismatch_is_named():
    from solera.build import method_note

    assert method_note({"source": "modules"}, {"source": "modules"}) is None
    assert method_note({"source": "modules"}, None) is None
    note = method_note({"source": "explicit"}, {"source": "modules"})
    assert "explicit build id" in note and "project's modules" in note and "SOLERA_BUILD" in note


async def test_the_engine_warns_when_a_worker_computed_its_revision_another_way(tmp_path, caplog):
    """A worker whose deploy differs because it hashed its modules while the
    engine used an explicit id fails its attempt as before, and the engine
    says why."""

    import logging

    from solera.sdk import Project, asset
    from solera_server.state import State

    from tests.server.engines import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "modules"}, "deploy": "elsewhere"}
    await engine.initialize()
    with caplog.at_level(logging.WARNING):
        detail = await engine.run_until((await engine.submit(["numbers"]))["id"], 10)
    assert detail["request"]["status"] == "failed"
    assert any("never match" in r.getMessage() for r in caplog.records)
    assert "SOLERA_BUILD" in detail["attempts"][detail["tasks"][0]["id"]][0]["error"]
    await state.close()


async def test_the_engine_warns_a_sensor_host_once(tmp_path, caplog):
    import logging

    from solera.sdk import Project, asset
    from solera_server.state import State

    from tests.server.engines import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "modules"}}
    await engine.initialize()
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            await engine.sensor_next("local", "other", "h1", 1, 0, "files")
    assert sum("never match" in r.getMessage() for r in caplog.records) == 1
    await state.close()
