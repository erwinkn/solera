import asyncio
import os
import textwrap

import pytest
from conftest import finish

from data_orchestrator import ByKey, Inventory, Project, ReplaceKeys, asset
from data_orchestrator.engine import Engine
from data_orchestrator.execution import LocalSubprocess


@pytest.mark.parametrize("case", ["unknown", "collision", "reserved", "missing", "variadic", "positional"])
def test_invalid_bindings_fail_at_registration(case):
    @asset
    def source():
        return 1

    resources = {}
    if case == "unknown":

        @asset(inputs={"typo": "source"})
        def consumer(value):
            return value
    elif case == "collision":

        @asset(inputs={"db": "source"})
        def consumer(db):
            return db

        resources = {"db": object()}
    elif case == "reserved":

        @asset(inputs={"ctx": "source"})
        def consumer(ctx):
            return ctx
    elif case == "missing":

        @asset(inputs={})
        def consumer(required):
            return required
    elif case == "variadic":

        @asset
        def consumer(**kwargs):
            return kwargs
    else:

        @asset
        def consumer(source, /):
            return source

    with pytest.raises(ValueError):
        Project([source, consumer], resources=resources)


async def test_incomplete_inventory_propagates_through_snapshot_transform(make_engine):
    source = {"rows": [{"id": "a", "revision": 1}, {"id": "b", "revision": 1}], "complete": True}

    @asset
    def inventory():
        return Inventory(source["rows"], source["complete"])

    @asset
    def intermediate(inventory):
        return inventory

    @asset(incremental=ByKey("intermediate"))
    def target(ctx, intermediate):
        keys = ctx.changes["upserted_keys"] + ctx.changes["deleted_keys"]
        return ReplaceKeys("id", keys, [r for r in intermediate if r["id"] in ctx.changes["upserted_keys"]])

    engine = await make_engine(Project([inventory, intermediate, target]))
    assert (await finish(engine, await engine.submit(["target"])))["request"]["status"] == "succeeded"
    source.update(rows=[{"id": "a", "revision": 2}], complete=False)
    assert (await finish(engine, await engine.submit(["target"])))["request"]["status"] == "succeeded"
    assert not (await engine.asset_detail("intermediate"))["head"]["ref"]["complete"]
    assert not (await engine.asset_detail("target"))["head"]["scope_complete"]
    assert {r["id"] for r in (await engine.asset_detail("target"))["preview"]} == {"a", "b"}
    source["complete"] = True
    await finish(engine, await engine.submit(["target"]))
    assert (await engine.asset_detail("target"))["preview"] == [{"id": "a", "revision": 2}]


async def test_unchanged_partial_scan_cannot_reuse_a_complete_output(make_engine):
    source = {"rows": [{"id": "a", "revision": 1}], "complete": True}

    @asset
    def inventory():
        return Inventory(source["rows"], source["complete"])

    @asset(incremental=ByKey("inventory"))
    def target(ctx, inventory):
        return ReplaceKeys(
            "id",
            ctx.changes["upserted_keys"],
            [r for r in inventory if r["id"] in ctx.changes["upserted_keys"]],
        )

    engine = await make_engine(Project([inventory, target]))
    await finish(engine, await engine.submit(["target"]))
    source["complete"] = False
    await finish(engine, await engine.submit(["target"]))
    assert not (await engine.asset_detail("target"))["head"]["ref"]["complete"]
    assert not (await engine.asset_detail("target"))["head"]["scope_complete"]
    source["complete"] = True
    await finish(engine, await engine.submit(["target"]))
    assert (await engine.asset_detail("target"))["head"]["scope_complete"]


async def test_noisy_project_import_does_not_corrupt_manifest(tmp_path, monkeypatch):
    (tmp_path / "noisy_project.py").write_text(
        textwrap.dedent("""
        print('Initializing a noisy project')
        from data_orchestrator import Project, asset
        @asset
        def value():
            print('Transform log')
            return 42
        project = Project([value])
    """)
    )
    monkeypatch.setenv("PYTHONPATH", str(tmp_path) + os.pathsep + os.getenv("PYTHONPATH", ""))
    backend = LocalSubprocess("noisy_project:project")
    manifest = await backend.manifest()
    assert "value" in manifest["owners"]
    result, logs = await backend.execute(
        {"revision": manifest["revision"], "producer": "value", "inputs": {}, "context": {}}
    )
    assert result["outputs"]["value"]["value"] == 42
    assert "Transform log" in logs


async def test_subprocess_log_flood_is_bounded(tmp_path, monkeypatch):
    (tmp_path / "loud_project.py").write_text(
        textwrap.dedent("""
        from data_orchestrator import Project, asset
        @asset
        def loud():
            while True:
                print('x' * 16384, flush=True)
        project = Project([loud])
    """)
    )
    monkeypatch.setenv("PYTHONPATH", str(tmp_path) + os.pathsep + os.getenv("PYTHONPATH", ""))
    backend = LocalSubprocess("loud_project:project", timeout=10, log_limit=131072)
    manifest = await backend.manifest()
    with pytest.raises(RuntimeError, match="log output exceeded"):
        async with asyncio.timeout(15):
            await backend.execute(
                {"revision": manifest["revision"], "producer": "loud", "inputs": {}, "context": {}}
            )


async def test_numeric_demo_inventory_keys_are_processed(state):
    backend = LocalSubprocess("data_orchestrator.demo:project")
    engine = Engine(state, await backend.manifest(), backend)
    await engine.initialize()
    run = await engine.submit(
        ["sample_quality"],
        config={"files": [{"id": 7, "revision": 1, "sample": "Numeric key", "calcium": 12}]},
    )
    assert (await finish(engine, run))["request"]["status"] == "succeeded"
    assert (await engine.asset_detail("sample_quality"))["preview"][0]["name"] == "Numeric key"
