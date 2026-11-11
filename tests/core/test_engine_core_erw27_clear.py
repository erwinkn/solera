"""Round 4 probes of the clear decision read from the outputs' heads."""

import pytest
from solera.sdk import Incremental, Output, Project, Result, Retry, asset
from solera.stores import FileStore
from solera_server.tracking import full

from tests.contract.harness import Solera, rebuild


@pytest.mark.parametrize("moved", ["upstream", "own value output", "own keyed output"])
async def test_c1_value_outputs_and_a_reset(tmp_path, moved):
    s = Solera(tmp_path, "core")
    rows = {"a": 1, "b": 1, "c": 1}
    batches = []

    def project(stores):
        @asset(outputs=Output("items", key="id", store=stores.get("upstream", "default")))
        def items():
            return [{"id": k, "v": v} for k, v in rows.items()]

        @asset(
            inputs={"items": Incremental(batch_size=10)},
            outputs=[
                Output("total", store=stores.get("own value output", "default")),
                Output("mirror", key="id", store=stores.get("own keyed output", "default")),
            ],
            retries=Retry(n=0),
        )
        async def digest(ctx, items: list):
            b = ctx.batch["items"]
            batches.append((b.reset, sorted(r["id"] for r in items), sorted(b.removed)))
            held = {} if b.reset else dict(await ctx.load("total") or {})
            held.update({r["id"]: r["v"] for r in items})
            for k in b.removed:
                held.pop(k, None)
            return {"total": held, "mirror": rebuild(b, [{"id": r["id"], "v": r["v"]} for r in items])}

        return Project(
            assets=[items, digest],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )

    try:
        await s.deploy(project({}))
        await s.run("items")
        await s.run("digest")
        del rows["c"]
        await s.run("items")  # "c" is gone upstream; digest has not run since
        await s.deploy(project({moved: "other"}))
        if moved == "upstream":
            await s.run("items")
        del batches[:]
        await s.run("digest")
        await s.run("digest", expect=None)
        state = {
            "batches": batches,
            "total": await s.read("total"),
            "mirror": sorted(await s.rows("mirror")),
            "waiting": full.waiting(s.engine),
        }
        assert (
            state["total"] == {"a": 1, "b": 1} and state["mirror"] == ["a", "b"] and not state["waiting"]
        ), state
    finally:
        await s.close()


@pytest.mark.xfail(
    strict=True, reason="ERW-27: a full run's clear rides on its first batch's commit of each output"
)
async def test_c3_a_start_over_whose_first_batch_omits_an_output_never_clears_it(tmp_path):
    import os

    s = Solera(tmp_path, os.environ.get("TRACKING_PATH", "core"))
    rows, omit = {"a": 1, "b": 1, "c": 1}, [False]

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": v} for k, v in rows.items()]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"), retries=Retry(n=0))
    async def copy(ctx, files: list):
        b = ctx.batch["files"]
        if omit[0] and b.index == 0:
            return Result(outputs={})
        return rebuild(b, [{"id": r["id"], "v": r["v"]} for r in files])

    try:
        await s.deploy(Project(assets=[files, copy], default_store=FileStore(tmp_path / "data")))
        await s.run("files")
        await s.run("copy")
        del rows["c"]
        await s.run("files")
        omit[0] = True
        await s.run("copy", mode="full")
        await s.settle(0.3)
        state = {"rows": sorted(await s.rows("copy")), "stale": await s.stale("copy")}
        assert state["rows"] == ["a", "b"], f"a full run that ended left a key its upstream removed: {state}"
    finally:
        await s.close()
