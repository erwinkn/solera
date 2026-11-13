"""Review round 4 (Fable F3): an asset with no output (a job) whose keyed upstream is reset by a store move.
Its next run must be a full run (batch.reset on its first batch), as for any consumer of a reset
output (docs/observed-set.md: "An upstream reset is a full run for its consumers")."""

import pytest
from solera.sdk import Incremental, Output, Project, Result, Retry, asset, job
from solera.stores import FileStore

from tests.contract.harness import Solera


@pytest.mark.parametrize("shape", ["job", "asset-with-output"])
async def test_a_job_whose_upstream_moved_is_told_to_start_over(tmp_path, shape):
    s = Solera(tmp_path)
    rows = {"a": 1, "b": 1, "c": 1}
    batches = []

    def project(store):
        @asset(outputs=Output("items", key="id", store=store))
        def items():
            return [{"id": k, "v": v} for k, v in rows.items()]

        if shape == "job":

            @job(inputs={"items": Incremental(batch_size=10)}, retries=Retry(n=0))
            async def sink(ctx, items: list):
                b = ctx.batch["items"]
                batches.append((b.reset, sorted(r["id"] for r in items), sorted(b.removed)))
                return Result(outputs={})

        else:

            @asset(inputs={"items": Incremental(batch_size=10)}, retries=Retry(n=0))
            async def sink(ctx, items: list):
                b = ctx.batch["items"]
                batches.append((b.reset, sorted(r["id"] for r in items), sorted(b.removed)))
                return len(items)

        return Project(
            assets=[items, sink],
            stores={"other": FileStore(tmp_path / "other")},
            default_store=FileStore(tmp_path / "data"),
        )

    try:
        await s.deploy(project("default"))
        await s.run("items")
        await s.run("sink")
        assert batches == [(False, ["a", "b", "c"], [])] or batches == [(True, ["a", "b", "c"], [])], batches
        del rows["c"]  # gone from the new life: no removal can be delivered for it
        await s.deploy(project("other"))  # items moves: a new life
        await s.run("items")
        del batches[:]
        await s.run("sink", expect=None)
        m = s.engine.m
        state = {
            "batches": batches,
            "definition": "definition" in m.partition("sink", ""),
            "heads": sorted(str(k) for k in m.heads),
        }
        assert batches and batches[0][0] is True, f"the sink was not told to start over: {state}"
    finally:
        await s.close()
