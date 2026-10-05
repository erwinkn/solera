"""A per-key asset's passes and its retry passes share its runs (per-key
processing §9): they alternate, and a run never ends with either left half
way (F31)."""

from solera.sdk import Incremental, Output, Project, Retry, Source, asset
from solera.stores import FileStore

from tests.sim.project import External, SourceStore

from .engines import drive, make_engine


async def test_a_run_finishes_the_pass_it_began_before_it_ends(state, tmp_path):
    """F31, from W38's trace: `checks` (Each over `feed`, batches of 2) has a
    failed key. feed commits k1, k2, k3, and a forced retry run of checks
    goes: its batches alternate between the delta pass and the retry pass.
    A retry batch that finds nothing used to end the run with the delta
    pass at batch 1 of 2, so k3 never reached checks. The run delivers
    every key before it ends."""

    outside, broken = External(), {"bad"}

    @asset(
        inputs={"item": Incremental("feed", batch_size=2, each=True)},
        outputs=Output("checks", key="id"),
        retries=Retry(0),
    )
    async def checks(ctx, item: list):
        if ctx.key in broken:
            raise RuntimeError("down")
        return [{"v": item[0]["v"]}]

    project = Project(
        assets=[checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(tmp_path / "ext", outside)},
        default_store=FileStore(tmp_path / "out"),
    )
    engine = make_engine(state, project)
    await engine.initialize()
    outside.feed.update(bad="1", a="1")
    await engine.commit_source("feed", upsert=["a", "bad"])
    await drive(engine, await engine.submit(["checks"]))
    assert sorted((await engine.list_keys("checks"))["keys"]) == ["a"]  # `bad` failed

    broken.clear()
    outside.feed.update(k1="1", k2="1", k3="1")
    await engine.commit_source("feed", upsert=["k1", "k2", "k3"])
    found = engine.retry_keys("checks", ["failed"], by="test")
    [run] = await engine.submit_retries("checks", found["partitions"], "test")
    detail = await drive(engine, run)
    assert detail["request"]["status"] == "succeeded"
    assert sorted((await engine.list_keys("checks"))["keys"]) == ["a", "bad", "k1", "k2", "k3"]
    assert await engine.complete("checks", "")
    assert sorted(await engine.observed("checks", "", "item")) == ["a", "bad", "k1", "k2", "k3"]
