"""F33 (Erwin's ruling): a source read as it is now no longer holds a key its
index names. Loading the batch fails retryable with the key and its version;
nothing is delivered and the position stays; the retries are the asset's
budget, then the attempt fails with that reason. The source's next commit,
restoring or removing the key, lets the next attempt through."""

from solera.sdk import Each, Incremental, Output, Project, Retry, Source, asset

from tests.sim.oracle import keyed_content
from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine, status_of


def project(root, outside: External, retries: Retry):
    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"), retries=retries)
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"row": Each("feed")}, outputs=Output("checks", key="id"), retries=retries)
    async def checks(ctx, row: list):
        return [{"v": row[0]["v"]}]

    return Project(
        assets=[items, checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
    )


async def _behind(state, tmp_path, asset_name, retries):
    """`feed` commits k1 and k2, then its client drops k1 outside without a commit."""

    outside = External()
    engine = make_engine(state, p := project(tmp_path, outside, retries))
    engine.p = p
    await engine.initialize()
    outside.feed.update({"k1": "1", "k2": "1"})
    await engine.commit_source("feed", upsert={"k1": "1", "k2": "1"})
    generation = state.model.heads[("feed", "")]["ref"]["generation"]
    del outside.feed["k1"]
    detail = await drive(engine, await engine.submit([asset_name]))
    return engine, outside, detail, generation


async def test_a_key_the_source_lost_fails_the_attempt_after_its_retries(state, tmp_path):
    engine, _, detail, generation = await _behind(state, tmp_path, "items", Retry(n=2, delay=0))
    assert status_of(detail) == "failed"
    (task,) = detail["tasks"]
    attempts = detail["attempts"][task["id"]]
    assert len(attempts) == 3, "the first attempt and its two retries, no more"
    assert all(a["outcome"] == "failed" for a in attempts)
    want = f"the source index says k1@{generation} but the source has no k1"
    assert all(want in a["error"] for a in attempts), attempts[-1]["error"]
    assert ("items", "") not in state.model.heads, "nothing was delivered"
    assert state.model.position("items", "feed", "") is None, "the position did not move"
    await engine.stop()


async def test_the_next_commit_restoring_the_key_lets_it_through(state, tmp_path):
    engine, outside, detail, _ = await _behind(state, tmp_path, "items", Retry(n=0))
    assert status_of(detail) == "failed"
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert={"k1": "2"})
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert await keyed_content(engine, engine.p, "items") == {"k1": "2", "k2": "1"}
    await engine.stop()


async def test_the_next_commit_removing_the_key_lets_it_through(state, tmp_path):
    engine, _, detail, _ = await _behind(state, tmp_path, "items", Retry(n=0))
    assert status_of(detail) == "failed"
    await engine.commit_source("feed", remove=["k1"])
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert await keyed_content(engine, engine.p, "items") == {"k2": "1"}
    await engine.stop()


async def test_an_each_page_missing_a_key_fails_alike(state, tmp_path):
    engine, _, detail, generation = await _behind(state, tmp_path, "checks", Retry(n=1, delay=0))
    assert status_of(detail) == "failed"
    (task,) = detail["tasks"]
    attempts = detail["attempts"][task["id"]]
    assert len(attempts) == 2
    assert f"k1@{generation} but the source has no k1" in attempts[-1]["error"]
    await engine.stop()
