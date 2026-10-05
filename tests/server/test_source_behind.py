"""A source read as it is now no longer holds a key its index names
(F33, superseded by the observed set: docs/observed-set.md,
"Observations"). The source says so: the key is observed absent — a key
never held, nothing; one held, a removal — and the batch commits what it
saw. The index still names the key, so the partition owes it until the
source's next commit restores it (delivered) or removes it (nothing)."""

from solera.sdk import Incremental, Output, Project, Retry, Source, asset

from tests.sim.oracle import keyed_content
from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine, status_of


def project(root, outside: External, retries: Retry):
    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"), retries=retries)
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"row": Incremental("feed", each=True)}, outputs=Output("checks", key="id"), retries=retries
    )
    async def checks(ctx, row: list):
        return [{"v": row[0]["v"]}]

    return Project(
        assets=[items, checks],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
    )


async def _behind(state, tmp_path, asset_name):
    """`feed` commits k1 and k2, then its client drops k1 outside without a commit."""

    outside = External()
    engine = make_engine(state, p := project(tmp_path, outside, Retry(n=0)))
    engine.p = p
    await engine.initialize()
    outside.feed.update({"k1": "1", "k2": "1"})
    await engine.commit_source("feed", upsert={"k1": "1", "k2": "1"})
    del outside.feed["k1"]
    detail = await drive(engine, await engine.submit([asset_name]))
    return engine, outside, detail


async def test_a_key_the_source_lost_is_observed_absent(state, tmp_path):
    engine, _, detail = await _behind(state, tmp_path, "items")
    assert status_of(detail) == "succeeded"
    assert await keyed_content(engine, engine.p, "items") == {"k2": "1"}
    assert sorted(await engine.observed("items", "", "feed")) == ["k2"]
    # Its index still names k1, which no batch has seen: owed, and the partition says so.
    assert await engine.stale_reasons("items", "") == ["input changed"]
    await engine.stop()


async def test_the_next_commit_restoring_the_key_delivers_it(state, tmp_path):
    engine, outside, _ = await _behind(state, tmp_path, "items")
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert={"k1": "2"})
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert await keyed_content(engine, engine.p, "items") == {"k1": "2", "k2": "1"}
    assert await engine.stale_reasons("items", "") == []
    await engine.stop()


async def test_the_next_commit_removing_the_key_owes_nothing(state, tmp_path):
    engine, _, _ = await _behind(state, tmp_path, "items")
    await engine.commit_source("feed", remove=["k1"])
    assert await engine.stale_reasons("items", "") == []
    detail = await drive(engine, await engine.submit(["items"]))
    assert status_of(detail) in ("succeeded", "skipped")
    assert await keyed_content(engine, engine.p, "items") == {"k2": "1"}
    await engine.stop()


async def test_a_per_key_batch_observes_it_alike(state, tmp_path):
    engine, _, detail = await _behind(state, tmp_path, "checks")
    assert status_of(detail) == "succeeded"
    assert await keyed_content(engine, engine.p, "checks") == {"k2": "1"}
    assert sorted(await engine.observed("checks", "", "row")) == ["k2"]
    await engine.stop()
