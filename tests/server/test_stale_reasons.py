"""Staleness is transitive and says why (K46, docs/positions-from-reads.md):
`input changed`, `upstream stale`, `definition changed` — on partition
statuses, the asset roll-up, `stale_keys`, its route and the CLI."""

import httpx
from solera.sdk import Incremental, Output, Project, Source, asset
from solera.stores import FileStore
from solera_server.api import create_app

from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine


def project(root, outside, copy_v="1"):
    """feed (a keyed source) -> items -> copy, both keyed incremental; and
    `summary`, reading `items` whole."""

    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"items": Incremental()},
        outputs=Output("copy", key="id"),
        version=copy_v,
    )
    def copy(ctx, items: list):
        return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

    @asset(inputs={"items": "items"})
    def summary(items: list):
        return {"n": len(items)}

    return Project(
        assets=[items, copy, summary],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside)},
        default_store=FileStore(root / "out"),
    )


async def reasons(engine, name) -> list[str]:
    [row] = (await engine.partition_statuses([name], every=False))[name]
    return row.get("reasons", []) if row["status"] == "stale" else []


async def built(state, tmp_path, **kw):
    outside = External()
    engine = make_engine(state, project(tmp_path, outside, **kw))
    await engine.initialize()
    outside.feed.update(k1="1", k2="1")
    await engine.commit_source("feed", upsert=["k1", "k2"])
    await drive(engine, await engine.submit(["copy", "summary"], upstream=True))
    return engine, outside


async def test_staleness_runs_down_the_lineage(state, tmp_path):
    """feed commits: items' input changed; copy and summary read an items
    that has not moved, yet are stale because items is. Once items reruns,
    their own inputs changed; once they rerun, all are fresh."""

    engine, outside = await built(state, tmp_path)
    assert [await reasons(engine, n) for n in ("items", "copy", "summary")] == [[], [], []]
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert=["k1"])
    assert await reasons(engine, "items") == ["input changed"]
    assert await reasons(engine, "copy") == ["upstream stale"]
    assert await reasons(engine, "summary") == ["upstream stale"]
    assert (await engine.asset_statuses())["copy"]["stale"] is True
    await drive(engine, await engine.submit(["items"]))
    assert await reasons(engine, "items") == []
    assert await reasons(engine, "copy") == ["input changed"]
    assert await reasons(engine, "summary") == ["input changed"], "a whole input at another version"
    await drive(engine, await engine.submit(["copy", "summary"]))
    assert [await reasons(engine, n) for n in ("items", "copy", "summary")] == [[], [], []]
    assert (await engine.asset_statuses())["copy"]["stale"] is False


async def test_a_partition_may_be_stale_for_several_reasons(state, tmp_path):
    """copy's version is bumped while items is stale: both reasons, in a
    stable order. Rerunning items leaves copy's input changed and its
    definition; a default run of copy clears both."""

    engine, outside = await built(state, tmp_path)
    await engine.stop()
    engine = make_engine(state, project(tmp_path, outside, copy_v="2"))
    await engine.initialize()
    outside.feed["k2"] = "2"
    await engine.commit_source("feed", upsert=["k2"])
    assert await reasons(engine, "copy") == ["upstream stale", "definition changed"]
    await drive(engine, await engine.submit(["items"]))
    assert await reasons(engine, "copy") == ["input changed", "definition changed"]
    await drive(engine, await engine.submit(["copy"]))
    assert await reasons(engine, "copy") == []


async def test_stale_keys_follow_the_partition_and_say_why(state, tmp_path):
    """A keyed output that is not `each` has all its keys stale or none; an
    unkeyed one has no keys. The route and the engine agree."""

    engine, outside = await built(state, tmp_path)
    assert await engine.stale_keys("copy") == {"tracked": True, "keys": [], "next": None, "reasons": []}
    outside.feed["k1"] = "2"
    await engine.commit_source("feed", upsert=["k1"])
    stale = await engine.stale_keys("copy")
    assert stale["keys"] == ["k1", "k2"] and stale["reasons"] == ["upstream stale"]
    assert (await engine.stale_keys("summary"))["tracked"] is False
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
        base = f"/api/projects/{engine.manifest['name']}"
        answer = await client.get(f"{base}/assets/copy/stale-keys", params={"limit": 1})
        assert answer.status_code == 200
        assert answer.json() == {"tracked": True, "keys": ["k1"], "next": "k1", "reasons": ["upstream stale"]}
