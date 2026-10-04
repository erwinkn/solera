"""K44: a keyed batch tells its consumer each key's change since the
input's position — added, updated or removed, net over the batch's
commits — and `ctx.load()` reads the consumer's own output at its pin, so
a total kept from the changes alone stays exact. On every built-in store
for the upstream (docs/architecture.md, "Added, updated, removed")."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, Source, asset
from solera.stores import FileStore
from solera_server.state import State

from tests.sim.oracle import value_content
from tests.sim.project import External, SourceStore, rebuild

from .engines import drive, make_engine
from .test_staleness import STORES, _store


def project(root, outside, store, decl, seen: list):
    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store="x", **decl))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"items": Incremental()}, outputs=Output("tally"))
    async def tally(ctx, items: list):
        batch = ctx.batch["items"]
        seen.append((list(batch.added), list(batch.updated), list(batch.removed)))
        before = 0 if batch.full and batch.first else (await ctx.load())["rows"]
        return {"rows": before + len(batch.added) - len(batch.removed)}

    return Project(
        assets=[items, tally],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(root / "ext", outside), "x": store},
        default_store=FileStore(root / "default"),
    )


async def _history(kind, root):
    store, decl, drop = _store(kind, root)
    outside, seen = External(), []
    state = await State.open((root / "state").as_uri(), "test", flush_interval=0.001)
    p = project(root, outside, store, decl, seen)
    engine = make_engine(state, p)
    try:
        await engine.initialize()

        async def feed(upserts=None, removes=()):
            outside.feed.update(upserts or {})
            for k in removes:
                outside.feed.pop(k, None)
            await engine.commit_source("feed", upsert=dict(upserts or {}), remove=list(removes))
            await drive(engine, await engine.submit(["items"]))

        async def tally(keys=None):
            seen.clear()
            detail = await drive(engine, await engine.submit(["tally"], keys=keys))
            assert detail["request"]["status"] == "succeeded", detail["request"]
            return seen[0] if seen else None, (await value_content(engine, p, "tally"))["rows"]

        await feed({"k1": "1", "k2": "1", "k3": "1"})
        assert await tally() == ((["k1", "k2", "k3"], [], []), 3)  # a first pass: all added

        await feed({"k2": "2", "k4": "1"}, ["k1"])
        assert await tally() == ((["k4"], ["k2"], ["k1"]), 3)

        await feed({"k5": "1"}, ["k3"])
        await feed({"k3": "1"}, ["k5"])
        # k5 added and removed again: nowhere; k3 removed and added back: updated.
        assert await tally() == (([], ["k3"], []), 3)

        await feed(removes=["k2"])
        assert await tally({"items": {"keys": ["k2"]}}) == (([], [], ["k2"]), 2)
        await feed({"k2": "3"})
        # Delivered removed by the keys= run, live now: added, against what that run delivered.
        assert await tally() == ((["k2"], [], []), 3)
    finally:
        await engine.stop()
        await state.close()
        drop()


@pytest.mark.parametrize("kind", STORES)
def test_a_total_kept_from_the_changes_stays_exact(kind, tmp_path):
    asyncio.run(_history(kind, tmp_path))
