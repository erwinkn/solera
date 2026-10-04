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


# -- A19: delivery accounting (D93: one rule) ----------------------------------------------


def a19(finding: str):
    """A strict xfail for an A19 finding until its fix (D93)."""

    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"A19 {finding}")


class Holdings:
    """What the consumer holds, from its batches alone: each batch's classes
    must agree with it — `added` a key it lacks, `updated` and `removed` one
    it holds — and a start-over (`full and first`) empties it. The count it
    keeps is its size. A disagreement is recorded, not raised: raised in the
    producer, it would only fail the attempt."""

    def __init__(self):
        self.held: set[str] = set()
        self.trace: list[tuple] = []
        self.wrong: list[str] = []

    def apply(self, batch) -> None:
        if batch.full and batch.first:
            self.held.clear()
        added, updated, removed = set(batch.added), set(batch.updated), set(batch.removed)
        self.trace.append((sorted(added), sorted(updated), sorted(removed)))
        if added & self.held:
            self.wrong.append(f"added again: {sorted(added & self.held)}")
        if (updated | removed) - self.held:
            self.wrong.append(f"never held: {sorted((updated | removed) - self.held)}")
        self.held = (self.held | added) - removed

    def check(self, keys: set[str]) -> None:
        assert not self.wrong and self.held == keys, (self.wrong, self.trace)


class Tally:
    """`feed` (a keyed source on a current-only store) -> `items` (keyed) ->
    `tally`, a count kept from the changes alone: before + added - removed."""

    async def start(self, root, include=None, batch_size=16):
        self.root, self.outside, self.holdings = root, External(), Holdings()
        self.state = await State.open((root / "state").as_uri(), "test", flush_interval=0.001)
        self.engine = None
        await self.deploy(include, batch_size)
        return self

    def project(self, include, batch_size):
        holdings = self.holdings

        @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id"))
        def items(ctx, feed: list):
            return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

        @asset(inputs={"items": Incremental(include=include, batch_size=batch_size)}, outputs=Output("tally"))
        async def tally(ctx, items: list):
            batch = ctx.batch["items"]
            holdings.apply(batch)
            before = 0 if batch.full and batch.first else (await ctx.load())["rows"]
            return {"rows": before + len(batch.added) - len(batch.removed)}

        return Project(
            assets=[items, tally],
            sources=[Source("feed", key="id", store="ext")],
            stores={"ext": SourceStore(self.root / "ext", self.outside)},
            default_store=FileStore(self.root / "default"),
        )

    async def deploy(self, include=None, batch_size=16):
        if self.engine is not None:
            await self.engine.stop()
        self.p = self.project(include, batch_size)
        self.engine = make_engine(self.state, self.p)
        await self.engine.initialize()

    async def feed(self, upserts=None, removes=()):
        self.outside.feed.update(upserts or {})
        for k in removes:
            self.outside.feed.pop(k, None)
        await self.engine.commit_source("feed", upsert=dict(upserts or {}), remove=list(removes))
        await self.run("items")

    async def run(self, name="tally", keys=None):
        detail = await drive(self.engine, await self.engine.submit([name], keys=keys))
        assert detail["request"]["status"] == "succeeded", detail["request"]

    async def count(self) -> int:
        return (await value_content(self.engine, self.p, "tally"))["rows"]

    async def exact(self, taken=lambda k: True) -> None:
        """The count is the upstream's, the holdings are its keys, and nothing is stale."""

        keys = {k for k in self.outside.feed if taken(k)}
        self.holdings.check(keys)
        assert await self.count() == len(keys)
        assert await self.engine.stale_reasons("tally", "") == []

    async def close(self):
        await self.engine.stop()
        await self.state.close()


@pytest.mark.parametrize(
    "change",
    [pytest.param("update", marks=a19("R1")), pytest.param("add", marks=a19("R2")), "remove"],
)
async def test_a_change_during_a_full_pass_is_counted_once(tmp_path, change):
    """A19 R1, R2: keys=(k1) starts `tally`'s full pass; the upstream then
    changes; a default run completes the pass. The pass reads its snapshot,
    so k1 updated, k3 added or k1 removed arrives once, by the next delta
    (D93)."""

    t = await Tally().start(tmp_path)
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run(keys={"items": {"keys": ["k1"]}})
        await t.feed(
            **{"update": {"upserts": {"k1": "2"}}, "add": {"upserts": {"k3": "1"}}}.get(
                change, {"removes": ["k1"]}
            )
        )
        await t.run()
        await t.run()  # what came after the snapshot, if the first did not take it
        await t.exact()
    finally:
        await t.close()


@a19("R3")
async def test_a_selection_completes_a_full_pass_only_with_the_removals_it_owes(tmp_path):
    """A19 R3: keys=(k1) starts the full pass and delivers k1; k1 is removed
    upstream; keys=(k2) then delivers the rest of the snapshot. k1's removal
    is still owed: the next run delivers it, and the count is 1."""

    t = await Tally().start(tmp_path)
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run(keys={"items": {"keys": ["k1"]}})
        await t.feed(removes=["k1"])
        await t.run(keys={"items": {"keys": ["k2"]}})
        await t.run()
        await t.exact()
    finally:
        await t.close()


@a19("R4")
async def test_a_selection_during_a_pattern_change_is_counted_once(tmp_path):
    """A19 R4: `tally` counts include=(k1); the patterns widen to k*; before
    the membership diff, keys=(k2) delivers k2. It is recorded (D93), so the
    diff does not add k2 again: the count is 2."""

    t = await Tally().start(tmp_path, include=["k1"])
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run()
        await t.deploy(include=["k*"])
        await t.run(keys={"items": {"keys": ["k2"]}})
        await t.run()
        await t.exact()
    finally:
        await t.close()


@a19("R5")
async def test_an_early_removal_from_a_current_only_source_is_delivered_once(tmp_path):
    """A19 R5: a delta over a current-only source, a key a batch: after k1's
    batch the source removes k2, so k2's batch finds it gone and delivers its
    removal early (F38). It is recorded (D93), so the delta that carries the
    removal does not deliver it again: the count is 1."""

    outside, holdings, removing = External(), Holdings(), {}
    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)

    @asset(inputs={"feed": Incremental(batch_size=1)}, outputs=Output("tally"))
    async def tally(ctx, feed: list):
        batch = ctx.batch["feed"]
        holdings.apply(batch)
        before = 0 if batch.full and batch.first else (await ctx.load())["rows"]
        if removing.pop("now", False):
            outside.feed.pop("k2")
            await removing["engine"].commit_source("feed", remove=["k2"])
        return {"rows": before + len(batch.added) - len(batch.removed)}

    p = Project(
        assets=[tally],
        sources=[Source("feed", key="id", store="ext")],
        stores={"ext": SourceStore(tmp_path / "ext", outside)},
        default_store=FileStore(tmp_path / "default"),
    )
    engine = removing["engine"] = make_engine(state, p)
    try:
        await engine.initialize()
        outside.feed.update({"k1": "1", "k2": "1"})
        await engine.commit_source("feed", upsert=dict(outside.feed))
        await drive(engine, await engine.submit(["tally"]))
        outside.feed.update({"k1": "2", "k2": "2"})
        await engine.commit_source("feed", upsert=dict(outside.feed))
        removing["now"] = True
        await drive(engine, await engine.submit(["tally"]))
        await drive(engine, await engine.submit(["tally"]))
        holdings.check({"k1"})
        assert (await value_content(engine, p, "tally"))["rows"] == 1
    finally:
        await engine.stop()
        await state.close()
