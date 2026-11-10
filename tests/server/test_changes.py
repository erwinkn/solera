"""K44: a keyed batch tells its consumer each key's change since the
input's position — added, updated or removed, net over the batch's
commits — and `ctx.load()` reads the consumer's own output at its pin, so
a total kept from the changes alone stays exact. On every built-in store
for the upstream (docs/architecture.md, "Added, updated, removed")."""

import asyncio

import pytest
from solera.sdk import Incremental, Output, Project, Source, asset
from solera.stores import FileStore, Patch
from solera_server.state import State

from tests.sim.oracle import value_content
from tests.sim.project import External, SourceStore, rebuild
from tests.staleness import Holdings

from .engines import drive, make_engine, status_of
from .test_staleness import STORES, _store


def project(root, outside, store, decl, seen: list):
    @asset(inputs={"feed": Incremental()}, outputs=Output("items", key="id", store="x", **decl))
    def items(ctx, feed: list):
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(inputs={"items": Incremental()}, outputs=Output("tally"))
    async def tally(ctx, items: list):
        batch = ctx.batch["items"]
        seen.append((list(batch.added), list(batch.updated), list(batch.removed)))
        before = ((await ctx.load()) or {"rows": 0})["rows"]
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
        assert await tally({"items": ["k2"]}) == (([], [], ["k2"]), 2)
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
            before = ((await ctx.load()) or {"rows": 0})["rows"]
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
    ["update", "add", "remove"],
)
async def test_a_change_during_a_full_pass_is_counted_once(tmp_path, change):
    """A19 R1, R2: keys=(k1) starts `tally`'s full pass; the upstream then
    changes; a default run completes the pass. The pass reads its snapshot,
    so k1 updated, k3 added or k1 removed arrives once, by the next delta
    (D93)."""

    t = await Tally().start(tmp_path)
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run(keys={"items": ["k1"]})
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


async def test_a_selection_completes_a_full_pass_only_with_the_removals_it_owes(tmp_path):
    """A19 R3: keys=(k1) starts the full pass and delivers k1; k1 is removed
    upstream; keys=(k2) then delivers the rest of the snapshot. k1's removal
    is still owed: the next run delivers it, and the count is 1."""

    t = await Tally().start(tmp_path)
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run(keys={"items": ["k1"]})
        await t.feed(removes=["k1"])
        await t.run(keys={"items": ["k2"]})
        await t.run()
        await t.exact()
    finally:
        await t.close()


async def test_a_selection_during_a_pattern_change_is_counted_once(tmp_path):
    """A19 R4: `tally` counts include=(k1); the patterns widen to k*; before
    the membership diff, keys=(k2) delivers k2. It is recorded (D93), so the
    diff does not add k2 again: the count is 2."""

    t = await Tally().start(tmp_path, include=["k1"])
    try:
        await t.feed({"k1": "1", "k2": "1"})
        await t.run()
        await t.deploy(include=["k*"])
        await t.run(keys={"items": ["k2"]})
        await t.run()
        await t.exact()
    finally:
        await t.close()


async def test_an_early_removal_from_a_current_only_source_is_delivered_once(tmp_path):
    """A19 R5: a current-only source, a key a batch: k1 and k2 updated;
    while k1's batch runs the source removes k2, and k2's batch, planned
    before that commit, finds no row. The source says so: k2, held, is
    removed then (docs/observed-set.md, "Observations"), and the commit
    after owes nothing more. The count goes 2, 2, 2, 1."""

    outside, holdings, removing, counts = External(), Holdings(), {}, []
    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)

    @asset(inputs={"feed": Incremental(batch_size=1)}, outputs=Output("tally"))
    async def tally(ctx, feed: list):
        batch = ctx.batch["feed"]
        holdings.apply(batch)
        before = ((await ctx.load()) or {"rows": 0})["rows"]
        if removing.pop("now", False):
            outside.feed.pop("k2")
            await removing["engine"].commit_source("feed", remove=["k2"])
        counts.append(before + len(batch.added) - len(batch.removed))
        return {"rows": counts[-1]}

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
        assert counts[2:] == [2, 1], (counts, holdings.trace)
        assert holdings.trace[2:] == [([], ["k1"], []), ([], [], ["k2"])]
        assert (await value_content(engine, p, "tally"))["rows"] == 1
    finally:
        await engine.stop()
        await state.close()


async def test_a_task_walks_as_long_as_its_longest_input(state, tmp_path):
    """Two keyed inputs walk together, a batch of each per attempt: with two
    keys in `a` and six in `b`, in batches of two, the task runs three
    batches. Its progress and batches are the task's, not the first input's:
    `a` ends in batch 1, but neither that batch nor the task reads final until
    `b` does, and every batch counts the keys of both."""

    @asset(outputs=Output("a", key="id"))
    def a():
        return [{"id": f"a{i}"} for i in range(2)]

    @asset(outputs=Output("b", key="id"))
    def b():
        return [{"id": f"b{i}"} for i in range(6)]

    @asset(inputs={"a": Incremental(batch_size=2), "b": Incremental(batch_size=2)}, outputs=Output("out"))
    def out(ctx, a: list, b: list):
        return {"n": len(a) + len(b)}

    project = Project(assets=[a, b, out], default_store=FileStore(tmp_path / "data"))
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["out"], upstream=True))
    assert status_of(detail) == "succeeded"
    (task,) = [t for t in detail["tasks"] if t["asset"] == "out"]
    batches = [x["batch"] for x in detail["attempts"][task["id"]]]
    assert [(x["index"], x["count"], x["final"], x["added"], x["last"]) for x in batches] == [
        (0, 3, False, 4, "b1"),
        (1, 3, False, 2, "b3"),
        (2, 3, True, 2, None),
    ]
    assert task["progress"] == {"batch": 2, "key": None}
    await engine.stop()


async def test_a_batch_goes_on_with_an_unkeyed_walk(state, tmp_path):
    """A keyed input of two keys and an unkeyed one of six commits, in
    batches of two: the keyed walk ends in batch 1, the unkeyed one in batch
    3. Every attempt says its batch, numbered on, and only the third is
    final; the batches after the keyed walk ended have no key range."""

    @asset(outputs=Output("a", key="id"))
    def a():
        return [{"id": f"a{i}"} for i in range(2)]

    @asset(outputs=Output("c", incremental=True))
    def c(ctx):
        return Patch([{"n": ctx.config.get("n", 0)}])

    @asset(inputs={"a": Incremental(batch_size=2), "c": Incremental(batch_size=2)}, outputs=Output("out"))
    def out(ctx, a: list, c: list):
        return {"n": len(a)}

    project = Project(assets=[a, c, out], default_store=FileStore(tmp_path / "data"))
    engine = make_engine(state, project)
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["a"]))) == "succeeded"
    for n in range(6):
        assert status_of(await drive(engine, await engine.submit(["c"], config={"n": n}))) == "succeeded"
    detail = await drive(engine, await engine.submit(["out"]))
    assert status_of(detail) == "succeeded"
    (task,) = detail["tasks"]
    batches = [x["batch"] for x in detail["attempts"][task["id"]]]
    assert [(x["index"], x["count"], x["final"], x["added"], x["last"]) for x in batches] == [
        (0, 3, False, 2, None),
        (1, 3, False, 0, None),
        (2, 3, True, 0, None),
    ]
    assert batches[0]["after"] is None and batches[1]["after"] is None
    assert task["progress"] == {"batch": 2, "key": None}
    await engine.stop()
