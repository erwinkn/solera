"""A world for the observed set's histories (docs/observed-set.md): `feed`, a
keyed source of current rows -> `items` (keyed) -> `tally`, a count kept from
the changes alone, and `checks`, per key over `items`, then `filtered`, per
key over `checks` taking k1 only (an each chain). `factor`, an unkeyed
source, is read whole by `tally` and `checks`: their context.

After every step, `verify()` holds the engine's decode of each observation
record to the literal observed set: `tally`'s as its producer recorded what
each batch gave it (tests/staleness.py, `ObservedSets`), `checks`' as its
output says — each key it holds, at the upstream version it was given."""

from __future__ import annotations

from solera.key_outcomes import StoredOutcome
from solera.sdk import In, Incremental, Loaded, Output, Project, Source, asset, source
from solera.stores import FileStore
from solera_server.state import State

from tests.sim.oracle import index_entries, keyed_content, value_content
from tests.sim.project import External, SourceStore, rebuild
from tests.staleness import ObservedSets, agree, decoded, head_versions

from .engines import drive, make_engine


def project(world: World, *, include=None, batch_size=16, version="1", items_store=None):
    outside, observed = world.outside, world.observed

    @source
    async def factor(keys, ctx):
        return Loaded(outside.knob, version=outside.knob)

    @asset(
        inputs={"feed": Incremental(batch_size=batch_size)},
        outputs=Output("items", key="id", store=items_store),
    )
    def items(ctx, feed: list):
        observed.apply("items", "", "feed", ctx.batch["feed"])
        return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

    @asset(
        inputs={"items": Incremental(include=include, batch_size=batch_size), "factor": In()},
        outputs=Output("tally"),
        version=version,
    )
    async def tally(ctx, items: list, factor: str):
        batch = ctx.batch["items"]
        observed.apply("tally", "", "items", batch, {"factor": factor})
        if world.during is not None:  # a step that happens while this batch runs
            during, world.during = world.during, None
            await during()
        held = {"rows": 0} if batch.reset else (await ctx.load()) or {"rows": 0}
        world.counts.append(held["rows"] + len(batch.added) - len(batch.removed))
        return {"rows": world.counts[-1]}

    @asset(
        inputs={"row": Incremental("items", each=True, include=include), "factor": In()},
        outputs=Output("checks", key="id"),
        version=version,
    )
    async def checks(ctx, row: list, factor: str):
        if ctx.key in outside.flaky:
            raise RuntimeError(f"{ctx.key}: the API is down")
        return [{"id": ctx.key, "g": ctx.generation, "factor": factor}]

    @asset(
        inputs={"row": Incremental("checks", each=True, include=["k1"])}, outputs=Output("filtered", key="id")
    )
    async def filtered(ctx, row: list):
        return [{"id": ctx.key, "g": ctx.generation}]

    return Project(
        assets=[items, tally, checks, filtered],
        sources=[Source("feed", key="id", store="ext"), factor],
        stores={"ext": SourceStore(world.root / "ext", outside), "other": FileStore(world.root / "other")},
        default_store=FileStore(world.root / "default"),
    )


class World:
    async def start(self, root, **deploy) -> World:
        self.root, self.outside, self.observed, self.counts = root, External(), ObservedSets(), []
        self.during = None  # a step to take inside tally's next batch
        self.outside.knob = "w1"
        self.state = await State.open((root / "state").as_uri(), "test", flush_interval=0.001)
        self.engine = None
        await self.deploy(**deploy)
        await self.engine.commit_source("factor", version=self.outside.knob)
        return self

    async def deploy(self, **deploy) -> None:
        """A deploy: the project again, with `include`, `batch_size`, an asset
        `version` (a definition change), or `items_store="other"` (`items`
        moved: an upstream reset)."""

        if self.engine is not None:
            await self.engine.stop()
        self.p = project(self, **deploy)
        self.engine = make_engine(self.state, self.p)
        await self.engine.initialize()

    async def feed(self, upserts=None, removes=(), items=True) -> None:
        """The source changes, and is committed; `items` follows unless told not to."""

        self.outside.feed.update(upserts or {})
        for k in removes:
            self.outside.feed.pop(k, None)
        await self.engine.commit_source("feed", upsert=dict(upserts or {}), remove=list(removes))
        if items:
            await self.run("items")

    async def factor(self, value: str) -> None:
        self.outside.knob = value
        await self.engine.commit_source("factor", version=value)

    async def run(self, name="tally", keys=None, **kw) -> dict:
        detail = await drive(self.engine, await self.engine.submit([name], keys=keys, **kw))
        assert detail["request"]["status"] == "succeeded", detail["request"]
        return detail

    async def verify(self) -> None:
        """The engine's decode of every observation record is the literal
        observed set, as the index can know it (`agree`)."""

        feed, items = await head_versions(self.engine, "feed"), await head_versions(self.engine, "items")
        for consumer, param, head in (("items", "feed", feed), ("tally", "items", items)):
            found, literal = (
                await decoded(self.engine, consumer, "", param),
                self.observed.of(consumer, "", param),
            )
            assert agree(found, literal, head), (consumer, found, literal)
        generations = await keyed_content(self.engine, self.p, "checks", column="g")
        factors = await keyed_content(self.engine, self.p, "checks", column="factor")
        want = {k: (int(g), {"factor": factors[k]}) for k, g in generations.items()}
        found = await decoded(self.engine, "checks", "", "row")
        # A failed key is observed too, at the generation it failed at (A19 R8): its
        # stored outcome says which; the context, its batch's.
        for k, (_, payload) in (await index_entries(self.engine.state, "@checks", "")).items():
            want[k] = (StoredOutcome.decode(payload).upstream, found.get(k, (None, None))[1])
        assert agree(found, want, items), ("checks", found, want)

    async def count(self) -> int:
        return (await value_content(self.engine, self.p, "tally"))["rows"]

    async def stale(self, name: str = "tally") -> list[str]:
        return await self.engine.stale_reasons(name, "")

    async def close(self) -> None:
        await self.engine.stop()
        await self.state.close()
