"""The contract's running example (docs/behaviors.md, "The running example"),
as a user declares it:

    feed (a keyed source: keys k1, k2, … each at a version)
      └─▶ items   keyed, incremental over feed, key "id"
            ├─▶ tally     plain incremental over items; a count: before + added − removed
            ├─▶ copy      plain incremental over items; a keyed copy of it
            └─▶ checks    per key over items
                  └─▶ filtered   per key over checks, include=["k1"]
    factor (an unkeyed source; tally and checks read it whole)

Every producer records what each of its batches received (`Example.seen`),
as a user's code would see it; nothing is read from the engine but its API."""

from __future__ import annotations

from dataclasses import dataclass, field

from solera.sdk import In, Incremental, Loaded, Output, Project, Source, asset, source
from solera.stores import FileStore

from tests.contract.harness import Outside, Solera, SourceStore, rebuild


@dataclass
class Batch:
    """What one batch gave a producer."""

    added: list
    updated: list
    removed: list
    unchanged: list
    rows: list
    index: int
    first: bool
    final: bool
    reset: bool
    held: object = None  # what `ctx.load()` returned, for `tally`
    context: dict = field(default_factory=dict)  # the whole inputs it read

    @classmethod
    def of(cls, batch, rows, **kw) -> Batch:
        return cls(
            added=sorted(batch.added),
            updated=sorted(batch.updated),
            removed=sorted(batch.removed),
            unchanged=sorted(batch.unchanged),
            rows=sorted(r["id"] for r in rows),
            index=batch.index,
            first=batch.first,
            final=batch.final,
            reset=batch.reset,
            **kw,
        )

    @property
    def classes(self) -> tuple[list, list, list]:
        return self.added, self.updated, self.removed


class Example:
    """The running example served by `solera`. `deploy(**decl)` serves it
    again with another declaration: `include=` (tally's, copy's and checks'
    patterns), `batch_size=`, `version=` (tally's and checks'), `items_store=`
    ("other" moves items), `copy_name=`."""

    def __init__(self, solera: Solera):
        self.solera, self.outside = solera, Outside()
        self.seen: dict[str, list[Batch]] = {}
        self.calls: dict[str, list[str]] = {}  # a per-key producer's calls, key by key
        self.failing: dict[str, Exception] = {}  # checks' keys that raise, and what
        self.during = None  # a step to take inside tally's next batch
        self.decl: dict = {}

    async def start(self, **decl) -> Example:
        await self.deploy(**decl)
        await self.factor("w1")
        return self

    async def deploy(self, **decl) -> None:
        self.decl = decl
        await self.solera.deploy(self.project(**decl))

    def project(
        self,
        include=None,
        batch_size: int = 16,
        version: str = "1",
        items_store: str | None = None,
        copy_name: str = "copy",
    ) -> Project:
        example, root = self, self.solera.root

        def record(name, batch, rows, **kw):
            example.seen.setdefault(name, []).append(Batch.of(batch, rows, **kw))

        @source
        async def factor(keys, ctx):
            return Loaded(example.outside.knob, version=example.outside.knob)

        @asset(
            inputs={"feed": Incremental(batch_size=batch_size)},
            outputs=Output("items", key="id", store=items_store),
        )
        def items(ctx, feed: list):
            record("items", ctx.batch["feed"], feed)
            return rebuild(ctx.batch["feed"], [{"id": r["id"], "v": r["v"]} for r in feed])

        @asset(
            inputs={"items": Incremental(include=include, batch_size=batch_size), "factor": In()},
            outputs=Output("tally"),
            version=version,
        )
        async def tally(ctx, items: list, factor: str):
            batch = ctx.batch["items"]
            held = await ctx.load()
            record("tally", batch, items, held=held, context={"factor": factor})
            if example.during is not None:
                during, example.during = example.during, None
                await during()
            before = 0 if batch.reset else (held or {"rows": 0})["rows"]
            return {"rows": before + len(batch.added) - len(batch.removed)}

        def copy_fn(ctx, items: list):
            record(copy_name, ctx.batch["items"], items)
            return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

        copy_fn.__name__ = copy_name
        copy = asset(
            inputs={"items": Incremental(include=include, batch_size=batch_size)},
            outputs=Output(copy_name, key="id"),
        )(copy_fn)

        @asset(
            inputs={"row": Incremental("items", each=True, include=include), "factor": In()},
            outputs=Output("checks", key="id"),
            version=version,
        )
        async def checks(ctx, row: list, factor: str):
            example.calls.setdefault("checks", []).append(ctx.key)
            if ctx.key in example.failing:
                raise example.failing[ctx.key]
            return [{"id": ctx.key, "v": row[0]["v"] if row else None, "factor": factor}]

        @asset(
            inputs={"row": Incremental("checks", each=True, include=["k1"])},
            outputs=Output("filtered", key="id"),
        )
        async def filtered(ctx, row: list):
            example.calls.setdefault("filtered", []).append(ctx.key)
            return [{"id": ctx.key, "v": row[0]["v"] if row else None}]

        return Project(
            assets=[items, tally, copy, checks, filtered],
            sources=[Source("feed", key="id", store="ext"), factor],
            stores={"ext": SourceStore(root / "ext", self.outside), "other": FileStore(root / "other")},
            default_store=FileStore(root / "default"),
        )

    # -- what a user does ----------------------------------------------------------

    async def feed(self, upserts: dict | None = None, removes=(), items: bool = True) -> None:
        """`feed` changes outside and is committed; `items` follows unless told not to."""

        self.outside.feed.update(upserts or {})
        for k in removes:
            self.outside.feed.pop(k, None)
        await self.solera.commit_source("feed", upsert=dict(upserts or {}), remove=list(removes))
        if items:
            await self.run("items")

    async def factor(self, value: str) -> None:
        self.outside.knob = value
        await self.solera.commit_source("factor", version=value)

    async def run(self, name: str = "tally", **fields) -> list[Batch]:
        """A run of `name` that succeeds: the batches its producer received."""

        before = len(self.seen.get(name, []))
        await self.solera.run(name, **fields)
        return self.seen.get(name, [])[before:]

    async def count(self) -> int:
        return (await self.solera.read("tally") or {"rows": 0})["rows"]

    async def stale(self, name: str = "tally") -> list:
        return await self.solera.stale(name)
