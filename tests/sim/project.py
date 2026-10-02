"""The project the simulation deploys, in variants a re-registration moves
between, and what each output must hold once the system is quiet: every
producer is a pure function of its inputs, so the oracle computes every
output from the sources alone.

    feed (keyed source) ──Incremental──▶ items ──Incremental(page 2)──▶ copy
    knob (version) ──dep──▶ per_site[site ∈ sites] ──AllPartitions──▶ summary
    knob ──dep──▶ log (batches) ──Incremental──▶ tally
    items ──Each(page 2)──▶ checks (fails while a key is flaky)
    outside (keyed source) ◀── watch (a sensor over an external map)

Variants (`Variant`): `items` on a FileStore or the simulation's fenced
table store; its declared version; `copy` renamed to `mirror` (with an
alias); `summary` removed; `copy` excluding keys `k1*`.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, replace

from solera.errors import Transient
from solera.sdk import (
    AllPartitions,
    Automation,
    AutoRefresh,
    Commit,
    Each,
    Every,
    Incremental,
    Output,
    PartitionSet,
    Project,
    Result,
    Retry,
    Source,
    Tick,
    asset,
    sensor,
)
from solera.stores import FileStore, Keys, Patch

from .stores import Database, TableStore


@dataclass(frozen=True)
class Variant:
    items_store: str = "file"  # "file" | "table" | "pg"
    alt: str = "table"  # the store `items` moves to from FileStore
    items_version: str = "1"
    copy_name: str = "copy"  # "copy" | "mirror" (renamed, aliases=["copy"])
    summary: bool = True
    exclude: str | None = None  # copy's edge: keys it leaves out

    def label(self) -> str:
        return (
            f"items@{self.items_store}/v{self.items_version} {self.copy_name}"
            f"{' -' + self.exclude if self.exclude else ''}{'' if self.summary else ' no-summary'}"
        )


VARIANTS = {
    "table": lambda v: replace(v, items_store=v.alt if v.items_store == "file" else "file"),
    "bump": lambda v: replace(v, items_version=str(int(v.items_version) + 1)),
    "rename": lambda v: replace(v, copy_name="mirror" if v.copy_name == "copy" else "copy"),
    "summary": lambda v: replace(v, summary=not v.summary),
    "exclude": lambda v: replace(v, exclude=None if v.exclude else "k1*"),
}


class External:
    """The world outside: what the `watch` sensor observes, and which keys
    `checks` currently fails on (a flaky API)."""

    def __init__(self):
        self.keys: dict[str, str] = {}
        self.flaky: set[str] = set()


class SourceStore(FileStore):
    """A keyed source's rows live elsewhere: a load answers from the
    selection, each key with the version the index holds for it."""

    async def load(self, ref, t, selection):
        if isinstance(selection, Keys):
            return [{"id": k, "v": _text(v[0])} for k, v in sorted(selection.revisions.items())]
        return []


def _text(version) -> str:
    return version.decode() if isinstance(version, bytes) else str(version)


def f_items(v: str, version: str) -> str:
    return f"{v}.{version}"


def build(variant: Variant, data_root, db: Database, outside: External, pg: str | None = None) -> Project:
    """The project of `variant`, its FileStore under `data_root`, its table
    store on `db`, and — given a schema `pg` — a PostgresStore writing there."""

    if variant.items_store == "pg":
        items_output = Output(
            "items", key="id", revision="v", store="pg", schema=pg, columns={"id": "text", "v": "text"}
        )
    else:
        items_output = Output(
            "items", key="id", revision="v", store="db" if variant.items_store == "table" else None
        )

    @asset(
        outputs=items_output,
        inputs={"feed": Incremental()},
        version=variant.items_version,
        on_version_change="full",
        automations=AutoRefresh(),
        retries=Retry(3, delay=1.0),
        timeout=300,
    )
    def items(ctx, feed: list):
        rows = [{"id": r["id"], "v": f_items(r["v"], variant.items_version)} for r in feed]
        return Patch(rows, remove=list(ctx.changes["feed"].deleted))

    copy_kw = {"aliases": ["copy"]} if variant.copy_name == "mirror" else {}

    def copy_fn(ctx, items: list):
        changes = ctx.changes["items"]
        return Patch([{"id": r["id"], "v": r["v"]} for r in items], remove=list(changes.deleted))

    copy_fn.__name__ = variant.copy_name
    copy = asset(
        copy_fn,
        outputs=Output(key="id", revision="v"),
        inputs={"items": Incremental(page_size=2, exclude=[variant.exclude] if variant.exclude else None)},
        automations=AutoRefresh(),
        retries=Retry(3, delay=1.0),
        timeout=300,
        **copy_kw,
    )

    @asset(
        partitions="sites",
        deps=["knob"],
        automations=[AutoRefresh(), Automation(trigger=Every(60), partitions="missing")],
    )
    def per_site(ctx):
        return {"site": ctx.partition}

    @asset(deps=["knob"], outputs=Output("log", incremental=True), automations=AutoRefresh())
    def log(ctx):
        return Patch([{"site": "*", "n": 1}])

    @asset(inputs={"log": Incremental()})
    def tally(ctx, log: list):
        changes = ctx.changes["log"]
        base = 0 if (changes.full and changes.first) or ctx.cursor is None else ctx.cursor
        total = base + len(log)
        return Result(outputs={"tally": {"rows": total}}, cursor=total)

    @asset(
        inputs={"item": Each("items", page_size=2, concurrency=2)},
        outputs=Output("checks", key="id", revision="w"),
        automations=AutoRefresh(),
        retries=Retry(3, delay=1.0),
        timeout=300,
    )
    async def checks(ctx, item: list):
        if ctx.key in outside.flaky:
            raise Transient(f"{ctx.key} is flaky", retry_after=5)
        return [{"w": f"x{item[0]['v']}"}]

    assets = [items, copy, per_site, log, tally, checks]
    if variant.summary:

        @asset(inputs={"per_site": AllPartitions()}, automations=AutoRefresh())
        def summary(per_site: dict[str, dict]):
            return sorted(per_site)

        assets.append(summary)

    @sensor(every=30, commits=["outside"])
    def watch(ctx):
        return Tick(commits=[Commit("outside", keys=dict(outside.keys))])

    return Project(
        assets=assets,
        sources=[
            Source("feed", key="id", store="ext"),
            Source("knob"),
            Source("outside", key="id", store="ext"),
            PartitionSet("sites"),
        ],
        sensors=[watch],
        stores={"ext": SourceStore(data_root), "db": TableStore(db), **_postgres(pg)},
        default_store=FileStore(data_root),
        build="sim",
        name="sim",
    )


def expected_items(feed: dict[str, str], variant: Variant) -> dict[str, str]:
    return {k: f_items(v, variant.items_version) for k, v in feed.items()}


def _postgres(schema: str | None) -> dict:
    if schema is None:
        return {}
    from solera_postgres import PostgresStore

    from .postgres import DSN

    return {"pg": PostgresStore(DSN)}


def expected_checks(items: dict[str, str]) -> dict[str, str]:
    return {k: f"x{v}" for k, v in items.items()}


def expected_copy(items: dict[str, str], variant: Variant) -> dict[str, str]:
    if variant.exclude is None:
        return dict(items)
    return {k: v for k, v in items.items() if not fnmatch.fnmatchcase(k, variant.exclude)}
