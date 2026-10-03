"""The project the simulation deploys, in variants a re-registration moves
between, and what each output must hold once the system is quiet: every
producer is a pure function of its inputs, so the oracle computes every
output from the sources alone.

    feed (keyed source) ──Incremental──▶ items ──Incremental(page 2)──▶ copy
    knob (version) ──dep──▶ per_site[site ∈ sites] ──AllPartitions──▶ summary
    knob ──dep──▶ log (batches) ──Incremental──▶ tally
    items ──Each(page 2)──▶ checks (fails while a key is flaky, by error class)
    items ──Incremental(page 2)──▶ split ──▶ odd (table store), even (FileStore); on a pool
    outside (keyed source) ◀── watch (a sensor over an external map; runs per_site when it changed)

Variants (`Variant`): `items` on a FileStore or the simulation's fenced
table store; its declared version; `copy` renamed to `mirror` (with an
alias); `summary` removed; `copy` excluding keys `k1*`.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, replace

from solera.errors import Abort, Failed, Rejected, Transient
from solera.executors import Pool
from solera.sdk import (
    AllPartitions,
    Automation,
    AutoRefresh,
    Commit,
    DynamicPartitions,
    Each,
    Every,
    Incremental,
    Output,
    Project,
    Result,
    Retry,
    RunRequest,
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


POOL = "pool"  # the executor `split` runs on: workers pull its attempts


class External:
    """The world outside: the `feed` its clients write, what the `watch`
    sensor observes (`keys`), and which keys `checks` currently fails on (a
    flaky API), with the error class it raises (`FLAKY`)."""

    def __init__(self):
        self.feed: dict[str, str] = {}
        self.keys: dict[str, str] = {}
        self.flaky: dict[str, str] = {}
        self.seen: dict[str, str] | None = None  # what `watch` saw on its last tick
        self.broken = False  # `watch` raises


# How `checks` fails on a flaky key, by error class (docs/per-key-processing.md):
# retried on its own backoff, once per deploy, when its input changes, or the
# whole attempt per `retries=`.
FLAKY = {
    "transient": lambda key: Transient(f"{key} is flaky", retry_after=5),
    "failed": lambda key: Failed(f"{key} is broken"),
    "rejected": lambda key: Rejected(f"{key} is refused"),
    "abort": lambda key: Abort(f"the API is down at {key}"),
}


class SourceStore(FileStore):
    """A keyed source's rows live elsewhere, and are read as they are now
    (docs/versions.md §6): a load answers each selected key the outside
    holds with its current value — which may be newer than the commit that
    named it."""

    def __init__(self, path, outside: External):
        super().__init__(path)
        self.outside = outside

    async def load(self, ref, t, selection):
        current = self.outside.feed if ref.output == "feed" else self.outside.keys
        if isinstance(selection, Keys):
            return [{"id": k, "v": current[k]} for k in sorted(selection.generations) if k in current]
        return []


def rebuild(changes, rows: list[dict]):
    """A keyed consumer's write for one page: a full delivery (a reset) starts
    the output over on its first page (architecture.md §5), then patches."""

    if changes.full and changes.first:
        return rows
    return Patch(rows, remove=list(changes.removed))


def f_items(v: str, version: str) -> str:
    return f"{v}.{version}"


def build(variant: Variant, data_root, db: Database, outside: External, pg: str | None = None) -> Project:
    """The project of `variant`, its FileStore under `data_root`, its table
    store on `db`, and — given a schema `pg` — a PostgresStore writing there."""

    if variant.items_store == "pg":
        items_output = Output("items", key="id", store="pg", schema=pg, columns={"id": "text", "v": "text"})
    else:
        items_output = Output("items", key="id", store="db" if variant.items_store == "table" else None)

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
        return rebuild(ctx.batch["feed"], rows)

    copy_kw = {"aliases": ["copy"]} if variant.copy_name == "mirror" else {}

    def copy_fn(ctx, items: list):
        return rebuild(ctx.batch["items"], [{"id": r["id"], "v": r["v"]} for r in items])

    copy_fn.__name__ = variant.copy_name
    copy = asset(
        copy_fn,
        outputs=Output(key="id"),
        inputs={"items": Incremental(batch_size=2, exclude=[variant.exclude] if variant.exclude else None)},
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
        changes = ctx.batch["log"]
        base = 0 if (changes.full and changes.first) or ctx.cursor is None else ctx.cursor
        total = base + len(log)
        return Result(outputs={"tally": {"rows": total}}, cursor=total)

    @asset(
        inputs={"item": Each("items", batch_size=2, concurrency=2)},
        outputs=Output("checks", key="id"),
        automations=AutoRefresh(),
        retries=Retry(3, delay=1.0),
        timeout=300,
    )
    async def checks(ctx, item: list):
        if ctx.key in outside.flaky:
            raise FLAKY[outside.flaky[ctx.key]](ctx.key)
        return [{"w": f"x{item[0]['v']}"}]

    @asset(
        outputs=[Output("odd", key="id", store="db"), Output("even", key="id")],
        inputs={"items": Incremental(batch_size=2)},
        automations=AutoRefresh(),
        retries=Retry(3, delay=1.0),
        timeout=300,
        executor=Pool(POOL)(cpu=2),
    )
    def split(ctx, items: list):
        """Each key in `odd` or `even` by its feed version: a key whose
        version flips moves from one output to the other in one commit, one
        output fenced, the other immutable. Its attempts run on a pool."""

        changes = ctx.batch["items"]
        odd = [r for r in items if is_odd(r["v"])]
        even = [r for r in items if not is_odd(r["v"])]
        if changes.full and changes.first:
            return Result(outputs={"odd": odd, "even": even})
        gone = list(changes.removed)
        return Result(
            outputs={
                "odd": Patch(odd, remove=[r["id"] for r in even] + gone),
                "even": Patch(even, remove=[r["id"] for r in odd] + gone),
            }
        )

    assets = [items, copy, per_site, log, tally, checks, split]
    if variant.summary:

        @asset(inputs={"per_site": AllPartitions()}, automations=AutoRefresh())
        def summary(per_site: dict[str, dict]):
            return sorted(per_site)

        assets.append(summary)

    @sensor(every=30, commits=["outside"])
    def watch(ctx):
        """Commits what it sees; when that changed since its last tick, also
        asks for a run of every `per_site` partition."""

        if outside.broken:
            raise ConnectionError("the outside cannot be reached")
        seen, outside.seen = outside.seen, dict(outside.keys)
        runs = [RunRequest("per_site", partitions="all")] if seen != outside.keys else []
        return Tick(commits=[Commit("outside", keys=dict(outside.keys))], runs=runs)

    return Project(
        assets=assets,
        sources=[
            Source("feed", key="id", store="ext"),
            Source("knob"),
            Source("outside", key="id", store="ext"),
            DynamicPartitions("sites"),
        ],
        sensors=[watch],
        stores={"ext": SourceStore(data_root, outside), "db": TableStore(db), **_postgres(pg)},
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


def is_odd(v: str) -> bool:
    """Whether an `items` value comes from an odd feed version."""

    return int(v.split(".", 1)[0]) % 2 == 1


def expected_split(items: dict[str, str]) -> tuple[dict[str, str], dict[str, str]]:
    odd = {k: v for k, v in items.items() if is_odd(v)}
    return odd, {k: v for k, v in items.items() if k not in odd}


def expected_checks(items: dict[str, str]) -> dict[str, str]:
    return {k: f"x{v}" for k, v in items.items()}


def expected_copy(items: dict[str, str], variant: Variant) -> dict[str, str]:
    if variant.exclude is None:
        return dict(items)
    return {k: v for k, v in items.items() if not fnmatch.fnmatchcase(k, variant.exclude)}
