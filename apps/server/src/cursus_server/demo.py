"""The cursus demo project — every pattern in docs/architecture.md, self-contained.

`uv run cursus serve --insecure` and nothing else: all resources are in-process
fakes. With `DATABASE_URL` set (docker compose up postgres) the relational
outputs go to PostgresStore and the Sql asset materializes in-database.
"""

from __future__ import annotations

import hashlib
import os
import time

from cursus.executors import Pool
from cursus.sdk import (
    AllPartitions,
    Automation,
    AutoRefresh,
    Cron,
    Every,
    Incremental,
    Migration,
    OnDeploy,
    Output,
    PartitionSet,
    Project,
    Result,
    Source,
    TableRef,
    TimePartitions,
    asset,
    job,
)
from cursus.stores import BlobStore, Patch, Sql
from cursus_postgres import PostgresStore

DATABASE = bool(os.getenv("DATABASE_URL"))
RELATIONAL = "postgres" if DATABASE else None  # None → default JsonStore


def migration_log(output_name: str):
    """A callable migration payload (§4): note the output in a side table so a
    `cursus migrate` or first write leaves a visible trace on a fresh database."""

    def apply(cur):
        cur.execute("CREATE TABLE IF NOT EXISTS demo_migrations (output text, name text)")
        cur.execute("INSERT INTO demo_migrations VALUES (%s, 'baseline')", (output_name,))

    return apply


def postgres_migrations(output_name: str, payload=None):
    """Every Postgres output carries one migration; the JsonStore path declares
    none — a store without `migrate` would reject them at registration."""
    if not DATABASE:
        return ()
    return (Migration("baseline", payload or migration_log(output_name)),)


# ---------------------------------------------------------------------------
# Resources: in-process fakes. Deterministic over wall-clock time so separate
# worker subprocesses observe the same world, and repeated polls within a tick
# return identical content — an unchanged commit wakes nothing (§9).
# ---------------------------------------------------------------------------

ALL_SITES = ["alpha", "bravo", "charlie", "delta"]


class SiteRegistry:
    """A pretend SharePoint site list; gains one site on every call."""

    def list_sites(self, seen: int) -> list[str]:
        n = min(2 + seen, len(ALL_SITES))
        return ALL_SITES[:n]


class FeedClient:
    """A pretend delta feed: one new batch per site every five seconds.

    `delta(site, since)` returns (events, token); the token is the cursor.
    Polling inside the same five-second tick returns identical events, so a
    re-run commits the same version and wakes nothing downstream. Run config
    `feed_tick_seconds` stretches the tick — handy for slowing the feed down
    while exploring the console.
    """

    def delta(self, site: str, since: str | None, tick_seconds: float = 5) -> tuple[list[dict], str]:
        tick = int(time.time() // tick_seconds)
        if since is not None and int(since) >= tick:
            return [], since
        events = [
            {
                "file_id": f"{site}-file-{i}",
                "version": f"t{tick}",
                "path": f"/sites/{site}/qaqc/f{i}.xlsx",
                "status": "warn" if i % 3 == 0 else "ok",
            }
            for i in range(4)
        ]
        # Occasionally the file set itself changes (a file drops out).
        if tick % 7 == 0:
            events = events[:-1]
        return events, str(tick)


class UploadReader:
    """Pretend upload store: bytes per external upload key."""

    def read(self, upload_id: str) -> bytes:
        return f"payload for {upload_id}".encode()


class Mailer:
    sent: list[dict] = []

    def send(self, to: str, subject: str, body: str) -> None:
        self.sent.append({"to": to, "subject": subject, "body": body})
        print(f"[mailer] {to}: {subject}")


ingest = Pool("ingest")


# ---------------------------------------------------------------------------
# Partition sets: `sites` refreshes on a cron and grows; `uploads` is fed from
# outside through `cursus commit uploads --upsert ...` (§5, §7).
# ---------------------------------------------------------------------------


@asset(outputs=PartitionSet(), automations=Automation(trigger=Cron("* * * * *")))
def sites(ctx, registry: SiteRegistry):
    """The site list is a partition set; each run may surface a new site.

    The cursor keeps the simulated count, so the list grows on every run —
    a new key arrives as missing work downstream (§7)."""
    seen = int(ctx.cursor or 0)
    return Result(outputs={"sites": registry.list_sites(seen)}, cursor=str(seen + 1))


uploads = PartitionSet("uploads")


# ---------------------------------------------------------------------------
# Per-site cursor asset: an unkeyed incremental event log and a keyed file
# inventory, patched both ways; the delta token is the cursor (§2, §5, §6).
# ---------------------------------------------------------------------------


@asset(
    outputs=(
        Output(
            "site_events",
            store=RELATIONAL,
            incremental=True,
            partition_column="site",
            migrations=postgres_migrations("site_events"),
        ),
        Output(
            "site_files",
            store=RELATIONAL,
            key="file_id",
            revision="version",
            partition_column="site",
            migrations=postgres_migrations("site_files"),
        ),
    ),
    partitions={"site": sites},
    automations=Automation(trigger=Every(10)),
)
def site_feed(ctx, feed: FeedClient):
    """Poll one site's feed; the delta token persists as ctx.cursor."""
    events, token = feed.delta(
        ctx.partition, ctx.cursor, tick_seconds=float(ctx.config.get("feed_tick_seconds", 5))
    )
    kept = {e["file_id"] for e in events}
    prior_ids = {f"{ctx.partition}-file-{i}" for i in range(4)}
    ctx.log("polled", site=ctx.partition, events=len(events))
    return Result(
        outputs={
            "site_events": Patch(events),
            # An empty delta means "nothing changed", not "everything gone" —
            # removals only apply once the feed reports the current file set.
            "site_files": Patch(events, remove=sorted(prior_ids - kept) if events else []),
        },
        cursor=token,
    )


# ---------------------------------------------------------------------------
# Incremental consumer: only changed file_ids arrive, in batches of two — a
# busy site shows `more` continuation; bump version="2" to reprocess every key.
# ---------------------------------------------------------------------------


@asset(
    outputs=Output(
        "file_index",
        store=RELATIONAL,
        key="file_id",
        partition_column="site",
        migrations=postgres_migrations("file_index"),
    ),
    partitions={"site": sites},
    inputs={"site_files": Incremental(batch_size=2)},
    version="2",
    automations=AutoRefresh(),
)
def file_index(ctx, site_files: list[dict]):
    """Index the files whose revision changed since the last commit (§6)."""
    changes = ctx.changes["site_files"]
    ctx.log("indexing", upserted=len(changes.upserted), deleted=len(changes.deleted))
    rows = [
        {"file_id": f["file_id"], "site": ctx.partition, "indexed_version": f["version"]} for f in site_files
    ]
    return Patch(rows, remove=changes.deleted)


# ---------------------------------------------------------------------------
# site × day: two dimensions; deps= on a plain Source gives the digest job
# lineage and change-watching without loading it. The output is a Blob.
# ---------------------------------------------------------------------------


@asset(
    outputs=Output("site_digest", store="blob"),
    partitions={"site": sites, "day": TimePartitions(start="2026-09-01", every="1d")},
    deps=["roadmap"],
    automations=Automation(trigger=Every(120)),
)
def site_digest(ctx, site_files: list[dict]):
    """A per-site-per-day digest blob; `roadmap` is pinned in lineage only."""
    body = "\n".join(
        f"{ctx.partitions['site']} {ctx.partitions['day']} {f['file_id']} {f['version']}"
        for f in sorted(site_files, key=lambda f: f["file_id"])
    )
    return body.encode()


# ---------------------------------------------------------------------------
# Fan-in: AllPartitions collapses the site dim into a dict[str, ref] (§7).
# ---------------------------------------------------------------------------


@asset(
    outputs=Output("fleet_index"),
    inputs={"file_index": AllPartitions()},
    automations=AutoRefresh(),
)
def fleet_index(ctx, file_index: dict[str, list[dict]]):
    """One row per site: committed heads at pin time, never a barrier."""
    rollup = [
        {
            "site": site,
            "files": len(rows),
            "digest": hashlib.sha256(",".join(sorted(r["file_id"] for r in rows)).encode()).hexdigest()[:12],
        }
        for site, rows in sorted(file_index.items())
    ]
    ctx.log("rolled up", sites=len(rollup))
    return rollup


if DATABASE:

    @asset(
        outputs=Output(
            "fleet_status",
            store="postgres",
            schema="ops",
            key="site",
            migrations=postgres_migrations("fleet_status", "CREATE SCHEMA IF NOT EXISTS ops"),
        ),
        inputs={"site_events": AllPartitions()},
        automations=AutoRefresh(),
    )
    def fleet_status(ctx, site_events: dict[str, TableRef]):
        """AllPartitions over TableRefs: the pins stay refs, the SELECT runs
        inside Postgres against each site's slice (§4, §7)."""
        union = " UNION ALL ".join(
            f"SELECT '{ref.where.get('site', site)}' AS site, count(*)::int AS events "
            f"FROM {ref.table} WHERE {ref.where_sql()}"
            for site, ref in sorted(site_events.items())
        )
        return Sql(union or "SELECT NULL::text AS site, 0::int AS events WHERE false")


# ---------------------------------------------------------------------------
# In-database SQL over a TableRef — Postgres only; with JsonStore the asset
# still runs and logs that it skipped (§4).
# ---------------------------------------------------------------------------

if DATABASE:

    @asset(
        outputs=Output(
            "site_status",
            store="postgres",
            schema="ops",
            partition_column="site",
            migrations=postgres_migrations("site_status", "CREATE SCHEMA IF NOT EXISTS ops"),
        ),
        partitions={"site": sites},
        automations=AutoRefresh(),
    )
    def site_status(ctx, site_events: TableRef) -> Sql:
        """A SELECT materialized inside Postgres; no row enters the harness."""
        return Sql(
            f"SELECT status, count(*) AS n FROM {site_events.table} WHERE {site_events.where_sql()} GROUP BY status"
        )

else:

    @asset(outputs=Output("site_status"), partitions={"site": sites}, automations=AutoRefresh())
    def site_status(ctx, site_events: list):
        ctx.log("DATABASE_URL unset — site_status skipped (needs Postgres)")
        return [{"site": ctx.partition, "status": "skipped"}]


# ---------------------------------------------------------------------------
# External uploads: registered through `cursus commit`, ingested by a pool
# worker. partitions="missing" plans every key with no complete head.
# ---------------------------------------------------------------------------


@asset(
    outputs=Output("upload_record", key="upload_id"),
    partitions={"upload": uploads},
    executor=ingest(cpu=1),
    automations=Automation(trigger=Every(30), partitions="missing"),
)
def manual_ingest(ctx, uploads_reader: UploadReader):
    """Runs on `cursus worker pool ingest` — claim, lease, run, complete (§10)."""
    payload = uploads_reader.read(ctx.partition)
    ctx.log("ingested", upload=ctx.partition, bytes=len(payload))
    return [{"upload_id": ctx.partition, "bytes": len(payload)}]


# ---------------------------------------------------------------------------
# A job on a weekly cron, and a standalone automation over two targets.
# ---------------------------------------------------------------------------


@job(
    inputs={"fleet_index": "fleet_index"},
    automations=Automation(trigger=Cron("0 7 * * 1")),
)
def weekly_digest(ctx, fleet_index: list, mailer: Mailer):
    """Jobs take inputs, placement and automations; they return no outputs."""
    lines = [f"{r['site']}: {r['files']} files" for r in fleet_index]
    mailer.send("ops@example.com", "Weekly digest", "\n".join(lines))


@job(automations=Automation(trigger=OnDeploy()))
def deploy_notice(ctx):
    """§9: fires once per served project revision — watch it run on boot."""

    ctx.log("project deployed", run=ctx.run_id)


project = Project(
    assets=[
        sites,
        site_feed,
        file_index,
        site_digest,
        fleet_index,
        site_status,
        *([fleet_status] if DATABASE else []),
        manual_ingest,
        weekly_digest,
        deploy_notice,
    ],
    sources=[
        Source("roadmap"),  # lineage-only: read via resources, pinned via deps=
        uploads,  # external PartitionSet, fed by the commit API
    ],
    stores={
        "postgres": PostgresStore(dsn="env:DATABASE_URL"),
        "blob": BlobStore(url=os.getenv("ARTIFACT_STORE")),
    },
    resources={
        "registry": SiteRegistry(),
        "feed": FeedClient(),
        "uploads_reader": UploadReader(),
        "mailer": Mailer(),
    },
    automations=[
        Automation("refresh-index", targets=[site_feed, file_index], trigger=Cron("*/5 * * * *")),
    ],
    name="demo",
)
