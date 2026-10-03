"""End-to-end project: every pattern in the architecture, in one file.

Brimstone-flavored: SharePoint sites polled through Microsoft Graph delta
feeds, a keyed output used as the site dynamic partitions, an externally fed
upload set, per-file Excel ingestion keyed by file_id (an `Each` edge: one
call per changed workbook, its failures kept per key), Postgres and blob
outputs, multi-dimensional partitions, sized executors, a job, and both
automation styles.

The normative reference for the API is docs/architecture.md.
"""

from __future__ import annotations

import os

import pandas as pd
from solera.errors import Rejected
from solera.executors import AWSECS, Pool
from solera.sdk import (
    AllPartitions,
    Automation,
    AutoRefresh,
    Cron,
    DynamicPartitions,
    Each,
    Every,
    Migration,
    OnDeploy,
    Output,
    Project,
    Result,
    Retry,
    Source,
    StaticPartitions,
    TableRef,
    TimePartitions,
    asset,
    job,
)
from solera.stores import Patch, S3Store, Sql
from solera_postgres import PostgresStore

# ---------------------------------------------------------------------------
# Resources: ordinary client objects, injected by parameter name. `env:`
# indirection resolves in the worker, like store config.
# ---------------------------------------------------------------------------


class GraphClient:
    def list_sites(self) -> list[str]: ...
    def delta(self, site: str, since: str | None) -> tuple[list[dict], str]: ...


class SharePointClient:
    async def read(self, file_id: str) -> bytes: ...


class DemClient:
    def fetch(self, site: str, day: str) -> bytes: ...


class Mailer:
    def send(self, to: str, subject: str, body: str) -> None: ...


def make_query(dsn: str):
    def query(sql: str, **params) -> list[dict]: ...

    return query


def compute_psa_mean_data(samples: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame: ...
def read_qaqc_excel(raw: bytes) -> pd.DataFrame: ...
def render_hillshade(tif: bytes) -> bytes: ...
def load_upload(raw: bytes) -> pd.DataFrame: ...
def compute_leach(max_workers: int, **frames: pd.DataFrame) -> dict[str, pd.DataFrame]: ...
def site_region(site: str) -> str: ...
def is_qaqc_workbook(event: dict) -> bool: ...


# Executors are named, project-level environments; a placement is built per
# asset by calling one with typed, kind-specific options.
ecs = AWSECS("lab", cluster="lab", region="us-east-1")
ingest = Pool("ingest")


# ---------------------------------------------------------------------------
# A dynamic partitions is an output. `sites` is an ordinary asset whose value is a
# key list; its key map is the set every consumer pins.
# ---------------------------------------------------------------------------


@asset(
    outputs=DynamicPartitions(),  # name defaults to the function name
    # Daily, and once per deploy so a fresh revision starts from a current list.
    automations=[Automation(trigger=Cron("0 6 * * *")), Automation(trigger=OnDeploy())],
)
def sites(graph: GraphClient) -> list[str]:
    """Refresh the SharePoint site list daily; new sites surface as missing work."""
    return graph.list_sites()


# The upload set is fed from outside: the uploads service calls
#   client.commit("uploads", upsert=["u-91"], remove=["u-12"])
uploads = DynamicPartitions("uploads")


# ---------------------------------------------------------------------------
# Ingress: polling lives in the graph. One cursor asset per site both keeps
# an unkeyed incremental event log (one delta batch per commit) and
# maintains the keyed file inventory that Incremental edges diff against.
# ---------------------------------------------------------------------------


@asset(
    outputs=(
        Output(
            "change_events",
            store="postgres",
            schema="sharepoint",
            incremental=True,
            partition_column="site",
        ),
        Output("qaqc_files", key="file_id"),
    ),
    partitions=sites,
    automations=Automation(trigger=Every(30)),
)
def graph_delta(ctx, graph: GraphClient):
    """Poll a site's Graph delta feed; the feed's cursor is the cursor."""
    events, token = graph.delta(site=ctx.partition, since=ctx.cursor)
    workbooks = [
        {"file_id": e["file_id"], "version": e["version"], "path": e["path"]}
        for e in events
        if not e.get("deleted") and is_qaqc_workbook(e)
    ]
    kept = {w["file_id"] for w in workbooks}
    return Result(
        outputs={
            # An unkeyed incremental Patch is one new batch; a retry after a
            # rejected commit replaces its own orphan batch rows.
            "change_events": Patch(events),
            # Every file_id in `workbooks` is re-owned; touched files that are
            # deleted or not QAQC workbooks leave the inventory. The store
            # computes the committed delta for this site.
            "qaqc_files": Patch(workbooks, remove=[e["file_id"] for e in events if e["file_id"] not in kept]),
        },
        cursor=token,
    )


class Unprocessable(Rejected):
    """A workbook that is bad on purpose — empty, a template: kept as a
    rejected key until the file changes, never retried blindly."""


@asset(
    outputs=Output(
        "qaqc_samples",
        store="postgres",
        schema="qaqc",
        # Every file_id is the group of its workbook's samples.
        key="file_id",
        primary_key=["file_id", "sample_id"],
        partition_column="site",
        # Schema owned by the output: the store applies pending ones before
        # its first write, keeps the ledger in Postgres, and a new entry
        # reprocesses every key (it enters the fingerprint).
        migrations=[
            Migration("0001_analyst", "ALTER TABLE qaqc.qaqc_samples ADD COLUMN IF NOT EXISTS analyst text"),
            Migration(
                "0002_measured_tz", "ALTER TABLE qaqc.qaqc_samples ALTER COLUMN measured TYPE timestamptz"
            ),
        ],
    ),
    partitions=sites,
    inputs={"workbook": Each("qaqc_files", concurrency=8)},
    version="2",  # bump to reprocess every key; code changes alone do not
    automations=AutoRefresh(),
)
async def qaqc_samples(ctx, workbook: list[dict], sharepoint: SharePointClient) -> pd.DataFrame:
    """One changed workbook of this site: its samples. Solera runs it for every
    file_id written since, eight at a time, writes all of them in one
    store write, and removes the samples of deleted workbooks; the store
    stamps `file_id` and `site`. A workbook that raises is recorded as a
    failing key, and the others still commit."""
    raw = await sharepoint.read(ctx.key)
    if not raw:
        raise Unprocessable("empty workbook")
    return read_qaqc_excel(raw)


# ---------------------------------------------------------------------------
# Lab data: store-bound sources, plain DataFrame outputs, sized executors.
# ---------------------------------------------------------------------------


@asset(
    outputs=Output(
        "psa_mean_data",
        store="postgres",
        schema="analytical",
        primary_key=["sample_id", "diameter"],
        columns={
            "sample_id": "string not null",
            "diameter": "float not null",
            "undersize": "float not null",
            "q": "float not null",
            "undersize_cleaned": "float",
            "q_cleaned": "float",
        },
    ),
    # Fires when the datasmart sources are advanced through the commit API;
    # refresh-lab's cron is the scheduled fallback.
    automations=AutoRefresh(),
)
def psa_mean_data(ctx, psa_samples: pd.DataFrame, psa_data: pd.DataFrame) -> pd.DataFrame:
    result = compute_psa_mean_data(psa_samples, psa_data)
    ctx.log("processed", rows=len(result))
    return result


@asset(
    outputs=(
        Output("leach_kinetics", store="postgres", schema="leaching"),
        Output("leach_invalid_sample_id_log", store="postgres", schema="datasmart"),
        # ... six more outputs
    ),
    executor=ecs(cpu=4, memory="30GB"),
    retries=Retry(3, delay=0.2, backoff="exponential"),
    timeout=3600,
)
def leach(
    ctx,
    project_team_map: pd.DataFrame,
    leach_crosswalk: pd.DataFrame,
    sample_id_crosswalk: pd.DataFrame,
    pmpt_project_matrix: pd.DataFrame,
    xrd_profex: pd.DataFrame,
    xrf: pd.DataFrame,
):
    """The resource tier lives on the asset; no job indirection."""
    max_workers = min(ctx.placement["options"]["memory"] // int(6e9), os.cpu_count())
    tables = compute_leach(
        max_workers,
        project_team_map=project_team_map,
        leach_crosswalk=leach_crosswalk,
        sample_id_crosswalk=sample_id_crosswalk,
        pmpt_project_matrix=pmpt_project_matrix,
        xrd_profex=xrd_profex,
        xrf=xrf,
    )
    # Result(outputs=<subset>) would keep prior refs for omitted outputs.
    return Result(outputs=tables)


# ---------------------------------------------------------------------------
# By reference, fan-in, other partition shapes, blobs, a job.
# ---------------------------------------------------------------------------


@asset(
    outputs=Output("site_health", store="postgres", schema="ops", partition_column="site"),
    partitions=sites,
    automations=AutoRefresh(),
)
def site_health(ctx, change_events: TableRef) -> Sql:
    """In-database: a TableRef in, a query out; no row enters the worker.
    The store materializes the SELECT into ops.site_health for this site."""
    return Sql(
        f"SELECT status, count(*) AS n FROM {change_events.table} WHERE {change_events.where_sql()} GROUP BY status"
    )


@asset(
    outputs=Output("fleet_dashboard", store="postgres", schema="ops"),
    inputs={"site_health": AllPartitions()},
    automations=AutoRefresh(),
)
async def fleet_dashboard(ctx, site_health: dict[str, TableRef]) -> pd.DataFrame:
    """Partitioned upstream, unpartitioned consumer: the site dimension is collapsed."""
    frames = [(await ctx.load(ref, pd.DataFrame)).assign(site=site) for site, ref in site_health.items()]
    return pd.concat(frames)


@asset(
    outputs=Output("region_rollup", store="postgres", schema="ops", partition_column="region"),
    partitions=StaticPartitions(["east", "west"]),
    inputs={"qaqc_samples": AllPartitions()},
    automations=AutoRefresh(),
)
async def region_rollup(ctx, qaqc_samples: dict[str, TableRef]) -> pd.DataFrame:
    """Static partitions over a site-partitioned input."""
    frames = [
        await ctx.load(ref, pd.DataFrame)
        for site, ref in qaqc_samples.items()
        if site_region(site) == ctx.partition
    ]
    return pd.concat(frames)


@asset(
    outputs=Output("manual_upload", store="postgres", schema="uploads", partition_column="upload_id"),
    partitions=uploads,
    executor=ingest(cpu=1, memory="4GB"),
    # partitions="missing": every key with no complete head is planned each
    # minute, which covers new upload keys and failed first runs alike.
    automations=Automation(trigger=Every(60), partitions="missing"),
)
async def manual_upload(ctx, sharepoint: SharePointClient) -> pd.DataFrame:
    """Upload keys are registered externally by the uploads service."""
    return load_upload(await sharepoint.read(ctx.partition))


@asset(
    outputs=Output("hillshade", store="blob"),
    partitions={"site": sites, "day": TimePartitions(start="2024-01-01", every="1d")},
    deps=["usgs_3dep_tiles"],
    automations=Automation(trigger=Every(3600)),  # partitions="latest": every site, newest day
)
def hillshade(ctx, dem: DemClient) -> bytes:
    """Two dimensions; deps= gives lineage and change-watching for a resource-read source."""
    return render_hillshade(dem.fetch(ctx.partitions["site"], ctx.partitions["day"]))


@job(
    inputs={"fleet_dashboard": "fleet_dashboard"},
    automations=Automation(trigger=Cron("0 7 * * 1")),
)
def weekly_report(ctx, fleet_dashboard: TableRef, mailer: Mailer, query) -> None:
    """A job: inputs, placement and automations like an asset, no outputs."""
    rows = query(f"SELECT site, n FROM {fleet_dashboard.table} ORDER BY n DESC LIMIT 20")
    mailer.send("ops@example.com", "Fleet health", "\n".join(f"{r['site']}: {r['n']}" for r in rows))


# ---------------------------------------------------------------------------
# Project assembly.
# ---------------------------------------------------------------------------

project = Project(
    assets=[
        sites,
        graph_delta,
        qaqc_samples,
        psa_mean_data,
        leach,
        site_health,
        fleet_dashboard,
        region_rollup,
        manual_upload,
        hillshade,
        weekly_report,
    ],
    sources=[
        # Store-bound: synthesized heads; the commit API supplies new revisions.
        Source("psa_samples", store="postgres", schema="datasmart"),
        Source("psa_data", store="postgres", schema="datasmart"),
        Source("project_team_map", store="postgres", schema="datasmart"),
        Source("leach_crosswalk", store="postgres", schema="datasmart"),
        Source("sample_id_crosswalk", store="postgres", schema="datasmart"),
        Source("pmpt_project_matrix", store="postgres", schema="datasmart"),
        Source("xrd_profex", store="postgres", schema="datasmart"),
        Source("xrf", store="postgres", schema="datasmart"),
        # Lineage only: read via a resource, referenced in deps=.
        Source("usgs_3dep_tiles"),
        # DynamicPartitions fed through the commit API.
        uploads,
    ],
    stores={
        "postgres": PostgresStore(dsn="env:DATABASE_URL", grants=["qgis", "felt"]),
        "blob": S3Store("env:ARTIFACT_STORE"),
    },
    resources={
        "graph": GraphClient(),
        "sharepoint": SharePointClient(),
        "query": make_query("env:DATABASE_URL"),
        "dem": DemClient(),
        "mailer": Mailer(),
    },
    automations=[
        # Standalone: multi-target, scheduled, direct asset references.
        Automation("refresh-lab", targets=[leach, psa_mean_data], trigger=Cron("0 8 * * *")),
    ],
)

# Equivalent without the explicit asset list:
# project = Project.from_package("brimstone.assets", stores=..., resources=...)
