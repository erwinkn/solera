"""End-to-end project: every pattern in the architecture, in one file.

Brimstone-flavored: SharePoint sites polled through Microsoft Graph delta
feeds, a keyed output used as the site partition set, an externally fed
upload set, incremental Excel ingestion keyed by file_id, Postgres and blob
outputs, multi-dimensional partitions, sized executors, a job, and both
automation styles.

The normative reference for the API is docs/architecture.md.
"""

from __future__ import annotations

import os

import pandas as pd
from cursus.executors import AWSECS, Pool
from cursus.sdk import (
    AllPartitions,
    Automation,
    AutoRefresh,
    ByKey,
    Cron,
    Every,
    Migration,
    OnDeploy,
    Output,
    PartitionSet,
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
from cursus.stores import BlobStore, Patch, Sql
from cursus_postgres import PostgresStore

# ---------------------------------------------------------------------------
# Resources: ordinary client objects, injected by parameter name. `env:`
# indirection resolves in the harness, like store config.
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


# Environments are project-level; a placement is built per asset by calling
# one with typed, kind-specific options.
ecs = AWSECS(cluster="lab", region="us-east-1")
ingest = Pool("ingest")


# ---------------------------------------------------------------------------
# A partition set is an output. `sites` is an ordinary asset whose value is a
# key list; its key map is the set every consumer pins.
# ---------------------------------------------------------------------------


@asset(
    outputs=PartitionSet(),  # name defaults to the function name
    # Daily, and once per deploy so a fresh revision starts from a current list.
    automations=[Automation(trigger=Cron("0 6 * * *")), Automation(trigger=OnDeploy())],
)
def sites(graph: GraphClient) -> list[str]:
    """Refresh the SharePoint site list daily; new sites surface as missing work."""
    return graph.list_sites()


# The upload set is fed from outside: the uploads service calls
#   client.commit("uploads", upsert=["u-91"], remove=["u-12"])
uploads = PartitionSet("uploads")


# ---------------------------------------------------------------------------
# Ingress: polling lives in the graph. One cursor asset per site both keeps
# an append-only event log (a keyed output whose keys are batch numbers) and
# maintains the keyed file inventory that downstream ByKey edges diff against.
# ---------------------------------------------------------------------------


@asset(
    outputs=(
        Output(
            "change_events", store="postgres", schema="sharepoint", mode="append", partition_column="site"
        ),
        Output("qaqc_files", key="file_id", revision="version"),
    ),
    partitions=sites,
    automations=Automation(trigger=Every(30)),
)
def graph_delta(ctx, graph: GraphClient):
    """Poll a site's Graph delta feed; the delta token is the cursor."""
    events, token = graph.delta(site=ctx.partition, since=ctx.cursor)
    workbooks = [
        {"file_id": e["file_id"], "version": e["version"], "path": e["path"]}
        for e in events
        if not e.get("deleted") and is_qaqc_workbook(e)
    ]
    kept = {w["file_id"] for w in workbooks}
    return Result(
        outputs={
            # On an append output a Patch is one new batch key; a retry after
            # a rejected commit replaces its own orphan.
            "change_events": Patch(events),
            # Every file_id in `workbooks` is re-owned; touched files that are
            # deleted or not QAQC workbooks leave the inventory. The store
            # returns the complete key -> version map for this site.
            "qaqc_files": Patch(workbooks, remove=[e["file_id"] for e in events if e["file_id"] not in kept]),
        },
        cursor=token,
    )


@asset(
    outputs=Output(
        "qaqc_samples",
        store="postgres",
        schema="qaqc",
        primary_key=["sample_id"],
        partition_column="site",
        indexes=[["file_id"]],
        # Schema owned by the output: the store applies pending ones before
        # its first write, keeps the ledger in Postgres, and a new entry
        # reprocesses every key (it enters the interpretation fingerprint).
        migrations=[
            Migration("0001_analyst", "ALTER TABLE qaqc.qaqc_samples ADD COLUMN IF NOT EXISTS analyst text"),
            Migration(
                "0002_measured_tz", "ALTER TABLE qaqc.qaqc_samples ALTER COLUMN measured TYPE timestamptz"
            ),
        ],
    ),
    partitions=sites,
    inputs={"qaqc_files": ByKey()},
    version="2",  # bump to reprocess every key; code changes alone do not
    automations=AutoRefresh(),
)
async def qaqc_samples(ctx, qaqc_files: list[dict], sharepoint: SharePointClient):
    """qaqc_files arrives filtered to the file_ids whose version changed for this site."""
    frames = []
    for row in qaqc_files:
        df = read_qaqc_excel(await sharepoint.read(row["file_id"]))
        df["file_id"] = row["file_id"]
        frames.append(df)  # the store stamps `site` from the scope
    changes = ctx.changes["qaqc_files"]
    return Patch(pd.concat(frames) if frames else pd.DataFrame(), remove=changes.deleted)


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
    max_workers = min(ctx.execution.memory // int(6e9), os.cpu_count())
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
    """In-database: a TableRef in, a statement out; no row enters the harness.
    The store materializes the SELECT into ops.site_health for this site."""
    return Sql(
        f"SELECT status, count(*) AS n FROM {change_events.table} WHERE {change_events.where} GROUP BY status"
    )


@asset(
    outputs=Output("fleet_dashboard", store="postgres", schema="ops"),
    inputs={"site_health": AllPartitions()},
    automations=AutoRefresh(),
)
def fleet_dashboard(ctx, site_health: dict[str, TableRef]) -> pd.DataFrame:
    """Partitioned upstream, unpartitioned consumer: the site dimension is collapsed."""
    frames = [ctx.load(ref, pd.DataFrame).assign(site=site) for site, ref in site_health.items()]
    return pd.concat(frames)


@asset(
    outputs=Output("region_rollup", store="postgres", schema="ops", partition_column="region"),
    partitions=StaticPartitions(["east", "west"]),
    inputs={"qaqc_samples": AllPartitions()},
    automations=AutoRefresh(),
)
def region_rollup(ctx, qaqc_samples: dict[str, TableRef]) -> pd.DataFrame:
    """Static partitions over a site-partitioned input."""
    frames = [
        ctx.load(ref, pd.DataFrame)
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
        # PartitionSet fed through the commit API.
        uploads,
    ],
    stores={
        "postgres": PostgresStore(dsn="env:DATABASE_URL", grants=["qgis", "felt"]),
        "blob": BlobStore(url="env:ARTIFACT_STORE"),
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
