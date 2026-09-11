"""Runnable, deterministic examples. No external credentials are required."""
from . import AssetContext, Automation, Batch, ByKey, Inventory, Project, ReplaceKeys, asset


@asset(group="Laboratory")
def source_files(ctx: AssetContext):
    """A complete keyed inventory. Request config can revise or delete files."""
    rows = ctx.config.get("files", [
        {"id": "LAB-001", "revision": "1", "sample": "Basalt A", "calcium": 18.4},
        {"id": "LAB-002", "revision": "1", "sample": "Basalt B", "calcium": 22.7},
        {"id": "LAB-003", "revision": "1", "sample": "Limestone C", "calcium": 39.2},
    ])
    return Inventory(rows, complete=ctx.config.get("inventory_complete", True))


@asset(outputs=("samples", "measurements"), inputs={"files": "source_files"}, incremental=ByKey("files", batch_size=2), group="Laboratory")
def parse_files(ctx: AssetContext, files):
    """Replace every row owned by a changed file, including empty results and deletions."""
    selected = [row for row in files if row["id"] in ctx.changes["upserted_keys"]]
    keys = ctx.changes["upserted_keys"] + ctx.changes["deleted_keys"]
    samples = [{"source_file_id": row["id"], "name": row["sample"]} for row in selected]
    measurements = [{"source_file_id": row["id"], "analyte": "Ca", "percent": row["calcium"]} for row in selected if row.get("calcium") is not None]
    print(f"Processing {len(selected)} revisions and {len(ctx.changes['deleted_keys'])} deletions")
    return Batch({"samples": ReplaceKeys("source_file_id", keys, samples), "measurements": ReplaceKeys("source_file_id", keys, measurements)})


@asset(group="Laboratory")
def sample_summary(samples, measurements):
    """Join committed sample and measurement snapshots."""
    values = {row["source_file_id"]: row["percent"] for row in measurements}
    return [{**row, "calcium_percent": values.get(row["source_file_id"])} for row in samples]


@asset(group="Laboratory")
def sample_quality(sample_summary):
    """A small downstream quality report."""
    return [{**row, "status": "measured" if row["calcium_percent"] is not None else "missing"} for row in sample_summary]


@asset(partitions="daily", group="Operations")
def daily_observations(ctx: AssetContext):
    """A reproducible partition for backfill testing."""
    day = int(ctx.partition[-2:])
    return [{"date": ctx.partition, "samples_processed": 10 + day, "failures": day % 3}]


@asset(partitions="daily", group="Operations")
def daily_report(ctx: AssetContext, daily_observations):
    """Independent daily partitions do not share the live ingestion cursor."""
    return [{**row, "success_rate": round(1 - row["failures"] / row["samples_processed"], 4)} for row in daily_observations]


project = Project([source_files, parse_files, sample_summary, sample_quality, daily_observations, daily_report], automations=[Automation("refresh_laboratory", ("sample_quality",), every_seconds=300)])
