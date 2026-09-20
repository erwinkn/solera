"""Example project: a small weather-station pipeline.

Point cursus at this file — the CLI resolves a filesystem path (optionally
followed by `:attribute`) as well as the `module:attribute` form:

    cursus serve --project example/lab.py
    cursus run daily_metrics --project example/lab.py --partition 2026-01-01

The console served by `cursus serve` reflects whatever project is loaded, so
the catalog, automations, and run controls all apply here unchanged.
"""

from cursus import (
    AssetContext,
    Automation,
    Batch,
    ByKey,
    Cron,
    Every,
    Inventory,
    OnCommit,
    Project,
    ReplaceKeys,
    asset,
)


@asset(group="Stations")
def station_feed(ctx: AssetContext):
    """The keyed source of truth. `ctx.config` can override it per run, e.g.
    `cursus run station_feed --config '{"readings": [...]}'`."""
    return Inventory(
        ctx.config.get(
            "readings",
            [
                {"station": "north", "revision": "1", "temp_c": 21.4, "humidity": 41},
                {"station": "south", "revision": "1", "temp_c": 18.9, "humidity": 55},
                {"station": "east", "revision": "1", "temp_c": 23.0, "humidity": 37},
            ],
        )
    )


@asset(
    outputs=("readings", "station_health"),
    inputs={"feed": "station_feed"},
    # `key` names the identity column the feed is diffed by; `revision`
    # (the default column name) decides whether a row changed.
    incremental=ByKey("feed", key="station", batch_size=2),
    group="Stations",
)
def ingest(ctx: AssetContext, feed):
    """Incremental over the feed: each commit replaces only the rows owned by
    the stations whose revision changed (or were deleted)."""
    changed = [row for row in feed if row["station"] in ctx.changes["upserted_keys"]]
    keys = ctx.changes["upserted_keys"] + ctx.changes["deleted_keys"]
    ctx.log(
        "Ingested station revisions",
        upserted=len(changed),
        deleted=len(ctx.changes["deleted_keys"]),
    )
    return Batch(
        {
            "readings": ReplaceKeys("station", keys, changed),
            "station_health": ReplaceKeys(
                "station",
                keys,
                [{"station": row["station"], "online": True} for row in changed],
            ),
        }
    )


@asset(group="Stations")
def climate_report(readings, station_health):
    """Join the committed snapshots of both upstream outputs atomically."""
    health = {row["station"]: row["online"] for row in station_health}
    return [
        {
            "station": row["station"],
            "temp_c": row["temp_c"],
            "humidity": row["humidity"],
            "online": health.get(row["station"], False),
        }
        for row in readings
    ]


@asset(partitions="daily", group="Operations")
def daily_metrics(ctx: AssetContext, readings):
    """One independent scope per day — backfill a range from the materialize
    dialog or `cursus run daily_metrics --partition 2026-01-01`."""
    day = int(ctx.partition[-2:])
    return [
        {
            "date": ctx.partition,
            "station": row["station"],
            "temp_c": round(row["temp_c"] + (day % 3) * 0.1, 2),
        }
        for row in readings
    ]


project = Project(
    [station_feed, ingest, climate_report, daily_metrics],
    automations=[
        # Recompute the report every five minutes while enabled.
        Automation("refresh_report", ("climate_report",), Every(300)),
        # Nightly re-materialization on a cron schedule (UTC). Automation
        # targets must be unpartitioned assets.
        Automation("nightly_report", ("climate_report",), Cron("0 2 * * *", "UTC")),
        # Fires when `readings` publishes changed output; delivered once the
        # report's inputs are all committed.
        Automation("on_readings", ("climate_report",), OnCommit(("readings",))),
    ],
)
