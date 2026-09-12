# Data Orchestrator — object-backed experiment

An asset-first Python engine and browser console whose only required durable storage is an object store. **Experimental alpha, not production-ready.** This branch uses SlateDB 0.16 for transactional metadata and `obstore` 0.11 for immutable JSON outputs. No PostgreSQL, Redis, or local metadata database is required.

This is a standalone implementation on `feat/s3-state-backend`. The earlier `feat/asset-engine-ui` branch contained only CI scaffolding when this experiment resumed; there is no PostgreSQL migration or drop-in backend switch in this branch. `main` is not changed.

## Local development

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). On supported platforms, SlateDB's Python wheel includes the native engine.

```bash
uv sync --locked
uv run dorc serve --insecure
# http://127.0.0.1:8000
```

The default `file:///.../.dorc` store uses the same object-store interfaces as S3, backed by ordinary files. No emulator is needed for day-to-day development. This is persistent local **object storage**, not a remote backup of a local database. Do not delete it expecting recovery from elsewhere.

Select `sample_quality` in the Materialize dialog. This executes:

```text
source_files -> samples + measurements -> sample_summary -> sample_quality

daily_observations -> daily_report  (daily partitions)
```

The console provides an asset catalog, dependency overview, data previews, committed output references, checkpoints, runs, attempts/logs, backfill requests, pause/resume/cancel/repair controls, interval/cron/changed-output automations, and backend diagnostics. It is a small static JS/CSS application served from the Python wheel, with no CDN or frontend build required. Node is used only for browser tests.

```bash
uv run dorc run sample_quality
uv run dorc run daily_report --partition 2026-01-01 --partition 2026-01-02
uv run dorc manifest --project data_orchestrator.demo:project
uv run dorc selftest --state-url file:///tmp/orchestrator-test-objects
```

**One coordinator per namespace.** Running `dorc run` while `dorc serve` uses the same namespace replaces/fences that server's writer. Use the UI/API to submit work to a running server, or give the CLI a different namespace.

## S3 and compatible services

Provision a private bucket, then configure both native clients using environment variables:

```bash
export DORC_STATE_URL=s3://your-private-bucket/orchestrator
export DORC_NAMESPACE=development
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export AWS_CONDITIONAL_PUT=etag
export DORC_API_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
uv run dorc serve
```

For a custom S3 endpoint:

```bash
export AWS_ENDPOINT=https://your-s3-endpoint
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
# Only for an HTTP emulator, never an unencrypted production connection:
# export AWS_ALLOW_HTTP=true
```

`AWS_ENDPOINT` is the Rust object-store client's setting, not just boto3's `AWS_ENDPOINT_URL`. The server never accepts credentials in a storage URL. Workers have these environment variables stripped, but they are trusted code with the same host identity: **subprocesses are not security sandboxes**.

Use HTTPS in front of any remote listener and set a strong `DORC_API_TOKEN`. `--insecure` is restricted to loopback in the CLI. The UI keeps the token in tab-scoped session storage. This is shared trusted-team authentication, not RBAC or tenancy isolation.

### Backend conformance

Before using a new S3-compatible service:

```bash
uv run dorc selftest --state-url "$DORC_STATE_URL"
```

This explicitly writes synthetic data into a fresh `probe-UUID` namespace. It checks create-if-absent and racing conditional updates on S3, a real subprocess pipeline, multi-output commits, unchanged-input skips, deletions and empty replacements, a backfill, reopening without coordinator state, durable command receipts, and native writer takeover. Test objects remain in that isolated namespace for inspection; remove only that prefix after all test processes are stopped. No existing workspace is deleted.

An emulator passing these checks is **not proof** that a different provider has equivalent semantics. See [Railway deployment](docs/railway.md).

## Authoring

```python
from data_orchestrator import (
    AssetContext, Batch, ByKey, Inventory, Project, ReplaceKeys, asset,
)

@asset
def source_files():
    return Inventory([
        {"id": "a.csv", "revision": "2", "records": [{"value": 42}]},
    ], complete=True)

@asset(inputs={"files": "source_files"}, incremental=ByKey("files"))
def measurements(ctx: AssetContext, files):
    changed = ctx.changes["upserted_keys"]
    affected = changed + ctx.changes["deleted_keys"]
    rows = [
        {"source_file": f["id"], **record}
        for f in files if str(f["id"]) in changed
        for record in f["records"]
    ]
    return ReplaceKeys("source_file", affected, rows)

project = Project([source_files, measurements])
```

Save as `my_project.py` and run `uv run dorc serve --insecure --project my_project:project` from that directory.

An ordinary return value replaces a snapshot. Explicit operations are `Replace`, `Inventory`, `ReplaceKeys`, `Upsert`, and `AppendBatch`. `Batch(outputs={...}, cursor=...)` returns multiple output mutations and the next user-managed cursor together. Checkpoints are scoped by producer and partition.

For `ByKey`, successful batches acknowledge only their selected source revisions. Incomplete inventories never imply deletions. Changing request configuration, producer code, or non-keyed dependencies conservatively reprocesses keyed items. A changed explicit `version` requires `mode="recompute"`; use it for incompatible state/schema changes. Resource and helper-module changes are not automatically fingerprinted: bump the asset version when they change semantics. An incomplete source inventory cannot be used for recompute. Recompute processes the full bounded scope at once rather than incrementally replacing an incomplete scope.

A multi-output producer executes as a unit. `fill_missing` reuses only complete published scopes; it is not a freshness check. A paused run starts no further attempts, but already running attempts may finish. Cancellation fences publication, not arbitrary external side effects. Repair retries failed work and retains already committed batches.

Automations take one of three triggers: `Every(seconds)`, `Cron(expression, timezone)`, or `OnCommit(assets)`. Commit triggers materialize their targets against the newly committed inputs inside the same transaction as the publication, and never re-run upstream producers; a commit that changes nothing fires nothing. Asset functions can emit structured entries with `ctx.log("message", **fields)`, shown alongside captured stdout on each attempt.

## Tests

```bash
uv run pytest -q -m 'not live'
cd ui && npm ci && npx playwright install chromium && npm test
```

The Python suite uses real SlateDB on filesystem storage, including abrupt process death and writer takeover. It also starts Moto as an HTTP S3 emulator and runs the remote conformance test. Synthetic fault injection separately verifies acknowledgement timing and ambiguous-outcome behavior. The live-provider test is opt-in:

```bash
DORC_TEST_S3_URL=s3://isolated-test-bucket/prefix uv run pytest -q -m live
```

GitHub Actions verifies Python contracts, browser behavior on desktop/mobile, wheel contents, and a Docker end-to-end run. `uv.lock` and the browser package lock are committed. CI uses locked installs; the storage engines are explicitly version-pinned.

## Boundaries

This is a feasibility implementation, not a claim of production readiness or benchmarked scalability. It deliberately serializes metadata transitions through remote acknowledgement. Inventory diffs and JSON snapshot rewrites are in-memory; queue/catalog/history scans need pagination and better indexing at larger scale. Limits are 64 MiB per JSON output, 1,000 partitions and 5,000 planned tasks per request. There is no Parquet/S3 table adapter, external-destination transaction recovery, distributed execution backend, arbitrary partition mapping, retained historical code image, artifact garbage collector, or zero-downtime multi-replica writer election. The durable outbox exists; an external event consumer is not implemented.

Read [the design and safety invariants](docs/architecture.md) before extending the backend. OpenAPI/UI endpoints are trusted-team surfaces, not a hardened multi-tenant service.

## License

Apache-2.0. The repository's visibility is unchanged.
