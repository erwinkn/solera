# Data Orchestrator

An asset-first, self-hosted materialization engine with a Python SDK and a web console. **Apache-2.0, including the UI, automation, backfills, and event history.** No cloud account, license server, Redis, Celery, or Kubernetes is required.

This is a working **0.1 alpha**, not a production-hardened replacement for Dagster. It prioritizes transactional incremental state and a small programming model over an extensive integration catalog.

## What works

| Area | Implementation |
| --- | --- |
| Authoring | Python assets, dependency inference, multiple outputs, resource bindings, synchronous and asynchronous functions |
| Incremental updates | Revisioned keyed inventories, explicit cursor checkpoints, bounded batches, ownership-key replacement, upsert/delete, idempotent append receipts |
| Durability | PostgreSQL queue and leases, publication fencing, atomic output-reference/checkpoint commits, pinned inputs, attempts, retries, partial-batch recovery |
| Partitions | Daily partitions, identity dependency mapping, bounded backfills, fill-missing/recompute modes, pause/resume/cancel/repair |
| Automation | Intervals, timezone-aware cron, and changed-output commit triggers through one request API |
| Storage | Immutable PostgreSQL JSON and content-addressed filesystem JSON snapshots; custom immutable-store protocol |
| Execution | Concurrent local subprocesses behind a narrow backend protocol; embedded or separate worker |
| Console | Searchable asset table, lineage graph, data previews, commit and checkpoint inspection, live runs/events, backfills, automation controls |

The console uses real API data. The bundled laboratory dataset is a fixture; its materializations and history are produced by actual executions.

## Run with Docker

```sh
git clone https://github.com/erwinkn/data-orchestrator.git
cd data-orchestrator
git checkout feat/asset-engine-ui  # until the initial implementation is merged
docker compose up --build --wait
```

Open **http://127.0.0.1:8000**. Select `sample_quality` and click **Materialize**. The planner discovers upstream files, parses two related outputs, and computes the summary and quality report. Use **Backfills** to process a range for `daily_report`.

Compose runs one application/worker service and PostgreSQL. State survives container restarts in the `postgres-data` volume. `docker compose down` keeps it; `docker compose down --volumes` deletes the local workspace and its data.

The supplied Compose configuration is **local development only**: loopback ports, a development database password, and optional authentication. Copy `.env.example` to `.env` and set `DORC_API_TOKEN` and a strong `POSTGRES_PASSWORD` before enabling remote access. Use TLS at a reverse proxy. See [SECURITY.md](SECURITY.md).

## Local development

Tested with Python 3.12, uv 0.12.10, Node 24, and PostgreSQL 17.

```sh
# Start only the bundled PostgreSQL service.
docker compose up -d postgres
export DORC_DATABASE_URL='postgresql://orchestrator:local-development-only@127.0.0.1:55432/orchestrator'

uv sync --locked
(cd ui && npm ci && npm run build)
uv run dorc dev examples.lab:definitions
```

`dev` initializes metadata, registers the definitions, starts the API/UI, and runs an embedded worker. It does not hot-reload Python definitions: register them again or restart `dev` after changing asset code. For frontend hot reload, run `npm run dev` inside `ui` and open port 5173; it proxies the API on port 8000.

To separate the control plane from execution:

```sh
uv run dorc migrate
uv run dorc register examples.lab:definitions
uv run dorc serve --no-worker
# In another process, with the same database and installed code:
uv run dorc worker --concurrency 4
```

The server reads a registered manifest; it does not import user projects. Registration and execution import code in their own processes. A request pins its definition manifest. This alpha verifies that the worker matches that manifest, but does not archive arbitrary historical Python environments for you.

## Author an asset

```python
from data_orchestrator import Definitions, asset

@asset
def raw_samples():
    return [{"id": "S-1", "value": 42}]

@asset
def cleaned_samples(raw_samples):
    return [row for row in raw_samples if row["value"] is not None]

definitions = Definitions([raw_samples, cleaned_samples], name="My workspace")
```

Save this as `pipeline.py`, then run `uv run dorc dev pipeline:definitions`. Functions remain directly callable in unit tests. There are no user-defined jobs or ops. Manual invocations and automations both submit durable materialization requests:

```sh
uv run dorc run cleaned_samples
uv run dorc run daily_report --partition 2026-08-20 --mode fill_missing
```

## Incremental ownership-key replacement

```python
from data_orchestrator import (
    AssetContext, ByKey, CommitBatch, Definitions, Inventory, ReplaceKeys, asset,
)

@asset
def source_files():
    # An actual connector would discover these revisions from its source.
    return Inventory([
        {"id": "file-a", "revision": "etag-1", "values": [1, 2]},
    ], complete=True)

@asset(incremental=ByKey("source_files", batch_size=100))
def measurements(ctx: AssetContext, source_files: Inventory):
    changes = ctx.changes("source_files")
    affected = (*changes.upserted_keys, *changes.deleted_keys)
    rows = [
        {"source_file_id": file["id"], "value": value}
        for file in changes.upserted
        for value in file["values"]
    ]
    return CommitBatch(
        outputs={"measurements": ReplaceKeys("source_file_id", affected, rows)},
        acknowledge=changes.token,
    )

definitions = Definitions([source_files, measurements])
```

The acknowledgement is published in the same metadata transaction as the output reference. A failure leaves that batch outstanding. Earlier committed batches remain available. Replacing a key with zero rows clears its previous child rows; deleting a source clears its owned output rows. Each consumer tracks its own processed revisions. An incomplete inventory (`complete=False`) **never infers deletion**.

The complete multi-output version is in [examples/lab.py](examples/lab.py). To exercise incrementality, edit `examples/lab_sources.json`, change a file's revision, and run `sample_quality` again. Removing a file, changing its children, and replacing its children with an empty list exercise different cases.

For opaque source cursors, use `Cursor(initial=...)` and return `CommitBatch(outputs=..., cursor=next_cursor)`. The asset author defines a bounded source read; the runtime commits the cursor together with the outputs. It does not infer CDC semantics or automatically incrementalize Python. See [docs/architecture.md](docs/architecture.md).

## Guarantees and deliberate limits

**At-least-once computation; fenced, transactional publication of supported immutable outputs.** Multi-output references, checkpoint changes, item revisions, append receipts, and downstream notifications publish together. Staging can leave unreferenced objects after failure. External side effects performed directly by user code are outside this guarantee.

This alpha deliberately does **not** implement native mutable warehouse transactions, S3/Parquet adapters, Docker/Kubernetes execution adapters, artifact registries, arbitrary partition mappings, scoped worker credentials, named-user RBAC, garbage collection, or a tamper-evident audit system. The backend interface exists, but only the local subprocess backend is implemented. PostgreSQL and filesystem stores preserve immutable JSON snapshots, not arbitrary Python objects.

For correctness-first scope control, short metadata transitions are serialized with a PostgreSQL advisory lock; asset computation remains concurrent. Inventory and item-state comparison currently loads each selected inventory into worker memory, and row mutations write a new output snapshot. Do not assume this version already supports million-item inventories or warehouse-scale incremental merging. Requests are bounded to 366 dates and 10,000 planned tasks; catalog and history views also have explicit limits.

Backfills never reset a live cursor. Historical partitions reproduce the available inputs, not an unretained historical source snapshot. Change an asset's explicit `version` when helper functions, resources, or dependencies change its transformation semantics; the framework does not hash an entire Python dependency closure. The current schema is an initial alpha schema, not a promise of migration compatibility across unreleased iterations.

## Verify

```sh
uv run ruff check src tests examples
uv run ruff format --check src tests examples
uv run mypy src/data_orchestrator
TEST_DATABASE_URL="$DORC_DATABASE_URL" uv run pytest -q
(cd ui && npm run format:check && npm run build)
(cd ui && npx playwright install --with-deps chromium && npm test)
```

Use a disposable database for browser tests. PostgreSQL integration tests isolate themselves in temporary schemas. CI runs the Python tests, type checking, frontend formatting/build, desktop/mobile Playwright flows, automated catalog accessibility checks, wheel-content verification, and a Docker Compose smoke test.

## Structure

```text
src/data_orchestrator/
  sdk.py          authoring model and versioned manifests
  planner.py      manifest-to-scope request planning
  database.py     PostgreSQL state transitions and leases
  engine.py       change batches, staging, and atomic publication
  stores.py       immutable snapshot storage
  worker.py       backend protocol and subprocess supervisor
  service.py      queries, run controls, and automation evaluation
  api.py          authenticated HTTP API and static console
  cli.py          registration, server, and worker commands
ui/               React + strict TypeScript console
examples/         editable laboratory pipeline and source fixture
tests/            unit, PostgreSQL integration, and recovery tests
```
