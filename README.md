# solera — asset-first data orchestration

A Python data orchestrator: assets declare outputs, partitions, inputs, and
placement; a durable control plane plans and executes runs; every committed
output is an immutable ref. The control plane keeps all state in SlateDB on
object storage (files, S3, or compatible services) — **no external service is
required for local operation**. Relational outputs can optionally live in
shared Postgres tables when `DATABASE_URL` is set.

**Experimental alpha — not production-ready.**

## Layout

One distribution, `solera`, built with maturin: four Python packages under
`python/` and a Rust extension.

- `python/solera` — the asset SDK project files import
  (`@asset`, `Output`, `Patch`, `Sql`, `PartitionSet`, `Incremental`,
  `AllPartitions`, `TimePartitions`, triggers, placements), and the key
  index (`solera.keys`). `solera_postgres` ships `PostgresStore`.
- `python/solera_server` — the control plane (state layer, engine,
  FastAPI, CLI) and the built console under `solera_server/web`.
- `python/solera_worker` — the task harness that executes attempts in
  subprocesses and pool workers.
- `native` — the Rust crate built into `solera._native`: the key index's
  per-key work (encoding, decoding, sorting, merging).
- `apps/console` — the pnpm/Vite/TanStack console source. The built bundle is
  committed, so running the server needs no Node.
- `python/solera_server/demo.py` — the self-contained demo project
  (`uv run solera serve --insecure` loads it by default).
- `example/brimstone.py` — a second reference project.

## Quick start

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/) and a Rust toolchain
([rustup](https://rustup.rs)): `uv sync` builds the extension, and rebuilds it
whenever a file under `native/` changes.

```bash
uv sync --locked
uv run solera serve --insecure
# console + API at http://127.0.0.1:8000
```

State lands in `./.solera` — a `file://` object store using the same client
interfaces as S3. Asset data lands in `.solera/data` next to the project
file (`$SOLERA_DATA` overrides it). `--insecure` disables token auth and is restricted to
loopback; set `SOLERA_API_TOKEN` for anything else.

## The demo project

The default project is designed to make every architecture feature visible:

| asset | shows |
| --- | --- |
| `sites` | a `PartitionSet` on a `Cron` — the site list grows one site per run (cursor-driven) and caps at four |
| `uploads` | an external `PartitionSet` source fed by `solera commit` |
| `upload_drop` | a sensor on `Every(15)` committing to `uploads`: a new upload a minute, three at most — its tick history shows committed and skipped ticks |
| `site_feed` | per-site cursor asset on `Every(10)`: `site_events` (unkeyed incremental) + `site_files` (keyed inventory), `Patch` both ways |
| `file_index` | `Incremental(batch_size=2)` consumer — watch `more` continuation; declared `version="2"` |
| `file_checks` | an `Each` edge: one call per changed file, four at a time; the fourth file is rejected on odd feed ticks (the failure index), and `exclude={"drafts": "*-file-2"}` leaves the third out |
| `site_digest` | `site × day` two-dimensional asset (`TimePartitions`), `deps=` on the `roadmap` source, a `bytes` output (pickled by FileStore) |
| `fleet_index` | `AllPartitions` fan-in: `dict[str, list[dict]]` on FileStore, `dict[str, TableRef]` on Postgres |
| `site_status` | `Sql` asset over a `TableRef` (Postgres); on FileStore it logs that it skipped |
| `fleet_status` | Postgres-only `AllPartitions` consumer over `TableRef`s — the SELECT runs in-database |
| `manual_ingest` | the one non-`Local` asset: `Pool("ingest")`, on an `Every(30)` schedule with `partitions="missing"` |
| `weekly_digest` | a `@job` on a weekly `Cron` — inputs and placement, no outputs |
| `refresh-index` | a standalone `Automation` targeting `site_feed` + `file_index` |

The feed fake emits a new batch per site every few seconds, and repeated polls
inside the same tick return identical content — an unchanged commit wakes
nothing downstream. Run config `feed_tick_seconds` stretches the tick:
`--config '{"feed_tick_seconds": 300}'`.

## Walkthrough

Everything below works against the running server. Set
`SOLERA_SERVER_URL=http://127.0.0.1:8000` so the CLI talks to it; unset, the
same commands drive an in-process engine against `SOLERA_STATE_URL`.

```bash
export SOLERA_SERVER_URL=http://127.0.0.1:8000
```

### 1. The growing partition set

```bash
uv run solera run sites        # run it a few times — one site appears per run
```

Console: **Assets → sites** shows the partition-set head; the keys endpoint
(`GET /api/projects/demo/outputs/sites/keys`) grows `alpha → delta`.
The `sites.cron.0` automation keeps it fresh on its own once the server is up.

### 2. Commit external partition keys

```bash
uv run solera commit uploads --upsert '["u-1", "u-2"]'
```

**Sources** in the console lists `uploads` with its committed keys — beside
`drop-1` to `drop-3`, which the `upload_drop` sensor commits on its own. These keys
are work for `manual_ingest` (below) — the `Every(30)` schedule with
`partitions="missing"` plans every key that lacks a complete head, so just
committing is enough once a pool worker is running.

### 3. The per-site cursor asset

```bash
uv run solera run site_feed --partitions all --upstream
```

Each site scope writes a `site_events` incremental batch and a `site_files` keyed
patch, and stores the feed token as its cursor. **Runs** shows the run; click a
task to see its attempt spec — `inputs.site_files` carries the pinned ref, the
pinned key index and the window to read (a delta-log range, or the whole index
for a first delivery); the attempt's result records the keys it delivered.

Run it again inside the same feed tick: the feed returns identical events,
the committed versions are unchanged, and `file_index` is not woken — that is
the "no change wakes nothing" corollary. To see a changed pass, wait one tick
(or shrink it) and let `site_feed.every.0` fire, or `run-now` it.

### 4. Incremental with `more` continuation

```bash
uv run solera run file_index --partitions all --upstream
```

`site_files` holds four files per site; `Incremental(batch_size=2)` delivers them in
two batches — the run detail shows the first attempt completing with
`more: true` and a follow-up attempt finishing the remaining keys.

`file_index` declares `version="2"`. To demonstrate a version bump, edit it to
`"3"` in `python/solera_server/demo.py` and re-run — the interpretation
fingerprint changes and every key reprocesses. To process selected keys only:
`uv run solera run file_index --keys 'site_files=alpha-file-0,alpha-file-1'`.

### 5. Two dimensions: site × day

```bash
uv run solera run site_digest --partitions all --upstream
```

`site_digest` is partitioned by `site` and a daily `TimePartitions` dim, so
its scopes look like `day=2026-09-01,site=alpha` (explicit multi-dim keys use
that canonical comma form with `--partition`). Its `deps=["roadmap"]` pins the
plain source in lineage without loading it. The output is bytes, which
FileStore pickles — check the head's ref on the asset page.

### 6. AllPartitions fan-in

```bash
uv run solera run fleet_index --upstream
```

`fleet_index` receives `file_index` as `dict[str, ...]` keyed by site —
committed heads at pin time, never a barrier on missing keys. With FileStore
the values are `list[dict]`; with Postgres they are `TableRef`s.

### 7. SQL inside Postgres

With `DATABASE_URL` set (next section), `site_status` materializes
`SELECT status, count(*) ... GROUP BY status` into `ops.site_status` and
`fleet_status` unions a per-site count across `site_events` slices — neither
row set enters the harness. Without Postgres, `site_status` still runs and
logs `DATABASE_URL unset — site_status skipped (needs Postgres)` — visible in
the attempt log.

### 8. The job and the standalone automation

```bash
uv run solera automations                        # list all nine
uv run solera automations run-now refresh-index  # fires site_feed + file_index
uv run solera run weekly_digest                  # sends the fake mail
```

**Automations** in the console shows trigger, last fire, and an enable/disable
toggle per automation; `weekly_digest.cron.0` runs Mondays at 07:00.

### 9. The pool worker

`manual_ingest` is placed on `Pool("ingest")` — submitted work waits queued
until an external worker claims it. In a second terminal:

```bash
export SOLERA_SERVER_URL=http://127.0.0.1:8000
export SOLERA_PROJECT=solera_server.demo:project   # the entrypoint the worker executes
uv run solera worker pool ingest
```

Commit a couple of uploads (step 2), then `uv run solera run manual_ingest
--partitions all`. Watch the worker log `[pool] claimed …` / `[pool] completed
…`, and the **Executors** page shows the registered worker and its claimed
task. Pool tasks carry `cpu`/`memory`/`gpu` needs and are only offered to
workers whose capacity fits.

## Postgres

The demo runs entirely on FileStore by default: files under `.solera/data`,
or in a bucket when `SOLERA_DATA_URL` names one (`S3Store`). To move the relational
outputs (`site_events`, `site_files`, `file_index`, `site_status`,
`fleet_status`) into shared Postgres tables:

```bash
docker compose up postgres
export DATABASE_URL=postgresql://solera:solera@127.0.0.1:5432/solera
uv run solera serve --insecure
```

Partitioned outputs share one physical table per output, sliced by their
`partition_column`; unkeyed incremental outputs get a `_batch`/`_seq` snapshot pair so a
pinned `TableRef` keeps reading the version it was committed at. The
engine's write fence keeps a dead attempt from writing over a live one:
an attempt the engine has aborted never writes. `Sql` assets materialize straight into `{schema}.{table}`.

The Compose file also has a full `solera` service:

```bash
SOLERA_API_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  docker compose up --build
```

It wires `DATABASE_URL` to the compose Postgres and keeps object state and
asset data on named volumes — swap `SOLERA_STATE_URL` for `s3://…` in `compose.yml` to run the
same stack on S3.

## Migrations

Schema changes are declared on the output, not implied by the store. An
`Output` carries `migrations=(Migration(name, payload), …)` in order; the
payload is a SQL string or a callable taking a cursor. Registration rejects
migrations on a store without `migrate` (FileStore has none), duplicate
names, and payloads that fail `can_store`.

PostgresStore keeps a `solera_migrations(output, name, at)` ledger and runs
each pending migration plus its ledger row in one transaction under an
advisory lock keyed on the output, so concurrent workers apply each exactly
once. A table that already exists must match the declaration —
drift fails non-retryably instead of triggering a silent `ALTER`. The
harness migrates before the first write in an attempt, and the output's
head carries the last applied name as `schema`.

Apply pending migrations without running the pipeline:

```bash
uv run solera migrate                 # every output that declares migrations
uv run solera migrate site_events     # named outputs only
```

## S3 and compatible services

Point the control plane at a private bucket:

```bash
export SOLERA_STATE_URL=s3://your-private-bucket/orchestrator
export SOLERA_NAMESPACE=development
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export AWS_CONDITIONAL_PUT=etag
export SOLERA_API_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
uv run solera serve
```

For a custom S3 endpoint, `AWS_ENDPOINT` (the Rust object-store client's
setting) plus `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false`; `AWS_ALLOW_HTTP=true`
only for a local HTTP emulator. Before trusting a new S3-compatible service,
run the conformance probe, which writes into a fresh isolated namespace:

```bash
uv run solera selftest --state-url "$SOLERA_STATE_URL"
```

**One coordinator per namespace.** A second writer on the same namespace
fences the first. Give ad-hoc CLI runs a distinct `--namespace`, or submit
through the server instead.

## CLI

```text
solera serve [--project SPEC] [--insecure]   API + console (default project: the demo)
solera manifest --project SPEC               print the project manifest
solera run TARGET... [--partitions latest|all|missing] [--partition KEY]
           [--upstream] [--full] [--keys EDGE=k1,k2] [--config JSON] [--tag NAME=VALUE]
solera runs [--status S] [--asset A] [--tag NAME=VALUE] [-q TEXT] [--before RUN_ID] [--limit N]
solera run-show RUN_ID / logs RUN_ID ATTEMPT_ID [--tail N]
solera runs delete RUN_ID                    delete a finished run
solera runs prune [--before DATE] [--asset A] [--keep N] [--dry-run]
solera automations [enable|disable|run-now NAME]
solera migrate [OUTPUT...]                   apply pending output migrations locally
solera commit SOURCE [--version V] [--keys JSON] [--upsert JSON] [--remove K]
solera worker pool NAME [--server URL]       claim and run pool tasks
solera worker sensors NAME [--server URL]    host the sensors of Pool(NAME)
solera scopes release ASSET [SCOPE]          release a scope held for an uncertain writer
solera scopes discards OUTPUT [SCOPE] [--clear]  pending and stuck data garbage
solera selftest                              storage conformance probe
```

`--project` accepts `module:attribute`, a `file.py` path, or `file.py:attr`;
it also reads `SOLERA_PROJECT`. With `SOLERA_SERVER_URL` set every command talks
to the server (token from `SOLERA_API_TOKEN`); without it they drive a local
engine against `SOLERA_STATE_URL`/`--state-url` (`--namespace` selects the
namespace). `solera worker pool` additionally needs `SOLERA_PROJECT` so claimed
attempts can load the project.

## Authoring

```python
from solera.sdk import Output, PartitionSet, Project, TimePartitions, asset
from solera.stores import Patch

sites = PartitionSet("sites")


@asset(outputs=Output("files", key="file_id", revision="version"), partitions={"site": sites})
def site_files(ctx):
    # Patch both ways: rows upsert by key, remove deletes keys.
    return Patch(rows, remove=gone)


@asset(
    outputs=Output("digests", key="file_id"),
    partitions={"site": sites, "day": TimePartitions(start="2026-01-01", every="1d")},
)
def daily_digests(ctx, site_files): ...


project = Project(assets=[site_files, daily_digests], sources=[sites], name="mine")
```

Save as `my_project.py` and `uv run solera serve --insecure --project
my_project.py` (the attribute defaults to `project`). Cursors, resources,
`Incremental`/`AllPartitions` inputs, placements, triggers, and the Postgres stores
are documented in [docs/architecture.md](docs/architecture.md);
`python/solera_server/demo.py` exercises all of them.

## Tests

```bash
uv run pytest -q            # includes tests/test_demo_e2e.py (server + pool worker, temp file:// store)
uv run pytest -q --slow     # also the long growth soaks (tests/test_soak.py), minutes
SOLERA_TEST_DATABASE_URL=postgresql://solera:solera@127.0.0.1:5432/solera uv run pytest -q -m postgres
SOLERA_TEST_S3=http://user:secret@127.0.0.1:9000/bucket uv run pytest -q   # also against an S3-compatible server
pnpm install --frozen-lockfile && pnpm -C apps/console exec playwright install chromium && pnpm -C apps/console test
```

Tests are the spec's examples: a test of a documented rule names its
section in its docstring (`"""§6: a version bump reprocesses every key."""`).
The object store is always real — `file://` through `obstore`, or an
S3-compatible server — never an in-memory stand-in, so conditional puts
and listings stay exercised. Postgres and S3 tests skip unless their
variable is set.

CI runs the Python suite, the Playwright suite on desktop and mobile, and a
wheel-contents check. `uv.lock` and `pnpm-lock.yaml` are committed; installs
are locked.

## Boundaries

This is a feasibility implementation, not a claim of production readiness.
One engine writes each namespace, through an append-only journal on the
object store; a second one fences the first rather than sharing the work.
Remote placements (AWS ECS, Kubernetes jobs, Modal) exist behind `SOLERA_*`
configuration but see far less exercise than `Local`/`Pool`. There is no
retained historical code image. Read
[docs/object-store-state.md](docs/object-store-state.md) for the storage
design and [docs/architecture.md](docs/architecture.md) before extending the
backend.

## License

Apache-2.0.
