# cursus — implementation plan

`docs/architecture.md` is normative. `example/brimstone.py` is the reference
project and must register without error at every gate from Phase 1 on.
This plan turns the current experimental code (`apps/server/src/cursus_server`,
`apps/worker/src/cursus_worker`, `packages/sdk/src/cursus`,
`apps/console`) into a working implementation of that document. Where
existing code disagrees with the document, the document wins and the code
is replaced, including its tests. Do not keep old concepts (`Inventory`,
`Batch`, `OnCommit`, `Backend.execute`, `Replace`, `AppendBatch`) alive next
to the new ones.

## Ground rules

- **Keep** the infrastructure that already works: SlateDB for transactional
  state, `obstore` for objects (`file://` and `s3://`), FastAPI + uvicorn,
  the `cursus` CLI entry point, the TanStack/shadcn console, `uv` and `pnpm`
  workspaces, `--insecure` loopback rule, `CURSUS_*` environment variables,
  `cursus selftest`.
- **Every phase ends with a gate.** A gate is green when all of these pass
  from a clean checkout:

  ```bash
  uv run ruff check . && uv run ruff format --check .
  uv run pytest -q                       # unit + integration, file:// backend
  uv run cursus manifest --project example/brimstone.py   # registers cleanly
  ```

  Phases 6 and 7 add `pnpm -C apps/console typecheck` and `pnpm -C
  apps/console test` (Playwright). Commit at each gate with a message that
  names the phase. Do not start the next phase on a red gate.
- **Tests are the spec's examples.** Each rule in the architecture doc that
  has observable behaviour gets a test that names the section in its
  docstring, e.g. `"""§6: a version bump reprocesses every key."""`.
- **Postgres is optional in CI, required for the demo.** Tests that need it
  are marked `postgres` and skip unless `CURSUS_TEST_DATABASE_URL` is set.
  `compose.yml` gets a `postgres` service so the demo and the marked tests
  can run locally.
- **Object store is always real.** Tests use `tmp_path.as_uri()` (file://)
  through `obstore`, never an in-memory dict, so S3 semantics (conditional
  put, listing) stay exercised. The existing `moto` dev dependency stays
  available for the `s3` marker.
- Keep the line length at 110 and `ruff` selection as configured. Python
  3.12+. Async engine, sync-or-async producers.

## Phase 1 — SDK: declarations, manifest, stores

Package `packages/sdk/src/cursus`. Everything a project file
imports. No engine code.

Deliverables:

- `Output(name=None, store=None, key=None, revision=None, mode=None,
  **config)`, `PartitionSet(name=None)`, `Source(name, store=None,
  key=None, **config)`, `Result(outputs, cursor)`, `Patch(rows,
  remove=())`, `Sql(stmt)`, `In`, `ByKey`, `AllPartitions`,
  `StaticPartitions`, `TimePartitions`, `Every`, `Cron`, `OnChange`,
  `Automation`, `AutoRefresh`, `Retry`, `@asset`, `@job`, `Project`,
  `Project.from_package`.
- Environments and placements: `Local`, `AWSECS`, `K8sJob`, `Modal`,
  `Pool`; each environment is callable and returns a frozen placement
  dataclass with typed options; `placement.serialized()` gives `{kind,
  environment, placement}` for the manifest and `ctx.execution`.
- Refs: `Ref`, `JsonRef`, `BlobRef`, `TableRef` (`.table`, `.where`,
  `.sql()`), all JSON round-trippable via `Ref.to_json()` / `Ref.from_json()`.
- Store protocol (§4): `can_load`, `can_store`, `store`, `load`; `Scope`,
  `Written`, `Keys`; `Store.version` class attribute.
- `JsonStore` (default, object store under `objects/{output}/{sha}.json`,
  supports bare values and `Patch` on `list[dict]` including
  `mode="append"` batches), `BlobStore` (`bytes` / `Path`), `PostgresStore`
  (`DataFrame`, `GeoDataFrame` optional, `list[dict]`, `Patch`, `Sql`,
  `partition_column` slicing, version marker table `cursus_markers(output,
  partition, version, batch)`, append batches via a `_batch` column).
- Manifest builder (§11): assets, outputs, inputs with edge kinds and
  `meta`, deps, partitions, placement, retries, timeout, version, code
  hash, load types from `typing.get_type_hints`, sources with synthesized
  heads, automations with their run fields, store names + versions,
  executor kinds, project revision. Deterministic JSON.
- Every registration error in §11 raised as `RegistrationError` with the
  asset/output name in the message.
- `TimePartitions` key generation: durations and cron `every`, `end`,
  `end_offset`, `timezone`, `format`, `"latest"` selection.

Tests (`tests/sdk/`):

- Manifest of `example/brimstone.py` is stable (snapshot in
  `tests/sdk/snapshots/brimstone.manifest.json`, regenerated on purpose
  only).
- One test per registration error in §11 (fourteen cases).
- Projection rule (§7): shared dimension identity, consumer-only
  broadcast, upstream-only requires `AllPartitions`, `ByKey` with
  upstream-only dimensions rejected.
- `TimePartitions`: keys for `every="1d"`, `"15m"`, a cron expression;
  `end_offset`; timezone alignment; `"latest"`.
- JsonStore: bare replace versions equal for equal content; `Patch`
  returns the complete key map merged with `prior_keys`; `remove` drops
  keys; duplicate keys are a write error; append batches number from
  `prior_keys`; `load` with `Keys` selection returns only those keys.
- PostgresStore (marker `postgres`): same matrix as JsonStore plus the
  version marker check on `store` and `load`, `partition_column` stamping
  and rejection of disagreeing rows, `Sql` materializing a SELECT into
  `{schema}.{table}` and returning a loadable `TableRef`, append snapshot
  `load(ref)` returning batches `<=` the ref's newest batch after later
  writes.
- Version recipe (§3): bare → `H(payload)`; `Patch` → `H(prior ‖ H(op))`;
  empty `Patch` → prior unchanged.

Gate 1: standard gate. `cursus manifest --project example/brimstone.py`
prints the manifest.

## Phase 2 — Server state

Package `apps/server/src/cursus_server/state.py` (replaces `storage.py`'s domain
layer; keep the SlateDB/obstore plumbing).

Deliverables, all keyed under the namespace and written through SlateDB
transactions:

- Project revision + manifest; heads per `(output, scope)`; commits
  (attempt, heads installed, `input_refs`, cursor, `changed` list, time);
  cursors per `(asset, scope)`; per-edge key state per `(consumer, edge,
  scope)` mapping `key → (revision, fingerprint)`; automation state
  (enabled, last fired, pending events); runs, tasks, attempts with
  per-scope lock, lease and generation; worker registrations and pool
  tasks with claim leases.
- Key maps stored as objects `keys/{sha256}.json`; helper to stage and
  fetch them, with the object count recorded in `ref.meta.keys`.
- Fencing primitive: `claim(scope) -> generation`, `renew(attempt)`
  raising `LostOwnership`, `commit(attempt, result)` refusing on stale
  generation or changed output heads since claim.

Tests (`tests/server/test_state.py`):

- Commit is atomic: heads, cursor, key state and pending automations all
  appear or none do (inject a failure mid-transaction).
- Stale generation cannot commit; a commit after the head moved is refused.
- Lease expiry lets a second claimant take the scope; the first's commit
  fails with `LostOwnership`.
- Key map round-trip through the object store, hash-addressed, idempotent.
- Restart: reopen the state and find active attempts with their handles.

Gate 2: standard gate.

## Phase 3 — Engine

`apps/server/src/cursus_server/engine.py` rewritten around §6–§9.

Deliverables:

- **Planning.** A run `{targets, partitions, mode, upstream, config,
  keys}` becomes tasks per `(asset, scope)`: partition selection
  (`"latest"`, `"missing"`, `"all"`, list), `upstream=True` closure,
  retired keys excluded, key sets read from the current key map of the
  partition-set output.
- **Input resolution.** Heads at attempt start by the projection rule;
  `AllPartitions` resolved to keys with complete heads at pin time; every
  pinned input must have a head (sources synthesize theirs).
- **ByKey.** Diff of the upstream key map against per-edge key state,
  batched by `batch_size`; `more` re-queues; `scope_complete` on the head;
  interpretation fingerprint `H(version, store versions, run config, refs
  of non-ByKey inputs and deps)`; `on_version_change` `fail` /
  `recompute`; run `keys` overrides (explicit list, `"full"`).
- **Outcomes.** `succeeded`, `skipped` (all diffs empty and heads
  complete, no harness launched), `failed` retryable with `Retry`
  backoff, non-retryable, `canceled`.
- **Commit validation.** Attempt id, refs name known outputs and this
  scope, keyed outputs carry a key map, omitted outputs keep prior head
  (error if none), cursor semantics, `changed` computed from version
  comparison only.
- **Sources commit API** (§5): `version=`, `keys=`, `upsert=`/`remove=`,
  PartitionSet patch; identical map is not a change.
- **Automations.** Eval loop: `Every` with floor, `Cron` with timezone,
  `OnChange` pended in the commit transaction and fanned by projection,
  skip-if-active per scope, `partitions="missing"` on a schedule, toggles
  by name, run-now. Attached names `{asset}.{trigger}.{index}`.
- **Placement loop** (§10) exactly as the pseudocode: put spec, launch,
  wait with lease renewals, timeout → cancel, `LostOwnership` → cancel,
  missing result → retryable failure, restart resumes `active/` attempts
  at `wait`, `max_concurrent` per environment.
- Engine takes a `placements` registry (built-ins + `Project(executors=)`)
  and an `InlinePlacement` for tests that runs the harness in-process
  against the real object store.

Tests (`tests/server/test_engine.py`, split by section if long). Each
scenario uses small inline projects, not brimstone:

- §2 return values: bare, `Result`, omitted output keeps head, omitted
  output without head errors, cursor set/kept/cleared on recompute.
- §5 edges: rename, `In(meta=)` recorded, `ByKey` filtered inputs and
  `ctx.changes`, `AllPartitions` dict of values and of refs, `deps` pinned
  but unbound, by-reference annotation receives the ref.
- §6: first write after key declaration upserts everything; only changed
  revisions reprocess; fingerprint change reprocesses all; code change
  alone does not; `version` mismatch fails non-retryably; `recompute`
  option; `batch_size` with `more`; `keys="full"` and explicit keys.
- §7: static, time, `PartitionSet` asset, external `PartitionSet` via
  commit; two-dimension asset with broadcast and collapse; retired keys
  leave fan-out but keep heads; `"latest"` / `"missing"` / `"all"`.
- §8: fencing under a concurrent claim; `upstream=False` never re-plans
  inputs; `recompute` clears prior, cursor and key state; `skipped`
  outcome launches nothing; retries with backoff and non-retryable
  classification; cancel.
- §9: `Every` skip-if-active; `Cron`; `OnChange` fan-out by projection;
  `AutoRefresh` over inputs and deps; standalone automation with
  `targets`; enable/disable; run-now; `partitions="missing"` picks up new
  keys and failed first runs; self-trigger rejected at registration.
- §10 loop: timeout cancels and fails retryably; lost ownership cancels;
  harness exit without result fails retryably; restart resumes at `wait`
  with a fake placement whose `wait` returns after reopen; `max_concurrent`
  respected.
- §1 corollary: a poll producing identical content yields no `changed`
  and wakes nothing.

Gate 3: standard gate. Engine tests run under two seconds each.

## Phase 4 — Harness and placements

`apps/worker/src/cursus_worker` and `apps/server/src/cursus_server/placements/`.

Deliverables:

- `python -m cursus_worker run --objects URL --attempt ID` per §10: fetch
  spec, refuse on revision mismatch as a failed result, resolve `env:`
  indirection, load inputs per annotation (`ctx.load` for refs), build
  `ctx`, run the producer (sync or async), `store()` each returned output,
  stage key maps, write the result last in one PUT, stream logs to
  `logs/{attempt}/{seq}.jsonl`. Project entrypoint from `CURSUS_PROJECT`.
  `manifest` mode.
- `Local` placement: subprocess with env allow-list, handle `{pid,
  started_at}`, mismatch = lost, SIGTERM then SIGKILL.
- `Pool` placement and worker endpoints: `POST /api/workers/register`,
  `POST /api/tasks/claim`, `POST /api/tasks/{id}/renew`, `POST
  /api/tasks/{id}/complete`; claim fit by `cpu`/`memory`/`gpu`; expired
  claims swept in the eval loop; `python -m cursus_worker pool --pool NAME
  --server URL` loops claim → run → complete.
- `AWSECS`, `K8sJob`, `Modal`: implemented against their SDKs behind
  optional extras, unit-tested with stubbed clients, not exercised in the
  demo. Their `launch` passes the two stage strings as documented.

Tests (`tests/worker/`, `tests/server/test_placements.py`):

- End to end with `Local` on `file://`: a two-asset project materializes,
  key map staged, result committed, logs readable through the API.
- Revision mismatch produces a failed result, not a crash.
- Harness killed mid-run: no result object, engine fails retryably, retry
  succeeds.
- Pool: a worker registers, claims a task that fits, renews, completes;
  a task that fits nobody waits; an expired claim is re-queued and the
  late `complete` is rejected; cancel stops a renewing worker.
- Stubbed `AWSECS` / `K8sJob` / `Modal`: `launch` passes `attempt` and
  `objects`, `wait` maps terminal states to `Exit`, vanished run is
  `Exit(None, "lost")`.

Gate 4: standard gate plus `tests/worker` end-to-end through a real
subprocess.

## Phase 5 — API and CLI

`apps/server/src/cursus_server/api.py`, `cli.py`.

Deliverables (all under `/api/projects/{p}` unless noted, token auth as
today):

- `GET manifest`, `GET assets`, `GET assets/{name}` (outputs, edges,
  partitions, placement, automations), `GET outputs/{name}/heads`
  (per scope: ref, version, key count, complete, cursor present), `GET
  outputs/{name}/keys?scope=` (paged key map), `GET partitions/{asset}`
  (current key set with head status: complete / missing / retired).
- `POST runs` with the run vocabulary, `GET runs`, `GET runs/{id}` (tasks,
  attempts, outcomes), `POST runs/{id}/cancel`, `GET attempts/{id}/logs`
  (streaming or paged), `GET attempts/{id}/spec` and `/result`.
- `GET automations`, `POST automations/{name}/enable|disable|run-now`.
- `POST sources/{name}/commit` (§5 shapes).
- `GET environments` (kinds, `max_concurrent`, in-flight), `GET workers`,
  worker/task endpoints from Phase 4.
- `GET healthz`, `GET diagnostics` (backend, namespace, revision).
- CLI: `cursus serve`, `cursus manifest`, `cursus run TARGET… [--partition K]…
  [--partitions latest|missing|all] [--recompute] [--upstream] [--config
  JSON] [--keys EDGE=full|k1,k2]`, `cursus runs`, `cursus run-show ID`,
  `cursus logs ATTEMPT`, `cursus automations [enable|disable|run-now NAME]`,
  `cursus commit SOURCE [--version V | --keys JSON | --upsert JSON --remove
  K…]`, `cursus worker pool NAME`, `cursus selftest`. Every command talks to a
  running server when `CURSUS_SERVER_URL` is set, else runs an in-process
  engine against `CURSUS_STATE_URL` (one coordinator per namespace rule
  stays).

Tests (`tests/server/test_api.py`, `tests/server/test_cli.py`): every
endpoint once through `httpx` against `create_app`; CLI commands through
`subprocess` against a served instance; auth required except `healthz`;
`--insecure` only on loopback.

Gate 5: standard gate.

## Phase 6 — Console

`apps/console`. Keep the existing shell, routing and component library.
Replace pages to match the new API.

Deliverables:

- **Assets**: graph of assets and sources with edge kinds; asset sheet
  with outputs, store, key column, placement, automations with toggles,
  partition grid (one cell per scope, coloured complete / missing /
  retired / running / failed, multi-dimension as rows × columns), key
  count per scope, cursor presence, materialize button opening the run
  dialog.
- **Run dialog**: targets, partitions (`latest` / `missing` / `all` /
  pick from grid), mode, upstream, config JSON, per-edge keys override.
- **Runs**: list with outcome badges; run page with tasks and attempts,
  attempt logs live-tailed, spec and result viewers, cancel.
- **Automations**: list with trigger, run fields, enabled toggle, last
  fired, run-now.
- **Sources**: current head and key map preview; commit form for the three
  shapes; PartitionSet patch form.
- **Executors**: environments with in-flight counts; pool workers with
  capacity and current task.
- Data preview stays out (§12 non-goal); show refs, handles and versions
  instead.

Tests (`apps/console/tests/*.spec.ts`, Playwright against the demo
project of Phase 7 running on `file://`): each page loads; materialize
`latest` from the dialog and watch the grid turn complete; toggle an
automation; commit a source and see the downstream run appear; tail logs
of an attempt; cancel a run. Desktop and mobile projects as configured.

Gate 6: standard gate plus `pnpm -C apps/console typecheck`, `pnpm -C
apps/console format:check`, `pnpm -C apps/console test`. Rebuild the
bundle (`pnpm -C apps/console build`) and commit `apps/server/src/cursus_server/web`.

## Phase 7 — Runnable demo

`apps/server/src/cursus_server/demo.py` rewritten as a self-contained project that
exercises everything in the doc with in-process fakes, so it runs with
`uv run cursus serve --insecure` and nothing else, and uses Postgres when
`DATABASE_URL` is set (compose service).

The demo project must contain, and the README must name, all of:

- a `PartitionSet` asset on a `Cron` (simulated site list that grows on
  each run), and an external `PartitionSet` under `sources=` fed by
  `cursus commit`;
- a per-site cursor asset with an append output and a keyed inventory
  output (`Patch` both ways), on `Every(10)`, backed by a fake feed
  resource that emits new events every few seconds and repeats identical
  content sometimes so the "no change wakes nothing" corollary is visible;
- a `ByKey` consumer with `batch_size` small enough to show `more`, and
  `version="2"` bumpable from the README;
- a two-dimension asset (`site` × `TimePartitions` daily) with
  `deps=` on a plain `Source`;
- an `AllPartitions` consumer producing a `dict[str, TableRef]` (Postgres)
  or `dict[str, list[dict]]` (JsonStore) rollup;
- a `Sql` asset over a `TableRef` (Postgres only, skipped with a log line
  when unset);
- a `BlobStore` output;
- a `@job` on a weekly `Cron` and a standalone `Automation` over two
  targets;
- `Local` placement for everything except one asset on `Pool("ingest")`,
  with README instructions to start `cursus worker pool ingest` in a second
  terminal and watch the task get claimed;
- `partitions="missing"` on a schedule for the external set.

Deliverables:

- `tests/test_demo_e2e.py`: starts the server on a temp `file://` store,
  submits `cursus run` for the rollup with `--upstream`, commits the
  external set, starts a pool worker, and asserts within a bounded wait
  that every asset has a complete head for `latest`, that a second tick
  of the cursor asset with identical feed content changes nothing, that
  a `ByKey` consumer processed only the changed keys on the second pass,
  and that the pool task was claimed and completed.
- README rewritten: quick start, the demo walkthrough in the order above
  with the CLI commands and what to look at in the console at each step,
  Postgres via `docker compose up postgres`, the pool worker terminal,
  and how to point the server at S3.
- `example/brimstone.py` still registers; `example/lab.py` deleted or
  ported (delete unless it takes under an hour to port).

Gate 7 (final): standard gate, console gates, `tests/test_demo_e2e.py`,
and a manual checklist in the PR description ticking each demo item with
the CLI command or console page used to verify it.

## Definition of done

- All seven gates green from a clean clone with `uv sync --locked` and
  `pnpm install`.
- One PR against `feat/s3-state-backend`, or a series of stacked PRs one
  per phase, each with the gate output pasted in the description.
- `docs/architecture.md` unchanged except for corrections found during
  implementation, each called out in the PR with the reason; the plan
  author reviews those.
- No TODOs left that reference this plan; open questions go in the PR.

## Phase 8 — Integrate on `main`, migrations, `OnDeploy`, per-scope outcomes

Context. PR #3 (branch
`bb/implement-cursus-architecture-plan-docs-implementa-thr_9uw5mdrpwk`,
seven phase commits, all gates green) implements Phases 1–7 under the old
`cursus` naming and targets `feat/s3-state-backend`. Meanwhile `main` renamed
the project to **cursus** (`dda6b06`: `cursus` → `cursus`,
`cursus` → `cursus_server`, `cursus_worker` → `cursus_worker`, `cursus` CLI →
`cursus`, `CURSUS_*` → `CURSUS_*`, `.cursus` → `.cursus`, `@cursus/ui`) and
collapsed the three distributions into one root `cursus` package
(`8c4d062`). `main` does **not** contain the implementation.

### 8a. Land the implementation on `main`

- Rebase or merge the seven phase commits onto `main`, applying the rename
  to every new file: package paths, imports, CLI name, env vars, compose
  service, README, Playwright config, test fixtures, the `cursus_postgres`
  package (→ `cursus_postgres`, which `example/brimstone.py` already
  imports), the console session key and API client. Keep the single-root
  distribution from `8c4d062`: no per-component `pyproject.toml`.
- Retarget PR #3 to `main` (or open a fresh PR and close #3 with a link).
- Gate 8a: the full Phase 7 gate on `main` naming: ruff, `pytest`
  (file:// and `postgres`-marked against compose), `tests/test_demo_e2e.py`,
  `cursus manifest --project example/brimstone.py`, console typecheck,
  format check and Playwright; `grep -rIl "cursus\|cursus\|CURSUS_"`
  over the tree returns nothing outside git history.

### 8b. Migrations on outputs (§2, §3, §4, §6, §11)

- SDK: `Migration(name, payload)`; `Output(migrations=())`; manifest
  records the ordered names per output; registration errors for a store
  without `migrate`, duplicate names, and a payload failing `can_store`.
- Store protocol: optional `migrate(output, migrations) -> list[str]`.
  `PostgresStore` implements it with a `cursus_migrations(output, name,
  at)` ledger, a `pg_advisory_xact_lock` keyed on the output, and each
  migration plus its ledger row in one transaction. Payloads: SQL string
  or `Callable[[cursor], None]`. `BlobStore` implements it with a
  `_migrations.json` ledger object under the output prefix and callable
  payloads; `JsonStore` has no `migrate` and therefore rejects the
  argument.
- `PostgresStore` table creation becomes create-if-missing only: drop the
  `ALTER TABLE ADD COLUMN` path from `_ensure`. Before a write, compare the
  live table against the declared config (`primary_key`, `partition_column`,
  declared `columns`) and fail non-retryably with a message naming the
  difference. Type inference from rows stays for creation only.
- Harness: before the first `store()` to an output in an attempt, call
  `migrate` when the output declares migrations; the last applied name
  goes into the handle as `schema`. Failure is a failed result with
  `retryable=false`.
- Engine: the interpretation fingerprint includes the migration names of
  the asset's outputs.
- CLI: `cursus migrate [OUTPUT…]` runs `migrate` for the named outputs (all
  migrating outputs by default) through a `Local` harness and prints the
  applied names.

Tests:

- `tests/sdk/test_manifest.py`: migrations in the manifest; the three
  registration errors.
- `tests/sdk/test_postgres.py` (marker `postgres`): pending migrations
  applied in order and recorded; second call applies nothing; two
  concurrent `migrate` calls apply each migration once; a failing
  migration leaves no ledger row; live-table drift (PK differs from the
  declaration) fails the write with the drift message; a migration that
  brings the table in line lets the write pass.
- `tests/sdk/test_blobstore.py`: ledger object round-trip, callable
  payload applied once.
- `tests/server/test_engine.py`: adding a migration changes the
  fingerprint and reprocesses every key; the handle of the new head
  carries `schema`.
- `tests/worker/test_worker.py`: `migrate` runs before the first write in
  a real subprocess attempt; a failing migration yields a non-retryable
  failed result.
- `tests/server/test_cli.py`: `cursus migrate` prints applied names and
  is idempotent.

### 8c. `OnDeploy()` (§9)

- SDK trigger; automation state gains `last_revision`; the eval loop fires
  each `OnDeploy` automation once when the served project revision
  differs from `last_revision`, then records it. Skip-if-active applies.
- Console: automations page shows `OnDeploy` with the revision last fired
  for.

Tests: fires once per new revision; silent on restart with the same
revision; two registrations before a tick fire once for the latest; the
demo project gets an `OnDeploy` job so `tests/test_demo_e2e.py` sees it
fire on boot.

### 8d. Per-scope outcomes instead of task-history scans (§8)

- State: `scope/{asset}/{scope}` record with `{last_outcome, last_attempt,
  at}`, written in the transaction that finalizes a task (succeeded,
  skipped, failed, canceled). The `pending/{asset}/{scope}/{task}` index
  already exists; keep it as the only source for "running".
- `GET /api/projects/{p}/partitions/{name}` reads heads, the scope
  records and the pending index; remove the `task/` scan. Same for any
  other endpoint or console view that scans `task/` (audit with
  `grep -rn 'scan("task/'`).
- `GET assets/{name}` and the console partition grid show `last_outcome`
  and `last_attempt` per cell, linking to the attempt.

Tests: `tests/server/test_api.py` asserts the endpoint's statuses for
complete, missing, running, failed and retired scopes using only the
state written by the engine, with a check that no `task/` scan occurs
(instrument `tx.scan` in the test); a scope that failed and later
succeeded reports `complete`.

### Gate 8

Gate 8a plus every test above, the console gates, and `tests/test_demo_e2e.py`
extended with: the demo's Postgres outputs declare one migration each and the
e2e run (Postgres variant) shows them applied; the `OnDeploy` job fires on
boot. README gains a "Migrations" section and `cursus migrate` in the CLI
table. One PR against `main`, gate output in the description.
