# The behaviour contract, executable

`docs/behaviors.md` is the contract every design of Solera keeps. Each of
its entries has tests here, named by its id, or a reason it has none yet.
A unit of the tracking redesign is done when the entries it names pass on
the new core.

## Running

```
uv run pytest tests/contract                          # today's engine
SOLERA_TRACKING=core uv run pytest tests/contract     # the new core
uv run python -m tests.contract.check                 # entries, tests, pending
```

Tests that need Postgres are marked `postgres` and run when
`SOLERA_TEST_DATABASE_URL` is set, as everywhere in `tests/`.

## What a contract test may do

Only what a user can: declare a project with the SDK, serve it, and use the
engine's HTTP API, the stores and a worker. The `solera` fixture
(`harness.Solera`) is one namespace served on one tracking path. It offers:

- serving: `deploy(project)` (again, for a new deploy), `restart()`,
  `close()`;
- runs: `submit`, `wait`, `run(targets, expect="succeeded", keys=, mode=,
  partitions=, …)`, `cancel`, `runs`, `settle(seconds)` (the engine works
  with nothing asked);
- sources: `commit_source(name, version=, keys=, upsert=, remove=)`;
- reads: `get`/`post`/`request` on any route, `stale`, `stale_keys`,
  `status`, `keys`, `head`, `read` and `rows` (an output through its
  store, as a downstream reader gets it);
- time: `advance(seconds)` moves the clock the engine plans by (retry
  backoffs, schedules, retention). Heartbeats, cancels, timeouts, sensor
  schedules and upkeep run on real time: tests shorten them with serve
  settings (`deploy(project, heartbeat_seconds=0.2, cancel_grace=…)`);
- waiting: `until(condition)`, `quiet()`, `upkeep()`;
- outcomes and history: `outcomes`, `explain`, `retry_keys`, `commits`;
- workers, hosts and processes a test plays: `workers(boot=)` and
  `kill_workers()`, `work(pool, project)` and `worker(stage)` (a pool
  worker), `offered(pool)`, `sensors`, `sensor_host`, `sensor_ticks` and
  `sensor_post`, `reloading(entry)` (a `--reload` serve), `serve_process()`,
  `cli(*argv)`;
- outages and takeovers: `start()` (the engine's own loops), `down()`,
  `outage(seconds)`, `successor()` (a second engine on the namespace);
- faults an operator or a broken worker causes: `tamper(run, attempt,
  change)` rewrites an attempt's control file, `refuse_upkeep(output)` makes
  an output's index upkeep fail. Both know today's storage layout, so the
  new path needs its own version of each.

`example.py` is the contract's running example (`feed → items → tally,
copy, checks → filtered`, `factor`), with what each producer received batch
by batch: what user code sees, never what the engine holds.

A test may not import the engine, the worker, or the SDK's tracking
internals (`check.INTERNAL`). Only `harness.py` and `conftest.py` reach the
engine, and only through its API and its lifecycle. The completeness check
enforces both rules.

## Adding an entry

1. Give the entry its id and text in `docs/behaviors.md`.
2. Write its test in `test_<area>.py`, named `test_<area>_<n>_<the rule>`
   (`test_inc_4_…` is INC-4's). Several tests of one entry share the prefix.
3. Point the entry's *Covered* line at it.
4. If today's engine lacks the entry's behaviour (the new core changes or
   adds it), list it in `entries.TARGET` with the decision: its tests assert
   the target, and today's path skips them. `SOLERA_CONTRACT_TARGET=1` runs
   them on today's engine anyway, to see them fail for the right reason.
5. If it cannot be tested through what a user reaches, list it in
   `entries.PENDING` with the reason instead.

`test_completeness.py` fails when an entry has neither a test nor a
pending reason, when a test names no entry, or when a test imports an
internal.

## The two paths

`SOLERA_TRACKING` selects the path; `harness.engine_options` turns it into
the engine's constructor arguments. Until the new core exists (build plan
B5), `core` skips every test that serves a namespace.
