# Verification: the deterministic simulation

Solera's bugs live where two features meet: a rename while an attempt is in
flight, a cancel in the middle of a paged delivery, a new engine opening while
the old one still writes. Hand-written tests check one feature at a time; the
simulation in `tests/sim` checks them together. It runs the real engine and the
real worker harness against a real `file://` object store, lets
[Hypothesis](https://hypothesis.readthedocs.io/en/latest/stateful.html) choose
long random sequences of things that happen to a deployment, checks invariants
after every step, and shrinks a failure to its shortest sequence. Like
[Antithesis](https://antithesis.com), everything in it is deterministic: one
seed replays the same run, request for request.

## What it models

**The world** (`tests/sim/world.py`) is one deployment on one namespace:

| Part | In the simulation | As in production |
|---|---|---|
| Engine | `Engine` on a `State`, started, stopped, crashed (every task killed, nothing buffered written) and replaced while still running (a *zombie* until the platform kills it) | the server process |
| Workers | `run_attempt` in process, one task per invocation, through `SimPlacement` (`Local`); handles outlive engines | a subprocess, container or job |
| Channel | `SimChannel`: each call runs as a request of whichever engine serves now, answered once durable (the API middleware's rule) | HTTPS to a stable engine URL |
| Heartbeats | the worker's `Reporter`, beating from a task instead of a thread | a thread |
| Key cache | the engine's `KeyService` on the simulation's loop instead of its own thread | a thread with its own loop |
| Stores | FileStore (`immutable`); `TableStore` (`fenced`, `tests/sim/stores.py`): an in-memory database whose transactions take the slice's fence and can stay open or lose their answer; and, with `SOLERA_TEST_DATABASE_URL`, a real PostgresStore in a schema of its own per run, every transaction and read recorded (`tests/sim/postgres.py`) | FileStore, S3Store, PostgresStore |
| Sensor host | a rule that polls `sensor_next` and posts each tick's outcome, late or twice | `solera_worker sensors` |
| Clients | rules calling the engine's API; a failed call is an unknown outcome | the API, the CLI |

**Time is virtual** (`tests/sim/core.py`). `SimLoop` is an asyncio event loop
whose clock jumps to the next timer whenever nothing is ready, so ten minutes of
heartbeats, retries and timeouts cost a few milliseconds, and `time.time()`
reads the same clock. Nothing runs beside the loop: object requests are
answered synchronously through obstore's blocking client, and work handed to a
thread runs to its end while the loop waits. Attempt ids, invocation tokens and
writer nonces come from the run's seed.

**The fault plan.** Every object request an actor makes goes through
`Objects._request`, which can:

- fail it before it reaches the store (`error`, an injected 503);
- let it land and lose the answer (`lost`, a connection reset after the write);
- delay it (`delay`, up to so many virtual seconds), letting other actors run
  meanwhile.

The `TableStore`'s transactions take the same plan: refused, committed with a
lost answer, or held open (with the fence row locked) for a while. A killed
actor can make no request at all: a crashed engine's tasks and a dead worker's
stop where they are, and their `finally` blocks find the store gone.

**The project** (`tests/sim/project.py`) is small and covers every edge kind:

```
feed (keyed source) ──Incremental──▶ items ──Incremental(page 2)──▶ copy
items ──Each(page 2)──▶ checks              (fails while a key is "flaky")
knob (version) ──dep──▶ per_site[site ∈ sites] ──AllPartitions──▶ summary
knob ──dep──▶ log (batches) ──Incremental──▶ tally
outside (keyed source) ◀── watch (a sensor over an external map)
```

Every producer is a pure function of its inputs, so the oracle computes what
each output must hold from the sources alone: `items` is `feed` with the
asset's version appended to each value, `copy` is `items` minus excluded keys,
`checks` is `x` + each `items` value, `tally` counts `log`'s rows, and so on.
Re-registration moves between *variants*: `items` on either store, its declared
version bumped, `copy` renamed to `mirror` (with an alias), `summary` removed,
`copy`'s edge excluding `k1*`.

## Rules

Each rule is one thing that happens. Hypothesis picks them, with their
arguments, up to 40 per run.

| Rule | Example | What it exercises |
|---|---|---|
| `commit_feed(op, keys, version)` | `commit_feed('replace', ['k1', 'k3'], '2')` | keyed source commits: patch, remove, full map |
| `commit_sites(op, site)` | `commit_sites('upsert', 'west')` | a partition set growing and shrinking |
| `commit_knob()` | | an unkeyed source's new version: `OnChange` over every scope |
| `change_outside(keys)` / `sensor_round(delay, twice)` | `sensor_round(delay=90, twice=True)` | a sensor tick posted late, or twice |
| `flaky(keys)` | `flaky(['k2'])` | `Each` keys failing with `Transient` |
| `submit(asset, mode, upstream, partitions)` | `submit('copy', mode='full', upstream=False, partitions='all')` | manual runs |
| `cancel(newest)` | | a user cancel of a live run |
| `wait(seconds)` | `wait(700)` | time passing: retries, timeouts, schedules |
| `doom_next_worker(fate)` | `doom_next_worker(Fate('die', 'gate', 'after'))` | the next launched worker dies or pauses before or after its claim, start, delta upload, gate, store write, result or `finished`; is muted (cannot reach the engine); or is started twice |
| `store_weather(error, lost, delay)` | `store_weather(error=0.1, lost=0.02, delay=15)` | the fault plan from now on |
| `restart(clean, down)` | `restart(clean=False, down=120)` | a crash or a clean restart, and an outage |
| `takeover(zombie, change)` | `takeover(zombie=60, change='bump')` | a new engine while the old one still runs (a rolling deploy), optionally on a new variant |
| `redeploy(change, clean)` | `redeploy('summary', clean=True)` | re-registration |
| `prune(keep)` | `prune(keep=0)` | deleting runs by hand |

## Invariants

Checked after every step:

| Invariant | Example of a violation |
|---|---|
| **No state breaks.** No engine's `State` fails applying an event (which would exit the process with code 70). | a reducer raising on a replayed event |
| **One end per attempt.** The journal holds at most one `AttemptFinished` per attempt, and no segment lands twice with different bytes. | a zombie engine and its successor both ending attempt `A` |
| **Nothing is read after collection.** No live attempt or serving engine finds an index file or data object gone because garbage collection deleted it. | a delta window's reader pin not holding its files |
| **Reads say what they read.** A PostgresStore read reports the generation that wrote the rows it loaded, the newest committed before its snapshot — never one that only acquired the slice — and the key versions an attempt reports are those of the rows it loaded. | an attempt that acquired and died surfacing as the read generation |
| **Committed keys are readable.** Every key an immutable store's head lists loads back at its indexed version, from an object a committed attempt wrote. | a stale writer's object referenced by the index |

Checked once the system is quiet, at the end of every run (`_converge`): faults
off, zombies killed, a fresh change to every source, then:

| Check | Example of a violation |
|---|---|
| **Quiet.** Within four virtual hours: no run in progress, no claim, no worker alive, no automation pending. | a task held forever |
| **Automations converge.** With no manual run, every automated output equals what the oracle computes from its sources — no change stuck, none silently consumed. | `copy` missing a key `items` has |
| **A catch-up run converges.** After a run of every asset with `upstream=True`, every output, `tally` included, equals the oracle's. | a watermark past rows never delivered |
| **Stores hold what was committed.** A fenced store holds exactly the rows its index lists, no stale writer's rows besides. | a dead writer's patch surviving |
| **The journal alone rebuilds the state.** A read-only `State` replayed from storage equals the live model. | an event applied differently on replay |

## Running it

```bash
uv run pytest tests/sim -q                       # the CI budget: 20 runs, about 20 s
SOLERA_TEST_DATABASE_URL=postgresql://… uv run pytest tests/sim -q   # items may also live in Postgres
uv run pytest tests/sim -q --slow                # 400 runs with shrinking: tens of minutes
SOLERA_SIM_EXAMPLES=2000 SOLERA_SIM_STEPS=60 uv run pytest tests/sim -q --slow
SOLERA_SIM_KNOWN=1 uv run pytest tests/sim -q    # do not set open findings aside
SOLERA_SIM_TRACE=1 uv run pytest tests/sim -q -s # print every run's trace
```

The CI budget is derandomized: the same runs every time. `--slow` draws new
ones on every invocation and reports throughput, e.g. `simulation: 400 runs,
15,880 steps, 61.2 h virtual in 8.5 min (112,000 steps/hour)`.

**Open findings are set aside.** A run that trips a finding still open (its
signature is recognized in `machine.py`, `_known`) is discarded rather than
failed, and counted, so the simulation keeps looking for new bugs; re-registrations
that trip one on almost every run (moving `items` between stores, renaming
`copy`) are left out (`KNOWN`). `SOLERA_SIM_KNOWN=1` puts all of it back.

## Reading a failure

Hypothesis prints the failing run as code, shortest first, followed by the
violation:

```
state = Simulation()
state.boot(seed=0, table=False)
state.submit(asset='copy', mode='full', partitions='latest', upstream=False)
state.takeover(change='rename', zombie=60.0)
state.teardown()
tests.sim.oracle.Violation: never quiet after 14400s: ... ('queued', ['invalid', "'copy'"])
```

That block is a program: saved to a file with the imports
(`from tests.sim.machine import Simulation` and
`from tests.sim.world import Fate`), it replays the run exactly. Each step adds a
line to `state.trace`; failed client calls show up there too
(`# commit failed: GenericError: injected: 503 …`). From a replay,
`state.journal` holds every segment that landed and every event an engine
applied, and `state.world.objects.log` every write, delete and failed read, with
the virtual time and the actor behind it — enough to tell what happened in
which order. A finding is then reduced to an engine-level test, without the
simulation, in `tests/server/test_sim_found.py`: a strict `xfail` until it is
fixed, an ordinary test after.

Determinism has one limit: Python randomizes string hashing per process, so a
set iterated by product code may order differently in another process. A
replay in the same process is exact; across processes it is the same run with
possibly another interleaving of equal-time events.

## Stores: the generated kit

The named scenarios of `solera.testing.stores` (`docs/stores.md`) are the
readable spec of a store; `solera.testing.storemachine` is its generated
counterpart, for store authors as much as for Solera's own stores. It plays
the engine for one output per run: attempts with growing generations
(acquiring first on a fenced store), writes that commit or are abandoned,
calls retried, stale writers and duplicate invocations, readers that pin and
read later, by-key loads as `Each` does them, discards of what nothing
references, and an incremental output's batches — appended, retried, reset,
rewritten by a stale writer. After every step the committed content, every
pinned read and every batch range must read back as the engine's key index
says. Example: `begin; write(commits=False); begin; stale_write` — the
second attempt holds the slice, so a fenced store must refuse the first.

```python
from solera.testing.storemachine import stateful

TestMyStore = stateful(lambda: Harness(MyStore(dsn), fresh_output)).TestCase
```

`tests/sdk/test_store_machine.py` runs it against FileStore, PostgresStore,
the example SQL store, S3Store and the simulation's table store (30 runs
each; ten times that with `--slow`).

## Findings

| # | Finding | Severity | Status |
|---|---|---|---|
| F1 | `Incremental(exclude="k1*")` with a string is split into characters: `*` excludes every key (`include=` wraps a string) | P3 | fixed in c521d9b — `test_one_exclude_pattern_is_a_pattern_not_its_characters` |
| F2 | A keyed output moved to another store keeps its key index: the next write stores only the changed keys there, and the others become unreadable | P1 | fixed in 59812c4 — `test_a_keyed_output_moved_to_another_store_stays_readable` |
| F3 | `Each` loads its upstream as `dict[str, T]`; the store contract, the conformance kit and `examples/json_table_store.py` do not say or do so, and `Each` over such a store fails every key | P2 | doc and example fixed in 217c8f4; by-key loads checked by the store machine (`read_by_key`) |
| F4 | A removed asset's launched attempt that asks for more pages, or fails retryably, re-queues a task no manifest can place: its run never ends | P1 | fixed in 59812c4 — `test_a_removed_assets_last_attempt_ends_its_run` |
| F5 | An attempt launched before a rename settles into a head that moved and an output the manifest no longer names: its claim is never released, its run never ends | P1 | fixed in 59812c4 — `test_an_attempt_launched_before_a_rename_settles` |
| F6 | A run that finishes an interrupted full delivery ends there though the upstream moved: the `OnChange` firing it ran for delivers nothing of its change | P1 | fixed in 1cad0bd — `test_a_change_made_during_a_full_delivery_reaches_downstream` |
| F7 | Journal cleanup deletes segments a writer still opening has not read; its fence lands in the hole and it serves a state without acknowledged events | P1 | fixed in 3c23397 — `test_a_slow_new_writer_never_fences_into_a_deleted_segment` |
| F8 | A `full` run resets an unkeyed incremental output at a new `base`; a consumer whose next batch is exactly that base gets the reset as a delta and keeps the rows the upstream let go (`next < base`, not `<=`) | P1 | fixed in 2af00fc — `test_a_batch_upstream_reset_right_after_a_delivery_is_delivered_in_full` |
| F9 | A keyed output moved to another store starts over with an index of its own; a consumer then keeps a key the move's first write dropped (seen when the consumer's first delivery and the moved write run together) | P1 | open — `tests/sim/test_replays.py::test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers` |
