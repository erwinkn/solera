# Verification: the deterministic simulation

Solera's bugs live where two features meet: a rename while an attempt is in
flight, a cancel in the middle of a pass, a new engine opening while
the old one still writes. Hand-written tests check one feature at a time; the
simulation in `tests/sim` checks them together. It runs the real engine and the
real worker against a real `file://` object store, lets
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
| Workers | `run_attempt` in process, one task per worker, through `SimPlacement` (`Local`), or started by a pool host for an attempt discovery offered it (`Pool`); handles outlive engines | a subprocess, container or job; `solera worker pool` |
| Channel | `SimChannel`: each call runs as a request of whichever engine serves now, answered once durable (the API middleware's rule) | HTTPS to a stable engine URL |
| Heartbeats | the worker's `Reporter`, beating from a task instead of a thread | a thread |
| Key cache | the engine's `KeyService` on the simulation's loop instead of its own thread | a thread with its own loop |
| Stores | FileStore (`immutable`); `TableStore` (`fenced`, `tests/sim/stores.py`): an in-memory database whose transactions take the partition's fence and can stay open or lose their answer; and, with `SOLERA_TEST_DATABASE_URL`, a real PostgresStore in a schema of its own per run, every transaction and read recorded (`tests/sim/postgres.py`) | FileStore, S3Store, PostgresStore |
| Sensor worker | a rule that polls `sensor_next` and posts each tick's outcome, late or twice | `solera_worker sensors` |
| Clients | rules calling the engine's API; a failed call is an unknown outcome | the API, the CLI |

**Time is virtual** (`tests/sim/core.py`). `SimLoop` is an asyncio event loop
whose clock jumps to the next timer whenever nothing is ready, so ten minutes of
heartbeats, retries and timeouts cost a few milliseconds, and `time.time()`
reads the same clock. Nothing runs beside the loop: object requests are
answered synchronously through obstore's blocking client, and work handed to a
thread runs to its end while the loop waits. Attempt ids, worker id and
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

**The project** (`tests/sim/project.py`) is small and covers every input kind:

```
feed (keyed source) ──Incremental──▶ items ──Incremental(batch 2)──▶ copy
items ──Each(batch 2)──▶ checks              (fails while a key is "flaky")
items ──Incremental(batch 2)──▶ split ──▶ odd (table store), even (FileStore); on a pool
items ──Incremental(batch 2)──▶ seen (a job: no output, its cursor holds what it read)
knob (version) ──dep──▶ per_site[site ∈ sites] ──AllPartitions──▶ summary
knob ──dep──▶ log (appends) ──Incremental──▶ tally
outside (keyed source) ◀── watch (a sensor over an external map; runs per_site when it changed)
```

Every producer is a pure function of its inputs, so the oracle computes what
each output must hold from the sources alone: `items` is `feed` with the
asset's version appended to each value, `copy` is `items` minus excluded keys,
`checks` is `x` + each `items` value, `split` puts each `items` key in `odd`
or `even` by its feed version (so a key whose version flips moves between
two outputs on two kinds of store in one commit), `tally` counts `log`'s
rows, and so on.
Re-registration moves between *variants*: `items` on either store, its declared
version bumped, `copy` renamed to `mirror` (with an alias), `summary` removed,
`copy`'s input excluding `k1*`.

## Rules

Each rule is one thing that happens. Hypothesis picks them, with their
arguments, up to 40 per run.

| Rule | Example | What it exercises |
|---|---|---|
| `commit_feed(op, keys, version)` | `commit_feed('replace', ['k1', 'k3'], '2')` | keyed source commits, each key at a version: patch, remove, full map |
| `commit_sites(op, site)` | `commit_sites('upsert', 'west')` | a dynamic partitions growing and shrinking |
| `commit_knob()` | | an unkeyed source's new version: `OnChange` over every partition |
| `change_outside(keys)` / `sensor_round(delay, twice)` | `sensor_round(delay=90, twice=True)` | a sensor tick posted late, or twice; its commit, and the run of `per_site` it requests when what it sees changed |
| `flaky(keys, error)` | `flaky(['k2'], 'failed')` | `Each` keys failing by error class: `Transient` (retried on its backoff), `Failed` (once per deploy), `Rejected` (when the input changes), `Abort` (the whole attempt, per `retries=`) |
| `retry_keys(classes)` | `retry_keys(['rejected'])` | a forced retry of failing keys, as `solera keys retry` asks for one |
| `submit(asset, mode, upstream, partitions, keys)` | `submit('checks', mode='incremental', upstream=False, partitions='all', keys=('k1', 'k10'))` | manual runs; `keys=` makes the target's keyed input read a full pass (`'full'`) or the keys named |
| `round_trip(between, clean)` | `round_trip('keys', clean=True)` | `items` moved to its other store and back, with nothing, a `keys=` run or a feed change in between (F13, F17) |
| `readd_live(clean, ends)` | `readd_live(clean=False, ends='succeeds')` | the job `seen` removed and added back while its attempt runs, which then succeeds or dies (F19) |
| `break_watch(broken)` | `break_watch(True)` | the sensor raising on every tick (the host posts the error), then working again |
| `pool_hosts(hosts)` | `pool_hosts(2)` | how many pool hosts poll `split`'s `Pool`: none (its attempts wait for one), one, or two racing to own each attempt (`lifecycle.md` §10) |
| `cancel(newest)` | | a user cancel of a live run |
| `wait(seconds)` | `wait(700)` | time passing: retries, timeouts, schedules |
| `doom_next_worker(fate)` | `doom_next_worker(Fate('die', 'gate', 'after'))` | the next launched worker dies or pauses before or after owning its attempt (point `claim`), start, delta upload, gate, store write, result or `finished`; is muted (cannot reach the engine); or is started twice |
| `store_weather(error, lost, delay)` | `store_weather(error=0.1, lost=0.02, delay=15)` | the fault plan from now on |
| `restart(clean, down)` | `restart(clean=False, down=120)` | a crash or a clean restart, and an outage |
| `takeover(zombie, change)` | `takeover(zombie=600, change='bump')` | a new engine while the old one still runs (a rolling deploy), optionally on a new variant; two takeovers within `zombie` seconds leave three engines running |
| `redeploy(change, clean)` | `redeploy('summary', clean=True)` | re-registration |
| `prune(keep)` | `prune(keep=0)` | deleting runs by hand |

## Invariants

Checked after every step:

| Invariant | Example of a violation |
|---|---|
| **No state breaks.** No engine's `State` fails applying an event (which would exit the process with code 70). | a reducer raising on a replayed event |
| **One end per attempt.** The journal holds at most one `AttemptFinished` per attempt, and no segment lands twice with different bytes and stays (an opener whose fence lands in a hole cleanup left deletes it and opens again, by design). | a zombie engine and its successor both ending attempt `A` |
| **Nothing is read after collection.** No live attempt or serving engine finds an index file or data object gone because garbage collection deleted it. | a delta pass's reader pin not holding its files |
| **Reads say what they read.** A PostgresStore read reports the generation that wrote the rows it loaded, the newest committed before its snapshot — never one that only acquired the partition. | an attempt that acquired and died surfacing as the read generation |
| **Committed keys are readable.** Every key an immutable store's head lists loads back at its indexed generation, from an object a committed attempt wrote. | a stale writer's object referenced by the index |
| **A fenced partition at rest holds its index.** A fenced store's partition that no attempt holds and no dead writer left owing a repair holds exactly the keys its index lists, and, in Postgres, reads as written by its head's generation (`versions.md` §5, §9). | a repair that marks a key live with no rows, or leaves a dead attempt's generation as the partition's |
| **A life is its own.** No attempt launched before its asset was added back (no alias carrying it) installs a commit after (F12's rule, F19). | `seen`'s first life's attempt committing its cursor into the `seen` a deploy added back |
| **One attempt per asset partition.** No attempt launches on an asset partition another launched attempt holds — in the journal, in order, and in the serving engine's claims. A task follows its asset through a rename (an alias); `mirror` renamed back to `copy` without one is another asset. | the hourly run and a manual run both launching `copy` before either ends |
| **A tick's runs are submitted once.** Each run a sensor tick requests is submitted at most once, however late, often, or across restarts the tick's outcome is posted. | a retried post of tick `T` submitting its `per_site` run a second time |
| **A fenced write holds its gate.** Every write a worker makes to a fenced store (the table store, Postgres) comes after its attempt's gate was created `writing` with that worker's id (`lifecycle.md` §2.4, §3). | a worker paused before its gate, whose attempt the engine closed meanwhile, writing `items` when it wakes; the twin of a `twice` worker writing beside the owner |

Checked once the system is quiet, at the end of every run (`_converge`): faults
off, zombies killed, a fresh change to every source, a forced retry of every
failed, rejected and canceled key (which by design come back only on request),
then:

| Check | Example of a violation |
|---|---|
| **Quiet.** Within four virtual hours: no run in progress, no claim, no worker alive, no automation pending. | a task held forever |
| **Automations converge.** With no manual run, every automated output equals what the oracle computes from its sources — no change stuck, none silently consumed. | `copy` missing a key `items` has |
| **A catch-up run converges.** After a run of every asset with `upstream=True`, every output, `tally` included, equals the oracle's. | a bookmark past rows never delivered |
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
ones on every worker and reports throughput, e.g. `simulation: 400 runs,
15,880 steps, 61.2 h virtual in 8.5 min (112,000 steps/hour)`.

**Open findings are set aside.** A run that trips a finding still open (its
signature is recognized in `machine.py`, `_known`) is set aside rather than
failed, and counted, so the simulation keeps looking for new bugs; a
re-registration that would trip one on almost every run is left out until
its fix (`KNOWN`). None today.
A signature is coarser than its bug and can hide another; each goes with its
fix. `SOLERA_SIM_KNOWN=1` puts all of it back.

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
calls retried, stale writers and duplicate workers, readers that pin and
read later, by-key loads as `Each` does them, cleanups of what nothing
references, and an incremental output's commits — appended, retried, reset,
rewritten by a stale writer. After every step the committed content, every
pinned read and every commit range must read back as the engine's key index
says. Example: `begin; write(commits=False); begin; stale_write` — the
second attempt holds the partition, so a fenced store must refuse the first.

```python
from solera.testing.storemachine import stateful

TestMyStore = stateful(lambda: Harness(MyStore(dsn), fresh_output)).TestCase
```

`tests/sdk/test_store_machine.py` runs it against FileStore, PostgresStore,
the example SQL store, S3Store and the simulation's table store (30 runs
each; ten times that with `--slow`).

## Property tests

Pieces with a simple model of their own are checked against it directly,
with Hypothesis drawing the inputs (in CI, a few seconds each):

- **The key index format** (`tests/sdk/test_keys_properties.py`): files
  the native extension or the Python reference wrote, the other reads back;
  lookups, range merges and compactions are newest-wins over a dict; a
  resolve writes exactly the keys a write changes, with their prior
  generations (`native/src/delta.rs`); sorted entries round-trip. Inputs
  reach the inputs: empty, 600-byte and shared-prefix keys, generations up
  to 2⁶⁴ − 1, empty payloads, one-byte blocks. A resolve request's framing
  is fuzzed: any body is either read with exact payload bounds or
  `Malformed`.
- **The engine's key cache** (`tests/sdk/test_keys_cache.py`): three
  indexes grow while one cache, its disk budget from a few files' worth to
  plenty, serves resolves; its files are deleted or corrupted under it, or
  it restarts on its directory. Every answer is the cold reader's, and it
  never holds more than its budget (1,500 runs once, 40 in CI).
- **Key patterns** (`tests/sdk/test_patterns.py`): every glob matches as
  an independent reference matcher does (`**/` takes whole directories or
  none, `*` and `?` stay within one, `[`, `\` and newlines are literal),
  and `include`/`exclude` combine as documented.

## What is not exercised yet

Known gaps, most valuable first; each says what would close it.

- **Time partitions and several dimensions.** The project has one dynamic
  dimension (`sites`): broadcast, fan-in over an upstream's extra
  dimensions, windows and `all_partitions=` are untested together. Waits
  for the input kinds of the model changes (`glossary.md`), then a
  `day × site` asset in the project.
- **Migrations** in the simulation: waits for F18's fix, since every run's
  `items` would share one ledger row.
- **A worker that resumes after its run was purged**: with the control
  file (`lifecycle.md` §2.4, decided, not built) it finds the file gone
  and stops, however long it paused; `Attempt.tla` checks it ("Formal
  model: the attempt control file"). Once built, the simulation can purge
  runs within its hours and resume stale workers after.

Closed in this round: the key index format, merges, compactions and
resolves against a dict (property tests, and F16); glob patterns; resolve
framing; claims (one attempt per asset partition); the gate under worker
death, pause and duplicates; rolling deploys with three or more engines;
`Each` errors by class and forced retries; runs with `keys=`; an asset
with two outputs on two kinds of store; a `Pool` with racing hosts; a
sensor that requests runs, and one that fails; a job; the key cache
under tight budgets and with its files deleted or corrupted under it.

## Sweeps

Long runs (`--slow`, new seeds each) and what they found. Steps count
rules, not invariant checks. `pg`: `items` may also live in Postgres.

| Run | Code | Seed | Runs × steps | Stores | Result |
|---|---|---|---|---|---|
| A | fa12f01 | 101 | 720 runs, 38,382 steps, 244 h virtual | default | F16 (`items` keeps a key the feed dropped) |
| E | `flaky` by class, `retry_keys` | 105 | 415 runs, 23,065 steps, 138 h | default | green |
| F | as E | 106 | 151 runs, 2,450 steps | pg | the gate invariant's Postgres clock (a simulation bug, fixed in 2808bbc) |
| G | `submit(keys=)` | 107 | 322 runs, 13,700 steps | default | the claims invariant equated `mirror` with `copy` (a simulation bug, fixed) |
| H | e39ead9 | 108 | 106 runs, 4,379 steps | pg | every `items` write refused: the database's fence table predated the `worker_id` rename (an environment bug: the simulation now drops a fence table of another shape at boot) |
| I | e39ead9 | 109 | 399 runs, 21,474 steps, 122 h | default | the claims invariant again; F16 |
| P1 | a6dcc0e (F16 set aside) | 201 | 157 runs, 6,312 steps | default | green |
| P2 | a6dcc0e | 202 | 158 runs, 6,177 steps | default | green |
| P3 | a6dcc0e | 203 | 106 runs, 4,060 steps | pg | green |
| Q1–Q3 | `split` on a `Pool` | 301–303 | 542 runs, 23,897 steps | default ×2, pg | a pool host never took an attempt again after its worker died before owning it (a simulation bug, fixed); F16 set aside twice |
| R1–R3 | `Pool` (fixed), `watch` requests runs | 511–513 | 503 runs, 23,744 steps | default ×2, pg | F17; F13 by a takeover; F16 set aside once |
| S1–S3 | cb3367b (F13, F17 set aside) | 521–523 | 572 runs, 26,112 steps, 121 h | default ×2, pg | green; F13 set aside 3 times |
| L1 | a780a55 (main) | 601 | 1,059 runs, 73,668 steps, 323 h virtual | default | green; F16 set aside 6 times |
| L2 | a780a55 | 602 | 616 runs × up to 120 steps, 67,036 steps, 260 h | default | green; F13 set aside 4 times |
| L3 | a780a55 | 603 | 527 runs, 29,798 steps, 161 h | pg | green; F13 set aside 10 times |
| L4 | a780a55 | 604 | 435 runs × up to 80 steps, 31,285 steps, 187 h | pg | green; F13 set aside 8 times |
| T1 | `break_watch` | 701 | 158 runs, 7,293 steps | default | green |
| C1–C3 | the key cache under pressure | 801–803 | 702 runs, 32,147 steps | default ×2, pg | green; F13 set aside 3 times |
| J1–J3 | the job `seen` | 811–813 | 668 runs, 33,179 steps | default ×2, pg | green; F13 set aside once |
| X1–X3 | 33fa183 (F10 fixed, `exclude` back) | 901–903 | 853 runs, 46,600 steps | default ×2, pg | green |
| Y1–Y3 | the reset rule's rules (off), `a_life_is_its_own` | 911–913 | 682 runs, 37,174 steps | default ×2, pg | green; F19 set aside once (rules off: a rename back without an alias) |
| Z1 | the reset rule's rules on (b7d8ae7) | 921 | 637 runs, 9,635 steps | default | F21; F20 set aside 9 times (since fixed) |
| Z2–Z3 | F16, F20 fixed; F21 set aside | 931–932 | 435 runs, 24,486 steps | default, pg | green; F21 set aside 28 times |
| Z4 | F21 fixed (b8bf1a4), nothing set aside | 941 | 203 runs, 14,284 steps, 61 h | default | green |

## Formal model: execution semantics (`spec/tla/Execution.tla`)

The simulation samples interleavings of the real code; the model checker
TLC explores **every** interleaving of the design, at small bounds. The
model is the design as decided (`glossary.md`, with its model changes),
not the code: where they differ, the code is the suspect.

```bash
spec/tla/check-execution.sh            # smoke and calibrations (CI): about a minute
spec/tla/check-execution.sh design     # store moves (with and without B), patterns and versions, removal, a zombie: ~15 min, liveness included
spec/tla/check-execution.sh big        # every deploy kind, every fault kind, each=True: too large to finish yet
spec/tla/check-execution.sh all
```

**State.**

- Three output partitions in a chain: a keyed **source** `S` (holding
  keys 1 and 2), an asset `A` reading it incrementally, an asset `B`
  reading `A` incrementally (with patterns; `B` is `each=True` in one
  configuration, `A` never). Each has a **commit log**, `[up, rm, reset]` per commit
  over a set of keys: its content is the log's fold, and a prefix's fold
  is what a bookmark at that commit number has read.
- Per incremental input, a **bookmark**: `next`, the **pass** under way (a
  `full` pass's position and the head it resumes deltas from; `delta` and
  `diff` passes are one batch each), and the **fingerprint** and
  **patterns** it reads under, and whether a **reset** took it (no full
  pass has caught it up since). The fingerprint is the asset's version:
  which store holds the output is not in it, since a move resets the
  output instead.
- Per output, two **fenced stores** (`A` can move between them) holding
  rows, a **fence** generation per store, the store the head is in, and
  the **repair** intents owed.
- **Runs** of one target (as an automation submits them, or a user with
  `keys=`), **attempts** with their **generation**, planned **batch**,
  worker progress, **gate** (`none`, `writing`, `aborted`) and **cancel**
  request. Claims are the attempts `prep`ared or launched.
- The **manifest** (`A`'s store, versions, `B`'s patterns, whether `B` is
  declared, and how many times each output was reset: a move of `A` or
  the re-adding of `B` makes it a new output under the same name) and
  the **engines**: one serving (none after a crash) and
  possibly a zombie (a rolling deploy).

**Actions.** The environment: commits to `S`; deploys (move `A`'s store,
change `B`'s patterns, bump `B`'s version, remove and re-add `B`); `keys=`
runs of `A`; worker crashes; engine crash, restart and takeover; timeouts;
user cancels. The engine: automations firing (`OnChange`), preparing an
attempt (claim, pin, plan the batch), launching, settling (commit; a
drained cancel; or lost: take the gate, owe a repair). Workers: start and
acquire the fence (reading back owed repairs), take the gate (or drain a
requested cancel), write (checking the fence), seal. A worker runs on
after the engine gave up on it. A zombie engine can only take gates
(`aborted`), and only of the attempts it created before it was fenced
(with `zombie` among the faults explored): its journal writes are fenced.
A move resets `A` at the deploy: its head and repair intents go, and so
do its own bookmarks and `B`'s on it. An attempt launched before a reset
of its output, or of the output it reads, is refused at commit and its
run carries on; what it wrote is owed a repair unless its own output was
reset. A `keys=` run of a partition a reset took bookmarks from reads a
full pass, and from then on is a run of the whole asset: it goes on until
the pass ends. Everything but the last
commit to `S` happens before it, so "after quiescence" is a state the
model reaches.

**Properties.**

| Property | Kind | Says |
|---|---|---|
| `OneAttemptPerPartition` | safety | at most one prepared or launched attempt per asset partition |
| `BookmarkHonest` | safety | with no pass under way, every key no later commit touched is in the output exactly when it was in the upstream at the bookmark, under its patterns: a bookmark never passes a change it did not deliver |
| `StoreMatchesJournal` | safety | with no attempt in flight, no repair owed and no stale writer at its gate, a store holds exactly its output's committed content |
| `RunsEndCaughtUp` | action | a run that succeeds leaves its asset caught up: no pass under way, the bookmark at the upstream's head, the output what the upstream holds under its patterns. It binds every run of a whole asset, and a `keys=` run that moved the bookmark; checked as the run succeeds, so no later change is needed to see one that ended halfway |
| `Quiesces` | liveness | eventually and forever, no run is active (every run ends) and `A` holds `S`'s keys, `B` holds `A`'s under its patterns (convergence) |

Liveness assumes weak fairness of all engine and worker steps together
(each behaviour takes finitely many: everything is bounded), of an engine
restarting after a crash, and of the final commit to `S`.

**Abstracted, and why.**

- *One partition per asset:* claims, bookmarks and passes are per asset
  partition and do not interact; fan-in reads committed heads only.
- *Dynamic partitions, not versions:* the properties are about which keys an output
  holds; a re-upsert of a present key changes nothing here.
- *One key per full-pass batch, one batch per delta or diff pass:* enough
  to interrupt a full pass between batches (F6) and to pin a delta.
- *Fenced stores only:* an immutable store's stale write is unreferenced
  by construction; the fenced kind is where gates, fences and repairs
  meet. A write lands whole or not at all.
- *Renames:* a rename moves state wholesale; its risks (F5, F12) were in
  how code looks names up, which the simulation covers. Removing and
  re-adding an asset is modelled (its state goes with its name).
- *Time:* no clocks; a timeout or a silent worker is an action the
  engine may take at any moment, bounded in number.
- *Retries exhausted:* the retry budget exceeds the fault budget, so no
  run fails by exhausting it (a design choice the model does not explore:
  a failed run leaves its change unconsumed only until the next change).
- *Finished records are forgotten:* a settled attempt whose worker ended,
  and an ended run, keep only what other records refer to, so that
  histories differing only there are one state.

**Calibration.** Each known bug is a switch restoring its pre-fix rule,
and TLC must find it (`check-execution.sh calibrate`):

| Switch | Rule as designed | Counterexample with it off |
|---|---|---|
| `FixF6` | a pass that ends behind the head goes on to it | `A` reads keys 1 and 2 in a full pass; key 1 commits; a cancel stops the run; `S` drops key 1; the firing resumes the pass, delivers key 2, and the task ends behind the head: `A` keeps 1 for good (`Quiesces`, 36 steps) |
| `FixF9` | a reset upstream commit makes its consumers read a full pass | (needed by the others; F9's own bug is the simulation's) |
| `FixF10` | a full pass reaches the consumer even when its patterns take no key; `each` reconciles at its end | `B` holds 1 and 2; `B` excludes key 2 and its version is bumped; `S` drops key 1, so `A` holds 2 alone; `B`'s full pass takes no key and is skipped: `B` keeps 1 (`BookmarkHonest`, 41 steps) |
| `ResetOnMove` (with `FixF17`: calibration `move`) | a move resets the output at the deploy (K10; b7d8ae7). With both off, a move only changes where `A`'s next write goes, and that write starts the store over without a full pass | `A`'s full pass commits key 1 into store 1; `A` moves to store 2; the pass's next batch, key 2, starts store 2 over: `A` holds 2 alone, its bookmark says 1 and 2 (`BookmarkHonest`, 17 steps). F13's mechanism as W15 described it; F13's replays turned out to take F10's route |
| `FixSelection` | a `keys=` run made a full pass (`FixF17`) reads the pass to its end | `A`'s first run is `keys=(1)`; its write starts the output over, so it reads a full pass, but ends after the first batch: `A` holds key 1 alone, a pass under way, and no run to finish it (`RunsEndCaughtUp`, 9 to 11 steps). Found by the spec's review (finding 1): the older properties needed a later source change to see it |
| `FixF17` | a `keys=` run of a partition a reset took bookmarks from reads that full pass (without `ResetOnMove`: a write that starts the output over reads a full pass) | `A` moves, which resets it; a `keys=` run for key 1 writes key 1 alone into the new, empty output and succeeds: `A` lacks key 2, though nothing removed it (`RunsEndCaughtUp`, 10 steps). Before the reset rule, moving back made it permanent (the simulation's F17) |

**Results.** With every fix on (TLC 1.7.4; the first six rows re-run
after the review's fixes and the reset rule, with 2 workers under a 5 GB,
1.5-CPU cap; the others as first run, with 8 workers; a shared machine):

| Configuration | What varies | States (distinct) | Time | Verdict |
|---|---|---|---|---|
| `smoke` | nothing: the plain pipeline | 10,852 | 9 s | passes, liveness included |
| `store` | `A` moves away and back, a `keys=` run between (`B` left out) | 45,983 | 24 s | passes |
| `reset` | `A` moved once while `B` reads it | 395,549 | 5 min 2 s | passes; without the repair a refused `B` attempt owes, `StoreMatchesJournal` fails |
| `shape` | two pattern changes or version bumps of `B`, in any mix | 398,934 | 5 min 20 s | passes |
| `remove` | `B` removed and re-added (two deploys: one pair) | 243,354 | 3 min 25 s | passes |
| `zombie` | a takeover, the zombie taking gates (`B` left out) | 1,368 | 3 s | passes; with the zombie free to take any attempt's gate, `Quiesces` fails (the review's finding 2) |
| `deploys` | one deploy, of any one kind, and a `keys=` run | over 4.5 million | capped at 25 min | no invariant violated in what was explored; liveness not reached (before the review's fixes) |
| `faults` | two faults in all, of any kinds | over 2.8 million | capped at 25 min | the same (before the review's fixes) |
| `each` | `B` is `each=True`; one deploy of any kind and one fault of any of four kinds | over 2.3 million | capped at 25 min | the same (before the review's fixes; then `A` was `each=True` too, finding 4) |
| `safety` | two deploys and two faults in all, of any kinds, and a `keys=` run: random behaviours | configured: 100,000 behaviours of up to 150 steps; run so far: 2,000 | 7 s for the 2,000 | passed what ran (`check-execution.sh safety` runs the configured target) |

The budgets are shared across kinds: `MaxDeploy` and `MaxFault` count
deploys and faults of any kind, so a configuration listing every kind
explores every *choice* of them within the budget, not every kind at
once. The bounds, exactly:

| Configuration | Keys | Source commits after the first | Deploys (budget: kinds) | Faults (budget: kinds) | `keys=` runs | `B` | `each` | Attempts, runs, tries |
|---|---|---|---|---|---|---|---|---|
| `smoke` | 2 | 1 | 0 | 0 | 0 | declared | — | 8, 6, 2 |
| `store` | 2 | 1 | 2: move | 0 | 1 | left out | — | 12, 10, 3 |
| `reset` | 2 | 1 | 1: move | 0 | 0 | declared | — | 12, 10, 3 |
| `shape` | 2 | 1 | 2: pattern, bump | 0 | 0 | declared | — | 12, 10, 3 |
| `remove` | 2 | 1 | 2: remove (a removal and a re-adding each spend one) | 0 | 0 | declared | — | 12, 10, 3 |
| `zombie` | 2 | 1 | 0 | 1: takeover (the zombie's aborts spend none) | 0 | left out | — | 12, 10, 3 |
| `deploys` | 2 | 1 | 1: move, pattern, bump, remove | 0 | 1 | declared | — | 12, 10, 3 |
| `faults` | 2 | 1 | 0 | 2: worker, crash, takeover, timeout, cancel, zombie | 0 | declared | — | 12, 10, 3 |
| `each` | 2 | 1 | 1: move, pattern, bump, remove | 1: worker, crash, timeout, cancel | 0 | declared | `B` | 12, 10, 3 |
| `safety` | 2 | 1 | 2: move, pattern, bump, remove | 2: worker, crash, takeover, timeout, cancel, zombie | 1 | declared | — | 14, 10, 4 |

No design bug found so far. Modeling errors found: a fingerprint change
during a full pass must start the pass over, as the engine does; and,
from an independent review of the spec, a promoted `keys=` run ended after one batch
(finding 1, now `FixSelection` and `RunsEndCaughtUp`) and a zombie could
abort attempts its successor created, exhausting their retries (finding
2). The review's other findings are addressed. `FixF13` (the store in
the fingerprint) was not F13's mechanism; the store is out of the
fingerprint now, as built, and `ResetOnMove` with `FixF17` off restores
the mechanism (3). `each` applies to `B` alone (4). A move resets the
output at the deploy, as built in b7d8ae7, so a move away and back is
two resets, and attempts in flight are refused (5). The tables above
give the exact, shared budgets (6). The
fairness argument with a control file, whose swap retries are loops, is
in "Formal model: the attempt control file".

**Store moves, as built** (b7d8ae7, `object-store-state.md` §2). A
deploy that moves an output, or removes it, resets it: the output under
the name is new (K10). In the model, at the move:
- `A`'s head and repair intents go;
- `A`'s own bookmarks and `B`'s bookmark on `A` go, marked `reset`.
  `A` reads `S` in a full pass, and `B` re-reads `A` from scratch. A
  bookmark marked `reset` claims nothing (`BookmarkHonest`) until a full
  pass catches it up.
- A move away and back is two resets.

Every attempt records which reset of its output, and of the output it
reads, it launched under. One launched before a later reset is refused at
commit, and its run carries on with a fresh attempt. What it wrote is
owed a repair if its own output was not reset (a `B` attempt refused
because `A` was reset), and dropped with the output if it was. TLC found
the need for that repair: without it, `B`'s refused attempt leaves rows in
`B`'s store that no commit records (`StoreMatchesJournal`, check `reset`).
The code does record it (`_fail` takes a refused result's gate intents).
The first write after a reset is a reset commit (its index starts
empty), planned as a full pass, also for a `keys=` run of a partition the
reset took bookmarks from (`FixF17`), which then runs to the pass's end
(`FixSelection`); a reset upstream commit makes every consumer read a
full pass (`FixF9`).

**F13, in plain words** (the store-move mechanism the decision removes;
the two F13 replays turned out to take F10's route, and pass since F10's
fix): `items` holds `k10` and `k11` in the
table store; a deploy moves it to FileStore. Nothing re-reads its input:
the store is not in the fingerprint, so `items`' next attempt reads only
the feed's new commits, a delta. But its write lands in a store the head
is not in, so it starts the output over: the new index holds just that
delta, and `k10` and `k11` are gone though the feed still has them. A key
the feed later removes is then removed from nothing, so `copy`, which read
the reset as an ordinary commit, keeps it. In the model (17 steps): `A`'s
full pass commits key 1 into store 1; `A` moves; the pass's next batch,
key 2, starts store 2 over: `A` holds key 2 alone, while its bookmark says
it read 1 and 2.

**Next** (open choices, what is too big):

- *Too big to exhaust* (`check-execution.sh big`): every deploy kind
  together (`deploys`, over 4.5 million states unfinished), every fault
  kind (`faults`, over 2.8 million), `each=True` with deploys and faults
  (over 2.3 million), and every deploy and fault kind together (over 22
  million). The ways down: split by kind (as `store`, `shape`,
  `remove` do), leave out `B` where it plays no part (`WithB`), or check
  safety on random behaviours (`safety`). TLC's `-simulate` with liveness
  is not sound; liveness needs the exhaustive splits.
- *Open modeling choices:* a pattern change's diff pass runs once the
  bookmark reaches the head (the design says "commits up to the change
  finish under the old patterns"; the cutover point is simplified); a
  write lands whole or not at all (no half-written batch); immutable
  stores, renames, fan-in, `Each`'s failed keys and retry passes, and
  time partitions are not modelled; runs have one target (no `upstream=`
  run graph); a deploy of `B`'s version makes it read a full pass only
  when it next runs (no `OnDeploy`).
- *What I would do next:* a run graph (`upstream=True`, a task waiting on
  another) for "every run ends" across tasks; a half-written batch with
  repair; and an immutable store beside the fenced one.

## Formal model: the journal (`spec/tla/Journal.tla`)

*Decided (K18): the journal becomes one object ("Formal model: the
journal object", below). This section describes the numbered segments,
which `journal.py` implements until that is built.*

The journal (`object-store-state.md` §3, §10; `python/solera_server/journal.py`)
is a protocol between engines that share nothing but the object store: an
old engine still appending, checkpointing and cleaning up while new ones
open. TLC checks it in every interleaving of a few engines. Most steps are
one object request; the exceptions are listed under "Abstracted". The
model follows the code; where the design doc says otherwise, that is listed
below.

**State.**

- The object store: two maps, `journal` (seq → segment) and `checkpoints`
  (seq → the state it holds), and which checkpoint, if any, cannot be
  parsed (at most one). A segment is the engine that wrote it and whether
  it is a fence.
- Per engine: where it is (opening, serving, cleaning up, closing,
  stopped); its state, as the segments folded into it; what its last LIST
  returned; the checkpoints it knows of (`_checkpoints`); its fence; what
  its cleanup has yet to delete.
- History, for the properties only: the first segment that landed at each
  seq, the appends whose create an engine saw succeed (acknowledged), and
  the fences engines serve under.

**Actions.** An engine starts at any time, also while others still run (a
rolling deploy, a zombie). Opening, it LISTs checkpoints, GETs the newest
listed one still there (or starts from nothing), LISTs the journal after
it and GETs each segment in order, then creates its fence at the next seq,
reading any segment found in the way and trying the seq after. A missing
segment, or a fence create that succeeds, where a checkpoint at or past it
exists sends it back to the start (the F7 fix), after deleting a fence it
created there; with the F14 and F15 fix, a fence it created or read first
goes through the hole test (below). A read-only open ends before the fence.
Serving, it creates
its next segment at seq + 1: success acknowledges the event; another
engine's segment there means it was fenced, and it stops. After an append
a checkpoint may be due; after a checkpoint, cleanup LISTs the journal and
DELETEs, one request each in any order, the checkpoints older than the
previous one and the segments at or below it but the fences its state
holds. A clean shutdown writes a last checkpoint and cleans up. A create
can land with its answer lost; the retry finds its own bytes. One
checkpoint may be written unparseable, which every reader skips. Any engine
crashes between any two steps; a restart is the next engine starting.

**Properties.** Invariants, except the last three.

| Property | Says |
|---|---|
| `NoAckedLoss` | Every acknowledged event is in what an engine opening now recovers: the newest readable checkpoint, then the segments after it. |
| `OneWriter` | At most one engine appends successfully at a time: every acknowledged segment lies past its engine's fence and below every newer engine's fence. |
| `FencedSeesAcked` | An engine that opened and fenced holds every event acknowledged below its fence. |
| `StatesArePrefixes` | The segments folded into every state an engine acts on (once it has fenced, or what a read-only open returns), and into every checkpoint, are a prefix of one history of landed segments. It says nothing of events an engine has applied and not yet flushed, which may differ between engines. |
| `CountersDense` | Nothing lands at seq n before n − 1 has. |
| `CleanupCovered` | A segment that landed and is gone is covered by a readable checkpoint still there, which holds it (`StatesArePrefixes`). |
| `HolesTwiceCovered` | A segment that landed and is gone is covered by two checkpoints still there. The hole test relies on it; before the F14 and F15 fix (1367919), F14 broke it. |
| `FencesStay` | A fence an engine serves under is never deleted. |
| `OpensNeverFail` | No opener gives up on the journal (before F7's fix, a gap was "journal corrupt"). |
| `Monotonic` | Once an engine serves, its state only grows, and the newest checkpoint only moves forward. |
| `OpensAlone` | Liveness, engines one at a time: an engine that starts opens, unless it crashes. |
| `AppendsAlone` | Liveness, engines one at a time: an append an engine begins succeeds, unless it crashes. |

Liveness assumes weak fairness for each engine's own steps (`Progress`),
none for starting, appending, closing, crashing or losing an answer.

What holds of cleanup and openers is this: **an opener that finds a
segment missing restarts from a checkpoint covering it** (`CleanupCovered`,
`OpensNeverFail`). Cleanup cannot know about openers (there is no
compare-and-swap and no registry of openers), so it does delete segments
an opener listed and has yet to read: `ListedStayUntilRead` fails in 20
steps.

**The object store's consistency, as relied on.** Each request is atomic
and sees every request completed before it:

- a create-only PUT (`If-None-Match: *`) is atomic: of two creates of one
  name, one lands and the other fails;
- GET, LIST and DELETE are strongly consistent with every completed PUT
  and DELETE (S3 since December 2020; any `file://` store);
- a DELETE is atomic per object; a batch delete is neither atomic nor
  ordered.

Nothing relies on a conditional overwrite (`If-Match`, compare-and-swap:
obstore's `file://` backend has none), on versioning, or on ordering
between objects beyond the above.

**Abstracted, and why.**

- *One event per segment, no buffer.* An append is one event, created as
  one segment. So the model checks segments, not events: it does not
  check how many events a segment holds, the event counter's value inside
  a batch, the buffer, the flush timer, or `durable()` and its waiters. Its
  liveness is "an append begun completes", not "a recorded event is
  eventually flushed".
- *A state is the list of segments folded into it.* A fold is a function
  of that list; what events mean is not the journal's business (650bea8
  and 1cc87d8 fixed the fold, not the log).
- *Steps of more than one request.* A LIST is one snapshot, though S3
  pages a long listing. A create and its read-back are one step (if the
  object is deleted in between, the read-back raises: divergence 2 below,
  a crash here). Loading tries the listed checkpoints newest first until
  one reads; the model takes the newest readable one still there, in one
  step.
- *Why a torn listing is safe (argued, not checked).* Each page is a
  snapshot of its key range at its own time, and keys only grow from page
  to page. A segment created after its page was read is missed, as by a
  stale listing, which the model has. A segment deleted after its page was
  read is found missing by its GET, which the model has. A later page
  cannot show a gap that did not exist at its own time, and nothing past
  the newest checkpoint is ever deleted, so a torn listing yields no
  false gap.
- *Checkpoint creates never lose their answer:* a retry finds its own
  bytes, so a lost answer is a delay, and a crash after landing is a crash
  after success.

**Bounds and cost** (TLC 2.19 from tla2tools 1.7.4, 8 workers on a shared
8-core VM):

| Model | Engines | Segments | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `Journal-small.cfg`: as built (with 1367919's fix), one checkpoint possibly unreadable | 2 | 6 | 665,146 | 55 | 21 s |
| `fixed` (`Journal-big.cfg`, four segments): the same | 3 | 4 | 2,044,229 | 55 | 53 s |
| `Journal-big.cfg`: the same, five segments (5 GB heap) | 3 | 5 | 23,314,158 | 62 | 23 min 39 s |
| `Journal-live.cfg`: as built, one engine at a time | 3 | 5 | 284,784 | 44 | 53 s |

Two engines (an old one and its successor, as in the simulation's
takeovers) are enough for F7 and every earlier journal bug. F14 needs
three, or two once a checkpoint can be unreadable; F15 needs three.

**Calibration.** Each journal fix in the history is a switch. Turning one
off, in the bigger model with the F14 and F15 fix on, restores the pre-fix
rule, and TLC must find the bug again. Traces as TLC prints them, shortest
first, in plain words (A is the old engine):

| Switch off | Fix | TLC finds | Trace |
|---|---|---|---|
| `FixF7` | 3c23397 | `OneWriter`, 24 steps | B replays segment 1. A appends 2 and 3, checkpointing at both, and cleanup deletes 2. B's fence create at 2 lands in the hole: B serves under it without A's acknowledged 2 and 3, and A's next append, at 4, succeeds too. |
| `FixF7` | 3c23397 | `OpensNeverFail`, 23 steps | The same, but B listed 1 to 3 before the cleanup: its GET of 2 finds nothing, and opening fails. |
| `KeepFences` | 0b3e226 | `NoAckedLoss`, 26 steps | B fences at 2. B appends 3 and 4, checkpointing at both; cleanup deletes 2, B's fence. A appends at 2: acknowledged, but checkpoint 4 holds B's fence there. |
| `FenceNonce` | f300500 | `OneWriter`, 15 steps | A and B both fence at 1 with the same bytes; each takes the other's for its own, and both serve. |
| `OwnBytes` | none: `_put_segment` always compared bytes | `AppendsAlone`, 15 to 21 steps (a liveness trace varies between runs) | A lone engine's create lands with its answer lost; the retry takes the segment for another engine's, and the engine stops. |
| `FixF14` | F14's half of the hole test | `NoAckedLoss`, 30 steps (checkpoints readable) | F14 (below). |
| `FixF15` | F15's half of the hole test | `StatesArePrefixes`, 29 steps; `FencedSeesAcked`, 31 | F15 (below): a read-only open, then a serving engine. |
| `FixF14`, `FixF15` | before 1367919 | `NoAckedLoss`, 27 steps (checkpoints readable) | F14. |
| `FixF14`, `FixF15` | before 1367919, two engines | `CleanupCovered`, 17 steps | B fences at 1; C reads it, fences at 2 and checkpoints at 2, unreadable; B deletes its fence as a hole's. Segment 1 is now covered by no readable checkpoint: every later opener finds it missing, loads nothing, and opens again forever. |

With every switch on, the design as built since 1367919, every model
passes. `spec/tla/check-journal.sh calibrate` runs all of the above and fails
unless each named property is the one violated.

**F14 and F15** (Findings, below). Both come from one wrong inference in
the F7 fix: "a checkpoint at or past my fence exists, so my fence landed in
a hole cleanup left". F14: the checkpoint can be a newer engine's that read
the fence and moved past it.

1. A serves; its fence is segment 1.
2. B opens: reads 1, creates its fence at 2. A is fenced.
3. C opens: reads 1 and 2 (B's fence), fences at 3, and checkpoints at 3
   (a clean shutdown, or enough appends).
4. B, still checking for a hole, finds checkpoint 3, deletes segment 2 and
   opens again.
5. A appends: its create at 2 succeeds and the event is acknowledged. No
   replay sees it: every opener starts from checkpoint 3, which holds B's
   fence at 2.

F15: a fence created in a hole is readable until its engine deletes it
(forever, if that engine crashes first).

1. A serves (fence 1). B and C start opening; there is no checkpoint yet.
   B lists the journal: 1.
2. A appends 2, 3 and 4, checkpointing at 2 and 4; cleanup deletes 2.
3. B creates its fence at 2: the create succeeds in the hole.
4. C lists the journal (1, 2, 3, 4) and replays it, B's fence in place of
   A's event 2.
5. C fences at 5, finds no checkpoint at or past it, and serves without
   A's acknowledged event 2; its next checkpoint makes that permanent. A
   read-only open can return the same state.

**The checked rule for F14 and F15** is the hole test, written as the
rule to build in `object-store-state.md` §10, "Fences in holes".
`FixF14` (the writer that created a fence runs it) and `FixF15` (every
opener that reads a fence runs it) model it request by request: the LIST
(`HoleList`, or `FenceCheck` for one's own fence), then, with two or more
checkpoints at or past the fence, a GET of any of them (`HoleGet`, a
nondeterministic choice, so every choice policy is checked). Cleanup may
delete that checkpoint in between (then: LIST again), and one checkpoint
may be unparseable (then: GET another). With the rule, every property
above holds, liveness included, and switching off any older fix still
fails.

A first version of the rule decided "unknown, open again" whenever the
checkpoints at or past the fence were all unreadable. TLC found that a
lone engine then reopens forever: B fences at 1 and closes with an
unreadable checkpoint at 1; the next engine reads fence 1, finds only that
checkpoint, and loops. Step 1's "fewer than two: real" removes the case:
a hole always has two covering checkpoints (`HolesTwiceCovered`).

Not checked: two unreadable checkpoints, and an opener that keeps
reopening while an old engine keeps checkpointing (bounded seqs end it).

**Where the docs and the code differ.**

1. Fixed in 1367919: §10 reasoned that "a fence create that succeeds
   where a checkpoint at or past it exists" landed in a hole (F14), and
   `_fence` let the `NotFoundError` of `create`'s read-back end the open
   instead of opening again.
2. `journal.py` as of 1367919 (`_hole`, `_apply_read`, `_fence`) follows
   the modeled hole test request by request, with three differences, none
   unsafe. The engine whose fence create succeeded LISTs twice (`_behind`,
   then `_hole`), where the model uses the first listing: `_hole` is a
   fresh test, which the model has as its "LIST again" path. If that
   second listing shows fewer than two covering checkpoints, the code
   keeps the fence and opens again, where the model would serve: opening
   again is always safe. And `_hole` returns "undecidable" when every
   covering checkpoint listed is unreadable; the model has that outcome
   but never reaches it, since it allows one unreadable checkpoint and
   the test only reads with two or more covering. With two unreadable
   covering checkpoints, beyond what the design survives, a lone engine
   reopens forever, the loop the first version of the rule had. Usually
   the journal is lost then anyway: the two newest checkpoints are
   unreadable, and cleanup removed what lies below the previous one. Only
   if cleanup did not run after them (a crash) does an older checkpoint
   and the journal after it remain, and the loop blocks a recovery the
   code before 1367919 would have made. Applying an undecidable fence
   instead would bring F15 back, so the code is right to refuse it; the
   way out is an operator, or a newer readable checkpoint.
3. `glossary.md` lists `seq` among the old names of the event counter.
   In `object-store-state.md` and `journal.py`, `seq` numbers segments, and
   a segment holds many events, so the two are different counters. The
   glossary has no name for a segment's number.

**Running it.** Java 11 or later; the script downloads `tla2tools.jar`
1.7.4 into `spec/tla/.tools/` (gitignored).

```bash
spec/tla/check-journal.sh              # the small model, two engines: ~20 s
spec/tla/check-journal.sh fixed        # three engines, four segments: ~1 min
spec/tla/check-journal.sh big          # the same, five segments: ~25 min
spec/tla/check-journal.sh live         # liveness: ~1 min
spec/tla/check-journal.sh calibrate    # every fix switched off in turn: ~2 min
spec/tla/check-journal.sh ci           # small, fixed, live and calibrate
```

CI's `journal-spec` job runs `ci`: about ten minutes on a GitHub runner.

## Formal model: the journal object (`spec/tla/JournalObject.tla`)

Decided (K18), not built yet: the journal becomes one object,
`control/journal.json`, swapped with `If-Match` (`object-store-state.md`
§0 and §10; why, and what it costs: `journal-object.md`). The object holds
the engine id of its writer, the name of the current checkpoint and the
events since that checkpoint. Until it is built, `Journal.tla` (above)
describes `journal.py`.

**What differs from `Journal.tla`.**

- The store is the journal (a body, or nothing yet), the checkpoints by
  unique name, and at most one unparseable checkpoint. An ETag is a
  function of the body, as on S3 and R2 (an MD5), so `If-Match` compares
  bodies. A store whose ETag is a version counter (GCS, `MemoryStore`, or
  `file://`'s SHA-256 under its lock) gives at least that.
- Journal writes are numbered (`writes`), for the properties only. A
  fence or an acknowledgment records the number of the write that carried
  it. `OneWriter` and `FencedSeesAcked` order by these numbers where
  `Journal.tla` uses a segment's `seq`. `MaxWrites` bounds the model, as
  `MaxSeq` does there.
- Faults: crashes between any two requests, overlapping and zombie
  engines, lost answers, 409s (a conditional write refused without
  landing), one unparseable checkpoint, and cleanup at any time. A
  checkpoint is due at any point after an append: the cadence does not
  matter to safety.
- `JournalResolves` replaces `CleanupCovered`: the journal names a
  checkpoint that is there and readable. `CountersDense`,
  `HolesTwiceCovered` and `FencesStay` have no counterpart, since there
  are no slots and no fences to keep.
- One step is one request, as in `Journal.tla`. Abstracted the same way:
  one event per append, a state as the list of events folded into it, a
  LIST as one snapshot, and checkpoint creates that never lose their
  answer.

**Calibration.** Each rule of the design is a switch. With it off, TLC
must find the bug it prevents (`check-journal.sh object`):

| Rule off | TLC finds | Trace |
|---|---|---|
| `EngineId`: the journal names its writer | `OneWriter`, 9 steps | A fences, then begins an append. B reads the journal and fences, but without an id its body is A's byte for byte, so the ETag does not change. A's append still matches, and it is acknowledged after B's fence. |
| `AskJournal`: a refused write reads the journal | `AppendsAlone`, 13 to 15 steps (a liveness trace varies between runs) | A lone engine's append lands, but its answer is lost. The retry is refused, because its own write changed the ETag. The engine takes that for a newer engine's write and stops. |
| `ReGet`: a gone checkpoint sends the opener back to the journal | `OpensNeverFail`, 25 steps | B reads the journal, which names `cp-A-1`. A moves to `cp-A-2`, and its cleanup deletes `cp-A-1`. B's GET of `cp-A-1` finds nothing, and B gives up. |
| `Verify`: a checkpoint is read back before the move | `NoAckedLoss`, 11 steps | A appends, writes a checkpoint nobody can parse, and moves the journal to it. An opener can no longer load A's acknowledged event. |
| `ListFirst`: cleanup deletes only what it listed before its move | `NoAckedLoss`, 24 steps | A moves to `cp-A-1`. B opens, fences, appends and writes `cp-B-1`. A, now a zombie, LISTs for its cleanup and deletes every checkpoint except `cp-A-1`, so `cp-B-1` goes too. B's move then lands, and the journal names a deleted checkpoint. |

**Bounds and cost** (TLC 2.19; `-workers 3`, `-Xmx6g`, on a shared
8-core VM):

| Model | Engines | Journal writes | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `JournalObject-small.cfg` | 2 | 7 | 766,771 | 53 | 26 s |
| `fixed` (`JournalObject-big.cfg`, five writes) | 3 | 5 | 4,663,723 | 46 | 2 min 52 s |
| `JournalObject-big.cfg` | 3 | 6 | 23,642,632 | 53 | 8 min 52 s |
| `JournalObject-live.cfg`: one engine at a time, no 409s | 3 | 7 | 1,075,329 | 47 | 1 min 57 s |

Liveness leaves out 409s. A store that refuses every conditional write
forever is an outage, not a fault the journal can outlast.

**Not modeled.** The `file://` lock, which gives one machine the same
`If-Match`. Flushes that run while a checkpoint is written
(`journal-object.md`, "A known cost").

```bash
spec/tla/check-journal.sh object       # two engines, three, liveness, calibration: ~6 min
spec/tla/check-journal.sh object-big   # three engines, six writes: ~9 min
```

## Formal model: the attempt control file (`spec/tla/Attempt.tla`)

Decided (K18), not built yet: one control file per attempt, swapped with
`If-Match`, replaces ownership (`.worker`), the gate (`.writing`) and the
result (`.result`) (`lifecycle.md` §2.4). The model is the protocol at the
level of requests. The durable state is apart from each actor's view of
it: the object store (the control files, the fenced store's generation
and rows) and the journal (`AttemptLaunched`, `AttemptFinished`) on one
side, and what each engine and worker last read on the other.

**State.** One output partition on a fenced store. Its attempts run one
after another (the claim), and each attempt's number is its generation.
Each attempt has:
- one or two worker processes, a duplicate being the second;
- its control file: missing, or a body naming its state, its writer and
  its write evidence;
- whether retention purged it.

The engine keeps what it last read of each file. Each worker keeps where
it is, the body it last read or wrote (its `If-Match`), and what it knows
of its own write. For the properties only, the model also records which
attempts' store writes landed, and in what order.

**Actions.**
- *The engine:*
  - launches the next attempt once the last one has ended, creating its
    file `open` first;
  - asks a worker to cancel (it drains);
  - reads a file, then ends the attempt on what it read: a swap to
    `ended`, recording `none` from `open` or `owned` and `writing` from
    `writing`;
  - settles from a final file (`sealed` or `ended`) with a durable
    `AttemptFinished`;
  - restarts, forgetting what it read.
- *A zombie engine* ends any live file.
- *Retention* deletes a settled attempt's files at any time.
- *A worker* boots (stopping if its spec is gone), reads the file, swaps
  it to `owned`, acquires its generation at the store, swaps it to
  `writing`, writes (the transaction checks the generation), and swaps it
  to `sealed`. A requested cancel lets it drain before `writing`. Its
  swaps can land with the answer lost; its retry, refused, reads back its
  own body. A refused swap that finds `ended`, another worker's body, or
  no file stops it. Workers pause anywhere and crash anywhere.

**Properties.**

| Property | Kind | Says |
|---|---|---|
| `NoWriteAfterNone` | safety | an attempt whose `AttemptFinished` says it wrote nothing has no store write that landed, before or after the decision: no write after an abort is decided |
| `CompleteLanded` | safety | a result that calls its writes complete did land them |
| `WritesInOrder` | safety | store writes land in generation order: one attempt writes the partition at a time, and an older attempt never writes over a newer one |
| `OneOutcome` | action | a final file (`sealed` or `ended`) never changes, only goes with its run; a decision is made once. So the worker's seal and the engine's end, racing on one version, cannot both land |
| `EveryAttemptEnds` | liveness | every attempt launched gets its `AttemptFinished` |

**Fairness.** The review of `Execution.tla` asked for this argument to be
redone, since swap retries are loops. Liveness assumes weak fairness of
each of the engine's steps (read, end, settle) for each attempt, and
nothing of workers: a worker may stop anywhere, forever, and the engine
must still end its attempt. The engine's retry loop is a read, then an
end that is refused, then another read. Each refusal needs the file to
have changed in between, and only these can change it:
- a worker, whose steps are bounded (each lands one state further on);
- the zombie, which ends a live file at most once;
- retention, which runs only after the decision.

Engine restarts are bounded too (`MaxRestarts`). So every behaviour
changes each file finitely often, an end that is weakly fair eventually
lands or finds a final file, and settling follows. TLC checks this
(`live`). An unbounded restart loop is the one way to starve it, and it is
a real one: an engine that keeps crashing ends nothing.

**Abstracted, and why.**
- One partition: attempts of different partitions share nothing here.
- A fenced store only. An immutable store takes no gate, and its stale
  writes are unreferenced by construction.
- The HTTP channel, `.beat` and clocks: they are evidence, deciding *when*
  the engine ends an attempt, which here is any time.
- The engine's end is atomic: a read, then a swap on what was read. A
  lost answer to it is a crash between the swap and the settle, after
  which a later read finds the file final. The zombie's end is one step:
  a refused swap changes nothing.
- A crash between the engine's create and `AttemptLaunched` leaves an
  `open` file that no worker is launched for: one step here.
- The cancel record's content (reasons, precedence), which shapes the
  result but not who may write.

**Calibration.** Each rule is a switch, and with it off TLC must find the
bug (`check-attempt.sh`; with several TLC workers, a trace's length varies
by a step between runs):

| Rule off | TLC finds | Trace |
|---|---|---|
| `PreCreate`: the engine creates the file before the launch, and nobody else creates it | `NoWriteAfterNone`, 11 to 12 steps | The create-if-absent gate with nothing retained, the case the old design kept gates `gate_days` for. The engine ends A before its worker reports, creating the file `ended` (`none`), and settles. A's worker boots and reads its spec, and retention deletes A's files. The worker finds no file, creates it `owned`, acquires (no later attempt has), marks `writing` and writes. |
| `TakeWriting`: the worker marks `writing` before its first write | `NoWriteAfterNone`, 10 to 11 steps | The worker owns A and acquires; the engine ends A from `owned` (`none`); the worker writes anyway |
| `EngineSwaps`: the engine ends with `If-Match`, not a blind PUT | `NoWriteAfterNone`, 11 to 12 steps | The engine reads `open`. The worker owns A, acquires and marks `writing`. The engine's blind PUT replaces `writing` with `ended` (`none`, from the `open` it read), and the worker's write lands. With `OneOutcome` also checked, TLC finds that first (5 steps): a blind end overwrites the zombie's. |
| `Classify`: ended from `writing`, the evidence is `writing` | `NoWriteAfterNone`, 10 to 11 steps | The worker marks `writing` and writes; the engine ends from `writing` but records `none` |

**Bounds and cost** (TLC 2.19; 3 workers, 6 GB, on a shared 8-core VM;
one engine restart, the zombie, lost answers):

| Model | Attempts | Workers each | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `Attempt-small.cfg` | 2 | 1 | 1,948,128 | 30 | 42 s |
| `Attempt-dup.cfg`: a duplicate worker | 1 | 2 | 41,018 | 19 | 2 s |
| `Attempt-live.cfg`: liveness, the engine fair | 2 | 1 | 1,948,128 | 30 | 3 min 59 s |

Two attempts with a duplicate worker each (`check-attempt.sh big`) is
too large to finish here. An earlier version, without draining, passed
68 million states before it was stopped. The duplicate (`dup`) and the
sequence of attempts (`small`) are each covered whole.

```bash
spec/tla/check-attempt.sh        # small, dup, live, calibration: ~5 min
spec/tla/check-attempt.sh big    # two attempts with a duplicate worker each: too large to finish
```

**Into `Execution.tla`.** That model has attempts end atomically
(`EndLost`) and its gate as one field. Once the control file is built,
its attempts can carry the file's states and the end as a read and a
swap, with this model as the reference for the protocol. Its fairness
argument then needs the bound above: finitely many file changes per
attempt.

## Findings

| # | Finding | Severity | Status |
|---|---|---|---|
| F1 | `Incremental(exclude="k1*")` with a string is split into characters: `*` excludes every key (`include=` wraps a string) | P3 | fixed in c521d9b — `test_one_exclude_pattern_is_a_pattern_not_its_characters` |
| F2 | A keyed output moved to another store keeps its key index: the next write stores only the changed keys there, and the others become unreadable | P1 | fixed in 59812c4 — `test_a_keyed_output_moved_to_another_store_stays_readable` |
| F3 | `Each` loads its upstream as `dict[str, T]`; the store contract, the conformance kit and `examples/json_table_store.py` do not say or do so, and `Each` over such a store fails every key | P2 | doc and example fixed in 217c8f4; by-key loads checked by the store machine (`read_by_key`) |
| F4 | A removed asset's launched attempt that asks for more batches, or fails retryably, re-queues a task no manifest can place: its run never ends | P1 | fixed in 59812c4 — `test_a_removed_assets_last_attempt_ends_its_run` |
| F5 | An attempt launched before a rename settles into a head that moved and an output the manifest no longer names: its claim is never released, its run never ends | P1 | fixed in 59812c4 — `test_an_attempt_launched_before_a_rename_settles` |
| F6 | A run that finishes an interrupted full pass ends there though the upstream moved: the `OnChange` firing it ran for delivers nothing of its change | P1 | fixed in 1cad0bd — `test_a_change_made_during_a_full_pass_reaches_downstream` |
| F7 | Journal cleanup deletes segments a writer still opening has not read; its fence lands in the hole and it serves a state without acknowledged events | P1 | fixed in 3c23397 — `test_a_slow_new_writer_never_fences_into_a_deleted_segment` |
| F8 | A `full` run resets an unkeyed incremental output at a new `base`; a consumer whose next commit is exactly that base gets the reset as a delta and keeps the rows the upstream let go (`next < base`, not `<=`) | P1 | fixed in 2af00fc — `test_an_unkeyed_upstream_reset_right_after_a_pass_is_delivered_in_full` |
| F9 | A keyed output moved to another store starts over with an index of its own; a consumer then keeps a key the move's first write dropped (seen when the consumer's first pass and the moved write run together) | P1 | fixed: a move sets the head's `base`, and a pass begun at or before it starts over — `test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers`, `test_a_key_a_moved_output_dropped_leaves_its_consumer` |
| F10 | A full pass (a reset) whose patterns take none of the upstream's keys is skipped without calling the producer, so the consumer never starts over: keys it held stay, though the upstream dropped them — also after a `full` run | P1 | fixed: a full pass always reaches the consumer (`architecture.md` §5) — `test_a_full_pass_that_takes_no_key_still_starts_over`, and the simulation's `exclude` change is back in its rules |
| F11 | A delta file a pending cleanup entry reads is deleted while an attempt that was handed the entry runs: the attempt cannot read it, the superseded objects it names leak, and the entry ends `stuck` | P2 | fixed in ffe6921 — `test_f11_a_discard_entrys_delta_outlives_the_attempt_reading_it`, `test_a_discard_entrys_delta_outlives_the_attempt_holding_it` |
| F12 | `copy` renamed to `mirror` and back over rolling deploys (engines overlapping): `mirror` ends holding a key `items` deleted while `mirror` was not served — its old index survives under a bookmark already past the deletion | P2 | fixed: a name the manifest no longer declares holds no live state — `test_f12_a_rename_back_and_forth_over_rolling_deploys_keeps_up`, `test_a_name_removed_and_added_back_starts_over` |
| F13 | `items` moved from the table store to FileStore by a crash redeploy; the feed then removes `k11`: `copy` keeps it (F9's territory, the deletion after the move). Also with no crash: `k0`, `k11` committed; a takeover moves `items` from FileStore to the table store; the feed removes both; `copy` and `split`'s `odd` keep them, `checks` drops them | P1 | fixed by the reset rule (object-store-state.md §2): a move resets the output, its consumers re-read it from scratch — `tests/sim/test_replays.py::test_f13_a_key_removed_after_a_store_move_leaves_its_consumers`, `test_f13_a_key_removed_after_a_takeover_moved_its_upstream_leaves_its_consumers`, `tests/server/test_sim_found.py::test_a_key_a_moved_output_dropped_leaves_its_consumer` |
| F14 | An engine that created its fence finds a checkpoint at or past it and deletes the fence as a hole's, but a newer engine had read it and checkpointed past it: the old engine's next append lands in the freed slot, is acknowledged, and no replay sees it (journal spec; trace and fix under "Formal model: the journal"; three engines, or two with an unreadable checkpoint) | P1 | fixed: the hole test (`object-store-state.md` §10), run by the engine that created the fence — `tests/server/test_journal.py::test_a_fence_a_newer_engine_moved_past_stays` |
| F15 | A fence created in a hole stays readable until its engine deletes it: another opener replays it in place of the event cleanup deleted, and serves without that acknowledged event (journal spec; trace and fix under "Formal model: the journal"; three engines) | P1 | fixed: the hole test (`object-store-state.md` §10), run by every opener that reads a fence — `tests/server/test_journal.py::test_an_opener_never_replays_a_fence_created_in_a_hole` |
| F16 | A key index compaction moves level-0 files into an empty level 1 without merging them, so level 1 holds overlapping files and a read takes an older entry: `k0` written (level 1); rewritten (level 0); the output replaced by nothing (a level-0 tombstone); a background compaction that empties the index lands only after `k0`, `k1` are written again (level 0, level 1 now empty); `k1` removed (level 0); the next compaction "moves the deepest level down whole" — level 1 holds `{k0, k1}` and `{k1 removed}` — and `k1` reads live. In the simulation `items` kept a key the feed dropped, kept `k3` at an old value, or named an object already collected (`KeyIndex.compact`: `out_level > depth` also holds for level 0 at depth 0) | P1 | fixed: level 0 is always merged, never moved down whole (its files overlap) — `tests/sdk/test_keys_index.py::test_any_workload_of_a_few_keys_matches_a_dict`, `tests/sim/test_replays.py::test_f16_*` |
| F17 | An output moved to another store and back loses keys when nothing moved its bookmark in between: `items` commits `k10` on FileStore; a takeover moves it to the table store, where only a `keys=('k1', 'k10')` run writes (a fresh index there; a selection moves no bookmark, which keeps FileStore's fingerprint); a takeover moves it back; the feed adds `k3`: the fingerprint matches, so `items` reads a delta, and the move starts its index over with `k3` alone — `k10` is gone. A move that starts the index over has to make the asset read a full pass | P1 | fixed by the reset rule (object-store-state.md §2): each move resets the output and takes its asset's bookmarks, so it reads full passes — `tests/sim/test_replays.py::test_f17_an_output_moved_away_and_back_keeps_its_keys`, `tests/server/test_sim_found.py::test_a_move_and_back_with_no_write_between_resets` |
| F18 | PostgresStore's migration ledger (`public.solera_migrations`) is keyed by output name, not by the table a migration changes: two projects (or a staging and a production namespace) on one database each write `orders` in a schema of their own; migration `note` adds a column to the first's table; the second's `migrate` finds the ledger row, skips it and reports it applied — its table never gets the column | P2 | fixed: the ledger (`solera_migration_ledger`) and its lock are keyed by the schema-qualified table — `tests/sdk/test_postgres.py::test_a_migration_applies_to_each_schemas_table_of_one_name` |
| F19 | An asset removed while its attempt runs and added back before that attempt ends resumes its first life (F12's rule, across a live attempt): `copy` (version 1) has an attempt running; a deploy removes `copy`, which defers retiring its head and bookmarks until the attempt settles; a deploy adds `copy` back (version 2) — its first life's head and bookmarks are still there; the old attempt then succeeds and its commit installs into the new `copy`: rows written by version 1's code become version 2's head, under version 1's bookmark (found by the execution spec's review; a failed or lost attempt installs nothing, and its run carries on with a fresh attempt of the new code) | P2 | fixed by the reset rule (object-store-state.md §2): a removal resets at its deploy, and an attempt launched before commits nothing — `tests/server/test_sim_found.py::test_a_name_removed_while_its_attempt_runs_and_added_back_starts_over`, `test_an_attempt_of_a_removed_and_readded_asset_stays_in_its_life` |
| F20 | The retry clock resubmits a retry run that cannot plan, at every tick: `checks` (`Each` over `items`, automated) has an operator's forced retry outstanding; `items` has no head (a store move reset it); the clock submits a retry run, which fails planning ("input 'items' has no head"), so the request is never done; the run's events wake the loop, which ticks again at once and submits it again — a hot loop, the journal and run history growing as fast as they can be written (in the simulation, at one virtual instant, until memory ran out) | P1 | fixed in b6d9968 (the retry clock waits for inputs with no head) — `tests/server/test_sim_found.py::test_the_retry_clock_waits_for_an_input_with_no_head` |
| F21 | A job added back takes its first life's commit (F19's case, for an asset with no output: the reset rule resets outputs, and a job has none): the job `seen` has an attempt running; a deploy removes `seen`, another adds it back; the attempt succeeds and its cursor and bookmarks install into the new `seen` | P2 | fixed: removing an asset resets it, and an attempt launched before commits nothing of it (`reset_at` by `@asset`) — `tests/server/test_sim_found.py::test_a_job_added_back_does_not_take_its_first_lifes_commit`, `test_a_job_removed_while_its_attempt_runs_and_added_back_starts_over` |
