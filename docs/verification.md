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
items ──Incremental(page 2)──▶ split ──▶ odd (table store), even (FileStore)
knob (version) ──dep──▶ per_site[site ∈ sites] ──AllPartitions──▶ summary
knob ──dep──▶ log (batches) ──Incremental──▶ tally
outside (keyed source) ◀── watch (a sensor over an external map)
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
`copy`'s edge excluding `k1*`.

## Rules

Each rule is one thing that happens. Hypothesis picks them, with their
arguments, up to 40 per run.

| Rule | Example | What it exercises |
|---|---|---|
| `commit_feed(op, keys, version)` | `commit_feed('replace', ['k1', 'k3'], '2')` | keyed source commits, each key at a version: patch, remove, full map |
| `commit_sites(op, site)` | `commit_sites('upsert', 'west')` | a partition set growing and shrinking |
| `commit_knob()` | | an unkeyed source's new version: `OnChange` over every scope |
| `change_outside(keys)` / `sensor_round(delay, twice)` | `sensor_round(delay=90, twice=True)` | a sensor tick posted late, or twice |
| `flaky(keys, error)` | `flaky(['k2'], 'failed')` | `Each` keys failing by error class: `Transient` (retried on its backoff), `Failed` (once per deploy), `Rejected` (when the input changes), `Abort` (the whole attempt, per `retries=`) |
| `retry_keys(classes)` | `retry_keys(['rejected'])` | a forced retry of failing keys, as `solera keys retry` asks for one |
| `submit(asset, mode, upstream, partitions, keys)` | `submit('checks', mode='incremental', upstream=False, partitions='all', keys=('k1', 'k10'))` | manual runs; `keys=` makes the target's keyed input read a full pass (`'full'`) or the keys named |
| `cancel(newest)` | | a user cancel of a live run |
| `wait(seconds)` | `wait(700)` | time passing: retries, timeouts, schedules |
| `doom_next_worker(fate)` | `doom_next_worker(Fate('die', 'gate', 'after'))` | the next launched worker dies or pauses before or after its claim, start, delta upload, gate, store write, result or `finished`; is muted (cannot reach the engine); or is started twice |
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
| **Nothing is read after collection.** No live attempt or serving engine finds an index file or data object gone because garbage collection deleted it. | a delta window's reader pin not holding its files |
| **Reads say what they read.** A PostgresStore read reports the generation that wrote the rows it loaded, the newest committed before its snapshot — never one that only acquired the slice. | an attempt that acquired and died surfacing as the read generation |
| **Committed keys are readable.** Every key an immutable store's head lists loads back at its indexed generation, from an object a committed attempt wrote. | a stale writer's object referenced by the index |
| **A fenced scope at rest holds its index.** A fenced store's scope that no attempt holds and no dead writer left unsettled holds exactly the keys its index lists, and, in Postgres, reads as written by its head's generation (`versions.md` §5, §9). | a repair that marks a key live with no rows, or leaves a dead attempt's generation as the slice's |
| **One attempt per asset partition.** No attempt launches on an asset partition another launched attempt holds — in the journal, in order, and in the serving engine's claims. A task follows its asset through a rename (an alias); `mirror` renamed back to `copy` without one is another asset. | the hourly run and a manual run both launching `copy` before either ends |
| **A fenced write holds its gate.** Every write a worker makes to a fenced store (the table store, Postgres) comes after its attempt's gate was created `writing` with that worker's id (`lifecycle.md` §2.4, §3). | a worker paused before its gate, whose attempt the engine closed meanwhile, writing `items` when it wakes; the twin of a `twice` worker writing beside the owner |

Checked once the system is quiet, at the end of every run (`_converge`): faults
off, zombies killed, a fresh change to every source, a forced retry of every
failed, rejected and canceled key (which by design come back only on request),
then:

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

## Property tests

Pieces with a simple model of their own are checked against it directly,
with Hypothesis drawing the inputs (in CI, a few seconds each):

- **The key index format** (`tests/sdk/test_keys_properties.py`): files
  the native extension or the Python reference wrote, the other reads back;
  lookups, range merges and compactions are newest-wins over a dict; a
  resolve writes exactly the keys a write changes, with their prior
  generations (`native/src/delta.rs`); sorted entries round-trip. Inputs
  reach the edges: empty, 600-byte and shared-prefix keys, generations up
  to 2⁶⁴ − 1, empty payloads, one-byte blocks. A resolve request's framing
  is fuzzed: any body is either read with exact payload bounds or
  `Malformed`.
- **Key patterns** (`tests/sdk/test_patterns.py`): every glob matches as
  an independent reference matcher does (`**/` takes whole directories or
  none, `*` and `?` stay within one, `[`, `\` and newlines are literal),
  and `include`/`exclude` combine as documented.

## Formal model: execution semantics (`spec/tla/Execution.tla`)

The simulation samples interleavings of the real code; the model checker
TLC explores **every** interleaving of the design, at small bounds. The
model is the design as decided (`glossary.md`, including its model
changes), not the code: where they differ, the code is the suspect.

**State.**

- Three output partitions in a chain: a keyed **source** `S`, an asset `A`
  reading it incrementally, an asset `B` reading `A` incrementally (with
  patterns, optionally `each=True`). Each has a **commit log**: one entry
  per commit, `[up, rm, reset]` over a set of keys. Its content is the
  log's fold; a prefix's fold is what a bookmark at that commit number has
  read.
- Per incremental input, a **bookmark**: `next` (commits read), the
  **pass** under way (`full` with its position and the head it resumes
  deltas from, `delta`, or a pattern change's `diff`), the **fingerprint**
  and **patterns** it reads under.
- Per output, two **fenced stores** (`A` can move between them) holding
  rows, a **fence** generation per store, and the **repair** intents owed.
- **Runs** (one target, as an automation submits), **attempts** with their
  **generation**, planned **batch**, worker progress, **gate**
  (`none`/`writing`/`aborted`) and **cancel** request. Claims are the
  attempts in `prepared` or `launched`.
- The **manifest**: `A`'s store, each asset's fingerprint, `B`'s patterns
  and whether `B` is declared. **Engines**: one serving (or none, after a
  crash), and possibly a zombie (rolling deploy).

**Actions.** The environment: commits to `S`; deploys (move `A`'s store,
change `B`'s patterns, bump `B`'s version, remove or re-add `B`); worker
crashes; engine crash, restart and takeover; timeouts; user cancels.
The engine: automations firing (`OnChange`), preparing an attempt (claim,
pin, plan the batch), launching, settling (commit, cancel, or lost: take
the gate, owe a repair). Workers: start, acquire the fence and read back
owed repairs, take the gate (or drain on a requested cancel), write, seal.
A zombie engine can only take gates (`aborted`): its journal writes are
fenced. Every action of the environment but the last commit to `S` happens
before it, so that "after quiescence" is a state the model reaches.

**Properties.**

- *One attempt per partition:* at most one prepared or launched attempt
  per asset partition.
- *Bookmarks are honest:* with no pass under way, an output's content is
  what its bookmark says it read: its upstream's content at `next`, under
  its patterns. (A bookmark never passes a change it did not deliver.)
- *Stores match the journal:* with no attempt in flight, no repair owed
  and no stale writer holding its gate, a store holds exactly its
  output's committed content.
- *Every run ends* (under weak fairness for workers and the engine, strong
  for claims and automations).
- *Convergence:* eventually and forever, `A` holds `S`'s keys and `B`
  holds `A`'s under its patterns.

**Abstracted, and why.**

- *One partition per asset:* claims, bookmarks and passes are per asset
  partition and do not interact; fan-in reads committed heads only.
- *Key sets, not versions:* the properties are about which keys an output
  holds; a re-upsert of a present key is a no-op here.
- *One key per full-pass batch, one batch per delta pass:* enough to
  interrupt a full pass between batches (F6) and to pin a delta.
- *Fenced stores only:* an immutable store's stale write is unreferenced
  by construction; the fenced kind is where the gate, fence and repair
  interact.
- *Renames:* a rename moves state wholesale; its risks (F5, F12) were in
  how code looks names up, which the simulation covers. Removal and
  re-adding are modelled.
- *Time:* no clocks; a timeout or a silent worker is an action the
  engine may take at any moment, bounded in number.
- *Retries exhausted:* the retry budget exceeds the fault budget, so a run
  fails only by design choice the model does not explore.

**Calibration.** Each known bug is a switch restoring its pre-fix rule; the
model must find it: `FixF6` (a pass that ends behind the head goes on to
it), `FixF9` (a reset upstream commit makes its consumers read a full
pass), `FixF10` (a full pass reaches the consumer even when its patterns
take no key; `each` reconciles), `FixF13` (moving an output's store
changes its asset's fingerprint, so its inputs read a full pass).

## Formal model: the journal (`spec/tla/Journal.tla`)

The journal (`object-store-state.md` §3, §10; `python/solera_server/journal.py`)
is a protocol between engines that share nothing but the object store: an
old engine still appending, checkpointing and cleaning up while new ones
open. TLC checks it in every interleaving of a few engines, one object
request per step. The model follows the code request by request; where the
design doc says otherwise, that is listed below.

**State.**

- The object store: two maps, `journal` (seq → segment) and `checkpoints`
  (seq → the state it holds). A segment is the engine that wrote it and
  whether it is a fence.
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
created there. A read-only open ends before the fence. Serving, it creates
its next segment at seq + 1: success acknowledges the event; another
engine's segment there means it was fenced, and it stops. After an append
a checkpoint may be due; after a checkpoint, cleanup LISTs the journal and
DELETEs, one request each in any order, the checkpoints older than the
previous one and the segments at or below it but the fences its state
holds. A clean shutdown writes a last checkpoint and cleans up. A create
can land with its answer lost; the retry finds its own bytes. Any engine
crashes between any two requests; a restart is the next engine starting.

**Properties.** Invariants, except the last three.

| Property | Says |
|---|---|
| `NoAckedLoss` | Every acknowledged event is in what an engine opening now recovers: the newest checkpoint, then the segments after it. |
| `OneWriter` | At most one engine appends successfully at a time: every acknowledged segment lies past its engine's fence and below every newer engine's fence. |
| `FencedSeesAcked` | An engine that opened and fenced holds every event acknowledged below its fence. |
| `StatesArePrefixes` | Every state an engine acts on (once it has fenced, or what a read-only open returns) and every checkpoint is a prefix of one history: an event counter value names the same event in every engine. |
| `CountersDense` | Nothing lands at seq n before n − 1 has. |
| `CleanupCovered` | A segment that landed and is gone is covered by a checkpoint still there, which holds its event (`StatesArePrefixes`). |
| `FencesStay` | A fence an engine serves under is never deleted. |
| `OpensNeverFail` | No opener gives up on the journal (before F7's fix, a gap was "journal corrupt"). |
| `Monotonic` | Once an engine serves, its state only grows, and the newest checkpoint only moves forward. |
| `OpensAlone` | Liveness, engines one at a time: an engine that starts opens, unless it crashes. |
| `AppendsAlone` | Liveness, engines one at a time: an append an engine begins succeeds, unless it crashes. |

Liveness assumes weak fairness for each engine's own steps (`Progress`),
none for starting, appending, closing, crashing or losing an answer.

"Cleanup never deletes a segment an opener still needs" holds as
`CleanupCovered`, not literally. Cleanup cannot know about openers (there
is no compare-and-swap and no registry of openers), so it may delete a
segment an opener listed and has yet to read: `ListedStayUntilRead` fails
in 20 steps. The opener copes instead. A missing segment that a checkpoint
covers was cleaned up, so the opener starts over from that checkpoint.

**The object store's consistency, as relied on.** Every step is one
request, atomic, seeing every request completed before it:

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

- *One event per segment.* A segment's events are applied together, so
  batching changes no interleaving of requests; the event counter is then
  the seq. With batches, the counter is the sum of the events up to a seq,
  and engines agree on it exactly when they agree on the segments.
- *A state is the list of segments folded into it.* A fold is a function
  of that list; what events mean is not the journal's business (650bea8
  and 1cc87d8 fixed the fold, not the log).
- *A LIST is one snapshot.* S3 pages a long listing, and pages are not one
  snapshot. Replay uses a listing as a guide and checks each segment with a
  GET, so a torn listing is no worse than a stale one, which the model has.
- *A create and its read-back are one step.* `solera.objects.create` reads
  a colliding object back to tell its own bytes. If the object is deleted
  in between, the GET raises (divergence 2 below); the model covers that
  as a crash.
- *Checkpoint creates never lose their answer:* a retry finds its own
  bytes, so a lost answer is a delay, and a crash after landing is a crash
  after success.
- *Not modeled:* an unreadable checkpoint, the reason the previous one is
  kept (`test_an_unreadable_newest_checkpoint_falls_back` covers it); the
  flush timer and buffer, which decide when a segment is written, not how.

**Bounds and cost** (TLC 2.19 from tla2tools 1.7.4, 8 workers on a shared
8-core VM):

| Model | Engines | Segments | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `Journal-small.cfg`: as built | 2 | 6 | 108,551 | 49 | 6 s |
| `Journal-big.cfg`: with `FixF14` and `FixF15` | 3 | 5 | 2,084,016 | 55 | 1 min 37 s |
| the same, one more segment (4 workers) | 3 | 6 | 13,546,387 | 61 | 15 min 49 s |
| `Journal-live.cfg`: as built, one engine at a time | 3 | 5 | 96,348 | 41 | 25 s |

Two engines (an old one and its successor, as in the simulation's
takeovers) are enough for F7 and every earlier journal bug. F14 and F15
need three: an old engine and two openers.

**Calibration.** Each journal fix in the history is a switch; turning it
off restores the pre-fix rule, and TLC must find the bug. Traces as TLC
prints them, shortest first, in plain words (A is the old engine):

| Switch off | Fix | TLC finds | Trace |
|---|---|---|---|
| `FixF7` | 3c23397 | `OneWriter`, 23 steps | B replays segment 1. A appends 2 and 3, checkpointing at both, and cleanup deletes 2. B's fence create at 2 lands in the hole: B serves under it without A's acknowledged 2 and 3, and A's next append, at 4, succeeds too. |
| `FixF7` | 3c23397 | `OpensNeverFail`, 22 steps | The same, but B listed 1 to 3 before the cleanup: its GET of 2 finds nothing, and opening fails. |
| `KeepFences` | 0b3e226 | `NoAckedLoss`, 25 steps | B fences at 2. B appends 3 and 4, checkpointing at both; cleanup deletes 2, B's fence. A appends at 2: acknowledged, but checkpoint 4 holds B's fence there. |
| `FenceNonce` | f300500 | `OneWriter`, 15 steps | A and B both fence at 1 with the same bytes; each takes the other's for its own, and both serve. |
| `OwnBytes` | none: `_put_segment` always compared bytes | `AppendsAlone`, about 15 steps (a liveness trace varies between runs) | A lone engine's create lands with its answer lost; the retry takes the segment for another engine's, and the engine stops. |

With every switch on, the small model passes. On three engines the design
as built fails: F14 in 27 steps, then, with `FixF14`, F15 in 29 steps
(31 to a serving engine). With both candidate fixes, the bigger model
passes. `spec/tla/check-journal.sh calibrate` runs all of the above and
fails unless each named property is the one violated.

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

The candidate fix the model checks: a fence segment at seq s is a hole's
exactly when a checkpoint at or past s exists and the newest checkpoint
does not list s in its `fences`. Cleanup never deletes a fence, and every
checkpoint written after a real fence holds it. The engine that created
the fence deletes it only if it is a hole's, and otherwise keeps it and
opens again (`FixF14`); an opener that reads a hole's fence opens again
(`FixF15`). This costs a LIST and a checkpoint GET per fence met, and an
engine meets few.

**Where the docs and the code differ.**

1. §10 of `object-store-state.md` reasons that "a fence create that
   succeeds where a checkpoint at or past it exists" landed in a hole.
   That is F14. The code does what the doc says.
2. `solera.objects.create` reads a colliding object back. If cleanup
   deleted that object meanwhile, the GET raises `NotFoundError`, which
   `_fence` does not catch: opening fails with that error, where §10 says
   a GET that finds nothing makes the opener start over. A restart opens
   again, so this costs only time.
3. `glossary.md` lists `seq` among the old names of the event counter.
   In `object-store-state.md` and `journal.py`, `seq` numbers segments, and
   a segment holds many events, so the two are different counters. The
   glossary has no name for a segment's number.

**Running it.** Java 11 or later; the script downloads `tla2tools.jar`
1.7.4 into `spec/tla/.tools/` (gitignored).

```bash
spec/tla/check-journal.sh              # the small model, as built: seconds
spec/tla/check-journal.sh big          # three engines, with the candidate fixes: ~1.5 min
spec/tla/check-journal.sh live         # liveness: ~30 s
spec/tla/check-journal.sh calibrate    # every fix switched off, and F14, F15: ~1 min
```

CI's `journal-spec` job runs `small`, `live` and `calibrate`, in about two
minutes.

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
| F9 | A keyed output moved to another store starts over with an index of its own; a consumer then keeps a key the move's first write dropped (seen when the consumer's first delivery and the moved write run together) | P1 | fixed: a move sets the head's `base`, and a delivery begun at or before it starts over — `test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers`, `test_a_key_a_moved_output_dropped_leaves_its_consumer` |
| F10 | A full delivery (a reset) whose patterns take none of the upstream's keys is skipped without calling the producer, so the consumer never starts over: keys it held stay, though the upstream dropped them — also after a `full` run | P1 | open — `test_a_full_delivery_that_takes_no_key_still_starts_over` |
| F11 | A delta file a pending discard entry reads is deleted while an attempt that was handed the entry runs: the attempt cannot read it, the superseded objects it names leak, and the entry ends `stuck` | P2 | fixed in ffe6921 — `test_f11_a_discard_entrys_delta_outlives_the_attempt_reading_it`, `test_a_discard_entrys_delta_outlives_the_attempt_holding_it` |
| F12 | `copy` renamed to `mirror` and back over rolling deploys (engines overlapping): `mirror` ends holding a key `items` deleted while `mirror` was not served — its old index survives under a watermark already past the deletion | P2 | fixed: a name the manifest no longer declares holds no live state — `test_f12_a_rename_back_and_forth_over_rolling_deploys_keeps_up`, `test_a_name_removed_and_added_back_starts_over` |
| F13 | `items` moved from the table store to FileStore by a crash redeploy; the feed then removes `k11`: `copy` keeps it (F9's territory, the deletion after the move) | P1 | open — `tests/sim/test_replays.py::test_f13_a_key_removed_after_a_store_move_leaves_its_consumers` |
| F14 | An engine that created its fence finds a checkpoint at or past it and deletes the fence as a hole's, but a newer engine had read it and checkpointed past it: the old engine's next append lands in the freed slot, is acknowledged, and no replay sees it (journal spec, three engines) | P1 | open — `tests/server/test_journal.py::test_a_fence_a_newer_engine_moved_past_stays` |
| F15 | A fence created in a hole stays readable until its engine deletes it: another opener replays it in place of the event cleanup deleted, and serves without that acknowledged event (journal spec, three engines) | P1 | open — `tests/server/test_journal.py::test_an_opener_never_replays_a_fence_created_in_a_hole` |
| F16 | A key index compaction moves level-0 files into an empty level 1 without merging them, so level 1 holds overlapping files and a read takes an older entry: `k0` written (level 1); rewritten (level 0); the output replaced by nothing (a level-0 tombstone); a background compaction that empties the index lands only after `k0`, `k1` are written again (level 0, level 1 now empty); `k1` removed (level 0); the next compaction "moves the deepest level down whole" — level 1 holds `{k0, k1}` and `{k1 removed}` — and `k1` reads live. In the simulation `items` kept a key the feed dropped, kept `k3` at an old value, or named an object already collected (`KeyIndex.compact`: `out_level > depth` also holds for level 0 at depth 0) | P1 | open — `tests/sdk/test_keys_index.py::test_any_workload_of_a_few_keys_matches_a_dict`, `tests/sim/test_replays.py::test_f16_a_compaction_landing_after_a_commit_keeps_the_newest_entry` |
