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
thread runs to its end while the loop waits. The loop's order among independent
callbacks is drawn from the run's seed (below, "Interleavings come from the
seed"). Attempt ids, worker id and
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
items ──Incremental(batch 2, each=True)──▶ checks              (fails while a key is "flaky"; knob a dep: each row says which knob it ran under)
items ──Incremental(batch 2)──▶ split ──▶ odd (table store), even (FileStore); on a pool
items ──Incremental(batch 2)──▶ seen (a job: no output, its cursor holds what it read)
knob (version) ──dep──▶ per_site[site ∈ sites] ──whole (fan-in)──▶ summary
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
| `flaky(keys, error)` | `flaky(['k2'], 'failed')` | Per-key keys failing by error class: `Transient` (retried on its backoff), `Failed` (once per deploy), `Rejected` (when the input changes), `Abort` (the whole attempt, per `retries=`) |
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
| `takeover(zombie, change)` | `takeover(zombie=600, change='bump')` | a new engine while the old one still runs (a rolling deploy), optionally on a new variant (a rename twice as often as any other); two takeovers within `zombie` seconds leave three engines running |
| `rename_burst(count, zombie)` | `rename_burst(3, zombie=0)` | renames in quick succession, no time between: one life's attempt still runs while its name goes away and comes back (F34) |
| `stall_launch(point, seconds, pool, crash)` | `stall_launch('flush', 30, pool=True, crash=True)` | the serving engine's next launch (a pool attempt's, with `pool`) stalls at its spec, its control file, or the flush that makes `AttemptLaunched` durable, while pool hosts and workers go on; then it carries on or crashes there, and the platform starts another at once (F26, F36) |
| `redeploy(change, clean)` | `redeploy('summary', clean=True)` | re-registration |
| `prune(keep)` | `prune(keep=0)` | deleting runs by hand |

## Invariants

Checked after every step:

| Invariant | Example of a violation |
|---|---|
| **No state breaks.** No engine's `State` fails applying an event (which would exit the process with code 70). | a reducer raising on a replayed event |
| **One end per attempt.** The journal holds at most one `AttemptFinished` per attempt, and every body that lands extends the events the last one held, or starts over at a newer checkpoint: no swap drops an event. | a zombie engine and its successor both ending attempt `A` |
| **Nothing is read after collection.** No live attempt or serving engine finds an index file or data object gone because garbage collection deleted it. | a delta pass's reader pin not holding its files |
| **Reads say what they read.** A PostgresStore read reports the generation that wrote the rows it loaded, the newest committed before its snapshot — never one that only acquired the partition. | an attempt that acquired and died surfacing as the read generation |
| **Committed keys are readable.** Every key an immutable store's head lists loads back at its indexed generation, from an object a committed attempt wrote. | a stale writer's object referenced by the index |
| **A fenced partition at rest holds its index.** A fenced store's partition that no attempt holds and no dead writer left owing a repair holds exactly the keys its index lists, and, in Postgres, reads as written by its head's generation (`versions.md` §5, §9). | a repair that marks a key live with no rows, or leaves a dead attempt's generation as the partition's |
| **A life is its own.** No attempt launched before its asset was added back (no alias carrying it) installs a commit after (F12's rule, F19). | `seen`'s first life's attempt committing its cursor into the `seen` a deploy added back |
| **One attempt per asset partition.** No attempt launches on an asset partition another launched attempt holds — in the journal, in order, and in the serving engine's claims. A task follows its asset through a rename (an alias); `mirror` renamed back to `copy` without one is another asset. | the hourly run and a manual run both launching `copy` before either ends |
| **A tick's runs are submitted once.** Each run a sensor tick requests is submitted at most once, however late, often, or across restarts the tick's outcome is posted. | a retried post of tick `T` submitting its `per_site` run a second time |
| **No hot loop.** Between external inputs (a step, a client or worker request, an engine start), each kind of engine activity — ticks, journal events, store requests — stays within `burst + rate × the virtual seconds since`: ticks 200 + 10/s, events and requests 500 + 5/s (`PACE` in `tests/sim/world.py`). Erwin's ruling (D60): no wake floor between ticks, the simulation catches hot loops. Counted as they happen, so a loop that never yields virtual time still ends the step. | F20's retry clock resubmitting an unplannable retry at every tick: 201 ticks at one virtual instant |
| **A task's attempts are bounded.** No task launches more than 100 attempts, counted from the durable journal; checked after every step and every 30 virtual seconds while convergence waits for quiet. Ordinary runs launch at most 8 (197 examples measured). The hot-loop bound cannot see a loop that runs through its workers, since every worker request opens a window: this does. | F42: a full run's task relaunching forever, every attempt starting its pass over |
| **Reads at endpoints are exact.** Every key-index read at an endpoint — `page` and `lookup` at a position, pin or snapshot, `changes` between two — equals the fold of the commits its index holds, up to there. A slice (a catch-up's pin) is checked for which keys changed and how they stand, not their class, which needs what came before it. A read the index cannot serve counts as wrong. So does a merge that drops an endpoint some reader holds, because no read at that endpoint can be exact. The fold never touches the spans or merges under test. Each commit's entries are read from its own delta files when it is installed, and each merge's inputs are recorded when it is published, so any index state traces back to its commits (`tests/sim/reads.py`). Which endpoints readers hold comes from the events, not from the engine's own list: attempts in flight from their launches to their ends, so that a claim the engine forgot is still checked. Positions come from the model's records, their life (resets, renames, pattern changes) being the model's to fold; an engine restored from a checkpoint has not seen launches before it. | W36's planted bug: merges planned with no endpoints, so a reader's position falls inside a merged span; "keys/items/_/: a merge dropped endpoint 1, which a reader of ('items', '') holds" |
| **A fenced write holds its gate.** Every write a worker makes to a fenced store (the table store, Postgres) comes after its attempt's gate was created `writing` with that worker's id (`lifecycle.md` §2.4, §3). | a worker paused before its gate, whose attempt the engine closed meanwhile, writing `items` when it wakes; the twin of a `twice` worker writing beside the owner |

Checked once the system is quiet, at the end of every run (`_converge`): faults
off, zombies killed, a fresh change to every source, a forced retry of every
failed, rejected and canceled key (which by design come back only on request),
then:

| Check | Example of a violation |
|---|---|
| **Quiet.** Within four virtual hours: no run in progress, no claim, no worker alive, no automation pending. | a task held forever |
| **Automations converge.** With no manual run, every automated output equals what the oracle computes from its sources — no change stuck, none silently consumed. | `copy` missing a key `items` has |
| **A catch-up run converges.** After a run of every asset with `upstream=True`, every output, `tally` included, equals the oracle's. | a position past rows never delivered |
| **Stores hold what was committed.** A fenced store holds exactly the rows its index lists, no stale writer's rows besides. | a dead writer's patch surviving |
| **The journal alone rebuilds the state.** A read-only `State` replayed from storage equals the live model. | an event applied differently on replay |

## Running it

```bash
uv run pytest tests/sim -q                       # the CI budget: 20 runs, about 20 s
SOLERA_TEST_DATABASE_URL=postgresql://… uv run pytest tests/sim -q   # items may also live in Postgres
uv run pytest tests/sim -q --slow                # 400 runs with shrinking: tens of minutes
SOLERA_SIM_EXAMPLES=2000 SOLERA_SIM_STEPS=60 uv run pytest tests/sim -q --slow
SOLERA_SIM_TRACE=1 uv run pytest tests/sim -q -s # print every run's trace
```

The CI budget is derandomized: the same runs every time, with or without
Postgres (without it, a run drawn for `pg` uses the table store, and says
so in its trace). `--slow` draws new
ones on every worker and reports throughput, e.g. `simulation: 400 runs,
15,880 steps, 61.2 h virtual in 8.5 min (112,000 steps/hour)`.

**An open finding fails runs like any other bug.** It gets a strict-xfail
replay (`test_replays.py`) or engine-level test. If it trips most runs while
its fix is pending, `machine.py` may discard the runs it matches
(`assume(False)` on its signature) so the sweeps can look past it. That
check lands with the finding and leaves with the fix. A signature is
coarser than its bug and can hide another. None is set aside today.

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

When Hypothesis gives up shrinking a long run (it stops after five minutes
of slow progress), `uv run python -m tests.sim.shrink replay.py "<signature>"`
shrinks the printed case by delta debugging: each candidate in a capped process
of its own, keeping a step's invariant calls with it, until no step can go
(`replay_min.py`). An F16 run of 55 steps went to 8 this way.

**A run is deterministic.** One program runs the same callbacks in the
same order every time: twice in one process (as Hypothesis re-runs an
example), in fresh processes, under any `PYTHONHASHSEED`, with the garbage
collector off or eager, with pytest imported or DEBUG logging on. It once
was not: the simulation cancelled a killed actor's tasks in
`asyncio.all_tasks()` order, a set ordered by the tasks' addresses, so the
order changed with whatever the process had allocated before. Z11 could
not be replayed, and F31's replay passed or failed with the file's
location. Tasks are now cancelled in creation order.
`tests/sim/test_determinism.py` holds it in CI (it fails on the old
order). When a run seems to change:

```bash
uv run python -m tests.sim.determinism replay.py                # two fresh processes
uv run python -m tests.sim.determinism --in-process replay.py   # twice in one process
```

Each run records every callback the simulation's loop runs and
schedules, with who scheduled it, labelled by coroutine and line, never
an address. The report shows the first place the two runs part. A
merge's own loop, which runs real threads while the simulation
waits, is not traced: its order does not reach the simulation's.

**Interleavings come from the seed.** asyncio runs ready callbacks in the
order they were scheduled, so a fixed program gets one interleaving
however often it runs. `SimLoop` instead draws each loop iteration's order
from the run's seed (`_ReadyQueue` in `tests/sim/core.py`). Callbacks with
one owner keep asyncio's order: a task's steps and what that task scheduled
(so `call_soon(f)` then `await sleep(0)` still runs `f` first), and one
future's callbacks. Only independent callbacks trade places: three tasks
woken by one `Event.set()` run in any order, and so do timers due
together. One seed, one order, so a failure still replays and shrinks.
`SOLERA_SIM_ORDER=fifo` runs asyncio's own order.

Measured before F34's fix, on 16 Hypothesis seeds of up to 150 examples ×
40 steps (about 2,300–2,500 examples per side):

| Order | Seeds that found F34 | Examples to the first find |
|---|---|---|
| asyncio's | 1 of 16 | 153 |
| seeded | 4 of 16 | 24, 65, 144, 153 |

With F26's fix undone (patched out in the sweep's process), neither order
hit F26 in as many examples. The same sweep, under asyncio's order, found
F36.

## Stores: the generated kit

The named scenarios of `solera.testing.stores` (`docs/stores.md`) are the
readable spec of a store; `solera.testing.storemachine` is its generated
counterpart, for store authors as much as for Solera's own stores. It plays
the engine for one output per run: attempts with growing generations
(acquiring first on a fenced store), writes that commit or are abandoned,
calls retried, stale writers and duplicate workers, readers that pin and
read later, by-key loads as per-key incremental does them, cleanups of what nothing
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
  lookups and range merges are newest-wins over a dict; a
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
- **Staleness** (`tests/server/test_staleness.py`, the rules in
  `tests/staleness.py`): random histories of upstream commits and resets,
  shared-input changes, an output's own reset, asset changes, `keys=` and
  default runs; after each step the engine's stale keys and its partition
  and asset statuses must equal a reference model's (K43, K45), and a plain
  incremental asset is delivered exactly what its record lacks, nothing
  twice. The read-ahead, its cap, full passes, transitive reasons, R2,
  the net delta and K44's classes pass on every store (the reference's
  switch for the build before the net delta is gone with it), each=True on
  the same record as every asset (K47). A19's histories of delivery
  accounting are strict xfails naming each finding until its fix (D93). On
  FileStore, PostgresStore and S3Store, a `keys=` run never touches a key
  it does not name.

**Staleness, by example.** The machine found these histories while K47
was built. Each was an engine bug then, fixed in K47; the first failed
again after semantic change (d), as F37 (fixed in fc29101). Each
is now a test that takes the machine's steps one at a time and states, at
every step, the stale keys and why the partition and asset are stale; the
machine checks the engine against the same answers after every step. All
start with `feed` holding k1 and k2, `items` having read them, and
nothing downstream run. `checks` is each=True over `items` (excluding
`x*`) with the source `knob` a dep; `copy` is a keyed copy of `items`;
`fchecks` is each=True over `feed`.

1. *A keys= run on a never-built output leaves it owing a full pass.*
   keys=[k2] writes k2. k1 is missing, and k2 is stale too: no pass has
   completed, so the record holds no `knob` version to say which one k2
   saw. `knob` moves: still k1 and k2. A default run is the full pass:
   it writes k1 and k2, k2 again under the new `knob`, and `checks` is
   fresh. (F37: the engine had kept the pass k2 began before the move.)
2. *A key rewritten after an asset change is no longer stale for it.*
   keys=[k2, k3] writes k2 (k3 is not upstream). `checks`' definition
   changes: k1 and k2 are stale, for input and definition. keys=[x1, k2,
   k3] rewrites k2 under the new definition: k1 and k2 are still stale,
   for input only (k1 missing, k2 with no `knob` version on record). A
   default run completes the pass.
3. *A key gone upstream stays stale through a keys= run that does not
   name it.* `checks` is reset; `feed` removes k2 and k3, which `items`
   has not read. A default run of `checks` writes k1 and k2 and is stale
   behind `items`. `items` is rebuilt: k1 at a new version, k2 gone.
   keys=[k1, x1, k3] rewrites k1; k2, held but gone upstream, stays stale,
   and the record keeps the keys= run (the reconcile is owed). A default
   run drops k2: fresh.
4. *keys= runs that rewrite every key after a `knob` move complete the
   pass.* keys= naming every key completes `checks`' first pass: fresh.
   `knob` moves: k1 and k2 are stale. keys=[k1, k2]: fresh again.
5. *A full pass spread over keys= runs delivers each key once.* `copy`
   runs; its definition changes, and its keys are stale. keys=[k1] starts
   the pass over and delivers k1 (`copy` now holds k1 alone). keys=[k1,
   k2] delivers k2 alone and completes the pass: fresh. A default run
   then delivers nothing.
6. *A key neither side holds is never stale.* `fchecks` runs: fresh.
   `feed` adds k3: stale, since `fchecks` lacks it. `feed` removes k3:
   fresh. Likewise after keys=[k1], with k2 missing, stale: `feed`
   removing k2 leaves `fchecks` fresh (F35).

## Kani: tried, then dropped

Kani (0.68, with CBMC) was tried on the native readers in October 2026.
It proved, for every input within its bound, that the varint reader stays
in bounds, round-trips every `u64` and drops no bits, and that the filters'
parser reads nothing outside its bytes whatever its offsets. On the code
before ab0c346 it found F23 in 11 s. Everything else ran out of memory or
time at a few bytes (block, index and garbage-file decoding), or was out of
its reach (zlib, and the writer and merges, which run on rayon's threads).
It was dropped because only small, allocation-free parsers were in reach,
and those are covered without its toolchain: `test_varints_read_alike_and_whole`
compares native and the reference on every varint length from 1 to 11
bytes and every pattern of the ninth and tenth bytes (it fails on the code
before ab0c346), `test_generations_at_the_varint_edges_round_trip` writes
every u64 edge, `test_filters_read_alike_whatever_they_hold` reads any
filter bytes both ways, and the `kx-file` fuzz target reaches the filters
with any offsets (in its corpus, 478 inputs parse whole and every refusal
of the filters' parser occurs).

## Fuzzing: bytes that may be corrupt or hostile

Each target, what ran on 2026-10-03 (M5 Max, one core each, libFuzzer's
RSS limit 2 GB), and the verdict. The native targets are in `native/fuzz`:
`uv run python native/fuzz/seeds.py` writes seed corpora (real files from
the writers), then `cargo +nightly fuzz run -O <target> corpus/<target> --
-max_total_time=1200`.

| Target | Reads | Ran | Verdict |
|---|---|---|---|
| `kx-file` | any bytes as a `.kx` file: footer, filters, index, sorted entries, each block, lookups. Each input runs twice: as it is, and with its index and filter CRCs made to match, as a hostile file's would | 10.0 M inputs in 20 min, 1,322 edges | clean. Keep it: the checksums stop random corruption, so this is the only thing exercising the parsers behind them |
| `kx-merge` | up to three runs of up to four arbitrary blocks, any codec, merged as a range and as pages | 3.9 M, 1,794 edges | clean |
| `kx-round-trip` | any entries (keys, generations, deletions, payloads, predecessors), any block size, both codecs: encoded, then decoded back equal | 2.0 M, 2,660 edges | clean |
| API request bodies (`tests/server/test_api_bodies.py`) | every route that records: runs, source commits, retries, prunes, cleanups; near misses of valid bodies and any JSON (NaN, infinities, integers of any size, deep nesting). A body taken must replay from the journal and survive a checkpoint, and nothing answers 500 | 60 examples in CI; 2,000 with `SOLERA_FUZZ_EXAMPLES=2000` (9 min), clean after the fixes | found F27 (P1) and F28 (P3) on its first runs. Keep it in CI |
| Resolve requests | already fuzzed (property tests, above) | — | — |
| The journal object and checkpoints | — | not fuzzed | not worth a target: a body that does not parse, or applies badly, fails the open loudly, and nothing catches it; what reaches the journal is the API's to check (above) |
| Control files | — | — | not built yet |

`lookup` and the merges decoded zlib blocks without a size limit (F29, since fixed). An
8 KB fuzz input expands at most about 1,000 times, so the fuzzers cannot
show it; its test builds a 32 KB block that inflates to 32 MiB.

## What is not exercised yet

Known gaps, most valuable first; each says what would close it.

- **Time partitions and several dimensions.** The project has one dynamic
  dimension (`sites`): broadcast, fan-in over an upstream's extra
  dimensions, windows and `all_partitions=` are untested together. Waits
  for the input kinds of the model changes (`glossary.md`), then a
  `day × site` asset in the project.
- **The sensor host and a sensor's body.** The simulation calls `watch`'s
  function directly (`sensor_round`) and posts its outcome: the host's
  polling, its supervision of running ticks, its timeouts and its
  reconnects are not simulated. Closing it: run `run_sensor_host` on the
  simulation's loop with a seeded channel.
- **Faults in the middle of an executor call.** A call run on a thread
  (`asyncio.to_thread`: a span merge, a Postgres transaction) runs to its end
  at one virtual instant, so no fault, kill or other actor's step falls
  inside it. A merge cannot meet a takeover halfway. Closing it: split
  the calls the simulation cares about at their object requests.
- **Migrations** in the simulation: no rule runs `migrate` yet. Nothing
  blocks one since F18's fix; it would run on `pg`.
- **A worker that resumes after its run was purged**: with the control
  file (`lifecycle.md` §2.4) it finds the file gone and stops, however
  long it paused; `Attempt.tla` checks it ("Formal model: the attempt
  control file"), and `tests/server/test_retention.py` plays it. The
  simulation does not purge runs within its hours yet, so it cannot
  resume a stale worker after one.

Closed in this round: the key index format, merges and resolves against
a dict (property tests, and F16; span merges and `changes` at live
endpoints in `tests/sdk/test_keys_index.py` and `test_keys_spans.py`); glob patterns; resolve
framing; claims (one attempt per asset partition); the gate under worker
death, pause and duplicates; rolling deploys with three or more engines;
per-key errors by class and forced retries; runs with `keys=`; an asset
with two outputs on two kinds of store; a `Pool` with racing hosts; a
sensor that requests runs, and one that fails; a job; the key cache
under tight budgets and with its files deleted or corrupted under it.

## Calibration: does it find what we know?

Each finding below was put back on main (ebaffaa): its fix reverse-applied to
a copy of the Python packages, first on `PYTHONPATH`, so no product code
changed. Then the simulation ran as a sweep runs, with Hypothesis seeds of 150
examples × 40 steps, in each order (`SOLERA_SIM_ORDER`). After a failure,
the seed's search goes on from the next Hypothesis seed, so one finding
cannot hide another. Each cell gives how many examples ran before the
finding's own signature first showed, counted across seeds in order. The
seeds that found it are in brackets. "Before" is the simulation as it was;
"after" is with the changes below.

| Finding (its fix) | Before: seeded / asyncio's order | After: seeded | After: asyncio's order |
|---|---|---|---|
| F20, retry storm (b6d9968) | the process ran out of memory (3 GB in 48 s), nothing reported | 14 of 450 (3/3) | 9 of 60 (1/1) |
| F26, pool offer before durable (4979e26) | not found in 600 / 750 | 44 of 450 (1/3) | not found in 450; 192 of 450 (1/3) while every stall crashed |
| F34, rename onto an earlier life (0d77890) | not found in 600 / 600 | 27 of 900 (5/6) | 27 of 900 (5/6) |
| F36, collection under a launching attempt (0720b87) | not found in 600 / 600 | 360 of 900 (1/6) | 93 of 900 (3/6) |
| F37, pass begun before a dep moved (fc29101) | not found in 600 / 600 | 131 of 450 (2/3) | 371 of 450 (1/3) |
| F38, current-only output outrun (083ca73) | 91 of 450 (1/3) / 91 of 450 (2/3) | 365 of 450 (1/3) | 233 of 450 (2/3) |

Before: F26–F37 on the code they were found in (7599798), F20 and F38 on
ebaffaa. Main itself, with the changes, failed in none of its 900 examples
per order; the hot-loop bound never fired on it.

What changed, and why each finding needed it:

- **`stall_launch`** (F26, F36). A launch writes its spec, then its control
  file, records `AttemptLaunched`, and waits for the flush that makes it
  durable. F26 needs a pool host to be offered the attempt inside that wait,
  and then the engine to die before the flush. F36 needs a cleanup the
  attempt was handed to be acknowledged by another inside it. Both windows
  are a fraction of a virtual second, and nothing held them open. The rule
  stalls the next launch at one of the three points. Then the engine goes
  on (F36: the attempt must live to read the file) or crashes there and the
  platform starts another at once (F26: the orphan's rows must meet a
  serving engine). It never strikes while an invariant reads, a clean stop
  runs, or the run converges, and never restarts an engine already replaced.
- **`checks` depends on `knob`, and says so in its rows** (F37). F37 left a
  key computed under an old `knob`; with `checks` blind to `knob`, the
  stale key looked like a fresh one. Now each row carries the `knob` it ran
  under, and convergence expects the last one. F37 also needs a pass left
  open, which flaky keys already provide.
- **Renames weigh double, and `rename_burst`** (F34). F34 is three renames
  with no time between, so that an earlier life's attempt is still running
  when its name comes back. Random steps put waits in between, and that
  attempt finishes first. Weighing renames double alone did not find it in
  900 examples per order; the burst finds it in 27.
- **The hot-loop bound** (F20). See the invariant above. Before it, F20 was a
  process that ran out of memory: no failing case, nothing to shrink. Now
  it is a violation at the step that started it. The bound comes from
  43,881 windows of ordinary runs (200 examples per order). There, ticks
  ran steady at 2.1 per virtual second (the eval loop's cadence), and no
  window held more than 40 events or 45 requests (0.3 per second on
  windows of a minute or more). The bound leaves 5× the tick rate and
  more than 10× every observed burst. F20 breaks it within the same
  virtual instant.
- F38 needed nothing: it was found before. It is found later now (365 and
  233 examples, against 91): the new rules take a share of every run's 40
  steps.

Ordinary runs are not slower. `tests/sim` took 45.8 s on average with the
changes against 55.1 s without. That is three pairs, run back to back on
the same base while other work shared the machine: 0.72×, 0.96× and 0.83×
per pair. The CI budget makes 1,202 steps in 33 runs, against 1,168 in 35.

**Reads at endpoints (T30).** W36's planted span bug is one line in
`Upkeep.maintain`, `endpoints = set()`: every merge plans and writes as if
no reader held an endpoint (branch `calib/spans-endpoint-bug`, 293957b,
never merged). Before the invariant, the simulation's own checks passed it.
On 293957b the CI run reported "1 passed" over 12 runs, and only the trace
check (`check-trace.py spans`) rejected it. The bug's reads are not wrong as
such: a merge drops the segment start a reader's position needs, and the
engine then plans that reader's next read from the wrong place, consistently.
So the invariant checks both that reads are exact and that merges keep the
endpoints readers hold.

| | Seeded | asyncio's order |
|---|---|---|
| The planted bug: examples to the first find | 10 of 905 (6/6 seeds) | 4 of 900 (6/6 seeds) |
| Main: failures | none in 750 | none in 750 |
| The CI run (12 examples, derandomized) with the bug | fails: "a merge dropped endpoint 1, which a reader of ('items', '') holds" | |

Main ran 750 examples per order, not 900: its sixth Hypothesis seed (5000)
ran out of memory (3 GB, the harness's cap) in both orders, on the
simulation without this invariant too. It turned out to be F42: one example
whose second full run of `copy` relaunched its task without end. A task's
attempts are now bounded, so such a run fails fast with a replay instead.

Two rules of `changes` had to be learned from false alarms on main. A key
live at neither end, or at both with equal payloads, is neither (the net
rule). And a merge that folds a neither's versions away drops it from the
page, harmlessly: no consumer is delivered one. So a neither may be listed,
while every added, updated or removed key must be.

`tests/sim` takes 1.42× as long with the invariant: 31.6 s against 22.3 s,
three interleaved pairs on the same product code. That is within the 1.5×
bound. The read checks themselves take about 0.1 % of a run; the rest is the
hooks around every install, event and read, which could be trimmed.

## Sweeps

Long runs (`--slow`, new seeds each). Steps count rules, not invariant
checks; `pg` means `items` may also live in Postgres.

Up to 6a941c9, 24 sweeps (seeds 101–941) made about 11,000 runs and 600,000
steps, a third of them `pg`, some of 80 or 120 steps per run. Every new
rule or invariant got one sweep on default stores and one on `pg`. They
found F13, F16, F17 and F21. They also found four bugs in the simulation
(a Postgres clock in the gate invariant, the claims invariant twice, a pool
host idle forever after its worker died before owning an attempt) and one
in the environment (a reused database's fence table of an older shape,
which the simulation now drops at boot).

New sweeps since that summary:

| Run | Code | Seed | Runs × steps | Stores | Result |
|---|---|---|---|---|---|
| Z4 | F21 fixed (b8bf1a4), nothing set aside | 941 | 203 runs, 14,284 steps, 61 h | default | green |
| Z5 | the journal object (cfdc723) and weeds (fedf1b2) | 951 | 250 runs × 50 steps | default | F24 (an engine fences itself after its checkpoint move's answer is lost) |
| Z6 | as Z5 | 952 | 265 runs, 16,102 steps, 64 h | pg | green |
| Z7 | the control file (9c2343d) | 961 | 271 runs, 14,774 steps, 67 h | default | green |
| Z8 | as Z7 | 962 | 250 runs × 50 steps | pg | F31 |
| Z9 | K45 read-ahead, K46 reasons (35c0766) | 971 | 266 runs, 14,913 steps, 74 h | default | green |
| Z10 | as Z9 | 972 | 263 runs, 14,304 steps, 58 h | pg | green |
| Z11 | F31, F32 and the reasons ruling fixed (bf17f26) | 981 | 250 runs × 50 steps | default | two attempts claimed `mirror` at once; Hypothesis could not replay it (the simulation's task-order leak): F34, see Z11b |
| Z12 | as Z11 | 982 | 250 runs × 50 steps | pg | `items` missed a feed key (`k1`) after automations: F33 (the case replays; an early extraction of mine mangled it) |
| Z11b | Z11 again, on the deterministic simulation (3a965b9) | 981 | 250 runs × 50 steps | default | F34, replayable (Hypothesis shrank it to 9 steps) |
| Z13 | semantic change (d) (fae165c), seeded interleavings | 991 | 241 runs, 12,514 steps, 28 h | pg | a worker read a `checks` delta file collection had deleted: F36 (fixed since, 0720b87) |
| Z14 | as Z13, asyncio's order (`SOLERA_SIM_ORDER=fifo`) | 991 | 250 runs × 50 steps | pg | F36 again, and `copy` and `checks` never converge after automations: F38 |
| Z15 | F38 fixed, the calibrated simulation (974dd1f): stalled launches, rename bursts, `checks` a dependent of `knob`, the hot-loop bound | 1001 | 260 runs, 15,210 steps, 71 h | pg | green |
| Z16 | as Z15, asyncio's order (`SOLERA_SIM_ORDER=fifo`) | 1001 | 260 runs, 15,203 steps, 70 h | pg | green |

## Formal models: which spec owns which rules

Six TLA+ specs, checked by `spec/tla/check.sh` (`check.sh` alone is CI).
Each rule has one owner, so none falls between them:

| Spec | Owns | Leaves to |
|---|---|---|
| `Execution.tla` | the engine: runs, claims, attempts, passes and batches of default runs; positions as default runs move them; resets and the reset rule; asset changes and `OnChange` (K34); fenced stores, gates and repairs; engine crash, restart and takeover; worker crashes, timeouts, cancels | `keys=` runs and staleness to `Positions.tla`; the attempt's control file to `Attempt.tla`; the journal to `JournalObject.tla` |
| `Positions.tla` | partition records: snapshots, K45's read-ahead and its cap, `each=True`'s per-key records, the full pass completed across runs; `keys=` runs; staleness, exact and transitive (K39, K46); and the concurrency that can break them: a batch planned at the claim and committed later, upstream commits between, a commit refused after a reset or an asset change | workers, faults and engines to `Execution.tla`; superseded by `ObservedSet.tla` with the rebuild (below) |
| `ObservedSet.tla` | the observed set (D126, D133) and its observation record: decode, candidates and classify-once, batches reclassified from what was served, points, ranges at each batch's head and their fold, the base with its before-image, rebases, every run kind (default runs in batches, `keys=`, per-key cancels, start-overs, dropped runs), pattern, context and definition changes, upstream resets, retention cuts, a batch in flight between dispatch and commit | what an attempt is to `Execution.tla`; the index and its endpoints to `Spans.tla` |
| `Attempt.tla` | the attempt control file: who owns an attempt, the gate, the sealed result or the engine's end; duplicates, zombies, retention; nobody learns of an attempt before its launch is durable (F26) | what an attempt computes to `Execution.tla` |
| `JournalObject.tla` | the journal: fencing, appends, checkpoints and their cleanup, lost answers, failed requests | what the events mean to the others |
| `Spans.tla` | the span key index's lifecycle as built: endpoints (positions, passes, an attempt's reads and landing point), the merge lanes, upload and publication in two steps, pins and garbage, empty spans, the orphan collector and its epochs, merge retries, crashes, takeovers, resets; trace-validated against the simulation | what a span holds, and why reads at its boundaries are exact, to the Lean proofs (`experiments/lean/KeyIndex`) |

## Formal model: execution semantics (`spec/tla/Execution.tla`)

The simulation samples interleavings of the real code; the model checker
TLC explores **every** interleaving of the design, at small bounds. The
model is the design as decided (`glossary.md`, with its model changes),
not the code: where they differ, the code is the suspect.

```bash
spec/tla/check.sh execution            # smoke and calibrations (CI): about a minute
spec/tla/check.sh execution design     # store moves (with and without B), patterns and versions, removal, a zombie: ~30 min at 6 workers, liveness included
spec/tla/check.sh execution big        # every deploy kind, every fault kind, each=True: too large to finish yet
spec/tla/check.sh execution all
```

**State.**

- Three output partitions in a chain: a keyed **source** `S` (holding
  keys 1 and 2), an asset `A` reading it incrementally, an asset `B`
  reading `A` incrementally (with patterns; `B` is `each=True` in one
  configuration, `A` never). Each has a **commit log**, `[up, rm, reset]` per commit
  over a set of keys: its content is the log's fold, and a prefix's fold
  is what a position at that commit number has read.
- Per incremental input, a **position**: `next`, the **pass** under way (a
  `full` pass's position and the head it resumes deltas from; `delta` and
  `diff` passes are one batch each), and the **fingerprint** and
  **patterns** it reads under, and whether a **reset** took it (no full
  pass has caught it up since). The fingerprint is the asset's version:
  which store holds the output is not in it, since a move resets the
  output instead.
- Per output, two **fenced stores** (`A` can move between them) holding
  rows, a **fence** generation per store, the store the head is in, and
  the **repair** intents owed.
- **Runs** of one target, as an automation submits them (default runs:
  `keys=` runs are `Positions.tla`'s), **attempts** with their **generation**, planned **batch**,
  worker progress, **gate** (`none`, `writing`, `aborted`) and **cancel**
  request. Claims are the attempts `prep`ared or launched.
- The **manifest** (`A`'s store, versions, `B`'s patterns, whether `B` is
  declared, and how many times each output was reset: a move of `A` or
  the re-adding of `B` makes it a new output under the same name) and
  the **engines**: one serving (none after a crash) and
  possibly a zombie (a rolling deploy).

**Actions.** The environment: commits to `S`; deploys (move `A`'s store,
change `B`'s patterns, bump `B`'s version, remove and re-add `B`); worker crashes; engine crash, restart and takeover; timeouts;
user cancels. The engine: automations firing (`OnChange`), preparing an
attempt (claim, pin, plan the batch), launching, settling (commit; a
drained cancel; or lost: take the gate, owe a repair). Workers: start and
acquire the fence (reading back owed repairs), take the gate (or drain a
requested cancel), write (checking the fence), seal. A worker runs on
after the engine gave up on it. A zombie engine can only take gates
(`aborted`), and only of the attempts it created before it was fenced
(with `zombie` among the faults explored): its journal writes are fenced.
A move resets `A` at the deploy: its head and repair intents go, and so
do its own positions and `B`'s on it. An attempt launched before a reset
of its output, or of the output it reads, is refused at commit and its
run carries on; what it wrote is owed a repair unless its own output was
reset. An asset change (K34, aa0e7dd: `B` added back, its
patterns or version changed, `A` moved) leaves the asset due: its
`OnChange` automation owes a firing, if its input has a head
(`FiringsOwed`; for a fan-in, a materialized upstream
partition, which one partition per asset does not reach). Deploys and faults may come after the last
commit to `S`, so the system must converge without a later change; only a
user cancel comes before it, since a canceled run's change waits for the
next one.

**Properties.**

| Property | Kind | Says |
|---|---|---|
| `OneAttemptPerPartition` | safety | at most one prepared or launched attempt per asset partition |
| `PositionHonest` | safety | with no pass under way, every key no later commit touched is in the output exactly when it was in the upstream at the position, under its patterns: a position never passes a change it did not deliver |
| `StoreMatchesJournal` | safety | with no attempt in flight, no repair owed and no stale writer at its gate, a store holds exactly its output's committed content |
| `RunsEndCaughtUp` | action | a run that succeeds leaves its asset caught up: no pass under way, the position at the upstream's head, the output what the upstream holds under its patterns. Checked as the run succeeds, so no later change is needed to see one that ended halfway |
| `Quiesces` | liveness | eventually and forever, no run is active (every run ends) and `A` holds `S`'s keys, `B` holds `A`'s under its patterns, each read under its current fingerprint (convergence) |

Liveness assumes weak fairness of all engine and worker steps together
(each behaviour takes finitely many: everything is bounded), of an engine
restarting after a crash, and of the commits to `S`.

**Abstracted, and why.**

- *One partition per asset:* claims, positions and passes are per asset
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
and TLC must find it (`check.sh execution calibrate`):

| Switch | Rule as designed | Counterexample with it off |
|---|---|---|
| `FixF6` | a pass that ends behind the head goes on to it | `A` reads keys 1 and 2 in a full pass; key 1 commits; a cancel stops the run; `S` drops key 1; the firing resumes the pass, delivers key 2, and the task ends behind the head: `A` keeps 1 for good (`Quiesces`, 36 to 37 steps) |
| `FixF10` | a full pass reaches the consumer even when its patterns take no key; `each` reconciles at its end | `S` drops key 1, so `A` and `B` hold 2 alone; `B` excludes key 2 and its version is bumped; `B`'s full pass takes no key and is skipped: `B` keeps 2, which its patterns exclude (`PositionHonest`, 27 steps) |
| `ResetOnMove` (with `FixF17`: calibration `move`) | a move resets the output at the deploy (K10; b7d8ae7). With both off, a move only changes where `A`'s next write goes, and that write starts the store over without a full pass | `A`'s full pass commits key 1 into store 1; `A` moves to store 2; the pass's next batch, key 2, starts store 2 over: `A` holds 2 alone, its position says 1 and 2 (`PositionHonest`, 17 steps). F13's mechanism as W15 described it; F13's replays turned out to take F10's route |
| `FixF22` | an asset change leaves its asset due, if its input has a head (K34, aa0e7dd; before it, d6585fb for a move): `A` moved, `B` added back, `B`'s patterns or version changed. Calibrations `F22-move`, `F22-shape`, `F22-add`, each with no commit to `S` after the first | `A` is built; `A` moves, which resets it; nothing fires `A`, and `S` never changes again: `A` stays empty (`Quiesces`, 18 steps). `B`'s patterns or version change: `B` keeps what it read under its old declaration (34 steps). `B` is removed and added back: `B` stays empty (19 steps). Before this change the model's last commit to `S` came after every deploy and fired `A`, which hid the first (as it hid finding 1) |

**Results.** With every fix on (TLC 1.7.4; the first six rows re-run
after the asset-change rule (K34), deploys and faults free to come after
the last commit to `S`, with 6 workers and a 24 GB heap on a MacBook Pro;
the others as first run, before the review's fixes, with 8 workers on a
shared machine):

| Configuration | What varies | States (distinct) | Time | Verdict |
|---|---|---|---|---|
| `smoke` | nothing: the plain pipeline | 10,852 | 2 s | passes, liveness included |
| `store` | `A` moves away and back (`B` left out) | 6,404 | 1 s | passes |
| `reset` | `A` moved once while `B` reads it | 2,993,830 | 10 min 4 s | passes; without the repair a refused `B` attempt owes, `StoreMatchesJournal` fails |
| `shape` | two pattern changes or version bumps of `B`, in any mix | 3,490,004 | 15 min 47 s | passes |
| `remove` | `B` removed and re-added (two deploys: one pair) | 228,033 | 1 min 40 s | passes |
| `zombie` | a takeover, the zombie taking gates (`B` left out) | 1,808 | 1 s | passes; with the zombie free to take any attempt's gate, `Quiesces` fails (the review's finding 2) |
| `deploys` | one deploy, of any one kind | over 4.5 million | capped at 25 min | no invariant violated in what was explored; liveness not reached (before the review's fixes) |
| `faults` | two faults in all, of any kinds | over 2.8 million | capped at 25 min | the same (before the review's fixes) |
| `each` | `B` is `each=True`; one deploy of any kind and one fault of any of four kinds | over 2.3 million | capped at 25 min | the same (before the review's fixes; then `A` was `each=True` too, finding 4) |
| `safety` | two deploys and two faults in all, of any kinds: random behaviours | configured: 100,000 behaviours of up to 150 steps; run so far: 2,000 | 7 s for the 2,000 | passed what ran (`check.sh execution safety` runs the configured target) |

The budgets are shared across kinds: `MaxDeploy` and `MaxFault` count
deploys and faults of any kind, so a configuration listing every kind
explores every *choice* of them within the budget, not every kind at
once. The bounds, exactly:

| Configuration | Keys | Source commits after the first | Deploys (budget: kinds) | Faults (budget: kinds) | `B` | `each` | Attempts, runs, tries |
|---|---|---|---|---|---|---|---|
| `smoke` | 2 | 1 | 0 | 0 | declared | — | 12, 10, 3 |
| `store` | 2 | 1 | 2: move | 0 | left out | — | 12, 10, 3 |
| `reset` | 2 | 1 | 1: move | 0 | declared | — | 12, 10, 3 |
| `shape` | 2 | 1 | 2: pattern, bump | 0 | declared | — | 12, 10, 3 |
| `remove` | 2 | 1 | 2: remove (a removal and a re-adding each spend one) | 0 | declared | — | 12, 10, 3 |
| `zombie` | 2 | 1 | 0 | 1: takeover (the zombie's aborts spend none) | left out | — | 12, 10, 3 |
| `deploys` | 2 | 1 | 1: move, pattern, bump, remove | 0 | declared | — | 12, 10, 3 |
| `faults` | 2 | 1 | 0 | 2: worker, crash, takeover, timeout, cancel, zombie | declared | — | 12, 10, 3 |
| `each` | 2 | 1 | 1: move, pattern, bump, remove | 1: worker, crash, timeout, cancel | declared | `B` | 12, 10, 3 |
| `safety` | 2 | 1 | 2: move, pattern, bump, remove | 2: worker, crash, takeover, timeout, cancel, zombie | declared | — | 14, 10, 4 |

No design bug found so far. Modeling errors found: a fingerprint change
during a full pass must start the pass over, as the engine does; and,
from an independent review of the spec, a promoted `keys=` run ended after one batch
(finding 1, which gave `RunsEndCaughtUp`; `keys=` runs are now `Positions.tla`'s) and a zombie could
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
- `A`'s own positions and `B`'s position on `A` go: `A` reads `S` in a
  full pass, and `B` re-reads `A` from scratch. No position claims
  nothing (`PositionHonest`) until a full pass catches it up.
- A move away and back is two resets.
- `A` is due for a rebuild: its `OnChange` automation owes a firing
  (an asset change, K34), so it is written again at once, not when `S`
  next changes.

Every attempt records which reset of its output, and of the output it
reads, it launched under. One launched before a later reset is refused at
commit, and its run carries on with a fresh attempt. What it wrote is
owed a repair if its own output was not reset (a `B` attempt refused
because `A` was reset), and dropped with the output if it was. TLC found
the need for that repair: without it, `B`'s refused attempt leaves rows in
`B`'s store that no commit records (`StoreMatchesJournal`, check `reset`).
The code does record it (`_fail` takes a refused result's gate intents).
The first write after a reset is a reset commit (its index starts
empty), planned as a full pass; a reset upstream commit makes every
consumer read a full pass (F9).

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
key 2, starts store 2 over: `A` holds key 2 alone, while its position says
it read 1 and 2.

**Next** (open choices, what is too big):

- *Too big to exhaust* (`check.sh execution big`): every deploy kind
  together (`deploys`, over 4.5 million states unfinished), every fault
  kind (`faults`, over 2.8 million), `each=True` with deploys and faults
  (over 2.3 million), and every deploy and fault kind together (over 22
  million). The ways down: split by kind (as `store`, `shape`,
  `remove` do), leave out `B` where it plays no part (`WithB`), or check
  safety on random behaviours (`safety`). TLC's `-simulate` with liveness
  is not sound; liveness needs the exhaustive splits.
- *Open modeling choices:* a pattern change's diff pass runs once the
  position reaches the head (the design says "commits up to the change
  finish under the old patterns"; the cutover point is simplified); a
  write lands whole or not at all (no half-written batch); immutable
  stores, renames, fan-in, per-key incremental's failed keys and retry passes, and
  time partitions are not modelled; runs have one target (no `upstream=`
  run graph); automations are `OnChange` only (an asset with a schedule,
  or none, waits for its next run after an asset change).
- *What I would do next:* a run graph (`upstream=True`, a task waiting on
  another) for "every run ends" across tasks; a half-written batch with
  repair; and an immutable store beside the fenced one.

## Formal model: positions and staleness (`spec/tla/Positions.tla`)

*The design of `positions-from-reads.md` (K43; K45 and its amendment; the
full pass completed across runs; K46; K47).* What a run reads,
what the partition record keeps of it, and whether the status derived
from that record tells the truth. `Execution.tla` keeps the engine,
faults and resets with default runs; this model leaves them out to reach
three keys.

```bash
spec/tla/check.sh positions          # base, each, the calibrations: ~1 min
spec/tla/check.sh positions three    # three keys: the note's k1, k2, k3
```

**State.** A chain `S -> A -> B` of keyed outputs, one partition each.
`A` copies `S`; `B` copies `A` under its patterns, `each=True` or not.
Every commit has a fresh id, and a key's version is the id of the commit
that last wrote it (0: absent). A run plans one batch when it claims the
partition and commits it later; the upstream may commit in between. The
record of an incremental input: a snapshot (the upstream commit read
through); the read-ahead, the keys `keys=` runs read past it with the
version read (K45: the amendment's `(commit, attempt)` entries say the
same), at most `MaxEntries` of them; the full pass due after a reset or
an asset change, with the keys it delivered; and the definition the last
finished pass was under. Every asset keeps this one record, `each=True`
too (K47): no per-key payloads. An `each=True` batch may fail keys it
hands the asset: read, so recorded, but not written, and failing (the
failure index, at the version read); a retry pass reads them again. A ghost records, per output key, the input version and the
definition it was produced from: the truth the record is checked
against.

**Runs.** A `keys=` run in a pass delivers its key into the pass, the
first delivery starting the output over; outside one, it reads its key
ahead if it is behind. A default run in a pass delivers the keys not yet
delivered at their version and finishes the pass; outside one, what is
behind. A keys= commit that leaves nothing behind finishes the pass or
collapses the read-ahead; so does a retry pass that leaves nothing
behind (K47). An attempt planned before a reset of its
output or its input, or before an asset change, or against a record that
moved since, commits nothing.

**Properties.**

| Property | Says |
|---|---|
| `StatusExact` | with no attempt in flight, the status the record gives equals the truth: the output holds exactly its input's keys under its patterns, each produced from the input's current version under its current definition, and, for `B`, so does `A` (K46: stale if its upstream is) |
| `DeliveredOnce` | no key is delivered again at the input version, under the definition, it was produced from (K45, and the pass completed across runs) |
| `Collapsed` | action: a commit that leaves nothing behind leaves no read-ahead entry (K47: the cap on entries is practically unreachable) |

The truth counts a key that failed at its input's current version as
failing, not stale; one that failed at an older version as stale.

**Calibration.**

| Rule off | TLC finds |
|---|---|
| `FixNet`: "behind" is the net delta (K39) | `StatusExact`, 4 steps: a key added and removed past the position counts as behind, though a rerun changes nothing |
| `FixTransitive`: stale if an upstream is (K46) | `StatusExact`, 3 steps: `S` commits; `B` looks fresh to its own check while `A` is stale |
| `FixSkip`: a default run skips a key a `keys=` run read at its version | `DeliveredOnce`, 8 steps |
| `FixContinue`: a default run continues a pass `keys=` runs began | `DeliveredOnce`, 6 steps: it starts the pass over and delivers their keys again |
| `FixCollapse`: a `keys=` commit collapses the record only once nothing is behind | `StatusExact`, 4 steps |
| `FixRetryCollapse`: a retry pass that leaves nothing behind collapses the record (K47) | `Collapsed`, 8 steps |

**Results.**

| Model | Keys | Distinct states | Time |
|---|---|---|---|
| `base`: `B` not `each`; two commits to `S`, two `keys=` runs, one deploy, one read-ahead entry | 2 | 1,857,464 | 14 s |
| `each`: `B` is `each=True`, on the same record, failing keys and one retry pass | 2 | 5,379,738 | ~1 min |
| `three` | 3 | 7,388,487 | 1 min 10 s |

Before K47, `B` as `each=True` with per-key records reached exactly the
states of `B` on the plain record, with the same verdicts: where an output
key depends on one input key, the snapshot and its read-ahead answer
exactly as per-key records do. That is why `each=True` keeps the same
record (K47).

**What the model found** (design points, sent to W22):
- *A key read ahead, then removed upstream.* `A`'s snapshot is at `N`;
  `S` adds key 1, a `keys=(1)` run reads it, `S` removes it. The net
  delta past `N` is empty (key 1 absent at both ends), so neither the
  status nor the next default run looks at it, and `A` keeps key 1 for
  good. The rule: a key's last read version is the read-ahead's, else the
  snapshot's; a read-ahead key whose version changed since is behind.
- *The latest read wins.* Two reads of one key, in a pass or ahead, keep
  only the later; the higher version is not the later one (a removal is
  0).
- *An asset change invalidates attempts in flight.* A batch planned
  under the old definition must not finish the pass due under the new
  one: the model refuses its commit, as for a reset. The note does not
  say yet; W22 to confirm.
- *A start-over drops the records of the keys it does not deliver,* and
  the failure index with them: the output is rebuilt from scratch.
- *A reset of the input drops the failure records* with the other
  records that read it; else an outdated failure is left with no pass to
  clear it.
- *A key failing at its current version is not behind,* in the record's
  status as in the truth; and a batch fails only keys it hands the asset
  (under the patterns, held upstream), not a removal or a start-over's
  drop, which the engine writes.

## Formal model: the span key index's lifecycle (`spec/tla/Spans.tla`)

*The code as built (key index step 2 and later, with c4eb4f7's epochs:
`upkeep.py`'s `maintain`, `_merge`, `collect`, `collect_orphans`;
`model.py`'s `endpoints`, pins and `IndexMerged`; `engine.py`'s
`_prepare`), against `key-index-design.md`'s "Lifecycles the
implementation must honour".* What a span holds is the Lean proofs'
(`WriteBound`, `Segments`, `Tiling`, `Keys`), taken as given: a read at
commit `e` from spans that keep `e` as a boundary (a span's first commit,
or a segment start inside it) agrees with the full history. So a span here
is its commits `[a, b]`, the segment starts it keeps, the index's life,
whether it has files, and its writer's epoch; a read is exact iff its
endpoints are boundaries.

```bash
spec/tla/check.sh spans         # five models and the calibrations: ~25 min at 4 workers
spec/tla/check.sh spans long    # takeover: ~20 min (the freeze round, and on demand)
```

**Model.** One index. Commits append spans, one with no files when a
commit changes no key, and tick the journal's event counter. A claim's
reads are endpoints from the step that makes it (`_prepare` is
synchronous); it lands at the head + 1 or, while a pass is under way, at
the pass's end; its manifest is the state, its pin the event counter. A
consumer may hold two claims (a retry claimed before the claim it replaces
goes). A claim settles (the position moves to its landing point), commits
a pass's batch (the position stays and holds the pass's end until its
last batch), or fails. Upkeep
plans a merge of adjacent spans from the endpoints it knows, keeping a
segment start at each, in one of two lanes that never share an input, and
uploads it under a name carrying its epoch. Publication has two steps:
the engine applies `IndexMerged` to its model (if the index is still of
its life and holds its inputs, `IndexState.holds`: their commits and file
names) and the journal makes it durable later; the index's other events
wait for that, as the journal orders them after it. Refused, the output is
deleted. Garbage is deleted, once durable, at or under the pin floor,
by the serving engine or by a zombie whose `durable()` passed before the
fence (its successor replays the same garbage). The
orphan collector lists the merge outputs, then deletes them one at a
time: those its model does not name (current spans, garbage, its running
merges), judged after the listing, of its own epoch or earlier. A takeover
writes epoch + 1 and replays the durable journal; the engine before runs
on as a zombie, with the model it had (a publication not yet durable
included) and its collector wherever it was, until it halts. A merge's
work can fail; after `R` = 3 failures of one input set the index merges no
more. A reset starts a new, empty life.

| Property | Says |
|---|---|
| `Tiling` | the spans tile this life's commits exactly, durable and in the serving engine's model |
| `ReadsExact` | every endpoint is a boundary of both |
| `StateStored` | every file the journal or the serving engine's model names is stored |
| `ReadersStored` | every file of a claim's manifest is stored |
| `PublishingStored` | the output of a merge the serving engine runs is stored |
| `AttemptsBounded` | no input set is attempted more than `R` times |

| Rule off | TLC finds |
|---|---|
| `FixLanding`: a claim's landing point is an endpoint from its plan on (A10) | `ReadsExact`, 7 steps: commit 1; an attempt claims, landing at 2; commit 2; a merge of both keeps no start at 2; the attempt settles inside the merged span |
| `FixBounds`: a merge keeps a segment start at every endpoint it was planned with | `ReadsExact`, 7 steps |
| `FixLanes` and `FixInputs` together: the lanes never share an input; publication re-checks the inputs | `Tiling`, 8 steps: two merges over one span; the second publishes over inputs the first replaced. Either rule alone suffices: `lanes` and `inputs-alone` pass |
| `FixLife`: publication re-checks the index's life | `Tiling`, 6 steps, with spans that have no files (W42): life 0 commits an empty span [1,1] and uploads a merge of it; a reset; life 1 commits an empty [1,1]; the merge publishes, its inputs' name lists (none) matching the new life's, and the index holds a span of life 0. With files, the input check alone refuses it: `life-files` passes |
| `FixSettleLife`: an attempt's commit re-checks the index's life | `ReadsExact`, 5 steps: an attempt claimed before a reset lands its position in the new life |
| `FixPinFloor`: garbage waits for the pin floor | `ReadersStored`, 5 steps |
| `FixDurable`: inputs become garbage at publication, not at upload | `StateStored`, 5 steps |
| `FixRetries`: at `R` failures the index stops merging | `AttemptsBounded`, 6 steps |
| `FixEpoch`: a collector deletes outputs of its epoch or earlier only | **F40, the code before c4eb4f7.** `PublishingStored`, 6 steps; `StateStored`, 7 steps (`epoch-state`): a takeover; the new engine commits, merges and publishes; the zombie's collector lists the output, which its model does not name, and deletes it |
| `FixJudgeAfter`: the collector judges what is named after its listing | `PublishingStored`, 6 steps: it names; a merge of its own is planned and uploaded; it lists, and deletes the output |
| `FixGarbageNamed`: the collector counts the garbage its model knows as named | `StateStored`, 9 steps (`garbage-named`, no readers): a publication applied but not yet durable; the collector deletes its inputs, which the journal still names |

**A zombie's publication that never became durable** (the coordinator's
question, also asked of W42): zombie A applies `IndexMerged(M)`,
replacing X, to its model; it is fenced before the event is durable, so
B's state still names X. A's collector does not delete X: the epoch rule
does not protect it (X is A's or older), but A's model holds X as garbage,
which the collector names. `takeover` passes with this reachable, and
`garbage-named` shows the rule it rests on. M itself is A's and unnamed by
B: B's collector deletes it, as it should.

The collector's listing is the files written up to it (`upto`): files are
never written again, so those still stored are what it listed and nobody
deleted since. `Collectors` turns the collectors on (`takeover`,
`orphans` and their calibrations); `base` runs without them, a sixth of
the size. While a publication is pending, the index's other events wait
for it: the spec does not interleave a commit or a claim into that
window, which only reorders events the journal orders anyway.

Not modelled: renames, which keep the index's prefix; a pattern change's
split, an endpoint held as a position's is; the merge policy's
thresholds and the write bound (Lean's); cleanup reads
(`Model.cleanup_reads`); pins other than claims' (a delta pass's, a
sensor tick's, the engine's own readers), which only make collection
wait longer; the read-ahead's entries (`Positions.tla`'s).

| Model | What | Distinct states | Time (4 workers) |
|---|---|---|---|
| `base` | 3 commits per life, 6 files, 1 reset, 1 claim, 1 merge at a time | 7,945,559 | 1.5 min |
| `takeover` (`long`) | a takeover and the collectors, 2 commits, 5 files, no reset | 73,114,314 | 20 min |
| `orphans` | `base` with the collectors, no reset | 11,195,707 | 3 min |
| `retries` | failing merges, 2 commits, no reset | 34,096,548 | 8 min |
| `empty` | spans with no files, 2 commits per life | 13,350,551 | 3 min |
| `passes` | passes, 2 claims, no reset | 27,170,742 | 8 min |

## Formal model: the observed set (`spec/tla/ObservedSet.tla`)

*The design of `docs/observed-set.md` (D126, D133, D153; modelled at
67b85eb on `docs/ledger-draft`, which dropped passes: a run keeps only its
cursor, in memory).* A
consumer partition's **observed set** `S` is, per key of a keyed
incremental input, what it processed: present or not, its version, the
context (whole and dep versions) and the upstream's life. Its stored
form, the **observation record** `E`, is a base (a commit, or a commit
with a before-image of what changed since), disjoint ranges (keys
observed at one head) and points (explicit observations). The model
keeps `S` literally beside `E` and checks, after every commit:

| Property | Says |
|---|---|
| `DecodeExact` | `decode(E) = S`, key by key, except while a start-over is owed |
| `CountExact` | a tally's count is the number of keys `S` holds present |
| `TallyExact` | once nothing is owed, the count is the effective input's |
| `OwedExact` | what the design owes (candidates, each classified once from its decoded state to upstream now) is exactly `S` against upstream now |
| `StaleExact` | stale iff something is owed, a start-over included |
| `RowsKept` | a batch never loads a row its store has deleted |
| `EndpointsKept` | a batch never records a range at an `H` retention has cut |
| `Admitted` | no run is refused (A26 N5) |
| `NoRegress` | an action property, for histories whose versions only grow: no output goes back to an older version |

```bash
spec/tla/check.sh observed    # free and current, then every history: ~10 min at 4 workers
```

**Model.** Three keys in key order, in prefix groups `a`, `b`, `c` (glob
`d` matches none); patterns are sets of globs, `{}` the universal
include; two contexts; versions to 2 (3 in histories), 0 absent. Upstream
is an index (commits of one life) and a store's rows: a **versioned**
store serves the rows a commit names, which cleanup deletes unless a
claim pins them; a **current-only** store serves its rows as they are
now, ahead of the index or reverted. A **default run** computes what is
owed (everything, for a start-over) and processes it in key order, a
batch at a time; its cursor and owed set live in its memory, which a
cancel, a failure or a takeover drops. A batch is dispatched at the head
`H` (its attempt holds the partition, one at a time; its claim pins `H`)
and commits unless its life or definition changed since. Its commit
classifies every key of its range from the decoded old state to `H`,
reclassifies from what was served, and records a range `(prev, c]` at
`H` and points where the store served something else; an older range
relabels to `H` where nothing in it changed, a changed key keeping its
decoded value as a point; a run's last batch rebases. A **`keys=` run**
takes one key, if owed, and writes a point. A **per-key batch** canceled
part done commits its finished keys as points. Deploys change patterns,
context, or the definition (a start-over); upstream commits, resets (a
new life: a start-over); retention cuts the index's history (the base
takes a before-image; an older range folds to the cut, its changed keys
becoming points); upkeep folds points. A run's last batch makes the base;
the design also makes one whenever ranges at one head span every key,
which only differs when an earlier, dropped run left the tail covered.

**Each finding as a history.** Every finding the rebuild answers is a
history of these steps (`ObservedSet.tla`'s `Histories`), checked twice:
with every rule on it runs through and holds; with the rule that
prevents it off, TLC finds it. 34 histories: 27 calibrated, and 7 that
hold by construction (no rule prevents them; nothing can break them):

| Finding | Rule that prevents it | Off: TLC finds |
|---|---|---|
| A19 R1, R3; A26 N2 | classes from the whole decoded old state, not the base alone (`FixDecodeOld`) | `CountExact`, 9 and 10 steps; `DecodeExact`, 10 |
| A19 R2 | a batch classifies every key of its range at its `H`, not just those owed when its run began (`FixRecheck`) | `DecodeExact`, 9 |
| A19 R4; A27 R5 (the label) | a point keeps the patterns it was read under (`FixPointPatterns`) | `DecodeExact`, 9 |
| A27 R5 (absence) | an absent point is a live value, not a tombstone (`FixAbsentPoints`) | `DecodeExact`, 8 |
| A19 R5; A26 N4; A27 R2; F41 | classes and points follow the row served (`FixObserve`) | `DecodeExact`, 10 to 15 |
| A26 N3; A27 R8 (fold) | a point folds only if decoding without it gives its value (`FixFold`) | `DecodeExact`, 12 and 13 |
| A26 N5 | points spill, nothing is refused (`FixNoCap`) | `Admitted`, 6 |
| A27 R1 | layers carry their context, and a moved context is a candidate (`FixContext`) | `OwedExact`, 7 |
| A27 R3 | the universal include's change scans everything (`FixUniversal`) | `OwedExact`, 7 |
| A27 R4 | net changes are candidates, not classes (`FixClassOnce`) | `OwedExact`, 9 |
| A27 R7 | an upstream reset owes a rebuild (`FixLives`) | `DecodeExact`, 6 |
| A27 R8 (supersede, split) | a range write drops the points inside it and keeps other ranges' parts (`FixSupersede`, `FixSplit`) | `DecodeExact`, 8 and 10 |
| A27 R9 | each range's changes since its own endpoint are candidates (`FixRangeCands`) | `OwedExact`, 12 |
| A27 R10 | staleness is the comparison, not a pattern label (`FixPending`) | `StaleExact`, 2 |
| a reader paused past the window (the doc's example) | a cut leaves a before-image (`FixImage`) | `DecodeExact`, 11 |
| the fold (the revised design) | an older range relabels to `H` only where nothing changed; a changed key becomes a point (`FixRelabel`) | `DecodeExact`, 9 |
| a batch in flight across a reset or a definition change | a batch commits only if its life and definition hold (`FixCommitCheck`) | `DecodeExact`, 10; `CountExact`, 12 |
| a batch in flight when the cut passes its `H` | its claim pins `H` against the cut (`FixClaimPin`) | `EndpointsKept`, 13 |

Hold by construction: A26 N1 and A27 R6 (a batch reads at the head,
whose rows no cleanup deletes); a run under way when the cut passes its
earlier batches (each batch reads at its own head); a run reaching a key
that a newer `keys=` run delivered (no regression: the batch reads at a
head at least as new); and a `keys=` batch in flight across a cut, a
definition change or a reset (it writes a point, which needs no
endpoint; it never clears the start-over that follows, and the
start-over discards it). A19 R6 and R8, D111's bounds and the failure
retries, are outside the record and kept as they are. F41's history is
its replay's end state (`tests/sim/test_replays.py`): `items` holds k11;
`feed` becomes {k11}, then {k1, k3}; the store still serves k11 when
`items` reads through that commit. Without `FixObserve` its range says
k11 is gone while it still holds it, and it is fresh.

**Found on the design.** On the design with passes (389fcfb; the model
then: 73eb893), three counterexamples, each with a fix checked there: a
retention cut past an active pass's `T`; a pass at an older `T` sending a
key back after a newer `keys=` delivery; a batch committing across an
upstream reset or definition change. W22 folded the fixes in (89ac81b);
Erwin then dropped passes. On the revised design the first two cannot
happen (`runcut`, `regress` hold with no rule); the third still needs
its check (`resetfly`, `definefly`), and so does the remainder of the
first: a single batch in flight, whose claim must hold the cut back
from its `H` (`cutfly`). The revised doc (67b85eb) has the batch's rows
pinned by its claim, but retention folds only bases and ranges before a
cut, and an in-flight batch's `H` is neither yet: its commit would write
a range at a head the index no longer has. The fold needs its own check (`relabel`), and
a batch must classify every key of its range at its head, not only what
was owed when its run began (`a19r2`): a key that changed meanwhile
would otherwise be recorded at `H` without being delivered.

**Free exploration.** Every step above, interleaved: too many states to
exhaust (a two-key scope still grew past 8.7M states at depth 14), so
random behaviours of 40 steps, every state checked. With every rule on:
71,884,258 states (versioned store) and 82,462,123 (current-only), no
violation. Each rule off, the free search finds its violation unaided
(`FixNoCap` aside, which needs two `keys=` points at once): the free
model reaches every guarded case. CI runs 20,000 behaviours of each,
seeded: about 56M states each.

**Not modelled:** an `each` chain's transitive staleness (A19 R10: the
doc intersects owed keys key by key, then rolls up); several keyed
inputs, and payloads apart from versions; the spill's bounds and cost
(A27 R11); prefix scans by byte order (a glob is its group here); a
before-image written over several cuts beyond one key's first entry;
takeovers and renames beyond dropping a run's memory (the record is
durable; a rename keeps the life).

**What to delete.** `Positions.tla` models today's positions: `next`
and passes, K45's read-ahead and its cap, the pattern change's
transition, three staleness predicates. The rebuild replaces all of
them, so delete `Positions.tla`, its `check.sh` entries and its section
here when the rebuild lands. Two things it checks are not in
`ObservedSet.tla` and should move with the rebuild: transitive staleness
along `each` chains (K46; A19 R10), and a `keys=` paged run's pages
(K45's shared entries) if paging stays.

## Formal model: the journal object (`spec/tla/JournalObject.tla`)

Built (K18): the journal is one object, `control/journal.json`, swapped
with `If-Match` (`object-store-state.md` §0 and §10; why, and what it costs:
`journal-object.md`; the code: `journal.py` on `solera.objects.swap`). The
object holds the engine id of its writer, the name of the current
checkpoint and the events since that checkpoint.

**What the model abstracts.**

- The store is the journal (a body, or nothing yet), the checkpoints by
  unique name, and at most one checkpoint that does not read back as
  written (the code compares the bytes it reads back). An ETag is a
  function of the body, as on S3 and R2 (an MD5), so `If-Match` compares
  bodies. A store whose ETag is a version counter (GCS, `MemoryStore`, or
  `file://`'s SHA-256 under its lock) gives at least that.
- Journal writes are numbered (`writes`), for the properties only. A
  fence or an acknowledgment records the number of the write that carried
  it. `OneWriter` and `FencedSeesAcked` order by these numbers.
  `MaxWrites` bounds the model.
- Faults: crashes between any two requests, overlapping and zombie
  engines, lost answers, 409s (a conditional write refused without
  landing), one checkpoint that does not read back, and cleanup at any time. A
  checkpoint is due at any point after an append: the cadence does not
  matter to safety. A write whose answer was lost GETs the journal at
  once, as `swap` does. With `Failures` (the `failures` model), a request
  may fail: one of a checkpoint or its cleanup, and the engine drops the
  rest and serves on (`GiveUp`), as `journal.py` does, a checkpoint left
  behind being deleted by the next cleanup's LIST; or the GET settling a
  swap, and the engine writes again (`AskFails`). Not the move: a move
  whose outcome is unknown is settled before the engine serves again
  (`AskStep`; the code does not yet, F24). Giving up without writing the
  journal could repeat forever, so that model bounds each engine's
  checkpoints (`Bounded`, a state constraint). Liveness leaves failures
  out: a store that fails every read forever is an outage.
- `JournalResolves`: the journal names a checkpoint that is there and
  readable.
- One step is one request. Abstracted: one event per append, a state as the list of events folded into it, a
  LIST as one snapshot, and checkpoint creates that never lose their
  answer.

**Calibration.** Each rule of the design is a switch. With it off, TLC
must find the bug it prevents (`check.sh journal calibrate`):

| Rule off | TLC finds | Trace |
|---|---|---|
| `EngineId`: the journal names its writer | `OneWriter`, 9 steps | A fences, then begins an append. B reads the journal and fences, but without an id its body is A's byte for byte, so the ETag does not change. A's append still matches, and it is acknowledged after B's fence. |
| `AskJournal`: a refused write reads the journal | `AppendsAlone`, 12 to 18 steps (a liveness trace varies between runs) | A lone engine's append lands, but its answer is lost. The retry is refused, because its own write changed the ETag. The engine takes that for a newer engine's write and stops. |
| `ReGet`: a gone checkpoint sends the opener back to the journal | `OpensNeverFail`, 25 steps | B reads the journal, which names `cp-A-1`. A moves to `cp-A-2`, and its cleanup deletes `cp-A-1`. B's GET of `cp-A-1` finds nothing, and B gives up. |
| `Verify`: a checkpoint is read back before the move | `NoAckedLoss`, 11 steps | A appends, writes a checkpoint that does not read back as written, and moves the journal to it. An opener can no longer load A's acknowledged event. |
| `ListFirst`: cleanup deletes only what it listed before its move | `NoAckedLoss`, 24 steps | A moves to `cp-A-1`. B opens, fences, appends and writes `cp-B-1`. A, now a zombie, LISTs for its cleanup and deletes every checkpoint except `cp-A-1`, so `cp-B-1` goes too. B's move then lands, and the journal names a deleted checkpoint. |

**Bounds and cost** (TLC 2.19; `-workers 3`, `-Xmx6g`, on a shared
8-core VM):

| Model | Engines | Journal writes | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `small` | 2 | 7 | 734,754 | 53 | 26 s |
| `fixed` | 3 | 5 | 4,464,794 | 46 | 2 min 52 s |
| `big` (before the steps below) | 3 | 6 | 23,642,632 | 53 | 8 min 52 s |
| `live`: one engine at a time, no 409s, no failed requests | 3 | 7 | 914,709 | 47 | 1 min 57 s |
| `failures`: a checkpoint, its cleanup or the read settling a swap may fail | 2 | 5 | 1,767,521 | 38 | 15 s |

The counts are of the model since trace validation: a lost answer reads
the journal at once, and requests may fail (`failures`). The times are of
the model before, on the VM; on a MacBook Pro with 5 workers, `ci` takes
about 6 minutes.

Liveness leaves out 409s. A store that refuses every conditional write
forever is an outage, not a fault the journal can outlast.

**Not modeled.** The `file://` lock, which gives one machine the same
`If-Match`. Flushes that run while a checkpoint is written
(`journal-object.md`, "The checkpoint's cost, measured").

```bash
spec/tla/check.sh journal       # two engines, three, liveness, calibration: ~6 min
spec/tla/check.sh journal big   # three engines, six writes: ~9 min
```

## Formal model: the attempt control file (`spec/tla/Attempt.tla`)

Decided (K18) and built: one control file per attempt, swapped with
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
  - claims the partition for the next attempt once the last one has
    ended, creating its file `open`, then makes `AttemptLaunched`
    durable; fenced or crashed in between, it abandons the attempt, which
    the next engine never learns of;
  - asks a worker to cancel (it drains);
  - reads a file, then ends the attempt on what it read: a swap to
    `ended`, recording `none` from `open` or `owned` and `writing` from
    `writing`;
  - settles from a final file (`sealed` or `ended`) with a durable
    `AttemptFinished`;
  - restarts, forgetting what it read.
- *A zombie engine* ends any live file.
- *Retention* deletes a settled attempt's files at any time.
- *A worker* exists once its attempt's launch is durable (`OfferDurable`;
  without it, once the engine created it, as F26's pool offer did). It
  boots (stopping if its spec is gone), reads the file, swaps
  it to `owned`, acquires its generation at the store, swaps it to
  `writing`, writes (the transaction checks the generation), and swaps it
  to `sealed`. A requested cancel lets it drain before `writing`; a
  batch with nothing to write seals without the gate (found by trace
  validation). Its
  swaps can land with the answer lost; its retry, refused, reads back its
  own body. A refused swap that finds `ended`, another worker's body, or
  no file stops it. Workers pause anywhere and crash anywhere.

**Properties.**

| Property | Kind | Says |
|---|---|---|
| `NoWriteAfterNone` | safety | an attempt whose `AttemptFinished` says it wrote nothing has no store write that landed, before or after the decision: no write after an abort is decided |
| `NoOrphanWrite` | safety | every store write that landed is of an attempt the journal launched: none of an attempt no engine knows, which nothing ends, commits or repairs |
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
bug (`check.sh attempt calibrate`; with several TLC workers, a trace's length varies
by a step between runs). Each trace is also a test against the code, in
`tests/server/test_control_file.py`: the scenario plays out and the bug
does not happen (switching the rule off in the code fails its test):

| Rule off | TLC finds | Trace |
|---|---|---|
| `PreCreate`: the engine creates the file before the launch, and nobody else creates it | `NoWriteAfterNone`, 11 to 12 steps | The create-if-absent gate with nothing retained, the case the old design kept gates `gate_days` for. The engine ends A before its worker reports, creating the file `ended` (`none`), and settles. A's worker boots and reads its spec, and retention deletes A's files. The worker finds no file, creates it `owned`, acquires (no later attempt has), marks `writing` and writes. |
| `TakeWriting`: the worker marks `writing` before its first write | `NoWriteAfterNone`, 10 to 11 steps | The worker owns A and acquires; the engine ends A from `owned` (`none`); the worker writes anyway |
| `EngineSwaps`: the engine ends with `If-Match`, not a blind PUT | `NoWriteAfterNone`, 11 to 12 steps | The engine reads `open`. The worker owns A, acquires and marks `writing`. The engine's blind PUT replaces `writing` with `ended` (`none`, from the `open` it read), and the worker's write lands. With `OneOutcome` also checked, TLC finds that first (5 steps): a blind end overwrites the zombie's. |
| `OfferDurable`: no worker learns of an attempt before its `AttemptLaunched` is durable | `NoOrphanWrite`, 8 steps | F26. The engine creates attempt 1's file `open` and is fenced before `AttemptLaunched` lands. A worker already offered the attempt owns the `open` file, acquires generation 1, marks `writing` and writes: rows of an attempt no engine knows |
| `Classify`: ended from `writing`, the evidence is `writing` | `NoWriteAfterNone`, 10 to 11 steps | The worker marks `writing` and writes; the engine ends from `writing` but records `none` |

**Bounds and cost** (TLC 2.19; 3 workers, 6 GB, on a shared 8-core VM;
one engine restart, the zombie, lost answers):

| Model | Attempts | Workers each | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `small` | 2 | 1 | 2,181,664 | 32 | 42 s |
| `dup`: a duplicate worker | 1 | 2 | 43,740 | 20 | 2 s |
| `live`: liveness, the engine fair | 2 | 1 | 2,181,664 | 32 | 3 min 59 s |

Two attempts with a duplicate worker each (`check.sh attempt big`) is
too large to finish here. An earlier version, without draining, passed
68 million states before it was stopped. The duplicate (`dup`) and the
sequence of attempts (`small`) are each covered whole.

```bash
spec/tla/check.sh attempt        # small, dup, live, calibration: ~5 min
spec/tla/check.sh attempt big    # two attempts with a duplicate worker each: too large to finish
```

**Into `Execution.tla`.** That model has attempts end atomically
(`EndLost`) and its gate as one field. Once the control file is built,
its attempts can carry the file's states and the end as a read and a
swap, with this model as the reference for the protocol. Its fairness
argument then needs the bound above: finitely many file changes per
attempt.

## Trace validation: simulation runs against the specs

The specs check the design; the simulation runs the code. Trace
validation joins them: it checks that what the code did in a simulation
run is a behaviour of the spec, request by request. A run that is not
shows either code doing what the design does not allow, or a step the
spec misses.

```bash
uv run python spec/tla/check-trace.py SPEC              # run the simulation, check each run
uv run python spec/tla/check-trace.py SPEC RUN.jsonl…   # check runs exported before
```

- **Export.** With `SOLERA_SIM_REQUESTS=DIR`, the simulation writes each
  run's object requests to `DIR/NNNNN.jsonl`: the actor (an engine, a
  worker, or a read-only open), the request, the path, and how it ended
  (`ok`, `missing`, `exists`, `refused`, `lost`, `error`), with what a
  LIST returned (`Objects.trace` in `tests/sim/core.py`). It only reads:
  the simulation runs the same with it on.
- **Check.** `check-trace.py` keeps the requests on the objects a spec
  describes and writes them as a TLA+ sequence. The spec's trace module
  (`{Spec}Trace.tla`) extends the spec: each request is a step of its
  actor that makes it and sees what the code saw, or a check without a
  step for a request the spec folds into another (a read-back after a
  create). Before each request its actor may take the steps that make no
  request. TLC searches for a behaviour that explains every request, one
  worker, and reports the run valid, or the first request no behaviour
  explains, with that actor's requests before it.

**First target: the segment journal** (`Journal.tla`, retired with its
code in cfdc723). Of a batch of 24 simulation runs
(8 to 673 journal requests each), the first three, of 125, 125 and 437
requests, were explained in full; the fourth, of 588, had 458 explained
when it was stopped at 5 minutes (the search's cost, not a request the
spec could not make), and the spec was not worth making faster. Mapping the code's requests onto the spec
found three steps the spec lacked (added for the check, not kept, since
the spec retired): the engine
whose fence create succeeded LISTs checkpoints twice (`_behind`, then the
hole test), and never serves under a fence a checkpoint covered; a failed
checkpoint create or cleanup DELETE drops the rest of the cleanup; a
checkpoint create is not retried, so one whose answer is lost lands
unknown to its writer. The cost was in the invariants, checked on every
state of a trace with a dozen engines and hundreds of segments, and in
branches the search keeps open (a run of 437 requests: 36 s and 298,493
states with `NotDone` alone, minutes with every invariant). The journal
object's trace module should settle each choice at the request that
reveals it.

**The journal object** (`JournalObject.tla`, `JournalObjectTrace.tla`;
`check-trace.py journal`). The code's requests map onto the spec's steps
almost one to one: a GET of the journal is an open's read or the read that
settles a refused or unanswered swap; a swap is a fence, an append or a
move; a GET of a checkpoint is a load or the read-back before the move.
The code names a checkpoint `{engine id}-{n}`, with an id new on every
open, and the spec `<<engine, n>>`: the translation maps each id to the
engine that creates a checkpoint under it. A write of the journal is
logged with the checkpoint its body names, so a move and an append, both
swaps, are told apart: without it, a checkpoint dropped just before its
move kept two readings of every later swap alive, and the search doubled
at each checkpoint.

Of a batch of 21 simulation runs (53 to 603 journal requests each), 19
were explained in full; two, of 515 and 290 requests, were stopped at
10 minutes with no request found unexplained (the first has 21 engines
overlapping; the search's cost, not yet looked into further). The
first batches found three steps the spec lacked, now in it: a write whose
answer was lost GETs the journal at once (`swap`'s read-back), where the
spec wrote again first; that GET can fail too, and the engine writes again
(`AskFails`); and a checkpoint or its cleanup can be given up midway, on a
failed request or because a clean shutdown cancels the flusher, also just
before the move (`GiveUp`). Checking the code against the spec by hand
found the move whose outcome is never settled (F24) and the 32-bit engine
id (F25).

**The attempt control file** (`Attempt.tla`, `AttemptTrace.tla`;
`check-trace.py attempt`). `Attempt.tla` models one partition, so a run
is split into one trace per partition: the export records the partition
from each attempt's spec and the state each control-file write wrote.
Attempts are numbered in the order their files were created, and an
engine older than the newest that has acted is the zombie. The fenced
store's acquire and writes, and the engine's journal decisions, make no
object request: they are steps any request may follow. Of a batch of 20
simulation runs (about nine partition traces each), 16 were explained in
full; the other 4, each with a partition of about 90 requests, were
stopped at 10 minutes (`TRACE_TIMEOUT`), the search's cost, not yet
looked into; no trace had a request the spec could not make. The batch
found three steps `Attempt.tla` lacked, now in it: a batch with nothing to
write seals without the gate (`NothingToWrite`); a launch that fails
before `AttemptLaunched` lands abandons its attempt with no engine
restarting (`Abandon`); and retention deletes an abandoned attempt's
files (`Purge`). Every `Attempt.tla` model and calibration passes or fails
as before.

**The span key index** (`Spans.tla`, `SpansTrace.tla`; `check-trace.py
spans`). Its rules act on what the journal decided (a publication and its
inputs, a life, a claim's reads and pin), so the export also writes the
journal's durable events, each after the request that made it durable,
and every engine request carries the event counter of that engine's
model. `check-trace.py` replays the events through the engine's own
`Model`, one trace per index: commits, publications, resets, and claims,
each with its landing point and whether it reads a pass, placed where the
engine prepared it: at its pin (the counter it was claimed at) or, if a
commit came between the claim and the plan, at that commit. The spec
claims and prepares in one step, so its pin is then later than the
code's, which only makes it laxer about garbage than the code. An engine request is placed after the
events its engine had applied: the engine merges spans its model holds
before the journal makes them durable. A claim ends as a settle (the
position moved to its landing point), a pass's batch (the position still
holds the pass) or a failure; a merge output's upload is its first file's
create, its starts taken from the publication; a deletion is the serving
engine's or a zombie's; a read must find its span stored. The trace
module fixes each step's parameters, so TLC walks the trace rather than
searching: a merge may keep more starts than this spec's endpoints (a
pattern change's), never fewer.

Of a batch of 41 simulation runs on the pushed tree, 265 index traces
(25,580 records, up to 334 a trace), all are valid: 671 merges uploaded,
668 published, 3 never; 1,877 claims (419 pass batches); 1,701 deletions
by the serving engine and 5 by zombies; 2,521 collector listings, 106 of
them a zombie's; 2,764 takeovers; 60 resets; 11,448 reads. No commit wrote an empty span and no publication was refused in
that batch: those are checked by the models alone. The first batches
found what `Spans.tla` lacked, now in it: a claim lands behind the head,
at a pass's end, held by the position only once a batch commits (a first
batch that fails leaves no pass); a consumer holds two claims, a retry
claimed before the claim it replaces goes; a zombie whose `durable()`
passed before the fence deletes garbage after it (an earlier batch had 43
such deletions); and an attempt claimed before a commit and prepared
after it (its pin before its reads). None was a bug in the code. A mutation that plans merges with no endpoints (`maintain` passing
an empty set) passes the simulation's own checks in 13 runs, while 7 of
their 85 traces stop at the first upload that keeps no start at an
endpoint.

**Next:** `Execution.tla` needs an abstraction map (key sets, one
partition per asset) and comes last.

## Findings

| # | Finding | Severity | Status |
|---|---|---|---|
| F1 | `Incremental(exclude="k1*")` with a string is split into characters: `*` excludes every key (`include=` wraps a string) | P3 | fixed in c521d9b — `test_one_exclude_pattern_is_a_pattern_not_its_characters` |
| F2 | A keyed output moved to another store keeps its key index: the next write stores only the changed keys there, and the others become unreadable | P1 | fixed in 59812c4 — `test_a_keyed_output_moved_to_another_store_stays_readable` |
| F3 | A per-key incremental input loads its upstream as `dict[str, T]`; the store contract, the conformance kit and `examples/json_table_store.py` do not say or do so, and per-key incremental over such a store fails every key | P2 | doc and example fixed in 217c8f4; by-key loads checked by the store machine (`read_by_key`) |
| F4 | A removed asset's launched attempt that asks for more batches, or fails retryably, re-queues a task no manifest can place: its run never ends | P1 | fixed in 59812c4 — `test_a_removed_assets_last_attempt_ends_its_run` |
| F5 | An attempt launched before a rename settles into a head that moved and an output the manifest no longer names: its claim is never released, its run never ends | P1 | fixed in 59812c4 — `test_an_attempt_launched_before_a_rename_settles` |
| F6 | A run that finishes an interrupted full pass ends there though the upstream moved: the `OnChange` firing it ran for delivers nothing of its change | P1 | fixed in 1cad0bd — `test_a_change_made_during_a_full_pass_reaches_downstream` |
| F7 | Journal cleanup deletes segments a writer still opening has not read; its fence lands in the hole and it serves a state without acknowledged events | P1 | fixed in 3c23397 — `test_a_slow_new_writer_never_fences_into_a_deleted_segment` |
| F8 | A `full` run resets an unkeyed incremental output at a new `base`; a consumer whose next commit is exactly that base gets the reset as a delta and keeps the rows the upstream let go (`next < base`, not `<=`) | P1 | fixed in 2af00fc — `test_an_unkeyed_upstream_reset_right_after_a_pass_is_delivered_in_full` |
| F9 | A keyed output moved to another store starts over with an index of its own; a consumer then keeps a key the move's first write dropped (seen when the consumer's first pass and the moved write run together) | P1 | fixed: a move sets the head's `base`, and a pass begun at or before it starts over — `test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers`, `test_a_key_a_moved_output_dropped_leaves_its_consumer` |
| F10 | A full pass (a reset) whose patterns take none of the upstream's keys is skipped without calling the producer, so the consumer never starts over: keys it held stay, though the upstream dropped them — also after a `full` run | P1 | fixed: a full pass always reaches the consumer (`architecture.md` §5) — `test_a_full_pass_that_takes_no_key_still_starts_over`, and the simulation's `exclude` change is back in its rules |
| F11 | A delta file a pending cleanup entry reads is deleted while an attempt that was handed the entry runs: the attempt cannot read it, the superseded objects it names leak, and the entry ends `stuck` | P2 | fixed in ffe6921 — `test_f11_a_cleanup_entrys_delta_outlives_the_attempt_reading_it`, `test_a_cleanup_entrys_delta_outlives_the_attempt_holding_it` |
| F12 | `copy` renamed to `mirror` and back over rolling deploys (engines overlapping): `mirror` ends holding a key `items` deleted while `mirror` was not served — its old index survives under a position already past the deletion | P2 | fixed: a name the manifest no longer declares holds no live state — `test_f12_a_rename_back_and_forth_over_rolling_deploys_keeps_up`, `test_a_name_removed_and_added_back_starts_over` |
| F13 | `items` moved from the table store to FileStore by a crash redeploy; the feed then removes `k11`: `copy` keeps it (F9's territory, the deletion after the move). Also with no crash: `k0`, `k11` committed; a takeover moves `items` from FileStore to the table store; the feed removes both; `copy` and `split`'s `odd` keep them, `checks` drops them | P1 | fixed by the reset rule (object-store-state.md §2): a move resets the output, its consumers re-read it from scratch — `tests/sim/test_replays.py::test_f13_a_key_removed_after_a_store_move_leaves_its_consumers`, `test_f13_a_key_removed_after_a_takeover_moved_its_upstream_leaves_its_consumers`, `tests/server/test_sim_found.py::test_a_key_a_moved_output_dropped_leaves_its_consumer` |
| F14 | An engine that created its fence finds a checkpoint at or past it and deletes the fence as a hole's, but a newer engine had read it and checkpointed past it: the old engine's next append lands in the freed slot, is acknowledged, and no replay sees it (the segment journal's spec, retired in cfdc723 with its code; three engines, or two with an unreadable checkpoint) | P1 | fixed: the hole test (`object-store-state.md` §10), run by the engine that created the fence — `tests/server/test_journal.py::test_a_fence_a_newer_engine_moved_past_stays` |
| F15 | A fence created in a hole stays readable until its engine deletes it: another opener replays it in place of the event cleanup deleted, and serves without that acknowledged event (the segment journal's spec, retired in cfdc723 with its code; three engines) | P1 | fixed: the hole test (`object-store-state.md` §10), run by every opener that reads a fence — `tests/server/test_journal.py::test_an_opener_never_replays_a_fence_created_in_a_hole` |
| F16 | A key index compaction moves level-0 files into an empty level 1 without merging them, so level 1 holds overlapping files and a read takes an older entry: `k0` written (level 1); rewritten (level 0); the output replaced by nothing (a level-0 tombstone); a background compaction that empties the index lands only after `k0`, `k1` are written again (level 0, level 1 now empty); `k1` removed (level 0); the next compaction "moves the deepest level down whole" — level 1 holds `{k0, k1}` and `{k1 removed}` — and `k1` reads live. In the simulation `items` kept a key the feed dropped, kept `k3` at an old value, or named an object already collected (`KeyIndex.compact`: `out_level > depth` also holds for level 0 at depth 0) | P1 | fixed: level 0 is always merged, never moved down whole (its files overlap); levels are gone since spans (`key-index-design.md`), and the case stays the explicit example of `tests/sdk/test_keys_index.py::test_any_workload_of_a_few_keys_matches_a_dict`, over span merges — and `tests/sim/test_replays.py::test_f16_*` |
| F17 | An output moved to another store and back loses keys when nothing moved its position in between: `items` commits `k10` on FileStore; a takeover moves it to the table store, where only a `keys=('k1', 'k10')` run writes (a fresh index there; a selection moves no position, which keeps FileStore's fingerprint); a takeover moves it back; the feed adds `k3`: the fingerprint matches, so `items` reads a delta, and the move starts its index over with `k3` alone — `k10` is gone. A move that starts the index over has to make the asset read a full pass | P1 | fixed by the reset rule (object-store-state.md §2): each move resets the output and takes its asset's positions, so it reads full passes — `tests/sim/test_replays.py::test_f17_an_output_moved_away_and_back_keeps_its_keys`, `tests/server/test_sim_found.py::test_a_move_and_back_with_no_write_between_resets` |
| F18 | PostgresStore's migration ledger (`public.solera_migrations`) is keyed by output name, not by the table a migration changes: two projects (or a staging and a production namespace) on one database each write `orders` in a schema of their own; migration `note` adds a column to the first's table; the second's `migrate` finds the ledger row, skips it and reports it applied — its table never gets the column | P2 | fixed: the ledger (`solera_migration_ledger`) and its lock are keyed by the schema-qualified table — `tests/sdk/test_postgres.py::test_a_migration_applies_to_each_schemas_table_of_one_name` |
| F19 | An asset removed while its attempt runs and added back before that attempt ends resumes its first life (F12's rule, across a live attempt): `copy` (version 1) has an attempt running; a deploy removes `copy`, which defers retiring its head and positions until the attempt settles; a deploy adds `copy` back (version 2) — its first life's head and positions are still there; the old attempt then succeeds and its commit installs into the new `copy`: rows written by version 1's code become version 2's head, under version 1's position (found by the execution spec's review; a failed or lost attempt installs nothing, and its run carries on with a fresh attempt of the new code) | P2 | fixed by the reset rule (object-store-state.md §2): a removal resets at its deploy, and an attempt launched before commits nothing — `tests/server/test_sim_found.py::test_a_name_removed_while_its_attempt_runs_and_added_back_starts_over`, `test_an_attempt_of_a_removed_and_readded_asset_stays_in_its_life` |
| F20 | The retry clock resubmits a retry run that cannot plan, at every tick: `checks` (per-key incremental over `items`, automated) has an operator's forced retry outstanding; `items` has no head (a store move reset it); the clock submits a retry run, which fails planning ("input 'items' has no head"), so the request is never done; the run's events wake the loop, which ticks again at once and submits it again — a hot loop, the journal and run history growing as fast as they can be written (in the simulation, at one virtual instant, until memory ran out) | P1 | fixed in b6d9968 (the retry clock waits for inputs with no head) — `tests/server/test_sim_found.py::test_the_retry_clock_waits_for_an_input_with_no_head` |
| F21 | A job added back takes its first life's commit (F19's case, for an asset with no output: the reset rule resets outputs, and a job has none): the job `seen` has an attempt running; a deploy removes `seen`, another adds it back; the attempt succeeds and its cursor and positions install into the new `seen` | P2 | fixed: removing an asset resets it, and an attempt launched before commits nothing of it (`reset_at` by asset) — `tests/server/test_sim_found.py::test_a_job_added_back_does_not_take_its_first_lifes_commit`, `test_a_job_removed_while_its_attempt_runs_and_added_back_starts_over` |
| F22 | An `OnChange` asset added back, or newly declared, is not built until its upstream next changes: `copy` (OnChange on `items`) is removed and added back; its automation comes back with nothing pending, so `copy` stays empty while `items` holds keys | P2 | fixed in aa0e7dd: any asset change (added, renamed, declaration changed, reset) leaves the asset due, and OnChange owes one firing per partition whose inputs have heads, decided once at the deploy — `tests/server/test_sim_found.py::test_an_onchange_asset_added_back_is_built` |
| F23 | The native `.kx` reader accepts a 10-byte varint with bits past 2^64 and drops them: a block entry whose generation is `ff` × 9, `7f` reads as 2^64 − 1 natively and as 2^70 − 1 in the Python reference. Only bytes no writer produces reach it (a hostile or corrupt file whose checksums match), but the two readers then disagree on a file both accept | P3 | fixed in ab0c346: a tenth byte above `0x01` is refused, natively and by the reference — `tests/sdk/test_keys_format.py::test_a_varint_past_64_bits_is_refused` |
| F24 | A checkpoint's move whose swap fails with an error that is no conflict (repeated 409s, or a lost answer whose read-back fails) is never written again: the engine keeps its pre-move ETag, so if the move landed, its next append is refused, the read-back finds neither its bytes nor its ETag, and a lone engine fences itself out (found by the spec worker's code check, and by a sweep, seed 951) | P2 | fixed: the move stays pending and is written again, the very same body, before anything else — `tests/server/test_journal.py::test_a_checkpoints_move_that_errors_is_written_again` |
| F25 | The engine id was 32 bits (`token_hex(4)`), while fencing needs no two processes to share one: a newer engine with the same id fencing an unchanged journal writes the old engine's bytes, and the old engine's `If-Match` still holds (TLC's `EngineId` calibration) | P3 | fixed: 64 bits (`token_hex(8)`), as object-store-state.md §10 says |
| F26 | A pool attempt is handed out before its launch is durable: `_launch` records `AttemptLaunched`, which puts the attempt in the model's pool at once, then waits for the flush, and `pool_work` offers from the model. In the simulation (spec worker; shrunk to 21 steps) engine A launched a `split` attempt and was replaced before its flush landed; a pool host had already taken the attempt, which ran and wrote `k2` into `odd`'s table on the fenced store (generation 189, a claim no journal holds) and finished. No engine knew the run or the attempt: nothing committed it, nothing owed a repair, and `odd`'s table kept a row its head never lists, visible to anyone reading the table directly | P1 | fixed: an attempt is `launching` from its record until `durable()` returns, and discovery offers none that is; one whose launch never lands is never offered — `tests/server/test_sim_found.py::test_a_pool_attempt_is_offered_only_once_its_launch_is_durable`. Checked alike: a placement's `launch` (local, inline, remote) runs only after `_launch` returns, the launch durable; the HTTP channel serves only a worker holding the attempt's token, which only the spec carries, and a worker learns of an attempt only through discovery or its placement's launch; the API's spec view drops the token; a sensor tick is memory-only and acts only through the engine's recorded decision |
| F27 | One API request wedges the engine: a run whose `config` holds an integer past 64 bits is accepted and journaled (events use the standard library's encoder), but the checkpoint's encoder (orjson) refuses it. From the first checkpoint due after it, every flush fails, `durable()` never returns, so every later write hangs, and nothing more reaches the journal (probe: 4 runs live, 0 after a replay). `Journal._seal` also empties the buffer before it encodes the snapshot | P1 | fixed: an event is encoded as checkpoints are, and refused (400) when that encoding cannot hold it exactly (integers past 64 bits, `inf`, `nan`, keys that are not strings) — at the one record boundary, so a worker's result is refused alike (its attempt fails); a seal encodes everything before it takes the buffer; a seal that fails stops the journal with its reason (`Stopped`): the engine is unavailable, not retrying — `tests/server/test_api_bodies.py::test_an_integer_past_64_bits_never_wedges_the_journal`, `tests/server/test_journal.py::test_an_event_no_checkpoint_can_hold_is_refused`, `::test_a_state_no_checkpoint_can_hold_stops_the_journal`, `tests/server/test_engine.py::test_a_cursor_the_journal_cannot_hold_fails_its_attempt`; found by the API body fuzzer |
| F28 | The two routes that read their body as raw JSON (`assets/{name}/keys:retry`, `cleanups:clear`) answer 500 for a body that is not an object, or a field of the wrong type (`{"output": []}`): they call `.get` and use the fields unchecked. Nothing is recorded | P3 | fixed: both routes read their body through a request model (`KeysRetryInput`, `CleanupsClearInput`), as the typed routes do, so a malformed body is a 422; the fuzzer now mutates their bodies too — `tests/server/test_api_bodies.py::test_a_malformed_body_on_a_raw_route_is_refused`; found by the API body fuzzer |
| F29 | The native readers that take a block without a limit (`lookup`, the merges, `decode_block`) inflate whatever its zlib stream holds: a 32 KB block inflates to 32 MiB, and a 64 MB one could ask for gigabytes. Writing such a block takes write access to the bucket; hardening: a block's decoded size bounded by what its file declares, failing fast past it | P3 | fixed: format v4 bounds what any block decodes to (16 MiB, keys expanded); readers refuse past it before inflating whole, writers close a block before it would exceed it — `tests/sdk/test_keys_format.py::test_a_block_that_inflates_past_its_bound_is_refused` (native and reference), `::test_a_writer_never_writes_a_block_past_the_bound` |
| F30 | A malformed control file wedged its partition (W38's control-file fuzzing): `State.attempt_result` read the file's fields unchecked, so one with no state, sealed with no result, empty or not JSON made the attempt's watcher raise `KeyError`; the engine adopted it again every tick (4,442 tracebacks in 90 s), the attempt never ended, and the partition never ran again. Any buggy or other-version worker could cause it | P2 | fixed: a control file is checked where it is read (`lifecycle.check_control`): a malformed one fails its attempt, retryably, ended as lost, with the reason in its error, and the engine ends the file on its version (write evidence `writing`: whether the gate was taken is unknown), so no worker writes after; a worker that finds one writes nothing. An attempt whose end itself fails is adopted again only after a back-off (0.5 s doubling to 60 s), never in a loop — `tests/server/test_control_file.py::test_a_malformed_control_file_fails_its_attempt_and_is_ended` (four bodies), `::test_an_attempt_that_cannot_be_ended_is_adopted_again_after_a_back_off` |
| F31 | A run of a per-key asset abandons a delta pass it began: sweep Z8 (pg), at convergence, `checks` (Each over `items`, batches of 2) gets an operator's forced retry run. Its first attempt delivers batch 1 of 2 of a delta pass on `items`' newest commit (k0, k11; `more`); its second is planned as the retry pass instead (`each.kind` retry), finds nothing to retry, seals an empty success, and the run ends with the delta pass at batch 1. Nothing resumes it and no automation fires again for that commit, so k3, in batch 2, never reaches `checks` | P1 | fixed: a rename bug. The glossary sweep (a05fd32) turned the key `changes` of the per-key retry and reconcile plans into `batch`, which `_each_commit` still read as `changes`: after a retry batch the run never saw the changes pending, and ended. The key is back, and a retry batch that ends its pass also goes on while the position owes a pass — `tests/server/test_each_passes.py::test_a_run_finishes_the_pass_it_began_before_it_ends` (deterministic, from the trace) |
| F32 | An `open` control file that names a worker wedges its partition (F30's class, past its fix; the control-file fuzzer): a worker never writes `open` (it swaps to `owned`), but `{"state": "open", "worker_id": "w"}` leaves the attempt waiting on that worker past any provisioning deadline, and the run never ends | P2 | fixed: `check_control` refuses a field a state cannot have (an `open` file names no worker), so the file is malformed and its attempt fails (F30's path); and the engine takes an owner only from `owned`, `writing` or `sealed`, so nothing but a worker's own swap makes it look started, and the provisioning deadline applies whatever the file says — `tests/server/test_control_file.py::test_an_open_control_file_naming_a_worker_fails_its_attempt` |
| F33 | A consumer of a source read current misses a key for good: `feed` commits `k1` (version 3); `items`' run for it fails twice; meanwhile the client changes the outside to `{k0, k3}` and its commit fails, so the source index still says `{k1}`; `items`' third attempt loads `k1` and finds nothing (the outside lacks it now), writes only the removals, and commits. The client then puts `k1` back at version 3: the same version is the same content (versions.md §2), so nothing redelivers it, and `items` never holds `k1` (sweep Z12) | P2 | fixed (Erwin's ruling): every keyed load (an incremental batch, a whole read by `Keys`, a per-key page) checks that each key it asked for came back (`check_loaded`); one missing fails the attempt with a retryable `SourceBehind`, "the source index says k1@3 but the source has no k1". Nothing is delivered and the position stays, so the third attempt no longer commits the removals alone: the asset's retry budget bounds the retries, with its back-off, then the attempt fails with that reason. The next run delivers `k1` once the source holds it again, at any version, or its next commit removes it — `tests/server/test_source_behind.py` (the bounded retry and its message; restored; removed; a per-key page) Also `tests/server/test_sim_found.py::test_a_key_a_current_read_missed_reaches_its_consumer_once_restored` (W38's, now passing) |
| F34 | A rename onto an earlier life's name loses that life's claim: `copy` renamed to `mirror`; `mirror`'s attempt runs; `copy` comes back without an alias (`mirror` removed, a new life); `copy`'s attempt runs; `copy` is renamed to `mirror` again. The rename re-keys `copy`'s claim over the earlier life's, still running (`claimed_partitions[(mirror, "")]`), so the engine no longer finds that attempt's claim, cannot settle it, and its run never ends (sweep Z11, unreplayable until the determinism fix) | P1 | fixed: a claim decides whether its attempt is live (`Model.claimed`), not the claim index, which gates dispatch for an asset's current life only. A removed asset's attempt in flight is an earlier life's (launched before the deploy that removed it: the commit's rule, `Model.earlier_life`): the reset drops it from the index, and so does a snapshot load, which had put it back. It still settles, its commit refused, and its task carries on in the new life (F21), behind whatever holds the name now. A rename never overwrites a claim still held: the simulation checks that every current-life claim is its partition's holder, and the journal check no longer counts a removed asset's attempts as holding the name — `tests/sim/test_replays.py::test_f34_a_rename_onto_an_earlier_lifes_name_keeps_its_attempt_settleable` |
| F35 | An each=True partition is stale with no stale key: `fchecks` (each=True over `feed`) runs with `keys=[k1]`; `feed` then removes `k2`, which `fchecks` never held. `stale_keys` lists nothing, as K43 says (`k2` is on neither side: no output unit), yet the partition status, which rolls up as "any key", reports `stale` (found by the Staleness machine at 300 examples; its CI run is now derandomized, as the simulation's is, so a rare history cannot make CI flaky) | P3 | fixed, K38's rule: for an each=True asset, the partition is stale for an input change exactly when one of its keys is. The partition's own records (a reset, an unrecorded or moved `seen`, the position behind) filter, cheaply, and the keys confirm: the per-key scan runs only when the filter says stale, so roll-ups stay cheap. Here the pass under way had made the filter say stale, while the per-key view, which counts a removal only where the output holds the key, said nothing — `tests/server/test_staleness.py::test_a_key_neither_side_holds_leaves_an_each_partition_fresh` |
| F36 | Collection deletes a delta file a launching attempt was handed: the engine prepares `checks`' next attempt, whose spec hands it pending delta entry 283 (it names level-4 delta file `…7JH0`, which compaction had let go of). While `_launch` writes the spec and the control file, another attempt acknowledges the entry; the launching attempt's claim names the entry's files only once `AttemptLaunched` is applied, so for that while `cleanup_reads()` holds nothing, and collection deletes the file (t=1041.70, between the spec at 1041.38 and the control file at 1041.98). The worker reads it at 1047.15: gone. Today the worker counts the entry unresolved and the other attempt has done it, so nothing is lost, but `cleanup_reads`' promise ("acknowledged by another meanwhile, it is still being read") breaks (sweep of the interleavings measurement, seed 11, under asyncio's order; it replays on a6db1a4 and stops at 2eb0e0b, which moved the timing, not the window) | P3 | fixed: the claim names the cleanup files its spec hands the attempt from `_prepare` on, as it does the delta log it reads (`reads`), not from `AttemptLaunched` on; `cleanup_reads()` holds them through the launch, so another attempt's acknowledgement meanwhile no longer lets collection take them — `tests/server/test_collection.py::test_a_delta_a_launching_attempt_was_handed_outlives_its_acknowledgement` |
| F37 | A full pass begun before a dep moved continues after it: `checks` has never run; keys=[k2] writes k2 (generation 10), which begins the full pass it owes (`began` 10, `seen` none). `knob` moves; engine and reference agree that k1 and k2 are stale. The default run calls `checks` on k1 alone, so k2 keeps what the old `knob` gave it, and the engine reports no stale key while `seen` is still none (found writing W22's K47 histories as examples, on fae165c) | P2 | fixed: the move point (the whole and dep inputs' latest commit) is taken whenever the partition record's `seen` is absent or differs, not only when it differs, so a full pass begun before the move starts over and redoes what it wrote under the old version; and a backstop: an each=True partition is never fresh while its record says no whole and dep versions, or old ones — `tests/server/test_staleness.py::test_a_keys_run_on_a_never_built_output_leaves_it_owing_a_full_pass` |
| F38 | A consumer of an output in a current-only store stalls for good: `items` lives in the table store (its current rows only). `copy` is in a delta pass over `items`' commit 1 (k1, k10, k2; batch 1 of 2 delivered) when `items`, its version bumped, rebuilds as commit 2 without k2. Batch 2 loads k2 by key; the store has none, so F33's check raises SourceBehind ("items: the source index says k2@106 but the source has no k2"), 28 times: the pass stays on commit 1, and the commit that removed k2 already exists, so no retry can pass. The budget runs out, no automation fires again, and `copy` and `checks` stay stale (sweep Z14, shrunk from 52 steps to 4; bisected to ba357e5, F33's fix) | P1 | fixed (the coordinator's ruling): a key missing from a current-only store is decided against the source's head index, not the pass's commit (`each.gone_since`; a delta pin carries the head). The head still names it: the store is behind its index, `SourceBehind`, retryable and bounded (F33). The head lacks it too: removed since, so the batch takes it as removed and the pass goes on. And a partition whose last outcome failed with nothing pending reports `failed`, its reasons kept, never quietly stale — `tests/sim/test_replays.py::test_f38_a_consumer_of_a_current_only_output_finishes_a_pass_its_upstream_outran`, `tests/server/test_source_behind.py::test_a_partition_whose_retries_ran_out_is_failing_not_stale` |
| F39 | A removed asset's attempt still owns its partition when a later life claims it: `seen`'s attempt 2 runs, its worker dead before its result; a redeploy removes `seen`, the next declares it again. Attempt 2 is an earlier life's, which since F34 holds no name, and nothing ended it: the new life's attempt 3 creates its control file at t=47 while attempt 2's is still owned, a request no behaviour of the attempt spec makes; attempt 2 is ended only at t=251 (trace validation of simulation example 2, seed 65535: `check-trace.py attempt`, "30 of 147 requests explained") | P2 | fixed (the coordinator's ruling: no exception): retiring with the asset ends the attempt as every other end does, `_end` first. The engine that adopts an attempt launched before its asset was removed fails it at once, retryable (`removed`), and its task carries on in the new life. Until it is ended it keeps its partition: dispatch holds a new-life task on it (`claim`), and the journal check counts it as holding the partition — `tests/sim/test_replays.py::test_f39_a_removed_assets_attempt_is_ended_before_its_partition_is_claimed_again` |
| F40 | After a takeover, the old engine's orphan collector deletes the new engine's span files: it runs every `ORPHAN_SECONDS` with the model it last knew and writes nothing to the journal first, so nothing tells it it is fenced. Engine A commits; B takes over, merges and publishes `m…-…kx`; A's `collect_orphans` lists `keys/`, finds the output its model does not name, and deletes it. B's index then names a file that is gone, and every read of that span fails; deleted before publication, B publishes a file that is gone (`Spans.tla`, calibrations `epoch`, `epoch-state`) | P1 | fixed in c4eb4f7 (found independently by review A17, its R1): each engine takes an epoch, one past its predecessor's, written by the journal swap that fences it; merge outputs are named `m{a}-{b}-{epoch}-{ULID}`, and a collector deletes an unreferenced output only if its epoch is at most its own, judging what is named and running after its listing — `tests/server/test_keys.py::test_f40_a_zombies_orphan_collector_spares_the_serving_engines_spans`, `test_a_fenced_engine_never_collects_its_successors_merge_outputs` |
| F41 | A consumer that read a removal keeps the key: `items` (keyed, incremental over `feed`, a source read as it is now; in the table store) goes through two rename takeovers, `seen` re-added while live, 2% errors and lost answers, and a redeploy; `feed` is then replaced by {k11}, then by {k1, k3}. After convergence `feed`'s index is {k1, k3} at its head, and `items`' position says it read through that commit, the one that removed k11; yet `items`' index still lists k11, written in the same pass as k1 and k3, and `items` reports itself fresh. A regression from 083ca73 (F38's fix): with it reverted, the same run converges. Found by the calibration run on ebaffaa, in asyncio's order; it shows with the simulation's changed timing (`checks` now a dependent of `knob`), its steps all older rules | P1 | no longer reproduces from acc65f3: its replay converges there (not bisected; 22 product commits since ebaffaa, K44's net delta and key-index step 3a among them; reverting 0f985de alone does not bring it back). Routed to the engine's seen-set rebuild (D126) to confirm — `tests/sim/test_replays.py::test_f41_a_consumer_that_read_a_removal_does_not_keep_the_key` (asyncio's order), an ordinary test |
| F42 | A full run never finishes its pass: two full runs of `copy` (`mode='full'`, every partition), with a commit of `feed` between them. The first finishes its two-batch pass. Every attempt of the second starts the pass over — batch 0, a new `began` — delivers batch 1 of 2 and succeeds, and the next starts over again. The task relaunches without end: over a thousand attempts in one run, a cleanup run each, until memory is gone (main's Hypothesis seed 5000, out of memory at 3 GB in both orders; shrunk from 44 steps to 3). On any store and any seed. The second run was submitted while the first ran: perhaps the rule that a full run starts a full pass compares the pass's `began` with the run's submission, which every pass it begins comes after | P1 | fixed by the observed set (D153, step 4.3): a full run resets on its task's first batch only, and its walk goes on from its progress — `tests/sim/test_replays.py::test_f42_a_full_run_finishes_its_pass` |
| F43 | Data a reader needs is deleted, then a run storm (W59's demo on FileStore, after about 15 minutes): `site_status` reads `site_events`, an append output, whole; its head spans commits 0–2,814, but commits 0–10 of some sites are gone ("commit 0 of site_events/alpha is gone"), and every upstream commit's OnChange runs it again: 7,500 failed runs in two hours. Root cause: two states shared one data root. The live demo and the console's Playwright and gate runs (fresh `SOLERA_STATE_URL`s, no data root of their own) all wrote the demo's FileStore beside its module; names carry no state, generations restart with each state, and each state's cleanups deleted what it took for its own — `site_events/delta/000000000000/` held the live commit (generation 224) beside fresh engines' 1791, 1899, 2203 | P1 | fixed: the demo's data lives under its own state's directory (`SOLERA_DATA_URL` the one variable, `file://` or `s3://`); a partition whose runs keep failing backs off its changes (60 s doubling to an hour, pending meanwhile) — `tests/test_demo_e2e.py::test_the_demo_keeps_its_data_beside_its_state`, `tests/server/test_engine.py::test_a_failing_partition_backs_off_its_changes`; a store root belongs to the namespace that first claims it, and another is refused (D167; `solera adopt-store` moves it) — `tests/sdk/test_filestore.py::test_a_root_belongs_to_the_namespace_that_first_claims_it`, `tests/server/test_engine.py::test_a_data_root_another_namespace_owns_is_refused`, `tests/server/test_cli.py::test_adopt_store_moves_a_root_once_its_namespace_is_retired` |
| F44 | A first run classed every key as updated (W59's demo: `file_index`, `{added: 0, updated: 2}` on each batch) | — | not an engine bug: the run reported was `file_index`'s second; its first, seconds earlier, delivered every key as added, and the second's `site_feed` rewrote every file at a new generation. Kept as regression tests: `tests/server/test_changes.py::test_a_first_run_delivers_every_key_as_added` |
| F45 | A merge drops an endpoint a batch in flight reads at (the simulation, seed 65535): a keyed batch planned its head over awaits and reserved it on its claim only after them; meanwhile commit 1 landed and upkeep planned a merge of spans [0] and [1], installed once the claim had reserved head 0 — dropping endpoint 1 | P1 | fixed: the claim reserves each keyed input's head before anything is awaited — `tests/sim/test_replays.py::test_a_batch_reserves_its_head_before_it_plans` |
