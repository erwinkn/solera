# The attempt lifecycle

Status: **built** (milestones 1, 2 and 5), but for sensors' host-side
resolution of bigger maps (§11.7). Where the build departs from the text, it says so
in place. In order: the records of §2 (`solera/lifecycle.py`), attempt
objects and ownership (§2–§4), the channel (§5: `solera_server/attempts.py`,
`solera_worker/channel.py`), heartbeats as evidence (§6), the two-phase
cancel (§7), the clocks (§8), Pool (§10), store
kinds (§9.5–§9.6; `stores.md`), PostgresStore generation
fencing (§9.7), FileStore / S3Store unique names and their collection
(§9.8), and sensors (§11: `solera_server/sensors.py`,
`solera_worker/sensors.py`).

It is the attempt protocol — attempt files, heartbeats, the pool, and the
write-safety rules — that `object-store-state.md` §8 and `architecture.md`
§10 summarize. It settles
D2–D4, and D1 as Erwin decided it after the review: heartbeats are evidence
only, and every store is exact: `immutable` or `fenced` (round-2
decision 2 dropped the `overwrite` kind, its grace and its holds).
Ticks are sensors (§11), which are not attempts at all.

It assumes the journal's fencing (`object-store-state.md` §10), and what
is built: create-only writes that recognize their own bytes
(`solera.objects.create`), results sealed once, durable retirement,
event-counter garbage pins, recorded placement handles, per-placement
"can't tell", a provisioning deadline, and a timeout that runs from the
worker's first report.

Its companions: `resolved-commits.md` (the resolver on this channel, and
the write phases of a keyed output), `per-key-processing.md` (batches,
failed keys, sensors' sources).

**This doc is the authority for four records the others use:** the cancel
record (§2.2), write-completion evidence (§2.3), the sensor snapshot
(§11.3) and accepted tick outcomes (§11.4). The key index entry's
generation and the delta's predecessor are named here (§9.8) and laid
out in bytes in `key-index-format.md`.

## 1. The shape in one paragraph

Durable facts live on the object store; signals go over HTTPS. The engine
writes an immutable **spec** and a **control file**, and authorizes the
launch in the journal; the worker that takes the control file first owns
the attempt, runs it and seals its **result** into the file. Every change
to the control file is a compare-and-swap, so the worker's moves and the
engine's end cannot both land. Heartbeats, live logs, cancel, resolve and pool
discovery are requests to the engine. None of them grants anything: a
heartbeat is evidence that a worker lives, never permission to write, and
the permission to write comes from the durable launch and, for stores that
can enforce it, from the store itself. So when the engine is down, workers
keep working and their results land on the object store; when a worker
falls silent, what the engine may do next depends on what its store can
guarantee (§9).

```
engine                                   object store                         worker
  claim partition, choose generation, pin
  PUT .spec  ──────────────────────────▶ runs/R/A.spec
  create .control (open) ──────────────▶ runs/R/A.control
  AttemptLaunched, durable
  placement.launch(A); AttemptPlaced ──────────────────────────────────────▶ boots, GET .spec
                                         runs/R/A.control ◀── swap open → owned
  ◀──────────────────────────────────────────────── POST attempts/A/start
  ◀──────────────────────────────────────────────── POST attempts/A/beat     every 10 s
                                                                              Store.acquire (fenced)
  ◀──────────────────────────────────────────────── POST attempts/A/resolve  (small keyed writes)
                                         keys/…/12-A.kx   ◀── deltas
                                         runs/R/A.control ◀── swap owned → writing (the gate; fenced stores)
                                         output data      ◀── store writes
                                         runs/R/A.log.000000…  ◀── log chunks
                                         runs/R/A.control ◀── swap → sealed, with the result
  ◀──────────────────────────────────────────────── POST attempts/A/finished (hint)
  GET .control, validate the result, AttemptFinished, durable
```

## 2. Objects and records

### 2.1 Attempt objects

All under `runs/{run}/`, all named after the attempt. Only the owner
writes anything but the spec and the control file's `open` and `ended`
(§4), so no name needs the worker token.

| Object | Written by | Mode | Meaning |
|---|---|---|---|
| `{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | what to run: today's `spec`, plus `engine` (HTTPS URL), `token` (§5.2), `generation` (§9.7) |
| `{attempt}.control` | the engine creates it before the launch; then the owner and the engine | created once, then only swapped (`If-Match`) | who owns the attempt, whether writing began and its intents, the sealed result, or the engine's end (§2.4) |
| `{attempt}.beat` | the owner, only while HTTP fails (§6) | overwritten | `{"worker", "seq", "timeline", "usage"}`: evidence the worker lives, never a decision |
| `{attempt}.log.{n:06d}` | worker | create-only, immutable | a gzip member of the log, flushed every 30 s or 1 MB; a short log has none (§13) |

Gone: the two-write `{attempt}.json`, the fence-reading `.beat` and its
done marker, the joined `.log`, chunk deletion, `AttemptClaimed`; with
the control file, `.worker`, `.writing` and `.result`.

**The result**, sealed into the control file, carries today's result,
plus the worker, the timeline, usage, the log index, and what is known of
the attempt's writes:

```json
{
  "worker": "k3v9q2",
  "status": "succeeded",
  "writes": "complete",
  "outputs": {"orders": {"ref": {"…": "…"}, "keys": {"files": [{"name": "000000000012-01J9…"}], "added": 3, "removed": 0, "exact": true}}},
  "delivered": {"uploads": {"after": null, "upserted": ["u-7"], "deleted": []}},
  "cursor": "token-42",
  "timeline": [{"type": "booted", "at": 1790074791.2}, {"type": "computing", "at": 1790074793.0}],
  "usage": {"cpu_seconds": 4.1, "peak_memory": 512000000},
  "log": {"chunks": [[0, 412, 1790074791.2], [1, 388, 1790074821.2]], "tail": "H4sIAAAA…", "lines": 812, "truncated": false}
}
```

- `status` is `succeeded`, `failed` or `canceled` (a drained cancel, §7).
- `writes` is the worker's write-completion evidence (§2.3).
- `cancel`, present when the worker sealed during a cancel, is the cancel
  record it acted on (§2.2).
- `log.chunks` lists `[n, lines, first timestamp]` per chunk; `log.tail`
  holds the lines written after the last chunk, gzipped, when they are
  under 64 KB (§13). Chunks are never joined: the console reads "the last
  200 lines" from the tail and the last few chunks.
- A chunk is sealed (its number, bytes and lines fixed) before it is
  written; lines logged meanwhile start the next. A write retried after an
  unknown outcome sends the same bytes, which the create-only write takes
  as its own. Lines the worker could not write at all are counted in
  `log.lost`: the log never keeps a result from being published.

### 2.2 The cancel record

What the engine decided to stop, latched, and what the worker acts on:

```json
{"phase": "requested", "reason": "user", "since": 184702}
```

- `phase`: `requested` (stop starting work, drain) or `forced` (stop now;
  a drain result is no longer accepted). It only advances.
- `reason`: `user` (an explicit cancel of the run), `timeout` (the
  attempt's timeout) or `provisioning` (no first report in time; no worker
  is running, so there is nothing to drain). The failed keys records
  interrupted keys by it (`per-key-processing.md` §5): `user` keys are
  dormant, `timeout` keys count a try and come due.
- **Precedence**: `user` over `timeout` over `provisioning`. A user cancel
  arriving during a timeout's drain changes the reason; a timeout during a
  user cancel's drain changes nothing.
- `since`: the event counter at which the engine latched it, so a
  restarted engine's re-derived cancel (§12) can be told from the first.
- **Pass**: every beat answer carries the current record (or `null`);
  the worker latches the strongest it has seen (§5.3).
- **In the result**: the worker copies the record it sealed with into
  its result. That record decides how its failure delta treats interrupted
  keys; a reason latched after the worker sealed does not rewrite it. The
  engine accepts a `canceled` result while the phase is `requested`, and
  refuses it once `forced`.

### 2.3 Write-completion evidence

Whether an ended attempt's writes can still land: `none`, `complete` or
`writing`. The release rules of §9.6 read nothing else.

| Source | Establishes |
|---|---|
| the worker, in its sealed result | `none` if it made no store call; `complete` if every `store.store()` call returned; `writing` if one raised, was cancelled or abandoned — **any store exception after the gate leaves it `writing`**: a client timeout may hide a backend that completed |
| the engine, ending an attempt without a result: it swaps the control file to `ended` | **from `open` or `owned`** → `none`: the worker never took the gate, and now never can |
| | **from `writing`** → `writing`, with the intents |
| | **finds `ended` by another engine** → what that engine recorded |
| | **finds `sealed`** → the result stands |
| | **cannot get an answer** (S3 unreachable) → not established: the engine retries, and treats the attempt as `writing` until it can |

Never from progress, silence or provider state: a worker can take the gate
and enter a store call before its next report. Attempts whose outputs are
all on `immutable` stores take no gate; their evidence is the result's, or
`writing`, which their release rule ignores (§9.6).

### 2.4 The control file

*Decided (K18) and built; checked in TLA+ (`spec/tla/Attempt.tla`,
`verification.md`, "Formal model: the attempt control file"), and each of
its calibrations played against the code (`tests/server/test_control_file.py`).*

Every attempt has one control file, `runs/{run}/{attempt}.control`. The
engine creates it before the launch. From then on it changes only by
`swap` (`object-store-state.md` §0), a write with `If-Match` on the
version its writer last read. It says who owns the attempt, whether
writing began (the gate) and with which intents, and holds the sealed
result or the engine's end. The spec stays a separate, immutable object.
Heartbeats stay out of the control file too (`.beat`, §6), so a report
never races a decision.

| State | Written by | From | When | The body adds |
|---|---|---|---|---|
| `open` | the engine, create-only | nothing | after the spec, before `AttemptLaunched` | the engine id |
| `owned` | the first worker | `open` | its first write (§4) | its worker id, host, pid |
| `writing` | the owner | `owned` | before its first mutation of a `fenced` store; only attempts with outputs on one take it (the gate) | the intents (§9.6) |
| `sealed` | the owner | `owned`, `writing` | once, at the end | the result (§2.1), with its write evidence (§2.3) and, if it took the gate, its intents |
| `ended` | the engine | `open`, `owned`, `writing` | it gives up: a forced cancel, a timeout, the provisioning deadline, a lost worker | `write`: `none` (from `open` or `owned`), or `writing` with the intents (from `writing`); the engine id |

**Nobody learns of an attempt before its launch is durable.** Nothing
that can act on the attempt learns of it until `AttemptLaunched` is
durable (§3, step 3): no placement launches it, no pool host is offered
it, and the channel serves only a worker holding its token, which only
the spec carries. An operator's view may show it before, marked
`launching`: an operator cannot act on it as a worker would. The engine may be fenced or
crash after creating the file `open` and before that event lands; the
attempt then goes with its memory, and the next engine never learns of it.
If a worker had been offered it, that worker would own the `open` file,
acquire, write, and seal, under an attempt that no engine ends, settles or
repairs: rows nobody committed (F26). Under the rule, no worker exists for
it, and its spec and `open` file wait for retention to delete them with
the run. (TLC: `OfferDurable` in `Attempt.tla`; without it, `NoOrphanWrite`
fails in 8 steps.)

`sealed` and `ended` are final: nothing changes them, and retention
deletes the file with its run. Every body names its writer, a worker id
or an engine id, so no two writes have the same bytes. `swap` relies on
that to resolve a lost answer.

**When a swap is refused**, its writer reads the file, then:

| Writer | Finds | Does |
|---|---|---|
| anyone | its own body | its write landed and the answer was lost: go on |
| a worker | another worker's `owned` | it lost the race: wait for the owner (§4), writing nothing |
| a worker | `ended` | stop and write nothing (exit 3); the result it was sealing is not taken |
| a worker | no file | stop. A worker never creates the control file. |
| the engine | `owned` or `writing` it had not read | end it again, on what it just read |
| the engine | `sealed` | the result stands: settle it (a drained cancel's too) |
| the engine | `ended` by another engine (a zombie) | settle from what that engine recorded |

**A worker paused for a week.** Attempt A of `orders:alpha` has generation
12.

1. The engine writes the spec, creates `A.control` (`open`), makes
   `AttemptLaunched` durable, and launches.
2. The worker reads the spec and swaps the file to `owned`. Then its VM
   freezes.
3. The worker falls silent. The engine swaps the file to `ended` (with
   `write: none`) and records `AttemptFinished`. The run fails, and
   nothing writes `orders:alpha` again that week.
4. A week later, retention deletes `runs/R/`, control file included.
   Nothing is kept, noted, or expired later.
5. The worker wakes, finishes computing, and acquires generation 12. No
   newer generation has acquired, so the store's fence accepts it. Then
   the worker swaps `owned` → `writing` on the version it last read. The
   file is gone, so the swap is refused. The worker reads nothing there
   and stops, writing nothing.

The fence alone cannot stop this worker, because nothing newer has
acquired the partition. With the old create-only gate, the worker would
have created `.writing` itself and written into `orders`. That is why
gates used to outlive their runs by `gate_days`.

**The worker finishes as the engine forces a cancel.** A's file reads
`writing`, at version v7, with intents `k1` to `k3`. The cancel's grace
runs out, and the engine swaps v7 → `ended` (`write: writing`, with the
intents). In the same instant the worker's writes return, and it swaps
v7 → `sealed` with its result. Exactly one of the two lands.

- **The worker's lands.** The engine's swap is refused; it reads
  `sealed`. The result stands, as for any result published before a
  force, and the engine settles it.
- **The engine's lands.** The worker's swap is refused; it reads `ended`
  and exits 3. `AttemptFinished` records `write: writing`, with `k1` to `k3`
  owed a repair. The next attempt reads them back after it acquires
  (§9.6).

Under the separate-object design, the engine read `.result` and then
created the gate, and the worker published `.result` independently, so
the two decisions could cross.

**What it removes:** keeping gates when a run is deleted, the notes under
`control/gates/`, the scan that expired them, `gate_days`, the `closed`
gate, the `.worker` object, and the separate `.writing` and `.result`
objects.

**What stays:**
- the immutable spec;
- the two-phase cancel: requested in a beat's answer (§7), forced by
  `ended`;
- generation fencing inside the store's transactions (§9.7), which still
  stops a worker that marked `writing` and paused past a later
  acquisition;
- heartbeats, in `.beat`.

## 3. One attempt, step by step

Example: `orders:alpha` writes a three-key `Patch` to PostgresStore, on
ECS, engine up throughout.

1. **Claim the partition** (memory, §3.1) and pin inputs. The claim's
   event counter becomes the attempt's **generation** (§9.7): chosen now,
   before the spec.
2. **Write the spec**: `PUT runs/R/A.spec` create-only; then create the
   control file `runs/R/A.control`, `open` (§2.4).
3. **Authorize**: record `AttemptLaunched`, await `durable()`. A replaced
   engine fails here and launches nothing. This is the durable launch
   authorization every later step rests on: nothing outside the engine
   learns of the attempt before it, neither a placement nor a pool host
   polling for work (§2.4).
4. **Launch**: `placement.launch(stage)` with the provider's name for `A`
   (ECS `clientToken`, Kubernetes job name); record `AttemptPlaced`.
5. **Own**: the worker boots, reads the spec, generates a worker id
   (`k3v9q2`) and swaps `A.control` from `open` to `owned`. If another
   worker's `owned` is there, it lost: §4. If the file is gone or
   `ended`, it stops.
6. **Start**: `POST attempts/A/start`. The engine binds the worker
   (§5.3). The attempt now runs: its `timeout` starts (§8).
7. **Compute**, beating every 10 s. Logs go live over HTTP and durably as
   chunks.
8. **Acquire** (fenced stores): `Store.acquire(context)` takes generation
   `g` for the output's write domain (§9.7) — after computing, before any
   repair read or mutation.
9. **Plan** each keyed output (`resolved-commits.md` §3): repair reads
   (and the full reconciliation of an unknown-writes intent, §9.6),
   resolve (`POST attempts/A/resolve`, or locally), upload the delta file.
10. **Gate**: swap `A.control` from `owned` to `writing`, with the
    intents (fenced stores only). Refused, with `ended` or no file there:
    the engine ended the attempt, so write nothing (§2.4).
11. **Write**: the store's transactions check generation `g` (§9.7).
12. **Seal**: build the result once and swap `A.control` to `sealed`
    with it, retried with the same bytes (a refused retry that reads its
    own body back landed); `POST attempts/A/finished` as a hint; exit.
    Refused, with `ended` there: the engine ended the attempt first, and
    the result is not taken (exit 3).
13. **Settle**: the engine reads `A.control`, checks the result's worker
    against the owner, validates it, records `AttemptFinished` and makes
    it durable. One journal decision installs the output deltas, the failure
    index delta (`per-key-processing.md` §9), the cursor and the
    positions together.

A failed attempt follows the same path with `status: failed`. Canceling
and timing out are §7.

### 3.1 Claims

A **claim** holds one asset partition for one attempt, from dispatch to
settlement: at most one attempt runs an asset partition at a time, and
the claim is what everything that must not run under, or delete from
under, an attempt reads. This is the one place claims are explained.

**What a claim is.** A record per task (`Model.claims`): the attempt, its
**generation** (the event counter when it was claimed: the attempt's
fence, §9.7, and its reader pin), and what it holds for others. An index,
`claimed_partitions`, maps (asset, partition) to the attempt holding it:
the dispatch gate. A claim is **memory only** until `AttemptLaunched` is
durable; nothing outside the engine learns of the attempt before then
(F26), so an engine replaced before it leaves nothing to clean up, and
its successor dispatches the task again. From `AttemptLaunched` on the
claim is durable: rebuilt from the task's launch record on replay, and
adopted by a restarted engine (§12).

**What it holds for others.**

- **Its reads** (`reads`: `(output, partition, first, end)` per keyed
  incremental input): the delta log it reads, from `first`, and the head
  + 1 its plan was cut at, `end`: where it may move the position, a
  retry pass that may cover what is left included. Log truncation and
  index merges (`Model.endpoints`) keep them. Set when `_prepare` plans,
  before the launch. A plan with no position to move (a keys= selection
  while a pattern change decides membership) holds none.
- **Its cleanups** (`cleanups`): the delta files of the pending cleanup
  entries its spec hands it, also set at `_prepare`: collection keeps
  them while the attempt runs, even once another attempt acknowledged the
  entry (F36).
- **Its reader pin**: by its generation, over the index prefixes it reads
  and writes (`prefixes`), or every one while it is still preparing:
  collection deletes nothing such a reader may still read (§9.8).

**Lives.** A name removed and declared again is another asset (F12). An
attempt launched before the deploy that removed its asset is an
**earlier life's** (`Model.earlier_life`, the commit's own rule): it holds
no entry in `claimed_partitions`, at the reset and when a snapshot is
loaded, so no name of the new life is kept behind it. It is ended at once,
as any attempt is (`_end` first, §2.3): the engine that adopts it after
the deploy fails it, retryable, and its task carries on in the new life
(F21). Until then it still holds its partition: dispatch keeps a task of
the new life off it, so two attempts never own one partition's files
(F39). Whether an attempt is live is the claim's own record
(`Model.claimed`), never the index (F34).

**Why a due task waits.** Dispatch claims due tasks in order and records,
when it changes, why one is held (`TasksHeld`, shown as "held: reason
(name)"): `claim` (another attempt holds its partition: one of the
current life, or an earlier life's not yet ended), `concurrency` (its asset's `concurrency=` partitions are
claimed), `merges` (a key index it writes — an output's, or a per-key asset's failure index — is too far behind on merges),
`engine` (the engine's own slots are full), `executor` (its executor's
limit), `invalid` (its placement cannot be built).

**How a claim ends.** With the attempt's `AttemptFinished` (settled,
failed, canceled, skipped, or lost: its control file is ended first,
§2.4), which releases the claim and its index entry. An attempt whose
claim another engine released (this one was replaced) returns without
deciding anything: the successor owns it.

**A claim from dispatch to settlement.** `file_index:alpha` (per-key,
over `site_files`, on a `Pool`):

1. The hourly run's task is due; nothing holds `file_index:alpha`.
   Dispatch claims it for attempt A at event 120: generation 120, memory
   only, `claimed_partitions[(file_index, alpha)] = A`.
2. `_prepare` pins `site_files:alpha` at commit 56..60 and hands A the
   pending cleanup entry 118.0 (a delta file `…/000000000055-B.0000`).
   The claim now holds `reads = [(site_files, alpha, 56, 61)]` and
   `cleanups = {…/000000000055-B.0000}`: truncation stops below 56, and
   collection keeps that file even if B's own cleanup acknowledges 118.0
   now.
3. The spec and the control file (`open`) are written; `AttemptLaunched`
   is recorded and awaited. Only now is A offered to the `ingest` pool.
4. Workers `w1` and `w2` long-poll; both are offered A; `w1` swaps the
   control file to `owned` first and runs it, `w2` loses the swap and
   polls again (§10).
5. `w1` seals its result; the engine commits it (`AttemptFinished`,
   succeeded): the claim, its reads, cleanups and pin go, and the
   partition is free for the next due task.

**An earlier life.** `copy` is renamed `mirror`; `mirror`'s attempt M
runs. A deploy removes `mirror` (`copy` back, without an alias): M is now
an earlier life's, and its index entry goes. `copy`'s attempt C runs;
`copy` is renamed `mirror` again: C's claim moves to `(mirror, "")`,
which M no longer holds. M settles, its commit refused; C is untouched.

**A held due task.** `report` has `concurrency=2` and three partitions
due. Dispatch claims `a` and `b`; `c` is held ("held: concurrency
(report)") and claimed as soon as one of them settles.

**What checks it.** The simulation asserts one attempt per partition, in
the journal and in the serving engine's memory, and that every
current-life claim is its partition's holder (`tests/sim/oracle.py`,
`two_attempts_at_once`; `tests/sim/machine.py`,
`one_attempt_per_partition`); `spec/tla/Execution.tla` checks
`OneAttemptPerPartition`, `spec/tla/Positions.tla` claims reads (`Claim`).

A sensor's **tick claim** (§11.4) is a different thing sharing the word:
one in-flight tick per sensor, memory only, with a reader pin; no
partition, attempt or control file.

## 4. Workers and duplicates

A placement may run one attempt twice: Kubernetes may start a second pod
for a Job even with `parallelism: 1`, and an engine restart can `resume`
(relaunch by name) while a launch whose answer was lost is starting. The
control file decides:

- **The first swap from `open` to `owned` wins.** It is the first thing a
  worker does after reading the spec: before computing, logging, resolving
  or uploading anything.
- **An ambiguous swap** (response lost) is resolved by reading the file
  back: its own worker id in `owned` is its own win.
- **The engine binds one worker per attempt** (§5.3) and accepts
  beats, resolves and results only from it.

**A losing worker must not end the attempt by exiting.** If it exited
at once with code 0, the engine would follow the handle it relaunched,
see the exit, find no result, and fail the attempt while the owner is
still computing; and a Kubernetes Job is complete as soon as one pod
succeeds. So:

- **The loser waits for the owner to finish**: every 30 s (rare, since
  duplicates are) it reads `A.control` for `sealed` or `ended`, and asks
  the engine with a beat. It exits 0 on either, on a file gone, or on
  `409 ended`; `409 not_owner`
  means the attempt is live under its owner, and it waits on. It never
  writes anything, not even a log.
- **The engine treats a provider exit as the owner's only if the owner has
  also fallen silent.** On an exit without a result, the engine checks the
  bound worker's last report: one within three beat intervals means
  the exit was another worker's, so the engine keeps waiting on the
  owner's reports (and drops the handle, which named a duplicate). With no
  worker bound yet, it reads `A.control` first. (Built: only reports
  received over the channel count here — a `.beat` read now may have been
  written before the exit — so a worker reporting only through `.beat`
  has its duplicate's exit taken for its own; its control file, `ended`,
  then stops it.)

Delayed duplicates, long after the attempt ended, are §9.5.

## 5. The HTTP channel

### 5.1 Endpoints

All under `/api/projects/{p}/`, over HTTPS.

| Route | Body → answer | From |
|---|---|---|
| `POST attempts/{a}/start` | `{worker, host, pid}` → `{cancel}` · `409 {reason}` | the owner, once |
| `POST attempts/{a}/beat` | `{worker, seq, events[], usage, progress?}` → `{cancel}` · `409` | worker, every 10 s |
| `POST attempts/{a}/logs` | `{worker, offset, lines[]}` → `{offset}` | worker, every 1 s while it logs |
| `POST attempts/{a}/resolve` | binary, versioned: `resolved-commits.md` §4 | worker, small keyed writes |
| `POST attempts/{a}/finished` | `{worker}` → `204` | worker, after sealing its result |
| `GET pools/{pool}/work?wait=30` | capacity → `[stage]` | pool workers (§10) |
| `GET sensors/next?wait=30` | deploy → `[{tick, sensor, cursor, snapshot}]` | sensor workers (§11) |
| `POST sensors/{s}/ticks/{t}` | the tick's outcome (§11.4); key maps in the resolver's framing → `{runs}` · `409` | sensor workers |

`cancel` is `null` or the cancel record (§2.2). `events` are timeline
events (`loaded`, `mark`, per-key outcomes); `progress` is free-form for
the console. Live logs and events go to an in-memory ring per attempt that
the console tails; the durable copy is the chunks and the result.

Gone: `POST /api/workers/register`, `/api/tasks/claim`,
`/api/tasks/{id}/renew`, `/api/tasks/{id}/complete`.

### 5.2 Authentication and bootstrap

- **Bootstrap.** A placement hands the worker the stage — `attempt`, `run`,
  `objects` — as today. The worker reads the spec with the environment's
  own object-store credentials; the spec gives it the engine's URL and its
  token. Pool workers get the stage from discovery (§10). Sensor workers
  need no spec: a remote host is configured with its pool token, and the
  local host gets a token for the engine's own pool in its environment
  when the engine starts it (§11).
- **Attempt token.** `HMAC(engine_secret, attempt)`, in the spec. Every
  `attempts/{a}/…` call carries it; the engine verifies it statelessly and
  only for that attempt's routes. Whoever can read the spec can already
  write the attempt's result, so the token adds no trust; it keeps
  credentials out of provider consoles, which carry only the stage.
- **The secret is stable across restarts**: the engine creates
  `control/engine-secret` once, create-only, and every later engine reads
  it, so tokens in specs written before a restart stay valid.
- **Pool token.** `pools/{pool}/work` takes a per-pool secret configured
  on the server and on that pool's workers. It reveals which attempts
  wait; owning one still takes the object store. (Built: one
  `SOLERA_POOL_TOKEN` for every pool, or the admin token.)
- The admin API keeps its own token; a worker's token reaches nothing else.
- The engine's URL is a stable HTTPS name (load balancer or DNS), so an
  engine restarted on another host is the same URL.

### 5.3 Rules for every request

Every route is idempotent and safe to retry, and nothing the worker sends
over HTTP is the only copy of a fact:

1. **Binding.** The first `start` binds its worker: only the owner sends
   `start`, since a loser knows it lost (§4). A request from any other
   worker id makes the engine read `A.control` (one GET), and the owner
   it names wins; the other gets `409 not_owner`. The binding lives in
   memory; after a restart the engine rebuilds it from `A.control` on the
   first request, never from the request itself.
2. **Sequence numbers.** Beats carry `seq`, logs carry the line `offset`
   of their first line; the engine keeps the highest per worker and
   drops what it has seen. A retried log batch is not shown twice; a gap
   (a batch lost for good) is accepted, since the chunks hold every line.
3. **Cancel is latched.** Once the engine has answered a cancel record
   (§2.2), every later answer carries that record or a stronger one (a
   later phase, a reason of higher precedence); the worker keeps the
   strongest it has seen. A late or reordered response never weakens it.
4. **Ended means ended.** For an attempt that has an `AttemptFinished`,
   every route answers `409 ended`; a worker receiving it stops sending
   (and, if it has not sealed a result, stops: §7).
5. **No answer grants anything.** A `200` to a beat is not permission to
   write; the worker writes on its launch authorization and its store's
   rules (§9), whether or not the engine answers.

### 5.4 When the engine is down

| Signal | With the engine | Engine down |
|---|---|---|
| ownership | `A.control` swapped to `owned`, then `start` | `A.control` swapped to `owned`; `start` retried in the background |
| heartbeat | HTTP every 10 s | `.beat` overwritten every 20 s (§6) |
| live logs, events | HTTP | dropped; the chunks hold the lines |
| resolve | HTTP | local resolve (`resolved-commits.md` §6) |
| cancel | the beat's answer | none arrives; whoever cancels does so on the next engine, which reaches the worker on its next beat |
| result | sealed into `A.control`, + `finished` | sealed into `A.control` |
| pool discovery | long-poll | none: nothing new is scheduled anyway |

Requests are retried with jittered backoff (1 s → 30 s). A worker never
fails because the engine is unreachable: it runs on its durable launch
authorization, and its writes are governed by its stores (§9.6), which
need no live engine. The next engine replays the journal, adopts every
launched attempt, rebuilds bindings from the control files (§5.3), and
settles each from its sealed result when it is there.

## 6. Heartbeats: evidence, never permission

Proposal for Erwin's question, decided: **HTTP heartbeats every 10 s; the
`.beat` object only while HTTP fails; liveness is evidence only.**

- The worker beats over HTTP every 10 s. After two failed beats it also
  overwrites `.beat` with its `seq` and timeline every 20 s until a beat
  succeeds again, and reads its control file each time: `ended` is the
  one cancel that reaches a worker the engine cannot answer. `.beat` is
  separate from the control file so a report never races a decision.
- The engine reads `.beat` only for attempts whose HTTP beats stopped,
  so a steady-state heartbeat costs no object-store request.
- A worker is **silent** when the engine has seen neither a beat nor a
  change of `.beat` for three intervals (30 s over HTTP, 60 s by
  `.beat`), measured on the engine's monotonic clock from when it
  received or observed the last report.
- Silence, a provider exit and a provider's "can't tell" are all
  **evidence**: they decide when the engine stops waiting and settles the
  attempt as lost. They never decide whether the attempt's writes may
  still land: that is the store kind's rule (§9.6).

Costs: per attempt-minute, 6 HTTP requests and no object request, against
today's 2 PUTs and 2 GETs (beat and fence read); during an engine outage,
3 PUTs per attempt-minute.

## 7. Settling, canceling, timing out

**Settling.** The engine settles when the first of these happens:

| Trigger | Engine |
|---|---|
| `finished`, or a provider exit with the owner silent (§4) | GET `A.control`; if sealed, validate the result and commit or fail it |
| no sealed result after that exit; or the owner silent with no handle | the attempt is lost: end it (`ended`, §2.4) under the release rule of its store kind (§9.6) |
| a cancel; the `timeout`; the provisioning deadline (§8) | the two phases below |

**Cancel is two phases,** carried by the cancel record (§2.2). A user
cancel and a timeout share them; the record's `reason` decides what
happens to the work left undone.

1. **Cancel requested.** The engine latches `{phase: requested, reason}`
   and answers it to the next beat (≤ 10 s). The worker stops starting new
   work: a per-key batch stops scheduling keys and cancels calls in flight
   (`per-key-processing.md` §5); a plain asset's producer is cancelled. Then
   it **drains**, within `cancel_grace` (60 s by default, per asset):
   - work that finished is written and published — for a per-key batch, the
     finished keys' outputs plus the interrupted holes in its failure
     index — as one result with `status: canceled`, which the engine
     commits as one journal decision (outputs, failure delta, position
     past the whole batch);
   - a plain asset that had not reached its writes publishes `canceled`
     with no outputs and `write: none`; one that had taken the gate
     completes its writes and publishes them, and the commit stands, as
     today;
   - either way the result carries the cancel record it sealed with.

   Result submission stays authorized throughout: the attempt is live
   until the engine records its end.
2. **Forced abort,** after `cancel_grace` without a result. The engine
   advances the record to `forced`, cancels at the provider, and ends the
   attempt: it swaps `A.control` to `ended` first and classifies the
   writes from what it swapped (§2.3); a result sealed before that stands.
   From then on a drain result is refused (its swap fails on `ended`; the
   channel answers `409 ended`) and its objects are garbage.
   `provisioning` skips phase 1: no worker reported, so there is nothing
   to drain.

**What is left undone.**

- **An explicit cancel does not resume itself.** Keys interrupted under
  reason `user` are recorded `canceled` and are not due: they
  run again only when a later run is requested for them (a retry, a new
  change of the key, a forced retry). A run canceled is a run the user
  wanted stopped.
- **Timeouts are bounded.** Keys interrupted under reason `timeout` are due after
  the asset's retry backoff, and each timeout interruption counts as a try
  (`tries` + 1): a key whose processing always outlives the timeout ends
  `failed` after the retry limit instead of cycling forever. An attempt
  timeout is retryable as today, within `retries`.

## 8. Provisioning and the runtime clock

From launch until the worker first reports, an attempt is provisioning:
an image pulling, a task waiting for capacity, a pool attempt waiting for
a worker. It has its own deadline, set per executor:

```python
gpu = AWSECS("gpu", cluster="ml", region="us-east-1", provision="20m")
etl = K8sJob("etl", cluster="prod", namespace="data")      # 10 min, the default
ingest = Pool("ingest")                                    # none: wait for a worker
ingest = Pool("ingest", provision="1h")                    # or give up after an hour
```

`provision` goes into the executor's environment in the manifest; every
placement of the executor inherits it. Past it, the attempt is aborted
like a timeout (`reason: provisioning`).

**The runtime clock** (the asset's `timeout`) starts at the first evidence
that the worker runs: `start`, a beat, or — with HTTP down — the engine
observing `.beat`. A 20-minute image pull does not eat a 30-second
timeout. Built today: the first report starts it.

**Adopted attempts.** Neither clock's start is persisted. An adopted
attempt still provisioning keeps what the launching engine's clock says is
left of its allowance, but never less than three heartbeats nor more than
all of it; one already running gets its whole timeout again from when the
new engine first hears from it. A restart can stretch an attempt by one
timeout, never cut it short. Built.

## 9. Writes the engine gave up on (D1)

**Decided** (Erwin): no lease protocol, and every store is exact. A store
writes only names no other attempt uses (`immutable`), or every write of
its checks the attempt's generation (`fenced`); there is no third kind.
The contract an implementer follows, its invariants and the scenarios that
check them are in `stores.md`.

### 9.1 The hazard

An attempt W1 is believed finished (failed, timed out, canceled, lost),
its partition is released, and W2 commits. Then a write of W1's lands.

| Store | W1's late write | Damage |
|---|---|---|
| keyed, one object or row per key | key `k` as W1 wrote it | `k` regresses: the index says W2 wrote it, the store holds W1's rows |
| keyed, W1 removes `k` | deletes W2's `k` | a key the index lists is missing |
| a value | the whole value | regresses to W1's |
| unkeyed incremental | commit `n`, which W2 also wrote (retries reuse commit numbers) | W2's commit replaced by W1's |

Only a write landing after a newer commit does damage, and only on a
store that writes in place — which is why such a store must be `fenced`.

How W1 can still write after the engine gave up on it: it lost contact but
runs on; it paused (GC, VM migration); a request it issued is still in
flight or queued at the backend (a `COMMIT` waiting on a lock); a store
call raised on the client while the backend completed it (a timeout); a
duplicate worker (§4, §9.5); user code writing outside the store
contract.

### 9.2 What others do

**Orchestrators detect and retry; none fences.** Airflow fails or retries
task instances whose heartbeat timed out
([tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html));
Airflow 3's supervisor kills its task when the API server answers `409
not_running` ([apache/airflow#48719](https://github.com/apache/airflow/issues/48719)).
Dagster's run monitoring marks runs failed or resumes them
([run monitoring](https://docs.dagster.io/deployment/execution/run-monitoring)).
Prefect marks runs Crashed after missed heartbeats
([zombie flows](https://docs.prefect.io/v3/advanced/detect-zombie-flows)).
Temporal retries activities and states they "may be executed multiple
times and may even partially complete more than once"
([activity definition](https://docs.temporal.io/activity-definition)).
dbt relies on warehouse transactions and asks users not to run one model
concurrently
([dbt forum](https://discourse.getdbt.com/t/multiple-dbt-runs-on-same-model-but-different-time-range-create-same-tmp-table/10221)).

**Storage systems fence.** Iceberg and Delta write immutable files and
publish them by one atomic commit; abandoned files are orphans, collected
behind a safety window
([orphan cleanup](https://www.dremio.com/blog/apache-iceberg-orphan-file-cleanup/),
[Delta storage](https://docs.delta.io/delta-storage/)); Hadoop's S3A
committers do the same for speculative attempts
([committer architecture](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committer_architecture.html)).
Kafka rejects writes from an older producer epoch
([Confluent](https://www.confluent.io/blog/transactions-apache-kafka/)),
HDFS JournalNodes from an older NameNode epoch
([QJM HA](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html)),
and Kleppmann's fencing tokens are checked by the storage
([How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)).
Leases with a delay — Chubby's lock-delay, which the paper itself calls
imperfect ([Chubby §2.4](https://research.google.com/archive/chubby-osdi06.pdf)) —
are a mitigation for resources that can check nothing.

### 9.3 The contracts considered

Not a ladder of safety levels: each is a different contract.

| Contract | What it guarantees | Chosen for |
|---|---|---|
| Detect and retry | at-least-once execution; late writes possible | **dropped**: a write in flight past any wait can still land after the next commit |
| Hold on uncertainty | no late write can follow a release, at the price of partitions blocked until completion is established | **dropped** with it: an approximation of what a store can guarantee itself |
| Self-fencing leases | risk reduction under timing and authority assumptions | **rejected**: the review showed it fails without exceeding its own margins (a superseded engine still granting renewals; a failed result bypassing the drain; renewals by `.worker` defeating cancel) |
| Generation checked by the store | exact exclusion of older writers, when acquisition precedes reads | PostgresStore (§9.7); user stores declaring `fenced` |
| Immutable outputs | stale writes are unreferenced garbage; pinned reads | FileStore, S3Store (§9.8); user stores declaring `immutable` |

### 9.4 The decision in one paragraph

A store declares how it writes: `immutable` or `fenced`; registration
refuses anything else, and a store that does not implement `cleanup` or
`acquire`, respectively. FileStore and S3Store are `immutable`,
PostgresStore is `fenced`, and a SQL store gets fencing in one line
(`solera.fencing.fence`). Partitions are released as soon as the engine ends
an attempt, whatever happened to its worker, and workers keep writing
through an engine outage: a writer the engine gave up on can no longer
change what a newer one committed.

### 9.5 Knowing whether writes completed

What the next attempt must repair reads one thing: the attempt's
write-completion evidence, `none`, `complete` or `writing`, defined in
§2.3. Two of its
rules carry the weight here. **Any store exception after the gate leaves
it `writing`**: a client that timed out after one second may see its request
finish at the backend five seconds later, and a store author cannot be
asked to tell the two apart; so a failed result after the gate leaves its
intents for the next attempt to repair. And **the engine classifies an
attempt without a result from its gate**, never from progress or
silence.

### 9.6 Store kinds

```python
class MyStore(Store):
    writes = "immutable"   # writes only names nothing committed references; implements cleanup()
    writes = "fenced"      # implements acquire() and keys(); every write checks the generation atomically
```

| Kind | Gate and intents | Repair | Partition released when the engine ends the attempt | A read sees |
|---|---|---|---|---|
| `immutable` | none | none: abandoned writes are unreferenced | at once | the pinned generation, exactly |
| `fenced` | gate with intents (repair, and the unknown-writes intent of an opaque write, below) | after `acquire`, by presence (`versions.md` §5) | at once: the next attempt's acquisition fences the old writer | current rows |

**Repair by presence.** The next attempt asks the store which of the
dead writer's intended keys it holds (`keys(ref, among)`, never a value):
a key present takes the repairing attempt's generation, so it counts as
changed; one absent and live in the index gets a tombstone; one absent
and not live, nothing. A key the repairing attempt writes or removes
itself ends as it says. The repair always runs its write transaction,
even with no rows of its own, so the partition reads as written by the
repair, not by the dead writer no commit has (`versions.md` §5).

```
index         k absent (a new key)
attempt g12   inserts k, dies after its gate; the insert did or did not land
attempt g15   k present → k@g15         k absent → nothing
```

**Reads.** Only an immutable store can return a pinned generation after a
newer one committed: its names are never reused. A fenced store keeps
one copy, so its loads read current rows: a run may read two
outputs at different moments, and a row changed since its pin is read in
its newer form and delivered again with its own change, a harmless repeat
(`architecture.md` §3, "What a read sees"). Fencing makes writes safe; it
does not make reads repeatable. No setting changes this.

**Unknown writes** (opaque). An opaque write's keys (`Opaque`, a value
its store reads itself: Postgres's `Sql`) are known only from the key
map the store reports after it committed, so its gate records an
intent with no key list: *unknown writes*. If its attempt dies after the
gate, the next attempt cannot repair by reading named keys — there are
none. For example, the statement deletes `a` and inserts `b`, commits, and
the worker dies before reporting its map; the next attempt patches `c`.
So, after acquisition (§9.7), an unknown-writes intent is settled only by
reading **every key the partition holds** (`keys(ref, None)`) and replacing
the index with them, the attempt's own patch laid over: every key the
store holds is written at the new generation, and live keys it lacks are
deleted. Consumers take everything again. A full replacement may instead rewrite the whole partition,
which settles it too. `resolved-commits.md` §3 carries the same branch for
the resolver's write phases.

**Failure deltas do not join the gate's intents** (the question of
`resolved-commits.md` §14.4). Intents name store writes a dead attempt may
have half-done, so that the next attempt can read them back. A failure
delta is a key-index file, engine metadata: it takes effect only through
the commit that names it, and an uncommitted one is cleaned up with the
attempt like any delta file. There is nothing to repair.

### 9.7 PostgresStore: generation fencing

Internal to the store; the engine supplies one number.

- **The generation** is the event counter (`applied`) of the attempt's
  claim, carried on `AttemptLaunched` as `pin` today and written into the
  spec. It is chosen before the spec and needs no allocation. A
  generation reaches the database only once its `AttemptLaunched` is
  durable, and that event itself moves `applied` past it: every later
  claim on the partition — by this engine, or a restarted one replaying the
  event — gets a larger one. (A claim dropped before launch may share its
  number with the next claim; it never wrote.)
- **The write domain** is the output's table and the partition,
  keyed by the table's OID, which survives `ALTER TABLE … RENAME`: an
  output renamed under an alias keeps its fence.
- **Bound to the worker.** One table per database:

  ```sql
  CREATE TABLE solera_generations (relid oid, part text, generation bigint, worker text,
                                   PRIMARY KEY (relid, part));
  ```

- **`Store.acquire(context)`** runs after the producer computed and before
  any repair read or mutation (`resolved-commits.md` §3), in a transaction
  of its own:

  ```sql
  INSERT INTO solera_generations VALUES ($relid, $part, $g, $inv)
  ON CONFLICT (relid, part) DO UPDATE SET generation = $g, worker = $inv
    WHERE solera_generations.generation < $g
       OR (solera_generations.generation = $g AND solera_generations.worker = $inv)
  RETURNING worker;               -- no row: a newer generation, or another worker of this one
  ```

  Postgres locks the conflicting row even when the `WHERE` refuses, so the
  newer acquisition waits behind an older writer's open transaction and
  every later transaction of the older writer is refused. (Checked on
  Postgres 17.)
- **Every write transaction** starts by locking the row and checking that
  it still holds `(g, worker)`; otherwise it raises before changing
  anything (a store exception after the gate: `writing`, §2.3).
- **Equal generation, other worker** is refused, so a duplicate of the
  newest attempt cannot write. An attempt the engine ended before it
  acquired is stopped by its control file (§2.4), not by the database,
  which never saw its generation.
- **A table that does not exist yet.** The first write establishes the
  table's shape from the rows the producer returned (today's `_ensure`),
  so acquisition cannot happen earlier. Creation is serialized: under a
  transaction-level advisory lock on the table's name, the store creates
  the table from the rows, then acquires in the same transaction, so the
  fence row exists from the table's first moment. A second attempt
  arriving meanwhile waits on the lock, finds the table, and acquires as
  usual.
- **Migrations** change the whole table, so each runs between writers,
  never under one. Every writer's transaction — an acquisition, a
  partition's first write, any write — first takes the table's write
  domain shared (a transaction-level advisory lock); a migration takes it
  exclusively, so it waits for every open write transaction and holds off
  new ones, of partitions with a fence row or not. Before it changes
  anything, it takes the attempt's own partition: an older attempt's migration
  is refused. One that **replaces the relation** (create, copy, drop,
  rename) gives the table a new OID; it moves the fence rows to the new OID
  in the same transaction, so the write domain keeps its generations. An
  operator's `solera migrate` has no generation: it only takes its turn.
- **Only a migration may replace the relation, or touch another partition.**
  Its `Sql` write is a query the store materializes into its own partition,
  never a statement: one that would update, delete or replace anything is
  refused before it runs (architecture.md, "Writes").
- **Cost**: one indexed upsert per acquisition and one row lock per write
  transaction; a takeover waits for at most one older transaction.

### 9.8 FileStore and S3Store: unique names, collected without a lock

**Decided** (Erwin): every object the store writes has a name no other
attempt will ever write, so deleting one can never hit something current.

**Names.** The physical name carries the attempt's **generation** (§9.7:
the claim's event counter, in the spec), which is also the key's version
(`versions.md`):

```
site_files/alpha/f-1/184467.json                 a key, as generation 184467 wrote it
site_status/alpha@184467.json                    a value
site_events/alpha/000000000042/184467.json       commit 42 of an append output, by generation 184467
```

Writes are create-only. Two writers of one name are the same attempt (a
delayed duplicate of itself), so they write the same bytes: an attempt
writes a key at most once.

**Why the generation, not the commit number.** A retry reuses its predecessor's
commit number: W1 (commit 57) dies having written `f-1/57`, and its retry
W2, also commit 57, may write that very name and commit it. W1's leftovers
could then be judged only once commit 57 is committed, and only by diffing
them against the committing delta. A generation is never reused, so an
attempt that ends without committing leaves objects nobody else names:
they can go at once. It costs the same one integer per index entry.

**The records.** Names are settled here; bytes in `key-index-format.md`.

- **Index entry:** `(key, generation, deleted, payload?)`. `generation`
  is the one that last wrote the key (a varint, ~4–5 bytes before
  compression; neighbouring entries share generations and compress
  well): its version and its object's name at once.
- **Delta entry:** the same, plus an optional **predecessor** generation
  — the entry it replaces or deletes. Writes are exact, so a writer always
  names it. A merge keeps it on a key's oldest kept version; a merge into
  the base drops it.
- **Everywhere an entry travels, the generation travels with it:**
  resolver responses (the delta file), reads answered at `start`
  (`resolved-commits.md` §7: `.kx` files), and the `Keys` selection a
  store receives (`{key: generation}`).

**How loads find names.** A keyed load computes every name from `Keys` and
needs no LIST. A full load of a keyed input becomes a `Keys` selection the
worker pages from the pinned index (the spec pins the index of every
keyed input, as it does for incremental ones); stores never read an index.
A value's name is in its head's ref. An append output's range load lists
the commits' prefix and keeps, per commit number, the file with the highest
generation: the attempts that used commit `n` all ran between the commits
of `n − 1` and `n`, one at a time, and the one that committed `n` was the
last of them.

**Predecessors.** Collecting a superseded object needs its exact name,
so its predecessor's generation. Writes are exact
(`key-index-design.md`): every key a filter holds has its entry read, so
every delta names the predecessor of each key it writes or removes, on
the engine, in the streaming merge-join and in the sparse reader alike.
Collection is prompt for every superseded version, and a merge never
needs to emit cleanup of its own.

**Reader pins.** An object or index file may go only when no reader can
still need it. The pins, all by event counter (`object-store-state.md`
§6):

- every live claim (§3.1);
- **durable multi-attempt reads**, recorded with their pin in the
  position state, so the pin holds in the gaps between attempts and
  across engine restarts, until the read ends:
  - a **delta pass over several batches**: an `Incremental` input delivering one pinned
    range `from…to` over several attempts (`after` set). A later commit
    may supersede a key inside the range, and an attempt launched after
    that commit would not otherwise cover the version the pass still
    delivers;
  - a **pattern change drain** and a **retry pass** (`per-key-processing.md`),
    each reading one pinned snapshot across many attempts.

**Collection.** Only from durable decisions; never from what a listing
shows.

- **Superseded versions, named at resolution.** A commit records one
  data-garbage entry for its delta file at its event counter. Once no
  pin predates it, the worker calls `store.cleanup` with the delta's
  predecessors, one identity pattern each — `(partition, key, generation)`
  (docs/stores.md § Cleanup) — 64 at a time; then the entry goes. A
  superseded value is its previous version's generation.
- **Versions dropped by a merge** need nothing of their own: writes are
  exact, so every version a merge drops was named as a predecessor by the
  delta that replaced it, and is cleaned up through that delta
  (docs/key-index-design.md).
- **Attempts that ended without committing.** Their names carry their own
  generation, which no other attempt uses, and their `AttemptFinished`
  without a commit is durable: one pattern, `(partition, generation)`,
  takes all they wrote — keys, value or commit.
- **The sweep**: what a worker still running after its attempt ended
  (given up on, or a duplicate) wrote later. It is the same cleanup, run
  late: an abandoned attempt's entry is due its end plus the asset's
  timeout and cancel grace (and no pin predating it), by when a worker
  that honours cancellation has written its last — once the control file
  is ended it can neither take the gate nor seal, so all it can add is the
  store calls already in flight. Its one call, `store.cleanup(output,
  home, partition, generation=G)`, takes what the attempt wrote, early and
  late. Nothing reads those objects meanwhile (the index never named
  them): the wait costs storage only.

**As built.** The engine holds no store credentials and runs no user
code, so workers clean up, twice over:

- **Right after a commit** (D8). The worker's `finished` waits until the
  engine settled the attempt and made its commit durable; the answer
  names the entries now due in its partition — what the commit let go of that
  no reader pins, and what was waiting — with each output's head. The
  worker calls `store.cleanup` with them and acknowledges by entry id
  (`POST attempts/{a}/cleaned up`, recorded as `CleanupsDone`). So a
  partition that never runs again keeps no garbage.
- **By the partition's next attempt**, the fallback for whatever the first
  missed: the engine unreachable, the worker gone before it
  acknowledged, a reader still pinned. The engine puts the due entries (at
  most 64) in the spec's output info, and the worker, after its own store
  call succeeds, cleanups them and reports which in its result;
  `AttemptFinished` then removes them.
- **By a cleanup task** (K25), where no attempt of the output will ever
  come again: an output removed, or moved to another store. The deploy
  records the life it ended — the old store (a built-in one's class and
  config, from the manifest), the output's home and declaration, and
  `before`, the first generation after — due once its `cleanup_after`
  has passed and no pin predates the deploy. The engine then submits a
  task of its own, asset `@cleanup`, on the default placement, retried
  like any (six tries); its worker rebuilds the store and calls
  `store.cleanup(output, home=…, before=G)`, so a later life of the name
  in that store keeps what it wrote. A store of the project's own that it
  no longer declares cannot be rebuilt: the task fails for good, and the
  entry is stuck with that reason, shown by `solera cleanups`, until an
  operator clears it (its objects stay).

Deleting a name twice is no harm, and only the index files an
acknowledged entry names become garbage. Due means no reader pin that
may read the entry's output partition predates it: pins are per output partition,
each named by its index prefix. An attempt's claim pins what it reads and
writes (§3.1); a delta
pass or a full pass (its snapshot), or a pattern change drain, its upstream; a sensor tick its sources; an
engine reader what it reads (`history/` for a history query). Index and
history files are collected by the same rule, by their paths, so one slow
reader holds back only what it reads. A delta file
a pending entry reads is kept, even once the index let go of it, until the
entry is done — and while an attempt whose spec holds the entry runs
(its claim's cleanups, §3.1). An entry whose files cannot be read stays pending; after
three such attempts it is `stuck`: no longer handed out, listed in
`/api/diagnostics` and on its partition's head record, until an operator runs
`solera cleanups OUTPUT [Partition] --clear` (its objects stay). An
abandoned attempt's objects go by its generation, found by the store
(FileStore by name, under the output life's home); its own delta files
(`{commit_number:012d}-{attempt}*` under the index prefix) go through the
ordinary index garbage.
A pattern change drain's snapshot pin (`position.pattern change.pin`) holds both
index-file garbage and data cleanups, as a live claim does; a retry pass
needs none, since each of its batches reads the state of its own prepare
(`per-key-processing.md` §20).

**Remaining orphans.** Two cases leave objects behind, harmless but for
the storage they take: the index never names them, and they are
collected when their output is removed or moved (the cleanup task's
`before=G`). A partition that never runs again hands its due entries to
no attempt. And the sweep's bound holds for workers that honour
cancellation: one whose machine pauses (a suspended VM) can resume after
the window and complete a write it had already started. Storage is the
cheap resource: no periodic task chases them.

Writing the same content again (`v1 → v2 → v1`) writes a new name
(`f-1/{g3}`): deleting the old `f-1/{g1}` cannot touch it. That is what removes the
GC lock, the dequeue-on-reuse rule and the DELETE timing assumption of the
alternative.

**Costs.** Per changed key: one PUT, as today, and one DELETE (free on
S3) when superseded, batched. Per commit: the delta's predecessor
generations (a few bytes per changed key). Per full load of a keyed input: a
read of its pinned index. In return: no gate, intents or repair for these
stores (§9.6); readers see the generation they pinned; a stale write is an
orphan.

**Readable listings.** A key's directory holds its current object, plus
superseded ones until collection catches up (minutes, behind the oldest
reader pin). `solera data get OUTPUT KEY` resolves the current one.

## 10. Pool

```
engine   AttemptLaunched (pool: ingest, needs: {cpu: 4}), durable — no placement call
worker   GET pools/ingest/work?wait=30 (capacity: cpu 8)   → [{attempt: A, run: R, objects}]
worker   swap runs/R/A.control open → owned              → wins, or loses and polls again
worker   POST attempts/A/start                               → runs as any attempt
```

An attempt on a pool moves through four states. The engine keeps them in
memory and rebuilds them after a restart from the journal and the
control file:

| State | Evidence | Leaves it by |
|---|---|---|
| **waiting** | launched, the control file `open` | an owner (`start`, or `owned` observed); the executor's `provision` deadline, if set (none by default) |
| **owned** | the control file `owned`, no report yet | `start`, a beat or a change of `.beat` → running; none within the ownership timeout (60 s) of the engine first observing `owned` → lost |
| **running** | `start`, beats, or `.beat` changing | a result, silence, cancel or timeout (§7) |
| **ended** | `AttemptFinished` | — |

- **Discovery** returns waiting attempts that fit the worker's capacity,
  oldest first. It is a hint: two workers may get the same attempt, and the
  swap to `owned` decides. The engine reads the control file (one GET) for an
  attempt it offered and has not seen start within 10 s, so an owner that
  died before `start` is noticed.
- **A dead owner expires into a new attempt id**, never a new owner of
  the old attempt: the attempt ends lost, and the retry policy launches a
  fresh attempt, with its own spec and control file. Its writes are
  classified from its control file (§2.3), never from what `.beat`
  showed: the owner may have taken the gate and entered a store call
  without reporting again. The engine's swap from `owned` to `ended`
  lands → `none`; it finds `writing` → `writing`, and its intents wait for
  the next attempt's repair; S3 cannot answer → the engine cannot
  establish `none`, and retries before releasing anything.
- **The runtime clock** starts at ownership when HTTP is unavailable: an
  owner that computes through an outage, reporting by `.beat`, is
  running, not provisioning.
- **No registration, no leases, no ownership event.** A pool worker's liveness
  is its attempt's heartbeat, like any worker's.
- **Engine down:** pool workers finish their attempts and get no new ones.
- The handle is `{attempt, pool}`. A cancel before a worker owns it withdraws the
  attempt from discovery and ends it; after it, cancel is §7.
- **One process per attempt.** A pool worker runs each attempt it gets in
  a child forked from a forkserver that imported the framework and nothing
  else: it starts no thread and never uses the object store, so forking
  from it is safe on any OS (the worker itself has threads: fork copies
  only the forking one, which macOS does not survive). The child imports
  the project itself — project code may start threads or hold locks a fork
  would copy without them. The child is the attempt's process: a forced
  cancel ends it, and it exits the moment its result is published.

**Every attempt process exits at once** (`os._exit`) once its sealed
result is published, or once it cannot be: Local, ECS and Kubernetes
workers as much as pool children. A thread the attempt gave up on — a
synchronous per-key incremental call canceled mid-flight, say — would otherwise keep
the process, and its placement, alive until it returned. (Modal runs the
worker as a function in a container of its own: not this exit.)

## 11. Sensors

**Decided** (Erwin): frequent checks that may or may not lead to a change
are **sensors**, Dagster's concept, not a variant of attempts. A tick is
not an attempt: no `.spec`, control file or run of its own,
and no journal event unless it changes something. Observable sources (`per-key-processing.md` §12) become
sugar for a sensor.

### 11.1 The API

```python
@sensor(every=60, commits=["uploads"], executor=Pool("sensors"))   # default executor: the local host
def new_uploads(ctx, s3: S3Client) -> Tick:
    page = s3.list_since(ctx.cursor)
    return Tick(
        cursor=page.token,                                   # optional
        commits=[Commit("uploads", upsert={o.key: o.etag for o in page.objects})],
        runs=[RunRequest(["ingest"], partitions=[o.prefix for o in page.objects])],
    )
```

A body returns `None` (nothing happened) or a `Tick` of up to three
things, all optional:

| Outcome | Lands as |
|---|---|
| `cursor` | the sensor's cursor, durable |
| `commits`: `Commit(source, version=…)`, `Commit(source, keys={…})`, `Commit(source, upsert=…, remove=…)` | a source commit, exactly the commit API's (`architecture.md` §5) |
| `runs`: `RunRequest(targets, partitions=, config=, keys=, tags=)` | a run submission, exactly the API's |

**`commits=` declares every source the sensor may commit to.** Dispatch
sends a snapshot of exactly those sources (§11.3), and a `Commit` to any
other is refused. Run requests need no declaration: submission validates
their targets.

`Source.observe()` is sugar: `Source("datasmart", observe=Every(300))`
declares a sensor `datasmart.observe` whose body calls `observe()` and
returns a `Commit` to its own source (a `str` as `version`, a map as
`keys`, `Observed` as `upsert`/`remove` plus `cursor`); its `commits=` is
that source.

### 11.2 Where ticks run

In a **sensor worker**: a long-lived process with the project loaded, like
Dagster's code server, so a tick costs a function call, not a process
start and an import.

- **Local (default).** When the project declares sensors, the engine
  keeps one sensor worker subprocess alive beside it, restarted with backoff
  if it exits and replaced when a new deploy is served.
- **Pool.** `solera_worker sensors --pool NAME` on any machine: the same
  host, remote, for sensors that need a network the engine cannot reach or
  that should not run beside it. `executor=` on the sensor picks.

A host long-polls `GET sensors/next?wait=30` with its pool token and its
deploy; the engine answers with due ticks for sensors on that
executor and deploy: `{tick, sensor, cursor, snapshot}` (§11.3). The
host runs the body (up to `concurrency` ticks at once, each within the
sensor's `timeout`, 60 s by default) and posts the outcome to
`POST sensors/{sensor}/ticks/{tick}`.

### 11.3 The sensor snapshot

What a tick observed against, sent with it, per declared source:

```json
{"uploads": {"head": "h:184702", "index": {"prefix": "keys/uploads/_/", "files": ["…"], "log": ["…"]}}}
```

- **`head`** is an opaque, monotonic identity of the source's current
  head: the event counter at which that head was installed (by
  registration, an API commit or a sensor). Every source kind has one,
  including unkeyed sources whose version is a string: a tick that saw
  `v1` while an API client committed `v2` meanwhile carries a stale `head`
  and is refused, whatever its version says.
- **`index`**, for keyed sources, is the pinned key index — the same
  record a spec pins — so a host that resolves a big map itself reads
  exactly that file set.
- **The tick holds a reader pin** at its dispatch event counter (§9.8) until
  it is applied, refused or dropped, so the files it reads are not
  collected under it. The pin is memory-only, like the tick: after a
  restart the tick's post is refused anyway.

### 11.4 Applying a tick, and accepted outcomes

The engine keeps, per sensor, one **tick claim** in memory: the tick it
dispatched, its cursor and snapshot. Durably, per sensor, it keeps the
**accepted-outcome record** of the last tick that changed something:

```json
{"cursor": "token-42", "accepted": {"tick": "01J9…", "runs": ["01J9…"], "commits": {"uploads": "h:184731"}}}
```

A posted outcome is decided in this order. Preparing reads the key index
and plans runs, so the decision has awaits: it is serialized per tick, not
synchronous.

1. **A duplicate waits.** If a post of this tick is being decided, a
   second post waits for that decision and is answered as the first was.
2. **Already accepted?** If the record names this tick, the post is a
   retry of an outcome already applied: answer it again, apply nothing,
   delete nothing — once the decision it acknowledges is durable.
3. **Current claim?** Otherwise the tick must be the sensor's current
   claim; else `409` (a late tick, or one from before a restart).
4. **Mark the claim deciding.** It stays, and with it the tick's reader
   pin, until the decision is recorded or its refusal cleaned up: no
   other tick of the sensor is dispatched meanwhile (which could move the
   cursor under it), and expiry leaves it alone. Then it is consumed,
   whatever the outcome.
5. **Check every snapshot together.** Each commit's source must still have
   the `head` the tick was dispatched with, and be declared; one stale
   source refuses the whole tick (`409`, nothing applied), and the next
   tick observes again. A stale full map must not undo a newer commit.
6. **Prepare without publishing**: source commits through the commit
   API's preparation (an identical version or map is no change), then run
   requests through submission's, planned against the heads those
   commits will install (a dynamic partitions's new elements), never touching
   the model. The sources' heads are checked again after.
7. **Record everything at once**: the source commits, the run submissions
   (`command = {tick}/{n}`, so the run receipts deduplicate them too),
   `SensorAdvanced {sensor, cursor}` if the cursor moved, and the new
   accepted-outcome record — one `record()`, so one flush holds
   all or none of it — and answer once it is durable. A tick whose
   outcome is nothing records nothing.

"Unchanged" therefore means no new version, no key change and no new
cursor (review finding 9). A cursor-only tick records only
`SensorAdvanced` and its accepted-outcome record: no delta, no new
generation, no consumer woken.

**Key maps.** A small map (up to `sensor_map_max`, 1M keys) is posted as a
sorted run in the resolver's framing (`resolved-commits.md` §4), and the
engine resolves it in-process against the snapshot's index, as it does for
API commits. A bigger one is resolved by the host — the streaming
merge-join over the snapshot's `index` — which uploads the delta file
under a name that includes the tick, and posts a **delta reference**
`{files, commit_number}`. Installed, it is the source's delta like any other. A
refused or dropped tick's file is **not deleted on the spot**: a retry of
an accepted tick may name it (step 1 answers that one), so the decision
queues it as garbage only if the source's index does not reference it,
and it goes through the causal garbage queue (§9.8). Large sources should
use a cursor anyway: comparing a 10M-key map every five minutes is a full
replacement every five minutes.

### 11.5 Restarts, failures, history

- **Claims are memory only.** After a restart no tick is in flight; each
  sensor is due again at its next interval, from its durable cursor. A
  host still running a tick from before posts it and gets `409`.
- **A tick that raises** is recorded as `failed` with its error; the
  cursor does not move. A tick over its `timeout` is dropped by the engine
  (its late post gets `409`); a host whose ticks keep overrunning is
  restarted.
- **History is the `ticks` table**: sensor, tick, started and ended, host,
  outcome (`skipped`, `committed`, `requested`, `failed`), error, the runs
  it requested. Its rows are buffered in memory and written with the next
  history flush (a `HistoryFlushed` installs the file, as for other
  tables), never journaled: a crash loses the last minute of ticks, which
  is acceptable for a log of checks. Kept a day; ticks that did something
  are kept as long as the runs they caused.
- **What a tick caused is a run like any other**: a source commit is a
  run with no tasks (as API commits are today), a requested run is a run,
  tagged with the sensor and tick.

### 11.6 What this removes

Compared with the spec-less tick attempt of the previous draft:

- no launch path without a spec, and no attempt that is not journaled;
- no per-launch bootstrap credential, no in-memory worker ownership per
  tick, no tick result route;
- no process start per check: a 300-second sensor on `Local` cost a
  subprocess and an import every tick;
- no `skipped` runs, attempts and `kind: observe` retention class: checks
  that found nothing are tick rows, not runs;
- no resolve route validated against an tick claim: small maps
  resolve in the engine, big ones on the host.

What it adds: the sensor worker (one long-lived process kind, which Pool
workers resemble), two routes, a cursor per sensor in the engine's state,
and the `ticks` table.

**Risks.** User code in a long-lived process can leak memory or state
between ticks, and a body that hangs holds a host thread: hosts are
restarted when ticks overrun or after `host_max_ticks` (10,000). A host
on an old deploy gets no ticks.

### 11.7 As built

- **API and records** as above, with `SensorAdvanced {sensor, cursor,
  accepted}` one event: the cursor and the accepted-outcome record
  together. A tick commits at most once per source.
- **Maps travel as JSON** in the posted `Tick`, and the engine resolves
  them against the snapshot's index as it does API commits. Host-side
  resolution of bigger maps and delta references are not built: a full map
  over `sensor_map_max` (1M keys) fails its tick.
- **Due at once after a restart**, not at the next interval: a sensor's
  next tick is memory-only, and a restart is rare enough that an early
  tick costs nothing.
- **Tick outcomes** are `skipped`, `advanced` (cursor only), `committed`,
  `requested`, `refused` (a stale snapshot) and `failed` (it raised, timed
  out, or asked for what it may not).
- **Tick rows all expire after a day.** What a tick caused outlives them:
  its runs carry the tags `sensor` and `tick`, its source commits `by:
  "sensor NAME"`.
- **Hosts.** A host runs up to 4 ticks at once, each body on a daemon
  thread (an `async` body or `observe()` is awaited there). After a tick
  overruns, or after 10,000 ticks, a host stops asking, gives the ticks
  still running five seconds, leaves the rest to expire and tick again,
  and a `solera_worker sensors` process re-executes itself; the engine keeps its
  local one running, with backoff, beside a served engine (one with an
  engine URL), and authenticates it with a token signed by the engine
  secret. Pool hosts use the pool token.
- **Reader pins.** A tick's pin joins the claims' and the delta passes'
  in one floor, which both data and index garbage respect.

## 12. Engine restart, attempt by attempt

| The attempt was | The next engine |
|---|---|
| preparing (no `AttemptLaunched`) | dispatches the task again; the orphan spec, if any, goes with the run |
| launched, no handle recorded | `resume` (ECS, Kubernetes: launch again by name). Local and Modal cannot be found by name: the engine follows the control file and `.beat`, and a launch that never happened ends at the provisioning deadline, as lost, retried under a new attempt id |
| provisioning | follows the handle; keeps the clamped remainder of its allowance (§8) |
| owned or running | rebuilds the binding from the control file; follows the handle; beats arrive again; if not, reads `.beat` |
| canceling | the cancel is durable for a canceled run (`RunControlled`); a timeout is re-derived from the restarted clocks. Either way it starts at phase 1 again, which a draining worker answers with its result |
| done, result sealed | reads it from the control file and settles it |
| ended by the old engine (control file `ended`) | settles it from what that engine recorded, under §9.6 |
| a sensor tick | forgotten: due again at its next interval, from its durable cursor (§11.5) |

## 13. What an attempt costs

At S3 list prices (PUT $5, GET $0.40 per million) for scenario E of
`key-index-costs.md` — one short attempt every 10 s, 259,200 a month —
counting what the attempt itself costs, apart from its key-index reads and
delta file (`resolved-commits.md` §9). A short attempt here logs a few
lines and sends no periodic heartbeat to the object store.

Two choices of this design keep a short attempt cheap:

- **`AttemptPlaced` rides the next segment.** It is recorded **lazily**:
  buffered without starting the flush timer, so it is written with
  whatever comes next — for a short attempt, its `AttemptFinished`. A crash
  before then loses only the handle, which `resume` or the worker's reports
  recover (§12). Lazy recording is a small journal feature (`record(…,
  lazy=True)`), useful for any event nothing waits on.
- **A short log travels inside the result.** Chunks are flushed every
  30 s or 1 MB; at the end, the lines not yet in a chunk go into
  the sealed result (`log.tail`) if they are under 64 KB, else into one last
  chunk. An attempt shorter than 30 s that logs a little writes no log
  object at all; its lines were live over HTTP meanwhile.

| Per attempt | Today (counted from the code) | Target, `immutable` store | Target, `fenced` |
|---|---|---|---|
| Engine PUTs | spec · journal: launch, placed, finished = 4 | spec, control file · journal: launch, finished = 4 | 4 |
| Engine GETs | beats while provisioning ~1, result 1 = 2 | control file 1 | 1 |
| Worker PUTs | beat, gate, log chunk, joined log, result, done beat = 6 (more chunks while it logs: one per 2 s) | owned, sealed = 2 | + writing = 3 |
| Worker GETs | spec, fence read by the beat = 2 | spec 1 | 1 |
| Worker DELETEs | chunks 1 (free) | — | — |
| **Requests** | **10 PUT + 4 GET** | **6 PUT + 2 GET** | **7 PUT + 2 GET** |
| **Per month** | **$13.38** | **$7.98** | **$9.28** |

Shared by all attempts, not per attempt: history flushes (one Parquet
file per table per minute, ~6 PUTs a minute: ~$1.30 a month, merges
extra), `engine/alive.json` every 30 s while runs are live (~$0.43), and
checkpoints (negligible here). The older model's $6.69 (4 PUT + 2 GET +
one journal PUT) left out the beats, the gate, `AttemptPlaced` and the
done beat: today really costs about twice it, and the target lands on it
as a count rather than an underestimate. The control file adds one PUT
per attempt (the engine's `open`, $1.30 a month here; requests are free
on Railway), and saves the `closed` tombstone an attempt on a gated
store paid when it ended without taking its gate. A longer attempt adds
one chunk per 30 s of logging; one that loses HTTP adds three `.beat`
PUTs and three control-file GETs a minute.

Not worth a second path: putting the spec in the `start` answer would save
the worker's GET ($0.10 a month).

**Sensors** cost none of this: a tick that finds nothing is a long-poll
answer and a post, with no object request and no journal write; one that
commits pays the source commit (a journal PUT, and a delta file for keyed
sources).

## 14. What changes from today

| Today | Target |
|---|---|
| `{attempt}.json`: spec, then overwritten with spec + result + log index | `.spec`, immutable, and the control file, swapped (§2.4) |
| `.beat` every 30 s with a fence GET, then a done beat | HTTP beat every 10 s; `.beat` only while HTTP fails; `sealed` means done |
| no worker identity; duplicates overwrite each other (R3) | the first swap to `owned` wins; losers write nothing and wait for the owner |
| log chunks joined at the end, chunks deleted | immutable chunks every 30 s / 1 MB, indexed from the result; live lines over HTTP |
| Pool: register, claim, renew, complete; in-memory leases; `AttemptClaimed` | long-poll discovery; ownership by the control file; four states; a dead owner expires into a new attempt |
| cancel read from the fence by every beat; engine aborts at once | two-phase cancel: requested and drained, then forced |
| any presumed death releases the partition (R1) | every store is `immutable` or `fenced`: released at once, the older writer unable to write |
| a failed result releases the partition | a failure after the gate leaves its write `writing`; an attempt without a result is classified from its gate (§2.3) |
| gates deleted with their run | the gate is the control file's `writing`, created by the engine before launch and only ever swapped: deleted with its run, a worker finding it gone stops (§2.4). (Built before K18: create-only gates retained `gate_days` as tombstones.) |
| cancel and timeout indistinguishable to the worker | a latched cancel record with phase and reason, carried into the result (§2.2) |
| repair before the store is fenced | `Store.acquire` before repair, for fenced stores |
| resolve through `.ask` objects (proposal) | binary `resolve` route, nothing persisted |
| observable sources as proposed attempts without a spec | sensors: ticks in a warm host, applied through the commit and run APIs, `SensorAdvanced` for the cursor, a lossy `ticks` table |
| `{key}.json` overwritten in place (FileStore, S3Store) | `{key}/{generation}.json`, create-only; superseded and abandoned names collected without a lock |
| one API token | admin token; per-attempt HMAC token from a stable secret; per-pool token |

## 15. Open questions

1. **`cancel_grace`.** 60 s by default; per asset, since a batch of
   16 concurrent calls into a slow API may need longer to drain.
2. **`sensor_map_max`.** Where a key map stops being posted to the engine
   and is resolved on the host instead; from the resolver's grid
   (`resolved-commits.md` §6), like its other thresholds.
3. **Generation size.** Settled by format v3: one varint per entry, 8 B
   per entry in all at 10M random ids without payloads
   (`bench/keys/results.md`).
4. **`gate_days`.** Settled by the control file (§2.4): there is no
   retention to bound. A worker that finds its control file deleted
   stops, however long it paused.
