# The attempt lifecycle — target design

Status: **target design**, not built, revised after the follow-up review.
It replaces the attempt files, heartbeat, pool protocol and write-safety
rules of `object-store-state.md` §8 and `architecture.md` §10. It settles
D2–D4, and D1 as Erwin decided it after the review: heartbeats are evidence
only, built-in stores are exact, user overwrite stores choose between a
bounded wait and holding the scope.

Two sections are **held** for Erwin's decisions and marked so: the
observation launch path (§11) and FileStore naming and collection (§9.8).

It assumes what is built: fence segments kept for good and carrying a
writer nonce, create-only writes that recognize their own bytes
(`solera.objects.create`), results sealed once, durable retirement,
event-position garbage pins, recorded placement handles, per-placement
"can't tell", a provisioning deadline, and a timeout that runs from the
worker's first report.

Its companions: `resolved-commits.md` (the resolver on this channel, and
the write phases of a keyed output), `per-key-processing.md` (pages,
failure indexes, observations).

## 1. The shape in one paragraph

Durable facts live on the object store; signals go over HTTPS. The engine
writes an immutable **spec** and authorizes the launch in the journal; the
worker that wins a create-only **claim** (`.worker`) runs it and seals an
immutable **result**. Heartbeats, live logs, cancel, resolve and pool
discovery are requests to the engine. None of them grants anything: a
heartbeat is evidence that a worker lives, never permission to write, and
the permission to write comes from the durable launch and, for stores that
can enforce it, from the store itself. So when the engine is down, workers
keep working and their results land on the object store; when a worker
falls silent, what the engine may do next depends on what its store can
guarantee (§9).

```
engine                                   object store                         worker
  claim scope, choose generation, pin
  PUT .spec  ──────────────────────────▶ runs/R/A.spec
  AttemptLaunched, durable
  placement.launch(A); AttemptPlaced ──────────────────────────────────────▶ boots, GET .spec
                                         runs/R/A.worker  ◀── create-if-absent (claim)
  ◀──────────────────────────────────────────────── POST attempts/A/start
  ◀──────────────────────────────────────────────── POST attempts/A/beat     every 10 s
                                                                              Store.acquire (fenced)
  ◀──────────────────────────────────────────────── POST attempts/A/resolve  (small keyed writes)
                                         keys/…/12-A.kx   ◀── deltas
                                         runs/R/A.writing ◀── gate (fenced, overwrite)
                                         output data      ◀── store writes
                                         runs/R/A.log.000000…  ◀── log chunks
                                         runs/R/A.result  ◀── create-only, sealed
  ◀──────────────────────────────────────────────── POST attempts/A/finished (hint)
  GET .result, validate, AttemptFinished, durable
```

## 2. Objects

All under `runs/{run}/`, all named after the attempt. Only the claim's
winner writes anything but the spec (§4), so no name needs the invocation
token.

| Object | Written by | Mode | Meaning |
|---|---|---|---|
| `{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | what to run: today's `spec`, plus `engine` (HTTPS URL), `token` (§5.2), `generation` (§9.7) |
| `{attempt}.worker` | the invocation that claims it | created once; then overwritten only by its owner, only while HTTP fails (§6) | the claim `{"invocation", "host", "pid", "at"}`; later also `{"seq", "timeline", "usage"}` |
| `{attempt}.writing` | worker before its first store write, or engine to abort | create-only | the gate, as today, now carrying `invocation`; only for outputs on `fenced` and `overwrite` stores (§9.6) |
| `{attempt}.log.{n:06d}` | worker | create-only, immutable | a gzip member of the log, flushed every 30 s or 1 MB |
| `{attempt}.result` | worker | create-only, immutable, sealed bytes | the outcome; its existence means the worker is done |

Gone: the two-write `{attempt}.json`, `.beat` and its done marker, the
joined `.log`, chunk deletion, `AttemptClaimed`.

**The result** carries today's result, plus the invocation, the timeline,
usage, the log index, and what is known of the attempt's writes:

```json
{
  "invocation": "k3v9q2",
  "status": "succeeded",
  "writes": "complete",
  "outputs": {"orders": {"ref": {"…": "…"}, "keys": {"files": [{"name": "000000000012-01J9…"}], "added": 3, "removed": 0, "exact": true}}},
  "delivered": {"uploads": {"after": null, "upserted": ["u-7"], "deleted": []}},
  "cursor": "token-42",
  "timeline": [{"type": "booted", "at": 1790074791.2}, {"type": "computing", "at": 1790074793.0}],
  "usage": {"cpu_seconds": 4.1, "peak_memory": 512000000},
  "log": {"chunks": [[0, 412, 1790074791.2], [1, 388, 1790074821.2]], "lines": 800, "truncated": false}
}
```

- `status` is `succeeded`, `failed` or `canceled` (a drained cancel, §7).
- `writes` is `none` (it made no store call), `complete` (every store call
  it made returned), or `uncertain` (a store call raised or was abandoned).
  §9.5 says why that matters.
- `log.chunks` lists `[n, lines, first timestamp]` per chunk. Chunks are
  never joined: the console reads "the last 200 lines" as the last few
  chunks.

## 3. One attempt, step by step

Example: `orders:alpha` writes a three-key `Patch` to PostgresStore, on
ECS, engine up throughout.

1. **Claim the scope** (memory) and pin inputs, as today. The claim's
   event position becomes the attempt's **generation** (§9.7): chosen now,
   before the spec.
2. **Write the spec**: `PUT runs/R/A.spec` create-only.
3. **Authorize**: record `AttemptLaunched`, await `durable()`. A replaced
   engine fails here and launches nothing. This is the durable launch
   authorization every later step rests on.
4. **Launch**: `placement.launch(stage)` with the provider's name for `A`
   (ECS `clientToken`, Kubernetes job name); record `AttemptPlaced`.
5. **Claim**: the worker boots, reads the spec, generates an invocation
   token (`k3v9q2`) and creates `A.worker`. If it finds another token
   there, it lost: §4.
6. **Start**: `POST attempts/A/start`. The engine binds the invocation
   (§5.3). The attempt now runs: its `timeout` starts (§8).
7. **Acquire** (fenced stores): `Store.acquire(scope)` takes generation
   `g` for the output's write domain (§9.7), before reading anything from
   the store.
8. **Compute**, beating every 10 s. Logs go live over HTTP and durably as
   chunks.
9. **Plan** each keyed output (`resolved-commits.md` §3): repair reads,
   resolve (`POST attempts/A/resolve`, or locally), upload the delta file.
10. **Gate**: create `A.writing` with the intents (fenced and overwrite
    stores only).
11. **Write**: the store's transactions check generation `g` (§9.7).
12. **Seal**: build the result once, `PUT A.result` create-only, retried
    with the same bytes; `POST attempts/A/finished` as a hint; exit.
13. **Settle**: the engine reads `A.result`, checks its invocation against
    the bound one, validates it, records `AttemptFinished` and makes it
    durable. One journal decision installs the output deltas, the failure
    index delta (`per-key-processing.md` §9), the cursor and the
    watermarks together.

A failed attempt follows the same path with `status: failed`. Canceling
and timing out are §7.

## 4. Invocations and duplicates

A placement may run one attempt twice: Kubernetes may start a second pod
for a Job even with `parallelism: 1`, and an engine restart can `resume`
(relaunch by name) while a launch whose answer was lost is starting. The
claim decides:

- **The first `.worker` create wins.** The claim is the first thing a
  worker does after reading the spec: before computing, logging, resolving
  or uploading anything.
- **An ambiguous create** (response lost) is resolved by reading the claim
  back: its own token is its own claim.
- **The engine binds one invocation per attempt** (§5.3) and accepts
  beats, resolves and results only from it.

**A losing invocation must not end the attempt by exiting.** If it exited
at once with code 0, the engine would follow the handle it relaunched,
see the exit, find no result, and fail the attempt while the owner is
still computing; and a Kubernetes Job is complete as soon as one pod
succeeds. So:

- **The loser waits for the owner to finish**: it polls for `A.result`
  every 30 s (one GET each; rare, since duplicates are), or until any
  request to the engine answers `409 ended`, then exits 0. It never writes
  anything, not even a log.
- **The engine treats a provider exit as the owner's only if the owner has
  also fallen silent.** On an exit without a result, the engine checks the
  bound invocation's last report: one within three beat intervals means
  the exit was another invocation's, so the engine keeps waiting on the
  owner's reports (and drops the handle, which named a duplicate). With no
  invocation bound yet, it reads `.worker` first.

Delayed duplicates, long after the attempt ended, are §9.5.

## 5. The HTTP channel

### 5.1 Endpoints

All under `/api/projects/{p}/`, over HTTPS.

| Route | Body → answer | From |
|---|---|---|
| `POST attempts/{a}/start` | `{invocation, host, pid}` → `{cancel}` · `409 {reason}` | the claim's winner, once |
| `POST attempts/{a}/beat` | `{invocation, seq, events[], usage, progress?}` → `{cancel}` · `409` | worker, every 10 s |
| `POST attempts/{a}/logs` | `{invocation, offset, lines[]}` → `{offset}` | worker, every 1 s while it logs |
| `POST attempts/{a}/resolve` | binary, versioned: `resolved-commits.md` §4 | worker, small keyed writes |
| `POST attempts/{a}/finished` | `{invocation}` → `204` | worker, after `.result` |
| `GET pools/{pool}/work?wait=30` | capacity → `[stage]` | pool workers (§10) |

`cancel` is `null`, `"requested"` or `"forced"` (§7). `events` are timeline
events (`loaded`, `mark`, per-key outcomes); `progress` is free-form for
the console. Live logs and events go to an in-memory ring per attempt that
the console tails; the durable copy is the chunks and the result.

Gone: `POST /api/workers/register`, `/api/tasks/claim`,
`/api/tasks/{id}/renew`, `/api/tasks/{id}/complete`.

### 5.2 Authentication and bootstrap

- **Bootstrap.** A placement hands the worker the stage — `attempt`, `run`,
  `objects` — as today. The worker reads the spec with the environment's
  own object-store credentials; the spec gives it the engine's URL and its
  token. Pool workers get the stage from discovery (§10). Observations:
  held with §11.
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
  wait; claiming still takes the object store.
- The admin API keeps its own token; a worker's token reaches nothing else.
- The engine's URL is a stable HTTPS name (load balancer or DNS), so an
  engine restarted on another host is the same URL.

### 5.3 Rules for every request

Every route is idempotent and safe to retry, and nothing the worker sends
over HTTP is the only copy of a fact:

1. **Binding.** The first `start` binds its invocation: only the claim's
   winner sends `start`, since a loser knows it lost (§4). A request from
   any other token makes the engine read `.worker` (one GET), and the
   claim's token wins; the other gets `409 not_owner`. The binding lives
   in memory; after a restart the engine rebuilds it from `.worker` on the
   first request, never from the request itself.
2. **Sequence numbers.** Beats carry `seq`, logs carry the line `offset`
   of their first line; the engine keeps the highest per invocation and
   drops what it has seen. A retried log batch is not shown twice; a gap
   (a batch lost for good) is accepted, since the chunks hold every line.
3. **Cancel is latched.** Once the engine has answered `cancel:
   "requested"`, every later answer says at least that; the worker latches
   it too. A late or reordered response never clears it.
4. **Ended means ended.** For an attempt that has an `AttemptFinished`,
   every route answers `409 ended`; a worker receiving it stops sending
   (and, if it has not sealed a result, stops: §7).
5. **No answer grants anything.** A `200` to a beat is not permission to
   write; the worker writes on its launch authorization and its store's
   rules (§9), whether or not the engine answers.

### 5.4 When the engine is down

| Signal | With the engine | Engine down |
|---|---|---|
| claim | `.worker` create, then `start` | `.worker` create; `start` retried in the background |
| heartbeat | HTTP every 10 s | `.worker` overwritten every 20 s (§6) |
| live logs, events | HTTP | dropped; the chunks hold the lines |
| resolve | HTTP | local resolve (`resolved-commits.md` §6) |
| cancel | the beat's answer | none arrives; whoever cancels does so on the next engine, which reaches the worker on its next beat |
| result | `.result` + `finished` | `.result` |
| pool discovery | long-poll | none: nothing new is scheduled anyway |

Requests are retried with jittered backoff (1 s → 30 s). A worker never
fails because the engine is unreachable: it runs on its durable launch
authorization, and its writes are governed by its stores (§9.6), which
need no live engine. The next engine replays the journal, adopts every
launched attempt, rebuilds bindings from `.worker` (§5.3), and settles each
from `.result` when it is there.

## 6. Heartbeats: evidence, never permission

Proposal for Erwin's question, decided: **HTTP heartbeats every 10 s; the
`.worker` object only while HTTP fails; liveness is evidence only.**

- The worker beats over HTTP every 10 s. After two failed beats it also
  overwrites `.worker` with its `seq` and timeline every 20 s until a beat
  succeeds again.
- The engine reads `.worker` only for attempts whose HTTP beats stopped,
  so a steady-state heartbeat costs no object-store request.
- A worker is **silent** when the engine has seen neither a beat nor a
  change of `.worker` for three intervals (30 s over HTTP, 60 s by
  `.worker`), measured on the engine's monotonic clock from when it
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
| `finished`, or a provider exit with the owner silent (§4) | GET `.result`; if present, validate and commit or fail it |
| no `.result` after that exit; or the owner silent with no handle | the attempt is lost: end it under the release rule of its store kind (§9.6) |
| a cancel; the `timeout`; the provisioning deadline (§8) | the two phases below |

**Cancel is two phases.** Cancel and timeout share them; they differ in
what happens to the work left undone.

1. **Cancel requested.** The engine latches it and answers `cancel:
   "requested"` to the next beat (≤ 10 s). The worker stops starting new
   work: a per-key page stops scheduling keys and cancels calls in flight
   (`per-key-processing.md` §5); a plain asset's producer is cancelled. Then
   it **drains**, within `cancel_grace` (60 s by default, per asset):
   - work that finished is written and published — for a per-key page, the
     finished keys' outputs plus the interrupted holes in its failure
     index — as one result with `status: canceled`, which the engine
     commits as one journal decision (outputs, failure delta, watermark
     past the whole page);
   - a plain asset that had not reached its writes publishes `canceled`
     with no outputs and `writes: none`; one that had taken the gate
     completes its writes and publishes them, and the commit stands, as
     today.

   Result submission stays authorized throughout: the attempt is live
   until the engine records its end.
2. **Forced abort,** after `cancel_grace` without a result. The engine
   answers `cancel: "forced"`, cancels at the provider, and ends the
   attempt: for a gated store it takes `.writing` as `aborted` first. From
   then on a drain result is refused (`409 ended`) and its objects are
   garbage. If the worker already held the gate, its writes may be under
   way: the attempt ends with `writes: uncertain` (§9.5).

**What is left undone.**

- **An explicit cancel does not resume itself.** Keys interrupted by a
  user's cancel are recorded with reason `canceled` and are not due: they
  run again only when a later run is requested for them (a retry, a new
  change of the key, a forced retry). A run canceled is a run the user
  wanted stopped.
- **Timeouts are bounded.** Keys interrupted by a timeout are due after
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
observing `.worker`. A 20-minute image pull does not eat a 30-second
timeout. Built today: the first report starts it.

**Adopted attempts.** Neither clock's start is persisted. An adopted
attempt still provisioning keeps what the launching engine's clock says is
left of its allowance, but never less than three heartbeats nor more than
all of it; one already running gets its whole timeout again from when the
new engine first hears from it. A restart can stretch an attempt by one
timeout, never cut it short. Built.

## 9. Writes the engine gave up on (D1)

**Decided** (Erwin, after the follow-up review): no lease protocol;
built-in stores are exact; user overwrite stores default to a bounded wait
and may opt into holding the scope. §9.8 is held.

### 9.1 The hazard

An attempt W1 is believed finished (failed, timed out, canceled, lost),
its scope is released, and W2 commits. Then a write of W1's lands.

| Store | W1's late write | Damage |
|---|---|---|
| keyed, one object or row per key | key `k` at W1's version | `k` regresses: the index says `v2`, the store holds `v1` |
| keyed, W1 removes `k` | deletes W2's `k` | a key the index lists is missing |
| a value | the whole value | regresses to W1's |
| unkeyed incremental | batch `n`, which W2 also wrote (retries reuse batch numbers) | W2's batch replaced by W1's |

Two writers of the same key at the same version write the same content
(that is the asset's revision promise), so their order does not matter.
Only a different version landing after a newer commit does damage, and
only on a store that overwrites in place.

How W1 can still write after the engine gave up on it: it lost contact but
runs on; it paused (GC, VM migration); a request it issued is still in
flight or queued at the backend (a `COMMIT` waiting on a lock); a store
call raised on the client while the backend completed it (a timeout); a
duplicate invocation (§4, §9.5); user code writing outside the store
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
| Detect and retry | at-least-once execution; late writes possible | user `overwrite` stores, by default (§9.9) |
| Hold on uncertainty | no late write can follow a release, at the price of scopes blocked until completion is established | user `overwrite` stores, opt-in (`strict`) |
| Self-fencing leases | risk reduction under timing and authority assumptions | **rejected**: the review showed it fails without exceeding its own margins (a superseded engine still granting renewals; a failed result bypassing the drain; renewals by `.worker` defeating cancel) |
| Generation checked by the store | exact exclusion of older writers, when acquisition precedes reads | PostgresStore (§9.7); user stores declaring `fenced` |
| Immutable outputs | stale writes are unreferenced garbage; pinned reads | FileStore, S3Store (§9.8); user stores declaring `immutable` |

### 9.4 The decision in one paragraph

A store declares how it writes: `immutable`, `fenced` or `overwrite` (the
default). Built-in stores are exact: FileStore and S3Store are
`immutable`, PostgresStore is `fenced`. Their scopes are released as soon
as the engine ends an attempt, whatever happened to its worker, and their
workers keep writing through an engine outage. A user `overwrite` store
gets at-least-once retries after a bounded wait, each such release marked
in the history; or, opted into `strict`, its scope stays blocked until the
attempt's completion is established.

### 9.5 Knowing whether writes completed

The release rule of an `overwrite` store needs to know whether an ended
attempt's writes can still land. The harness knows, and says so in the
result's `writes` (§2):

| What happened | `writes` |
|---|---|
| no store call was made: the worker never reached its writes, or the engine took the gate as `aborted` first | `none` |
| every `store.store()` call it made returned | `complete` |
| a `store.store()` call raised, was cancelled, or the worker died after the gate (no result) | `uncertain` |

**Any store exception after the gate is uncertain completion.** A client
that timed out after one second may see its request finish at the backend
five seconds later; a store author cannot be asked to tell the two apart.
So a **failed result after the gate does not release a scope early**; it
follows the uncertain rule like a silent worker. This closes the review's
finding 2 without any new obligation on stores.

### 9.6 Store kinds and their release rules

```python
class MyStore(Store):
    writes = "immutable"   # writes only names nothing committed references; implements discard()
    writes = "fenced"      # implements acquire(); every write checks the generation atomically
    writes = "overwrite"   # the default: gate, intents, repair; released by policy (§9.9)
```

| Kind | Gate and intents | Repair | Released when the engine ends the attempt with `writes: uncertain` |
|---|---|---|---|
| `immutable` | none | none: abandoned writes are unreferenced | at once |
| `fenced` | gate with intents (repair, and the "unknown writes" intent of `Sql`) | after `acquire` (`resolved-commits.md` §3) | at once: the next attempt's acquisition fences the old writer |
| `overwrite`, default | gate with intents | as today | after `late_write_grace` (§9.9), marked in the history |
| `overwrite`, `strict` | gate with intents | as today | when completion is established (§9.9) |

With `writes: none` or `complete`, every kind releases at once. An
attempt whose outputs use several stores follows the strictest rule among
them.

**Failure deltas do not join the gate's intents** (the question of
`resolved-commits.md` §14.4). Intents name store writes a dead attempt may
have half-done, so that the next attempt can read them back. A failure
delta is a key-index file, engine metadata: it takes effect only through
the commit that names it, and an uncommitted one is discarded with the
attempt like any delta file. There is nothing to repair.

### 9.7 PostgresStore: generation fencing

Internal to the store; the engine supplies one number.

- **The generation** is the event position (`applied`) of the attempt's
  claim, carried on `AttemptLaunched` as `pin` today and written into the
  spec. It is chosen before the spec and needs no allocation. A
  generation reaches the database only once its `AttemptLaunched` is
  durable, and that event itself moves `applied` past it: every later
  claim on the scope — by this engine, or a restarted one replaying the
  event — gets a larger one. (A claim dropped before launch may share its
  number with the next claim; it never wrote.)
- **The write domain** is the output's table and the scope's partition,
  keyed by the table's OID, which survives `ALTER TABLE … RENAME`: an
  output renamed under an alias keeps its fence.
- **Bound to the invocation.** One table per database:

  ```sql
  CREATE TABLE solera_generations (relid oid, part text, generation bigint, invocation text,
                                   PRIMARY KEY (relid, part));
  ```

- **`Store.acquire(scope)`** runs before any read of the store (repair
  reads included, `resolved-commits.md` §3), in a transaction of its own:

  ```sql
  INSERT INTO solera_generations VALUES ($relid, $part, $g, $inv)
  ON CONFLICT (relid, part) DO UPDATE SET generation = $g, invocation = $inv
    WHERE solera_generations.generation < $g
       OR (solera_generations.generation = $g AND solera_generations.invocation = $inv)
  RETURNING invocation;               -- no row: a newer generation, or another invocation of this one
  ```

  Postgres locks the conflicting row even when the `WHERE` refuses, so the
  newer acquisition waits behind an older writer's open transaction and
  every later transaction of the older writer is refused. (Checked on
  Postgres 17.)
- **Every write transaction** starts by locking the row and checking that
  it still holds `(g, invocation)`; otherwise it raises before changing
  anything, and the harness records `writes: uncertain` for the earlier
  calls that returned before it.
- **Equal generation, other invocation** is refused, so a delayed
  duplicate of the newest attempt cannot write (review finding 8).
- **Cost**: one indexed upsert per acquisition and one row lock per write
  transaction; a takeover waits for at most one older transaction.

### 9.8 FileStore and S3Store: naming and collection — HELD

> **Held for Erwin's decision.** The two candidates, both `immutable`:
>
> - **`{key}/{version}`**, collected per scope under that scope's lock
>   (preferred by Erwin). Safe on the assumption that the engine's own
>   DELETEs do not land after it stopped waiting for them.
> - **`{key}/{version}.{batch}`**, unique physical names with the batch
>   recorded per key in the index. Provably safe with no lock and no
>   timing assumption; costs a key-index format change.
>
> Either way: reused versions (`v1 → v2 → v1`) must never be deleted while
> current or about to be (review finding 5); reads become pinned; values
> get unique names from the head's ref (`alpha@{attempt}.json`) with no
> index change; logical batch numbers of append outputs stay dense, their
> physical files uniquely named and listed by the commit. This section
> will specify the layout, `discard()`, collection and its costs once
> decided.

### 9.9 User `overwrite` stores

**Default: retry after a bounded wait.** When an attempt ends with
`writes: uncertain`, the scope stays blocked for `late_write_grace` (2
min by default, per store) after the last evidence of the worker — its
last report or the provider's exit, whichever is later — and is then
released. The next attempt repairs the intents as today. This is the
at-least-once contract of Airflow and Dagster, with a documented risk:
a write still in flight after the grace can land after the next commit.
Every such release is recorded on the attempt (`released: "grace"`) and
shown in the console, so the rare case is visible and countable.

**Opt-in: `strict`.** `Project(stores={"crm": CrmStore(strict=True)})`, or
`strict = True` on the class. The scope stays blocked, visibly ("waiting
for an uncertain writer"), until completion is established by one of:

- the worker's own result, sealed after its store calls returned
  (`writes: complete`), even if it arrives late;
- an operator's `solera scopes release OUTPUT SCOPE`, recorded with who
  released it;
- a store that can tell (`Store.settled(scope) -> bool`, optional): for
  example, an API whose writes carry the attempt id and that can be asked
  whether any are pending.

A worker that never returns never releases a strict scope on its own; that
is the contract's point.

## 10. Pool

```
engine   AttemptLaunched (pool: ingest, needs: {cpu: 4}), durable — no placement call
worker   GET pools/ingest/work?wait=30 (capacity: cpu 8)   → [{attempt: A, run: R, objects}]
worker   create runs/R/A.worker {invocation}                 → wins, or loses and polls again
worker   POST attempts/A/start                               → runs as any attempt
```

An attempt on a pool moves through four states. The engine keeps them in
memory and rebuilds them after a restart from the journal and `.worker`:

| State | Evidence | Leaves it by |
|---|---|---|
| **waiting** | launched, no `.worker` | a claim (`start`, or `.worker` observed); the executor's `provision` deadline, if set (none by default) |
| **claimed** | `.worker` exists, no report yet | `start` or a beat → running; no report within `claim_timeout` (60 s) of the engine first observing `.worker` → lost |
| **running** | `start`, beats, or `.worker` changing | a result, silence, cancel or timeout (§7) |
| **ended** | `AttemptFinished` | — |

- **Discovery** returns waiting attempts that fit the worker's capacity,
  oldest first. It is a hint: two workers may get the same attempt, and the
  `.worker` create decides. The engine checks `.worker` (one GET) for an
  attempt it offered and has not seen start within 10 s, so a claim whose
  worker died before `start` is noticed.
- **A dead claim expires into a new attempt id**, never a new owner of the
  old claim: the attempt ends lost (`writes: none` unless `.worker` showed
  progress past the gate), and the retry policy launches a fresh attempt,
  with its own spec and claim.
- **The runtime clock** starts at the claim when HTTP is unavailable: a
  claimant that computes through an outage, reporting by `.worker`, is
  running, not provisioning.
- **No registration, no leases, no claim event.** A pool worker's liveness
  is its attempt's heartbeat, like any worker's.
- **Engine down:** pool workers finish their attempts and get no new ones.
- The handle is `{attempt, pool}`. A cancel before the claim withdraws the
  attempt from discovery and ends it; after it, cancel is §7.

## 11. Observations — HELD

> **Held for Erwin's decision:** observations as ordinary attempts in v1
> (one review's advice: no new protocol), or the spec-less fast path
> (`per-key-processing.md` §12): launched without `.spec`, `.worker` or
> `.result`, with an in-memory invocation claim at `start`, the spec in the
> `start` answer, a bootstrap credential delivered with the launch, and
> history rows for skipped observations that a crash may lose. This
> section will specify the chosen path, its claim (which
> `resolved-commits.md` §4 validates resolves against) and its bootstrap.

These hold either way:

- **"Unchanged" means no durable change**: no new version, no key change,
  and no new cursor. A cursor-only observation commits the cursor to the
  journal (no delta file, no new version, no consumer woken); a truly
  unchanged one commits nothing (review finding 9).
- An observation writes no store data, so it needs no gate and no repair;
  its source commit is the engine's.
- An observation in flight at an engine restart may be rerun by the next
  tick; its source's cursor did not move, so nothing is lost.

## 12. Engine restart, attempt by attempt

| The attempt was | The next engine |
|---|---|
| preparing (no `AttemptLaunched`) | dispatches the task again; the orphan spec, if any, goes with the run |
| launched, no handle recorded | `resume` (ECS, Kubernetes: launch again by name). Local and Modal cannot be found by name: the engine follows `.worker`, and a launch that never happened ends at the provisioning deadline, as lost, retried under a new attempt id |
| provisioning | follows the handle; keeps the clamped remainder of its allowance (§8) |
| claimed or running | rebuilds the binding from `.worker`; follows the handle; beats arrive again; if not, reads `.worker` |
| canceling | the cancel is durable for a canceled run (`RunControlled`); a timeout is re-derived from the restarted clocks. Either way it starts at phase 1 again, which a draining worker answers with its result |
| done, result written | reads `.result` and settles it |
| aborted by the old engine (gate `aborted`) | ends it as the old engine would have, under §9.6 |
| an observation | §11 |

## 13. What an attempt costs

At S3 list prices (PUT $5, GET $0.40 per million) for scenario E of
`key-index-costs.md` — one short attempt every 10 s, 259,200 a month —
counting what the attempt itself costs, apart from its key-index reads and
delta file (`resolved-commits.md` §9). A short attempt here logs a few
lines and sends no periodic heartbeat to the object store.

| Per attempt | Today (counted from the code) | Target, `immutable` store | Target, `fenced`/`overwrite` |
|---|---|---|---|
| Engine PUTs | spec · journal: launch, placed, finished = 4 | same = 4 | same = 4 |
| Engine GETs | beats while provisioning ~1, result 1 = 2 | result 1 | result 1 |
| Worker PUTs | beat, gate, log chunk, joined log, result, done beat = 6 (more chunks while it logs: one per 2 s) | claim, log chunk, result = 3 | + gate = 4 |
| Worker GETs | spec, fence read by the beat = 2 | spec 1 | spec 1 |
| Worker DELETEs | chunks 1 (free) | — | — |
| **Requests** | **10 PUT + 4 GET** | **7 PUT + 2 GET** | **8 PUT + 2 GET** |
| **Per month** | **$13.38** | **$9.28** | **$10.58** |

Shared by all attempts, not per attempt: history flushes (one Parquet
file per table per minute, ~6 PUTs a minute: ~$1.30 a month, merges
extra), `engine/alive.json` every 30 s while runs are live (~$0.43), and
checkpoints (negligible here). The older model's $6.69 (4 PUT + 2 GET +
one journal PUT) left out the beats, the gate, `AttemptPlaced` and the
done beat; the measured today is about twice it.

Where the rest could go, in order of value:

- **`AttemptPlaced` without a flush of its own** (−1 PUT, −$1.30): record
  it lazily, to ride the next segment (`AttemptFinished`'s, for a short
  attempt). A crash before then loses only the handle, which `resume` or
  the worker's reports recover (§12).
- **No chunk when the result can carry the log** (−1 PUT): a short log
  (under 64 KB) travels inside `.result`.
- **The spec in the `start` answer** saves the worker's GET (−$0.10); not
  worth a second path.

With both of the first two, a short attempt on an immutable store costs
5 PUT + 2 GET: **$6.69 a month**, today's model figure, now as a measured
count rather than an underestimate.

## 14. What changes from today

| Today | Target |
|---|---|
| `{attempt}.json`: spec, then overwritten with spec + result + log index | `.spec` and `.result`, both immutable |
| `.beat` every 30 s with a fence GET, then a done beat | HTTP beat every 10 s; `.worker` only while HTTP fails; `.result` means done |
| no invocation identity; duplicates overwrite each other (R3) | `.worker` claim; losers write nothing and wait for the owner |
| log chunks joined at the end, chunks deleted | immutable chunks every 30 s / 1 MB, indexed from the result; live lines over HTTP |
| Pool: register, claim, renew, complete; in-memory leases; `AttemptClaimed` | long-poll discovery; `.worker` claim; four states; claim expiry into a new attempt |
| cancel read from the fence by every beat; engine aborts at once | two-phase cancel: requested and drained, then forced |
| any presumed death releases the scope (R1) | per store kind: `immutable` and `fenced` at once, `overwrite` after a bounded wait or held (`strict`) |
| a failed result releases the scope | a failure after the gate is uncertain completion |
| repair before the store is fenced | `Store.acquire` before repair, for fenced stores |
| resolve through `.ask` objects (proposal) | binary `resolve` route, nothing persisted |
| observations as full attempts | held (§11) |
| one API token | admin token; per-attempt HMAC token from a stable secret; per-pool token |

## 15. Open questions

1. **`late_write_grace`.** 2 minutes is an operational value, not a bound;
   per-store overrides let a store with known client and server timeouts
   pick its own.
2. **`cancel_grace`.** 60 s by default; per asset, since a page of
   16 concurrent calls into a slow API may need longer to drain.
3. **Strict stores during an engine outage.** A worker on a strict store
   keeps writing on its launch authorization; nothing changes for it. An
   attempt ended uncertain stays blocked across the restart, as recorded.
4. **Lazy events in the journal** (§13): a buffered event that does not
   start the flush timer is a small journal feature, useful beyond
   `AttemptPlaced`.
