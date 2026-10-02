# The attempt lifecycle

Status: **built** (milestones 1, 2 and 5), but for the §9.8 sweep of what a
worker writes after its attempt ended, and sensors' host-side resolution
of bigger maps (§11.7). Where the build departs from the text, it says so
in place. In order: the records of §2 (`solera/lifecycle.py`), attempt
objects and claims (§2–§4), the channel (§5: `solera_server/attempts.py`,
`solera_worker/channel.py`), heartbeats as evidence (§6), the two-phase
cancel (§7), the clocks (§8), retained gates (§2.4), Pool (§10), store
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
Observations are sensors (§11), which are not attempts at all.

It assumes what is built: fence segments kept for good and carrying a
writer nonce, create-only writes that recognize their own bytes
(`solera.objects.create`), results sealed once, durable retirement,
event-position garbage pins, recorded placement handles, per-placement
"can't tell", a provisioning deadline, and a timeout that runs from the
worker's first report.

Its companions: `resolved-commits.md` (the resolver on this channel, and
the write phases of a keyed output), `per-key-processing.md` (pages,
failure indexes, sensors' sources).

**This doc is the authority for four records the others use:** the cancel
record (§2.2), write-completion evidence (§2.3), the sensor snapshot
(§11.3) and accepted tick outcomes (§11.4). The key index entry's
`(version, locator)` and the delta's predecessor are named here (§9.8) and laid
out in bytes in `key-index-format.md`.

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
                                         runs/R/A.writing ◀── gate (fenced stores)
                                         output data      ◀── store writes
                                         runs/R/A.log.000000…  ◀── log chunks
                                         runs/R/A.result  ◀── create-only, sealed
  ◀──────────────────────────────────────────────── POST attempts/A/finished (hint)
  GET .result, validate, AttemptFinished, durable
```

## 2. Objects and records

### 2.1 Attempt objects

All under `runs/{run}/`, all named after the attempt. Only the claim's
winner writes anything but the spec (§4), so no name needs the invocation
token.

| Object | Written by | Mode | Meaning |
|---|---|---|---|
| `{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | what to run: today's `spec`, plus `engine` (HTTPS URL), `token` (§5.2), `generation` (§9.7) |
| `{attempt}.worker` | the invocation that claims it | created once; then overwritten only by its owner, only while HTTP fails (§6) | the claim `{"invocation", "host", "pid", "at"}`; later also `{"seq", "timeline", "usage"}` |
| `{attempt}.writing` | worker before its first store write, or engine to abort or close | create-only | the gate: `writing` (with `invocation` and intents), `aborted` or `closed`; only for attempts with outputs on `fenced` stores (§9.6). Outlives its run (§2.4) |
| `{attempt}.log.{n:06d}` | worker | create-only, immutable | a gzip member of the log, flushed every 30 s or 1 MB; a short log has none (§13) |
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
  is running, so there is nothing to drain). The failure index records
  interrupted keys by it (`per-key-processing.md` §5): `user` keys are
  dormant, `timeout` keys count a try and come due.
- **Precedence**: `user` over `timeout` over `provisioning`. A user cancel
  arriving during a timeout's drain changes the reason; a timeout during a
  user cancel's drain changes nothing.
- `since`: the event position at which the engine latched it, so a
  restarted engine's re-derived cancel (§12) can be told from the first.
- **Delivery**: every beat answer carries the current record (or `null`);
  the worker latches the strongest it has seen (§5.3).
- **In the result**: the worker copies the record it sealed with into
  `.result`. That record decides how its failure delta treats interrupted
  keys; a reason latched after the worker sealed does not rewrite it. The
  engine accepts a `canceled` result while the phase is `requested`, and
  refuses it once `forced`.

### 2.3 Write-completion evidence

Whether an ended attempt's writes can still land: `none`, `complete` or
`uncertain`. The release rules of §9.6 read nothing else.

| Source | Establishes |
|---|---|
| the worker, in `.result` | `none` if it made no store call; `complete` if every `store.store()` call returned; `uncertain` if one raised, was cancelled or abandoned — **any store exception after the gate is uncertain**: a client timeout may hide a backend that completed |
| the engine, ending an attempt without a result: a create-only `aborted` gate | **wins** → `none`: the worker never took the gate, and now never can |
| | **finds `writing`** and no conclusive result → `uncertain` |
| | **finds `aborted` or `closed`** → what the attempt that wrote it recorded |
| | **cannot get an answer** (S3 unreachable) → not established: the engine retries, and treats the attempt as `uncertain` until it can |

Never from progress, silence or provider state: a worker can take the gate
and enter a store call before its next report. Attempts whose outputs are
all on `immutable` stores take no gate; their evidence is the result's, or
`uncertain`, which their release rule ignores (§9.6).

### 2.4 Gates outlive their runs

A delayed worker can resume long after its attempt ended: it read its
spec, paused, and comes back after its run directory was deleted. Its
claim and its gate are gone, so it could claim again, take a new gate and
write — and a `fenced` store that never saw its generation would accept
it. So the gate is the attempt's **tombstone**:

- When the engine ends an attempt with outputs on `fenced` stores and no
  gate exists, it creates one, `closed` (create-only; a
  `writing` found there is the worker's and stands).
- Retention deletes a run's directory **except its gates**, which it keeps
  for `gate_days` (30) beyond the run. (A `fenced` store also refuses an
  old generation by itself once a later one was acquired; an attempt whose
  gated outputs are all fenced can lose its gate as soon as a later
  attempt on the same write domain has committed.)
- A worker takes its gate before its first store mutation (§3), so a
  delayed worker finds `aborted` or `closed` and writes nothing. A
  generation acquired by it (§9.7) changes nothing: acquisition is not a
  mutation of output data, and the next real acquisition supersedes it.

What is left is a worker paused for more than `gate_days`; the doc does not
claim more.

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
7. **Compute**, beating every 10 s. Logs go live over HTTP and durably as
   chunks.
8. **Acquire** (fenced stores): `Store.acquire(scope)` takes generation
   `g` for the output's write domain (§9.7) — after computing, before any
   repair read or mutation.
9. **Plan** each keyed output (`resolved-commits.md` §3): repair reads
   (and the full reconciliation of an unknown-writes intent, §9.6),
   resolve (`POST attempts/A/resolve`, or locally), upload the delta file.
10. **Gate**: create `A.writing` with the intents (fenced stores only). A gate already there — `aborted` or `closed` — means the
    engine ended the attempt: write nothing (§2.4).
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

- **The loser waits for the owner to finish**: every 30 s (rare, since
  duplicates are) it looks for `A.result` and a terminal gate (`aborted`
  or `closed`: the engine has ended the attempt), and asks the engine with
  a beat. It exits 0 on any of them, or on `409 ended`; `409 not_owner`
  means the attempt is live under its owner, and it waits on. It never
  writes anything, not even a log.
- **The engine treats a provider exit as the owner's only if the owner has
  also fallen silent.** On an exit without a result, the engine checks the
  bound invocation's last report: one within three beat intervals means
  the exit was another invocation's, so the engine keeps waiting on the
  owner's reports (and drops the handle, which named a duplicate). With no
  invocation bound yet, it reads `.worker` first. (Built: only reports
  received over the channel count here — a `.worker` read now may have been
  written before the exit — so a worker reporting only through `.worker`
  has its duplicate's exit taken for its own; its gate then stops it.)

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
| `GET sensors/next?wait=30` | revision → `[{tick, sensor, cursor, snapshot}]` | sensor hosts (§11) |
| `POST sensors/{s}/ticks/{t}` | the tick's outcome (§11.4); key maps in the resolver's framing → `{runs}` · `409` | sensor hosts |

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
  token. Pool workers get the stage from discovery (§10). Sensor hosts
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
  wait; claiming still takes the object store. (Built: one
  `SOLERA_POOL_TOKEN` for every pool, or the admin token.)
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
  succeeds again, and reads its gate each time: an `aborted` or `closed`
  gate is the one cancel that reaches a worker the engine cannot answer.
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

**Cancel is two phases,** carried by the cancel record (§2.2). A user
cancel and a timeout share them; the record's `reason` decides what
happens to the work left undone.

1. **Cancel requested.** The engine latches `{phase: requested, reason}`
   and answers it to the next beat (≤ 10 s). The worker stops starting new
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
     today;
   - either way the result carries the cancel record it sealed with.

   Result submission stays authorized throughout: the attempt is live
   until the engine records its end.
2. **Forced abort,** after `cancel_grace` without a result. The engine
   advances the record to `forced`, cancels at the provider, and ends the
   attempt: for a gated store it creates `.writing` as `aborted` first and
   classifies the writes from what it finds (§2.3). From then on a drain
   result is refused (`409 ended`) and its objects are garbage.
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
observing `.worker`. A 20-minute image pull does not eat a 30-second
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
only on a store that writes in place — which is why such a store must be
`fenced`.

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
| Detect and retry | at-least-once execution; late writes possible | **dropped**: a write in flight past any wait can still land after the next commit |
| Hold on uncertainty | no late write can follow a release, at the price of scopes blocked until completion is established | **dropped** with it: an approximation of what a store can guarantee itself |
| Self-fencing leases | risk reduction under timing and authority assumptions | **rejected**: the review showed it fails without exceeding its own margins (a superseded engine still granting renewals; a failed result bypassing the drain; renewals by `.worker` defeating cancel) |
| Generation checked by the store | exact exclusion of older writers, when acquisition precedes reads | PostgresStore (§9.7); user stores declaring `fenced` |
| Immutable outputs | stale writes are unreferenced garbage; pinned reads | FileStore, S3Store (§9.8); user stores declaring `immutable` |

### 9.4 The decision in one paragraph

A store declares how it writes: `immutable` or `fenced`; registration
refuses anything else, and a store that does not implement `discard` or
`acquire`, respectively. FileStore and S3Store are `immutable`,
PostgresStore is `fenced`, and a SQL store gets fencing in one line
(`solera.fencing.fence`). Scopes are released as soon as the engine ends
an attempt, whatever happened to its worker, and workers keep writing
through an engine outage: a writer the engine gave up on can no longer
change what a newer one committed.

### 9.5 Knowing whether writes completed

What the next attempt must repair reads one thing: the attempt's
write-completion evidence, `none`, `complete` or `uncertain`, defined in
§2.3. Two of its
rules carry the weight here. **Any store exception after the gate is
uncertain**: a client that timed out after one second may see its request
finish at the backend five seconds later, and a store author cannot be
asked to tell the two apart; so a failed result after the gate leaves its
intents for the next attempt to repair. And **the engine classifies an
attempt without a result from its gate**, never from progress or
silence.

### 9.6 Store kinds

```python
class MyStore(Store):
    writes = "immutable"   # writes only names nothing committed references; implements discard()
    writes = "fenced"      # implements acquire(); every write checks the generation atomically
```

| Kind | Gate and intents | Repair | Scope released when the engine ends the attempt | A read sees |
|---|---|---|---|---|
| `immutable` | none | none: abandoned writes are unreferenced | at once | the pinned version, exactly |
| `fenced` | gate with intents (repair, and the unknown-writes intent of `Sql`, below) | after `acquire` (`resolved-commits.md` §3) | at once: the next attempt's acquisition fences the old writer | current rows |

**Reads.** Only an immutable store can return a pinned version after a
newer one committed: its names are never reused. A fenced store keeps
one copy, so its loads read current rows: a run may read two
outputs at different moments, and a row changed since its pin is read in
its newer form and delivered again with its own change, a harmless repeat
(`architecture.md` §3, "What a read sees"). Fencing makes writes safe; it
does not make reads repeatable. No setting changes this.

**Unknown writes** (`Sql`). A `Sql` write's keys are known only from the
key map the store reports after it committed, so its gate records an
intent with no key list: *unknown writes*. If its attempt dies after the
gate, the next attempt cannot repair by reading named keys — there are
none. For example, the statement deletes `a` and inserts `b`, commits, and
the worker dies before reporting its map; the next attempt patches `c`.
So, after acquisition (§9.7), an unknown-writes intent is settled only by
reading the store's **whole key map for the scope** and reconciling it
with the index: every key the index and the store disagree on joins the
commit's delta. A full replacement may instead rewrite the whole scope,
which settles it too. `resolved-commits.md` §3 carries the same branch for
the resolver's write phases.

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

- **`Store.acquire(scope)`** runs after the producer computed and before
  any repair read or mutation (`resolved-commits.md` §3), in a transaction
  of its own:

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
  anything (a store exception after the gate: `uncertain`, §2.3).
- **Equal generation, other invocation** is refused, so a duplicate of the
  newest attempt cannot write. An attempt the engine ended before it
  acquired is stopped by its retained gate (§2.4), not by the database,
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
  anything, it takes the attempt's own slice: an older attempt's migration
  is refused. One that **replaces the relation** (create, copy, drop,
  rename) gives the table a new OID; it moves the fence rows to the new OID
  in the same transaction, so the write domain keeps its generations. An
  operator's `solera migrate` has no generation: it only takes its turn.
- **Only a migration may replace the relation.** A `Sql` statement that
  leaves a new OID behind is rolled back with an error: a fence row would
  not follow it.
- **Cost**: one indexed upsert per acquisition and one row lock per write
  transaction; a takeover waits for at most one older transaction.

### 9.8 FileStore and S3Store: unique names, collected without a lock

**Decided** (Erwin): every object the store writes has a name no other
attempt will ever write, so deleting one can never hit something current.

**Names.** The physical name carries the logical version and the
attempt's **generation** (§9.7: the claim's event position, in the spec):

```
site_files/alpha/f-1/9c41e0d2….184467.json       a key, at its version, by generation 184467
site_status/alpha@184467.json                    a value
site_events/alpha/000000000042.184467.json       batch 42 of an append output, by generation 184467
```

Writes are create-only. Two writers of one name are the same attempt (a
delayed duplicate of itself), so they write the same bytes. A declared
`revision` longer than 32 bytes is named by the first 16 bytes of its
SHA-256 instead.

**Why the generation, not the batch.** A retry reuses its predecessor's
batch number: W1 (batch 57) dies having written `f-1/v.57`, and its retry
W2, also batch 57, may write that very name and commit it. W1's leftovers
could then be judged only once batch 57 is committed, and only by diffing
them against the committing delta. A generation is never reused, so an
attempt that ends without committing leaves objects nobody else names:
they can go at once. It costs the same one integer per index entry.

**The records.** Names are settled here; bytes in `key-index-format.md`.

- **Index entry:** `(key, version, locator, deleted)`. `locator` is the
  generation that wrote that key at that version (a varint, ~4–5 bytes
  before compression; neighbouring entries share generations and compress
  well). Deciding whether a write is a change still compares versions
  only.
- **Delta entry:** the same, plus an optional **predecessor**
  `(version, locator)` — the entry it replaces or deletes — when the
  writer read it. Compaction drops predecessors.
- **Everywhere an entry travels, the locator travels with it:** resolver
  responses (the delta file), inline pages and summaries
  (`resolved-commits.md` §7–8: `{key: [version, locator]}`), and the
  `Keys` selection a store receives (`{key: (revision, locator)}`).
- It rides the `.kx` format bump of the row-digest work.

**How loads find names.** A keyed load computes every name from `Keys` and
needs no LIST. A full load of a keyed input becomes a `Keys` selection the
harness pages from the pinned index (the spec pins the index of every
keyed input, as it does for incremental ones); stores never read an index.
A value's name is in its head's ref. An append output's range load lists
the batches' prefix and keeps, per batch, the file with the highest
generation: the attempts that used batch `n` all ran between the commits
of `n − 1` and `n`, one at a time, and the one that committed `n` was the
last of them.

**Predecessors.** Collecting a superseded object needs its exact name,
so its predecessor's `(version, locator)`. `resolved-commits.md` §6
chooses two triggers, and this section's collection follows them:

- **At resolution, whenever the old entry was read** — always on the
  engine and in the streaming merge-join, and for the sparse reader's
  maybe keys — the delta names the predecessor.
- **At compaction, for the rest.** A key the pair filter cleared has no
  named predecessor, but its old entry is still in the index, shadowed.
  Every merge that drops an entry (shadowed, or under a bottom-level
  tombstone) emits it as data garbage, named before or not: names are
  never reused, so discarding one twice is a no-op.

So the cold path keeps its cost, and collection is prompt wherever the old
entry was read, deferred to compaction elsewhere.

**Reader pins.** An object or index file may go only when no reader can
still need it. The pins, all by event position (`object-store-state.md`
§6):

- every live claim (as today);
- **durable multi-attempt reads**, recorded with their pin in the
  watermark state, so the pin holds in the gaps between attempts and
  across engine restarts, until the read ends:
  - a **paged delta window**: an `Incremental` edge delivering one pinned
    window `from…to` over several attempts (`after` set). A later commit
    may supersede a key inside the window, and an attempt launched after
    that commit would not otherwise cover the version the window still
    delivers;
  - a **rescope drain** and a **retry pass** (`per-key-processing.md`),
    each reading one pinned snapshot across many attempts.

**Collection.** Only from durable decisions; never from what a listing
shows.

- **Superseded versions, named at resolution.** A commit records one
  data-garbage entry for its delta file at its event position. Once no
  pin predates it, the engine calls `store.discard(output, scope, names)`
  with the delta's predecessors, in batches of 1,000; then the entry goes.
  A superseded value is one name from the previous head's ref.
- **Entries dropped by compaction.** An `IndexCompacted` records, at its
  event position, one data-garbage entry naming the dropped entries (in a
  sidecar file the compaction writes, not in the event); they are
  discarded under the same pin rule. A name already discarded through a
  predecessor is discarded again, harmlessly.
- **Attempts that ended without committing.** Their names carry their own
  generation, which no other attempt uses, and their `AttemptFinished`
  without a commit is durable: the engine discards the names in their
  uploaded delta file, their value and their batch file.
- **The sweep**, occasional, for what a worker still running after its
  attempt ended (given up on, or a duplicate) wrote later. It lists a
  scope and deletes a name only if its generation belongs to an attempt
  with a durable end and no commit (the history records each attempt's
  generation). A name with any other generation — committed, in flight,
  unknown — is never swept: "not current in the index" is not garbage (an
  attempt about to commit has uploaded names the index does not hold yet;
  a pinned reader needs superseded ones).

**As built.** The engine holds no store credentials and runs no user
code, so workers discard, twice over:

- **Right after a commit** (D8). The worker's `finished` waits until the
  engine settled the attempt and made its commit durable; the answer
  names the entries now due in its scope — what the commit let go of that
  no reader pins, and what was waiting — with each output's head. The
  worker calls `store.discard` with them and acknowledges by entry id
  (`POST attempts/{a}/discarded`, recorded as `DiscardsDone`). So a
  partition that never runs again keeps no garbage.
- **By the scope's next attempt**, the fallback for whatever the first
  missed: the engine unreachable, the worker gone before it
  acknowledged, a reader still pinned. The engine puts the due entries (at
  most 64) in the spec's output info, and the worker, after its own store
  call succeeds, discards them and reports which in its result;
  `AttemptFinished` then removes them.

Deleting a name twice is no harm, and only the index files an
acknowledged entry names become garbage. Due means no reader pin that
may read the entry's output scope predates it: pins are per output scope,
each named by its index prefix. An attempt's claim names the scopes it
reads and writes (every scope, while it is still preparing); a paged
window or a rescope drain its upstream; a sensor tick its sources; an
engine reader what it reads (`history/` for a history query). Index and
history files are collected by the same rule, by their paths, so one slow
reader holds back only what it reads. A delta file
a pending entry reads is kept, even once the index let go of it, until the
entry is done. An entry whose files cannot be read stays pending; after
three such attempts it is `stuck`: no longer handed out, listed in
`/api/diagnostics` and on its scope's head record, until an operator runs
`solera scopes discards OUTPUT [SCOPE] --clear` (its objects stay). An
abandoned attempt's keyed names come from listing its own delta files
(`{batch:012d}-{attempt}*` under the index prefix, complete because
deltas are uploaded before data), not from a sweep; those delta files and
consumed compaction sidecars then go through the ordinary index garbage.
A rescope drain's snapshot pin (`watermark.rescope.pin`) holds both
index-file garbage and data discards, as a live claim does; a retry pass
needs none, since each of its pages reads the state of its own prepare
(`per-key-processing.md` §20). Not built: the sweep, so a worker that
writes after its attempt ended leaves orphans.

Reusing a version (`v1 → v2 → v1`) writes a new name (`f-1/v1.{g3}`):
deleting the old `f-1/v1.{g1}` cannot touch it. That is what removes the
GC lock, the dequeue-on-reuse rule and the DELETE timing assumption of the
alternative.

**Costs.** Per changed key: one PUT, as today, and one DELETE (free on
S3) when superseded, batched. Per commit: the delta's previous-version
columns (a few bytes per changed key). Per full load of a keyed input: a
read of its pinned index. In return: no gate, intents or repair for these
stores (§9.6); readers see the version they pinned; a stale write is an
orphan.

**Readable listings.** A key's directory holds its current object, plus
superseded ones until collection catches up (minutes, behind the oldest
reader pin). `solera data get OUTPUT KEY` resolves the current one.

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
| **claimed** | `.worker` exists, no report yet | `start`, a beat or a change of `.worker` → running; none within `claim_timeout` (60 s) of the engine first observing `.worker` → lost |
| **running** | `start`, beats, or `.worker` changing | a result, silence, cancel or timeout (§7) |
| **ended** | `AttemptFinished` | — |

- **Discovery** returns waiting attempts that fit the worker's capacity,
  oldest first. It is a hint: two workers may get the same attempt, and the
  `.worker` create decides. The engine checks `.worker` (one GET) for an
  attempt it offered and has not seen start within 10 s, so a claim whose
  worker died before `start` is noticed.
- **A dead claim expires into a new attempt id**, never a new owner of the
  old claim: the attempt ends lost, and the retry policy launches a fresh
  attempt, with its own spec and claim. Its writes are classified from its
  gate (§2.3), never from what `.worker` showed: the claimant may have
  taken the gate and entered a store call without reporting again. The
  engine's `aborted` create wins → `none`; it finds `writing` → `uncertain`,
  and its intents wait for the next attempt's repair; S3 cannot answer →
  the engine cannot establish `none`, and retries before releasing
  anything.
- **The runtime clock** starts at the claim when HTTP is unavailable: a
  claimant that computes through an outage, reporting by `.worker`, is
  running, not provisioning.
- **No registration, no leases, no claim event.** A pool worker's liveness
  is its attempt's heartbeat, like any worker's.
- **Engine down:** pool workers finish their attempts and get no new ones.
- The handle is `{attempt, pool}`. A cancel before the claim withdraws the
  attempt from discovery and ends it; after it, cancel is §7.
- **One process per attempt.** A pool worker imports the project once, in
  a forkserver, and runs each attempt it gets in a child forked from it.
  The forkserver starts no thread and never uses the object store, so
  forking from it is safe on any OS (the worker itself has threads: fork
  copies only the forking one, which macOS does not survive) and every
  child starts warm. The child is the attempt's process: a forced cancel
  ends it, and it exits the moment its result is published.

**Every attempt process exits at once** (`os._exit`) once its sealed
result is published, or once it cannot be: Local, ECS and Kubernetes
workers as much as pool children. A thread the attempt gave up on — a
synchronous `Each` call canceled mid-flight, say — would otherwise keep
the process, and its placement, alive until it returned. (Modal runs the
harness as a function in a container of its own: not this exit.)

## 11. Sensors

**Decided** (Erwin): frequent checks that may or may not lead to a change
are **sensors**, Dagster's concept, not a variant of attempts. A tick is
not an attempt: no `.spec`, `.worker`, `.result`, gate or run of its own,
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

In a **sensor host**: a long-lived process with the project loaded, like
Dagster's code server, so a tick costs a function call, not a process
start and an import.

- **Local (default).** When the project declares sensors, the engine
  keeps one sensor host subprocess alive beside it, restarted with backoff
  if it exits and replaced when a new revision is served.
- **Pool.** `solera_worker sensors --pool NAME` on any machine: the same
  host, remote, for sensors that need a network the engine cannot reach or
  that should not run beside it. `executor=` on the sensor picks.

A host long-polls `GET sensors/next?wait=30` with its pool token and its
project revision; the engine answers with due ticks for sensors on that
executor and revision: `{tick, sensor, cursor, snapshot}` (§11.3). The
host runs the body (up to `concurrency` ticks at once, each within the
sensor's `timeout`, 60 s by default) and posts the outcome to
`POST sensors/{sensor}/ticks/{tick}`.

### 11.3 The sensor snapshot

What a tick observed against, sent with it, per declared source:

```json
{"uploads": {"head": "h:184702", "index": {"prefix": "keys/uploads/_/", "files": ["…"], "log": ["…"]}}}
```

- **`head`** is an opaque, monotonic identity of the source's current
  head: the event position at which that head was installed (by
  registration, an API commit or a sensor). Every source kind has one,
  including unkeyed sources whose version is a string: a tick that saw
  `v1` while an API client committed `v2` meanwhile carries a stale `head`
  and is refused, whatever its version says.
- **`index`**, for keyed sources, is the pinned key index — the same
  record a spec pins — so a host that resolves a big map itself reads
  exactly that file set.
- **The tick holds a reader pin** at its dispatch position (§9.8) until
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
   commits will install (a partition set's new elements), never touching
   the model. The sources' heads are checked again after.
7. **Record everything at once**: the source commits, the run submissions
   (`command = {tick}/{n}`, so the run receipts deduplicate them too),
   `SensorAdvanced {sensor, cursor}` if the cursor moved, and the new
   accepted-outcome record — one `record()`, so one journal segment holds
   all or none of it — and answer once it is durable. A tick whose
   outcome is nothing records nothing.

"Unchanged" therefore means no version, no key change and no new cursor
(review finding 9). A cursor-only tick records only `SensorAdvanced` and
its accepted-outcome record: no delta, no new version, no consumer woken.

**Key maps.** A small map (up to `sensor_map_max`, 1M keys) is posted as a
sorted run in the resolver's framing (`resolved-commits.md` §4), and the
engine resolves it in-process against the snapshot's index, as it does for
API commits. A bigger one is resolved by the host — the streaming
merge-join over the snapshot's `index` — which uploads the delta file
under a name that includes the tick, and posts a **delta reference**
`{files, batch}`. Installed, it is the source's delta like any other. A
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

Compared with the spec-less observation attempt of the previous draft:

- no launch path without a spec, and no attempt that is not journaled;
- no per-launch bootstrap credential, no in-memory invocation claim per
  observation, no observation result route;
- no process start per check: a 300-second sensor on `Local` cost a
  subprocess and an import every tick;
- no `skipped` runs, attempts and `kind: observe` retention class: checks
  that found nothing are tick rows, not runs;
- no resolve route validated against an observation claim: small maps
  resolve in the engine, big ones on the host.

What it adds: the sensor host (one long-lived process kind, which Pool
workers resemble), two routes, a cursor per sensor in the engine's state,
and the `ticks` table.

**Risks.** User code in a long-lived process can leak memory or state
between ticks, and a body that hangs holds a host thread: hosts are
restarted when ticks overrun or after `host_max_ticks` (10,000). A host
on an old revision gets no ticks.

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
- **Reader pins.** A tick's pin joins the claims' and the paged windows'
  in one floor, which both data and index garbage respect.

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
  `.result` (`log.tail`) if they are under 64 KB, else into one last
  chunk. An attempt shorter than 30 s that logs a little writes no log
  object at all; its lines were live over HTTP meanwhile.

| Per attempt | Today (counted from the code) | Target, `immutable` store | Target, `fenced` |
|---|---|---|---|
| Engine PUTs | spec · journal: launch, placed, finished = 4 | spec · journal: launch, finished = 3 | 3 |
| Engine GETs | beats while provisioning ~1, result 1 = 2 | result 1 | 1 |
| Worker PUTs | beat, gate, log chunk, joined log, result, done beat = 6 (more chunks while it logs: one per 2 s) | claim, result = 2 | + gate = 3 |
| Worker GETs | spec, fence read by the beat = 2 | spec 1 | 1 |
| Worker DELETEs | chunks 1 (free) | — | — |
| **Requests** | **10 PUT + 4 GET** | **5 PUT + 2 GET** | **6 PUT + 2 GET** |
| **Per month** | **$13.38** | **$6.69** | **$7.98** |

Shared by all attempts, not per attempt: history flushes (one Parquet
file per table per minute, ~6 PUTs a minute: ~$1.30 a month, merges
extra), `engine/alive.json` every 30 s while runs are live (~$0.43), and
checkpoints (negligible here). The older model's $6.69 (4 PUT + 2 GET +
one journal PUT) left out the beats, the gate, `AttemptPlaced` and the
done beat: today really costs about twice it, and the target lands on it
as a count rather than an underestimate. A longer attempt adds one chunk
per 30 s of logging; one that loses HTTP adds three `.worker` PUTs a
minute. An attempt on a gated store that ends without taking its gate
(it wrote nothing) costs the engine one PUT for its `closed` tombstone
(§2.4), in place of the worker's gate.

Not worth a second path: putting the spec in the `start` answer would save
the worker's GET ($0.10 a month).

**Sensors** cost none of this: a tick that finds nothing is a long-poll
answer and a post, with no object request and no journal write; one that
commits pays the source commit (a journal PUT, and a delta file for keyed
sources).

## 14. What changes from today

| Today | Target |
|---|---|
| `{attempt}.json`: spec, then overwritten with spec + result + log index | `.spec` and `.result`, both immutable |
| `.beat` every 30 s with a fence GET, then a done beat | HTTP beat every 10 s; `.worker` only while HTTP fails; `.result` means done |
| no invocation identity; duplicates overwrite each other (R3) | `.worker` claim; losers write nothing and wait for the owner |
| log chunks joined at the end, chunks deleted | immutable chunks every 30 s / 1 MB, indexed from the result; live lines over HTTP |
| Pool: register, claim, renew, complete; in-memory leases; `AttemptClaimed` | long-poll discovery; `.worker` claim; four states; claim expiry into a new attempt |
| cancel read from the fence by every beat; engine aborts at once | two-phase cancel: requested and drained, then forced |
| any presumed death releases the scope (R1) | every store is `immutable` or `fenced`: released at once, the older writer unable to write |
| a failed result releases the scope | a failure after the gate is uncertain completion; an attempt without a result is classified from its gate (§2.3) |
| gates deleted with their run | gates retained `gate_days` beyond it as tombstones; an attempt that took none gets a `closed` one (§2.4) |
| cancel and timeout indistinguishable to the worker | a latched cancel record with phase and reason, carried into the result (§2.2) |
| repair before the store is fenced | `Store.acquire` before repair, for fenced stores |
| resolve through `.ask` objects (proposal) | binary `resolve` route, nothing persisted |
| observable sources as proposed attempts without a spec | sensors: ticks in a warm host, applied through the commit and run APIs, `SensorAdvanced` for the cursor, a lossy `ticks` table |
| `{key}.json` overwritten in place (FileStore, S3Store) | `{key}/{version}.{generation}.json`, create-only; superseded and abandoned names collected without a lock |
| one API token | admin token; per-attempt HMAC token from a stable secret; per-pool token |

## 15. Open questions

1. **`cancel_grace`.** 60 s by default; per asset, since a page of
   16 concurrent calls into a slow API may need longer to drain.
2. **`sensor_map_max`.** Where a key map stops being posted to the engine
   and is resolved on the host instead; from the resolver's grid
   (`resolved-commits.md` §6), like its other thresholds.
3. **Locator size.** The `.kx` bump adds a generation per entry; the
   index benchmarks should report bytes per entry with it, at 1M–100M
   keys, before the format is frozen.
4. **`gate_days`.** 30 days bounds how long a paused worker can resume and
   still be stopped by its gate. A backend-recorded revocation would
   remove the bound for stores that can keep one; none is needed for v1.
