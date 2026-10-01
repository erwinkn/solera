# The attempt lifecycle — target design

Status: **proposed**, not built. It replaces the attempt files, heartbeat,
pool protocol and write-safety rules of `object-store-state.md` §8 and
`architecture.md` §10, and settles decisions D2–D4 of the lifecycle review.
§9 investigates D1, which is **not decided**, and recommends an answer.

It assumes what round one built: fence segments kept for good, create-only
writes that recognize their own bytes (`solera.objects.create`), results
sealed once, durable retirement, event-position garbage pins, recorded
placement handles and a provisioning deadline.

## 1. The shape in one paragraph

Durable facts live on the object store; signals go over HTTPS. The engine
writes an immutable **spec**; the worker that wins a create-only **claim**
(`.worker`) runs it and seals an immutable **result**. Everything between
— heartbeats, live logs, cancel, resolve, pool discovery — is a request to
the engine, which is never required: if the engine is down, workers keep
working, write their claims, deltas, logs and results to the object store,
and the next engine settles them from there. A lost message costs latency,
never an outcome.

```
engine                                   object store                         worker
  pin inputs, PUT .spec  ───────────────▶ runs/R/A.spec
  AttemptLaunched, durable
  placement.launch(A) ─────────────────────────────────────────────────────▶ boots
                                         runs/R/A.worker  ◀── create-if-absent (claim)
  ◀──────────────────────────────────────────────── POST /attempts/A/start   (lease)
  ◀──────────────────────────────────────────────── POST /attempts/A/beat    every 10 s
  ◀──────────────────────────────────────────────── POST /attempts/A/resolve (small keyed writes)
                                         keys/…/12-A.kx   ◀── deltas
                                         runs/R/A.writing ◀── gate (overwrite stores only)
                                         output data      ◀── store writes
                                         runs/R/A.log.000000…  ◀── log chunks
                                         runs/R/A.result  ◀── create-only, sealed
  ◀──────────────────────────────────────────────── POST /attempts/A/finished (hint)
  GET .result, validate, AttemptFinished, durable
```

## 2. Objects

All under `runs/{run}/`, all named after the attempt. Only the claim's
winner ever writes anything but the spec, so no other name needs the
invocation token.

| Object | Written by | Mode | Meaning |
|---|---|---|---|
| `{attempt}.spec` | engine, before `AttemptLaunched` | create-only, immutable | what to run: today's `spec`, plus `engine` (HTTPS URL), `token` (§5.2), `lease` (§6) |
| `{attempt}.worker` | the invocation that claims it | created once; then overwritten only by its owner, only while HTTP fails (§6) | the claim: `{"invocation", "host", "pid", "at"}`; later also `{"seq", "timeline", "usage"}` |
| `{attempt}.writing` | worker before its first store write, or engine before ending the attempt | create-only | the gate, as today, now carrying `invocation`; only for outputs on overwrite stores (§9) |
| `{attempt}.log.{n:06d}` | worker | create-only, immutable | a gzip member of the log, flushed every 30 s or 1 MB |
| `{attempt}.result` | worker | create-only, immutable, sealed bytes | the outcome; its existence means the attempt is done |

Gone: the two-write `{attempt}.json`, `.beat` and its done marker, the
joined `.log`, chunk deletion, and `AttemptClaimed`.

**The result** carries what today's result does, plus the timeline, usage
and the log index:

```json
{
  "invocation": "k3v9q2",
  "status": "succeeded",
  "outputs": {"orders": {"ref": {"…": "…"}, "keys": {"files": [{"name": "000000000012-01J9…"}], "added": 3, "removed": 0, "exact": true}}},
  "delivered": {"uploads": {"after": null, "upserted": ["u-7"], "deleted": []}},
  "cursor": "token-42",
  "timeline": [{"type": "booted", "at": 1790074791.2}, {"type": "computing", "at": 1790074793.0}],
  "usage": {"cpu_seconds": 4.1, "peak_memory": 512000000},
  "log": {"chunks": [[0, 412, 1790074791.2], [1, 388, 1790074821.2]], "lines": 800, "truncated": false}
}
```

`log.chunks` lists `[n, lines, first timestamp]` per chunk; the console
reads "the last 200 lines" as the last few chunks. Chunks are written
create-only and never joined: one PUT per 30 s of logging instead of
today's chunk-then-join-then-delete.

## 3. One attempt, step by step

The example: `orders:alpha` writes a three-key `Patch` to a FileStore
output, on ECS, engine up throughout.

1. **Claim the scope** (memory) and pin inputs, as today.
2. **Write the spec**: `PUT runs/R/A.spec` create-only. An existing spec
   with the same bytes is this write's own (round one's primitive).
3. **Authorize**: record `AttemptLaunched` and await `durable()`. A
   replaced engine fails here and launches nothing.
4. **Launch**: `placement.launch(stage)` with `clientToken = A`; record
   `AttemptPlaced {handle}`. The provisioning clock starts (§8).
5. **Claim**: the worker boots, reads the spec, generates an invocation
   token (`k3v9q2`) and creates `A.worker`. If the create finds an object
   with another token, a second invocation of `A` won: this one exits
   without computing, writing or deleting anything.
6. **Start**: `POST /attempts/A/start {invocation}`. The engine answers with
   the lease (§6) or `409` (the attempt ended, or another invocation
   started it: exit untouched). The provisioning clock stops; the
   attempt's `timeout` starts.
7. **Compute**, beating every 10 s over HTTP. Logs go live over HTTP and
   durably as chunks.
8. **Plan**: for the 3-key patch, `POST /attempts/A/resolve` with the run of
   `(key, version, deleted)`; the engine answers from its warm index (D4,
   `resolved-commits.md`) and persists nothing. The worker uploads the
   delta file itself, create-only. Unreachable engine: it resolves
   locally with the merge-join, as today.
9. **Write**, under the output store's write rule (§9). For an overwrite
   store, the gate `A.writing` first.
10. **Seal**: stop the beat thread, build the result once, `PUT A.result`
    create-only, retried with the same bytes. Then `POST /attempts/A/finished`
    as a hint, and exit.
11. **Settle**: on the hint (or the provider's exit, or a look at `.result`),
    the engine reads `A.result`, checks its `invocation` against the one
    that started, validates, records `AttemptFinished`, and makes it durable.

A failed attempt follows the same path with `status: failed`. An aborted
one (cancel, timeout) writes no result: the engine ends it (§7).

## 4. Invocations and duplicates

A placement may run one attempt twice: Kubernetes Jobs do so even with
`parallelism: 1` and `restartPolicy: Never`, and a relaunch after an
engine restart (`resume`) can race a launch whose answer was lost. The
claim makes this harmless:

- The first `.worker` create wins. The loser reads the claim back, sees
  another token, and exits with code 0, having touched nothing — not
  even a log.
- An ambiguous create (the response lost) is resolved by reading the claim
  back: its own token is its own claim (round one's primitive).
- The engine accepts heartbeats, resolves and the result only from the
  invocation that `start`ed; after a restart, from the one in `.worker`.

Delayed duplicates long after the attempt ended are covered in §9.5.

## 5. The HTTP channel

### 5.1 Endpoints

All under `/api/projects/{p}/`, all JSON over HTTPS, all idempotent.

| Route | Body → answer | Who |
|---|---|---|
| `POST attempts/{a}/start` | `{invocation, host, pid}` → `{lease_seconds, spec?}` · `409 {reason}` | the claiming worker |
| `POST attempts/{a}/beat` | `{invocation, seq, events[], usage, progress?}` → `{lease_seconds, cancel}` | worker, every 10 s |
| `POST attempts/{a}/logs` | `{invocation, lines[]}` → `204` | worker, every 1 s while it logs |
| `POST attempts/{a}/resolve` | `{invocation, output, batch, run}` (`.kx` bytes) → `{delta}` · `503` | worker, small keyed writes (D4) |
| `POST attempts/{a}/finished` | `{invocation, result?}` → `204` | worker, after `.result`; an observation sends its result inline (§11) |
| `GET pools/{pool}/work?wait=30` | capacity → `[{attempt, run, objects, needs}]` | pool workers (§10) |

`events` are timeline events (`loaded`, `mark`, per-key outcomes from
`per-key-processing.md`); `progress` is free-form for the console. Live
logs and events go to an in-memory ring per attempt that the console
tails; the durable copy is the chunks and the result.

Gone: `POST /api/workers/register`, `/api/tasks/claim`,
`/api/tasks/{id}/renew`, `/api/tasks/{id}/complete`.

### 5.2 Authentication

- **Attempt token.** The engine mints `token = HMAC(engine_secret,
  attempt)` and puts it in the spec. Every `attempts/{a}/…` call carries
  `Authorization: Bearer {token}`; the engine verifies it statelessly and
  only for that attempt's routes, and rejects it once the attempt ended.
  Anyone who can read the spec can already write the attempt's result, so
  the token adds no new trust; it keeps it out of provider consoles (ECS
  task descriptions, Kubernetes manifests), which carry only the stage.
- **Pool token.** `pools/{pool}/work` takes a per-pool secret configured
  on the server and on the pool's workers. It only reveals which attempts
  wait; claiming still takes write access to the object store.
- The admin API keeps its own token. A worker token reaches no other route.

The worker learns the engine's URL from the spec (`engine`), so the
engine needs a stable public HTTPS name (load balancer or DNS), not a
host address: a restarted engine on another host is the same URL.

### 5.3 When the engine is down

| Signal | With the engine | Engine down |
|---|---|---|
| claim | `.worker` create, then `start` | `.worker` create; `start` retried in the background |
| heartbeat | HTTP every 10 s | `.worker` overwritten every 20 s (§6) |
| live logs, events | HTTP | dropped; chunks still go to the object store |
| resolve | HTTP | local merge-join (today's path) |
| cancel | in the beat's answer | none needed: nobody is canceling |
| result | `.result` + `finished` hint | `.result` |
| pool discovery | long-poll | none: no new work is scheduled anyway |

Requests are retried with jittered backoff (1 s → 30 s). A worker never
fails because the engine is unreachable. The one exception is a worker
that never established a lease and must write to an overwrite store; it
waits for the engine before its first store write (§9.4).

The next engine replays the journal, adopts every launched attempt (round
one: through its recorded handle), and for each reads `.result`, else
`.worker`. Workers' requests start succeeding again; nothing else needs
reconciling, because nothing the worker sent over HTTP was the only copy
of a fact.

## 6. Heartbeats and leases

Erwin asked whether the 30 s heartbeat stays. Proposal: **yes to HTTP
heartbeats every 10 s with `.worker` as the fallback, and the heartbeat
becomes a lease.**

- The worker beats over HTTP every 10 s (`beat`). The answer renews its
  **lease** for `lease_seconds` (T = 60 s) and carries `cancel`.
- After two failed beats in a row, it also overwrites `.worker` with its
  `seq` and timeline every 20 s, until a beat succeeds again. A PUT that
  succeeds renews the lease too, because the engine never releases a
  scope on silence without first reading `.worker`: a worker cut off from
  the engine but not from the object store is seen alive, by this engine
  or the next.
- The worker measures its lease on its own monotonic clock, from when it
  *sent* the renewal that succeeded. The engine measures silence on its own
  monotonic clock, from when it *received* the last beat or *saw*
  `.worker` change. Receipt and observation come after sending, so the
  engine's view of expiry is never earlier than the worker's; no wall
  clocks are compared.
- The engine reads `.worker` only for attempts whose HTTP beats stopped:
  in steady state a heartbeat costs no object-store request at all.

Costs: per attempt-minute, 6 HTTP requests and no PUT or GET, against
today's 2 PUTs and 2 GETs (beat + fence read). During an engine outage,
3 PUTs per attempt-minute.

**Where the proposal needs challenging.** "Liveness is evidence only,
never permission to release a scope" is right for a heartbeat as it is
today: nothing stops a worker the engine gave up on. It stops being right
once the worker **self-fences**: a worker whose lease lapsed stops issuing
writes and exits, and the engine waits a margin longer than the lease
before releasing. Then lease expiry is permission, under stated timing
assumptions. The alternative for a store with no fencing is to hold the
scope forever on any doubt, which turns every partition into a stuck scope
an operator must clear. §9 weighs this and recommends the lease for
overwrite stores, with exact fencing in every built-in store so the
timing assumption only ever covers user-written stores.

## 7. Settling, canceling, timing out

The engine settles an attempt when the first of these happens:

| Trigger | Engine does |
|---|---|
| `finished` hint, or the provider reports exit | GET `.result`; if present, validate and commit or fail it |
| `.result` absent after the provider's exit | the worker died: end it as lost (rule of §9) |
| cancel, or `timeout` since `start` | answer `cancel: true` to beats; take the gate as `aborted` (overwrite stores); cancel at the provider; end it under the store's release rule (§9) |
| no word for the provisioning deadline (§8) | as a timeout, `reason: provisioning` |
| lease lapsed + margin, no result | as lost |

Cancel reaches a working worker within one beat (≤ 10 s). It stops
computing; a worker already writing to an overwrite store finishes its
writes and its result, and the commit stands (today's rule). Timeouts no
longer stop applying once a worker is writing (review R8): a writer past
its timeout is told to stop, its lease is not renewed, and it self-fences.

Deadlines are the engine's monotonic clock; an adopted attempt keeps what
the launching engine's clock says is left, bounded below by three beats
and above by the whole budget (round one).

## 8. Provisioning

From launch until `start`, an attempt is provisioning: an image pulling, a
task waiting for capacity, a pool attempt waiting for a worker. It has its
own deadline, set per executor:

```python
gpu = AWSECS("gpu", cluster="ml", region="us-east-1", provision="20m")
etl = K8sJob("etl", cluster="prod", namespace="data")      # 10 min, the default
ingest = Pool("ingest")                                    # none: wait for a worker
ingest = Pool("ingest", provision="1h")                    # or give up after an hour
```

`provision` goes into the manifest's executor environment; every
placement of the executor inherits it. Past it, the attempt is aborted,
canceled at the provider and failed retryably (`reason: provisioning`).
The attempt's `timeout` counts from `start`, not from launch: a 20-minute
image pull no longer eats a 30-minute timeout.

## 9. D1 — writers the engine gave up on

**Not decided.** This section is the investigation; §9.6 recommends.

### 9.1 The hazard, precisely

An attempt W1 is believed finished (failed, timed out, canceled, lost),
its scope is released, and W2 commits. Then a write of W1's lands. Which
writes can do damage depends on how the store names things:

| Store | W1's late write | Damage |
|---|---|---|
| keyed, one object or row per key (FileStore, Postgres) | key `k` at W1's version | `k` regresses: the index says `v2`, the store holds `v1`; readers disagree with the index |
| keyed, W1 removes `k` | deletes W2's `k` | a key the index lists is missing |
| a value (`site_status/alpha.json`) | the whole value | regresses to W1's |
| unkeyed incremental (batch files) | batch `n`, which W2 also wrote, since retries reuse batch numbers | W2's batch replaced by W1's |

Erwin's observation holds: two writers of the same key at the same version
write the same content, since that is what the asset's revision promises,
so their order does not matter. Only a write of a different version after
a newer commit is dangerous, and only on a store that overwrites in place.

How W1 can still write after the engine gave up on it:

1. It lost contact (partition, engine down) but runs on.
2. It paused (GC, VM migration, SIGSTOP) past its heartbeat timeout.
3. A request it issued before dying, or before being told to stop, is
   still in flight or queued at the backend: a PUT's last bytes are sent,
   a `COMMIT` waits on a lock.
4. A placement duplicate or a delayed duplicate (§4, §9.5).
5. User code writes outside the store contract (its own client, a thread
   it started).

### 9.2 What others do

**Orchestrators detect and retry; none fences.**

- **Airflow** finds task instances whose heartbeat timed out
  ("task instance heartbeat timeouts", formerly zombies) and marks them
  failed or up for retry
  ([tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)).
  In Airflow 3 the worker's supervisor heartbeats to the API server and
  kills its task when the server answers `409 not_running`
  ([apache/airflow#48719](https://github.com/apache/airflow/issues/48719)):
  self-termination, but only once a heartbeat gets through, and with no
  margin before the retry runs.
- **Dagster** run monitoring marks runs failed past `start_timeout`,
  `max_runtime` or a failed worker health check, and can resume them on a
  new run worker; the docs describe status changes, not stopping a live
  worker's writes
  ([run monitoring](https://docs.dagster.io/deployment/execution/run-monitoring)).
- **Prefect** marks a flow run Crashed after three missed heartbeats
  (9 minutes by default); its issue tracker shows runs marked Crashed that
  then completed
  ([zombie flows](https://docs.prefect.io/v3/advanced/detect-zombie-flows),
  [PrefectHQ/prefect#16459](https://github.com/PrefectHQ/prefect/issues/16459)).
- **Temporal** times out activities by heartbeat and start-to-close
  timeouts and retries them; it states that an activity "may be executed
  multiple times and may even partially complete more than once" and asks
  activities to be idempotent, keyed by workflow run and activity id
  ([detecting failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures),
  [activity definition](https://docs.temporal.io/activity-definition)).
- **dbt** relies on the warehouse: a table build is a temp relation swapped
  in by renames inside one transaction. Concurrent runs of one model
  collide on the fixed `__dbt_tmp` name, and dbt's answer is not to run
  them concurrently
  ([dbt forum](https://discourse.getdbt.com/t/multiple-dbt-runs-on-same-model-but-different-time-range-create-same-tmp-table/10221),
  [dbt-adapters#2162](https://github.com/dbt-labs/dbt-adapters/issues/2162)).

**Storage systems fence, in three ways.**

- **Immutable files, one atomic commit.** Iceberg and Delta write data files
  under unique names and publish them by atomically swapping a metadata
  pointer or creating the next log entry; a writer that never commits
  leaves orphan files, removed later with a safety window
  (`remove_orphan_files` defaults to 3 days because in-progress writes look
  like orphans) ([Dremio on orphan cleanup](https://www.dremio.com/blog/apache-iceberg-orphan-file-cleanup/)).
  Delta on S3 needed put-if-absent for that one commit, first through
  DynamoDB ([Delta storage](https://docs.delta.io/delta-storage/)); S3 has
  offered `If-None-Match` since August 2024 and `If-Match` since November
  2024 ([announcement](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes)).
  Hadoop's S3A committers do the same for speculative task attempts: each
  attempt uploads parts, and only the one job commit completes them
  ([committer architecture](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committer_architecture.html)).
  Solera's journal is already that commit pointer.
- **Tokens the storage checks.** Kafka bumps a producer epoch per
  `transactional.id`; writes from an older epoch are rejected
  ([Confluent](https://www.confluent.io/blog/transactions-apache-kafka/)).
  HDFS JournalNodes accept edits from one NameNode epoch at a time
  ([QJM HA](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html)).
  Kleppmann's fencing tokens: the storage remembers the highest token it
  saw and refuses older ones; anything else rests on bounded network
  delay, pauses and clock error
  ([How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)).
- **Leases with self-fencing and a delay.** Chubby passes sequencers to
  servers that can check them, and for those that cannot, holds a lost
  lock for a *lock-delay* (up to a minute) before anyone else may take it
  ([Chubby, §2.4](https://research.google.com/archive/chubby-osdi06.pdf)).
  A Bigtable tablet server stops serving when it loses its Chubby lock and
  kills itself once its server file is deleted; the master deletes the
  file only after acquiring the lock itself
  ([Bigtable, §5.2](https://research.google.com/archive/bigtable-osdi06.pdf)).

So: orchestrators accept at-least-once execution and hope; storage systems
that need safety either make stale writes invisible (immutable + commit) or
make the store reject them (tokens). Leases with a delay are the fallback
for resources that can check nothing, and they are only as safe as their
timing assumptions.

### 9.3 Options, in tiers

| Tier | Mechanism | Prevents (§9.1) | Leaves open | Runtime cost | Asks of a user store |
|---|---|---|---|---|---|
| 0 | Detect and retry (Airflow, Dagster, Prefect; Solera before round one) | nothing | all | none | nothing |
| 1 | **Hold on doubt**: release only on a result or provider-confirmed exit plus a drain margin; anything else blocks the scope visibly | 1, 2, 4 while the worker cannot be accounted for; 3 after exit only within the margin | 3 beyond the margin, 5; and availability: every partition or lost handle is a stuck scope needing an operator | none | nothing |
| 2 | **Harness self-fencing** (§9.4): a lease; the worker stops writing and exits when it cannot renew; the engine waits lease + margin | 1, 4; 2 and 3 up to the margin | pauses or in-flight writes longer than the margin; 5 | ~nothing (beats exist); release delay T + M ≈ 2 min after a worker goes silent | nothing; advice: bound client timeouts below M |
| 3 | **Generation fencing**: the engine assigns a generation per (output, scope); the store checks it atomically with each write | 1–4 exactly | 5 | one row lock and compare per transaction | a transactional store; an optional `Scope.generation` it may check |
| 4 | **Immutable, content-addressed outputs**: write `{key}/{version}`, never overwrite; the commit (journal) decides what is current; superseded versions are collected | 1–4 exactly, and stale writes become garbage; also pins reads | 5 | +1 DELETE per ~1,000 superseded versions, batched; full loads read the index | nothing; a user store may adopt the layout |

Not pursued:

- **Per-object compare-and-swap** (`If-Match` on each key's ETag). It needs
  each key's last ETag in the index and a CAS-capable backend
  (`file://` has none), and buys less than tier 4 does.
- **Exactly-once for user side effects** (5). No orchestrator offers it;
  Temporal's answer, idempotency keys checked by the downstream service,
  is the user's to apply. Solera can give them one: `ctx.attempt`.

### 9.4 Tier 2 in detail: harness self-fencing

Every store write passes through the harness (`_store_outputs` calls
`store.store()`), so the harness can refuse to start one.

- **Lease.** Established by `start` (or, with the engine down, by the
  `.worker` claim followed by a second read of the spec — §9.5), renewed
  by beats or `.worker` PUTs (§6). Valid until `sent + T` on the worker's
  monotonic clock, T = 60 s.
- **Self-fence.** Before each `store.store()` call, the harness checks
  that the lease has at least `T/2` left; if not, it renews first, and if
  it cannot, it raises before writing. The beat thread watches the lease
  during writes and calls `os._exit` the moment it lapses, so a long
  `store()` (a million-row upsert) stops issuing statements.
- **Engine.** Releases an overwrite store's scope only on a result, on a
  confirmed exit plus M, or after T + M with neither an HTTP beat nor a
  change of `.worker`, where M (60 s) is the margin for writes already
  issued to land.
- **What it needs of user stores:** nothing. Advice in the store guide:
  set client and statement timeouts below M.

What it leaves open, exactly:

- **A pause between the check and the write** longer than M. The harness
  checks with T/2 to spare, so the pause must exceed T/2 + M (90 s), and the
  heartbeat thread, which shares no lock with the writer, would normally
  have killed the process by then. A frozen VM freezes both, though.
- **A request issued in time that lands after T + M**: a `COMMIT` waiting
  on a lock for minutes, a client library that retries internally past its
  timeout, a multipart upload completed late. Client timeouts below M
  close most of it; a database that applies a transaction after its client
  vanished does not care about the client's timeout.
- **Writes outside `store.store()`** (§9.1, 5).

Those are the windows of every lease-based system without a checked
token (Kleppmann; Chubby's lock-delay). They are rare and bounded by
assumptions, not impossible.

### 9.5 Delayed duplicates and retention

A duplicate invocation that read the spec, then paused past the attempt's
end and its run's retention (days), and resumed: its claim succeeds (the
run directory is gone) and it may write.

- Tier 4: its writes are unreferenced garbage, collected as orphans.
- Tier 3: its generation is stale; the store rejects it.
- Tier 2: it must hold a lease. `start` returns 409 for an ended attempt,
  so it gets none. With the engine unreachable, it re-reads the spec after
  claiming and finds it gone. What is left is a deletion in progress
  during that re-read, after a pause of days.

### 9.6 Recommendation

**Default, for every store, asking nothing of store authors:** tier 2.
Self-fencing leases on top of round one's gate and repair, with the
release rule of §9.4. It removes the review's R1 paths (Pool lease
starvation, partition, heartbeat failure, placement uncertainty) for
everything but the timing windows above, and it keeps scopes available:
no operator is ever needed to unblock a partitioned worker's scope.
Releases by lease expiry are marked in the history (`end: "lease"`), so
the rare case is visible.

**Built-in stores go further, so the timing assumption only ever covers
user-written stores:**

- **FileStore and S3Store: tier 4, content-addressed.**

  ```
  site_files/alpha/f-1/9c41e0….json        a key, at its version (digest or declared revision)
  site_status/alpha@5d2a….json             a value
  site_events/alpha/000000000042-01J9….json  a batch, named by its attempt
  ```

  A keyed load already receives `Keys(key -> revision)`, so it computes
  names and needs no LIST. A full load of a keyed input reads that input's
  key index, pinned in the spec, then loads by name. A delta records each
  changed key's previous version, so after a commit (and once no reader
  pins it) the engine asks the store to `discard` superseded versions;
  `discard` is a new optional store method, the only one this adds, and
  only immutable stores implement it. Appended batches get a fresh batch
  number per attempt, never reused; numbers an attempt took and never
  committed are recorded on the head (`gaps`, cleared by a full run), so
  a range load skips them and a stale writer's batch file is garbage.
  Because nothing is overwritten, **the gate, unsettled intents and repair
  are not needed for these stores**: they release a scope as soon as the
  engine ends the attempt, with no margin. Reads become pinned too: a
  consumer pinned to version 12 reads version 12, which removes the
  "reads are not pinned" caveat of `object-store-state.md` §9.

- **PostgresStore: tier 3, generation fencing.** One table per database:

  ```sql
  CREATE TABLE solera_generations (output text, scope text, generation bigint, attempt text,
                                   PRIMARY KEY (output, scope));
  ```

  The engine gives each attempt `generation` = the model's event position
  (`applied`) of its `AttemptLaunched`: monotonic per scope, since a
  scope's launches are serialized, and no new state. The harness passes it
  as `Scope.generation`.
  Every `store()` transaction starts with

  ```sql
  INSERT INTO solera_generations VALUES ($o, $s, $g, $a)
  ON CONFLICT (output, scope) DO UPDATE SET generation = $g, attempt = $a
    WHERE solera_generations.generation <= $g
  RETURNING attempt;                       -- no row: a newer generation holds it; abort
  ```

  which locks the row — Postgres locks a conflicting row even when the
  `WHERE` refuses the update — and holds it to commit. (Checked on
  Postgres 17: a takeover waits behind an older attempt's open
  transaction, and the older attempt's next transaction gets no row.) A newer attempt's first
  transaction waits behind any older one in flight, and from then on every
  transaction of the older attempt fails the check. Repair reads happen
  after acquisition, so they see everything the old writer committed. The
  gate stays, for its intents (and for `Sql`, the "unknown writes, rescan
  the scope" intent of R7), but the scope is released as soon as the
  engine ends the attempt: the next attempt's acquisition is the fence.

**What a user store author does:** nothing, by default (tier 2). A store
that can do better declares it, and the engine releases its scopes
without the margin:

```python
class MyStore(Store):
    writes = "immutable"   # names nothing a commit has not referenced; implements discard()
    writes = "fenced"      # checks Scope.generation atomically with each write
    writes = "overwrite"   # the default: gate, intents, repair, lease + margin
```

An attempt whose outputs use several stores follows the strictest rule.

**Cost of the recommendation**, per attempt: none at runtime for tier 2
(the beats exist); one row lock per Postgres transaction; for FileStore,
superseded versions collected in batched DELETEs, and a key-index read per
full load of a keyed input. What it saves: for built-in object stores, the
gate PUT, the fence read on every beat, unsettled intents and repair
reads.

## 10. Pool (D3)

```
engine   AttemptLaunched (pool: ingest, needs: {cpu: 4}), durable — no placement call
worker   GET pools/ingest/work?wait=30 (capacity: cpu 8)   → [{attempt: A, run: R, …}]
worker   create runs/R/A.worker {invocation}                 → wins (or reads back: lost, poll again)
worker   POST attempts/A/start                               → lease; the attempt runs as any other
```

- **Discovery** is a long-poll returning launched pool attempts that fit
  the worker's capacity and have not started, oldest first. It is a hint:
  two workers may get the same attempt; the `.worker` create decides.
- **No registration, no leases of its own, no claim event.** A pool
  worker's liveness is its attempt's heartbeat, like any worker's
  (review R1: Pool's lease contradicted fresh beats). After a restart the
  engine re-offers pool attempts it has not seen start; a claimed one is
  harmless, since the create fails.
- **Engine down:** pool workers keep running their attempts and stop
  getting new ones, as nothing new is scheduled anyway.
- **Provisioning** for a pool attempt is the wait for a worker: no
  deadline unless the executor sets one (§8).
- The handle is `{attempt, pool}`; `cancel` before `start` withdraws it
  from discovery; after `start`, cancel is the beat's answer.

## 11. Observations

An observation (`per-key-processing.md` §12) reads a source and changes
nothing until it finds a change, so it gets a lighter path:

- **No spec object, no journal entry at launch.** The engine launches it
  with a memory-only claim (its source's scope); the worker's `start`
  answer carries the spec (`spec` field).
- **No `.worker`, no gate.** It writes no store data; a duplicate is
  harmless, and the engine accepts the first report.
- **Unchanged:** the worker reports over HTTP (`finished` with the result
  inline; a keyed map is checked with `resolve`, which answers "no delta"
  from the warm cache). The engine records only history rows, outcome
  `skipped`, kind `observe`, kept a day.
- **Changed:** the worker uploads the delta file, then reports the result
  inline; the engine commits it as a source commit, with its cursor. The
  delta file is the only object written.
- **Engine restart:** an observation in flight is forgotten; its worker's
  next request gets `409` and it exits. The next tick observes again. A
  change it found but could not report is found again, since the source's
  cursor did not move.
- Engine down at the end: the observation exits without a result. Nothing
  depends on it.

Open for implementation: the skipped history rows are not journaled, so
the history lake gains a side buffer that a crash may lose.

## 12. Engine restart, attempt by attempt

| The attempt was | The next engine |
|---|---|
| preparing (no `AttemptLaunched`) | dispatches the task again; the orphan spec, if any, is collected with the run |
| launched, no handle recorded | `resume` (ECS, K8s: launch again by name) or follows `.worker`; provisioning deadline applies |
| provisioning | follows the handle; the deadline continues (round one's clamp) |
| running | follows the handle; heartbeats arrive over HTTP again; if they do not, reads `.worker` |
| done, result written | reads `.result` and settles it |
| aborted by the old engine (gate `aborted`) | ends it as the old engine would have (release rule of §9) |
| an observation | forgotten; the next tick reruns it |

## 13. What changes from today

| Today | Target |
|---|---|
| `{attempt}.json`: spec, then overwritten with spec + result + log index | `.spec` (immutable) and `.result` (immutable, sealed) |
| `{attempt}.beat` every 30 s with a fence GET, then a done beat | HTTP beat every 10 s; `.worker` only while HTTP fails; `.result` means done |
| no invocation identity; duplicates overwrite each other's result (R3) | `.worker` claim with an invocation token; losers touch nothing |
| log chunks, joined at the end, chunks deleted | immutable chunks every 30 s / 1 MB, indexed from the result; live lines over HTTP |
| Pool: register, claim, renew, complete; in-memory leases; `AttemptClaimed` | long-poll discovery; `.worker` claim; ordinary heartbeats |
| resolve through `.ask` objects and polling (proposal) | `POST resolve`, no persistence; local fallback |
| cancel read from the fence by every beat | `cancel` in the beat's answer; the gate remains the durable abort for overwrite stores |
| timeout from launch; ignored once writing (R8) | timeout from `start`; applies while writing, through the lease |
| provisioning deadline engine-wide | per executor (`provision=`) |
| every attempt: gate, intents, repair; release on presumed death (R1) | per store: immutable (built-in object stores), fenced (Postgres), overwrite (lease + margin) |
| observations as full attempts | spec-less, journal-less until they change something |
| one API token | admin token; per-attempt HMAC token; per-pool token |

## 14. Open questions

1. **T and M.** 60 s each puts a silent worker's scope back in ~2 min
   (today: 90 s, unsafely). Shorter is faster recovery and a smaller
   pause budget.
2. **Lease establishment with the engine down.** As written, a worker that
   never reached the engine may still write to an overwrite store after
   re-reading its spec. Stricter: overwrite stores wait for the engine.
   That costs only availability, and only for user stores during outages.
3. **FileStore readability.** `{key}/{version}.json` is still browsable,
   but a human sees every unreclaimed version until collection. Collection
   should therefore be prompt (seconds after the last pin lets go).
4. **Declared revisions as names.** A `revision` column is the user's
   promise that equal revisions mean equal rows. If it is a timestamp,
   that promise is weak; naming by a digest of the row instead costs
   nothing extra on the worker but differs from the index's version.
