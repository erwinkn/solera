# Engine architecture and contracts

## Public model

An asset is a named output; its decorated function is an internal producer. Selecting one output of a multi-output producer runs the function and publishes every declared output. Dependencies are bound by function parameter name or explicit input mapping. Resources are ordinary named values supplied by `Definitions.resources`; stores implement the immutable snapshot protocol.

Partitions are independently addressable scopes. The initial partition mapping is daily identity, with an unpartitioned upstream allowed to feed partitioned downstream tasks. Implicitly collecting a partitioned upstream into an unpartitioned asset is rejected. Keyed item progress is stored separately from partitions; files do not become individual graph nodes or runs.

## Runtime boundaries

The SDK is Python. A PostgreSQL-backed control plane stores versioned manifests and plans requests without importing user code. The default application runs an execution supervisor alongside its HTTP API. The supervisor launches a subprocess per ready producer/scope; alternatively, run it with `dorc worker` on another trusted machine sharing the database and code environment.

The execution backend handles `submit`, `lookup`, `inspect`, `cancel`, `logs`, and `release`. It does not decide dependencies, batching, invalidation, or checkpoints. The implemented subprocess backend has process-local submission lookup. A supervisor crash can lead to duplicate computation after lease expiry; this is tolerated through publication fencing, not presented as durable exactly-once infrastructure submission. Future remote backends must add durable submission-key lookup.

A registered manifest includes function source fingerprints, explicit transformation versions, input/output bindings, partition/incremental contracts, and automation definitions. Requests pin that manifest. Workers refuse mismatched definitions rather than silently executing new code against an old request. External helper code, resource values, and dependency environments are not automatically content-addressed: explicit `version` changes and operator-managed immutable deployments are required.

## Request and task lifecycle

Manual requests, backfills, and automations use the same planner. Each task is a producer plus an output scope. Upstream producer/scope pairs are deduplicated within a request. Input references are bound before execution and persisted on the task; retries reuse them. Planned dependencies read the commit produced by their own upstream task, not whatever is globally latest afterward.

The durable queue uses PostgreSQL task rows, retry times, and `SKIP LOCKED` claims. Short state transitions additionally acquire a transaction-level advisory lock in this alpha. This conservative serialization prevents metadata lock inversions while still allowing concurrent computation. Removing that lock is a measured scaling project, not an untested micro-optimization.

A claim reserves each output scope with an attempt token and a renewable lease. The supervisor heartbeats active executions. Expired tasks are recovered and retry within the declared budget. A late worker cannot publish after losing its token, after cancellation, or after checkpoint generation changes.

Cancellation retains committed data and fences unpublished work. Pause prevents new claims but allows running tasks to finish. Repair creates a linked request: successful tasks retain their original output commits; failed or blocked tasks become executable again. This preserves successful upstream inputs even when newer global materializations exist.

## Incremental changes

### Keyed inventories

`Inventory(items, complete=...)` is an explicit source result. `ByKey(input, key, revision, batch_size)` compares that immutable inventory with the consumer's `item_state` rows. Keys are normalized to strings; source revisions are canonical JSON values. Keys must be unique. New keys, revision changes, transformation changes, and explicit recomputation create processing work.

A complete inventory can identify deletions. An incomplete inventory cannot. Discovery failure should fail the source asset, not return an empty complete inventory. Each consumer and partition has independent state. In the alpha, comparison loads the inventory and relevant item state into worker memory, then creates bounded batches; database persistence is indexed, but processing is not streaming at arbitrary scale.

Every batch receives an acknowledgement token. The function must return it with all its outputs in `CommitBatch`. Only successful publication updates processed revisions and deletes acknowledged item state. A later failure retains earlier batches, and a retry processes the remaining changes. Downstream tasks within a request wait for the whole producer task to finish, not merely for its first committed batch.

### Cursors

`Cursor(initial, state_version)` provides an opaque previous position through `ctx.cursor`. The asset author obtains a bounded source batch and returns its next position as `CommitBatch(cursor=...)`. The engine cannot prove source ordering, completeness, deletion semantics, or late-arrival handling for an arbitrary token. Returning an advanced cursor for incompletely processed work is a user-code error.

Changing `state_version` fails safely pending an explicit migration. There is no automatic cursor-reset command. Recompute is rejected for cursor assets, including cursor assets pulled into a backfill through upstream dependencies. A new independent producer/scope can establish separate state; migration tooling is future work.

## Output operations

- `Replace(value)` publishes a complete new value for a scope. Returning a plain value does the same.
- `ReplaceKeys(column, keys, rows)` removes all prior rows owned by the supplied keys, then inserts replacement rows. Every replacement row must belong to an explicitly affected key. An empty replacement still deletes previous children.
- `Upsert(primary_key, rows, delete_keys)` updates keyed rows and applies explicit deletions. Duplicate new keys and contradictory upsert/delete keys are rejected.
- `Append(batch_id, rows)` uses a durable receipt. Repeated identical batches do not append again. Reusing the same ID with a different payload fails.

These operations currently produce immutable JSON snapshots. They are not SQL pushdown or in-place warehouse mutations. Their correctness comes from publishing versioned references, not from an imaginary transaction spanning arbitrary systems. For source files contributing to shared deduplicated samples, maintain contributor provenance and recompute affected sample IDs from all contributors; ownership replacement alone does not define cross-source precedence.

## Publication transaction

1. Bind exact input references and the expected checkpoint generation under the attempt lease.
2. Compute the batch and stage immutable output objects. The store must durably persist each object before returning its reference.
3. In one PostgreSQL transaction, verify current task ownership and every output-scope token.
4. Compare and advance checkpoint generation; insert the commit, all output heads, item-state changes, append receipts, and event-trigger outbox entries.
5. Mark task completion separately, retaining recoverable batch progress if the worker dies between publication and completion.

The PostgreSQL JSON store inserts content-addressed objects. The filesystem store fsyncs a temporary file, links it into a content-addressed immutable path, and fsyncs the directory. Both expose exact historical versions. Multi-output atomicity means readers resolving one commit observe one output bundle. Staged-but-unpublished objects are unreachable through committed heads; garbage collection is not implemented yet.

A mutable destination needs a different adapter contract: write destination changes plus a durable receipt in one destination transaction, reconcile an ambiguous result, and finalize the metadata checkpoint. That protocol is designed for a future iteration and is **not** implemented by the current `Store` interface. Calling a database or external API directly from user code does not inherit the engine's publication guarantee.

## Caching and invalidation

Derived snapshot tasks skip when their input content versions and transformation fingerprint match a coherent existing output bundle. Source functions normally run to discover current state; fill-missing may reuse a valid source snapshot. No-change keyed consumers skip without generating an output commit. Committing identical content does not enqueue downstream commit-trigger automation.

The UI reports missing materialization, direct input-version or definition staleness, latest failure, and active execution. It is an operational view, not an arbitrary transitive freshness/impact analysis engine. Current catalog aggregation examines stored heads; large-scale pagination and custom impact mappings remain future work.

## Automation

Intervals, cron, and changed-output commit triggers all submit ordinary requests. Timer requests and schedule advancement share a transaction, and missed timer ticks coalesce rather than generating an unlimited catch-up storm. Cron includes an explicit timezone and uses the cron library's daylight-saving behavior. Commit events use an outbox, not a fragile globally increasing event cursor; pending entries are marked delivered only alongside successful request creation.

Triggers run through the worker supervisor. Disabling all workers also pauses schedule evaluation. Arbitrary user polling functions, webhooks, freshness policies, and custom event schemas are not implemented. Avoid feedback cycles between automations; direct same-target triggers are rejected, but general automation-cycle analysis is not yet exhaustive.

## Persistence, security, and next boundaries

PostgreSQL is both the durable queue and metadata source of truth. The event journal supports operational inspection; it is not event-sourced reconstruction of all current state, and it is not a tamper-evident audit system. Back up the database and any filesystem snapshot roots together. Retention and garbage collection must preserve referenced history.

The initial deployment trusts its users, installed Python code, and workers. One shared bearer token protects the API; a subprocess is not a sandbox. See `SECURITY.md` before remote use. Priorities beyond the alpha are mutable-destination commit receipts, Parquet/S3 reference-based data access, durable remote execution adapters, immutable code artifact registration, scalable inventory scans, explicit partition/impact mappings, and scoped identity/authorization.
