# Object-backed control plane

## Decision: reuse a storage engine

SlateDB is the only metadata authority. Its Rust `object_store` abstraction supplies filesystem, memory, and S3 implementations to the Python binding. `obstore` supplies the same family of interfaces for immutable data objects. There is no custom HEAD object layered above SlateDB, no second journal, and no SQLite file asynchronously backed up to a bucket.

```
API / console -> Engine -> serializable SlateDB transaction -> object-store WAL
                      \-> worker subprocess -> prepared JSON objects
```

All user project imports, manifest construction, and transformations occur in subprocesses. The coordinator uses a JSON manifest. This separates user-library imports from the server, but does not sandbox execution.

## Acknowledgement boundary

A native `commit()` result is not necessarily remote durability. Every dirty transaction awaits `WriteHandle.await_durable()` before returning. A process-local lock spans this wait and blocks application reads as well as other writes, so callers cannot observe a memory-only result and mistake it for an acknowledged result.

If commit/durability raises or is cancelled after commit begins, the process is poisoned: it refuses further application transactions. Restart and resolve a durable command/commit receipt. A timed-out operation may already have committed. Never infer absence from a timeout.

This conservative lock sacrifices write batching and throughput. It is an explicit prototype tradeoff. A future batched state machine must preserve the same visibility and acknowledgement boundary.

## Key spaces

- `definition/`, `system/`: manifests, schema, current revision.
- `command/`, `run/`, `runs/`: idempotency receipts, requests, time index.
- `task/`, `attempt/`, `ready/`, `active/`, `scope/`: scheduling and ownership.
- `head/`, `commit/`, `checkpoint/`, `item/`, `append/`: output versions and incremental state.
- `event/`, `automation/`, `outbox/`: audit events and automation decisions. Commit triggers are pended on the `automation/` record inside the publication transaction and delivered by the coordinator's evaluation loop once every pinned target input resolves; triggered runs never re-execute upstream producers.

Keys are separate indexed records, not one huge JSON workspace blob. Some current query paths still scan full prefixes; this is not yet a high-cardinality scheduler benchmark.

## Materialization protocol

1. Commit a task claim and its generation. Bind exact input references and baseline output heads.
2. Read the bounded source-change plan and checkpoint. Run trusted user code outside metadata transactions.
3. Upload immutable, SHA-256 addressed JSON outputs with create-if-absent. Verify hashes on load.
4. Recheck attempt generation, live lease, scope ownership, input references, baseline output heads, and checkpoint generation inside the publication transaction.
5. Atomically write every output head, one shared commit, the new checkpoint, item acknowledgements, append receipts, task/attempt completion, ready downstream tasks, and an outbox record.
6. Await object-store durability and only then acknowledge success.

An upload is not a publication. Readers resolve `head/` references, not object-prefix listings. A failed multi-output staging operation can leave orphan objects, but cannot move one output head without the other.

A stable attempt commit ID plus a full publication fingerprint prevents duplicate publication and rejects a replay that reuses the identity with different outputs, cursor, or item acknowledgement. Request IDs similarly deduplicate accepted run requests across restarts.

## Safety choices

There is one publication owner per producer/partition. Distinct output names cannot be registered to two producers. Multi-output heads are published in one transaction. Late workers are fenced at publication, not merely when reporting success.

A new native writer fences the previous native writer. Initialization also abandons/requeues interrupted domain attempts and advances their generations. Repeated interruption is bounded by the task's attempt budget. A failed runtime becomes unhealthy and does not reopen itself, avoiding a writer-takeover fight with its replacement. Deploy one process and one replica per namespace; `uvicorn --workers N` is unsafe for this mode.

ByKey item records include source revision and an interpretation fingerprint. Their consumption positions are per producer/partition, so one consumer cannot acknowledge another consumer's changes. Incomplete inventories do not generate deletion work. Empty child replacements remove obsolete rows. Partial batch heads are marked incomplete so fill-missing cannot silently accept an unfinished asset.

Input-version checks deliberately reject stale publications when an upstream head has advanced. This is conservative: concurrent requests may need resubmission rather than letting an old backfill overwrite newer data. Historical inputs remain immutable, but historical code artifacts are not retained.

A dependency across a multi-output producer must be read from the same completed upstream task. An output bundle has atomic metadata visibility. There is no claim of atomicity for a function independently mutating an external database or API.

## Recovery and deployment

For S3, every acknowledged durable state transition and JSON output is reconstructible with just the bucket, namespace, and installed project. Local filesystem mode makes the selected directory the object store; that directory itself is not disposable. Functional filesystem tests do not establish power-loss/fsync guarantees or multi-host semantics.

SlateDB handles its metadata WAL, replay, SSTs, compaction, and metadata garbage collection. The orchestrator does not yet collect orphan/obsolete output objects or prune run history. Never apply blanket lifecycle expiry to live metadata prefixes. Retention and state-schema migrations require explicit design before production.

## Source references

- https://slatedb.io/docs/design/writes/ — write handles and remote durability.
- https://slatedb.io/docs/design/consistency/ — consistency, transactions, writer fencing.
- https://slatedb.io/docs/get-started/quickstart/ — native binding and object stores.
- https://developmentseed.org/obstore/latest/api/store/local/ — LocalStore.
- https://developmentseed.org/obstore/latest/api/put/ — conditional put operations.
- https://cursor.com/blog/git-at-any-scale — motivating separation of durable object state and fast local representations.

## Review regressions

Incomplete-inventory flags are propagated through every output reference, including ordinary snapshot transformations; a downstream keyed consumer must not turn a partial scan into deletions. A no-change skip also requires complete inputs. Manifest protocol files are separate from user stdout. Subprocess log pipes have bounded buffers and an 8 MiB aggregate limit; exceeding it terminates the process group. Registration rejects ambiguous input/resource/context bindings and unsupported signatures.

Engine lifecycle is one-shot: after `stop`, construct a new Engine and call `initialize` before resuming work. That startup reconciles persisted active attempts; a process interrupted at an uncertain commit must not blindly release its scope.
