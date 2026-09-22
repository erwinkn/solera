"""Domain state over SlateDB (§1, §4): heads, cursors, edge watermarks,
commits, automation state, runs/tasks/attempts with per-scope fencing,
workers and pool claims.

The in-memory model is canonical for the working set (§4.2): heads, cursors,
watermarks, automation records, locks, workers, and the non-terminal
run/task slice live in memory; durable SlateDB keys are write-through and
are read at open plus whenever the API asks for evicted history.

Durable key layout (all under the SlateDB namespace):

    sys/schema, sys/revision, sys/project, sys/commit_seq, sys/entrypoint
    manifest/{revision}
    head/{output}/{scope}            -> committed ref + metadata
    cursor/{asset}/{scope}           -> producer cursor (§6)
    watermark/{asset}/{edge}/{scope} -> {batch, offset, fingerprint} (§2.2)
    commit/{seq:020d}                -> commit record, ordered by seq
    automation_state/{name}          -> {enabled, last_*, commit_watermark}
    run/{id}                         -> run request record
    task/{id}                        -> one per (asset, scope) of a run
    attempt/{task}/{generation}      -> one per execution of a task
    lock/{asset}/{scope}             -> scope fencing {attempt, generation}
    worker/{id}                      -> registered external worker

Memory-only, rebuilt at open (§4.2): the ready queue, pending-per-scope,
scope outcomes, pool claims, lease expiries, the dependents index with
unfinished-dependency counters, per-run rollup counters, and automation
pending events (derived from commits since `commit_watermark`).
"""

from __future__ import annotations

import asyncio
import copy
import json
from urllib.parse import quote

import obstore

from .storage import (
    SlateState,
    Transaction,  # re-export
)


def esc(value) -> str:
    return quote(str(value), safe="")


def unesc(value: str) -> str:
    from urllib.parse import unquote

    return unquote(value)


class LostOwnership(Exception):
    """The attempt no longer owns its scope (fencing, §8)."""


class Conflict(Exception):
    """A commit precondition failed (moved head, stale generation).

    retryable=True for races (a head moved under the attempt); False for
    violations no retry can fix (undeclared output, missing prior head)."""

    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


_DEL = object()
_MISSING = object()

TERMINAL_TASK = frozenset({"succeeded", "skipped", "failed", "blocked", "canceled"})
TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
# Dep outcomes that block (never run) their dependents.
BAD_OUTCOME = frozenset({"failed", "blocked", "canceled"})
# Task statuses still fencing a scope lease.
LIVE_ATTEMPT = frozenset({"claimed", "running"})
# Non-terminal task statuses that a dead lease requeues.
RECOVERABLE = frozenset({"running", "claimable"})

# Removed durable prefixes, swept once at open (§4.1): `automation/` records
# migrate to `automation_state/` first; the rest were caches the in-memory
# model now owns outright.
LEGACY_PREFIXES = ("queue/", "pending/", "runindex/", "active/", "pool/", "scope/", "keystate/")


class Tx:
    """Domain operations inside one SlateDB transaction (§4.2).

    Reads hit the transaction's write overlay, then State's in-memory model,
    then — for evicted history (terminal runs/tasks, attempts, commits) —
    the inner SlateDB transaction. Writes land in the overlay plus the inner
    transaction; the overlay applies to memory only after the durable commit
    (or the clean rollback of a memory-only write) is acknowledged."""

    def __init__(self, inner: Transaction, state: "State"):
        self.t = inner
        self.state = state
        self.clock = state.clock
        self._ov: dict[str, dict] = {}
        self._seq = 0

    get = property(lambda self: self.t.get)
    put = property(lambda self: self.t.put)
    delete = property(lambda self: self.t.delete)
    scan = property(lambda self: self.t.scan)

    # -- overlay plumbing -------------------------------------------------------

    def _w(self, coll: str, key, value):
        self._ov.setdefault(coll, {})[key] = value

    def _r(self, coll: str, key, mem: dict | None):
        entry = self._ov.get(coll)
        if entry is not None and key in entry:
            value = entry[key]
            return None if value is _DEL else value
        if mem is not None and key in mem:
            return mem[key]
        return _MISSING

    def _merged(self, coll: str, mem: dict) -> dict:
        merged = dict(mem)
        for key, value in self._ov.get(coll, {}).items():
            if value is _DEL:
                merged.pop(key, None)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _copy(value):
        return copy.deepcopy(value) if isinstance(value, (dict, list)) else value

    # -- heads ------------------------------------------------------------------

    async def head(self, output: str, scope: str):
        value = self._r("heads", (output, scope), self.state._heads)
        return None if value is _MISSING else self._copy(value)

    async def put_head(self, output: str, scope: str, record: dict):
        await self.t.put(f"head/{esc(output)}/{esc(scope)}", record)
        self._w("heads", (output, scope), record)

    async def heads(self, output: str):
        return sorted(
            (scope, self._copy(rec))
            for (out, scope), rec in self._merged("heads", self.state._heads).items()
            if out == output
        )

    async def all_heads(self):
        return [
            (f"head/{esc(out)}/{esc(scope)}", self._copy(rec))
            for (out, scope), rec in self._merged("heads", self.state._heads).items()
        ]

    # -- cursors ----------------------------------------------------------------

    async def cursor(self, asset: str, scope: str):
        value = self._r("cursors", (asset, scope), self.state._cursors)
        return None if value is _MISSING else self._copy(value)

    async def put_cursor(self, asset: str, scope: str, value):
        if value is None:
            await self.t.delete(f"cursor/{esc(asset)}/{esc(scope)}")
            self._w("cursors", (asset, scope), _DEL)
        else:
            await self.t.put(f"cursor/{esc(asset)}/{esc(scope)}", value)
            self._w("cursors", (asset, scope), value)

    # -- per-edge watermarks (§2.2) ----------------------------------------------
    # watermark = {batch, offset, fingerprint}: all items of delta batches < batch
    # are delivered, plus the first `offset` items of batch `batch`. A missing or
    # fingerprint-mismatched watermark resets the edge to a full delivery.

    async def watermark(self, asset: str, edge: str, scope: str):
        value = self._r("watermarks", (asset, edge, scope), self.state._watermarks)
        return None if value is _MISSING else self._copy(value)

    async def put_watermark(self, asset, edge, scope, record):
        await self.t.put(f"watermark/{esc(asset)}/{esc(edge)}/{esc(scope)}", record)
        self._w("watermarks", (asset, edge, scope), record)

    async def del_watermark(self, asset, edge, scope):
        await self.t.delete(f"watermark/{esc(asset)}/{esc(edge)}/{esc(scope)}")
        self._w("watermarks", (asset, edge, scope), _DEL)

    # -- commits ------------------------------------------------------------------
    # commit/{seq:020d}: the sequence is durable in sys/commit_seq, allocated
    # inside the commit's own transaction so a rolled-back tx leaves no gap.

    async def commit_record(self, commit_id: str):
        return await self.t.get(f"commit/{commit_id}")

    async def put_commit(self, record: dict) -> str:
        seq = max(self.state._commit_seq, self._seq) + 1
        self._seq = seq
        record["id"] = f"{seq:020d}"
        await self.t.put(f"commit/{record['id']}", record)
        await self.t.put("sys/commit_seq", seq)
        self._w("commit_seq", "v", seq)
        return record["id"]

    async def commits(self, limit=100):
        """Newest-first commit listing: the seq key is the order (§4.1)."""

        out = []
        seq, probes = max(self.state._commit_seq, self._seq), 0
        while len(out) < limit and seq > 0 and probes < 4 * limit + 16:
            key = f"commit/{seq:020d}"
            record = await self.t.get(key)
            seq, probes = seq - 1, probes + 1
            if record is not None:
                out.append((key, record))
        return out

    async def commits_after(self, seq: int, limit=None):
        """Commit log entries after `seq` — automation watermark consumption (§4.3)."""

        return await self.t.scan("commit/", limit, after=f"commit/{seq:020d}")

    # -- automations ---------------------------------------------------------------

    async def automation(self, name: str):
        value = self._r("automations", name, self.state._automations)
        return None if value is _MISSING else self._copy(value)

    async def put_automation(self, name: str, record: dict):
        await self.t.put(f"automation_state/{esc(name)}", record)
        self._w("automations", name, record)

    async def automations(self):
        return sorted(
            (name, self._copy(rec))
            for name, rec in self._merged("automations", self.state._automations).items()
        )

    async def auto_pending(self, name: str) -> list:
        return list(self.state._auto_pending.get(name, ()))

    async def set_auto_pending(self, name: str, events):
        self._w("auto_pending", name, list(events) if events else _DEL)

    # -- runs ----------------------------------------------------------------------

    async def run(self, run_id: str):
        value = self._r("runs", run_id, self.state._runs)
        if value is _MISSING:
            return await self.t.get(f"run/{esc(run_id)}")
        return self._copy(value)

    async def put_run(self, run: dict):
        await self.t.put(f"run/{esc(run['id'])}", run)
        self._w("runs", run["id"], run)

    async def runs(self, limit=50):
        """Newest-first by created_at (§4.2: the listing index is in memory).
        Live runs hit memory; evicted history is point-gets, never a scan."""

        order = dict(self.state._run_order)
        for rid, value in self._ov.get("runs", {}).items():
            if value is _DEL:
                order.pop(rid, None)
            else:
                order[rid] = value.get("created_at") or 0
        out = []
        for rid in sorted(order, key=lambda r: (order[r], r), reverse=True)[:limit]:
            run = await self.run(rid)
            if run is not None:
                out.append(run)
        return out

    # -- tasks ---------------------------------------------------------------------

    async def task(self, task_id: str):
        value = self._r("tasks", task_id, self.state._tasks)
        if value is _MISSING:
            return await self.t.get(f"task/{esc(task_id)}")
        return self._copy(value)

    async def put_task(self, task: dict):
        await self.t.put(f"task/{esc(task['id'])}", task)
        self._w("tasks", task["id"], task)

    # pending index (memory): set at submit, cleared at the terminal transition.

    async def put_pending(self, task: dict):
        self._w("pending", (task["asset"], task["scope"], task["id"]), True)

    async def del_pending(self, task: dict):
        self._w("pending", (task["asset"], task["scope"], task["id"]), _DEL)

    def _pending_set(self, asset: str, scope: str) -> set:
        out = set(self.state._pending.get((asset, scope), ()))
        for (a, s, tid), value in self._ov.get("pending", {}).items():
            if a == asset and s == scope:
                (out.discard if value is _DEL else out.add)(tid)
        return out

    async def pending(self, asset: str, scope: str) -> bool:
        return bool(self._pending_set(asset, scope))

    async def pending_scopes(self, asset: str) -> set[str]:
        candidates = {s for (a, s) in self.state._pending if a == asset}
        candidates |= {s for (a, s, _) in self._ov.get("pending", {}) if a == asset}
        return {s for s in candidates if self._pending_set(asset, s)}

    # -- per-scope outcomes (§8, memory; rebuilt at open) ------------------------------

    async def scope_outcome(self, asset: str, scope: str):
        value = self._r("outcomes", (asset, scope), self.state._outcomes)
        return None if value is _MISSING else self._copy(value)

    async def scope_outcomes(self, asset: str) -> dict[str, dict]:
        return {
            scope: self._copy(rec)
            for (a, scope), rec in self._merged("outcomes", self.state._outcomes).items()
            if a == asset
        }

    async def put_scope_outcome(self, asset: str, scope: str, outcome: str, attempt: str | None = None):
        prior = await self.scope_outcome(asset, scope)
        self._w(
            "outcomes",
            (asset, scope),
            {
                "last_outcome": outcome,
                "last_attempt": attempt or (prior or {}).get("last_attempt"),
                "at": self.clock(),
            },
        )

    # -- attempts --------------------------------------------------------------------
    # attempt/{task}/{generation}: durable history; no lease_until (§4.3).

    async def attempt(self, task_id: str, generation: int):
        value = self._r("attempts", (task_id, generation), None)
        if value is _MISSING:
            return await self.t.get(f"attempt/{esc(task_id)}/{generation:010d}")
        return self._copy(value)

    async def put_attempt(self, task_id: str, generation: int, record: dict):
        await self.t.put(f"attempt/{esc(task_id)}/{generation:010d}", record)
        self._w("attempts", (task_id, generation), record)

    async def attempts(self, task_id: str):
        merged = {
            int(k.rsplit("/", 1)[-1]): v for k, v in await self.t.scan(f"attempt/{esc(task_id)}/")
        }
        for (tid, gen), value in self._ov.get("attempts", {}).items():
            if tid == task_id:
                if value is _DEL:
                    merged.pop(gen, None)
                else:
                    merged[gen] = value
        return [(f"attempt/{esc(task_id)}/{gen:010d}", merged[gen]) for gen in sorted(merged)]

    # -- scope fencing -----------------------------------------------------------------
    # lock/{asset}/{scope} = {attempt, generation} — durable at claim time;
    # the lease expiry itself lives in memory (§4.3).

    async def lock(self, asset: str, scope: str):
        value = self._r("locks", (asset, scope), self.state._locks)
        return None if value is _MISSING else self._copy(value)

    async def locks(self):
        return sorted(self._merged("locks", self.state._locks).items())

    async def put_lock(self, asset: str, scope: str, record: dict):
        record = {"attempt": record["attempt"], "generation": record["generation"]}
        await self.t.put(f"lock/{esc(asset)}/{esc(scope)}", record)
        self._w("locks", (asset, scope), record)

    async def del_lock(self, asset: str, scope: str):
        await self.t.delete(f"lock/{esc(asset)}/{esc(scope)}")
        self._w("locks", (asset, scope), _DEL)

    # -- lease expiries (memory only, §4.3) ---------------------------------------------

    async def lease(self, attempt_id: str):
        value = self._r("leases", attempt_id, self.state._leases)
        return None if value is _MISSING else value

    async def set_lease(self, attempt_id: str, until: float):
        self._w("leases", attempt_id, until)

    async def clear_lease(self, attempt_id: str):
        self._w("leases", attempt_id, _DEL)

    async def lease_until(self, lock) -> float:
        """A lock's expiry: absent lease state means expired (open semantics)."""

        return await self.lease(lock["attempt"]) or 0.0

    # -- dispatch queue (memory only) -----------------------------------------------------

    async def enqueue(self, task_id: str, ready_at: float):
        self._w("queue", task_id, int(ready_at * 1000))

    async def dequeue(self, task_id: str):
        self._w("queue", task_id, _DEL)

    async def queued(self):
        """Due order as [(task_id, ready_ms)] — the ready queue is memory (§4.2)."""

        merged = self._merged("queue", self.state._queue)
        return sorted(merged.items(), key=lambda kv: (kv[1], kv[0]))

    # -- pool tasks + workers ------------------------------------------------------------------
    # A pool attempt is a task with placement.kind == "Pool" and status
    # `claimable`; the claimable listing itself is memory — the spec is
    # already in the object store (§4.1).

    async def pool_task(self, attempt_id: str):
        value = self._r("pool", attempt_id, self.state._pool)
        return None if value is _MISSING else self._copy(value)

    async def put_pool_task(self, attempt_id: str, record: dict):
        self._w("pool", attempt_id, record)

    async def del_pool_task(self, attempt_id: str):
        self._w("pool", attempt_id, _DEL)

    async def pool_tasks(self):
        return sorted(self._merged("pool", self.state._pool).items())

    async def worker(self, worker_id: str):
        value = self._r("workers", worker_id, self.state._workers)
        return None if value is _MISSING else self._copy(value)

    async def put_worker(self, worker_id: str, record: dict):
        await self.t.put(f"worker/{esc(worker_id)}", record)
        self._w("workers", worker_id, record)

    async def workers(self):
        return sorted(self._merged("workers", self.state._workers).items())

    # -- dependents index + rollup counters (§4.2) ------------------------------------------
    # _deps[task] = waiting dependents; _unfinished[task] = {left, bad, deps}
    # for waiting tasks; _runstats[run] = {left, bad} over non-terminal tasks.

    async def dependents(self, task_id: str) -> set:
        merged = set(self.state._deps.get(task_id, ()))
        entry = self._ov.get("deps", {})
        if task_id in entry:
            merged = set() if entry[task_id] is _DEL else set(entry[task_id])
        return merged

    async def set_dependents(self, task_id: str, ids):
        self._w("deps", task_id, set(ids) if ids else _DEL)

    async def add_dependent(self, task_id: str, dependent: str):
        await self.set_dependents(task_id, (await self.dependents(task_id)) | {dependent})

    async def unfinished(self, task_id: str):
        value = self._r("unfinished", task_id, self.state._unfinished)
        return None if value is _MISSING else dict(value)

    async def set_unfinished(self, task_id: str, record):
        self._w("unfinished", task_id, dict(record) if record else _DEL)

    async def run_stats(self, run_id: str):
        value = self._r("runstats", run_id, self.state._runstats)
        return None if value is _MISSING else dict(value)

    async def set_run_stats(self, run_id: str, record):
        self._w("runstats", run_id, dict(record) if record else _DEL)


class State:
    """The control-plane state (§1): never moves, parses or interprets payloads."""

    def __init__(self, store: SlateState, clock=None):
        import time

        self.store = store
        self.clock = clock or time.time
        self.objects = store.objects
        self.namespace = store.namespace
        self.url = store.url
        self.objects_url = store.objects_url
        self._loaded = False
        self._load_lock = asyncio.Lock()
        self._commit_seq = 0
        self._heads: dict = {}
        self._cursors: dict = {}
        self._watermarks: dict = {}
        self._automations: dict = {}
        self._locks: dict = {}
        self._workers: dict = {}
        self._tasks: dict = {}
        self._runs: dict = {}
        self._run_order: dict = {}
        self._queue: dict = {}
        self._pending: dict = {}
        self._outcomes: dict = {}
        self._pool: dict = {}
        self._leases: dict = {}
        self._deps: dict = {}
        self._unfinished: dict = {}
        self._runstats: dict = {}
        self._auto_pending: dict = {}

    def transaction(self) -> Tx:
        return _TxCtx(self)

    async def _ensure_loaded(self):
        if self._loaded:
            return
        async with self._load_lock:
            if not self._loaded:
                await self._load()

    async def _load(self):
        """Rebuild the in-memory model from the durable prefixes (§4.2).

        Runs under the single-writer lock: one transaction reads every
        trimmed prefix, migrates legacy `automation/` records to
        `automation_state/`, sweeps the removed prefixes, and requeues every
        leased scope — on open all leases are expired (§4.3)."""

        now = self.clock()
        async with self.store.transaction() as t:
            seq = await t.get("sys/commit_seq")
            heads = await t.scan("head/")
            cursors = await t.scan("cursor/")
            watermarks = await t.scan("watermark/")
            autos = await t.scan("automation_state/")
            legacy_autos = await t.scan("automation/")
            locks = await t.scan("lock/")
            workers = await t.scan("worker/")
            runs = await t.scan("run/")
            tasks = await t.scan("task/")
            if seq is None:
                # One-time migration: derive the sequence high-water mark from
                # any numeric commit keys a pre-§4.1 store may carry.
                seq = 0
                for key, _ in await t.scan("commit/"):
                    suffix = key.split("/", 1)[1]
                    if suffix.isdigit():
                        seq = max(seq, int(suffix))
                await t.put("sys/commit_seq", seq)
            legacy_pending = {}
            for key, record in legacy_autos:
                # Migrate `automation/` to `automation_state/` (§4.1): pending[]
                # becomes in-memory events; the commit watermark starts at the
                # log's current end (pre-sequence commits are not replayed).
                pending = record.pop("pending", None) or []
                if pending:
                    legacy_pending[record["name"]] = pending
                record["commit_watermark"] = int(seq)
                await t.put(f"automation_state/{esc(record['name'])}", record)
                await t.delete(key)
            for key, value in await t.scan("command/"):
                # Idempotency receipts move under sys/ (§4.1).
                await t.put(f"sys/command/{key.split('/', 1)[1]}", value)
                await t.delete(key)
            for prefix in LEGACY_PREFIXES:
                for key, _ in await t.scan(prefix):
                    await t.delete(key)

        self._commit_seq = int(seq)
        self._heads = {self._head_key(k): v for k, v in heads}
        self._cursors = {self._pair_key(k, "cursor/"): v for k, v in cursors}
        self._watermarks = {self._triple_key(k, "watermark/"): v for k, v in watermarks}
        self._locks = {}
        self._leases = {}
        self._workers = {unesc(k.split("/", 1)[1]): v for k, v in workers}
        self._queue, self._pending, self._outcomes = {}, {}, {}
        self._pool = {}
        self._deps, self._unfinished, self._runstats = {}, {}, {}
        self._auto_pending = legacy_pending
        self._automations = {}
        for key, record in autos:
            record.pop("pending", None)
            record.setdefault("commit_watermark", self._commit_seq)
            self._automations[unesc(key.split("/", 1)[1])] = record
        for _, record in legacy_autos:
            self._automations[record["name"]] = record
        run_records = {unesc(k.split("/", 1)[1]): v for k, v in runs}
        task_records = {unesc(k.split("/", 1)[1]): v for k, v in tasks}
        self._runs = {
            rid: rec for rid, rec in run_records.items() if rec.get("status") not in TERMINAL_RUN
        }
        self._run_order = {rid: rec.get("created_at") or 0 for rid, rec in run_records.items()}
        self._tasks = {
            tid: rec
            for tid, rec in task_records.items()
            if rec.get("run") in self._runs and rec.get("status") not in TERMINAL_TASK
        }
        # Scope outcomes fold: the newest run's terminal task per scope wins.
        stamps = {}
        for tid, rec in task_records.items():
            if rec.get("status") not in TERMINAL_TASK:
                continue
            run = run_records.get(rec["run"]) or {}
            key = (rec["asset"], rec["scope"])
            stamp = run.get("created_at") or 0
            if key not in stamps or stamp >= stamps[key]:
                stamps[key] = stamp
                self._outcomes[key] = {
                    "last_outcome": rec["status"],
                    "last_attempt": f"{tid}/{rec.get('generation', 0)}",
                    "at": run.get("updated_at") or stamp,
                }
        for tid, task in self._tasks.items():
            self._pending.setdefault((task["asset"], task["scope"]), set()).add(tid)
            if task["status"] == "queued":
                self._queue[tid] = int((task.get("ready_at") or 0) * 1000)
            elif task["status"] == "waiting":
                left = bad = 0
                for dep in task.get("deps") or []:
                    status = (task_records.get(dep) or {}).get("status")
                    if status in TERMINAL_TASK:
                        bad += status in BAD_OUTCOME
                    else:
                        left += 1
                        self._deps.setdefault(dep, set()).add(tid)
                self._unfinished[tid] = {"left": left, "bad": bad, "deps": task.get("deps") or []}
        for rid, run in self._runs.items():
            owned = [task_records.get(tid) for tid in run.get("tasks") or ()]
            self._runstats[rid] = {
                "left": sum(1 for t in owned if t and t.get("status") not in TERMINAL_TASK),
                "bad": sum(1 for t in owned if t and t.get("status") in BAD_OUTCOME),
            }
        # §4.3: every lock is expired at open — expire the attempt and requeue
        # the task through the lease-recovery path (sweep_scope_leases).
        async with self.store.transaction() as t:
            for key, lock in locks:
                asset, scope = self._pair_key(key, "lock/")
                task_id, _, generation = lock["attempt"].rpartition("/")
                gen = int(generation)
                arec = await t.get(f"attempt/{esc(task_id)}/{gen:010d}")
                if arec and arec["status"] in LIVE_ATTEMPT:
                    arec["status"] = "expired"
                    arec["finished_at"] = now
                    await t.put(f"attempt/{esc(task_id)}/{gen:010d}", arec)
                await t.delete(key)
                task = self._tasks.get(task_id)
                if task and task["status"] in RECOVERABLE:
                    task["status"] = "queued"
                    task["ready_at"] = now
                    await t.put(f"task/{esc(task_id)}", task)
                    self._queue[task_id] = int(now * 1000)
            locked = {self._pair_key(k, "lock/") for k, _ in locks}
            for tid, task in self._tasks.items():
                # A lockless in-flight task is stranded work — requeue it too.
                if task["status"] in RECOVERABLE and (task["asset"], task["scope"]) not in locked:
                    task["status"] = "queued"
                    task["ready_at"] = now
                    await t.put(f"task/{esc(tid)}", task)
                    self._queue[tid] = int(now * 1000)
        self._loaded = True

    @staticmethod
    def _head_key(key: str):
        output, scope = key.split("/", 1)[1].rsplit("/", 1)
        return unesc(output), unesc(scope)

    @staticmethod
    def _pair_key(key: str, prefix: str):
        first, second = key[len(prefix) :].split("/", 1)
        return unesc(first), unesc(second)

    @staticmethod
    def _triple_key(key: str, prefix: str):
        first, rest = key[len(prefix) :].split("/", 1)
        second, third = rest.rsplit("/", 1)
        return unesc(first), unesc(second), unesc(third)

    def _apply(self, ov: dict):
        """Fold a committed transaction's overlay into the in-memory model.
        Runs inside the writer lock after the durable commit lands."""

        for seq in ov.get("commit_seq", {}).values():
            if seq is not _DEL:
                self._commit_seq = max(self._commit_seq, seq)
        for (asset, scope, tid), value in ov.get("pending", {}).items():
            bucket = self._pending.setdefault((asset, scope), set())
            (bucket.discard if value is _DEL else bucket.add)(tid)
            if not bucket:
                self._pending.pop((asset, scope), None)
        for key, value in ov.get("locks", {}).items():
            if value is _DEL:
                old = self._locks.pop(key, None)
                if old:
                    self._leases.pop(old["attempt"], None)
            else:
                self._locks[key] = value
        for key, value in ov.get("leases", {}).items():
            if value is _DEL:
                self._leases.pop(key, None)
            else:
                self._leases[key] = value
        for key, value in ov.get("tasks", {}).items():
            if value is _DEL or value["status"] in TERMINAL_TASK:
                self._tasks.pop(key, None)
            else:
                self._tasks[key] = value
        for key, value in ov.get("runs", {}).items():
            record = value if value is not _DEL else self._runs.get(key)
            if value is _DEL or value["status"] in TERMINAL_RUN:
                # A terminal run leaves the working set (§4.2): its tasks and
                # indexes evict; SlateDB keeps the history for the API.
                self._runs.pop(key, None)
                self._runstats.pop(key, None)
                for tid in (record or {}).get("tasks") or ():
                    task = self._tasks.pop(tid, None)
                    self._queue.pop(tid, None)
                    self._deps.pop(tid, None)
                    self._unfinished.pop(tid, None)
                    if task:
                        bucket = self._pending.get((task["asset"], task["scope"]))
                        if bucket:
                            bucket.discard(tid)
                            if not bucket:
                                self._pending.pop((task["asset"], task["scope"]))
            else:
                self._runs[key] = value
        for rid, value in ov.get("runs", {}).items():
            # The listing index tracks every run, live or evicted.
            if value is not _DEL:
                self._run_order[rid] = value.get("created_at") or self._run_order.get(rid, 0)
        for coll, mem in (
            ("heads", self._heads),
            ("cursors", self._cursors),
            ("watermarks", self._watermarks),
            ("automations", self._automations),
            ("workers", self._workers),
            ("queue", self._queue),
            ("outcomes", self._outcomes),
            ("pool", self._pool),
            ("deps", self._deps),
            ("unfinished", self._unfinished),
            ("runstats", self._runstats),
            ("auto_pending", self._auto_pending),
        ):
            for key, value in ov.get(coll, {}).items():
                if value is _DEL:
                    mem.pop(key, None)
                else:
                    mem[key] = value

    async def get(self, key: str, default=None):
        async with self.transaction() as tx:
            return await tx.get(key, default)

    async def scan(self, prefix: str, limit=None):
        async with self.transaction() as tx:
            return await tx.scan(prefix, limit)

    async def automations(self) -> list:
        async with self.transaction() as tx:
            return await tx.automations()

    # -- project registration ---------------------------------------------------

    async def initialize(self, manifest: dict, revision: str):
        """Install the project manifest and seed automation state (§11, §9).

        Automation toggles and the commit watermark survive a re-registration
        when the trigger is unchanged; a changed trigger restarts consumption
        at the log's current end."""

        async with self.transaction() as tx:
            await tx.put(f"manifest/{esc(revision)}", manifest)
            for key, _ in await tx.scan("manifest/"):
                if key != f"manifest/{esc(revision)}":
                    await tx.delete(key)
            await tx.put("sys/revision", revision)
            await tx.put("sys/project", manifest.get("project") or manifest.get("name"))
            for auto in manifest["automations"].values():
                existing = await tx.automation(auto["name"])
                record = {
                    **auto,
                    "last_at": None,
                    "last_run": None,
                    "last_revision": None,
                    "commit_watermark": self._commit_seq,
                }
                if existing is not None:
                    record["enabled"] = existing["enabled"]
                    if existing["trigger"] == auto["trigger"]:
                        record["last_at"] = existing.get("last_at")
                        record["last_run"] = existing.get("last_run")
                        record["last_revision"] = existing.get("last_revision")
                        record["commit_watermark"] = existing.get(
                            "commit_watermark", self._commit_seq
                        )
                await tx.put_automation(auto["name"], record)

    async def manifest(self) -> dict:
        async with self.transaction() as tx:
            revision = await tx.get("sys/revision")
            if revision is None:
                raise Conflict("no project registered")
            manifest = await tx.get(f"manifest/{esc(revision)}")
            manifest["revision"] = revision
            return manifest

    # -- automation commit-log consumption (§4.3) ---------------------------------

    async def automation_events(self, tx: Tx, auto: dict):
        """(events, high_watermark) — OnChange work this automation has not
        consumed: migrated pending events plus commit-log entries after
        `commit_watermark` whose `changed` set touches `watched` (§9)."""

        migrated = tx._r("auto_pending", auto["name"], self._auto_pending)
        events = [] if migrated in (_MISSING, None, _DEL) else list(migrated)
        watched = set(auto.get("watched") or auto["trigger"].get("outputs") or [])
        watermark = int(auto.get("commit_watermark") or 0)
        high = watermark
        for key, commit in await tx.commits_after(watermark):
            suffix = key.rsplit("/", 1)[-1]
            if not suffix.isdigit():
                continue  # pre-sequence commit ids are history, not events
            high = max(high, int(suffix))
            changed = set(commit.get("changed") or [])
            if changed & watched:
                events.append(
                    {
                        "commit": commit.get("id"),
                        "asset": commit.get("asset"),
                        "scope": commit.get("scope") or "",
                        "outputs": sorted(changed & watched),
                    }
                )
        return events, high

    # -- fencing (§8) ---------------------------------------------------------

    async def _expire_lock(self, tx: Tx, asset: str, scope: str, lock: dict, now: float):
        """Lease-recovery path shared by sweep, claim-takeover and open: fence
        the dead attempt and requeue its task (§4.3)."""

        task_id, _, generation = lock["attempt"].rpartition("/")
        attempt = await tx.attempt(task_id, int(generation))
        await tx.del_lock(asset, scope)
        await tx.del_pool_task(lock["attempt"])
        if attempt and attempt["status"] in LIVE_ATTEMPT:
            attempt["status"] = "expired"
            attempt["finished_at"] = now
            await tx.put_attempt(task_id, int(generation), attempt)
        task = await tx.task(task_id)
        if task and task["status"] in RECOVERABLE:
            task["status"] = "queued"
            task["ready_at"] = now
            await tx.enqueue(task["id"], task["ready_at"])
            await tx.put_task(task)

    async def claim(self, tx: Tx, task: dict, lease_seconds: float) -> dict:
        """Take the per-scope lock under a generation; returns the attempt record.

        The lock record is durable at claim time; the lease itself is memory
        (§4.3). An expired lease lets a new claimant take over: the old
        attempt is fenced and any commit it tries fails with LostOwnership (§8)."""

        now = self.clock()
        lock = await tx.lock(task["asset"], task["scope"])
        if lock is not None:
            if await tx.lease_until(lock) > now:
                raise Conflict(f"scope {task['scope']} is already claimed")
            await self._expire_lock(tx, task["asset"], task["scope"], lock, now)
        task["generation"] += 1
        task["attempt_count"] += 1
        attempt_id = f"{task['id']}/{task['generation']}"
        lease_until = now + lease_seconds
        await tx.put_lock(
            task["asset"],
            task["scope"],
            {"attempt": attempt_id, "generation": task["generation"]},
        )
        await tx.set_lease(attempt_id, lease_until)
        await tx.put_attempt(
            task["id"],
            task["generation"],
            {
                "id": attempt_id,
                "task": task["id"],
                "generation": task["generation"],
                "status": "claimed",
                "started_at": now,
            },
        )
        return {"id": attempt_id, "generation": task["generation"], "lease_until": lease_until}

    async def renew(self, tx: Tx, attempt_id: str, lease_seconds: float):
        """Extend the attempt's lease — memory only (§4.3); raises
        LostOwnership when fenced (§8, §10)."""

        info = await self._lock_of(tx, attempt_id)
        if info is None:
            raise LostOwnership(attempt_id)
        asset, scope = info
        lock = await tx.lock(asset, scope)
        if lock is None or lock["attempt"] != attempt_id:
            raise LostOwnership(attempt_id)
        lease_until = self.clock() + lease_seconds
        await tx.set_lease(attempt_id, lease_until)
        return lease_until

    async def _lock_of(self, tx: Tx, attempt_id: str):
        task_id, _, _ = attempt_id.rpartition("/")
        task = await tx.task(task_id)
        if task is None:
            return None
        return task["asset"], task["scope"]

    async def release(self, tx: Tx, task: dict):
        """Release the scope lock if this generation still owns it."""

        lock = await tx.lock(task["asset"], task["scope"])
        if lock and lock["attempt"] == f"{task['id']}/{task['generation']}":
            await tx.del_lock(task["asset"], task["scope"])
            await tx.clear_lease(lock["attempt"])

    async def owned(self, tx: Tx, task: dict) -> dict:
        """The attempt's lock record, or raise LostOwnership (fencing, §8)."""

        lock = await tx.lock(task["asset"], task["scope"])
        attempt_id = f"{task['id']}/{task['generation']}"
        if (
            lock is None
            or lock["attempt"] != attempt_id
            or await tx.lease_until(lock) <= self.clock()
        ):
            raise LostOwnership(attempt_id)
        return lock

    # -- the atomic commit (§8) -------------------------------------------------

    async def _owned_task(self, tx: Tx, attempt_id: str):
        """The task behind an attempt, verified to still own its scope lease."""

        task_id, _, _ = attempt_id.rpartition("/")
        task = await tx.task(task_id)
        if task is None or task["status"] not in {"running", "claimable"}:
            raise LostOwnership(attempt_id)
        lock = await tx.lock(task["asset"], task["scope"])
        if (
            lock is None
            or lock["attempt"] != attempt_id
            or await tx.lease_until(lock) <= self.clock()
        ):
            raise LostOwnership(attempt_id)
        return task

    async def commit_attempt(self, manifest, attempt_id: str, prepared: dict, result: dict) -> dict:
        """Install an attempt's result: heads, input_refs, cursor, edge
        watermarks and the commit record — all in one transaction (§8).
        OnChange automations consume the record from the log (§4.3)."""

        from cursus.sdk import UNSET

        async with self.transaction() as tx:
            task = await self._owned_task(tx, attempt_id)

            # Pinned inputs must still be the committed heads (§8); source heads
            # are seeded at initialize and replaced by source commits, so an
            # external pin that moved is caught like any other.
            for name, pin in prepared["inputs"].items():
                refs = pin["refs"].values() if "refs" in pin else [pin["ref"]]
                for ref in refs:
                    head = await tx.head(ref["output"], ref["partition"])
                    if head is None or head["ref"]["version"] != ref["version"]:
                        raise Conflict(
                            f"input {name}: {ref['output']}/{ref['partition']} moved after pinning"
                        )
            # Output heads must be unchanged since the claim.
            for output, baseline in prepared["baseline"].items():
                if await tx.head(output, task["scope"]) != baseline:
                    raise Conflict(f"output {output} head changed since this attempt was claimed")

            outputs = result.get("outputs") or {}
            asset = manifest["assets"][task["asset"]]
            declared = {o["name"]: o for o in asset["outputs"]}
            for name, ref in outputs.items():
                if name not in declared:
                    raise Conflict(f"result names undeclared output {name!r}", retryable=False)
                if ref["partition"] != task["scope"]:
                    raise Conflict(
                        f"output {name}: ref scope {ref['partition']!r} != {task['scope']!r}",
                        retryable=False,
                    )
                decl = declared[name]
                meta = ref.get("meta") or {}
                baseline_ref = (prepared["baseline"].get(name) or {}).get("ref")
                unchanged = baseline_ref is not None and baseline_ref["version"] == ref["version"]
                if unchanged:
                    # Identical content keeps the head as-is — no new delta or
                    # partitions list is written (and a legacy head may carry
                    # neither).
                    continue
                if decl.get("incremental") and not meta.get("delta"):
                    raise Conflict(f"incremental output {name}: ref carries no delta", retryable=False)
                if decl.get("partition_set") and meta.get("partitions") is None:
                    raise Conflict(
                        f"partition-set output {name}: ref carries no partitions list",
                        retryable=False,
                    )
            for name in set(declared) - set(outputs):
                if prepared["baseline"].get(name) is None:
                    raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)

            changed = sorted(
                name
                for name, ref in outputs.items()
                if prepared["baseline"].get(name) is None
                or prepared["baseline"][name]["ref"]["version"] != ref["version"]
            )
            record = {
                "attempt": attempt_id,
                "task": task["id"],
                "run": task["run"],
                "asset": task["asset"],
                "scope": task["scope"],
                "at": self.clock(),
                "input_refs": prepared["inputs"],
                "outputs": outputs,
                "changed": changed,
            }
            commit_id = await tx.put_commit(record)
            for name, ref in outputs.items():
                await tx.put_head(
                    name,
                    task["scope"],
                    {
                        "ref": ref,
                        "commit": commit_id,
                        "at": self.clock(),
                        "complete": prepared["scope_complete"],
                        "asset": task["asset"],
                        "version": asset["version"],
                    },
                )
            if result.get("cursor", UNSET) is not UNSET:
                await tx.put_cursor(task["asset"], task["scope"], result["cursor"])
            elif prepared.get("full"):
                # A full run withholds the cursor from the producer and clears
                # the committed one (§8: no prior, no cursor, watermarks reset).
                await tx.put_cursor(task["asset"], task["scope"], None)

            # Per-edge watermarks: the delivered position in each delta log (§2.2).
            for edge, update in (prepared.get("watermark_updates") or {}).items():
                await tx.put_watermark(task["asset"], edge, task["scope"], update)

            prior = await tx.attempt(task["id"], task["generation"]) or {}
            await tx.put_attempt(
                task["id"],
                task["generation"],
                {
                    "id": attempt_id,
                    "task": task["id"],
                    "generation": task["generation"],
                    "status": "succeeded",
                    "started_at": prior.get("started_at"),
                    "finished_at": self.clock(),
                    "commit": commit_id,
                    "result": outputs,
                },
            )
            await self.release(tx, task)
            if prepared.get("more"):
                task["status"] = "queued"
                task["ready_at"] = self.clock()
                await tx.enqueue(task["id"], task["ready_at"])
                await tx.put_task(task)
                await self.advance_run(tx, task["run"])
            else:
                task["status"] = "succeeded"
                await tx.put_task(task)
                await self._finish_task(tx, task, "succeeded", attempt_id)
            return record

    async def _finish_task(self, tx: Tx, task: dict, outcome: str, attempt_id: str | None = None):
        """A terminal transition (§4.2): clear the pending index, record the
        scope outcome, decrement each waiting dependent's counters — enqueue
        it when all deps are done, block it when any dep failed — and roll up
        the run. O(dependents of this task), never O(tasks in run)."""

        await tx.del_pending(task)
        await tx.put_scope_outcome(task["asset"], task["scope"], outcome, attempt_id)
        for dep_id in sorted(await tx.dependents(task["id"])):
            counter = await tx.unfinished(dep_id)
            if counter is None:
                continue
            counter["left"] -= 1
            counter["bad"] += outcome in BAD_OUTCOME
            dep = await tx.task(dep_id)
            if counter["left"] > 0:
                await tx.set_unfinished(dep_id, counter)
                continue
            await tx.set_unfinished(dep_id, None)
            for parent in counter["deps"]:
                await self._remove_dependent(tx, parent, dep_id)
            if dep is None or dep["status"] != "waiting":
                continue
            if counter["bad"]:
                dep["status"] = "blocked"
                await tx.put_task(dep)
                await self._finish_task(tx, dep, "blocked")
            else:
                dep["status"] = "queued"
                dep["ready_at"] = self.clock()
                await tx.enqueue(dep_id, dep["ready_at"])
                await tx.put_task(dep)
        await tx.set_dependents(task["id"], None)
        stats = await tx.run_stats(task["run"])
        if stats is not None:
            stats["left"] -= 1
            stats["bad"] += outcome in BAD_OUTCOME
            await tx.set_run_stats(task["run"], stats)
        await self.advance_run(tx, task["run"])

    async def _remove_dependent(self, tx: Tx, task_id: str, dependent: str):
        ids = await tx.dependents(task_id)
        ids.discard(dependent)
        await tx.set_dependents(task_id, ids)

    async def advance_run(self, tx: Tx, run_id: str):
        """Roll up the run status from the in-memory counters — O(1) per
        completion, never a task/ scan (§4.2)."""

        run = await tx.run(run_id)
        if run is None or run["status"] in TERMINAL_RUN:
            return
        stats = await tx.run_stats(run_id)
        if stats is None:
            # Rebuild for a run re-entering the working set (retry after
            # terminal eviction) — the only O(tasks) path, and it is cold.
            stats = {"left": 0, "bad": 0}
            for tid in run.get("tasks") or ():
                task = await tx.task(tid)
                if task is None:
                    continue
                stats["left"] += task["status"] not in TERMINAL_TASK
                stats["bad"] += task["status"] in BAD_OUTCOME
        run["updated_at"] = self.clock()
        run["status"] = (
            ("succeeded" if not stats["bad"] else "failed")
            if stats["left"] <= 0
            else "running"
        )
        await tx.put_run(run)
        if stats["left"] <= 0:
            await tx.set_run_stats(run_id, None)

    async def fail_attempt(self, attempt_id: str, error: str, *, retryable: bool, delay: float = 0):
        """Mark the running attempt failed; retryable failures requeue by policy."""

        async with self.transaction() as tx:
            try:
                task = await self._owned_task(tx, attempt_id)
            except LostOwnership:
                return
            await tx.put_attempt(
                task["id"],
                task["generation"],
                {
                    "id": attempt_id,
                    "task": task["id"],
                    "generation": task["generation"],
                    "status": "failed",
                    "finished_at": self.clock(),
                    "error": error[-8000:],
                },
            )
            await self.release(tx, task)
            await tx.del_pool_task(attempt_id)
            if retryable and task["attempt_count"] < task["max_attempts"]:
                task["status"] = "queued"
                task["ready_at"] = self.clock() + delay
                await tx.enqueue(task["id"], task["ready_at"])
                await tx.put_task(task)
                await self.advance_run(tx, task["run"])
            else:
                task["status"] = "failed"
                task.pop("error", None)
                await tx.put_task(task)
                await self._finish_task(tx, task, "failed", attempt_id)

    async def skip_attempt(self, attempt_id: str, baseline: dict, watermark_updates=None):
        """§8: every Incremental plan was empty and heads are complete — nothing
        launched. The watermarks still advance over dead deltas (§2.2)."""

        async with self.transaction() as tx:
            try:
                task = await self._owned_task(tx, attempt_id)
            except LostOwnership:
                return
            for edge, update in (watermark_updates or {}).items():
                await tx.put_watermark(task["asset"], edge, task["scope"], update)
            await tx.put_attempt(
                task["id"],
                task["generation"],
                {
                    "id": attempt_id,
                    "task": task["id"],
                    "generation": task["generation"],
                    "status": "skipped",
                    "finished_at": self.clock(),
                    "result": {
                        name: (head or {}).get("ref") for name, head in (baseline or {}).items()
                    },
                },
            )
            await self.release(tx, task)
            await tx.del_pool_task(attempt_id)
            task["status"] = "skipped"
            await tx.put_task(task)
            await self._finish_task(tx, task, "skipped", attempt_id)

    # -- pool tasks and workers (§10) ------------------------------------------

    async def register_worker(self, worker_id: str, pools: list[str], meta: dict) -> dict:
        record = {"id": worker_id, "pools": pools, "meta": meta, "seen_at": self.clock()}
        async with self.transaction() as tx:
            await tx.put_worker(worker_id, record)
        return record

    async def stage_pool_task(self, task: dict, prepared: dict, spec: dict):
        """A queued task placed on Pool(...) becomes claimable pool work (§10).

        The claim lives in memory (§4.1); the durable signal is the task's
        `claimable` status plus the spec object under `specs/`."""

        attempt_id = f"{task['id']}/{task['generation']}"
        placement = spec["execution"]
        record = {
            "attempt": attempt_id,
            "task": task["id"],
            "run": task["run"],
            "asset": task["asset"],
            "scope": task["scope"],
            "pool": placement["environment"]["name"],
            "needs": {
                k: placement["placement"][k]
                for k in ("cpu", "memory", "gpu")
                if placement["placement"].get(k) is not None
            },
            "status": "queued",
            "claimed_by": None,
            "lease_until": None,
            "created_at": self.clock(),
        }
        async with self.transaction() as tx:
            await tx.put_pool_task(attempt_id, record)
            task["status"] = "claimable"
            await tx.put_task(task)
        return record

    async def claim_pool_task(self, worker_id: str, pools: list[str], capacity: dict, lease_seconds: float):
        """Oldest unclaimed (or expired) pool task in the worker's pools that
        fits the worker's cpu/memory/gpu capacity (§10)."""

        async with self.transaction() as tx:
            staged = sorted(
                (rec for _, rec in await tx.pool_tasks()),
                key=lambda rec: rec["created_at"],
            )
            for task in staged:
                if task["status"] != "queued" or task["pool"] not in pools:
                    continue
                if task["lease_until"] is not None and task["lease_until"] > self.clock():
                    continue
                needs = task.get("needs") or {}
                if any(capacity.get(dim) is None or capacity[dim] < want for dim, want in needs.items()):
                    continue
                task["status"] = "claimed"
                task["claimed_by"] = worker_id
                task["claimed_at"] = self.clock()
                task["lease_until"] = self.clock() + lease_seconds
                await tx.put_pool_task(task["attempt"], task)
                return task
        return None

    async def get_pool_task(self, attempt_id: str):
        async with self.transaction() as tx:
            return await tx.pool_task(attempt_id)

    async def get_worker(self, worker_id: str):
        async with self.transaction() as tx:
            return await tx.worker(worker_id)

    async def list_workers(self):
        async with self.transaction() as tx:
            return await tx.workers()

    async def heartbeat_pool_task(self, worker_id: str, attempt_id: str, lease_seconds: float):
        """Pool worker heartbeat — memory only (§4.3)."""

        async with self.transaction() as tx:
            task = await tx.pool_task(attempt_id)
            if (
                task is None
                or task["status"] != "claimed"
                or task["claimed_by"] != worker_id
                or task["lease_until"] <= self.clock()
            ):
                raise LostOwnership(attempt_id)
            task["lease_until"] = self.clock() + lease_seconds
            await tx.put_pool_task(attempt_id, task)
        return task["lease_until"]

    async def release_pool_task(self, worker_id: str, attempt_id: str):
        async with self.transaction() as tx:
            task = await tx.pool_task(attempt_id)
            if task and task["claimed_by"] == worker_id:
                await tx.del_pool_task(attempt_id)

    async def sweep_pool_leases(self):
        """Expired pool claims return to queued so another worker can take them."""

        async with self.transaction() as tx:
            for _, task in await tx.pool_tasks():
                if (
                    task["status"] == "claimed"
                    and task["lease_until"] is not None
                    and task["lease_until"] <= self.clock()
                ):
                    task["status"] = "queued"
                    task["claimed_by"] = None
                    task["lease_until"] = None
                    await tx.put_pool_task(task["attempt"], task)

    async def sweep_scope_leases(self):
        """Attempts whose scope lease expired are fenced and requeued (§8)."""

        async with self.transaction() as tx:
            now = self.clock()
            for (asset, scope), lock in await tx.locks():
                if await tx.lease_until(lock) > now:
                    continue
                await self._expire_lock(tx, asset, scope, lock, now)

    # -- object helpers --------------------------------------------------------

    async def delta(self, output: str, scope: str, batch: int) -> dict | None:
        """One delta object, or None when the batch was never written or is
        already pruned (§2.1)."""

        from cursus.stores import delta_path

        data = await self.get_object(delta_path(output, scope, batch))
        return json.loads(data) if data is not None else None

    async def delta_key_map(self, output: str, scope: str, hi: int) -> dict[str, str]:
        """The live key map of a keyed incremental output at delta `hi`,
        folded from `deltas/{output}/{scope}/…` (§2.1). Deltas are pure
        diffs applied forward; missing objects are treated as
        never-written — a real gap forces a full reset at plan."""

        keys: dict[str, str] = {}
        for b in range(0, int(hi) + 1):
            delta = await self.delta(output, scope, b)
            if delta is None:
                continue
            # Deltas are pure diffs: apply forward, removes cancel upserts. A
            # reset delta's `deleted` already lists every key it dropped.
            for key in delta.get("deleted") or []:
                keys.pop(str(key), None)
            keys.update({str(k): str(v) for k, v in (delta.get("upserted") or {}).items()})
        return keys

    async def put_object(self, key: str, value: bytes):
        await obstore.put_async(self.objects, key, value, mode="overwrite", use_multipart=False)

    async def get_object(self, key: str) -> bytes | None:
        from obstore.exceptions import NotFoundError

        try:
            result = await obstore.get_async(self.objects, key)
        except (NotFoundError, FileNotFoundError):
            return None
        return bytes(await result.bytes_async())

    async def list_objects(self, prefix: str) -> list[str]:
        out = []
        async for batch in obstore.list(self.objects, prefix=prefix):
            out.extend(meta["path"] for meta in batch)
        return sorted(out)

    async def list_object_meta(self, prefix: str) -> list[dict]:
        """(path, last_modified) pairs — retention sweeps need object ages (§5)."""

        out = []
        async for batch in obstore.list(self.objects, prefix=prefix):
            out.extend(dict(meta) for meta in batch)
        return sorted(out, key=lambda m: m["path"])

    async def delete_objects(self, keys: list[str]):
        if keys:
            await obstore.delete_async(self.objects, keys)

    @property
    def poisoned(self):
        return self.store.poisoned

    async def close(self):
        await self.store.close()


class _TxCtx:
    def __init__(self, state: State):
        self.state = state
        self._cm = None
        self._tx = None

    async def __aenter__(self) -> Tx:
        await self.state._ensure_loaded()
        self._cm = self.state.store.transaction()
        inner = await self._cm.__aenter__()
        self._tx = Tx(inner, self.state)
        inner.post_commit = lambda: self.state._apply(self._tx._ov)
        return self._tx

    async def __aexit__(self, *exc):
        return await self._cm.__aexit__(*exc)
