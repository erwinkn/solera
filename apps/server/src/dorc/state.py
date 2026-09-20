"""Domain state over SlateDB (§1): heads, cursors, key state, commits,
automations, runs/tasks/attempts with per-scope fencing, workers and pools.

Key layout (all under the SlateDB namespace):

    sys/schema, sys/revision, sys/project
    manifest/{revision}
    head/{output}/{scope}         -> committed ref + metadata
    cursor/{asset}/{scope}        -> producer cursor (§6)
    keystate/{asset}/{edge}/{scope}/{key} -> {r: revision, f: fingerprint}
    commit/{id}                   -> commit record
    automation/{name}             -> {enabled, next_at, last_at, last_run, pending}
    run/{id}, runindex/{rev-ts}/{id}
    task/{id}                     -> one per (asset, scope) of a run
    attempt/{task}/{generation}   -> one per execution of a task
    active/{attempt}              -> in-flight attempt with its run handle
    lock/{asset}/{scope}          -> scope fencing {attempt, generation, lease_until}
    queue/{ready_ms}/{task}       -> dispatch index
    pool/{attempt}                -> claimable pool task (§10)
    worker/{id}                   -> registered external worker
"""

from __future__ import annotations

import hashlib
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


class Tx:
    """Domain operations inside one SlateDB transaction."""

    def __init__(self, inner: Transaction, clock):
        self.t = inner
        self.clock = clock

    get = property(lambda self: self.t.get)
    put = property(lambda self: self.t.put)
    delete = property(lambda self: self.t.delete)
    scan = property(lambda self: self.t.scan)

    # -- heads ---------------------------------------------------------------
    async def head(self, output: str, scope: str):
        return await self.t.get(f"head/{esc(output)}/{esc(scope)}")

    async def put_head(self, output: str, scope: str, record: dict):
        await self.t.put(f"head/{esc(output)}/{esc(scope)}", record)

    async def heads(self, output: str):
        return [(unesc(k).rsplit("/", 1)[-1], v) for k, v in await self.t.scan(f"head/{esc(output)}/")]

    async def all_heads(self):
        return await self.t.scan("head/")

    # -- cursors ---------------------------------------------------------------
    async def cursor(self, asset: str, scope: str):
        return await self.t.get(f"cursor/{esc(asset)}/{esc(scope)}")

    async def put_cursor(self, asset: str, scope: str, value):
        if value is None:
            await self.t.delete(f"cursor/{esc(asset)}/{esc(scope)}")
        else:
            await self.t.put(f"cursor/{esc(asset)}/{esc(scope)}", value)

    # -- per-edge key state -----------------------------------------------------
    async def key_state(self, asset: str, edge: str, scope: str) -> dict[str, dict]:
        prefix = f"keystate/{esc(asset)}/{esc(edge)}/{esc(scope)}/"
        return {unesc(k[len(prefix) :]): v for k, v in await self.t.scan(prefix)}

    async def put_key_state(self, asset, edge, scope, key, record):
        await self.t.put(f"keystate/{esc(asset)}/{esc(edge)}/{esc(scope)}/{esc(key)}", record)

    async def del_key_state(self, asset, edge, scope, key):
        await self.t.delete(f"keystate/{esc(asset)}/{esc(edge)}/{esc(scope)}/{esc(key)}")

    async def clear_key_state(self, asset, edge, scope):
        for key, _ in await self.t.scan(f"keystate/{esc(asset)}/{esc(edge)}/{esc(scope)}/"):
            await self.t.delete(key)

    # -- commits -----------------------------------------------------------------
    async def commit_record(self, commit_id: str):
        return await self.t.get(f"commit/{commit_id}")

    async def put_commit(self, commit_id: str, record: dict):
        await self.t.put(f"commit/{commit_id}", record)

    async def commits(self, limit=100):
        return await self.t.scan("commit/", limit)

    # -- automations ---------------------------------------------------------------
    async def automation(self, name: str):
        return await self.t.get(f"automation/{esc(name)}")

    async def put_automation(self, name: str, record: dict):
        await self.t.put(f"automation/{esc(name)}", record)

    async def automations(self):
        return await self.t.scan("automation/")

    # -- runs ----------------------------------------------------------------------
    async def run(self, run_id: str):
        return await self.t.get(f"run/{esc(run_id)}")

    async def put_run(self, run: dict):
        await self.t.put(f"run/{esc(run['id'])}", run)

    async def runs(self, limit=50):
        rows = await self.t.scan("runindex/", limit)
        return [await self.run(v) for _, v in rows]

    async def index_run(self, run: dict):
        ts = int(run["created_at"] * 1000)
        await self.t.put(f"runindex/{9999999999999999 - ts:016d}/{run['id']}", run["id"])

    # -- tasks ---------------------------------------------------------------------
    async def task(self, task_id: str):
        return await self.t.get(f"task/{esc(task_id)}")

    async def put_task(self, task: dict):
        await self.t.put(f"task/{esc(task['id'])}", task)

    # -- attempts --------------------------------------------------------------------
    async def attempt(self, task_id: str, generation: int):
        return await self.t.get(f"attempt/{esc(task_id)}/{generation:010d}")

    async def put_attempt(self, task_id: str, generation: int, record: dict):
        await self.t.put(f"attempt/{esc(task_id)}/{generation:010d}", record)

    async def attempts(self, task_id: str):
        return await self.t.scan(f"attempt/{esc(task_id)}/")

    async def active(self, attempt_id: str):
        return await self.t.get(f"active/{esc(attempt_id)}")

    async def put_active(self, attempt_id: str, record: dict):
        await self.t.put(f"active/{esc(attempt_id)}", record)

    async def del_active(self, attempt_id: str):
        await self.t.delete(f"active/{esc(attempt_id)}")

    async def actives(self):
        return await self.t.scan("active/")

    # -- scope fencing -----------------------------------------------------------------
    async def lock(self, asset: str, scope: str):
        return await self.t.get(f"lock/{esc(asset)}/{esc(scope)}")

    async def put_lock(self, asset: str, scope: str, record: dict):
        await self.t.put(f"lock/{esc(asset)}/{esc(scope)}", record)

    async def del_lock(self, asset: str, scope: str):
        await self.t.delete(f"lock/{esc(asset)}/{esc(scope)}")

    # -- dispatch queue --------------------------------------------------------------------
    async def enqueue(self, task_id: str, ready_at: float):
        await self.t.put(f"queue/{int(ready_at * 1000):020d}/{esc(task_id)}", task_id)

    async def dequeue_key(self, task_id: str, ready_at: float):
        await self.t.delete(f"queue/{int(ready_at * 1000):020d}/{esc(task_id)}")

    async def queued(self):
        return await self.t.scan("queue/")

    # -- pool tasks + workers ------------------------------------------------------------------
    async def pool_task(self, attempt_id: str):
        return await self.t.get(f"pool/{esc(attempt_id)}")

    async def put_pool_task(self, attempt_id: str, record: dict):
        await self.t.put(f"pool/{esc(attempt_id)}", record)

    async def del_pool_task(self, attempt_id: str):
        await self.t.delete(f"pool/{esc(attempt_id)}")

    async def pool_tasks(self):
        return await self.t.scan("pool/")

    async def worker(self, worker_id: str):
        return await self.t.get(f"worker/{esc(worker_id)}")

    async def put_worker(self, worker_id: str, record: dict):
        await self.t.put(f"worker/{esc(worker_id)}", record)

    async def workers(self):
        return await self.t.scan("worker/")


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

    def transaction(self) -> Tx:
        return _TxCtx(self)

    async def get(self, key: str, default=None):
        async with self.transaction() as tx:
            return await tx.get(key, default)

    async def scan(self, prefix: str, limit=None):
        async with self.transaction() as tx:
            return await tx.scan(prefix, limit)

    # -- project registration ---------------------------------------------------

    async def initialize(self, manifest: dict, revision: str):
        """Install the project manifest and seed automation state (§11, §9).

        Automation toggles and pending events survive a re-registration when the
        trigger is unchanged; a changed trigger resets the pending queue."""

        async with self.transaction() as tx:
            await tx.put(f"manifest/{esc(revision)}", manifest)
            await tx.put("sys/revision", revision)
            await tx.put("sys/project", manifest.get("project") or manifest.get("name"))
            for auto in manifest["automations"].values():
                existing = await tx.automation(auto["name"])
                record = {
                    **auto,
                    "last_at": None,
                    "last_run": None,
                    "pending": [],
                }
                if existing is not None:
                    record["enabled"] = existing["enabled"]
                    if existing["trigger"] == auto["trigger"]:
                        record["last_at"] = existing.get("last_at")
                        record["last_run"] = existing.get("last_run")
                        record["pending"] = existing.get("pending", [])
                await tx.put_automation(auto["name"], record)

    async def manifest(self) -> dict:
        async with self.transaction() as tx:
            revision = await tx.get("sys/revision")
            if revision is None:
                raise Conflict("no project registered")
            manifest = await tx.get(f"manifest/{esc(revision)}")
            manifest["revision"] = revision
            return manifest

    # -- fencing (§8) ---------------------------------------------------------

    async def claim(self, tx: Tx, task: dict, lease_seconds: float) -> dict:
        """Take the per-scope lock under a generation; returns the attempt record.

        An expired lease lets a new claimant take over: the old attempt is
        fenced and any commit it tries fails with LostOwnership (§8)."""

        lock = await tx.lock(task["asset"], task["scope"])
        if lock is not None:
            if lock["lease_until"] > self.clock():
                raise Conflict(f"scope {task['scope']} is already claimed")
            old_task_id, _, old_generation = lock["attempt"].rpartition("/")
            old_task = await tx.task(old_task_id)
            old_attempt = await tx.attempt(old_task_id, int(old_generation))
            if old_attempt and old_attempt["status"] in {"claimed", "running"}:
                old_attempt["status"] = "expired"
                old_attempt["finished_at"] = self.clock()
                await tx.put_attempt(old_task_id, int(old_generation), old_attempt)
            if old_task_id != task["id"] and old_task and old_task["status"] == "running":
                old_task["status"] = "queued"
                old_task["ready_at"] = self.clock()
                await tx.enqueue(old_task["id"], old_task["ready_at"])
                await tx.put_task(old_task)
        task["generation"] += 1
        task["attempt_count"] += 1
        attempt_id = f"{task['id']}/{task['generation']}"
        lease_until = self.clock() + lease_seconds
        await tx.put_lock(
            task["asset"],
            task["scope"],
            {"attempt": attempt_id, "generation": task["generation"], "lease_until": lease_until},
        )
        await tx.put_attempt(
            task["id"],
            task["generation"],
            {
                "id": attempt_id,
                "task": task["id"],
                "generation": task["generation"],
                "status": "claimed",
                "started_at": self.clock(),
                "lease_until": lease_until,
            },
        )
        return {"id": attempt_id, "generation": task["generation"], "lease_until": lease_until}

    async def renew(self, tx: Tx, attempt_id: str, lease_seconds: float):
        """Extend the attempt's lease; raises LostOwnership when fenced (§8, §10)."""

        info = await self._lock_of(tx, attempt_id)
        if info is None:
            raise LostOwnership(attempt_id)
        asset, scope = info
        lock = await tx.lock(asset, scope)
        if lock is None or lock["attempt"] != attempt_id:
            raise LostOwnership(attempt_id)
        lock["lease_until"] = self.clock() + lease_seconds
        await tx.put_lock(asset, scope, lock)
        task_id, _, generation = attempt_id.rpartition("/")
        attempt = await tx.attempt(task_id, int(generation))
        if attempt:
            attempt["lease_until"] = lock["lease_until"]
            await tx.put_attempt(task_id, int(generation), attempt)
        return lock["lease_until"]

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

    async def owned(self, tx: Tx, task: dict) -> dict:
        """The attempt's lock record, or raise LostOwnership (fencing, §8)."""

        lock = await tx.lock(task["asset"], task["scope"])
        attempt_id = f"{task['id']}/{task['generation']}"
        if lock is None or lock["attempt"] != attempt_id or lock["lease_until"] <= self.clock():
            raise LostOwnership(attempt_id)
        return lock

    # -- the atomic commit (§8) -------------------------------------------------

    async def _owned_task(self, tx: Tx, attempt_id: str):
        """The task behind an attempt, verified to still own its scope lease."""

        task_id, _, _ = attempt_id.rpartition("/")
        task = await tx.task(task_id)
        if task is None or task["status"] != "running":
            raise LostOwnership(attempt_id)
        lock = await tx.lock(task["asset"], task["scope"])
        if lock is None or lock["attempt"] != attempt_id or lock["lease_until"] <= self.clock():
            raise LostOwnership(attempt_id)
        return task

    async def commit_attempt(self, manifest, attempt_id: str, prepared: dict, result: dict) -> dict:
        """Install an attempt's result: heads, input_refs, cursor, key state,
        `changed`, pending OnChange automations — all in one transaction (§8)."""

        import uuid

        from data_orchestrator.sdk import UNSET

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
                if (
                    decl.get("key") is not None or decl.get("partition_set") or decl.get("mode") == "append"
                ) and not (ref.get("meta") or {}).get("keys"):
                    raise Conflict(f"keyed output {name}: ref carries no key map", retryable=False)
            for name in set(declared) - set(outputs):
                if prepared["baseline"].get(name) is None:
                    raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)

            changed = sorted(
                name
                for name, ref in outputs.items()
                if prepared["baseline"].get(name) is None
                or prepared["baseline"][name]["ref"]["version"] != ref["version"]
            )
            commit_id = uuid.uuid4().hex
            record = {
                "id": commit_id,
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
            await tx.put_commit(commit_id, record)
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
            elif prepared.get("recompute"):
                # Recompute withholds the cursor from the producer and clears the
                # committed one (§8: no prior, no cursor, key state cleared).
                await tx.put_cursor(task["asset"], task["scope"], None)

            # Per-edge key state: committed revisions + fingerprints (§6).
            for edge, update in (prepared.get("key_updates") or {}).items():
                if update.get("clear"):
                    await tx.clear_key_state(task["asset"], edge, task["scope"])
                for key in update.get("deleted", []):
                    await tx.del_key_state(task["asset"], edge, task["scope"], key)
                for key, value in update.get("upserted", {}).items():
                    await tx.put_key_state(task["asset"], edge, task["scope"], key, value)

            # OnChange automations pend in the same transaction (§9).
            if changed:
                for _, auto in await tx.automations():
                    watched = set(auto.get("watched") or auto["trigger"].get("outputs") or [])
                    if auto["enabled"] and auto["trigger"]["kind"] == "onchange" and watched & set(changed):
                        auto.setdefault("pending", []).append(
                            {
                                "commit": commit_id,
                                "asset": task["asset"],
                                "scope": task["scope"],
                                "outputs": sorted(watched & set(changed)),
                            }
                        )
                        await tx.put_automation(auto["name"], auto)

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
                },
            )
            await self.release(tx, task)
            await tx.del_active(attempt_id)
            if prepared.get("more"):
                task["status"] = "queued"
                task["ready_at"] = self.clock()
                await tx.enqueue(task["id"], task["ready_at"])
                await tx.put_task(task)
            else:
                task["status"] = "succeeded"
                task["result"] = outputs
                task["error"] = None
                await tx.put_task(task)
            await self.advance_run(tx, task["run"])
            return record

    async def advance_run(self, tx: Tx, run_id: str):
        """Unblock dependent tasks and roll up the run status (§8)."""

        run = await tx.run(run_id)
        if run is None or run["status"] == "canceled":
            return
        tasks = {tid: await tx.task(tid) for tid in run["tasks"]}
        done = {"succeeded", "skipped"}
        for task in tasks.values():
            if task["status"] != "waiting":
                continue
            dep_status = [tasks[d]["status"] for d in task["deps"]]
            if any(s in {"failed", "blocked", "canceled"} for s in dep_status):
                task["status"] = "blocked"
                await tx.put_task(task)
            elif all(s in done for s in dep_status):
                task["status"] = "queued"
                task["ready_at"] = self.clock()
                await tx.enqueue(task["id"], task["ready_at"])
                await tx.put_task(task)
        statuses = [t["status"] for t in tasks.values()]
        if all(s in done | {"failed", "blocked", "canceled"} for s in statuses):
            run["status"] = "succeeded" if all(s in done for s in statuses) else "failed"
        else:
            run["status"] = "running"
        run["updated_at"] = self.clock()
        await tx.put_run(run)

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
            await tx.del_active(attempt_id)
            task["error"] = error[-8000:]
            if retryable and task["attempt_count"] < task["max_attempts"]:
                task["status"] = "queued"
                task["ready_at"] = self.clock() + delay
                await tx.enqueue(task["id"], task["ready_at"])
            else:
                task["status"] = "failed"
            await tx.put_task(task)
            await self.advance_run(tx, task["run"])

    async def skip_attempt(self, attempt_id: str, baseline: dict):
        """§8: every ByKey diff was empty and heads are complete — nothing launched."""

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
                    "status": "skipped",
                    "finished_at": self.clock(),
                },
            )
            await self.release(tx, task)
            await tx.del_active(attempt_id)
            task["status"] = "skipped"
            task["result"] = {name: (head or {}).get("ref") for name, head in (baseline or {}).items()}
            await tx.put_task(task)
            await self.advance_run(tx, task["run"])

    # -- pool tasks and workers (§10) ------------------------------------------

    async def register_worker(self, worker_id: str, pools: list[str], meta: dict) -> dict:
        record = {"id": worker_id, "pools": pools, "meta": meta, "seen_at": self.clock()}
        async with self.transaction() as tx:
            await tx.put_worker(worker_id, record)
        return record

    async def stage_pool_task(self, task: dict, prepared: dict, spec: dict):
        """A queued task placed on Pool(...) becomes claimable pool work (§10)."""

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
            "spec": spec,
            "prepared": prepared,
            "status": "queued",
            "claimed_by": None,
            "lease_until": None,
            "created_at": self.clock(),
        }
        async with self.transaction() as tx:
            await tx.put_pool_task(attempt_id, record)
        return record

    async def claim_pool_task(self, worker_id: str, pools: list[str], capacity: dict, lease_seconds: float):
        """Oldest unclaimed (or expired) pool task in the worker's pools that
        fits the worker's cpu/memory/gpu capacity (§10)."""

        async with self.transaction() as tx:
            for _, task in await tx.pool_tasks():
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
                if task["status"] == "claimed" and task["lease_until"] <= self.clock():
                    task["status"] = "queued"
                    task["claimed_by"] = None
                    task["lease_until"] = None
                    await tx.put_pool_task(task["attempt"], task)

    async def sweep_scope_leases(self):
        """Attempts whose scope lease expired are fenced and requeued (§8)."""

        async with self.transaction() as tx:
            now = self.clock()
            for key, lock in await tx.scan("lock/"):
                if lock["lease_until"] > now:
                    continue
                _, asset_e, scope_e = key.split("/", 2)
                asset, scope = unesc(asset_e), unesc(scope_e)
                task_id, _, generation = lock["attempt"].rpartition("/")
                task = await tx.task(task_id)
                attempt = await tx.attempt(task_id, int(generation))
                await tx.del_lock(asset, scope)
                if attempt and attempt["status"] in {"claimed", "running"}:
                    attempt["status"] = "expired"
                    attempt["finished_at"] = now
                    await tx.put_attempt(task_id, int(generation), attempt)
                await tx.del_active(lock["attempt"])
                if task and task["status"] == "running":
                    task["status"] = "queued"
                    task["ready_at"] = now
                    await tx.enqueue(task["id"], task["ready_at"])
                    await tx.put_task(task)

    # -- object helpers --------------------------------------------------------

    async def stage_key_map(self, keys: dict) -> dict:
        """Key maps live as objects: `keys/{sha256}.json` (§6)."""

        body = json.dumps(keys, sort_keys=True, allow_nan=False).encode()
        sha = hashlib.sha256(body).hexdigest()
        await obstore.put_async(self.objects, f"keys/{sha}.json", body, mode="overwrite", use_multipart=False)
        return {"object": f"keys/{sha}.json", "count": len(keys)}

    async def fetch_key_map(self, info: dict | None) -> dict | None:
        if not info:
            return None
        from obstore.exceptions import NotFoundError

        try:
            result = await obstore.get_async(self.objects, info["object"])
        except (NotFoundError, FileNotFoundError):
            return None
        return json.loads(bytes(await result.bytes_async()))

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

    @property
    def poisoned(self):
        return self.store.poisoned

    async def close(self):
        await self.store.close()


class _TxCtx:
    def __init__(self, state: State):
        self.state = state
        self._cm = None

    async def __aenter__(self) -> Tx:
        self._cm = self.state.store.transaction()
        inner = await self._cm.__aenter__()
        return Tx(inner, self.state.clock)

    async def __aexit__(self, *exc):
        return await self._cm.__aexit__(*exc)
