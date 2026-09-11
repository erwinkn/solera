from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import time
import uuid
from urllib.parse import quote

from .sdk import digest
from .storage import Unavailable

SUCCESS = {"succeeded", "skipped"}
TERMINAL = SUCCESS | {"failed", "blocked", "canceled"}


class Conflict(ValueError):
    pass


class LostOwnership(Conflict):
    pass


def esc(value):
    # Empty strings must not collide with a real item key such as '_'.
    return quote(str(value), safe="")


def scope(producer, partition):
    return f"{esc(producer)}/{esc(partition)}"


def head_key(asset, partition):
    return f"head/{scope(asset, partition)}"


class Engine:
    def __init__(
        self, state, manifest, backend, *, lease_seconds=60, concurrency=4, clock=time.time, retry_delay=1
    ):
        if lease_seconds < 1 or concurrency < 1:
            raise ValueError("Lease duration and concurrency must be positive")
        self.state, self.manifest, self.backend = state, manifest, backend
        self.lease_seconds, self.concurrency = lease_seconds, concurrency
        self.clock, self.retry_delay = clock, retry_delay
        self.runner = None
        self.executions = set()
        self.last_error = None

    async def initialize(self):
        async with self.state.transaction() as tx:
            await tx.put(f"definition/{self.manifest['revision']}", self.manifest)
            await tx.put("system/current", self.manifest["revision"])
            names = {a["name"] for a in self.manifest["automations"]}
            for key, old in await tx.scan("automation/"):
                if old["name"] not in names:
                    await tx.delete(key)
            for automation in self.manifest["automations"]:
                key = f"automation/{esc(automation['name'])}"
                previous = await tx.get(key)
                record = dict(automation)
                record.update(
                    {
                        "enabled": previous["enabled"] if previous else automation["enabled"],
                        "next_at": previous["next_at"]
                        if previous
                        else self.clock() + automation["every_seconds"],
                    }
                )
                await tx.put(key, record)
            # Opening a new native writer fences the previous one. Its active
            # attempt generations must still be revoked in our domain state.
            affected = set()
            for _, task_id in await tx.scan("active/"):
                task = await tx.get(f"task/{task_id}")
                if task and task["status"] == "running":
                    await self._release(tx, task)
                    await tx.put(
                        f"attempt/{task_id}/{task['generation']:010d}",
                        {
                            "generation": task["generation"],
                            "status": "abandoned",
                            "error": "Coordinator replaced",
                        },
                    )
                    task["generation"] += 1
                    if task["attempt_count"] >= task["max_attempts"]:
                        task.update(
                            status="failed", error="Repeated coordinator interruptions; retry manually"
                        )
                        await tx.put(f"task/{task_id}", task)
                    else:
                        await self._queue(tx, task)
                    affected.add(task["run_id"])
                    await self._event(
                        tx, task["run_id"], "recovered", "Recovered an interrupted attempt", task_id
                    )
            for run_id in affected:
                await self._advance(tx, run_id)

    def _plan(self, targets, partitions):
        if not targets or len(set(targets)) != len(targets):
            raise ValueError("Select at least one unique asset")
        owners, producers = self.manifest["owners"], self.manifest["producers"]
        if any(a not in owners for a in targets):
            raise ValueError("Unknown target asset")
        needs_partitions = any(producers[owners[a]]["partitions"] for a in targets)
        if needs_partitions and not partitions:
            raise ValueError("Daily assets require explicit YYYY-MM-DD partition keys")
        if partitions and not needs_partitions:
            raise ValueError("Partition keys only apply to partitioned targets")
        if len(partitions) > 1000 or len(set(partitions)) != len(partitions):
            raise ValueError("A backfill accepts at most 1,000 unique partitions")
        for p in partitions:
            if dt.date.fromisoformat(p).isoformat() != p:
                raise ValueError("Partition keys must use YYYY-MM-DD")
        planned = {}

        def visit(name, partition):
            producer = producers[name]
            partition = partition if producer["partitions"] else ""
            identity = scope(name, partition)
            if identity in planned:
                return identity
            deps = {}
            for arg, asset in producer["inputs"].items():
                parent = producers[owners[asset]]
                parent_partition = partition if parent["partitions"] else ""
                dep = visit(parent["name"], parent_partition)
                deps[arg] = {"identity": dep, "asset": asset, "partition": parent_partition}
            planned[identity] = {"identity": identity, "producer": name, "partition": partition, "deps": deps}
            return identity

        for asset in targets:
            for partition in partitions or [""]:
                visit(owners[asset], partition)
        if len(planned) > 5000:
            raise ValueError("Request exceeds the alpha's 5,000-task limit")
        return list(planned.values())

    async def submit(
        self, targets, *, partitions=None, mode="incremental", config=None, command_id=None, cause="manual"
    ):
        partitions, config = partitions or [], config or {}
        plan = self._plan(targets, partitions)
        if mode not in {"incremental", "recompute", "fill_missing"}:
            raise ValueError("Unknown materialization mode")
        command_id = command_id or str(uuid.uuid4())
        if not isinstance(command_id, str) or not 1 <= len(command_id) <= 200:
            raise ValueError("Invalid idempotency key")
        body = {
            "targets": targets,
            "partitions": partitions,
            "mode": mode,
            "config": config,
            "revision": self.manifest["revision"],
        }
        async with self.state.transaction() as tx:
            return await self._submit(tx, plan, body, command_id, cause)

    async def _submit(self, tx, plan, body, command_id, cause):
        key = f"command/{esc(command_id)}"
        previous = await tx.get(key)
        fingerprint = digest(body)
        if previous:
            if previous["fingerprint"] != fingerprint:
                raise Conflict("Idempotency key was already used for a different request")
            return await tx.get(f"run/{previous['id']}")
        run_id, now = str(uuid.uuid4()), self.clock()
        ids = {p["identity"]: str(uuid.uuid5(uuid.UUID(run_id), p["identity"])) for p in plan}
        run = {
            "id": run_id,
            **body,
            "cause": cause,
            "created_at": now,
            "updated_at": now,
            "status": "queued",
            "paused": False,
            "tasks": list(ids.values()),
        }
        for p in plan:
            task = {
                "id": ids[p["identity"]],
                "run_id": run_id,
                "producer": p["producer"],
                "partition": p["partition"],
                "scope": p["identity"],
                "deps": {arg: {**dep, "task": ids[dep["identity"]]} for arg, dep in p["deps"].items()},
                "status": "waiting",
                "generation": 0,
                "attempt_count": 0,
                "max_attempts": 3,
                "result": {},
                "revision": body["revision"],
                "mode": body["mode"],
            }
            if not task["deps"]:
                await self._queue(tx, task)
            else:
                await tx.put(f"task/{task['id']}", task)
        await tx.put(f"run/{run_id}", run)
        await tx.put(f"runs/{9999999999999999 - int(now * 1000):016d}/{run_id}", run_id)
        await tx.put(key, {"id": run_id, "fingerprint": fingerprint})
        await self._event(tx, run_id, "requested", f"Requested {len(plan)} tasks ({cause})")
        return run

    async def _event(self, tx, run_id, kind, message, task_id=None, data=None):
        event_id = f"{int(self.clock() * 1000000):020d}-{uuid.uuid4().hex}"
        await tx.put(
            f"event/{run_id}/{event_id}",
            {
                "id": event_id,
                "at": self.clock(),
                "kind": kind,
                "message": message,
                "task_id": task_id,
                "data": data,
            },
        )

    async def _queue(self, tx, task, delay=0):
        task["status"] = "queued"
        task["queue_key"] = f"ready/{int((self.clock() + delay) * 1000):020d}/{task['id']}"
        task["ready_at"] = self.clock() + delay
        await tx.put(task["queue_key"], task["id"])
        await tx.put(f"task/{task['id']}", task)

    async def _release(self, tx, task):
        owner = await tx.get(f"scope/{task['scope']}")
        if owner and owner["task"] == task["id"] and owner["generation"] == task["generation"]:
            await tx.delete(f"scope/{task['scope']}")
        await tx.delete(f"active/{task['id']}")
        if task.get("queue_key"):
            await tx.delete(task["queue_key"])

    async def _advance(self, tx, run_id):
        run = await tx.get(f"run/{run_id}")
        if run["status"] == "canceled":
            return
        tasks = {task_id: await tx.get(f"task/{task_id}") for task_id in run["tasks"]}
        for task in tasks.values():
            if task["status"] != "waiting":
                continue
            deps = [tasks[d["task"]]["status"] for d in task["deps"].values()]
            if any(s in {"failed", "blocked", "canceled"} for s in deps):
                task["status"] = "blocked"
                await tx.put(f"task/{task['id']}", task)
            elif all(s in SUCCESS for s in deps):
                await self._queue(tx, task)
        statuses = [t["status"] for t in tasks.values()]
        if all(s in TERMINAL for s in statuses):
            run["status"] = "succeeded" if all(s in SUCCESS for s in statuses) else "failed"
        else:
            run["status"] = "paused" if run.get("paused") else "running"
        run["updated_at"] = self.clock()
        await tx.put(f"run/{run_id}", run)

    async def claim(self):
        async with self.state.transaction() as tx:
            for queue_key, task_id in await tx.scan("ready/"):
                task = await tx.get(f"task/{task_id}")
                if not task or task["status"] != "queued":
                    await tx.delete(queue_key)
                    continue
                if task["ready_at"] > self.clock():
                    break
                run = await tx.get(f"run/{task['run_id']}")
                if run.get("paused") or await tx.get(f"scope/{task['scope']}"):
                    continue
                if task["revision"] != self.manifest["revision"]:
                    task["status"], task["error"] = "failed", "Project revision changed; submit a new request"
                    await tx.delete(queue_key)
                    await tx.put(f"task/{task_id}", task)
                    await self._advance(tx, task["run_id"])
                    continue
                producer = self.manifest["producers"][task["producer"]]
                baseline = {a: await tx.get(head_key(a, task["partition"])) for a in producer["outputs"]}
                reusable = all(
                    h and h.get("scope_complete") and h.get("state_version") == producer["version"]
                    for h in baseline.values()
                )
                if task["mode"] == "fill_missing" and reusable:
                    task["status"], task["result"] = "skipped", {a: h["ref"] for a, h in baseline.items()}
                    await tx.delete(queue_key)
                    await tx.put(f"task/{task_id}", task)
                    await self._advance(tx, task["run_id"])
                    continue
                inputs = {}
                for arg, dep in task["deps"].items():
                    upstream = await tx.get(f"task/{dep['task']}")
                    if upstream["status"] not in SUCCESS:
                        raise RuntimeError("Ready index references an unsatisfied dependency")
                    inputs[arg] = {**dep, "ref": upstream["result"][dep["asset"]]}
                task.update(
                    {
                        "status": "running",
                        "generation": task["generation"] + 1,
                        "attempt_count": task["attempt_count"] + 1,
                        "input_refs": inputs,
                        "baseline": baseline,
                        "lease_until": self.clock() + self.lease_seconds,
                        "started_at": self.clock(),
                    }
                )
                await tx.delete(queue_key)
                await tx.put(f"task/{task_id}", task)
                await tx.put(f"active/{task_id}", task_id)
                await tx.put(f"scope/{task['scope']}", {"task": task_id, "generation": task["generation"]})
                await tx.put(
                    f"attempt/{task_id}/{task['generation']:010d}",
                    {"generation": task["generation"], "status": "running", "started_at": self.clock()},
                )
                await self._advance(tx, task["run_id"])
                return task
        return None

    async def _owned(self, tx, claim):
        task = await tx.get(f"task/{claim['id']}")
        owner = await tx.get(f"scope/{claim['scope']}")
        if (
            not task
            or task["status"] != "running"
            or task["generation"] != claim["generation"]
            or task["lease_until"] <= self.clock()
            or owner != {"task": claim["id"], "generation": claim["generation"]}
        ):
            raise LostOwnership("Attempt no longer owns its publication scope")
        return task

    async def _validate_inputs(self, tx, claim):
        for info in claim["input_refs"].values():
            current = await tx.get(head_key(info["asset"], info["partition"]))
            if not current or current["ref"] != info["ref"]:
                raise Conflict("Upstream data advanced after inputs were pinned; submit a fresh request")

    async def renew(self, claim):
        async with self.state.transaction() as tx:
            task = await self._owned(tx, claim)
            task["lease_until"] = self.clock() + self.lease_seconds
            await tx.put(f"task/{task['id']}", task)

    async def recover_expired(self):
        async with self.state.transaction() as tx:
            for _, task_id in await tx.scan("active/"):
                task = await tx.get(f"task/{task_id}")
                if task["status"] == "running" and task["lease_until"] <= self.clock():
                    await self._release(tx, task)
                    await tx.put(
                        f"attempt/{task_id}/{task['generation']:010d}",
                        {"generation": task["generation"], "status": "abandoned", "error": "Lease expired"},
                    )
                    task["generation"] += 1
                    if task["attempt_count"] >= task["max_attempts"]:
                        task.update(status="failed", error="Lease repeatedly expired; retry manually")
                        await tx.put(f"task/{task_id}", task)
                    else:
                        await self._queue(tx, task)
                    await self._event(
                        tx,
                        task["run_id"],
                        "lease_expired",
                        "Attempt expired; publication rights revoked",
                        task_id,
                    )
                    await self._advance(tx, task["run_id"])

    async def prepare(self, claim):
        producer = self.manifest["producers"][claim["producer"]]
        values = {arg: await self.state.load(info["ref"]) for arg, info in claim["input_refs"].items()}
        async with self.state.transaction() as tx:
            await self._owned(tx, claim)
            await self._validate_inputs(tx, claim)
            checkpoint = await tx.get(
                f"checkpoint/{claim['scope']}",
                {"generation": 0, "cursor": None, "state_version": producer["version"]},
            )
            if checkpoint["state_version"] != producer["version"] and claim["mode"] != "recompute":
                raise Conflict("Incremental state version changed; a recompute is required")
            items = dict(await tx.scan(f"item/{claim['scope']}/")) if producer["incremental"] else {}
            run = await tx.get(f"run/{claim['run_id']}")
        changes = {"upserted_keys": [], "deleted_keys": []}
        revisions, more = {}, False
        by_key = producer["incremental"]
        interpretation = digest(
            [
                producer["code_hash"],
                producer["version"],
                run["config"],
                {k: v["ref"] for k, v in claim["input_refs"].items() if not by_key or k != by_key["input"]},
            ]
        )
        if by_key:
            rows = values[by_key["input"]]
            if not isinstance(rows, list):
                raise ValueError("A keyed inventory must be a JSON list")
            current = {}
            for row in rows:
                key = str(row[by_key["key"]])
                if key in current:
                    raise ValueError(f"Duplicate inventory key: {key}")
                current[key] = row[by_key["revision"]]
            previous = {v["key"]: v for v in items.values()}
            complete = claim["input_refs"][by_key["input"]]["ref"]["complete"]
            if claim["mode"] == "recompute" and not complete:
                raise Conflict("Cannot rebuild an asset from an incomplete inventory")
            upserts = sorted(
                k
                for k, v in current.items()
                if claim["mode"] == "recompute"
                or k not in previous
                or previous[k]["revision"] != v
                or previous[k].get("interpretation") != interpretation
            )
            deletes = sorted(previous.keys() - current.keys()) if complete else []
            work = [(k, False) for k in upserts] + [(k, True) for k in deletes]
            # A rebuild replaces the full scope once. Normal incremental work is
            # bounded and independently committed; prior batches survive failure.
            selected = work if claim["mode"] == "recompute" else work[: by_key["batch_size"]]
            more = len(selected) < len(work)
            for key, deleted in selected:
                changes["deleted_keys" if deleted else "upserted_keys"].append(key)
                if not deleted:
                    revisions[key] = current[key]
        spec = {
            "revision": claim["revision"],
            "producer": claim["producer"],
            "inputs": values,
            "context": {
                "partition": claim["partition"],
                "cursor": None if claim["mode"] == "recompute" else checkpoint["cursor"],
                "changes": changes,
                "run_id": claim["run_id"],
                "config": run["config"],
            },
        }
        no_changes = bool(
            by_key
            and claim["mode"] != "recompute"
            and not changes["upserted_keys"]
            and not changes["deleted_keys"]
            and all(h and h.get("scope_complete") for h in claim["baseline"].values())
        )
        return {
            "spec": spec,
            "checkpoint": checkpoint,
            "changes": changes,
            "revisions": revisions,
            "interpretation": interpretation,
            "more": more,
            "skip": no_changes,
        }

    async def stage_result(self, claim, result):
        producer = self.manifest["producers"][claim["producer"]]
        if set(result["outputs"]) != set(producer["outputs"]):
            raise ValueError("Output set differs from the registered producer")
        refs, receipts = {}, {}
        for asset, write in result["outputs"].items():
            baseline = claim["baseline"][asset]
            old = await self.state.load(baseline["ref"]) if baseline and claim["mode"] != "recompute" else []
            kind, complete = write["kind"], True
            if kind == "Replace":
                value = write["value"]
            elif kind == "Inventory":
                value, complete = write["rows"], write["complete"]
                if not isinstance(value, list) or not isinstance(complete, bool):
                    raise ValueError("Inventory requires rows and a boolean completeness flag")
            else:
                if not isinstance(old, list) or not isinstance(write["rows"], list):
                    raise ValueError("Row mutations require JSON row lists")
                if kind == "ReplaceKeys":
                    column, keys = write["column"], set(map(str, write["keys"]))
                    if any(str(row[column]) not in keys for row in write["rows"]):
                        raise ValueError("Replacement rows fall outside the declared ownership keys")
                    value = [row for row in old if str(row[column]) not in keys] + write["rows"]
                elif kind == "Upsert":
                    columns = write["keys"]
                    if not columns or len(set(columns)) != len(columns):
                        raise ValueError("Upsert requires unique primary-key columns")

                    def key(row, columns=columns):
                        return digest([row[c] for c in columns])

                    mapped = {key(row): row for row in old}
                    for deletion in write["deletes"]:
                        if len(deletion) != len(columns):
                            raise ValueError("Invalid deletion key")
                        mapped.pop(digest(deletion), None)
                    incoming = {key(row): row for row in write["rows"]}
                    if len(incoming) != len(write["rows"]):
                        raise ValueError("Duplicate primary key in upsert")
                    mapped.update(incoming)
                    value = list(mapped.values())
                elif kind == "AppendBatch":
                    batch_id = write["batch_id"]
                    if not isinstance(batch_id, str) or not 1 <= len(batch_id) <= 200:
                        raise ValueError("Append requires a stable, nonempty batch ID")
                    receipt_key = f"append/{claim['scope']}/{esc(asset)}/{esc(batch_id)}"
                    fingerprint = digest(write["rows"])
                    previous = await self.state.get(receipt_key)
                    if previous and previous != fingerprint:
                        raise Conflict("Append batch ID reused with a different payload")
                    if claim["mode"] == "recompute":
                        raise Conflict(
                            "Append assets cannot use recompute; use a new partition or a Replace asset"
                        )
                    value = old if previous else old + write["rows"]
                    receipts[receipt_key] = fingerprint
                else:
                    raise ValueError(f"Unknown write operation: {kind}")
            refs[asset] = await self.state.stage(value, complete=complete)
        return refs, receipts

    async def commit(self, claim, prepared, result, refs, receipts, logs=""):
        commit_id = digest([claim["id"], claim["generation"]])
        fingerprint = digest(
            [
                refs,
                receipts,
                result.get("cursor"),
                prepared["checkpoint"],
                prepared["revisions"],
                prepared["changes"],
                prepared["interpretation"],
                prepared["more"],
            ]
        )
        async with self.state.transaction() as tx:
            previous = await tx.get(f"commit/{commit_id}")
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise Conflict("Attempt commit replay has a different result")
                return previous
            task = await self._owned(tx, claim)
            await self._validate_inputs(tx, claim)
            for asset, baseline in claim["baseline"].items():
                if await tx.get(head_key(asset, claim["partition"])) != baseline:
                    raise Conflict("Output head changed after this attempt was claimed")
            cp = await tx.get(f"checkpoint/{claim['scope']}", {"generation": 0})
            if cp["generation"] != prepared["checkpoint"]["generation"]:
                raise Conflict("Checkpoint advanced after this attempt was prepared")
            producer = self.manifest["producers"][claim["producer"]]
            checkpoint = {
                "generation": cp["generation"] + 1,
                "cursor": result.get("cursor"),
                "state_version": producer["version"],
                "commit_id": commit_id,
            }
            changed = [
                a for a, ref in refs.items() if not claim["baseline"][a] or claim["baseline"][a]["ref"] != ref
            ]
            record = {
                "id": commit_id,
                "fingerprint": fingerprint,
                "run_id": claim["run_id"],
                "task_id": claim["id"],
                "producer": claim["producer"],
                "partition": claim["partition"],
                "generation": claim["generation"],
                "created_at": self.clock(),
                "inputs": claim["input_refs"],
                "outputs": refs,
                "checkpoint": checkpoint,
                "changed": changed,
            }
            await tx.put(f"commit/{commit_id}", record)
            scope_complete = not prepared["more"] and all(
                info["ref"]["complete"] for info in claim["input_refs"].values()
            )
            for asset, ref in refs.items():
                await tx.put(
                    head_key(asset, claim["partition"]),
                    {
                        "ref": ref,
                        "commit_id": commit_id,
                        "updated_at": self.clock(),
                        "producer": claim["producer"],
                        "partition": claim["partition"],
                        "scope_complete": scope_complete and ref["complete"],
                        "state_version": producer["version"],
                    },
                )
            prefix = f"item/{claim['scope']}/"
            if claim["mode"] == "recompute":
                for key, _ in await tx.scan(prefix):
                    await tx.delete(key)
            for key in prepared["changes"]["deleted_keys"]:
                await tx.delete(prefix + esc(key))
            for key, revision in prepared["revisions"].items():
                await tx.put(
                    prefix + esc(key),
                    {"key": key, "revision": revision, "interpretation": prepared["interpretation"]},
                )
            for key, receipt in receipts.items():
                existing = await tx.get(key)
                if existing and existing != receipt:
                    raise Conflict("Append receipt conflict")
                await tx.put(key, receipt)
            await tx.put(f"checkpoint/{claim['scope']}", checkpoint)
            await tx.put(
                f"outbox/{commit_id}",
                {"commit_id": commit_id, "changed": changed, "created_at": self.clock()},
            )
            await tx.put(
                f"attempt/{claim['id']}/{claim['generation']:010d}",
                {
                    "generation": claim["generation"],
                    "status": "succeeded",
                    "commit_id": commit_id,
                    "logs": logs[-65536:],
                },
            )
            task["result"], task["error"], task["attempt_count"] = refs, None, 0
            await self._release(tx, task)
            if prepared["more"]:
                await self._queue(tx, task)
            else:
                task["status"] = "succeeded"
                await tx.put(f"task/{task['id']}", task)
            await self._event(
                tx,
                task["run_id"],
                "materialized",
                f"Committed {', '.join(refs)}",
                task["id"],
                {
                    "commit_id": commit_id,
                    "changed": changed,
                    "processed_keys": len(prepared["revisions"]),
                    "deleted_keys": len(prepared["changes"]["deleted_keys"]),
                    "more": prepared["more"],
                },
            )
            await self._advance(tx, task["run_id"])
            return record

    async def _skip(self, claim):
        async with self.state.transaction() as tx:
            task = await self._owned(tx, claim)
            await self._validate_inputs(tx, claim)
            task["status"], task["result"] = "skipped", {a: h["ref"] for a, h in claim["baseline"].items()}
            await self._release(tx, task)
            await tx.put(f"task/{task['id']}", task)
            await tx.put(
                f"attempt/{claim['id']}/{claim['generation']:010d}",
                {"generation": claim["generation"], "status": "skipped"},
            )
            await self._event(tx, task["run_id"], "unchanged", "No new source revisions", task["id"])
            await self._advance(tx, task["run_id"])

    async def fail(self, claim, error):
        async with self.state.transaction() as tx:
            try:
                task = await self._owned(tx, claim)
            except LostOwnership:
                return
            task["error"] = str(error)[-65536:]
            await self._release(tx, task)
            await tx.put(
                f"attempt/{claim['id']}/{claim['generation']:010d}",
                {"generation": claim["generation"], "status": "failed", "error": task["error"]},
            )
            if task["attempt_count"] < task["max_attempts"] and not isinstance(error, Conflict):
                await self._queue(tx, task, self.retry_delay)
            else:
                task["status"] = "failed"
                await tx.put(f"task/{task['id']}", task)
            await self._event(tx, task["run_id"], "failed", task["error"][-4000:], task["id"])
            await self._advance(tx, task["run_id"])

    async def execute(self, claim):
        async def heartbeat():
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                await self.renew(claim)

        renewal = asyncio.create_task(heartbeat())
        try:
            prepared = await self.prepare(claim)
            if prepared["skip"]:
                await self._skip(claim)
                return
            result, logs = await self.backend.execute(prepared["spec"])
            if renewal.done():
                renewal.result()
            refs, receipts = await self.stage_result(claim, result)
            await self.commit(claim, prepared, result, refs, receipts, logs)
        except Unavailable:
            raise
        except Exception as error:
            if self.state.poisoned:
                raise
            await self.fail(claim, error)
        finally:
            renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await renewal

    async def execute_next(self):
        await self.recover_expired()
        claim = await self.claim()
        if claim is None:
            return False
        await self.execute(claim)
        return True

    async def cancel(self, run_id):
        async with self.state.transaction() as tx:
            run = await tx.get(f"run/{run_id}")
            if not run:
                raise KeyError(run_id)
            if run["status"] in {"succeeded", "failed", "canceled"}:
                return run
            run["status"], run["updated_at"] = "canceled", self.clock()
            await tx.put(f"run/{run_id}", run)
            for task_id in run["tasks"]:
                task = await tx.get(f"task/{task_id}")
                if task["status"] not in TERMINAL:
                    await self._release(tx, task)
                    task["status"], task["generation"] = "canceled", task["generation"] + 1
                    await tx.put(f"task/{task_id}", task)
            await self._event(
                tx, run_id, "canceled", "Publication rights revoked; committed outputs retained"
            )
            return run

    async def pause(self, run_id, paused=True):
        async with self.state.transaction() as tx:
            run = await tx.get(f"run/{run_id}")
            if not run:
                raise KeyError(run_id)
            if run["status"] in {"succeeded", "failed", "canceled"}:
                raise Conflict("Only active requests can be paused or resumed")
            run["paused"] = paused
            await tx.put(f"run/{run_id}", run)
            await self._advance(tx, run_id)
            return await tx.get(f"run/{run_id}")

    async def retry(self, run_id):
        async with self.state.transaction() as tx:
            run = await tx.get(f"run/{run_id}")
            if not run:
                raise KeyError(run_id)
            if run["status"] != "failed":
                raise Conflict("Only failed requests can be repaired")
            if run["revision"] != self.manifest["revision"]:
                raise Conflict("Code changed; submit a new request")
            for task_id in run["tasks"]:
                task = await tx.get(f"task/{task_id}")
                if task["status"] in {"failed", "blocked"}:
                    task.update(status="waiting", attempt_count=0, error=None)
                    await tx.put(f"task/{task_id}", task)
            run["status"], run["paused"] = "running", False
            await tx.put(f"run/{run_id}", run)
            await self._advance(tx, run_id)
            await self._event(tx, run_id, "repair", "Retrying failed work; committed batches retained")
            return await tx.get(f"run/{run_id}")

    async def evaluate_automations(self):
        async with self.state.transaction() as tx:
            for key, a in await tx.scan("automation/"):
                if not a["enabled"] or a["next_at"] > self.clock():
                    continue
                body = {
                    "targets": a["targets"],
                    "partitions": [],
                    "mode": "incremental",
                    "config": {},
                    "revision": self.manifest["revision"],
                }
                await self._submit(
                    tx,
                    self._plan(a["targets"], []),
                    body,
                    f"automation:{a['name']}:{a['next_at']}",
                    f"automation:{a['name']}",
                )
                a["last_at"], a["next_at"] = self.clock(), self.clock() + a["every_seconds"]
                await tx.put(key, a)

    async def set_automation(self, name, enabled):
        async with self.state.transaction() as tx:
            key = f"automation/{esc(name)}"
            value = await tx.get(key)
            if not value:
                raise KeyError(name)
            value["enabled"] = enabled
            await tx.put(key, value)
            return value

    async def list_runs(self, limit=50):
        async with self.state.transaction() as tx:
            return [await tx.get(f"run/{run_id}") for _, run_id in await tx.scan("runs/", limit)]

    async def run_detail(self, run_id):
        async with self.state.transaction() as tx:
            run = await tx.get(f"run/{run_id}")
            if not run:
                raise KeyError(run_id)
            tasks = [await tx.get(f"task/{task_id}") for task_id in run["tasks"]]
            events = [v for _, v in await tx.scan(f"event/{run_id}/")]
            attempts = {t["id"]: [v for _, v in await tx.scan(f"attempt/{t['id']}/")] for t in tasks}
            return {"request": run, "tasks": tasks, "events": events, "attempts": attempts}

    async def catalog(self):
        async with self.state.transaction() as tx:
            result = []
            for name, producer_name in self.manifest["owners"].items():
                p = self.manifest["producers"][producer_name]
                heads = [h for _, h in await tx.scan(f"head/{esc(name)}/")]
                result.append(
                    {
                        "name": name,
                        **{k: p[k] for k in ("group", "description", "partitions", "version", "incremental")},
                        "producer": producer_name,
                        "inputs": list(p["inputs"].values()),
                        "heads": heads,
                    }
                )
            return result

    async def asset_detail(self, name, partition=""):
        if name not in self.manifest["owners"]:
            raise KeyError(name)
        producer = self.manifest["owners"][name]
        async with self.state.transaction() as tx:
            head = await tx.get(head_key(name, partition))
            checkpoint = await tx.get(f"checkpoint/{scope(producer, partition)}")
            commit = await tx.get(f"commit/{head['commit_id']}") if head else None
        data = await self.state.load(head["ref"]) if head else None
        return {
            "head": head,
            "checkpoint": checkpoint,
            "commit": commit,
            "preview": data[:100] if isinstance(data, list) else data,
        }

    async def start(self):
        async def loop():
            try:
                while True:
                    for task in list(self.executions):
                        if task.done():
                            self.executions.remove(task)
                            task.result()
                    await self.evaluate_automations()
                    await self.recover_expired()
                    while len(self.executions) < self.concurrency:
                        claim = await self.claim()
                        if claim is None:
                            break
                        self.executions.add(asyncio.create_task(self.execute(claim)))
                    await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = str(error)
                self.state.poisoned = True

        if self.runner:
            raise RuntimeError("Engine is already running")
        self.runner = asyncio.create_task(loop())

    async def stop(self):
        tasks = list(self.executions) + ([self.runner] if self.runner else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.runner = None
        self.executions.clear()
