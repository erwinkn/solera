"""The engine (§6–§10): control-plane only. It plans runs into per-(asset,
scope) tasks, resolves inputs to pinned heads, plans Incremental edges over
per-edge watermarks, dispatches attempts through placements, and commits
results through State.

Structure lives in the manifest, state lives in the spec, effects live in the
result — the engine never interprets a payload.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import uuid
from itertools import product
from zoneinfo import ZoneInfo

from croniter import croniter
from cursus.sdk import TimePartitions, canonical_partition, digest, split_partition
from cursus.stores import delta_path, next_batch

from .placements import PlacementContext, Registry
from .state import Conflict, LostOwnership, State, Tx

SUCCESS = {"succeeded", "skipped"}
TERMINAL = SUCCESS | {"failed", "blocked", "canceled"}
LEASE_SECONDS = 60.0
GRACE_SECONDS = 5.0


class Retryable(RuntimeError):
    """A dispatch-time failure that the retry policy may absorb."""


class NonRetryable(RuntimeError):
    """A dispatch-time failure no retry will fix (§8: full run required, …)."""


class Engine:
    def __init__(
        self,
        state: State,
        manifest: dict,
        *,
        registry: Registry | None = None,
        placements: dict | None = None,
        project: str = "",
        lease_seconds: float = LEASE_SECONDS,
        concurrency: int = 4,
        clock=None,
        eval_interval: float = 0.5,
        gc_interval: float = 300.0,
    ):
        import time

        if lease_seconds < 1 or concurrency < 1:
            raise ValueError("Lease duration and concurrency must be positive")
        self.state, self.manifest = state, manifest
        self.project = project
        self.clock = clock or time.time
        self.lease_seconds, self.concurrency = lease_seconds, concurrency
        self.eval_interval = eval_interval
        self.gc_interval = gc_interval
        self._gc_at = self.clock() + self.gc_interval
        ctx = PlacementContext(state, state.objects_url, project, self.clock)
        self.registry = registry or Registry(ctx, extra=placements)
        self.inflight: dict[str, asyncio.Task] = {}
        # Attempts consuming a local execution slot. Pool waiters only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.env_inflight: dict[str, int] = {}
        self.runner: asyncio.Task | None = None
        self.last_error = None
        self._stopping = False

    # -- lifecycle ---------------------------------------------------------------

    async def initialize(self):
        await self.state.initialize(self.manifest, self.manifest["revision"])
        if self.project:
            async with self.state.transaction() as tx:
                # The worker entrypoint spec for Local/pool launches.
                await tx.put("sys/entrypoint", self.project)
        async with self.state.transaction() as tx:
            for name, source in self.manifest["sources"].items():
                if await tx.head(name, "") is None:
                    await tx.put_head(
                        name,
                        "",
                        {
                            "ref": source["head"],
                            "commit": None,
                            "at": self.clock(),
                            "complete": True,
                            "asset": None,
                            "version": None,
                        },
                    )

    async def start(self):
        """Resume in-flight attempts at `wait` (§10) and start the eval loop."""

        async with self.state.transaction() as tx:
            for _, active in await tx.actives():
                attempt = active["attempt"]
                task = await tx.task(active["task"])
                if task is None or task["status"] != "running":
                    continue
                lock = await tx.lock(task["asset"], task["scope"])
                if lock is None or lock["attempt"] != attempt:
                    continue
                placement = self._placement(task)
                self._launch_waiter(task, attempt, active["prepared"], placement, active["handle"])
        self._stopping = False
        self.runner = asyncio.create_task(self._loop())

    async def stop(self):
        self._stopping = True
        if self.runner:
            self.runner.cancel()
            try:
                await self.runner
            except asyncio.CancelledError:
                pass
            self.runner = None
        if self.inflight:
            await asyncio.gather(*self.inflight.values(), return_exceptions=True)
            self.inflight.clear()

    async def _loop(self):
        while not self._stopping:
            try:
                await self.tick()
                self.last_error = None
            except Exception as error:
                self.last_error = f"{type(error).__name__}: {error}"
            await asyncio.sleep(self.eval_interval)

    async def tick(self):
        """One evaluation pass: lease sweeps, dispatch, automations, storage GC."""

        await self.state.sweep_scope_leases()
        await self.state.sweep_pool_leases()
        await self._dispatch_due()
        await self._automation_tick()
        await self._gc_due()

    async def _gc_due(self):
        """One SlateDB GC pass per `gc_interval` — bounds WAL/manifest/compacted
        objects under long-running file:// deployments (§4.4)."""

        if self.clock() < self._gc_at:
            return
        self._gc_at = self.clock() + self.gc_interval
        gc_once = getattr(self.state.store, "gc_once", None)
        if gc_once is not None:
            await gc_once()

    async def run_until(self, run_id: str, timeout: float = 120.0):
        """Tick until the run reaches a terminal status (CLI and tests)."""

        deadline = self.clock() + timeout
        while self.clock() < deadline:
            await self.tick()
            run = await self._run(run_id)
            if run and run["status"] in TERMINAL:
                # Wait only on this run's attempts — unrelated pool-placed runs
                # may hold inflight waiters until a worker claims them.
                mine = [t for a, t in self.inflight.items() if a.startswith(f"{run_id}/")]
                await asyncio.gather(*mine, return_exceptions=True)
                return await self.run_detail(run_id)
            await asyncio.sleep(0.05)
        raise TimeoutError(f"run {run_id} did not finish within {timeout}s")

    # -- planning (§7, §8) ---------------------------------------------------------

    async def submit(
        self,
        targets,
        partitions="latest",
        mode="incremental",
        upstream=False,
        config=None,
        keys=None,
        automation=None,
        command_id=None,
        skip_active=False,
    ):
        """A run request becomes one task per (asset, scope) (§8)."""

        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            raise ValueError("A run needs at least one target")
        if mode not in {"incremental", "full"}:
            raise ValueError(f"Unknown mode: {mode!r}")
        config = config or {}
        if not isinstance(config, dict):
            raise ValueError("config must be a JSON object")
        async with self.state.transaction() as tx:
            if command_id:
                existing = await tx.get(f"command/{command_id}")
                if existing:
                    return await tx.run(existing)
            assets = {}
            for target in targets:
                name = self._asset_of(target)
                assets[name] = await self._scopes(tx, name, partitions)
            if upstream:
                queue = [(n, s) for n, scopes in assets.items() for s in scopes]
                seen = set(queue)
                while queue:
                    name, scope = queue.pop()
                    for owner, up_scope in await self._upstream_of(tx, name, scope):
                        if owner is None or (owner, up_scope) in seen:
                            continue
                        seen.add((owner, up_scope))
                        assets.setdefault(owner, []).append(up_scope)
                        queue.append((owner, up_scope))
            if keys:
                incremental_outputs = {
                    e["output"]
                    for name in assets
                    for e in self.manifest["assets"][name]["inputs"].values()
                    if e["kind"] == "incremental"
                }
                unknown = set(keys) - incremental_outputs
                if unknown:
                    raise ValueError(f"keys= names no Incremental edge: {sorted(unknown)}")
            if skip_active:
                for name in list(assets):
                    kept = []
                    for scope in assets[name]:
                        lock = await tx.lock(name, scope)
                        live = lock is not None and lock["lease_until"] > self.clock()
                        if not live and not await tx.pending(name, scope):
                            kept.append(scope)
                    assets[name] = kept
                if not any(assets.values()):
                    return None  # §9: the tick is skipped — every scope is in flight
            run_id = uuid.uuid4().hex
            run = {
                "id": run_id,
                "targets": sorted(assets),
                "partitions": partitions if isinstance(partitions, str) else list(partitions),
                "mode": mode,
                "upstream": bool(upstream),
                "config": config,
                "keys": keys,
                "automation": automation,
                "status": "queued",
                "paused": False,
                "tasks": [],
                "created_at": self.clock(),
                "updated_at": self.clock(),
            }
            tasks = {}
            for name, scopes in assets.items():
                for scope in sorted(set(scopes)):
                    task_id = f"{run_id}/{name}:{scope}"
                    tasks[task_id] = {
                        "id": task_id,
                        "run": run_id,
                        "asset": name,
                        "scope": scope,
                        "status": "queued",
                        "deps": [],
                        "generation": 0,
                        "attempt_count": 0,
                        "max_attempts": 1 + self.manifest["assets"][name].get("retries", {}).get("n", 0),
                        "retry": self.manifest["assets"][name].get("retries"),
                        "ready_at": self.clock(),
                    }
            for task_id, task in tasks.items():
                for owner, up_scope in await self._upstream_of(tx, task["asset"], task["scope"]):
                    dep_id = f"{run_id}/{owner}:{up_scope}"
                    if dep_id in tasks and dep_id != task_id:
                        task["deps"].append(dep_id)
                        task["status"] = "waiting"
            for task in tasks.values():
                await tx.put_task(task)
                await tx.put_pending(task)
                if task["status"] == "queued":
                    await tx.enqueue(task["id"], task["ready_at"])
            run["tasks"] = sorted(tasks)
            await tx.put_run(run)
            await tx.index_run(run)
            if command_id:
                await tx.put(f"command/{command_id}", run_id)
            await self.state.advance_run(tx, run_id)
            return run

    def _asset_of(self, name: str) -> str:
        if name in self.manifest["assets"]:
            return name
        if name in self.manifest["outputs"] and self.manifest["outputs"][name].get("asset"):
            return self.manifest["outputs"][name]["asset"]
        raise ValueError(f"Unknown asset or output: {name!r}")

    async def _upstream_of(self, tx: Tx, asset: str, scope: str):
        """(owner_asset, upstream_scope) for every edge, dep, and partition-set
        dimension of (asset, scope) — a bound key set is a pinned dep (§7)."""

        info = self.manifest["assets"][asset]
        edges = list(info["inputs"].values()) + [{"kind": "dep", "output": d} for d in info["deps"]]
        out = []
        for edge in edges:
            owner = self.manifest["outputs"][edge["output"]].get("asset")
            up_dims = self._dims(owner) if owner else {}
            if edge["kind"] in {"all_partitions", "dep"} and owner is not None:
                out.extend((owner, s) for s in await self._spread(tx, info, scope, up_dims))
                continue
            out.append((owner, self._project(info, scope, up_dims)))
        for dim in self._dims(asset).values():
            if dim["kind"] == "set":
                owner = self.manifest["outputs"][dim["output"]].get("asset")
                if owner is not None:
                    out.append((owner, ""))
        return out

    async def _spread(self, tx: Tx, consumer: dict, consumer_scope: str, upstream_dims: dict):
        """Every upstream scope visible at (consumer, consumer_scope): shared
        dims pin the consumer's key; upstream-only dims expand over the current
        key set (§7). Used by planning and by dep/AllPartitions pinning."""

        if not upstream_dims:
            return [""]
        c_dims = self._dims_of(consumer)
        parts = split_partition(c_dims, consumer_scope) if c_dims else {}
        pinned, free = {}, {}
        for name, dim in upstream_dims.items():
            match = next((cn for cn, cd in c_dims.items() if self._same_dim(dim, cd)), None)
            if match is not None:
                pinned[name] = parts[match]
            else:
                free[name] = dim
        if not free:
            return [canonical_partition(upstream_dims, pinned)]
        keys = await self._dim_keys(tx, free)
        return [
            canonical_partition(upstream_dims, {**pinned, **dict(zip(free, combo, strict=True))})
            for combo in product(*keys)
        ]

    async def _scopes(self, tx: Tx, asset: str, selection) -> list[str]:
        """`partitions` selects keys from the current set (§7, §8)."""

        dims = self._dims(asset)
        if not dims:
            current = [""]
        else:
            keys = await self._dim_keys(tx, dims)
            current = [
                canonical_partition(dims, parts)
                for parts in (dict(zip(dims, combo, strict=True)) for combo in product(*keys))
            ]
        if selection == "all" or selection is None:
            return current
        if selection == "latest":
            if not dims:
                return current
            chosen = []
            keys = await self._dim_keys(tx, dims)
            for name, dim in dims.items():
                if dim["kind"] == "time":
                    latest = self._time(dim).latest(self._now())
                    chosen.append([latest] if latest else [])
                else:
                    chosen.append(keys[list(dims).index(name)])
            return [
                canonical_partition(dims, dict(zip(dims, combo, strict=True)))
                for combo in product(*chosen)
                if all(v is not None for v in combo)
            ]
        if selection == "missing":
            missing = []
            for scope in current:
                outputs = self.manifest["assets"][asset]["outputs"]
                heads = [await tx.head(o["name"], scope) for o in outputs]
                if not heads or any(h is None or not h["complete"] for h in heads):
                    missing.append(scope)
            return missing
        if isinstance(selection, dict):
            selection = selection.get(asset, [])
        wanted = {self._canon(dims, k) for k in (selection or [])} if dims else set(selection or [])
        return [s for s in current if s in wanted]

    def _canon(self, dims: dict, key: str) -> str:
        if len(dims) == 1:
            return str(key)
        return canonical_partition(dims, split_partition(dims, key))

    async def _dim_keys(self, tx: Tx, dims: dict) -> list[list[str]]:
        out = []
        for dim in dims.values():
            if dim["kind"] == "static":
                out.append([str(k) for k in dim["keys"]])
            elif dim["kind"] == "time":
                out.append(self._time(dim).keys(self._now()))
            else:
                head = await tx.head(dim["output"], "")
                keys = await self._head_keys(head)
                out.append(sorted(keys) if keys else [])
        return out

    async def _head_keys(self, head) -> list[str] | None:
        """A set-dim output's current element list: `meta.partitions` on a
        partition-set/source head, else a fold of the delta log (§2.1, §7)."""

        if head is None:
            return None
        meta = head["ref"].get("meta") or {}
        if meta.get("partitions") is not None:
            return [str(e) for e in meta["partitions"]]
        if meta.get("delta"):
            keys = await self.state.delta_key_map(
                head["ref"]["output"], head["ref"].get("partition", ""), meta["delta"]["batch"]
            )
            return sorted(keys)
        return []

    def _time(self, dim: dict) -> TimePartitions:
        return TimePartitions(
            dim["start"],
            dim["every"],
            end=dim.get("end"),
            end_offset=dim.get("end_offset"),
            timezone=dim.get("timezone") or "UTC",
            format=dim.get("format"),
        )

    def _now(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.clock(), dt.UTC)

    def _dims(self, asset: str | None) -> dict:
        if asset is None:
            return {}
        return (self.manifest["assets"][asset].get("partitions") or {}).get("dims") or {}

    def _same_dim(self, a: dict, b: dict) -> bool:
        if a["kind"] != b["kind"]:
            return False
        if a["kind"] == "set":
            return a["output"] == b["output"]
        return a == b

    def _project(self, consumer: dict, consumer_scope: str, upstream_dims: dict) -> str:
        """The projection rule (§7): shared dims take the consumer key;
        consumer-only dims broadcast away; upstream-only dims must be collapsed."""

        c_dims = self._dims_of(consumer)
        if not upstream_dims:
            return ""
        parts = split_partition(c_dims, consumer_scope) if c_dims else {}
        out = {}
        for name, dim in upstream_dims.items():
            for c_name, c_dim in c_dims.items():
                if self._same_dim(dim, c_dim):
                    out[name] = parts[c_name]
                    break
            else:
                raise Conflict(f"upstream-only dimension {name!r} requires AllPartitions")
        return canonical_partition(upstream_dims, out)

    def _project_downstream(self, producer: str, scope: str, target: str) -> dict[str, str]:
        """Shared dims pinned by the changed scope; target-only dims are left
        for the caller to expand over the current key set (§7, §9)."""

        p_dims, t_dims = self._dims(producer), self._dims(target)
        if not p_dims or not t_dims:
            return {}
        parts = split_partition(p_dims, scope)
        pinned = {}
        for t_name, t_dim in t_dims.items():
            for p_name, p_dim in p_dims.items():
                if self._same_dim(t_dim, p_dim):
                    pinned[t_name] = parts[p_name]
                    break
        return pinned

    def _dims_of(self, asset: dict) -> dict:
        return (asset.get("partitions") or {}).get("dims") or {}

    # -- dispatch ---------------------------------------------------------------

    async def _dispatch_due(self):
        dispatched = []
        dispatched_env: dict[str, int] = {}
        dispatched_engine = 0
        async with self.state.transaction() as tx:
            now_ms = self.clock() * 1000
            for key, task_id in await tx.queued():
                if int(key.split("/")[1]) > now_ms:
                    break
                await tx.delete(key)
                task = await tx.task(task_id)
                if task is None or task["status"] != "queued":
                    continue
                run = await tx.run(task["run"])
                if run is None or run["status"] == "canceled" or run.get("paused"):
                    await tx.enqueue(task_id, task["ready_at"])
                    continue
                placement = self._placement(task)
                spec = self.manifest["assets"][task["asset"]]["placement"]
                env_key = self.registry.env_key(spec)
                limit = getattr(placement, "max_concurrent", None)
                if (
                    spec["kind"] != "Pool"
                    and len(self.engine_inflight) + dispatched_engine >= self.concurrency
                ) or (
                    limit is not None
                    and self.env_inflight.get(env_key, 0) + dispatched_env.get(env_key, 0) >= limit
                ):
                    await tx.enqueue(task_id, self.clock() + 1)
                    continue
                try:
                    claim = await self.state.claim(tx, task, self.lease_seconds)
                except Conflict:
                    await tx.enqueue(task_id, self.clock() + 2)
                    continue
                try:
                    prepared = await self._prepare(tx, task, run)
                except Retryable as error:
                    task["status"] = "running"
                    await tx.put_task(task)
                    dispatched.append((task, claim["id"], None, None, (str(error), True)))
                    continue
                except NonRetryable as error:
                    task["status"] = "running"
                    await tx.put_task(task)
                    dispatched.append((task, claim["id"], None, None, (str(error), False)))
                    continue
                task["status"] = "running"
                task["started_at"] = self.clock()
                await tx.put_task(task)
                dispatched.append((task, claim["id"], prepared, placement, None))
                dispatched_env[env_key] = dispatched_env.get(env_key, 0) + 1
                if spec["kind"] != "Pool":
                    dispatched_engine += 1
        for task, attempt, prepared, placement, error in dispatched:
            if error is not None:
                message, retryable = error
                await self.state.fail_attempt(attempt, message, retryable=retryable)
                continue
            if prepared.get("skip"):
                await self.state.skip_attempt(
                    attempt, prepared["baseline"], prepared.get("watermark_updates")
                )
                continue
            env_key = self.registry.env_key(self.manifest["assets"][task["asset"]]["placement"])
            self.env_inflight[env_key] = self.env_inflight.get(env_key, 0) + 1
            asyncio_task = asyncio.create_task(self._execute(task, attempt, prepared, placement))
            self.inflight[attempt] = asyncio_task
            asyncio_task.add_done_callback(lambda _t, a=attempt: self.inflight.pop(a, None))

    def _placement(self, task: dict):
        spec = self.manifest["assets"][task["asset"]]["placement"]
        return self.registry.build(spec)

    # -- input resolution + Incremental plans (§5, §6, §8) --------------------------

    async def _prepare(self, tx: Tx, task: dict, run: dict) -> dict:
        """Pin heads at attempt start; plan Incremental edges; decide skip (§8)."""

        asset = self.manifest["assets"][task["asset"]]
        scope = task["scope"]
        full = run["mode"] == "full"
        baseline = {}
        for output in asset["outputs"]:
            head = await tx.head(output["name"], scope)
            baseline[output["name"]] = head
            if (
                not full
                and head is not None
                and head.get("version") is not None
                and head["version"] != asset["version"]
            ):
                if asset.get("on_version_change") == "full":
                    full = True
                else:
                    raise NonRetryable(
                        f"{task['asset']}: committed version {head['version']} != declared "
                        f"{asset['version']}: a full run is required"
                    )
        edges = list(asset["inputs"].items())
        for dep in asset["deps"]:
            edges.append((dep, {"kind": "dep", "output": dep}))
        # A bound partition set pins its ref in lineage (§7).
        for dim in self._dims(task["asset"]).values():
            if dim["kind"] == "set" and dim["output"] not in {e["output"] for _, e in edges}:
                # The set pins into lineage and plans upstream, but it is the
                # dimension — not interpretation — so it stays out of the
                # fingerprint: adding a key must not invalidate existing ones.
                edges.append((dim["output"], {"kind": "dep", "output": dim["output"], "set_dim": True}))
        # Pass 1: pin every non-Incremental edge; their refs enter the fingerprint (§6).
        inputs, pinned = {}, {}
        incremental = []
        for param, edge in edges:
            output = edge["output"]
            owner = self.manifest["outputs"][output].get("asset")
            up_dims = self._dims(owner)
            if edge["kind"] == "all_partitions":
                refs = await self._all_partitions(tx, task, asset, up_dims, output)
                inputs[param] = {"refs": refs}
                pinned[param] = refs
                continue
            if edge["kind"] == "dep":
                refs = {}
                for s in await self._spread(tx, asset, scope, up_dims):
                    refs[s] = await self._pin_at(tx, output, s)
                inputs[param] = {"refs": refs}
                if not edge.get("set_dim"):
                    pinned[param] = refs
                continue
            if edge["kind"] == "incremental":
                incremental.append((param, edge, up_dims))
                continue
            inputs[param] = {"ref": await self._pin(tx, output, up_dims, asset, scope)}
            pinned[param] = inputs[param]["ref"]
        fingerprint = self._fingerprint(asset, run, pinned)
        # Pass 2: Incremental plans against the fingerprinted interpretation (§2.2).
        watermark_updates, all_empty = {}, True
        for param, edge, up_dims in incremental:
            ref = await self._pin(tx, edge["output"], up_dims, asset, scope)
            pin, update, empty = await self._incremental_plan(
                tx, task, asset, param, edge, ref, fingerprint, run, full
            )
            inputs[param] = pin
            if update is not None:
                watermark_updates[param] = update
            all_empty = all_empty and empty
        more = any(u.get("more") for u in watermark_updates.values())
        skip = bool(incremental) and all_empty and not more and not full
        if skip:
            for output in asset["outputs"]:
                head = baseline[output["name"]]
                if head is None or not head["complete"]:
                    skip = False
                    break
        prior = {name: head["ref"] for name, head in baseline.items() if head is not None}
        cursor = await tx.cursor(task["asset"], scope)
        if full:
            prior, cursor = {}, None
        return {
            "inputs": inputs,
            "baseline": baseline,
            "watermark_updates": watermark_updates,
            "more": more,
            "scope_complete": not more,
            "full": full,
            "skip": skip,
            "prior": prior,
            "cursor": cursor,
            "fingerprint": fingerprint,
            "batches": {
                o["name"]: next_batch(baseline[o["name"]]["ref"] if baseline[o["name"]] else None)
                for o in asset["outputs"]
                if o.get("incremental")
            },
        }

    async def _pin(self, tx: Tx, output: str, up_dims: dict, asset: dict, scope: str):
        """Resolve one edge to its head ref; sources synthesize theirs (§5, §8)."""

        return await self._pin_at(tx, output, self._project(asset, scope, up_dims))

    async def _pin_at(self, tx: Tx, output: str, up_scope: str):
        """Head ref for one upstream scope; sources synthesize theirs (§5, §8)."""

        head = await tx.head(output, up_scope)
        if head is None:
            source = self.manifest["sources"].get(output)
            if source is not None and up_scope == "":
                return dict(source["head"])
            raise Retryable(f"input {output!r} has no head for scope {up_scope!r}")
        return head["ref"]

    async def _all_partitions(self, tx, task, asset, up_dims, output) -> dict:
        """AllPartitions pins every upstream key with a complete head (§7)."""

        parts = split_partition(self._dims_of(asset), task["scope"]) if self._dims_of(asset) else {}
        collapsed = {}
        for name, dim in up_dims.items():
            shared = any(self._same_dim(dim, c) for c in self._dims_of(asset).values())
            if not shared:
                collapsed[name] = dim
        if not collapsed:
            head = await tx.head(output, self._project(asset, task["scope"], up_dims))
            return {"": head["ref"]} if head is not None and head["complete"] else {}
        keys = await self._dim_keys(tx, collapsed)
        refs = {}
        for combo in product(*keys):
            collapsed_parts = dict(zip(collapsed, combo, strict=True))
            full = dict(collapsed_parts)
            for name, dim in up_dims.items():
                if name not in collapsed:
                    for c_name, c_dim in self._dims_of(asset).items():
                        if self._same_dim(dim, c_dim):
                            full[name] = parts[c_name]
            scope = canonical_partition(up_dims, full)
            head = await tx.head(output, scope)
            if head is not None and head["complete"]:
                refs[canonical_partition(collapsed, collapsed_parts)] = head["ref"]
        return refs

    async def _incremental_plan(self, tx, task, asset, param, edge, ref, fingerprint, run, full):
        """Plan one Incremental edge: the pending delta items after the edge's
        watermark, capped at `batch_size` (§2.2).

        watermark = {"batch", "offset", "fingerprint"}: items of batches below
        `batch` are delivered, plus the first `offset` items of `batch`. A
        missing watermark, a fingerprint change, or a `full` run resets the
        edge to a whole-head delivery. Returns (pin, watermark_update, empty);
        the update carries `more` while pending items remain after the take."""

        decl = self.manifest["outputs"][edge["output"]]
        keyed = decl.get("key") is not None
        batch_size = int(edge.get("batch_size") or 100)
        head_batch = int(((ref.get("meta") or {}).get("delta") or {}).get("batch", -1))
        up_scope = ref.get("partition") or ""
        override = (run.get("keys") or {}).get(edge["output"])
        wm = await tx.watermark(task["asset"], param, task["scope"])
        reset = full or wm is None or wm.get("fingerprint") != fingerprint or override == "full"

        # A keys= override is a one-off selection — it never moves the watermark.
        if isinstance(override, dict) and "keys" in override and not reset:
            keys = [str(k) for k in override["keys"]]
            pin = {
                "ref": ref,
                "changes": {"upserted": {k: "" for k in keys}, "deleted": [], "full": False},
            }
            return pin, None, not keys

        if reset:
            changes = {"full": True}
            if keyed:
                changes["upserted"], changes["deleted"] = {}, []
            else:
                changes["batches"] = [0, head_batch]
            pin = {"ref": ref, "changes": changes}
            update = {"batch": head_batch + 1, "offset": 0, "fingerprint": fingerprint, "more": False}
            return pin, update, head_batch < 0

        # wm.batch is the first not-fully-delivered batch: offset items of it
        # are already consumed, batches below it are fully delivered.
        deltas = []
        gap = False
        for b in range(max(0, wm["batch"]), head_batch + 1):
            d = await self.state.delta(edge["output"], up_scope, b)
            if d is None:
                gap = True
                break
            deltas.append(d)
        if gap:
            # The log was pruned under the watermark — restart from the head.
            changes = {"full": True}
            if keyed:
                changes["upserted"], changes["deleted"] = {}, []
            else:
                changes["batches"] = [0, head_batch]
            pin = {"ref": ref, "changes": changes}
            update = {"batch": head_batch + 1, "offset": 0, "fingerprint": fingerprint, "more": False}
            return pin, update, head_batch < 0

        if not keyed:
            # Batch-mode upstream: every pending batch is one item; a reset
            # batch supersedes everything before it.
            resets = [i for i, d in enumerate(deltas) if d.get("reset")]
            if resets:
                deltas = deltas[resets[-1] :]
            pending = [d["batch"] for d in deltas]
            take = pending[:batch_size]
            more = len(pending) > len(take)
            changes = {"batches": [take[0], take[-1]] if take else [0, -1], "full": False}
            pin = {"ref": ref, "changes": changes}
            update = {
                "batch": (take[-1] + 1) if take else wm["batch"],
                "offset": 0,
                "fingerprint": fingerprint,
                "more": more,
            }
            return pin, update, not take

        latest = {}
        for d in deltas:
            if d.get("reset"):
                # A reset supersedes every pending item before it; the keys it
                # dropped ride along in its own `deleted` list.
                latest.clear()
            upserted = d.get("upserted") or {}
            deleted = set(d.get("deleted") or [])
            for i, key in enumerate(sorted(set(upserted) | deleted)):
                if d["batch"] == wm["batch"] and i < wm["offset"]:
                    continue
                latest[key] = (d["batch"], i, "del" if key in deleted else "upsert")
        pending = sorted(latest.items(), key=lambda kv: kv[1])
        take, rest = pending[:batch_size], pending[batch_size:]
        delivered_ups, delivered_del = {}, []
        for key, (b, i, kind) in take:
            if kind == "del":
                delivered_del.append(key)
            else:
                delivered_ups[key] = ""
        # Revisions ride along in the delta; fill them in for the pin.
        for d in deltas:
            for k, r in (d.get("upserted") or {}).items():
                if k in delivered_ups:
                    delivered_ups[k] = str(r)
        pin = {
            "ref": ref,
            "changes": {"upserted": delivered_ups, "deleted": delivered_del, "full": False},
        }
        if take:
            b_last, i_last, _ = take[-1][1]
            position = {"batch": b_last, "offset": i_last + 1}
        else:
            position = {"batch": wm["batch"], "offset": wm["offset"]}
        update = {**position, "fingerprint": fingerprint, "more": bool(rest)}
        return pin, update, not take

    def _fingerprint(self, asset, run, pinned):
        """H(version, store versions of input+output stores, run config,
        refs of non-Incremental inputs and deps) — per-key interpretation
        state; a change resets the edge's watermark (§2.2, §6)."""

        stores = set()
        for output in asset["outputs"]:
            stores.add(output["store"])
        for edge in asset["inputs"].values():
            stores.add(self.manifest["outputs"][edge["output"]]["store"])
        for dep in asset["deps"]:
            stores.add(self.manifest["outputs"][dep]["store"])
        return digest(
            {
                "version": asset["version"],
                "stores": sorted(f"{name}@{self.manifest['stores'][name]['version']}" for name in stores),
                "migrations": {o["name"]: o["migrations"] for o in asset["outputs"] if o.get("migrations")},
                "config": run.get("config") or {},
                "refs": pinned,
            }
        )

    # -- the placement loop (§10) ---------------------------------------------------

    async def _execute(self, task, attempt, prepared, placement):
        run = await self._run(task["run"]) or {}
        spec = {
            "attempt": attempt,
            "revision": self.manifest["revision"],
            "asset": task["asset"],
            "partition": task["scope"],
            "run": {"id": task["run"], "config": run.get("config") or {}},
            "prior": prepared["prior"],
            "baseline": {
                name: head["ref"] for name, head in prepared["baseline"].items() if head is not None
            },
            "batches": prepared["batches"],
            "inputs": prepared["inputs"],
            "execution": self.manifest["assets"][task["asset"]]["placement"],
        }
        if prepared["cursor"] is not None:
            spec["cursor"] = prepared["cursor"]
        env_key = self.registry.env_key(spec["execution"])
        is_pool = spec["execution"]["kind"] == "Pool"
        if not is_pool:
            self.engine_inflight.add(attempt)
        try:
            await self.state.put_object(f"specs/{attempt}.json", json.dumps(spec).encode())
            if is_pool:
                await self.state.stage_pool_task(task, prepared, spec)
            try:
                handle = await placement.launch({"attempt": attempt, "objects": self.state.objects_url})
            except Exception as error:
                await self.state.fail_attempt(attempt, f"launch: {error}", retryable=True)
                return
            async with self.state.transaction() as tx:
                await tx.put_active(
                    attempt,
                    {"attempt": attempt, "task": task["id"], "handle": handle, "prepared": prepared},
                )
            await self._wait_loop(task, attempt, prepared, placement, handle)
        finally:
            self.engine_inflight.discard(attempt)
            self.env_inflight[env_key] = max(0, self.env_inflight.get(env_key, 1) - 1)

    async def _wait_loop(self, task, attempt, prepared, placement, handle):
        timeout_s = self.manifest["assets"][task["asset"]].get("timeout") or 3600
        deadline = self.clock() + timeout_s
        while True:
            try:
                exit_ = await placement.wait(handle, self.lease_seconds / 3)
            except Exception as error:
                await self.state.fail_attempt(attempt, f"wait: {error}", retryable=True)
                return
            if exit_ is not None:
                break
            if self.clock() > deadline:
                await self._cancel(placement, handle)
                await self.state.fail_attempt(attempt, "timeout", retryable=True)
                return
            try:
                async with self.state.transaction() as tx:
                    await self.state.renew(tx, attempt, self.lease_seconds)
            except (LostOwnership, Conflict):
                await self._cancel(placement, handle)
                return
        result_data = await self.state.get_object(f"results/{attempt}.json")
        if result_data is None:
            await self.state.fail_attempt(
                attempt, f"harness exited without a result: {exit_}", retryable=True
            )
            return
        result = json.loads(result_data)
        if result.get("status") == "failed":
            error = result.get("error") or {}
            await self.state.fail_attempt(
                attempt,
                f"{error.get('type', 'Error')}: {error.get('message', '')}",
                retryable=bool(error.get("retryable")),
                delay=self._retry_delay(task),
            )
            return
        try:
            await self.state.commit_attempt(self.manifest, attempt, prepared, result)
        except LostOwnership:
            return
        except Conflict as error:
            await self.state.fail_attempt(attempt, str(error), retryable=getattr(error, "retryable", True))

    def _retry_delay(self, task) -> float:
        retry = task.get("retry") or {}
        delay = float(retry.get("delay", 1.0))
        if retry.get("backoff") == "exponential":
            delay *= 2 ** max(0, task.get("attempt_count", 1) - 1)
        return delay

    async def _cancel(self, placement, handle):
        import contextlib

        with contextlib.suppress(Exception):
            await placement.cancel(handle)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(placement.wait(handle, GRACE_SECONDS), GRACE_SECONDS + 1)

    def _launch_waiter(self, task, attempt, prepared, placement, handle):
        async def resumed():
            await self._wait_loop(task, attempt, prepared, placement, handle)

        asyncio_task = asyncio.create_task(resumed())
        self.inflight[attempt] = asyncio_task
        asyncio_task.add_done_callback(lambda _t, a=attempt: self.inflight.pop(a, None))

    # -- sources commit API (§5) ------------------------------------------------------

    async def commit_source(self, name: str, *, version=None, keys=None, upsert=None, remove=None):
        """Advance a source without moving data (§2.3): a keyed source commit
        diffs the supplied map against the delta log's fold and writes the
        same delta object a store would; `meta.partitions` carries the
        committed element list. An identical map is not a change."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        async with self.state.transaction() as tx:
            head = await tx.head(name, "")
            keyed = source.get("key") is not None
            if not keyed and (keys is not None or upsert is not None or remove is not None):
                raise ValueError(f"Source {name!r} is unkeyed; pass version=")
            prior_delta = (((head or {}).get("ref") or {}).get("meta") or {}).get("delta")
            batch = int(prior_delta["batch"]) + 1 if prior_delta else 0
            current = (
                await self.state.delta_key_map(name, "", int(prior_delta["batch"]))
                if prior_delta
                else {}
            )
            if keyed:
                if keys is not None:
                    if source.get("key") == "<elements>" and not isinstance(keys, dict):
                        new_map = {str(k): "1" for k in keys}
                    else:
                        new_map = {str(k): str(v) for k, v in dict(keys).items()}
                else:
                    new_map = dict(current)
                    items = upsert.items() if isinstance(upsert, dict) else ((k, "1") for k in upsert or [])
                    for k, v in items:
                        new_map[str(k)] = str(v)
                    for k in remove or []:
                        new_map.pop(str(k), None)
                new_version = digest(new_map)
            else:
                if version is None:
                    raise ValueError(f"Source {name!r} requires version=")
                new_version = str(version)
                new_map = current
            if head is not None and head["ref"]["version"] == new_version and new_map == current:
                return {"changed": False, "ref": head["ref"]}
            ref = dict(source["head"])
            ref["version"] = new_version
            meta = dict(ref.get("meta") or {})
            meta["external"] = True
            if keyed:
                upserted = {k: r for k, r in new_map.items() if current.get(k) != r}
                deleted = sorted(set(current) - set(new_map))
                delta = {
                    "batch": batch,
                    "rows": len(upserted) + len(deleted),
                    "upserted": upserted,
                }
                if deleted:
                    delta["deleted"] = deleted
                if keys is not None:
                    delta["reset"] = True  # a full-map commit supersedes the log
                path = delta_path(name, "", batch)
                await self.state.put_object(
                    path, json.dumps(delta, sort_keys=True, allow_nan=False).encode()
                )
                meta["delta"] = {
                    "object": path,
                    "batch": batch,
                    "rows": delta["rows"],
                }
                meta["partitions"] = sorted(new_map)
            ref["meta"] = meta
            commit_id = uuid.uuid4().hex
            record = {
                "id": commit_id,
                "attempt": None,
                "task": None,
                "run": None,
                "asset": None,
                "scope": "",
                "at": self.clock(),
                "input_refs": {},
                "outputs": {name: ref},
                "changed": [name],
                "source": name,
            }
            await tx.put_commit(commit_id, record)
            await tx.put_head(
                name,
                "",
                {
                    "ref": ref,
                    "commit": commit_id,
                    "at": self.clock(),
                    "complete": True,
                    "asset": None,
                    "version": None,
                },
            )
            for _, auto in await tx.automations():
                watched = set(auto.get("watched") or [])
                if auto["enabled"] and auto["trigger"]["kind"] == "onchange" and name in watched:
                    auto.setdefault("pending", []).append(
                        {"commit": commit_id, "asset": None, "scope": "", "outputs": [name]}
                    )
                    await tx.put_automation(auto["name"], auto)
            return {"changed": True, "ref": ref, "commit": commit_id}

    # -- automations (§9) ------------------------------------------------------------

    async def _automation_tick(self):
        fired = []
        async with self.state.transaction() as tx:
            now = self.clock()
            for _, auto in await tx.automations():
                if not auto["enabled"]:
                    continue
                trigger = auto["trigger"]
                if trigger["kind"] == "every":
                    due = auto["last_at"] is None or now >= auto["last_at"] + trigger["seconds"]
                    if due:
                        fired.append((auto, auto.get("partitions") or "latest"))
                        auto["last_at"] = now
                        await tx.put_automation(auto["name"], auto)
                elif trigger["kind"] == "cron":
                    zone = ZoneInfo(trigger.get("timezone") or "UTC")
                    last = auto["last_at"] or 0
                    base = dt.datetime.fromtimestamp(last, zone)
                    nxt = croniter(trigger["expression"], base).get_next(dt.datetime)
                    if nxt.timestamp() <= now:
                        fired.append((auto, auto.get("partitions") or "latest"))
                        auto["last_at"] = now
                        await tx.put_automation(auto["name"], auto)
                elif trigger["kind"] == "onchange" and auto.get("pending"):
                    fired.append((auto, {"__pending__": auto["pending"]}))
                    auto["pending"] = []
                    auto["last_at"] = now
                    await tx.put_automation(auto["name"], auto)
                elif trigger["kind"] == "ondeploy":
                    revision = self.manifest["revision"]
                    if auto.get("last_revision") != revision:
                        fired.append((auto, {"__ondeploy__": revision}))
        for auto, selection in fired:
            if isinstance(selection, dict) and "__pending__" in selection:
                await self._fire_onchange(auto, selection["__pending__"])
            elif isinstance(selection, dict) and "__ondeploy__" in selection:
                await self._fire_ondeploy(auto, selection["__ondeploy__"])
            else:
                await self._fire(auto, selection)

    async def _fire(self, auto, partitions):
        try:
            run = await self.submit(
                auto["targets"],
                partitions=partitions,
                mode=auto.get("mode") or "incremental",
                upstream=auto.get("upstream") or False,
                config=auto.get("config"),
                keys=auto.get("keys"),
                automation=auto["name"],
                skip_active=True,
            )
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
            return
        if run is None:
            return
        async with self.state.transaction() as tx:
            record = await tx.automation(auto["name"])
            if record:
                record["last_run"] = run["id"]
                await tx.put_automation(auto["name"], record)

    async def _fire_ondeploy(self, auto, revision):
        """§9: fire once for the served revision, then record it. A submit
        error leaves last_revision unset so the next tick retries."""

        try:
            run = await self.submit(
                auto["targets"],
                partitions=auto.get("partitions") or "latest",
                mode=auto.get("mode") or "incremental",
                upstream=auto.get("upstream") or False,
                config=auto.get("config"),
                keys=auto.get("keys"),
                automation=auto["name"],
                skip_active=True,
            )
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
            return
        async with self.state.transaction() as tx:
            record = await tx.automation(auto["name"])
            if record:
                record["last_revision"] = revision
                record["last_at"] = self.clock()
                if run is not None:
                    record["last_run"] = run["id"]
                await tx.put_automation(auto["name"], record)

    async def _fire_onchange(self, auto, pending):
        """Project each changed upstream scope to the target's scopes (§7, §9)."""

        per_asset = {}
        for event in pending:
            producer = event["asset"]
            if producer is None:
                for target in auto["targets"]:
                    per_asset.setdefault(target, set()).add("")
                continue
            for target in auto["targets"]:
                t_dims = self._dims(target)
                if not t_dims:
                    per_asset.setdefault(target, set()).add("")
                    continue
                pinned = self._project_downstream(producer, event["scope"], target)
                # target-only dims expand over their current key sets
                free = {n: d for n, d in t_dims.items() if n not in pinned}
                async with self.state.transaction() as tx:
                    dim_keys = await self._dim_keys(tx, free)
                missing_dims = list(free)
                combos = product(*dim_keys) if dim_keys else [()]
                for combo in combos:
                    merged = dict(pinned)
                    merged.update(dict(zip(missing_dims, combo, strict=True)))
                    per_asset.setdefault(target, set()).add(canonical_partition(t_dims, merged))
        for target, scopes in per_asset.items():
            if not scopes:
                continue
            try:
                run = await self.submit(
                    [target],
                    partitions=sorted(scopes),
                    mode=auto.get("mode") or "incremental",
                    upstream=auto.get("upstream") or False,
                    config=auto.get("config"),
                    keys=auto.get("keys"),
                    automation=auto["name"],
                    skip_active=True,
                )
                if run is not None:
                    async with self.state.transaction() as tx:
                        record = await tx.automation(auto["name"])
                        if record:
                            record["last_run"] = run["id"]
                            await tx.put_automation(auto["name"], record)
            except Exception as error:
                self.last_error = f"automation {auto['name']}: {error}"

    async def set_automation(self, name: str, enabled: bool):
        async with self.state.transaction() as tx:
            auto = await tx.automation(name)
            if auto is None:
                raise KeyError(name)
            auto["enabled"] = bool(enabled)
            await tx.put_automation(name, auto)
            return auto

    async def run_automation(self, name: str):
        async with self.state.transaction() as tx:
            auto = await tx.automation(name)
            if auto is None:
                raise KeyError(name)
        await self._fire(auto, auto.get("partitions") or "latest")
        async with self.state.transaction() as tx:
            return await tx.automation(name)

    # -- run control ------------------------------------------------------------------

    async def _run(self, run_id):
        async with self.state.transaction() as tx:
            return await tx.run(run_id)

    async def cancel(self, run_id: str):
        async with self.state.transaction() as tx:
            run = await tx.run(run_id)
            if run is None:
                raise KeyError(run_id)
            run["status"] = "canceled"
            run["updated_at"] = self.clock()
            for task_id in run["tasks"]:
                task = await tx.task(task_id)
                if task is None:
                    continue
                if task["status"] in {"queued", "waiting"}:
                    task["status"] = "canceled"
                    await tx.put_task(task)
                    await tx.del_pending(task)
                    await tx.put_scope_outcome(task["asset"], task["scope"], "canceled")
                elif task["status"] == "running":
                    # Fence the attempt: its next renew raises LostOwnership and
                    # the placement loop cancels the run (§8).
                    lock = await tx.lock(task["asset"], task["scope"])
                    await tx.del_lock(task["asset"], task["scope"])
                    task["status"] = "canceled"
                    await tx.put_task(task)
                    await tx.del_pending(task)
                    await tx.put_scope_outcome(
                        task["asset"], task["scope"], "canceled", (lock or {}).get("attempt")
                    )
            await tx.put_run(run)
            return run

    async def pause(self, run_id: str, paused=True):
        async with self.state.transaction() as tx:
            run = await tx.run(run_id)
            if run is None:
                raise KeyError(run_id)
            run["paused"] = bool(paused)
            await tx.put_run(run)
            return run

    async def retry(self, run_id: str):
        async with self.state.transaction() as tx:
            run = await tx.run(run_id)
            if run is None:
                raise KeyError(run_id)
            for task_id in run["tasks"]:
                task = await tx.task(task_id)
                if task and task["status"] in {"failed", "blocked"}:
                    task["status"] = "queued"
                    task["error"] = None
                    task["ready_at"] = self.clock()
                    await tx.enqueue(task["id"], task["ready_at"])
                    await tx.put_task(task)
                    await tx.put_pending(task)
            run["status"] = "running"
            run["paused"] = False
            await tx.put_run(run)
            await self.state.advance_run(tx, run_id)
            return run

    # -- read models (Phase 5 will shape these for the API) ----------------------------

    async def list_runs(self, limit=50):
        async with self.state.transaction() as tx:
            return await tx.runs(limit)

    async def run_detail(self, run_id: str):
        async with self.state.transaction() as tx:
            run = await tx.run(run_id)
            if run is None:
                raise KeyError(run_id)
            tasks = [await tx.task(tid) for tid in run["tasks"]]
            attempts = {}
            for task in tasks:
                if task:
                    attempts[task["id"]] = [a for _, a in await tx.attempts(task["id"])]
            return {"request": run, "tasks": tasks, "attempts": attempts}

    async def asset_detail(self, name: str, scope=""):
        asset = self._asset_of(name)
        info = self.manifest["assets"][asset]
        async with self.state.transaction() as tx:
            heads = {}
            for output in info["outputs"]:
                heads[output["name"]] = await tx.heads(output["name"])
            cursor = await tx.cursor(asset, scope)
            watermarks = {}
            for param, edge in info["inputs"].items():
                if edge["kind"] == "incremental":
                    watermarks[param] = await tx.watermark(asset, param, scope)
            dims = self._dims(asset)
            current = await self._dim_keys(tx, dims) if dims else []
            outcomes = await tx.scope_outcomes(asset)
        return {
            "asset": info,
            "heads": heads,
            "cursor": cursor,
            "watermarks": watermarks,
            "current_keys": current,
            "scopes": outcomes,
        }

    async def catalog(self):
        async with self.state.transaction() as tx:
            heads = await tx.all_heads()
        by_output = {}
        for key, head in heads:
            _, output, scope = key.split("/", 2)
            by_output.setdefault(output, {})[scope] = head
        assets = []
        for name, info in self.manifest["assets"].items():
            assets.append(
                {
                    **info,
                    "name": name,
                    "heads": {o["name"]: by_output.get(o["name"], {}) for o in info["outputs"]},
                }
            )
        return assets
