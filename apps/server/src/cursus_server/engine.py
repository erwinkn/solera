"""The engine (§6–§10): control-plane only. It plans runs into per-(asset,
scope) tasks, resolves inputs to pinned heads, plans Incremental edges over
per-edge watermarks, dispatches attempts through placements, and commits
their results.

State lives in the model (model.py), changed only by events the engine emits
(docs/object-store-state.md §4). A precondition check and the event that
depends on it happen in one synchronous step, so no other coroutine can
interleave between them. Claims, leases and pool work are memory only.

Structure lives in the manifest, state lives in the spec, effects live in the
result — the engine never interprets a payload.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
from itertools import product
from zoneinfo import ZoneInfo

from croniter import croniter
from cursus.ids import ulid
from cursus.sdk import TimePartitions, canonical_partition, digest, split_partition
from cursus.stores import delta_path, next_batch

from .model import TERMINAL_RUN
from .placements import PlacementContext, Registry
from .state import Conflict, LostOwnership, State

log = logging.getLogger(__name__)

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
    ):
        import time

        if lease_seconds < 1 or concurrency < 1:
            raise ValueError("Lease duration and concurrency must be positive")
        self.state, self.manifest = state, manifest
        self.project = project
        self.clock = clock or time.time
        self.lease_seconds, self.concurrency = lease_seconds, concurrency
        self.eval_interval = eval_interval
        ctx = PlacementContext(state, state.objects_url, project, self.clock)
        self.registry = registry or Registry(ctx, extra=placements)
        # attempt id -> (run id, asyncio task): attempts this process is driving.
        self.inflight: dict[str, tuple[str, asyncio.Task]] = {}
        # Live placement handles per attempt.
        self.handles: dict[str, dict] = {}
        # Attempts consuming a local execution slot. Pool attempts only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.env_inflight: dict[str, int] = {}
        self.runner: asyncio.Task | None = None
        self.last_error = None
        self._stopping = False
        self._firing: set[str] = set()

    @property
    def m(self):
        return self.state.model

    # -- lifecycle ---------------------------------------------------------------

    async def initialize(self):
        """Register the served project (§11): a new manifest reconciles
        automation state and seeds source heads."""

        m = self.m
        if (
            m.revision != self.manifest["revision"]
            or m.manifest != self.manifest
            or m.project != self.project
        ):
            await self.state.emit(
                {
                    "type": "ProjectRegistered",
                    "revision": self.manifest["revision"],
                    "manifest": self.manifest,
                    "project": self.project,
                    "at": self.clock(),
                }
            )

    async def start(self):
        """Start the eval loop. Work in flight before a restart holds no claim
        now — its tasks are queued again and the dispatch path relaunches them."""

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
            await asyncio.gather(*(t for _, t in self.inflight.values()), return_exceptions=True)
            self.inflight.clear()

    async def _loop(self):
        while not self._stopping:
            try:
                await self.tick()
                self.last_error = None
            except Exception as error:
                self.last_error = f"{type(error).__name__}: {error}"
                log.exception("engine tick failed")
            await asyncio.sleep(self.eval_interval)

    async def tick(self):
        """One evaluation pass: expired leases, dispatch, automations, archiving."""

        await self._sweep_leases()
        self._sweep_pool()
        await self._dispatch_due()
        await self._automation_tick()
        await self._archive_due()

    async def run_until(self, run_id: str, timeout: float = 120.0):
        """Tick until the run reaches a terminal status (CLI and tests)."""

        deadline = self.clock() + timeout
        while self.clock() < deadline:
            await self.tick()
            run = self.m.runs.get(run_id) or await self.state.archived(run_id)
            if run and run["status"] in TERMINAL:
                # Wait only on this run's attempts — unrelated pool-placed runs
                # may hold inflight waiters until a worker claims them.
                mine = [t for r, t in self.inflight.values() if r == run_id]
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
        m = self.m
        if command_id and command_id in m.receipts:
            return await self._run_view_of(m.receipts[command_id])
        assets = {}
        for target in targets:
            name = self._asset_of(target)
            assets[name] = await self._scopes(name, partitions)
        if upstream:
            queue = [(n, s) for n, scopes in assets.items() for s in scopes]
            seen = set(queue)
            while queue:
                name, scope = queue.pop()
                for owner, up_scope in await self._upstream_of(name, scope):
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
                assets[name] = [s for s in assets[name] if not self._scope_active(name, s)]
            if not any(assets.values()):
                return None  # §9: the tick is skipped — every scope is in flight
        now = self.clock()
        run_id = ulid(now)
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
                    "max_attempts": 1 + self.manifest["assets"][name].get("retries", {}).get("n", 0),
                    "retry": self.manifest["assets"][name].get("retries"),
                    "ready_at": now,
                    "attempts": [],
                }
        for task_id, task in tasks.items():
            for owner, up_scope in await self._upstream_of(task["asset"], task["scope"]):
                dep_id = f"{run_id}/{owner}:{up_scope}"
                if dep_id in tasks and dep_id != task_id:
                    task["deps"].append(dep_id)
                    task["status"] = "waiting"
        run = {
            "id": run_id,
            "targets": sorted(assets),
            "partitions": partitions if isinstance(partitions, str) else list(partitions),
            "mode": mode,
            "upstream": bool(upstream),
            "config": config,
            "keys": keys,
            "automation": automation,
            "status": "running",
            "paused": False,
            "created_at": now,
            "updated_at": now,
            "tasks": tasks,
        }
        if command_id and command_id in m.receipts:  # submitted while we planned
            return await self._run_view_of(m.receipts[command_id])
        await self.state.emit({"type": "RunSubmitted", "run": run, "command": command_id})
        return self._run_view(m.runs.get(run_id) or run)

    def _scope_active(self, asset: str, scope: str) -> bool:
        attempt = self.m.locks.get((asset, scope))
        if attempt is not None:
            claim = self.m.claimed(attempt)
            if claim is not None and claim["lease_until"] > self.clock():
                return True
        return self.m.is_pending(asset, scope)

    def _asset_of(self, name: str) -> str:
        if name in self.manifest["assets"]:
            return name
        if name in self.manifest["outputs"] and self.manifest["outputs"][name].get("asset"):
            return self.manifest["outputs"][name]["asset"]
        raise ValueError(f"Unknown asset or output: {name!r}")

    async def _upstream_of(self, asset: str, scope: str):
        """(owner_asset, upstream_scope) for every edge, dep, and partition-set
        dimension of (asset, scope) — a bound key set is a pinned dep (§7)."""

        info = self.manifest["assets"][asset]
        edges = list(info["inputs"].values()) + [{"kind": "dep", "output": d} for d in info["deps"]]
        out = []
        for edge in edges:
            owner = self.manifest["outputs"][edge["output"]].get("asset")
            up_dims = self._dims(owner) if owner else {}
            if edge["kind"] in {"all_partitions", "dep"} and owner is not None:
                out.extend((owner, s) for s in await self._spread(info, scope, up_dims))
                continue
            out.append((owner, self._project(info, scope, up_dims)))
        for dim in self._dims(asset).values():
            if dim["kind"] == "set":
                owner = self.manifest["outputs"][dim["output"]].get("asset")
                if owner is not None:
                    out.append((owner, ""))
        return out

    async def _spread(self, consumer: dict, consumer_scope: str, upstream_dims: dict):
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
        keys = await self._dim_keys(free)
        return [
            canonical_partition(upstream_dims, {**pinned, **dict(zip(free, combo, strict=True))})
            for combo in product(*keys)
        ]

    async def _scopes(self, asset: str, selection) -> list[str]:
        """`partitions` selects keys from the current set (§7, §8)."""

        dims = self._dims(asset)
        if not dims:
            current = [""]
        else:
            keys = await self._dim_keys(dims)
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
            keys = await self._dim_keys(dims)
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
            outputs = self.manifest["assets"][asset]["outputs"]
            for scope in current:
                heads = [self.m.heads.get((o["name"], scope)) for o in outputs]
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

    async def _dim_keys(self, dims: dict) -> list[list[str]]:
        out = []
        for dim in dims.values():
            if dim["kind"] == "static":
                out.append([str(k) for k in dim["keys"]])
            elif dim["kind"] == "time":
                out.append(self._time(dim).keys(self._now()))
            else:
                keys = await self._head_keys(self.m.heads.get((dim["output"], "")))
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

    # -- leases (memory only, §4.3) ------------------------------------------------------

    async def _sweep_leases(self):
        """An attempt whose scope lease expired loses its claim; its task is
        queued again (§8). A harness still running cannot commit any more."""

        now = self.clock()
        for task_id, claim in list(self.m.claims.items()):
            if claim["lease_until"] > now:
                continue
            task = self.m.task(task_id)
            if task is None:
                continue
            await self._finish(task, claim, "expired", error="lease expired")

    def _sweep_pool(self):
        """Expired pool claims return to queued so another worker can take them."""

        now = self.clock()
        for record in self.m.pool.values():
            if (
                record["status"] == "claimed"
                and record["lease_until"] is not None
                and record["lease_until"] <= now
            ):
                record["status"] = "queued"
                record["claimed_by"] = None
                record["lease_until"] = None

    def _renew(self, attempt: str) -> float:
        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)
        claim["lease_until"] = self.clock() + self.lease_seconds
        return claim["lease_until"]

    async def _finish(
        self,
        task: dict,
        claim: dict,
        outcome: str,
        *,
        error=None,
        retryable=False,
        delay=0.0,
        commit=None,
        more=False,
    ):
        event = {
            "type": "AttemptFinished",
            "run": task["run"],
            "task": task["id"],
            "attempt": claim["attempt"],
            "outcome": outcome,
            "started_at": claim["started_at"],
            "finished_at": self.clock(),
        }
        if error is not None:
            event["error"] = str(error)[-8000:]
            event["retryable"] = bool(retryable)
            if delay:
                event["delay"] = float(delay)
        if commit is not None:
            event["commit"] = commit
        if more:
            event["more"] = True
        await self.state.emit(event)

    async def fail_attempt(self, attempt: str, error: str, *, retryable: bool, delay: float = 0):
        """Mark the running attempt failed; retryable failures requeue by policy."""

        claim = self.m.claimed(attempt)
        if claim is None:
            return
        task = self.m.task(self.m.attempts[attempt])
        await self._finish(task, claim, "failed", error=error, retryable=retryable, delay=delay)

    # -- dispatch ---------------------------------------------------------------

    async def _dispatch_due(self):
        now = self.clock()
        engine_used = len(self.engine_inflight)
        env_used = dict(self.env_inflight)
        for task_id in self.m.due(now):
            task = self.m.task(task_id)
            if task is None or task["status"] != "queued" or task_id in self.m.claims:
                continue
            run = self.m.runs[self.m.task_run[task_id]]
            if run["status"] == "canceled" or run.get("paused"):
                continue
            spec = self.manifest["assets"][task["asset"]]["placement"]
            placement = self.registry.build(spec)
            env_key = self.registry.env_key(spec)
            limit = getattr(placement, "max_concurrent", None)
            is_pool = spec["kind"] == "Pool"
            if not is_pool and engine_used >= self.concurrency:
                continue
            if limit is not None and env_used.get(env_key, 0) >= limit:
                continue
            if self._scope_active_claim(task["asset"], task["scope"]):
                continue
            attempt = ulid(now)
            self.m.claim(task_id, attempt, now, self.lease_seconds)
            env_used[env_key] = env_used.get(env_key, 0) + 1
            if not is_pool:
                engine_used += 1
                self.engine_inflight.add(attempt)
            self.env_inflight[env_key] = self.env_inflight.get(env_key, 0) + 1
            job = asyncio.create_task(self._attempt(task_id, attempt, placement, env_key))
            self.inflight[attempt] = (task["run"], job)
            job.add_done_callback(lambda _t, a=attempt: self.inflight.pop(a, None))

    def _scope_active_claim(self, asset: str, scope: str) -> bool:
        attempt = self.m.locks.get((asset, scope))
        return attempt is not None and self.m.claimed(attempt) is not None

    async def _attempt(self, task_id: str, attempt: str, placement, env_key: str):
        """One attempt, start to finish: prepare, skip or launch, wait, commit.
        Every path ends in exactly one AttemptFinished (or a lost claim)."""

        try:
            task = self.m.task(task_id)
            claim = self.m.claimed(attempt)
            if task is None or claim is None:
                return
            run = self.m.runs[task["run"]]
            try:
                prepared = await self._prepare(task, run)
            except (Retryable, NonRetryable, Conflict) as error:
                if self.m.claimed(attempt) is not None:
                    await self._finish(
                        task,
                        claim,
                        "failed",
                        error=error,
                        retryable=not isinstance(error, NonRetryable) and getattr(error, "retryable", True),
                    )
                return
            if self.m.claimed(attempt) is None:
                return
            if prepared.get("skip"):
                await self._finish(
                    task, claim, "skipped", commit={"watermarks": prepared["watermark_updates"]}
                )
                return
            await self._execute(task, run, attempt, prepared, placement)
        except LostOwnership:
            return
        except Exception as error:  # never leave a claim behind
            log.exception("attempt %s failed unexpectedly", attempt)
            claim = self.m.claimed(attempt)
            task = self.m.task(task_id)
            if claim is not None and task is not None:
                with contextlib.suppress(Exception):
                    await self._finish(task, claim, "failed", error=f"engine: {error}", retryable=True)
        finally:
            self.engine_inflight.discard(attempt)
            self.env_inflight[env_key] = max(0, self.env_inflight.get(env_key, 1) - 1)

    # -- input resolution + Incremental plans (§5, §6, §8) --------------------------

    async def _prepare(self, task: dict, run: dict) -> dict:
        """Pin heads at attempt start; plan Incremental edges; decide skip (§8)."""

        asset = self.manifest["assets"][task["asset"]]
        scope = task["scope"]
        full = run["mode"] == "full"
        baseline = {}
        for output in asset["outputs"]:
            head = self.m.heads.get((output["name"], scope))
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
                refs = await self._all_partitions(task, asset, up_dims, output)
                inputs[param] = {"refs": refs}
                pinned[param] = refs
                continue
            if edge["kind"] == "dep":
                refs = {}
                for s in await self._spread(asset, scope, up_dims):
                    refs[s] = self._pin_at(output, s)
                inputs[param] = {"refs": refs}
                if not edge.get("set_dim"):
                    pinned[param] = refs
                continue
            if edge["kind"] == "incremental":
                incremental.append((param, edge, up_dims))
                continue
            inputs[param] = {"ref": self._pin(output, up_dims, asset, scope)}
            pinned[param] = inputs[param]["ref"]
        fingerprint = self._fingerprint(asset, run, pinned)
        # Pass 2: Incremental plans against the fingerprinted interpretation (§2.2).
        watermark_updates, all_empty = {}, True
        for param, edge, up_dims in incremental:
            ref = self._pin(edge["output"], up_dims, asset, scope)
            pin, update, empty = await self._incremental_plan(task, param, edge, ref, fingerprint, run, full)
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
        cursor = self.m.cursors.get((task["asset"], scope))
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

    def _pin(self, output: str, up_dims: dict, asset: dict, scope: str):
        """Resolve one edge to its head ref; sources synthesize theirs (§5, §8)."""

        return self._pin_at(output, self._project(asset, scope, up_dims))

    def _pin_at(self, output: str, up_scope: str):
        """Head ref for one upstream scope; sources synthesize theirs (§5, §8)."""

        head = self.m.heads.get((output, up_scope))
        if head is None:
            source = self.manifest["sources"].get(output)
            if source is not None and up_scope == "":
                return dict(source["head"])
            raise Retryable(f"input {output!r} has no head for scope {up_scope!r}")
        return head["ref"]

    async def _all_partitions(self, task, asset, up_dims, output) -> dict:
        """AllPartitions pins every upstream key with a complete head (§7)."""

        parts = split_partition(self._dims_of(asset), task["scope"]) if self._dims_of(asset) else {}
        collapsed = {}
        for name, dim in up_dims.items():
            shared = any(self._same_dim(dim, c) for c in self._dims_of(asset).values())
            if not shared:
                collapsed[name] = dim
        if not collapsed:
            head = self.m.heads.get((output, self._project(asset, task["scope"], up_dims)))
            return {"": head["ref"]} if head is not None and head["complete"] else {}
        keys = await self._dim_keys(collapsed)
        refs = {}
        for combo in product(*keys):
            collapsed_parts = dict(zip(collapsed, combo, strict=True))
            full = dict(collapsed_parts)
            for name, dim in up_dims.items():
                if name not in collapsed:
                    for c_name, c_dim in self._dims_of(asset).items():
                        if self._same_dim(dim, c_dim):
                            full[name] = parts[c_name]
            head = self.m.heads.get((output, canonical_partition(up_dims, full)))
            if head is not None and head["complete"]:
                refs[canonical_partition(collapsed, collapsed_parts)] = head["ref"]
        return refs

    async def _incremental_plan(self, task, param, edge, ref, fingerprint, run, full):
        """Plan one Incremental edge: the pending delta items after the edge's
        watermark, capped at `batch_size` (§2.2).

        watermark = {"batch", "offset", "fingerprint"}: every item of batches
        below `batch` is delivered, plus the first `offset` items of `batch`
        (each delta's own upserted|deleted keys sorted). `batch=-1` marks a
        reset drain: `offset` keys of the folded live map are delivered. A
        missing watermark, a fingerprint change, a `full` run, or a keys=
        'full' override starts (or restarts) the drain. Returns (pin,
        watermark_update, empty); the update carries `more` while pending
        items remain after the take."""

        decl = self.manifest["outputs"][edge["output"]]
        keyed = decl.get("key") is not None
        batch_size = int(edge.get("batch_size") or 100)
        head_batch = int(((ref.get("meta") or {}).get("delta") or {}).get("batch", -1))
        up_scope = ref.get("partition") or ""
        override = (run.get("keys") or {}).get(edge["output"])
        wm = self.m.watermarks.get((task["asset"], param, task["scope"]))
        draining = wm is not None and wm["batch"] == -1
        reset = full or wm is None or draining or wm.get("fingerprint") != fingerprint or override == "full"

        # A keys= override is a one-off selection — it never moves the watermark.
        if isinstance(override, dict) and "keys" in override and not reset:
            keys = [str(k) for k in override["keys"]]
            pin = {
                "ref": ref,
                "changes": {"upserted": {k: "" for k in keys}, "deleted": [], "full": False},
            }
            return pin, None, not keys

        if reset and keyed:
            # Fold the whole log: the live map, plus every key the log ever
            # deleted (the consumer may hold them; removes are idempotent).
            live, ever_deleted = {}, set()
            for b in range(0, head_batch + 1):
                d = await self.state.delta(edge["output"], up_scope, b)
                if d is None:
                    continue
                for k in d.get("deleted") or []:
                    live.pop(str(k), None)
                    ever_deleted.add(str(k))
                live.update({str(k): str(v) for k, v in (d.get("upserted") or {}).items()})
            offset = wm["offset"] if draining else 0
            ordered = sorted(live)
            take = ordered[offset : offset + batch_size]
            deleted = [] if draining else sorted(ever_deleted - set(live))
            done = offset + len(take) >= len(ordered)
            pin = {
                "ref": ref,
                "changes": {
                    "upserted": {k: live[k] for k in take},
                    "deleted": deleted,
                    "full": True,
                },
            }
            update = {
                "batch": head_batch + 1 if done else -1,
                "offset": 0 if done else offset + len(take),
                "fingerprint": fingerprint,
                "more": not done,
            }
            return pin, update, not take and not deleted

        if reset:  # batch-mode: pending is the batch range after the last reset
            deltas = []
            for b in range(0, head_batch + 1):
                d = await self.state.delta(edge["output"], up_scope, b)
                if d is not None:
                    deltas.append(d)
            resets = [i for i, d in enumerate(deltas) if d.get("reset")]
            if resets:
                deltas = deltas[resets[-1] :]
            pending = [d["batch"] for d in deltas]
            take = pending[:batch_size]
            pin = {
                "ref": ref,
                "changes": {
                    "batches": [take[0], take[-1]] if take else [0, -1],
                    "full": True,
                },
            }
            update = {
                "batch": (take[-1] + 1) if take else head_batch + 1,
                "offset": 0,
                "fingerprint": fingerprint,
                "more": len(pending) > len(take),
            }
            return pin, update, not take

        # Incremental: pending items sit in deltas [wm.batch .. head_batch],
        # skipping the first wm.offset items of wm.batch.
        deltas = []
        for b in range(wm["batch"], head_batch + 1):
            d = await self.state.delta(edge["output"], up_scope, b)
            if d is None:
                # The log was pruned under the watermark — restart the drain (§2.2).
                return await self._incremental_plan(task, param, edge, ref, fingerprint, run, full=True)
            deltas.append(d)

        if not keyed:
            # Batch-mode upstream: every pending batch is one item; a reset
            # batch supersedes everything before it.
            resets = [i for i, d in enumerate(deltas) if d.get("reset")]
            if resets:
                deltas = deltas[resets[-1] :]
            pending = [d["batch"] for d in deltas]
            take = pending[:batch_size]
            changes = {"batches": [take[0], take[-1]] if take else [0, -1], "full": False}
            pin = {"ref": ref, "changes": changes}
            update = {
                "batch": (take[-1] + 1) if take else wm["batch"],
                "offset": 0,
                "fingerprint": fingerprint,
                "more": len(pending) > len(take),
            }
            return pin, update, not take

        latest = {}
        for d in deltas:
            # Pure diffs, last writer wins: a reset batch's `deleted` already
            # lists every key it dropped.
            upserted = d.get("upserted") or {}
            deleted = set(d.get("deleted") or [])
            for i, key in enumerate(sorted(set(upserted) | deleted)):
                if d["batch"] == wm["batch"] and i < wm["offset"]:
                    continue
                latest[key] = (d["batch"], i, "del" if key in deleted else "upsert")
        pending = sorted(latest.items(), key=lambda kv: kv[1])
        take, rest = pending[:batch_size], pending[batch_size:]
        delivered_ups, delivered_del = {}, []
        for key, (b, _i, kind) in take:
            if kind == "del":
                delivered_del.append(key)
            else:
                delivered_ups[key] = str((self._delta_upsert(deltas, b) or {}).get(key, ""))
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

    @staticmethod
    def _delta_upsert(deltas, batch):
        for d in deltas:
            if d["batch"] == batch:
                return d.get("upserted") or {}
        return {}

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

    async def _execute(self, task, run, attempt, prepared, placement):
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
        await self.state.put_object(f"specs/{attempt}.json", json.dumps(spec).encode())
        if spec["execution"]["kind"] == "Pool":
            self._stage_pool(task, attempt, spec)
        try:
            handle = await placement.launch({"attempt": attempt, "objects": self.state.objects_url})
        except Exception as error:
            await self.fail_attempt(attempt, f"launch: {error}", retryable=True)
            return
        self.handles[attempt] = handle
        try:
            await self._wait_loop(task, attempt, prepared, placement, handle)
        finally:
            self.handles.pop(attempt, None)

    async def _wait_loop(self, task, attempt, prepared, placement, handle):
        timeout_s = self.manifest["assets"][task["asset"]].get("timeout") or 3600
        deadline = self.clock() + timeout_s
        while True:
            polled = self.clock()
            try:
                exit_ = await placement.wait(handle, self.lease_seconds / 3)
            except Exception as error:
                await self.fail_attempt(attempt, f"wait: {error}", retryable=True)
                return
            if exit_ is not None:
                break
            # A placement that returns before its timeout must not spin the loop.
            await asyncio.sleep(max(0.0, 0.05 - (self.clock() - polled)))
            if self.clock() > deadline:
                await self._cancel(placement, handle)
                await self.fail_attempt(attempt, "timeout", retryable=True)
                return
            try:
                self._renew(attempt)
            except LostOwnership:
                await self._cancel(placement, handle)
                return
        result_data = await self.state.get_object(f"results/{attempt}.json")
        if result_data is None:
            await self.fail_attempt(attempt, f"harness exited without a result: {exit_}", retryable=True)
            return
        result = json.loads(result_data)
        if result.get("status") == "failed":
            error = result.get("error") or {}
            await self.fail_attempt(
                attempt,
                f"{error.get('type', 'Error')}: {error.get('message', '')}",
                retryable=bool(error.get("retryable")),
                delay=self._retry_delay(task),
            )
            return
        try:
            await self.commit_attempt(attempt, prepared, result)
        except LostOwnership:
            return
        except Conflict as error:
            await self.fail_attempt(attempt, str(error), retryable=getattr(error, "retryable", True))

    def _retry_delay(self, task) -> float:
        retry = task.get("retry") or {}
        delay = float(retry.get("delay", 1.0))
        if retry.get("backoff") == "exponential":
            failures = sum(1 for a in task["attempts"] if a["outcome"] == "failed")
            delay *= 2**failures
        return delay

    async def _cancel(self, placement, handle):
        with contextlib.suppress(Exception):
            await placement.cancel(handle)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(placement.wait(handle, GRACE_SECONDS), GRACE_SECONDS + 1)

    # -- the commit (§8) ---------------------------------------------------------------

    async def commit_attempt(self, attempt: str, prepared: dict, result: dict) -> dict:
        """Install an attempt's result: heads, cursor, edge watermarks (§8).

        Every precondition is checked against the model, and the event emitted,
        without an await in between."""

        from cursus.sdk import UNSET

        claim = self.m.claimed(attempt)
        if claim is None or claim["lease_until"] <= self.clock():
            raise LostOwnership(attempt)
        task = self.m.task(self.m.attempts[attempt])
        # Pinned inputs must still be the committed heads (§8).
        for name, pin in prepared["inputs"].items():
            refs = pin["refs"].values() if "refs" in pin else [pin["ref"]]
            for ref in refs:
                head = self.m.heads.get((ref["output"], ref["partition"]))
                if head is None or head["ref"]["version"] != ref["version"]:
                    raise Conflict(f"input {name}: {ref['output']}/{ref['partition']} moved after pinning")
        # Output heads must be unchanged since the claim.
        for output, baseline in prepared["baseline"].items():
            if self.m.heads.get((output, task["scope"])) != baseline:
                raise Conflict(f"output {output} head changed since this attempt was claimed")
        outputs = result.get("outputs") or {}
        asset = self.manifest["assets"][task["asset"]]
        declared = {o["name"]: o for o in asset["outputs"]}
        for name, ref in outputs.items():
            if name not in declared:
                raise Conflict(f"result names undeclared output {name!r}", retryable=False)
            if ref["partition"] != task["scope"]:
                raise Conflict(
                    f"output {name}: ref scope {ref['partition']!r} != {task['scope']!r}", retryable=False
                )
            decl = declared[name]
            meta = ref.get("meta") or {}
            baseline_ref = (prepared["baseline"].get(name) or {}).get("ref")
            if baseline_ref is not None and baseline_ref["version"] == ref["version"]:
                continue  # identical content keeps the head as it is
            if decl.get("incremental") and not meta.get("delta"):
                raise Conflict(f"incremental output {name}: ref carries no delta", retryable=False)
            if decl.get("partition_set") and meta.get("partitions") is None:
                raise Conflict(
                    f"partition-set output {name}: ref carries no partitions list", retryable=False
                )
        for name in set(declared) - set(outputs):
            if prepared["baseline"].get(name) is None:
                raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)
        commit = {
            "heads": {
                name: {
                    "ref": ref,
                    "complete": prepared["scope_complete"],
                    "asset": task["asset"],
                    "version": asset["version"],
                }
                for name, ref in outputs.items()
            },
            "watermarks": dict(prepared.get("watermark_updates") or {}),
        }
        if result.get("cursor", UNSET) is not UNSET:
            commit["cursor"] = result["cursor"]
        elif prepared.get("full"):
            commit["cursor"] = None  # a full run clears the committed cursor (§8)
        await self._finish(task, claim, "succeeded", commit=commit, more=bool(prepared.get("more")))
        return {"run": task["run"], "attempt": attempt, "outputs": outputs}

    # -- pool work (§10, memory only) ----------------------------------------------------

    def _stage_pool(self, task: dict, attempt: str, spec: dict):
        placement = spec["execution"]
        self.m.pool[attempt] = {
            "attempt": attempt,
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
        claim = self.m.claims.get(task["id"])
        if claim is not None:
            claim["status"] = "claimable"

    def register_worker(self, worker_id: str, pools: list[str], meta: dict) -> dict:
        record = {"id": worker_id, "pools": pools, "meta": meta, "seen_at": self.clock()}
        self.m.workers[worker_id] = record
        return record

    def claim_pool_task(self, worker_id: str, pools: list[str], capacity: dict, lease_seconds: float):
        """Oldest unclaimed (or expired) pool task in the worker's pools that
        fits the worker's cpu/memory/gpu capacity (§10)."""

        now = self.clock()
        for record in sorted(self.m.pool.values(), key=lambda r: r["created_at"]):
            if record["status"] != "queued" or record["pool"] not in pools:
                continue
            needs = record.get("needs") or {}
            if any(capacity.get(dim) is None or capacity[dim] < want for dim, want in needs.items()):
                continue
            record.update(
                status="claimed", claimed_by=worker_id, claimed_at=now, lease_until=now + lease_seconds
            )
            claim = self.m.claims.get(record["task"])
            if claim is not None:
                claim["status"] = "running"
            return record
        return None

    def heartbeat_pool_task(self, worker_id: str, attempt: str, lease_seconds: float) -> float:
        record = self.m.pool.get(attempt)
        if (
            record is None
            or record["status"] != "claimed"
            or record["claimed_by"] != worker_id
            or record["lease_until"] <= self.clock()
        ):
            raise LostOwnership(attempt)
        record["lease_until"] = self.clock() + lease_seconds
        return record["lease_until"]

    def release_pool_task(self, worker_id: str, attempt: str) -> None:
        record = self.m.pool.get(attempt)
        if record and record["claimed_by"] == worker_id:
            del self.m.pool[attempt]

    # -- sources commit API (§5) ------------------------------------------------------

    async def commit_source(self, name: str, *, version=None, keys=None, upsert=None, remove=None):
        """Advance a source without moving data (§2.3): a keyed source commit
        diffs the supplied map against the delta log's fold and writes the
        same delta object a store would; `meta.partitions` carries the
        committed element list. An identical map is not a change."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        head = self.m.heads.get((name, ""))
        keyed = source.get("key") is not None
        if not keyed and (keys is not None or upsert is not None or remove is not None):
            raise ValueError(f"Source {name!r} is unkeyed; pass version=")
        prior_delta = (((head or {}).get("ref") or {}).get("meta") or {}).get("delta")
        batch = int(prior_delta["batch"]) + 1 if prior_delta else 0
        current = await self.state.delta_key_map(name, "", int(prior_delta["batch"])) if prior_delta else {}
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
            delta = {"batch": batch, "rows": len(upserted) + len(deleted), "upserted": upserted}
            if deleted:
                delta["deleted"] = deleted
            if keys is not None:
                delta["reset"] = True  # a full-map commit supersedes the log
            path = delta_path(name, "", batch)
            await self.state.put_object(path, json.dumps(delta, sort_keys=True, allow_nan=False).encode())
            meta["delta"] = {"object": path, "batch": batch, "rows": delta["rows"]}
            meta["partitions"] = sorted(new_map)
        ref["meta"] = meta
        if self.m.heads.get((name, "")) != head:
            raise Conflict(f"source {name!r} moved while committing; retry")
        await self.state.emit(
            {
                "type": "SourceCommitted",
                "source": name,
                "head": {
                    "ref": ref,
                    "run": None,
                    "attempt": None,
                    "complete": True,
                    "asset": None,
                    "version": None,
                },
                "at": self.clock(),
            }
        )
        return {"changed": True, "ref": ref, "commit": f"source/{name}/{batch}"}

    # -- automations (§9) ------------------------------------------------------------

    async def _automation_tick(self):
        now = self.clock()
        fired = []
        for auto in list(self.m.automations.values()):
            if not auto["enabled"] or auto["name"] in self._firing:
                continue
            trigger = auto["trigger"]
            if trigger["kind"] == "every":
                if auto["last_at"] is None or now >= auto["last_at"] + trigger["seconds"]:
                    fired.append((auto, "schedule"))
            elif trigger["kind"] == "cron":
                zone = ZoneInfo(trigger.get("timezone") or "UTC")
                base = dt.datetime.fromtimestamp(auto["last_at"] or 0, zone)
                if croniter(trigger["expression"], base).get_next(dt.datetime).timestamp() <= now:
                    fired.append((auto, "schedule"))
            elif trigger["kind"] == "onchange":
                if auto["pending"]:
                    fired.append((auto, "onchange"))
            elif trigger["kind"] == "ondeploy":
                if auto.get("last_revision") != self.manifest["revision"]:
                    fired.append((auto, "ondeploy"))
        for auto, why in fired:
            self._firing.add(auto["name"])
            try:
                if why == "onchange":
                    await self._fire_onchange(auto)
                elif why == "ondeploy":
                    await self._fire_ondeploy(auto)
                else:
                    await self._fire(auto, auto.get("partitions") or "latest")
            finally:
                self._firing.discard(auto["name"])

    async def _submit_for(self, auto, partitions, targets=None):
        return await self.submit(
            targets or auto["targets"],
            partitions=partitions,
            mode=auto.get("mode") or "incremental",
            upstream=auto.get("upstream") or False,
            config=auto.get("config"),
            keys=auto.get("keys"),
            automation=auto["name"],
            skip_active=True,
        )

    async def _fire(self, auto, partitions):
        run = None
        try:
            run = await self._submit_for(auto, partitions)
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
        await self.state.emit(
            {"type": "AutomationFired", "name": auto["name"], "at": self.clock(), "run": run and run["id"]}
        )

    async def _fire_ondeploy(self, auto):
        """§9: fire once for the served revision, then record it. A submit
        error leaves last_revision unset so the next tick retries."""

        try:
            run = await self._submit_for(auto, auto.get("partitions") or "latest")
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
            return
        await self.state.emit(
            {
                "type": "AutomationFired",
                "name": auto["name"],
                "at": self.clock(),
                "run": run and run["id"],
                "revision": self.manifest["revision"],
            }
        )

    async def _fire_onchange(self, auto):
        """Project each changed upstream scope to the target's scopes (§7, §9),
        then drop the consumed changes from the pending set."""

        consumed = [list(p) for p in auto["pending"]]
        per_asset = {}
        for producer, scope in consumed:
            for target in auto["targets"]:
                t_dims = self._dims(target)
                if producer is None or not t_dims:
                    per_asset.setdefault(target, set()).add("")
                    continue
                pinned = self._project_downstream(producer, scope, target)
                # target-only dims expand over their current key sets
                free = {n: d for n, d in t_dims.items() if n not in pinned}
                dim_keys = await self._dim_keys(free)
                missing_dims = list(free)
                for combo in product(*dim_keys) if dim_keys else [()]:
                    merged = dict(pinned)
                    merged.update(dict(zip(missing_dims, combo, strict=True)))
                    per_asset.setdefault(target, set()).add(canonical_partition(t_dims, merged))
        last_run = None
        for target, scopes in per_asset.items():
            if not scopes:
                continue
            try:
                run = await self._submit_for(auto, sorted(scopes), targets=[target])
                if run is not None:
                    last_run = run["id"]
            except Exception as error:
                self.last_error = f"automation {auto['name']}: {error}"
                return  # the changes stay pending: the next tick replays them
        await self.state.emit(
            {
                "type": "AutomationFired",
                "name": auto["name"],
                "at": self.clock(),
                "run": last_run,
                "consumed": consumed,
            }
        )

    async def set_automation(self, name: str, enabled: bool):
        if name not in self.m.automations:
            raise KeyError(name)
        await self.state.emit({"type": "AutomationChanged", "name": name, "enabled": bool(enabled)})
        return self.m.automations[name]

    async def run_automation(self, name: str):
        auto = self.m.automations.get(name)
        if auto is None:
            raise KeyError(name)
        await self._fire(auto, auto.get("partitions") or "latest")
        return self.m.automations[name]

    # -- run control ------------------------------------------------------------------

    async def _control(self, run_id: str, action: str):
        await self.state.emit({"type": "RunControlled", "run": run_id, "action": action, "at": self.clock()})

    async def cancel(self, run_id: str):
        run = self.m.runs.get(run_id)
        if run is None:
            archived = await self.state.archived(run_id)
            if archived is None:
                raise KeyError(run_id)
            return self._run_view(archived)
        if run["status"] not in TERMINAL_RUN:
            # The claims go with the tasks: each attempt's next renew raises
            # LostOwnership and its wait loop cancels the placement (§8).
            await self._control(run_id, "cancel")
        return self._run_view(self.m.runs.get(run_id) or run)

    async def pause(self, run_id: str, paused=True):
        run = self.m.runs.get(run_id)
        if run is None:
            if await self.state.archived(run_id) is None:
                raise KeyError(run_id)
            raise Conflict(f"run {run_id} is finished")
        await self._control(run_id, "pause" if paused else "resume")
        return self._run_view(self.m.runs[run_id])

    async def retry(self, run_id: str):
        run = self.m.runs.get(run_id)
        if run is None:
            archived = await self.state.archived(run_id)
            if archived is None:
                raise KeyError(run_id)
            await self.state.emit({"type": "RunReopened", "run": archived, "at": self.clock()})
        await self._control(run_id, "retry")
        return self._run_view(self.m.runs[run_id])

    # -- finished runs -------------------------------------------------------------------

    async def _archive_due(self):
        """Write finished runs to `runs/{run}/run.json` and drop them from memory,
        once none of their attempts is still in flight here."""

        busy = {run_id for run_id, _ in self.inflight.values()}
        for run_id in sorted(self.m.archivable):
            if run_id in busy:
                continue
            run = self.m.runs.get(run_id)
            if run is None or any(tid in self.m.claims for tid in run["tasks"]):
                continue
            await self.state.archive(run_id)

    # -- read models -------------------------------------------------------------------

    def _run_view(self, run: dict) -> dict:
        view = {k: v for k, v in run.items() if k != "tasks"}
        view["tasks"] = sorted(run["tasks"])
        return view

    async def _run_view_of(self, run_id: str) -> dict:
        run = self.m.runs.get(run_id) or await self.state.archived(run_id)
        if run is None:
            raise KeyError(run_id)
        return self._run_view(run)

    def _task_view(self, task: dict, live: bool) -> dict:
        view = {k: v for k, v in task.items() if k != "attempts"}
        claim = self.m.claims.get(task["id"]) if live else None
        attempts = task["attempts"]
        view["status"] = claim["status"] if claim else task["status"]
        view["generation"] = view["attempt_count"] = len(attempts) + (1 if claim else 0)
        latest = attempts[-1] if attempts else None
        if latest:
            view.setdefault("error", latest.get("error"))
            view.setdefault("result", latest.get("outputs"))
        return view

    def _attempt_views(self, task: dict, live: bool) -> list[dict]:
        out = []
        for n, a in enumerate(task["attempts"], 1):
            view = {
                "id": a["id"],
                "task": task["id"],
                "generation": n,
                "status": a["outcome"],
                "started_at": a.get("started_at"),
                "finished_at": a.get("finished_at"),
            }
            if a.get("error"):
                view["error"] = a["error"]
            if a.get("outputs"):
                view["result"] = a["outputs"]
                view["commit"] = f"{task['run']}/{a['id']}"
            out.append(view)
        claim = self.m.claims.get(task["id"]) if live else None
        if claim:
            out.append(
                {
                    "id": claim["attempt"],
                    "task": task["id"],
                    "generation": len(out) + 1,
                    "status": claim["status"],
                    "started_at": claim["started_at"],
                }
            )
        return out

    async def list_runs(self, limit=50):
        runs = list(self.m.runs.values())
        needed = limit - len(runs)
        if needed > 0:
            active = set(self.m.runs)
            archived = [r for r in await self.state.archived_ids() if r not in active]
            for run_id in reversed(archived[-needed:]):
                run = await self.state.archived(run_id)
                if run is not None:
                    runs.append(run)
        runs.sort(key=lambda r: (r["created_at"], r["id"]), reverse=True)
        return [self._run_view(r) for r in runs[:limit]]

    async def run_detail(self, run_id: str):
        run = self.m.runs.get(run_id)
        live = run is not None
        if run is None:
            run = await self.state.archived(run_id)
            if run is None:
                raise KeyError(run_id)
        tasks = [run["tasks"][tid] for tid in sorted(run["tasks"])]
        return {
            "request": self._run_view(run),
            "tasks": [self._task_view(t, live) for t in tasks],
            "attempts": {t["id"]: self._attempt_views(t, live) for t in tasks},
        }

    def head_view(self, head: dict) -> dict:
        view = dict(head)
        view["commit"] = f"{head['run']}/{head['attempt']}" if head.get("attempt") else None
        return view

    def outcome_view(self, record: dict) -> dict:
        return {
            "last_outcome": record["outcome"],
            "last_attempt": f"{record['run']}/{record['attempt']}" if record.get("attempt") else None,
            "at": record["at"],
        }

    async def asset_detail(self, name: str, scope=""):
        asset = self._asset_of(name)
        info = self.manifest["assets"][asset]
        heads = {
            o["name"]: [(s, self.head_view(h)) for s, h in self.m.heads_of(o["name"])]
            for o in info["outputs"]
        }
        watermarks = {
            param: self.m.watermarks.get((asset, param, scope))
            for param, edge in info["inputs"].items()
            if edge["kind"] == "incremental"
        }
        dims = self._dims(asset)
        return {
            "asset": info,
            "heads": heads,
            "cursor": self.m.cursors.get((asset, scope)),
            "watermarks": watermarks,
            "current_keys": await self._dim_keys(dims) if dims else [],
            "scopes": {s: self.outcome_view(r) for s, r in self.m.outcomes_of(asset).items()},
        }

    async def catalog(self):
        assets = []
        for name, info in self.manifest["assets"].items():
            heads = {
                o["name"]: {s: self.head_view(h) for s, h in self.m.heads_of(o["name"])}
                for o in info["outputs"]
            }
            assets.append({**info, "name": name, "heads": heads})
        return assets
