"""The engine (§6–§10): control-plane only. It plans runs into per-(asset,
scope) tasks, resolves inputs to pinned heads, plans Incremental edges over
per-edge watermarks, dispatches attempts through placements, and commits
their results.

State lives in the model (model.py), changed only by events the engine emits
(docs/object-store-state.md §4). A precondition check and the event that
depends on it happen in one synchronous step, so no other coroutine can
interleave between them. A launched attempt is durable: an engine that
restarts adopts it, waits for its worker, and commits its result (§8).

Structure lives in the manifest, state lives in the spec, effects live in the
result — the engine never interprets a payload.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import math
from itertools import product
from zoneinfo import ZoneInfo

from croniter import croniter
from obstore.exceptions import AlreadyExistsError
from solera.ids import ulid, ulid_time
from solera.keys.index import IndexState, KeyIndex, Options, key_bytes, key_str
from solera.keys.io import ObjectIO, key_cache
from solera.sdk import TimePartitions, canonical_partition, digest, split_partition

from .history import MAX_METADATA, History, RunFilter
from .model import TERMINAL_RUN
from .placements import PlacementContext, Registry
from .state import Conflict, LostOwnership, State

log = logging.getLogger(__name__)

SUCCESS = {"succeeded", "skipped"}
TERMINAL = SUCCESS | {"failed", "blocked", "canceled"}
HEARTBEAT_SECONDS = 30.0  # a worker beats this often; three missed beats and it is dead
SOURCE_KEYS_RECORDED = 1000  # a source commit's run lists changed keys up to this many, else counts
GRACE_SECONDS = 5.0


class Retryable(RuntimeError):
    """A dispatch-time failure that the retry policy may absorb."""


class NonRetryable(RuntimeError):
    """A dispatch-time failure no retry will fix (§8: full run required, …)."""


def check_tags(tags) -> dict[str, str]:
    """Run tags: up to 32 short string pairs."""

    tags = tags or {}
    if not isinstance(tags, dict) or len(tags) > 32:
        raise ValueError("tags must be an object of at most 32 entries")
    for key, value in tags.items():
        if not isinstance(key, str) or not isinstance(value, str) or not key or "=" in key:
            raise ValueError("tags map non-empty names without '=' to strings")
        if len(key) > 64 or len(value) > 256:
            raise ValueError("A tag name is at most 64 characters, its value at most 256")
    return dict(sorted(tags.items()))


class Engine:
    def __init__(
        self,
        state: State,
        manifest: dict,
        *,
        registry: Registry | None = None,
        placements: dict | None = None,
        project: str = "",
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        concurrency: int = 4,
        clock=None,
        eval_interval: float = 0.5,
        key_options: Options | None = None,
        recount_interval: float = 3600.0,
        maintenance_concurrency: int = 2,
        retention_interval: float = 60.0,
        history: History | None = None,
    ):
        import time

        if heartbeat_seconds <= 0 or concurrency < 1:
            raise ValueError("Heartbeat interval and concurrency must be positive")
        self.state, self.manifest = state, manifest
        self.project = project
        self.clock = clock or time.time
        self.heartbeat_seconds, self.concurrency = heartbeat_seconds, concurrency
        self.eval_interval = eval_interval
        ctx = PlacementContext(state, state.objects_url, project, self.clock)
        self.registry = registry or Registry(ctx, extra=placements)
        # attempt id -> (run id, asyncio task): attempts this process is driving.
        self.inflight: dict[str, tuple[str, asyncio.Task]] = {}
        # Attempts consuming a local execution slot. Pool attempts only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.env_inflight: dict[str, int] = {}
        self.runner: asyncio.Task | None = None
        self.last_error = None
        self._stopping = False
        self._firing: set[str] = set()
        # Key index upkeep (§6), all memory only: which delta log batches each
        # in-flight attempt reads, indexes with compaction or a recount running,
        # when each index was last recounted, and the state last checked.
        self.key_options = key_options or Options()
        self.recount_interval = recount_interval
        self.maintenance_concurrency = maintenance_concurrency
        self.reading: dict[str, list[tuple]] = {}
        self.maintaining: dict[tuple, asyncio.Task] = {}
        self._recounted: dict[tuple, float] = {}
        self._checked: dict[tuple, IndexState] = {}
        self._io: ObjectIO | None = None
        self.history = history or History(state, clock=self.clock)
        self.retention_interval = retention_interval
        self._swept = -math.inf
        self._set_dims = {
            dim["output"]
            for a in manifest["assets"].values()
            for dim in ((a.get("partitions") or {}).get("dims") or {}).values()
            if dim["kind"] == "set"
        }

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
        """Start the eval loop. Its first tick adopts the attempts launched
        before a restart: their workers keep running, and this engine waits
        for them and commits their results (§8)."""

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
            # Launched attempts keep running: the next engine adopts them.
            jobs = [t for _, t in self.inflight.values()]
            for job in jobs:
                job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            self.inflight.clear()
        for job in list(self.maintaining.values()):
            job.cancel()
        await asyncio.gather(*self.maintaining.values(), return_exceptions=True)
        await self.history.stop()

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
        """One evaluation pass: adoption, dispatch, automations, archiving,
        and upkeep of key indexes and the history."""

        self._adopt()
        await self._dispatch_due()
        await self._automation_tick()
        await self._archive_due()
        await self._maintain_indexes()
        await self._retention_sweep()
        await self.history.tick()

    async def run_until(self, run_id: str, timeout: float = 120.0):
        """Tick until the run reaches a terminal status (CLI and tests)."""

        deadline = self.clock() + timeout
        while self.clock() < deadline:
            await self.tick()
            run = self.m.runs.get(run_id) or await self.history.run(run_id)
            if run and run["status"] in TERMINAL:
                # Wait only on this run's attempts — unrelated pool-placed runs
                # may hold inflight waiters until a worker claims them.
                mine = [t for r, t in self.inflight.values() if r == run_id]
                await asyncio.gather(*mine, return_exceptions=True)
                detail = await self.run_detail(run_id)
                await self._archive_due()  # a one-shot caller (the CLI) leaves nothing behind
                return detail
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
        by=None,
        tags=None,
    ):
        """A run request becomes one task per (asset, scope) (§8). `by` says
        who asked (the API or CLI, or what the caller names); automation runs
        carry `automation` instead. `tags` label the run for finding it later."""

        if isinstance(targets, str):
            targets = [targets]
        if not targets:
            raise ValueError("A run needs at least one target")
        if mode not in {"incremental", "full"}:
            raise ValueError(f"Unknown mode: {mode!r}")
        config = config or {}
        if not isinstance(config, dict):
            raise ValueError("config must be a JSON object")
        tags = check_tags(tags)
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
            "by": by,
            "tags": tags,
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
        return self._scope_active_claim(asset, scope) or self.m.is_pending(asset, scope)

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
        """A set dimension's current keys: the element list its head carries (§7)."""

        if head is None:
            return None
        return [str(e) for e in head.get("elements") or ()]

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

    # -- attempt outcomes ------------------------------------------------------------

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
        unsettled=None,
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
        if unsettled:
            event["unsettled"] = unsettled
        await self.state.emit(event)

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
            self.m.claim(task_id, attempt, now)
            env_used[env_key] = env_used.get(env_key, 0) + 1
            engine_used += not is_pool
            self._spawn(task["run"], attempt, is_pool, env_key, self._attempt(task_id, attempt, placement))

    def _spawn(self, run_id: str, attempt: str, is_pool: bool, env_key: str, work):
        """Drive one attempt in the background, holding its execution slots."""

        if not is_pool:
            self.engine_inflight.add(attempt)
        self.env_inflight[env_key] = self.env_inflight.get(env_key, 0) + 1
        job = asyncio.create_task(work)
        self.inflight[attempt] = (run_id, job)

        def done(_job):
            self.inflight.pop(attempt, None)
            self.reading.pop(attempt, None)
            self.engine_inflight.discard(attempt)
            self.env_inflight[env_key] = max(0, self.env_inflight.get(env_key, 1) - 1)

        job.add_done_callback(done)

    def _adopt(self):
        """Wait again for attempts launched before a restart (§8): their
        claims are durable, but nothing in this process waits on them yet."""

        for task_id, claim in list(self.m.claims.items()):
            attempt = claim["attempt"]
            if not claim.get("launched") or attempt in self.inflight:
                continue
            task = self.m.task(task_id)
            launched = task["launched"]
            execution = launched["execution"]
            try:
                placement = self.registry.build(execution)
            except Exception:
                log.exception("attempt %s: placement %s unavailable", attempt, execution["kind"])
                placement = None
            self.reading[attempt] = self._reads(launched["prepared"])
            work = self._resume(task_id, attempt, placement)
            self._spawn(
                task["run"], attempt, execution["kind"] == "Pool", self.registry.env_key(execution), work
            )

    def _scope_active_claim(self, asset: str, scope: str) -> bool:
        attempt = self.m.locks.get((asset, scope))
        return attempt is not None and self.m.claimed(attempt) is not None

    async def _attempt(self, task_id: str, attempt: str, placement):
        """One attempt, start to finish: prepare, skip or launch, wait, settle.
        Every path ends in exactly one AttemptFinished, a lost claim, or — when
        the engine stops — a launched attempt the next engine adopts."""

        try:
            task = self.m.task(task_id)
            claim = self.m.claimed(attempt)
            if task is None or claim is None:
                return
            run = self.m.runs[task["run"]]
            try:
                prepared = await self._prepare(task, run, attempt)
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
                watermarks = {
                    param: plan["update"] if "update" in plan else self._watermark(plan, None)
                    for param, plan in prepared["plans"].items()
                    if plan is not None
                }
                await self._finish(task, claim, "skipped", commit={"watermarks": watermarks})
                return
            stage = await self._launch(task, run, attempt, prepared)
            try:
                handle = await placement.launch(stage)
            except Exception as error:
                await self._fail(task_id, attempt, f"launch: {error}", retryable=True)
                return
            await self._watch(task_id, attempt, placement, handle)
        except LostOwnership:
            return
        except Exception as error:  # never leave a claim behind
            await self._crashed(task_id, attempt, error)
        finally:
            self.m.release(task_id, attempt)

    async def _resume(self, task_id: str, attempt: str, placement):
        """An adopted attempt: wait for it and settle it, as `_attempt` would have."""

        try:
            task = self.m.task(task_id)
            stage = {"attempt": attempt, "run": task["run"], "objects": self.state.objects_url}
            handle = None
            if callable(getattr(placement, "resume", None)):
                try:
                    handle = await placement.resume(stage)
                except Exception:
                    log.exception("attempt %s: resuming its placement failed", attempt)
            await self._watch(task_id, attempt, placement, handle)
        except LostOwnership:
            return
        except Exception as error:
            await self._crashed(task_id, attempt, error)

    async def _crashed(self, task_id: str, attempt: str, error: Exception):
        log.exception("attempt %s failed unexpectedly", attempt)
        claim = self.m.claimed(attempt)
        task = self.m.task(task_id)
        if claim is None or task is None:
            return
        with contextlib.suppress(Exception):
            if claim.get("launched"):
                await self._fail(task_id, attempt, f"engine: {error}", retryable=True)
            else:
                await self._finish(task, claim, "failed", error=f"engine: {error}", retryable=True)

    # -- input resolution + Incremental plans (§5, §6, §8) --------------------------

    async def _prepare(self, task: dict, run: dict, attempt: str | None = None) -> dict:
        """Pin heads at attempt start; plan Incremental edges; decide skip (§8).

        Each output the attempt may write is pinned with its batch number and,
        when keyed, its key index: the harness works out the delta against
        exactly that state (§6)."""

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
        plans, all_empty = {}, True
        for param, edge, up_dims in incremental:
            up_scope = self._project(asset, scope, up_dims)
            ref = self._pin_at(edge["output"], up_scope)
            pin, plan, empty = self._incremental_plan(
                task, param, edge, ref, up_scope, fingerprint, run, full
            )
            inputs[param] = pin
            plans[param] = plan
            all_empty = all_empty and empty
        if attempt is not None:
            # Keep the delta log this attempt reads until it finishes (§6).
            self.reading[attempt] = self._reads({"plans": plans})
        more = any(p.get("more") for p in plans.values() if p)
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
        outputs = {}
        for output in asset["outputs"]:
            name, head = output["name"], baseline[output["name"]]
            info = {"exists": head is not None}
            if head is not None:
                info["head"] = head["ref"]
            if name == task["asset"] and asset.get("aliases"):
                info["aliases"] = list(asset["aliases"])
            if output.get("incremental"):
                info["batch"] = int((head or {}).get("batch", -1)) + 1
            if output.get("key") is not None:
                info["index"] = self.m.index(name, scope).pinned().to_json()
                if (name, scope) in self.m.unsettled:
                    info["unsettled"] = self.m.unsettled[(name, scope)]
                if output.get("partition_set") or name in self._set_dims:
                    info["elements"] = list((head or {}).get("elements") or ())
            outputs[name] = info
        # The input versions its outputs will be built from, for the history (§7).
        lineage = []
        for param, edge in edges:
            pin = inputs.get(param) or {}
            refs = [pin["ref"]] if pin.get("ref") else list((pin.get("refs") or {}).values())
            for ref in refs:
                lineage.append([edge["output"], ref.get("partition") or "", ref.get("version"), param])
        return {
            "inputs": inputs,
            "lineage": lineage,
            "baseline": baseline,
            "plans": plans,
            "more": more,
            "full": full,
            "skip": skip,
            "prior": prior,
            "cursor": cursor,
            "fingerprint": fingerprint,
            "outputs": outputs,
        }

    @staticmethod
    def _reads(prepared: dict) -> list[tuple]:
        """The delta logs an attempt reads: `(output, scope, first batch)`."""

        return [(p["output"], p["up"], p["from"]) for p in prepared["plans"].values() if p and "from" in p]

    @staticmethod
    def _durable(prepared: dict) -> dict:
        """What settling an attempt needs of its preparation, kept on its task
        so that an engine that restarts can still commit it (§8). The pinned
        key indexes stay in the spec; settling needs only where they live."""

        outputs = {}
        for name, info in prepared["outputs"].items():
            info = dict(info)
            index = info.pop("index", None)
            if index is not None:
                info["prefix"] = index["prefix"]
            outputs[name] = info
        kept = {k: prepared[k] for k in ("baseline", "plans", "more", "full", "prior", "lineage")}
        return {**kept, "outputs": outputs}

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

    def _incremental_plan(self, task, param, edge, ref, up_scope, fingerprint, run, full):
        """Plan one Incremental edge from its watermark (§6): returns the pin
        for the spec, the plan the commit turns into the next watermark, and
        whether nothing is pending.

        A keyed upstream is read through its key index by the harness: the
        delta log from `wm.batch` to the head, or — for a missing watermark, a
        fingerprint change, a `full` run, a keys='full' override, or a log that
        no longer holds the window — the whole index, restarting from the
        head's next batch so changes made while draining arrive afterwards as
        deltas. Either is delivered `batch_size` keys at a time; the harness
        reports where it stopped (`after`), and a task with more to deliver is
        queued again. A batch-mode upstream is planned here: the next
        `batch_size` batches after the watermark, all since the last reset
        (`base`) when starting over.

        Watermark: {"batch", "until"?, "after", "full", "fingerprint", "output", "up"}."""

        output = edge["output"]
        keyed = self.manifest["outputs"][output].get("key") is not None
        limit = int(edge.get("batch_size") or 100)
        head = self.m.heads.get((output, up_scope)) or {}
        head_batch = int(head.get("batch", -1))
        override = (run.get("keys") or {}).get(output)
        wm = self.m.watermarks.get((task["asset"], param, task["scope"]))
        reset = full or wm is None or wm.get("fingerprint") != fingerprint or override == "full"
        base = {"output": output, "up": up_scope, "fingerprint": fingerprint}

        # A keys= override is a one-off selection — it never moves the watermark.
        if isinstance(override, dict) and "keys" in override and not reset:
            keys = sorted({str(k) for k in override["keys"]})
            return {"ref": ref, "changes": {"keys": keys, "full": False}}, None, not keys

        if not keyed:
            first = int(head.get("base", 0))
            reset = reset or int(wm["batch"]) < first
            lo = first if reset else int(wm["batch"])
            hi = min(head_batch, lo + limit - 1)
            pin = {"ref": ref, "changes": {"batches": [lo, hi], "full": reset}}
            update = {**base, "batch": max(lo, hi + 1), "after": None, "full": False}
            return pin, {"update": update, "more": hi < head_batch}, hi < lo

        index = self.m.index(output, up_scope)
        empty = False
        if reset:
            window = {"full": True, "from": head_batch + 1, "after": None}
            empty = index.count == 0 and not index.files
        elif wm.get("full"):
            window = {"full": True, "from": wm["batch"], "after": wm.get("after")}
        elif wm.get("after") is not None:
            window = {"full": False, "from": wm["batch"], "to": wm["until"], "after": wm["after"]}
        else:
            window = {"full": False, "from": wm["batch"], "to": head_batch, "after": None}
            empty = wm["batch"] > head_batch
        if not window["full"] and not index.covers(window["from"], window["to"]):
            # The log no longer holds this window: deliver everything again.
            window = {"full": True, "from": head_batch + 1, "after": None}
            empty = False
        pinned = index.pinned() if window["full"] else index.pinned(window["from"], window["to"])
        pin = {"ref": ref, "index": pinned.to_json(), "changes": {**window, "limit": limit}}
        return pin, {**base, **window}, empty

    @staticmethod
    def _watermark(plan: dict, after: str | None) -> dict:
        """The watermark after delivering a keyed plan's page, which ended at
        `after` (`None`: the window is done)."""

        base = {k: plan[k] for k in ("output", "up", "fingerprint")}
        if plan["full"]:
            if after is None:
                return {**base, "batch": plan["from"], "after": None, "full": False}
            return {**base, "batch": plan["from"], "after": after, "full": True}
        if after is None:
            return {**base, "batch": max(plan["from"], plan["to"] + 1), "after": None, "full": False}
        return {**base, "batch": plan["from"], "until": plan["to"], "after": after, "full": False}

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

    async def _launch(self, task, run, attempt, prepared) -> dict:
        """Write the attempt file and make the launch durable (§8): from here
        on, the attempt outlives this engine. Returns the placement's stage."""

        spec = {
            "attempt": attempt,
            "revision": self.manifest["revision"],
            "asset": task["asset"],
            "partition": task["scope"],
            "run": {"id": task["run"], "config": run.get("config") or {}},
            "prior": prepared["prior"],
            "outputs": prepared["outputs"],
            "inputs": prepared["inputs"],
            "execution": self.manifest["assets"][task["asset"]]["placement"],
            "heartbeat": self.heartbeat_seconds,
        }
        if prepared["cursor"] is not None:
            spec["cursor"] = prepared["cursor"]
        # The attempt file (§8): created here with the spec; the harness
        # rewrites it once, with the spec, its result and its log index.
        path = f"{self.state.attempt_path(task['run'], attempt)}.json"
        await self.state.create_object(path, json.dumps({"spec": spec}).encode())
        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)  # canceled while the spec was written
        execution = spec["execution"]
        event = {
            "type": "AttemptLaunched",
            "run": task["run"],
            "task": task["id"],
            "attempt": attempt,
            "started_at": claim["started_at"],
            "at": self.clock(),
            "execution": execution,
            "prepared": self._durable(prepared),
        }
        if execution["kind"] == "Pool":
            needs = {
                k: v for k in ("cpu", "memory", "gpu") if (v := execution["placement"].get(k)) is not None
            }
            event["pool"] = {"name": execution["environment"]["name"], "needs": needs}
        await self.state.emit(event)
        return {"attempt": attempt, "run": task["run"], "objects": self.state.objects_url}

    async def _watch(self, task_id: str, attempt: str, placement, handle):
        """Wait for a launched attempt to end, then settle it (§8).

        The placement handle says when the worker exits. Without one — after
        a restart — the worker's heartbeat does: it rewrites `{attempt}.beat`
        every `heartbeat_seconds`, marks it done once its result is written,
        and is dead after three missed beats. A cancel or a timeout aborts the
        attempt; one that is already writing is waited for instead."""

        task = self.m.task(task_id)
        run_id, launched = task["run"], self._launched(task, attempt)
        beat_path = f"{self.state.attempt_path(run_id, attempt)}.beat"
        limit = (self.manifest["assets"].get(task["asset"]) or {}).get("timeout") or 3600
        deadline = launched["at"] + limit
        poll = self.heartbeat_seconds / 3
        beat, beat_at = None, self.clock()
        writing = False  # an abort found the worker writing: wait for it
        while True:
            polled = self.clock()
            exit_ = None
            if handle is not None:
                try:
                    exit_ = await placement.wait(handle, poll)
                except Exception as error:
                    log.warning("attempt %s: placement wait failed, following heartbeats: %s", attempt, error)
                    handle = None
            else:
                await asyncio.sleep(poll)
            if exit_ is not None:
                break
            # A placement that returns before its timeout must not spin the loop.
            await asyncio.sleep(max(0.0, 0.05 - (self.clock() - polled)))
            now = self.clock()
            if handle is None:
                current = await self.state.get_object(beat_path)
                if current is not None and json.loads(current).get("done"):
                    exit_ = {"code": None, "reason": "done", "meta": {}}
                    break
                if current != beat:
                    beat, beat_at = current, now
                elif now - beat_at > 3 * self.heartbeat_seconds:
                    exit_ = {"code": None, "reason": "no heartbeat", "meta": {}}
                    break
            task = self.m.task(task_id)
            canceled = task is None or task["status"] == "canceled"
            if writing or not (canceled or now > deadline):
                continue
            if await self._abort(run_id, attempt) is not None:
                writing = True
                continue
            if handle is not None:
                await self._cancel(placement, handle)
            if canceled:
                await self._fail(task_id, attempt, "canceled", outcome="canceled")
            else:
                await self._fail(task_id, attempt, "timeout", retryable=True)
            return
        await self._settle(task_id, attempt, exit_)

    async def _settle(self, task_id: str, attempt: str, exit_: dict):
        """Commit an ended attempt's result, or fail it."""

        task = self.m.task(task_id)
        prepared = self._launched(task, attempt)["prepared"]
        record = await self.state.attempt_record(task["run"], attempt)
        result = (record or {}).get("result")
        if result is None:
            await self._fail(task_id, attempt, f"harness exited without a result: {exit_}", retryable=True)
            return
        if result.get("status") == "failed":
            error = result.get("error") or {}
            await self._fail(
                task_id,
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
            await self._fail(task_id, attempt, str(error), retryable=getattr(error, "retryable", True))

    @staticmethod
    def _launched(task: dict | None, attempt: str) -> dict:
        launched = (task or {}).get("launched")
        if launched is None or launched["attempt"] != attempt:
            raise LostOwnership(attempt)  # it finished meanwhile
        return launched

    async def _abort(self, run_id: str, attempt: str) -> dict | None:
        """Take the attempt's write fence (§8). Returns None once the attempt
        is aborted — it can never write; else the fence its worker took first:
        it is writing, and the fence lists its intents."""

        path = f"{self.state.attempt_path(run_id, attempt)}.writing"
        try:
            await self.state.create_object(path, json.dumps({"state": "aborted"}).encode())
            return None
        except AlreadyExistsError:
            fence = json.loads(await self.state.get_object(path))
            return None if fence["state"] == "aborted" else fence

    async def _fail(
        self, task_id: str, attempt: str, error: str, *, retryable=False, delay=0.0, outcome="failed"
    ):
        """End a launched attempt without a commit (§8). It is aborted first,
        so it can never write after this. If it had begun writing, the keyed
        outputs it meant to change stay unsettled until a later commit takes
        in what landed; their intent files are kept for that."""

        task = self.m.task(task_id)
        prepared = self._launched(task, attempt)["prepared"]
        fence = await self._abort(task["run"], attempt)
        unsettled = (fence or {}).get("intents") or {}
        claim = self.m.claimed(attempt)
        if claim is None:
            return
        await self._finish(
            task, claim, outcome, error=error, retryable=retryable, delay=delay, unsettled=unsettled
        )
        await self._discard(attempt, prepared, keep=set(unsettled))

    async def _discard(self, attempt: str, prepared: dict, keep=()):
        """Delete the delta files an attempt wrote but never committed, except
        the intents of outputs it left unsettled. They are named after the
        attempt, so nothing else can hold them (§6)."""

        for name, info in (prepared.get("outputs") or {}).items():
            if info.get("prefix") is None or name in keep:
                continue
            prefix = f"{info['prefix']}{int(info['batch']):012d}-{attempt}"
            with contextlib.suppress(Exception):
                await self.state.delete_objects(await self.state.list_objects(prefix))

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

        from solera.sdk import UNSET

        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)
        task = self.m.task(self.m.attempts[attempt])
        # Inputs may have moved since they were pinned: the attempt's output
        # derives from what it read (the spec records it), its watermarks
        # cover only the window it was given, and a moved input changes the
        # next attempt's fingerprint. Refusing here would only strand a write
        # a shared-table store has already made.
        # Output heads must be unchanged since the claim.
        for output, baseline in prepared["baseline"].items():
            if self.m.heads.get((output, task["scope"])) != baseline:
                raise Conflict(f"output {output} head changed since this attempt was claimed")
        outputs = result.get("outputs") or {}
        asset = self.manifest["assets"][task["asset"]]
        declared = {o["name"]: o for o in asset["outputs"]}
        # Where each keyed Incremental page ended decides the next watermark.
        delivered = result.get("delivered") or {}
        watermarks, more = {}, bool(prepared.get("more"))
        for param, plan in (prepared.get("plans") or {}).items():
            if plan is None:
                continue
            if "update" in plan:
                watermarks[param] = plan["update"]
                continue
            if param not in delivered:
                raise Conflict(f"input {param}: the result reports no delivery", retryable=False)
            after = delivered[param].get("after")
            watermarks[param] = self._watermark(plan, after)
            more = more or after is not None
        heads, keys = {}, {}
        for name, entry in outputs.items():
            if name not in declared:
                raise Conflict(f"result names undeclared output {name!r}", retryable=False)
            decl, before = declared[name], prepared["baseline"].get(name)
            info = (prepared.get("outputs") or {}).get(name) or {}
            if entry.get("unchanged"):
                if before is None:
                    raise Conflict(f"output {name}: unchanged, but there is no head", retryable=False)
                ref = before["ref"]
            else:
                ref = entry.get("ref")
                if ref is None or ref["partition"] != task["scope"]:
                    raise Conflict(f"output {name}: ref scope != {task['scope']!r}", retryable=False)
            head = {"ref": ref, "complete": not more, "asset": task["asset"], "version": asset["version"]}
            if decl.get("key") is not None:
                delta = entry.get("keys")
                if delta is None and not entry.get("unchanged"):
                    raise Conflict(f"keyed output {name}: the result carries no key delta", retryable=False)
                head["batch"] = int((before or {}).get("batch", -1))
                if delta is not None:
                    if delta["files"]:
                        head["batch"] = int(info["batch"])
                    keys[name] = {**delta, "batch": head["batch"]}
                if "elements" in info:
                    head["elements"] = entry.get("elements", info["elements"])
            elif decl.get("incremental"):
                if before is not None and before["ref"]["version"] == ref["version"]:
                    head["batch"], head["base"] = before.get("batch", -1), before.get("base", 0)
                else:
                    # A write with no prior (a first write, or a full run) starts over.
                    head["batch"] = int(info["batch"])
                    head["base"] = head["batch"] if name not in prepared["prior"] else before.get("base", 0)
            heads[name] = head
        for name in set(declared) - set(outputs):
            if prepared["baseline"].get(name) is None:
                raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)
        commit = {"heads": heads, "watermarks": watermarks}
        if keys:
            commit["keys"] = keys
        # What the attempt said of each output version, for the history (§7).
        metadata, rows = {}, {}
        for name, entry in outputs.items():
            found = entry.get("metadata")
            if isinstance(found, dict) and found:
                if len(json.dumps(found, default=str)) <= MAX_METADATA:
                    metadata[name] = found
                else:
                    log.warning("%s: metadata of %s over %d bytes dropped", attempt, name, MAX_METADATA)
            if isinstance(entry.get("rows"), int):
                rows[name] = entry["rows"]
        if metadata:
            commit["metadata"] = metadata
        if rows:
            commit["rows"] = rows
        settled = sorted(
            name
            for name, entry in outputs.items()
            if ((prepared.get("outputs") or {}).get(name) or {}).get("unsettled")
            and not entry.get("unchanged")
        )
        if settled:
            commit["settled"] = settled
        if result.get("cursor", UNSET) is not UNSET:
            commit["cursor"] = result["cursor"]
        elif prepared.get("full"):
            commit["cursor"] = None  # a full run clears the committed cursor (§8)
        await self._finish(task, claim, "succeeded", commit=commit, more=more)
        return {"run": task["run"], "attempt": attempt, "outputs": outputs}

    # -- pool work (§10) ---------------------------------------------------------------

    def register_worker(self, worker_id: str, pools: list[str], meta: dict) -> dict:
        record = {"id": worker_id, "pools": pools, "meta": meta, "seen_at": self.clock()}
        self.m.workers[worker_id] = record
        return record

    async def claim_pool_task(self, worker_id: str, pools: list[str], capacity: dict, lease_seconds: float):
        """Oldest unclaimed pool task in the worker's pools that fits the
        worker's cpu/memory/gpu capacity (§10). The claim is durable, so a
        restarted engine never offers it to a second worker."""

        now = self.clock()
        for record in sorted(self.m.pool.values(), key=lambda r: r["created_at"]):
            if record["status"] != "queued" or record["pool"] not in pools:
                continue
            if (self.m.task(record["task"]) or {}).get("status") == "canceled":
                continue  # being aborted
            needs = record.get("needs") or {}
            if any(capacity.get(dim) is None or capacity[dim] < want for dim, want in needs.items()):
                continue
            record["lease_until"] = now + lease_seconds
            await self.state.emit(
                {"type": "AttemptClaimed", "attempt": record["attempt"], "worker": worker_id, "at": now}
            )
            claimed = self.m.pool.get(record["attempt"])
            if claimed is not None and claimed["claimed_by"] == worker_id:
                return claimed
        return None

    def heartbeat_pool_task(self, worker_id: str, attempt: str, lease_seconds: float) -> float:
        record = self.m.pool.get(attempt)
        if (
            record is None
            or record["status"] != "claimed"
            or record["claimed_by"] != worker_id
            or (record["lease_until"] is not None and record["lease_until"] <= self.clock())
        ):
            raise LostOwnership(attempt)
        record["lease_until"] = self.clock() + lease_seconds
        return record["lease_until"]

    def release_pool_task(self, worker_id: str, attempt: str) -> None:
        record = self.m.pool.get(attempt)
        if record and record["claimed_by"] == worker_id:
            del self.m.pool[attempt]

    # -- sources commit API (§5) ------------------------------------------------------

    async def commit_source(
        self, name: str, *, version=None, keys=None, upsert=None, remove=None, by: str | None = None
    ):
        """Advance a source without moving data (§2.3, §6). A keyed source
        commit is checked against the source's key index like any write: a
        full map (`keys=`) replaces its content, `upsert`/`remove` patch it,
        and the changes become the commit's delta file. A commit that changes
        nothing is not a change. An unkeyed source takes a `version=`.

        Every change is recorded as a run with no tasks (§7):
        `{"id", "source", "by", "batch", "upserted", "deleted"}` — or `version`
        for an unkeyed source. `by` says where the commit came from."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        head = self.m.heads.get((name, ""))
        keyed = source.get("key") is not None
        if not keyed and (keys is not None or upsert is not None or remove is not None):
            raise ValueError(f"Source {name!r} is unkeyed; pass version=")
        ref = dict(head["ref"] if head is not None else source["head"])
        run_id = ulid(self.clock())
        record = {
            "ref": ref,
            "run": run_id,
            "attempt": None,
            "complete": True,
            "asset": None,
            "version": None,
        }
        event = {"type": "SourceCommitted", "source": name, "head": record}
        batch = None
        run = {"id": run_id, "source": name, "by": by}
        if not keyed:
            if version is None:
                raise ValueError(f"Source {name!r} requires version=")
            if head is not None and head["ref"]["version"] == str(version):
                return {"changed": False, "ref": head["ref"]}
            ref["version"] = str(version)
            run["version"] = ref["version"]
        else:
            if keys is not None:
                items = keys.items() if isinstance(keys, dict) else ((k, "1") for k in keys)
                new, removes, replace = {str(k): str(v) for k, v in items}, [], True
            else:
                items = upsert.items() if isinstance(upsert, dict) else ((k, "1") for k in upsert or [])
                new = {str(k): str(v) for k, v in items}
                removes, replace = [str(k) for k in remove or [] if str(k) not in new], False
            index = KeyIndex(self._key_io(), None, self.m.index(name, "").pinned(), self.key_options)
            delta = await index.changes(
                [key_bytes(k) for k in new],
                [key_bytes(v) for v in new.values()],
                [key_bytes(k) for k in removes],
                replace=replace,
            )
            if not len(delta):
                return {"changed": False, "ref": ref}
            batch = int((head or {}).get("batch", -1)) + 1
            files = await index.write(batch, ulid(self.clock()), delta)
            ref["version"] = digest([ref["version"], batch, [f.name for f in files.files]])
            record["batch"] = batch
            if source.get("key") == "<elements>" or name in self._set_dims:
                before = set((head or {}).get("elements") or ())
                record["elements"] = sorted(set(new) if replace else (before - set(removes)) | set(new))
            event["keys"] = {**files.to_json(), "batch": batch}
            run["batch"] = batch
            for field, flag in (("upserted", 0), ("deleted", 1)):
                changed = [key_str(k) for k, d in zip(delta.keys, delta.deleted, strict=True) if d == flag]
                run[field] = changed if len(changed) <= SOURCE_KEYS_RECORDED else len(changed)
        meta = dict(ref.get("meta") or {})
        meta["external"] = True
        ref["meta"] = meta
        if self.m.heads.get((name, "")) != head:
            if "keys" in event:
                await self.state.delete_objects([index.path(f["name"]) for f in event["keys"]["files"]])
            raise Conflict(f"source {name!r} moved while committing; retry")
        event["at"] = self.clock()
        event["run"] = run
        await self.state.emit(event)
        return {"changed": True, "ref": ref, "run": run_id}

    # -- key index upkeep (§6) --------------------------------------------------------

    def _key_io(self) -> ObjectIO:
        if self._io is None:
            cache = key_cache(self.manifest.get("key_cache"), self.state.objects_url)
            self._io = ObjectIO(self.state.objects, cache=cache)
        return self._io

    async def list_keys(self, output: str, scope: str = "", *, after=None, offset=0, limit=1000) -> dict:
        """One page of an output's live keys, read from its key index."""

        if (output, scope) not in self.m.heads:
            raise KeyError(f"{output}/{scope}")
        state = self.m.indexes.get((output, scope))
        if state is None:
            return {"total": 0, "exact": True, "keys": {}, "next": None}
        index = KeyIndex(self._key_io(), None, state.pinned(), self.key_options)
        start = key_bytes(after) if after is not None else None
        keys, versions, nxt = await index.page(start, offset + limit)
        return {
            "total": state.count,
            "exact": state.count_exact,
            "keys": {key_str(k): key_str(v) for k, v in list(zip(keys, versions, strict=True))[offset:]},
            "next": key_str(nxt) if nxt is not None else None,
        }

    async def _maintain_indexes(self):
        """Truncate delta logs to what consumers still need, start compactions
        and recounts, and delete index files nothing references any more."""

        now = self.clock()
        needed: dict[tuple, int] = {}
        for wm in self.m.watermarks.values():
            if "up" in wm:
                key = (wm["output"], wm["up"])
                needed[key] = min(needed.get(key, math.inf), int(wm["batch"]))
        for reads in self.reading.values():
            for output, up, first in reads:
                needed[(output, up)] = min(needed.get((output, up), math.inf), int(first))
        truncations = []
        for (output, scope), index in self.m.indexes.items():
            if not index.log:
                continue
            below = needed.get((output, scope), index.log[-1][0] + 1)
            if index.log[0][0] < below:
                truncations.append(
                    {"type": "IndexTruncated", "output": output, "scope": scope, "below": below, "at": now}
                )
        if truncations:
            await self.state.emit(*truncations)

        for key, index in list(self.m.indexes.items()):
            if len(self.maintaining) >= self.maintenance_concurrency:
                break
            if key in self.maintaining or self._checked.get(key) is index:
                continue
            plan = KeyIndex(None, None, index, self.key_options).plan_compaction()
            if plan is not None:
                self._start_maintenance(key, index, recount=False)
            elif not index.count_exact:
                if now - self._recounted.get(key, -math.inf) >= self.recount_interval:
                    self._start_maintenance(key, index, recount=True)
            else:
                self._checked[key] = index

        if self.m.garbage:
            oldest = min((c["started_at"] for c in self.m.claims.values()), default=math.inf)
            due = [path for path, at in self.m.garbage if at < oldest]
            if due:
                await self._delete_files(due)
                await self.state.emit({"type": "GarbageDeleted", "paths": due})

    async def _delete_files(self, paths: list[str]):
        from obstore.exceptions import NotFoundError

        try:
            await self.state.delete_objects(paths)
        except (NotFoundError, FileNotFoundError):
            for path in paths:
                with contextlib.suppress(NotFoundError, FileNotFoundError):
                    await self.state.delete_objects([path])

    def _start_maintenance(self, key: tuple, index: IndexState, *, recount: bool):
        job = asyncio.create_task(self._maintenance(key, index, recount))
        self.maintaining[key] = job
        job.add_done_callback(lambda _t: self.maintaining.pop(key, None))

    async def _maintenance(self, key: tuple, index: IndexState, recount: bool):
        """One compaction or recount, run on a worker thread with its own event
        loop so merging never blocks the engine (§6, engine work)."""

        cache = key_cache(self.manifest.get("key_cache"), self.state.objects_url)
        options, objects = self.key_options, self.state.objects

        def work():
            async def go():
                keys = KeyIndex(ObjectIO(objects, cache=cache), None, index, options)
                return await (keys.recount() if recount else keys.compact())

            return asyncio.run(go())

        try:
            result = await asyncio.to_thread(work)
        except Exception as error:
            self.last_error = f"key index {key[0]}/{key[1]}: {type(error).__name__}: {error}"
            log.exception("key index maintenance failed for %s", key)
            self._recounted[key] = self.clock()
            return
        output, scope = key
        current = self.m.indexes.get(key)
        if recount:
            self._recounted[key] = self.clock()
            if current is index:  # no commit landed meanwhile, so the count is still current
                await self.state.emit(
                    {
                        "type": "IndexCompacted",
                        "output": output,
                        "scope": scope,
                        "added": [],
                        "removed": [],
                        "recount": result,
                        "at": self.clock(),
                    }
                )
            return
        if result is None:
            return
        added, removed = result
        if current is None or not set(removed) <= {f.name for f in current.files}:
            created = [f.name for f in added if f.name not in removed]
            await self._delete_files([index.path(n) for n in created])
            return
        await self.state.emit(
            {
                "type": "IndexCompacted",
                "output": output,
                "scope": scope,
                "added": [f.to_json() for f in added],
                "removed": removed,
                "at": self.clock(),
            }
        )

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
            tags=auto.get("tags"),
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
            archived = await self.history.run(run_id)
            if archived is None:
                raise KeyError(run_id)
            return self._run_view(archived)
        if run["status"] not in TERMINAL_RUN:
            # Unlaunched claims go with the tasks. Each launched attempt is
            # aborted by its wait loop — or, if already writing, committed (§8).
            await self._control(run_id, "cancel")
        return self._run_view(self.m.runs.get(run_id) or run)

    async def pause(self, run_id: str, paused=True):
        run = self.m.runs.get(run_id)
        if run is None:
            if await self.history.run(run_id) is None:
                raise KeyError(run_id)
            raise Conflict(f"run {run_id} is finished")
        await self._control(run_id, "pause" if paused else "resume")
        return self._run_view(self.m.runs[run_id])

    async def retry(self, run_id: str):
        run = self.m.runs.get(run_id)
        if run is None:
            archived = await self.history.run(run_id)
            if archived is None or "source" in archived:
                raise KeyError(run_id)
            if run_id not in self.m.runs:  # reopened while we read it
                await self.state.emit({"type": "RunReopened", "run": archived, "at": self.clock()})
        await self._control(run_id, "retry")
        return self._run_view(self.m.runs[run_id])

    # -- finished runs -------------------------------------------------------------------

    async def _archive_due(self):
        """Move finished runs from memory into the history, once none of
        their attempts is still in flight here (§7)."""

        busy = {run_id for run_id, _ in self.inflight.values()}
        for run_id in sorted(self.m.archivable):
            if run_id in busy:
                continue
            run = self.m.runs.get(run_id)
            if run is None or any(tid in self.m.claims for tid in run["tasks"]):
                continue
            await self.state.emit({"type": "RunArchived", "run": run_id, "at": self.clock()})

    # -- retention (§11) ---------------------------------------------------------------

    def _horizon(self, policy: dict | None, nth: float | None) -> float | None:
        """Runs older than this may go; `None` keeps everything. `nth` is
        when the policy's `runs`-th newest committing run was created. With
        both `days` and `runs`, whichever keeps more."""

        if policy is None:
            return None
        bounds = []
        if policy.get("days"):
            bounds.append(self.clock() - float(policy["days"]) * 86400)
        if policy.get("runs"):
            bounds.append(nth if nth is not None else -math.inf)
        return min(bounds)

    async def _retention_sweep(self):
        """Delete finished runs every asset they ran has let go of (§11): a
        run is kept while any of its assets keeps it, or keeps everything."""

        now = self.clock()
        if now - self._swept < self.retention_interval:
            return
        self._swept = now
        policies = {name: self.m.policy(name) for name in self.manifest["assets"]}
        keeps = {name: int(p["runs"]) for name, p in policies.items() if p and p.get("runs")}
        nth = await self.history.nth_newest(keeps)
        horizons = {name: self._horizon(p, nth.get(name)) for name, p in policies.items()}
        finite = [h for h in horizons.values() if h is not None]
        default = self._horizon(self.m.policy(None), None)
        if not finite and default is None:
            return
        latest = max(finite + ([default] if default is not None else []))
        doomed = []
        for run_id, created, assets, status in await self.history.older_than(latest):
            bounds = [horizons.get(a) for a in assets] if assets else [default]
            if all(h is not None and created < h for h in bounds):
                doomed.append((run_id, status))
        await self._delete_runs(doomed)

    async def _delete_runs(self, runs: list[tuple[str, str | None]]) -> None:
        """Delete finished runs, `(id, status)`: their attempt files and logs,
        then their history."""

        for run_id, status in runs:
            if status != "skipped":  # a skipped run launched nothing
                await self.state.delete_run(run_id)
        await self.history.delete([run_id for run_id, _ in runs])

    async def delete_run(self, run_id: str) -> None:
        """Delete a finished run: its history, attempt files and logs. Current
        state never depends on runs, so only a run in progress is refused."""

        if run_id in self.m.runs:
            raise Conflict(f"run {run_id} is still active", retryable=False)
        await self._delete_runs([(run_id, None)])

    async def prune(self, *, before=None, asset=None, keep=None, dry_run=False) -> dict:
        """Delete finished runs created before `before` (epoch seconds), of
        `asset` if given, except the `keep` newest of them."""

        doomed = await self.history.prunable(before=before, asset=asset, keep=keep)
        if not dry_run:
            await self._delete_runs(doomed)
        return {"deleted": [run_id for run_id, _ in doomed], "dry_run": bool(dry_run)}

    # -- read models -------------------------------------------------------------------

    def _run_view(self, run: dict) -> dict:
        if "source" in run:
            # A source commit: its record holds only what it adds (§7).
            at = ulid_time(run["id"])
            view = {"targets": [run["source"]], "partitions": "", "mode": "commit", "status": "succeeded"}
            return {**view, **run, "paused": False, "created_at": at, "updated_at": at, "tasks": []}
        view = {k: v for k, v in run.items() if k != "tasks"}
        view["tasks"] = sorted(run["tasks"])
        return view

    async def _run_view_of(self, run_id: str) -> dict:
        run = self.m.runs.get(run_id) or await self.history.run(run_id)
        if run is None:
            raise KeyError(run_id)
        return self._run_view(run)

    def _task_view(self, task: dict, live: bool) -> dict:
        view = {k: v for k, v in task.items() if k not in ("attempts", "launched")}
        claim = self.m.claims.get(task["id"]) if live else None
        attempts = task["attempts"]
        view["status"] = claim["status"] if claim else task["status"]
        view["generation"] = view["attempt_count"] = len(attempts) + (1 if claim else 0)
        latest = attempts[-1] if attempts else None
        if latest:
            view.setdefault("error", latest.get("error"))
            view.setdefault("outputs", latest.get("outputs"))
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
                view["outputs"] = a["outputs"]
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

    async def list_runs(
        self,
        filter: RunFilter | None = None,
        *,
        before: str | None = None,
        anchor: str | None = None,
        offset: int = 0,
        limit=50,
    ):
        """Runs, newest first — in progress and finished alike (§7)."""

        return await self.history.runs(
            filter or RunFilter(), before=before, anchor=anchor, offset=offset, limit=limit
        )

    async def run_detail(self, run_id: str):
        run = self.m.runs.get(run_id)
        live = run is not None
        if run is None:
            run = await self.history.run(run_id)
            if run is None:
                raise KeyError(run_id)
        tasks = [run["tasks"][tid] for tid in sorted(run.get("tasks") or {})]
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
            "unsettled": {
                o["name"]: sorted(s for (n, s) in self.m.unsettled if n == o["name"]) for o in info["outputs"]
            },
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
