"""The engine (§6–§10): control-plane only. It plans runs into per-(asset,
scope) tasks, resolves inputs to pinned heads, plans Incremental edges over
per-edge watermarks, dispatches attempts through placements, and commits
their results.

State lives in the model (model.py), changed only by events the engine records
(docs/object-store-state.md §3, §4). Recording is synchronous and never waits
on storage: the journal writes in the background. A precondition check and
the event that depends on it happen in one synchronous step, so no other
coroutine can interleave between them. The one wait is before launching an
attempt, until its launch is durable: an engine that restarts adopts it,
waits for its worker, and commits its result (§8). Storage upkeep (upkeep.py)
and the history lake run on loops of their own.

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
import secrets
from zoneinfo import ZoneInfo

from croniter import croniter
from obstore.exceptions import AlreadyExistsError
from solera.failures import lower
from solera.ids import ulid, ulid_time
from solera.keys import Rows, SortedRun
from solera.keys.index import (
    DeltaFiles,
    FileInfo,
    IndexState,
    KeyIndex,
    Options,
    delta_keys,
    key_bytes,
    key_str,
)
from solera.keys.io import ObjectIO
from solera.sdk import digest

from . import history, planning
from .attempts import POOL_OFFERED_GRACE, Attempts, Live
from .history import MAX_METADATA, History, RunFilter
from .keyservice import KeyService, cache_root
from .model import TERMINAL_RUN, commit_of, delta_reads
from .placements import PlacementContext, Registry
from .sensors import Sensors
from .state import Conflict, LostOwnership, State
from .upkeep import ALIVE, Upkeep
from .views import Views

log = logging.getLogger(__name__)

SUCCESS = {"succeeded", "skipped"}
TERMINAL = SUCCESS | {"failed", "blocked", "canceled"}
HEARTBEAT_SECONDS = 10.0  # a worker beats this often (docs/lifecycle.md §6)
PROVISION_SECONDS = 600.0  # a launched worker reports within this, or it never started
CANCEL_GRACE = 60.0  # a requested cancel's time to drain before it is forced (§7)
DISCARDS = 64  # data-garbage entries one attempt discards
SOURCE_KEYS_RECORDED = 1000  # a source commit's run lists changed keys up to this many, else counts
GRACE_SECONDS = 5.0


class Retryable(RuntimeError):
    """A dispatch-time failure that the retry policy may absorb."""


class NonRetryable(RuntimeError):
    """A dispatch-time failure no retry will fix (§8: full run required, …)."""


def _pages(keys: int, limit: int) -> int:
    """Pages of `limit` a delivery of `keys` is planned to take: at least one."""

    return max(1, -(-int(keys) // max(1, int(limit))))


class Engine(Attempts, Sensors, Views):
    Conflict = Conflict
    GRACE_SECONDS = GRACE_SECONDS

    def __init__(
        self,
        state: State,
        manifest: dict,
        *,
        registry: Registry | None = None,
        placements: dict | None = None,
        project: str = "",
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        provision_seconds: float = PROVISION_SECONDS,
        cancel_grace: float = CANCEL_GRACE,
        engine_url: str | None = None,
        pool_offered_grace: float = POOL_OFFERED_GRACE,
        concurrency: int = 4,
        clock=None,
        eval_interval: float = 0.5,
        key_options: Options | None = None,
        recount_interval: float = 3600.0,
        maintenance_concurrency: int = 2,
        retention_interval: float = 60.0,
        history: History | None = None,
        resolve_cache: str | None | bool = True,
        sensor_host=None,
    ):
        import time

        if heartbeat_seconds <= 0 or concurrency < 1:
            raise ValueError("Heartbeat interval and concurrency must be positive")
        self.state, self.manifest = state, manifest
        self.project = project
        self.clock = clock or time.time
        self.heartbeat_seconds, self.concurrency = heartbeat_seconds, concurrency
        self.provision_seconds, self.cancel_grace = provision_seconds, cancel_grace
        # Where workers reach this engine (docs/lifecycle.md §5); without one,
        # they report through `.worker` alone.
        self.engine_url = engine_url
        self.pool_offered_grace = pool_offered_grace
        self.secret: bytes | None = None  # signs attempt tokens; stable across restarts
        self.live: dict[str, Live] = {}  # attempt id -> what its worker reported
        self.pollers: dict[str, dict] = {}  # pool workers that asked for work lately
        self._pool_changed = asyncio.Event()
        self.eval_interval = eval_interval
        ctx = PlacementContext(state, state.objects_url, project, self.clock, self)
        self.registry = registry or Registry(ctx, extra=placements)
        # attempt id -> (run id, asyncio task): attempts this process is driving.
        self.inflight: dict[str, tuple[str, asyncio.Task]] = {}
        # attempt id -> set when its run is controlled, so its watcher looks at once.
        self._stirred: dict[str, asyncio.Event] = {}
        # Attempts consuming a local execution slot. Pool attempts only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.executor_inflight: dict[str, int] = {}
        self.runner: asyncio.Task | None = None
        self.last_error = None
        self._stopping = False
        self._firing: set[str] = set()
        self.key_options = key_options or Options()
        self._io: ObjectIO | None = None
        # The key cache and resolver (docs/resolved-commits.md §4–§5): where
        # `resolve_cache` says, else beside `file://` state or in a temporary
        # directory; None or False: off.
        self.keys: KeyService | None = None
        if resolve_cache:
            root = resolve_cache if isinstance(resolve_cache, str) else cache_root(state.objects_url)
            self.keys = KeyService(state.objects, root, options=self.key_options)
        self.history = history or History(state, clock=self.clock)
        self.upkeep = Upkeep(
            state,
            self.history,
            manifest,
            clock=self.clock,
            key_options=self.key_options,
            recount_interval=recount_interval,
            concurrency=maintenance_concurrency,
            retention_interval=retention_interval,
            keys=self.keys,
        )
        self._sensors_init(sensor_host)
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
        automation state and seeds source heads. Runs in progress record
        that the engine was down, since it last said it was alive."""

        m = self.m
        await self._load_secret()
        alive = await self.state.get_object(ALIVE)
        if alive is not None and any(run["status"] not in TERMINAL_RUN for run in m.runs.values()):
            now = self.clock()
            down = min(json.loads(alive)["at"], now)
            self.state.record({"type": "EngineRestarted", "down": down, "at": now})
        if (
            m.revision != self.manifest["revision"]
            or m.manifest != self.manifest
            or m.project != self.project
        ):
            self.state.record(
                {
                    "type": "ProjectRegistered",
                    "revision": self.manifest["revision"],
                    "manifest": self.manifest,
                    "project": self.project,
                    "at": self.clock(),
                }
            )

    async def _load_secret(self) -> bytes:
        """The secret attempt tokens are signed with: created once, kept in
        the namespace, so tokens in specs survive a restart (§5.2)."""

        if self.secret is None:
            path = "control/engine-secret"
            with contextlib.suppress(AlreadyExistsError):
                await self.state.create_object(path, secrets.token_hex(32).encode())
            self.secret = await self.state.get_object(path)
        return self.secret

    async def start(self):
        """Start the eval loop, and storage upkeep beside it. Its first tick
        adopts the attempts launched before a restart: their workers keep
        running, and this engine waits for them and commits their results (§8)."""

        self._stopping = False
        if self.keys is not None:
            try:
                self.keys.start()
            except Exception as e:  # an accelerator: without it, workers resolve their writes
                log.warning("key cache disabled: %s", e)
        self.runner = asyncio.create_task(self._loop())
        self.upkeep.start()
        self.history.start()
        self._start_sensor_host()

    async def stop(self):
        self._stopping = True
        if self.runner:
            self.runner.cancel()
            try:
                await self.runner
            except asyncio.CancelledError:
                pass
            self.runner = None
        await self._stop_sensor_host()
        if self.inflight:
            # Launched attempts keep running: the next engine adopts them.
            jobs = [t for _, t in self.inflight.values()]
            for job in jobs:
                job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            self.inflight.clear()
        await self.upkeep.stop()
        await self.history.stop()
        if self.keys is not None:
            await self.keys.stop()

    async def _loop(self):
        """Tick whenever state changes — a submit, a finished attempt, the
        tick's own events — and every `eval_interval` for what comes due
        with time: retries, schedules, timeouts."""

        while not self._stopping:
            self.state.changed.clear()
            try:
                await self.tick()
                self.last_error = None
            except Exception as error:
                self.last_error = f"{type(error).__name__}: {error}"
                log.exception("engine tick failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.state.changed.wait(), self.eval_interval)

    @property
    def failing(self) -> str | None:
        """What last went wrong, in the eval loop or in storage upkeep."""

        return self.last_error or self.upkeep.last_error

    async def tick(self):
        """One evaluation pass: adoption, dispatch, automations, archiving."""

        self._adopt()
        self._dispatch_due()
        self._sensor_sweep()
        await self._automation_tick()
        await self._retry_tick()
        self._archive_due()

    async def run_until(self, run_id: str, timeout: float = 120.0):
        """Tick until the run reaches a terminal status (CLI and tests)."""

        deadline = self.clock() + timeout
        while self.clock() < deadline:
            self.state.changed.clear()
            await self.tick()
            run = self.m.runs.get(run_id) or await self.history.run(run_id)
            if run and run["status"] in TERMINAL:
                # Wait only on this run's attempts — unrelated pool-placed runs
                # may hold inflight waiters until a worker claims them.
                mine = [t for r, t in self.inflight.values() if r == run_id]
                await asyncio.gather(*mine, return_exceptions=True)
                detail = await self.run_detail(run_id)
                self._archive_due()  # a one-shot caller (the CLI) leaves nothing behind
                return detail
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.state.changed.wait(), self.eval_interval)
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
        skip_missing_inputs=False,
        by=None,
        tags=None,
        retry_of=None,
    ):
        """A run request becomes one task per (asset, scope) (§8). `by` says
        who asked (the API or CLI, or what the caller names); automation runs
        carry `automation` instead. `tags` label the run for finding it later.
        `skip_active` and `skip_missing_inputs` leave out the scopes already
        in flight, or with an input never written; `None` if none is left."""

        if command_id and command_id in self.m.receipts:  # answered once it is durable
            await self.state.durable()
            return await self._run_view_of(self.m.receipts[command_id])
        run = self._plan_run(
            targets,
            partitions,
            mode,
            upstream,
            config,
            keys,
            automation=automation,
            skip_active=skip_active,
            skip_missing_inputs=skip_missing_inputs,
            by=by,
            tags=tags,
            retry_of=retry_of,
        )
        if run is None:
            return None
        if command_id and command_id in self.m.receipts:  # submitted while we planned
            return await self._run_view_of(self.m.receipts[command_id])
        self.state.record({"type": "RunSubmitted", "run": run, "command": command_id})
        return self._run_view(self.m.runs.get(run["id"]) or run)

    def planner(self, projected: dict | None = None) -> planning.Planner:
        """Planning over the committed heads — with `projected` heads, which a
        sensor's commits are about to install, over them — and the engine's
        clock."""

        return planning.Planner(
            self.manifest, lambda o, s: self.m.heads.get((o, s)), self.m.heads_of, self.clock(), projected
        )

    def _plan_run(
        self,
        targets,
        partitions="latest",
        mode="incremental",
        upstream=False,
        config=None,
        keys=None,
        *,
        projected=None,
        **options,
    ) -> dict | None:
        """The run a request becomes, without submitting it (`planning.Planner.plan_run`)."""

        return self.planner(projected).plan_run(
            targets, partitions, mode, upstream, config, keys, active=self._scope_active, **options
        )

    def _scope_active(self, asset: str, scope: str) -> bool:
        return self._scope_active_claim(asset, scope) or self.m.is_pending(asset, scope)

    # -- attempt outcomes ------------------------------------------------------------

    def _finish(
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
        worker=None,
        end=None,
        reason=None,
        writes=None,
        retry_for=None,
        keys=None,
    ):
        """End an attempt. `worker` is what its worker reported — its result,
        or its last heartbeat: the events it recorded and what it used. `end`
        names how it ended if its outcome does not say (`aborted`, `lost`),
        `reason` why, and `writes` what is known of its writes
        (docs/lifecycle.md §2.3)."""

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
            if retry_for is not None:
                event["retry_for"] = float(retry_for)
        if commit is not None:
            event["commit"] = commit
        if more:
            event["more"] = True
        if unsettled:
            event["unsettled"] = unsettled
        if worker:
            event["worker"] = {k: worker[k] for k in ("events", "usage") if worker.get(k)}
        if end is not None:
            event["end"] = end
        if reason is not None:
            event["reason"] = str(reason)[:200]
        if writes is not None:
            event["writes"] = writes
        for field in ("discarded", "discard_unresolved", "discarded_files"):  # data garbage (§9.8)
            if (worker or {}).get(field):
                event[field] = worker[field]
        if keys:
            event["keys"] = keys  # an Each attempt's keys by outcome
        self.state.record(event)
        if self.keys is not None:
            for name, keys in ((commit or {}).get("keys") or {}).items() if outcome == "succeeded" else ():
                self._cache_commit(name, task["scope"], keys)
            self.keys.ended(claim["attempt"])

    def _cache_commit(self, name: str, scope: str, keys: dict | None) -> None:
        """Keep the engine's cache warm with what a commit installed (§5), and a
        summary of a small delta for inlined pages (§7)."""

        if not keys or not keys.get("files"):
            return
        index = self.m.indexes.get((name, scope))
        if index is not None:
            files = [FileInfo.from_json(f) for f in keys["files"]]
            batch = int(keys["batch"])
            logged = bool(index.log) and index.log[-1][0] == batch  # a consumer will read it
            self.keys.committed(index.prefix, index.path, batch, files, logged, self.m.applied)

    # -- dispatch ---------------------------------------------------------------

    def _dispatch_due(self):
        """Claim the tasks that are due, as far as the engine, their
        executors and their scopes allow. Those held back are recorded, with
        why, when that changes."""

        now = self.clock()
        engine_used = len(self.engine_inflight)
        executor_used = dict(self.executor_inflight)
        held = {}
        for task_id in self.m.due(now):
            task = self.m.task(task_id)
            if task is None or task["status"] != "queued" or task_id in self.m.claims:
                continue
            run = self.m.runs[self.m.task_run[task_id]]
            if run["status"] == "canceled" or run.get("paused"):
                continue
            spec = self.manifest["assets"][task["asset"]]["placement"]
            placement = self.registry.build(spec)
            executor = spec["executor"]
            limit = getattr(placement, "max_concurrent", None)
            is_pool = spec["kind"] == "Pool"
            holder = self.m.locks.get((task["asset"], task["scope"]))
            if holder is not None and self.m.claimed(holder) is not None:
                held[task_id] = ["lock", holder]
            elif not is_pool and engine_used >= self.concurrency:
                held[task_id] = ["engine", None]
            elif limit is not None and executor_used.get(executor, 0) >= limit:
                held[task_id] = ["executor", executor]
            if task_id in held:
                if task.get("held") == held[task_id]:
                    del held[task_id]
                continue
            attempt = ulid(now)
            self.m.claim(task_id, attempt, now)
            executor_used[executor] = executor_used.get(executor, 0) + 1
            engine_used += not is_pool
            self._spawn(task["run"], attempt, is_pool, executor, self._attempt(task_id, attempt, placement))
        if held:
            self.state.record({"type": "TasksHeld", "held": held, "at": now})

    def _spawn(self, run_id: str, attempt: str, is_pool: bool, executor: str, work):
        """Drive one attempt in the background, holding its execution slots."""

        if not is_pool:
            self.engine_inflight.add(attempt)
        self.executor_inflight[executor] = self.executor_inflight.get(executor, 0) + 1

        async def drive():
            try:
                await work
            finally:  # before anyone awaiting the attempt resumes
                self.inflight.pop(attempt, None)
                self._stirred.pop(attempt, None)
                self.live.pop(attempt, None)
                self.engine_inflight.discard(attempt)
                self.executor_inflight[executor] = max(0, self.executor_inflight.get(executor, 1) - 1)

        self.inflight[attempt] = (run_id, asyncio.create_task(drive()))

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
            work = self._resume(task_id, attempt, placement)
            self._spawn(task["run"], attempt, execution["kind"] == "Pool", execution["executor"], work)

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
                    self._finish(
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
                    if plan is not None and plan.get("update", True) is not None
                }
                self._finish(task, claim, "skipped", commit={"watermarks": watermarks})
                return
            stage = await self._launch(task, run, attempt, prepared)
            try:
                handle = await placement.launch(stage)
            except Exception as error:
                await self._fail(task_id, attempt, f"launch: {error}", retryable=True, reason="launch")
                return
            self._placed(attempt, handle)
            await self._watch(task_id, attempt, placement, handle)
            self._released(placement, handle)
        except LostOwnership:
            return
        except Exception as error:  # never leave a claim behind
            await self._crashed(task_id, attempt, error)
        finally:
            self.m.release(task_id, attempt)

    @staticmethod
    def _released(placement, handle) -> None:
        """Settled: what the placement kept of the launch may go."""

        if handle is not None and callable(getattr(placement, "release", None)):
            placement.release(handle)

    async def _resume(self, task_id: str, attempt: str, placement):
        """An adopted attempt: wait for it and settle it, as `_attempt` would
        have — through the handle its launch recorded. With none (the engine
        stopped before it was recorded, or before the launch), a placement
        that names its runs after their attempts finds or starts it again
        (`resume`); any other is followed through the worker's reports."""

        try:
            task = self.m.task(task_id)
            handle = self._launched(task, attempt).get("handle")
            if handle is None and callable(getattr(placement, "resume", None)):
                stage = {"attempt": attempt, "run": task["run"], "objects": self.state.objects_url}
                try:
                    handle = await placement.resume(stage)
                    self._placed(attempt, handle)
                except Exception:
                    log.exception("attempt %s: resuming its placement failed", attempt)
            await self._watch(task_id, attempt, placement, handle, adopted=True)
            self._released(placement, handle)
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
                await self._fail(task_id, attempt, f"engine: {error}", retryable=True, reason="engine")
            else:
                self._finish(task, claim, "failed", error=f"engine: {error}", retryable=True, reason="engine")

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
        planner = self.planner()
        try:
            edges = planner.edges(task["asset"], scope)
        except planning.UpstreamOnly as error:
            raise Conflict(str(error)) from None
        # Pass 1: pin every non-Incremental edge; their refs enter the fingerprint (§6).
        inputs, pinned = {}, {}
        incremental = []
        for edge in edges:
            param, output = edge.param, edge.output
            if edge.kind == "all_partitions":
                refs = self._all_partitions(planner, edge)
                inputs[param] = {"refs": refs}
                indexes = {k: self._whole_index(output, ref) for k, ref in refs.items()}
                if any(i is not None for i in indexes.values()):
                    inputs[param]["indexes"] = {k: i for k, i in indexes.items() if i is not None}
                pinned[param] = refs
            elif edge.kind == "dep":
                # Across upstream-only dimensions, a dep pins the heads that exist and
                # agree with this scope's keys; otherwise its one projected head (§7).
                if edge.fan_in:
                    refs = {k: h["ref"] for k, h in planner.fan_in(edge, complete=False).items()}
                else:
                    refs = {edge.scope: self._pin_at(output, edge.scope)}
                inputs[param] = {"refs": refs}
                # A bound partition set pins into lineage, but it is the dimension — not
                # interpretation: adding a key must not invalidate existing ones.
                if not edge.set_dim:
                    pinned[param] = refs
            elif edge.kind == "incremental":
                incremental.append(edge)
            else:
                inputs[param] = {"ref": self._pin_at(output, edge.scope)}
                if (index := self._whole_index(output, inputs[param]["ref"])) is not None:
                    inputs[param]["index"] = index
                pinned[param] = inputs[param]["ref"]
        fingerprint = self._fingerprint(asset, run, pinned)
        if full and run["mode"] == "full" and incremental:
            # This run's reset began the pass every edge is on: resume it, page by page.
            started = [
                (self.m.watermarks.get((task["asset"], e.param, scope)) or {}).get("pass")
                for e in incremental
            ]
            if all(s == run["id"] for s in started):
                full = False
        # Pass 2: Incremental plans against the fingerprinted interpretation (§2.2).
        plans, all_empty, each_page = {}, True, None
        for edge in incremental:
            param, up_scope = edge.param, edge.scope
            ref = self._pin_at(edge.output, up_scope)
            claim = self.m.claimed(attempt) if attempt is not None else None
            pin, plan, empty = self._incremental_plan(
                task, param, edge.spec, ref, up_scope, fingerprint, run, full, (claim or {}).get("pin")
            )
            if edge.spec.get("each") is not None:
                pin, plan, empty = self._each_plan(
                    task, asset, param, edge.spec, ref, up_scope, pin, plan, empty
                )
                each_page = pin["each"]
            inputs[param] = pin
            plans[param] = plan
            all_empty = all_empty and empty
        claim = self.m.claimed(attempt) if attempt is not None else None
        if claim is not None:
            # Keep the delta log this attempt reads until it finishes (§6).
            claim["reads"] = delta_reads(plans)
        more = any(p.get("more") for p in plans.values() if p)
        skip = bool(incremental) and all_empty and not more and not full
        # An Each asset whose keys all failed so far has no head yet: nothing to wait for.
        if skip and each_page is None:
            for output in asset["outputs"]:
                head = baseline[output["name"]]
                if head is None or not head["complete"]:
                    skip = False
                    break
        prior = {name: head["ref"] for name, head in baseline.items() if head is not None}
        cursor = self.m.cursors.get((task["asset"], scope))
        if full and each_page is None:
            # Withheld: the write is the whole content. An Each page keeps its prior:
            # it patches by key, and a key that fails keeps its last good output (§5).
            prior, cursor = {}, None
        outputs = {}
        for output in asset["outputs"]:
            name, head = output["name"], baseline[output["name"]]
            # The contract it is launched under: settled by it, whatever is served by then.
            info = {"exists": head is not None, "decl": {k: output.get(k) for k in ("key", "incremental")}}
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
            if due := self._due_discards(name, scope, attempt):
                info["discard"] = due
            outputs[name] = info
        # The input versions its outputs will be built from, for the history (§7).
        lineage = []
        for edge in edges:
            pin = inputs.get(edge.param) or {}
            refs = [pin["ref"]] if pin.get("ref") else list((pin.get("refs") or {}).values())
            for ref in refs:
                lineage.append([edge.output, ref.get("partition") or "", ref.get("version"), edge.param])
        return {
            "version": asset["version"],
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
            # An Each page's failure delta, for discarding if it never commits.
            "failures": None
            if each_page is None
            else {
                "prefix": self.m.index(f"@{task['asset']}", scope).prefix,
                "batch": each_page["batch"],
            },
        }

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
        kept = {
            k: prepared.get(k)
            for k in ("version", "baseline", "plans", "more", "full", "prior", "lineage", "failures")
        }
        return {**kept, "outputs": outputs}

    def _pin_at(self, output: str, up_scope: str):
        """Head ref for one upstream scope; sources synthesize theirs (§5, §8)."""

        head = self.m.heads.get((output, up_scope))
        if head is None:
            source = self.manifest["sources"].get(output)
            if source is not None and up_scope == "":
                return dict(source["head"])
            raise Retryable(f"input {output!r} has no head for scope {up_scope!r}")
        return head["ref"]

    def _whole_index(self, output: str, ref: dict) -> dict | None:
        """The pinned key index a whole read of a keyed output on an immutable
        store needs to name its objects (docs/lifecycle.md §9.8)."""

        record = self.manifest["outputs"].get(output) or {}
        store = self.manifest["stores"].get(record.get("store")) or {}
        if record.get("key") is None or record.get("partition_set") or store.get("writes") != "immutable":
            return None
        return self.m.index(output, ref.get("partition") or "").pinned().to_json()

    @staticmethod
    def _all_partitions(planner: planning.Planner, edge: planning.Edge) -> dict:
        """AllPartitions pins every current upstream partition with a complete
        head that agrees with this scope's shared keys, keyed by the dimensions
        it collapses (§7) — chosen among the heads that exist, never by
        expanding the partition domain."""

        if not edge.fan_in:
            head = planner.head(edge.output, edge.scope)
            return {"": head["ref"]} if head is not None and head["complete"] else {}
        return {edge.key(s): h["ref"] for s, h in planner.fan_in(edge, complete=True).items()}

    def _incremental_plan(self, task, param, edge, ref, up_scope, fingerprint, run, full, claim_pin=None):
        """Plan one Incremental edge from its watermark (§6): returns the pin
        for the spec, the plan the commit turns into the next watermark, and
        whether nothing is pending.

        A keyed upstream is read through its key index by the harness: the
        delta log from `wm.batch` to the head, or — for a missing watermark, a
        fingerprint change, a `full` run, a keys='full' override, or a log that
        no longer holds the window — the whole index, restarting from the
        head's next batch so changes made while draining arrive afterwards as
        deltas. Either is delivered `page_size` keys at a time; the harness
        reports where it stopped (`after`), and a task with more to deliver is
        queued again. A batch-mode upstream is planned here: the next
        `page_size` batches after the watermark, all since the last reset
        (`base`) when starting over.

        Watermark: {"batch", "until"?, "after", "full", "fingerprint", "output", "up"}."""

        output = edge["output"]
        keyed = self.manifest["outputs"][output].get("key") is not None
        limit = int(edge.get("page_size") or 100)
        head = self.m.heads.get((output, up_scope)) or {}
        head_batch = int(head.get("batch", -1))
        override = (run.get("keys") or {}).get(output)
        wm = self.m.watermarks.get((task["asset"], param, task["scope"]))
        # A `full` run or a keys="full" override starts one pass per run, which the
        # run's later attempts resume (`pass` on the watermark) instead of restarting.
        again = override == "full" and (wm or {}).get("pass") != run["id"]
        reset = full or wm is None or wm.get("fingerprint") != fingerprint or again
        # A reset starts a pass, which the run's later attempts resume (`pass` on the
        # watermark) instead of starting over at every page.
        base = {"output": output, "up": up_scope, "fingerprint": fingerprint}
        base["pass"] = run["id"] if reset else (wm or {}).get("pass")
        if edge.get("each") is not None:
            # A full delivery of an Each edge ends with a cleanup of the keys it no longer
            # names — needed only if the asset held keys when the delivery began (§11).
            held = [o["name"] for o in self.manifest["assets"][task["asset"]]["outputs"]] + [
                f"@{task['asset']}"
            ]
            base["cleanup"] = (
                any(self.m.index(name, task["scope"]).count for name in held)
                if reset
                else bool((wm or {}).get("cleanup"))
            )

        # A keys= override is a one-off selection — it never moves the watermark. The
        # edge's patterns still decide which of the keys it takes (per-key §11).
        if isinstance(override, dict) and "keys" in override and not reset:
            keys = sorted({str(k) for k in override["keys"]})
            pin = {"ref": ref, "changes": {"keys": keys, "full": False}}
            if keyed:  # the keys' versions and locators, for the store to find them
                pin["index"] = self.m.index(output, up_scope).pinned().to_json()
            if edge.get("patterns") is not None:
                pin["patterns"] = edge["patterns"]
            return pin, None, not keys

        if not keyed:
            first = int(head.get("base", 0))
            reset = reset or int(wm["batch"]) < first
            lo = first if reset else int(wm["batch"])
            hi = min(head_batch, lo + limit - 1)
            # Where this page sits in its delivery: planned when the delivery starts,
            # kept on the watermark while it continues (§5).
            if not reset and (wm or {}).get("page") is not None:
                page, pages = int(wm["page"]), int(wm["pages"])
            else:
                page, pages = 0, _pages(head_batch - lo + 1, limit)
            more = hi < head_batch
            changes = {
                "batches": [lo, hi],
                "full": reset,
                "more": more,
                "page": page,
                "pages": pages,
            }
            update = {**base, "batch": max(lo, hi + 1), "after": None, "full": False}
            if more:
                update.update(page=page + 1, pages=pages)
            return {"ref": ref, "changes": changes}, {"update": update, "more": more}, hi < lo

        index = self.m.index(output, up_scope)
        patterns = edge.get("patterns")
        base["patterns"] = patterns
        rescope = None if reset else wm.get("rescope")
        if not reset and rescope is None and wm.get("patterns") != patterns:
            # The edge's patterns changed: cut over at the upstream's head (per-key §11).
            # Changes up to it finish under the old patterns, then membership is
            # diffed against the index as of the cutover, pinned until the diff ends.
            rescope = {
                "from": wm.get("patterns"),
                "to": patterns,
                "cutover": head_batch,
                "snapshot": index.pinned().to_json(),
                "pin": claim_pin if claim_pin is not None else self.m.applied,
                "after": None,
            }
        if rescope is not None:
            base["patterns"], base["rescope"] = rescope["from"], rescope
            if not wm.get("full") and int(wm["batch"]) > rescope["cutover"]:
                if rescope["after"] is not None and wm.get("page") is not None:
                    page, pages = int(wm["page"]), int(wm["pages"])
                else:
                    page, pages = 0, _pages(IndexState.from_json(rescope["snapshot"]).count, limit)
                pin = {
                    "ref": ref,
                    "index": rescope["snapshot"],
                    "changes": {
                        "rescope": {"from": rescope["from"], "to": rescope["to"]},
                        "after": rescope["after"],
                        "limit": limit,
                        "page": page,
                        "pages": pages,
                    },
                }
                plan = {**base, "diff": True, "batch": int(wm["batch"]), "page": page, "pages": pages}
                return pin, plan, False
            head_batch = min(head_batch, rescope["cutover"])  # finish: under the old patterns
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
            if rescope is not None:  # a full delivery is under the new patterns: no diff left
                base.pop("rescope")
                base["patterns"] = patterns
            if edge.get("each") is not None:
                # What the lost log held — deletions, keys the patterns now leave out —
                # is found by the cleanup after the delivery (§11).
                base["cleanup"] = True
        if base.get("rescope") is not None:
            empty = False  # the transition has its diff still to do
        pinned = index.pinned() if window["full"] else index.pinned(window["from"], window["to"])
        # Where this page sits in its delivery (§5): planned when the delivery starts —
        # the keys in the whole index or in the window's delta files, by `page_size`,
        # an estimate when patterns filter or a count is inexact — and kept on the
        # watermark while the delivery continues.
        if window["after"] is not None and wm is not None and wm.get("page") is not None:
            page, pages = int(wm["page"]), int(wm["pages"])
        else:
            keys = (
                pinned.count if window["full"] else sum(f.entries for _, files in pinned.log for f in files)
            )
            page, pages = 0, _pages(keys, limit)
        changes = {**window, "limit": limit, "page": page, "pages": pages}
        pin = {"ref": ref, "index": pinned.to_json(), "changes": changes}
        if not window["full"] and not empty and self.keys is not None:
            # The first page of the pinned window, from summaries in memory (§7 of
            # docs/resolved-commits.md): the worker then reads no delta file.
            inline = self.keys.inline(index.prefix, window["from"], window["to"], window["after"], limit)
            if inline is not None:
                pin["changes"]["inline"] = inline
        if base["patterns"] is not None:
            pin["patterns"] = base["patterns"]  # the worker filters the page, inlined or read
        plan = {**base, **window, "page": page, "pages": pages}
        if not window["full"]:  # a window paged over attempts holds its first page's reader pin
            plan["pin"] = wm.get("pin") if window["after"] is not None and wm else claim_pin
        return pin, plan, empty

    # -- Each pages (docs/per-key-processing.md §5, §9) ------------------------------

    def _forced_pos(self, record: dict) -> int:
        return max((record.get("forced") or {}).values(), default=0)

    def _has_retries(self, record: dict | None) -> bool:
        """Whether a scope's failure record has keys to retry now: a pass in
        progress, a key due, a failed key not yet tried under this revision
        epoch, or a forced request newer than the last pass completed. The
        bounds are conservative: a pass may find nothing, which makes them
        exact (§9)."""

        if not record:
            return False
        due, epoch_min = record.get("due"), record.get("epoch_min")
        return bool(
            record.get("retry")
            or (due is not None and due <= self.clock())
            or (epoch_min is not None and epoch_min < self.m.epoch)
            or self._forced_pos(record) > int(record.get("done_forced") or 0)
        )

    def _each_plan(self, task, asset, param, edge, ref, up_scope, pin, plan, empty):
        """An Each edge's page: the changes of its window, or the keys its
        failure index has due again. When both are pending they alternate —
        neither starves, and there is no fraction to tune (§9). A full
        delivery reprocesses every key anyway, so retries wait for it."""

        key = (task["asset"], task["scope"])
        record = self.m.failures.get(key) or {}
        failures = self.m.index(f"@{task['asset']}", task["scope"])
        changes = not empty
        wm = self.m.watermarks.get((task["asset"], param, task["scope"]))
        # After a full delivery, the output's keys it no longer names go first (§11).
        reconcile = None if (plan or {}).get("full") else (wm or {}).get("reconcile")
        # A full delivery reprocesses every key, a pattern transition and its cleanup
        # decide which keys are the edge's: retries wait for them to end.
        transition = (
            (plan or {}).get("full") or (plan or {}).get("rescope") is not None or reconcile is not None
        )
        retries = not transition and self._has_retries(record)
        if reconcile is not None:
            kind = "reconcile"
        elif changes and retries:
            kind = "retry" if record.get("last") == "changes" else "changes"
        else:
            kind = "retry" if retries else "changes"
        forced = dict(record.get("forced") or {})
        current = self._forced_pos(record)
        retry = record.get("retry")
        if retry is not None and (retry["epoch"] != self.m.epoch or retry["forced_pos"] != current):
            retry = None  # its predicate's inputs moved: the pass starts over (§9)
        each = {
            "kind": kind,
            "concurrency": edge["each"]["concurrency"],
            "epoch": self.m.epoch,
            "forced": forced,
            "forced_pos": current,
            "now": self.clock(),
            "retries": asset.get("retries", {}).get("n", 0),
            "failures": failures.pinned().to_json(),
            "batch": int(record.get("batch", -1)) + 1,
            "pass_after": (retry or {}).get("after"),
        }
        limit = int(edge.get("page_size") or 100)
        if kind == "reconcile":
            pin = {
                "ref": ref,
                "index": self.m.index(edge["output"], up_scope).pinned().to_json(),
                "changes": {"reconcile": {"after": reconcile["after"]}, "limit": limit},
                "each": each,
            }
            if wm.get("patterns") is not None:
                pin["patterns"] = wm["patterns"]
            return pin, {"update": wm, "each": {"kind": "reconcile", "changes": changes}}, False
        if kind == "retry":
            if retry is None:
                retry = {
                    "pass": int(record.get("passes") or 0) + 1,
                    "epoch": self.m.epoch,
                    "forced_pos": current,
                    "after": None,
                    "due_acc": None,
                    "epoch_acc": None,
                }
            each["pass_after"] = retry["after"]
            pin = {
                "ref": ref,
                "index": self.m.index(edge["output"], up_scope).pinned().to_json(),
                "changes": {"retry": {"after": retry["after"]}, "limit": limit},
                "each": each,
            }
            if (wm or {}).get("patterns") is not None:
                pin["patterns"] = wm["patterns"]  # a due key the edge no longer takes goes
            return pin, {"update": wm, "each": {"kind": "retry", "pass": retry, "changes": changes}}, False
        pin = {**pin, "each": each}
        page = {"kind": "changes", "retries": retries, "pass": retry}
        # A keys= override is a one-off selection: no watermark moves (plan None).
        plan = {"update": None, "each": page} if plan is None else {**plan, "each": page}
        return pin, plan, empty

    def _each_commit(self, task, plan: dict, result: dict) -> tuple[dict, bool, dict | None]:
        """What an Each page commits to its failure record, and whether the
        task has more to do (§9): the outcome counts move by the page's
        transitions; the bounds are lowered by the records it wrote; a retry
        page advances its pass and folds the range it walked into the pass's
        accumulators, which become the exact bounds when the pass completes;
        a change page folds what it wrote behind the pass's position; a
        reconcile page moves the cleanup on. Returns the record's commit, the
        `more`, and the watermark a reconcile page leaves (else None)."""

        record = self.m.failures.get((task["asset"], task["scope"])) or {}
        run = self.m.runs.get(task["run"]) or {}
        report = result.get("failures") or {}
        counts = dict(record.get("counts") or {})
        for name, delta in (report.get("counts") or {}).items():
            counts[name] = counts.get(name, 0) + int(delta)
        counts = {k: v for k, v in counts.items() if v}
        due = lower(record.get("due"), report.get("due"))
        epoch_min = lower(record.get("epoch_min"), report.get("epoch_min"))
        page = plan["each"]
        commit = {
            "keys": report.get("keys") or {"files": []},
            "batch": int(record.get("batch", -1)) + 1,
            "counts": counts,
            "last": page["kind"],
            # The configuration the scope runs under, for the runs retries start (§9).
            "config": run.get("config") or {},
        }
        retry, more, watermark = page.get("pass"), False, None
        delivered = (result.get("delivered") or {}).get(plan.get("param") or "", {})
        # A forced request newer than the pass in progress, or than the last one done,
        # is owed a pass: this run takes it rather than waiting for unrelated activity.
        forced_after = self._forced_pos(record) > int(
            (retry or {}).get("forced_pos", record.get("done_forced") or 0)
        )
        if page["kind"] == "reconcile":
            after = delivered.get("after")
            wm = dict(plan["update"])
            if after is None:
                wm.pop("reconcile", None)
            else:
                wm["reconcile"] = {"after": after}
            watermark = wm
            more = after is not None or bool(page.get("changes")) or forced_after
        elif page["kind"] == "retry":
            walked = report.get("range") or {}
            retry = {
                **retry,
                "due_acc": lower(retry.get("due_acc"), walked.get("due")),
                "epoch_acc": lower(retry.get("epoch_acc"), walked.get("epoch_min")),
            }
            after = delivered.get("after")
            if after is None:  # the pass is complete: its accumulators are the exact bounds
                due, epoch_min = retry["due_acc"], retry["epoch_acc"]
                commit.update({"passes": retry["pass"], "done_forced": retry["forced_pos"], "retry": None})
                more = bool(page.get("changes")) or forced_after
            else:
                commit["retry"] = {**retry, "after": after}
                more = True
        else:
            if retry is not None:
                fold = report.get("fold") or {}
                commit["retry"] = {
                    **retry,
                    "due_acc": lower(retry.get("due_acc"), fold.get("due")),
                    "epoch_acc": lower(retry.get("epoch_acc"), fold.get("epoch_min")),
                }
            # Keys this page left due at once are retried in the same run.
            more = (
                bool(page.get("retries"))
                or (report.get("due") is not None and report["due"] <= self.clock())
                or forced_after
            )
        commit.update({"due": due, "epoch_min": epoch_min})
        return commit, more, watermark

    @classmethod
    def _watermark(cls, plan: dict, after: str | None) -> dict:
        """The watermark after delivering a keyed plan's page, which ended at
        `after` (`None`: the window is done). A delivery that continues keeps
        its page plan: the next page's index, and how many it planned (§5)."""

        wm = cls._next_watermark(plan, after)
        if after is not None and plan.get("pages") is not None:
            wm.update(page=int(plan["page"]) + 1, pages=int(plan["pages"]))
        return wm

    @staticmethod
    def _next_watermark(plan: dict, after: str | None) -> dict:
        """The watermark a keyed plan's page leads to. A delta window delivered
        over several attempts keeps the reader pin of the attempt that began
        it: its later pages still read versions as of then (lifecycle.md §9.8)."""

        base = {k: plan[k] for k in ("output", "up", "fingerprint")}
        if plan.get("pass") is not None:  # the run whose reset began this pass
            base["pass"] = plan["pass"]
        if plan.get("cleanup") and plan["full"]:  # a full Each delivery owes a cleanup (§11)
            base["cleanup"] = True
        if plan.get("patterns") is not None:  # what the edge delivers under (per-key §11)
            base["patterns"] = plan["patterns"]
        rescope = plan.get("rescope")
        if plan.get("diff"):  # a rescope's membership diff (per-key §11)
            if after is None:  # done: the new patterns from the cutover on
                done = {**base, "batch": plan["batch"], "after": None, "full": False}
                done.pop("patterns", None)
                return {**done, "patterns": rescope["to"]} if rescope["to"] is not None else done
            return {
                **base,
                "batch": plan["batch"],
                "after": None,
                "full": False,
                "rescope": {**rescope, "after": after},
            }
        if rescope is not None:
            base["rescope"] = rescope
        if plan["full"]:
            if after is None:
                done = {**base, "batch": plan["from"], "after": None, "full": False}
                done.pop("cleanup", None)
                if plan.get("each") is not None and plan.get("cleanup"):
                    # An Each output may hold keys the delivery no longer names — gone
                    # upstream, or left out by the patterns: reconcile them next (§11).
                    done["reconcile"] = {"after": None}
                return done
            return {**base, "batch": plan["from"], "after": after, "full": True}
        if after is None:
            return {**base, "batch": max(plan["from"], plan["to"] + 1), "after": None, "full": False}
        paged = {**base, "batch": plan["from"], "until": plan["to"], "after": after, "full": False}
        return {**paged, "pin": plan["pin"]} if plan.get("pin") is not None else paged

    def _due_discards(self, output: str, scope: str, attempt: str | None) -> list[dict]:
        """The data garbage of an immutable output's scope that no reader can
        still need: every entry let go of before the oldest reader pin but this
        attempt's own, which reads none of it. At most `DISCARDS` of them, for
        this attempt to discard (§9.8)."""

        entries = self.m.discards.get((output, scope))
        if not entries:
            return []
        floor = self.m.pin_floor(but=attempt)
        return [e for e in entries if e["n"] <= floor and not e.get("stuck")][:DISCARDS]

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

    # -- the commit (§8) ---------------------------------------------------------------

    async def commit_attempt(
        self,
        attempt: str,
        prepared: dict,
        result: dict,
        *,
        outcome: str = "succeeded",
        error: str | None = None,
        retryable: bool = False,
        delay: float = 0.0,
    ) -> dict:
        """Install an attempt's result: heads, cursor, edge watermarks (§8).
        A drained Each page commits as the attempt it ended as: `canceled`, or
        `failed` (a timeout, retryable).

        Every precondition is checked against the model, and the event recorded,
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
            if commit_of(self.m.heads.get((output, task["scope"]))) != commit_of(baseline):
                raise Conflict(f"output {output} head changed since this attempt was claimed")
        outputs = result.get("outputs") or {}
        # Settled under the contract it was launched with, not today's manifest.
        declared = {name: info["decl"] for name, info in (prepared.get("outputs") or {}).items()}
        # Where each keyed Incremental page ended decides the next watermark.
        delivered = result.get("delivered") or {}
        watermarks, more = {}, bool(prepared.get("more"))
        failures = None
        for param, plan in (prepared.get("plans") or {}).items():
            if plan is None:
                continue
            if "each" in plan:
                failures, each_more, reconciled = self._each_commit(task, {**plan, "param": param}, result)
                more = more or each_more
                if reconciled is not None:
                    watermarks[param] = reconciled
                    continue
            if "update" in plan:
                if plan["update"] is not None:
                    watermarks[param] = plan["update"]
                continue
            if param not in delivered:
                raise Conflict(f"input {param}: the result reports no delivery", retryable=False)
            after = delivered[param].get("after")
            watermarks[param] = self._watermark(plan, after)
            # A pattern transition that finished its old-pattern changes has its diff to do;
            # a full Each delivery that finished has its cleanup to do.
            more = (
                more
                or after is not None
                or (plan.get("rescope") is not None and not plan.get("diff"))
                or "reconcile" in watermarks[param]
            )
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
            head = {"ref": ref, "complete": not more, "asset": task["asset"], "version": prepared["version"]}
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
            # An Each page whose keys all failed writes nothing, and makes no head yet;
            # nor does a page whose keys the edge's patterns all left out.
            if prepared["baseline"].get(name) is None and failures is None and not result.get("skipped"):
                raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)
        commit = {"heads": heads, "watermarks": watermarks}
        if failures is not None:
            commit["failures"] = failures
            if result.get("key_outcomes"):
                commit["key_outcomes"] = result["key_outcomes"]
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
        worked = bool((task.get("outcomes") or {}).get("succeeded"))
        if outcome == "succeeded" and result.get("skipped") and not more and not worked:
            outcome = "skipped"  # the patterns took none of its keys: it did nothing (per-key §11)
        self._finish(
            task,
            claim,
            outcome,
            commit=commit,
            more=more and outcome == "succeeded",
            worker=result,
            writes=result.get("writes"),
            error=error,
            retryable=retryable,
            delay=delay,
            keys=result.get("keys"),
        )
        return {"run": task["run"], "attempt": attempt, "outputs": outputs}

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

        head = self.m.heads.get((name, ""))
        event, ref = await self._prepare_commit(name, version, keys, upsert, remove, by)
        if event is None:
            return {"changed": False, "ref": ref}
        if commit_of(self.m.heads.get((name, ""))) != commit_of(head):
            await self._drop_prepared([event])
            raise Conflict(f"source {name!r} moved while committing; retry")
        event["at"] = self.clock()
        self.state.record(event)
        self._committed_keys([event])
        return {"changed": True, "ref": ref, "run": event["run"]["id"]}

    async def _prepare_commit(self, name, version, keys, upsert, remove, by, tags=None):
        """A source commit's `SourceCommitted`, its delta file written but
        nothing recorded, and the ref it installs; no event if it changes
        nothing. Whoever does not record it drops it (`_drop_prepared`)."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        # Adapters say "no removals" as an empty list: the same as none, for any source.
        remove = remove or None
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
        run = {"id": run_id, "source": name, "by": by, **({"tags": tags} if tags else {})}
        if not keyed:
            if version is None:
                raise ValueError(f"Source {name!r} requires version=")
            if head is not None and head["ref"]["version"] == str(version):
                return None, head["ref"]
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
            batch = int((head or {}).get("batch", -1)) + 1
            attempt = ulid(self.clock())
            sorted_run = SortedRun.of(
                [key_bytes(k) for k in new],
                [key_bytes(v) for v in new.values()],
                [key_bytes(k) for k in removes],
            )
            with self.m.reading():  # the index it resolves against outlives compaction meanwhile
                pinned = self.m.index(name, "").pinned()
                index = KeyIndex(self._key_io(), None, pinned, self.key_options)
                files = await self._resolve_source(index, pinned, sorted_run, replace, batch, attempt)
                if files is not None:
                    files, changed = files
                elif replace:
                    rows = Rows.pairs(list(new.items()))
                    files, changed = await index.replace(
                        rows, batch, attempt, collect=2 * SOURCE_KEYS_RECORDED
                    )
                else:
                    files, changed = await index.resolve(
                        sorted_run, batch=batch, attempt=attempt, collect=2 * SOURCE_KEYS_RECORDED
                    )
            if not files.files:
                return None, ref
            ref["version"] = digest([ref["version"], batch, [f.name for f in files.files]])
            record["batch"] = batch
            if source.get("key") == "<elements>" or name in self._set_dims:
                before = set((head or {}).get("elements") or ())
                record["elements"] = sorted(set(new) if replace else (before - set(removes)) | set(new))
            event["keys"] = {**files.to_json(), "batch": batch}
            run["batch"] = batch
            counts = (sum(f.entries for f in files.files) - files.removed, files.removed)
            for field, keys, count in zip(
                ("upserted", "deleted"), changed or (None, None), counts, strict=True
            ):
                listed = keys is not None and len(keys) <= SOURCE_KEYS_RECORDED
                run[field] = [key_str(k) for k in keys] if listed else count
        meta = dict(ref.get("meta") or {})
        meta["external"] = True
        ref["meta"] = meta
        event["run"] = run
        return event, ref

    def _committed_keys(self, events: list[dict]) -> None:
        """Warm the key cache with what recorded source commits installed."""

        if self.keys is not None:
            for event in events:
                if "keys" in event:
                    self._cache_commit(event["source"], "", event["keys"])

    async def _drop_prepared(self, events: list[dict]) -> None:
        """Delete the delta files of prepared commits never recorded."""

        paths = []
        for event in events:
            if "keys" in event:
                index = KeyIndex(
                    self._key_io(), None, self.m.index(event["source"], "").pinned(), self.key_options
                )
                paths += [index.path(f["name"]) for f in event["keys"]["files"]]
        if paths:
            await self.state.delete_objects(paths)

    async def _resolve_source(self, index, pinned, run, replace, batch, attempt):
        """A small source commit through the warm resolver, in process
        (docs/resolved-commits.md §4): its files and changed keys, or None when
        the cache cannot answer and the commit resolves cold."""

        from solera.keys.resolver import Limits

        lim = Limits()
        size = len(run) + (pinned.count if replace else 0)
        if self.keys is None or size > (lim.max_entries if replace else lim.max_keys):
            return None
        name = f"{batch:012d}-{attempt}.0000"
        answer, delta = await self.keys.direct(
            pinned, "replace" if replace else "patch", run, 0, batch, index.path(name), self.m.applied
        )
        if answer["result"] == "empty":
            return DeltaFiles([], 0, 0, True), ({}, [])
        if answer["result"] != "delta":
            return None
        await index.io.write(index.path(name), delta)
        files = DeltaFiles([FileInfo.describe(name, 0, delta)], answer["added"], answer["removed"], True)
        return files, delta_keys(delta)

    # -- key index upkeep (§6) --------------------------------------------------------

    def _key_io(self) -> ObjectIO:
        if self._io is None:
            self._io = ObjectIO(self.state.objects)
        return self._io

    async def list_keys(self, output: str, scope: str = "", *, after=None, offset=0, limit=1000) -> dict:
        """One page of an output's live keys, read from its key index."""

        if (output, scope) not in self.m.heads:
            raise KeyError(f"{output}/{scope}")
        state = self.m.indexes.get((output, scope))
        if state is None:
            return {"total": 0, "exact": True, "keys": {}, "next": None}
        start = key_bytes(after) if after is not None else None
        with self.m.reading():  # its files outlive compaction until the page is read
            index = KeyIndex(self._key_io(), None, state.pinned(), self.key_options)
            keys, versions, _, nxt = await index.page(start, offset + limit)
        return {
            "total": state.count,
            "exact": state.count_exact,
            "keys": {
                key_str(k): self._rendered(output, v)
                for k, v in list(zip(keys, versions, strict=True))[offset:]
            },
            "next": key_str(nxt) if nxt is not None else None,
        }

    def _rendered(self, output: str, version: bytes) -> str:
        """A key's version in `output`, as `key_outcomes` shows it: a declared
        revision's text, a source's version, a set's element — else a row
        digest, in hex."""

        record = self.manifest["outputs"].get(output) or {}
        if record.get("source") or record.get("partition_set") or record.get("revision"):
            return version.decode(errors="replace")
        return version.hex()

    # -- automations (§9) ------------------------------------------------------------

    @staticmethod
    def _due_at(auto: dict) -> float | None:
        """When a schedule comes due (§9): `every` its interval after it last
        fired, at once if it never has; `cron` its next time after it last
        fired, counted from the epoch if it never has. `None` for the
        triggers that wait on an event, and for a disabled automation."""

        trigger = auto["trigger"]
        if not auto["enabled"]:
            return None
        if trigger["kind"] == "every":
            return -math.inf if auto["last_at"] is None else auto["last_at"] + trigger["seconds"]
        if trigger["kind"] == "cron":
            zone = ZoneInfo(trigger.get("timezone") or "UTC")
            base = dt.datetime.fromtimestamp(auto["last_at"] or 0, zone)
            return croniter(trigger["expression"], base).get_next(dt.datetime).timestamp()
        return None

    def automation_view(self, auto: dict) -> dict:
        """An automation as the API shows it: a copy, with `next_at`, when its
        schedule next fires (now, if it is due), or null."""

        due = self._due_at(auto)
        return {**auto, "next_at": None if due is None else max(due, self.clock())}

    async def _automation_tick(self):
        now = self.clock()
        fired = []
        for auto in list(self.m.automations.values()):
            if not auto["enabled"] or auto["name"] in self._firing:
                continue
            trigger = auto["trigger"]
            if trigger["kind"] in ("every", "cron"):
                if self._due_at(auto) <= now:
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

    async def _retry_tick(self):
        """The retry clock (docs/per-key-processing.md §9): an automated Each
        asset whose failure index has keys due again runs for that scope, even
        when nothing upstream changed. An asset run by hand picks them up on
        its next run."""

        automated = {t for auto in self.m.automations.values() if auto["enabled"] for t in auto["targets"]}
        for (asset, scope), record in list(self.m.failures.items()):
            if asset not in automated or asset not in self.manifest["assets"]:
                continue
            if self._scope_active(asset, scope) or self.m.is_pending(asset, scope):
                continue
            if self._has_retries(record):
                await self.submit_retries(asset, [scope], "retry clock")

    async def submit_retries(self, asset: str, scopes, by: str | None) -> list[dict]:
        """Runs for an Each asset's scopes that have keys to retry, each under
        the configuration its scope last ran with (kept on its failure record):
        a retry under another configuration would read other inputs, and its
        new fingerprint would redeliver every key (§9). Scopes already active
        are left to the run they are in."""

        by_config: dict[str, list[str]] = {}
        for scope in scopes:
            config = (self.m.failures.get((asset, scope)) or {}).get("config") or {}
            by_config.setdefault(json.dumps(config, sort_keys=True), []).append(scope)
        runs = []
        for config, group in sorted(by_config.items()):
            run = await self.submit(
                [asset], partitions=sorted(group), config=json.loads(config), skip_active=True, by=by
            )
            if run is not None:
                runs.append(run)
        return runs

    def retry_keys(self, asset: str, classes, scope: str | None = None, by: str | None = None) -> dict:
        """`solera keys retry`: a forced request for an Each asset's failing
        keys of `classes` (`failed`, `rejected`, `canceled`, `retrying`,
        `timed_out`, or `all`), every scope or one. Its position in the event
        order is its identity; a retry pass takes each such key once (§9)."""

        from solera.failures import NAMES

        if not any(
            e.get("each") for e in (self.manifest["assets"].get(asset) or {}).get("inputs", {}).values()
        ):
            raise ValueError(f"{asset} has no Each edge: it keeps no failing keys")
        classes = sorted(set(NAMES.values()) if "all" in classes else set(classes))
        unknown = set(classes) - set(NAMES.values())
        if unknown or not classes:
            raise ValueError(f"unknown key classes: {sorted(unknown) or classes}")
        self.state.record(
            {"type": "KeysRetryRequested", "asset": asset, "scope": scope, "classes": classes, "by": by}
        )
        scopes = sorted(s for (a, s) in self.m.failures if a == asset and (scope is None or s == scope))
        return {"asset": asset, "classes": classes, "scopes": scopes}

    def _automation_run(self, auto, partitions, targets=None) -> dict | None:
        """The run a firing becomes, in the run's own vocabulary (§9); `None`
        if every scope is in flight or waits for its inputs."""

        return self._plan_run(
            targets or auto["targets"],
            partitions,
            auto.get("mode") or "incremental",
            auto.get("upstream") or False,
            auto.get("config"),
            auto.get("keys"),
            automation=auto["name"],
            skip_active=True,
            skip_missing_inputs=auto.get("skip_missing_inputs") or False,
            tags=auto.get("tags"),
        )

    def _fired(self, auto, run: dict | None, **fields) -> None:
        """Submit a firing's run and record the firing, together."""

        submitted = [{"type": "RunSubmitted", "run": run, "command": None}] if run is not None else []
        fired = {
            "type": "AutomationFired",
            "name": auto["name"],
            "at": self.clock(),
            "run": run and run["id"],
        }
        self.state.record(*submitted, {**fired, **fields})

    async def _fire(self, auto, partitions):
        run = None
        try:
            run = self._automation_run(auto, partitions)
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
        self._fired(auto, run)

    async def _fire_ondeploy(self, auto):
        """§9: fire once for the served revision, then record it. A planning
        error leaves last_revision unset so the next tick retries."""

        try:
            run = self._automation_run(auto, auto.get("partitions") or "latest")
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
            return
        self._fired(auto, run, revision=self.manifest["revision"])

    async def _fire_onchange(self, auto):
        """One run per firing (§9). Each target's scopes are the automation's
        `partitions`, if it names them, else every changed upstream scope's
        projection onto the target (§7); planned together, a target that
        reads another waits for it. The changes it covers leave the pending
        set with it."""

        planner, explicit = self.planner(), auto.get("partitions")
        consumed, selected = [], {}
        try:
            named = {t: planner.scopes(t, explicit) for t in auto["targets"]} if explicit else None
            for producer, scope in (list(p) for p in auto["pending"]):
                owed = named or {t: planner.reach(producer, scope, t) for t in auto["targets"]}
                if any(self._scope_active_claim(t, s) for t, scopes in owed.items() for s in scopes):
                    # A running attempt pinned its inputs before this change: it cannot cover
                    # it. The change stays pending until that attempt ends, then fires.
                    continue
                consumed.append([producer, scope])
                for target, scopes in owed.items():
                    selected.setdefault(target, set()).update(scopes)
            if not consumed:
                return
            partitions = {t: sorted(scopes) for t, scopes in selected.items() if scopes}
            run = self._automation_run(auto, partitions, sorted(partitions)) if partitions else None
        except Exception as error:
            self.last_error = f"automation {auto['name']}: {error}"
            return  # the changes stay pending: the next tick replays them
        self._fired(auto, run, consumed=consumed)

    async def set_automation(self, name: str, enabled: bool):
        if name not in self.m.automations:
            raise KeyError(name)
        self.state.record({"type": "AutomationChanged", "name": name, "enabled": bool(enabled)})
        return self.m.automations[name]

    async def run_automation(self, name: str):
        auto = self.m.automations.get(name)
        if auto is None:
            raise KeyError(name)
        await self._fire(auto, auto.get("partitions") or "latest")
        return self.m.automations[name]

    # -- run control ------------------------------------------------------------------

    def _control(self, run_id: str, action: str, by: str | None):
        event = {"type": "RunControlled", "run": run_id, "action": action, "at": self.clock()}
        self.state.record({**event, "by": by} if by else event)
        for attempt, (run, _job) in self.inflight.items():
            if run == run_id:
                self._stirred.setdefault(attempt, asyncio.Event()).set()

    async def cancel(self, run_id: str, by: str | None = None):
        run = self.m.runs.get(run_id)
        if run is None:
            archived = await self.history.run(run_id)
            if archived is None:
                raise KeyError(run_id)
            return self._run_view(archived)
        if run["status"] not in TERMINAL_RUN:
            # Unlaunched claims go with the tasks. Each launched attempt is
            # aborted by its wait loop — or, if already writing, committed (§8).
            self._control(run_id, "cancel", by)
        return self._run_view(self.m.runs.get(run_id) or run)

    async def pause(self, run_id: str, paused=True, by: str | None = None):
        run = self.m.runs.get(run_id)
        if run is None:
            if await self.history.run(run_id) is None:
                raise KeyError(run_id)
            raise Conflict(f"run {run_id} is finished")
        self._control(run_id, "pause" if paused else "resume", by)
        return self._run_view(self.m.runs[run_id])

    async def retry(self, run_id: str, by: str | None = None):
        """Run a finished run's failed and canceled work again, with what it
        blocked, as a new run linked to it (`retry_of`): the run stays as it
        ended. Retries inside a running task are attempts, not this."""

        run = self.m.runs.get(run_id) or await self.history.run(run_id)
        if run is None or "source" in run:
            raise KeyError(run_id)
        if run["status"] not in TERMINAL_RUN:
            raise Conflict(f"run {run_id} has not finished", retryable=False)
        scopes: dict[str, list[str]] = {}
        for task in sorted(run["tasks"].values(), key=lambda t: t["id"]):
            if task["status"] in ("failed", "canceled", "blocked"):
                scopes.setdefault(task["asset"], []).append(task["scope"])
        if not scopes:
            raise Conflict(f"run {run_id} has nothing to retry", retryable=False)
        return await self.submit(
            sorted(scopes),
            partitions=scopes,
            mode=run.get("mode") or "incremental",
            config=run.get("config"),
            keys=run.get("keys"),
            by=by,
            tags=run.get("tags"),
            retry_of=run_id,
        )

    # -- finished runs -------------------------------------------------------------------

    def _archive_due(self):
        """Move finished runs from memory into the history, once none of
        their attempts is still in flight here (§7)."""

        busy = {run_id for run_id, _ in self.inflight.values()}
        for run_id in sorted(self.m.archivable):
            if run_id in busy:
                continue
            run = self.m.runs.get(run_id)
            if run is None or any(tid in self.m.claims for tid in run["tasks"]):
                continue
            self.state.record({"type": "RunArchived", "run": run_id, "at": self.clock()})

    async def delete_run(self, run_id: str) -> None:
        """Delete a finished run: its history, attempt files and logs. Current
        state never depends on runs, so only a run in progress is refused."""

        if run_id in self.m.runs:
            raise Conflict(f"run {run_id} is still active", retryable=False)
        await self.upkeep.delete_runs([(run_id, None)])

    async def prune(self, *, before=None, asset=None, keep=None, dry_run=False) -> dict:
        """Delete finished runs created before `before` (epoch seconds), of
        `asset` if given, except the `keep` newest of them."""

        doomed = await self.history.prunable(before=before, asset=asset, keep=keep)
        if not dry_run:
            await self.upkeep.delete_runs(doomed)
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

    TASK_INTERNALS = (
        *("launched", "ready_at", "queued_at"),
        *("tries", "outcomes", "duration", "first_at", "last_at", "last", "error", "executor"),
    )

    def _task_view(self, task: dict, attempts: list[dict], live: bool) -> dict:
        view = {k: v for k, v in task.items() if k not in self.TASK_INTERNALS}
        claim = self.m.claims.get(task["id"]) if live else None
        view["status"] = claim["status"] if claim else task["status"]
        view["generation"] = view["attempt_count"] = len(attempts) + (1 if claim else 0)
        latest = attempts[-1] if attempts else None
        if latest:
            view.setdefault("error", latest.get("error"))
            view.setdefault("outputs", latest.get("outputs"))
        return view

    def _attempt_views(self, task: dict, attempts: list[dict], live: bool) -> list[dict]:
        out = []
        for n, a in enumerate(attempts, 1):
            view = {
                "id": a["id"],
                "task": task["id"],
                "generation": n,
                "status": a["outcome"],
                "started_at": a.get("started_at"),
                "finished_at": a.get("finished_at"),
                **{k: a[k] for k in (*history.PHASES, *history.USAGE, *history.EXECUTION) if k in a},
            }
            if a.get("error"):
                view["error"] = a["error"]
            if a.get("outputs"):
                view["outputs"] = a["outputs"]
                view["commit"] = f"{task['run']}/{a['id']}"
            if a.get("keys"):
                view["keys"] = a["keys"]  # an Each attempt's keys by outcome
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
                    **(history.execution(task["launched"]["execution"]) if claim.get("launched") else {}),
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
        """A run, its tasks and their attempts: ended attempts from the
        history, whether the run is in progress or finished."""

        run, live = self.m.runs.get(run_id), True
        if run is None:
            run, live = await self.history.run(run_id), False
            if run is None:
                raise KeyError(run_id)
        attempts = await self.history.attempts(run_id) if run.get("tasks") else {}
        return self._detail(run, live, attempts)

    def _detail(self, run: dict, live: bool, attempts: dict[str, list[dict]]) -> dict:
        tasks = [run["tasks"][tid] for tid in sorted(run.get("tasks") or {})]
        return {
            "request": self._run_view(run),
            "tasks": [self._task_view(t, attempts.get(t["id"], []), live) for t in tasks],
            "attempts": {t["id"]: self._attempt_views(t, attempts.get(t["id"], []), live) for t in tasks},
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
        asset = self.planner().asset_of(name)
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
        dims = self.planner().dims(asset)
        return {
            "asset": info,
            "heads": heads,
            "cursor": self.m.cursors.get((asset, scope)),
            "watermarks": watermarks,
            "current_keys": self.planner().dim_keys(dims) if dims else [],
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
