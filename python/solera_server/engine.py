"""The engine (§6–§10): control-plane only. It plans runs into per-(asset,
partition) tasks, resolves inputs to pinned heads, plans Incremental inputs from
what each partition observed of them (docs/observed-set.md), dispatches
attempts through placements, and commits their results.

State lives in the model (model.py), changed only by events the engine records
(docs/object-store-state.md §3, §4). Recording is synchronous and never waits
on storage: the journal writes in the background. A precondition check and
the event that depends on it happen in one synchronous step, so no other
coroutine can interleave between them: planning and submitting a run,
preparing an attempt, installing its commit and firing an automation are
plain functions, never suspended. The one wait is before launching an
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
from solera import lifecycle
from solera.ids import ulid, ulid_time
from solera.keys.io import ObjectIO
from solera.keys.layers import (
    delta_names,
)
from solera.sdk import default_placement, digest
from solera.tasks import Tasks

from . import history, planning
from .attempts import POOL_OFFERED_GRACE, REPLAN_DELAY, Attempts, Live, worker_report
from .executors import PlacementContext, Registry
from .history import MAX_METADATA, History, RunFilter
from .model import (
    CLEANUP,
    TERMINAL_RUN,
    TERMINAL_TASK,
    _delta_files,
    commit_of,
    declaration,
    is_log,
    output_id,
    reads,
)
from .observing import Observing
from .sensors import Sensors
from .staleness import Staleness
from .state import Conflict, LostOwnership, State
from .tracking import WALK, Core, Generation, Redeployed, b64, cache_root, output_pin
from .tracking import commit as core_commit
from .tracking import full as core_full
from .tracking import merge as core_merge
from .tracking import prepare as core_prepare
from .tracking import sources as core_sources
from .upkeep import ALIVE, Upkeep
from .views import Views

log = logging.getLogger(__name__)

SUCCESS = {"succeeded", "skipped"}
TERMINAL = SUCCESS | {"failed", "blocked", "canceled"}
HEARTBEAT_SECONDS = 10.0  # a worker beats this often (docs/lifecycle.md §6)
PROVISION_SECONDS = 600.0  # a launched worker reports within this, or it never started
CANCEL_GRACE = 60.0  # a requested cancel's time to drain before it is forced (§7)
CLEANUPS = 64  # cleanup entries one cleanup task takes at most
CLEANUP_INTERVAL = 3600.0  # the cleanup job looks for cleanup due anywhere this often (§9.8)
CLEANUP_COALESCE = 60.0  # after commits, one cleanup task per output partition this often at most
SOURCE_KEYS_RECORDED = 1000  # a source commit's run lists changed keys up to this many, else counts
GRACE_SECONDS = 5.0
CLEANUP_TRIES = 6  # a cleanup task's attempts, its retries backing off from a minute (K25)
CLEANUP_RETRY_DELAY = 60.0  # a failed cleanup task's first retry, then doubling
CHANGE_BACKOFF, CHANGE_BACKOFF_MAX = 60.0, 3600.0  # a failing partition's changes wait, doubling (F43)


class Retryable(RuntimeError):
    """A dispatch-time failure that the retry policy may absorb."""


class NonRetryable(RuntimeError):
    """A dispatch-time failure no retry will fix (§8: full run required, …)."""


def _dynamic_dims(manifest: dict) -> set[str]:
    """The outputs a manifest's dynamic partition dimensions come from."""

    return {
        dim["output"]
        for a in manifest["assets"].values()
        for dim in ((a.get("partitions") or {}).get("dims") or {}).values()
        if dim["kind"] == "dynamic"
    }


class Engine(Attempts, Observing, Sensors, Staleness, Views):
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
        maintenance_concurrency: int = 2,
        retention_interval: float = 60.0,
        history: History | None = None,
        resolve_cache: str | None | bool = True,
        sensor_host=None,
        cleanup_interval: float = CLEANUP_INTERVAL,
        cleanup_coalesce: float = CLEANUP_COALESCE,
    ):
        import time

        if heartbeat_seconds <= 0 or concurrency < 1:
            raise ValueError("Heartbeat interval and concurrency must be positive")
        self.state, self.manifest = state, manifest
        self.project = project
        self.clock = clock or time.time
        self.heartbeat_seconds, self.concurrency = heartbeat_seconds, concurrency
        self.provision_seconds, self.cancel_grace = provision_seconds, cancel_grace
        self.cleanup_interval, self._cleanup_job_at = cleanup_interval, 0.0  # the job's next look
        # Commits' cleanup, coalesced (§9.8): when each output partition last got a
        # cleanup task after a commit, and those whose commits wait for their next.
        self.cleanup_coalesce = cleanup_coalesce
        self._cleanup_at: dict[tuple, float] = {}
        self._cleanup_waiting: set[tuple] = set()
        self.local_app = None  # its routes for workers it runs in this process (`api.local_transport`)
        # Where workers reach this engine (docs/lifecycle.md §5); without one,
        # they report through `.beat` alone.
        self.engine_url = engine_url
        self.pool_offered_grace = pool_offered_grace
        self.secret: bytes | None = None  # signs attempt tokens; stable across restarts
        self.live: dict[str, Live] = {}  # attempt id -> what its worker reported
        # (output, partition) -> key -> what a batch loaded of it that nothing explains: an alarm,
        # in memory, until the output's next commit (`tracking.loaded`)
        self.anomalies: dict[tuple, dict] = {}
        self.pollers: dict[str, dict] = {}  # pool workers that asked for work lately
        self._pool_changed = asyncio.Event()
        self.eval_interval = eval_interval
        ctx = PlacementContext(state, state.objects_url, project, self.clock, self)
        self.registry = registry or Registry(ctx, extra=placements)
        # (run id, attempt id) -> its watcher: attempts this process is driving.
        self.watchers = Tasks("attempts")
        self.tasks = Tasks("engine")  # the eval loop, then the sensor host: _halt stops them so
        self.fence = Tasks("fence")  # what halts it the moment its state ends
        # Attempts whose end failed: (times, not adopted again before this monotonic time).
        self._crashes: dict[str, tuple[int, float]] = {}
        self._covers: dict[tuple, tuple] = {}  # an input record with gaps: is it covered? (`complete`)
        # (asset, partition, input) -> batches committed here: what `complete` knew of an edge is old.
        self._walked: dict[tuple, int] = {}
        # attempt id -> set when its run is controlled, so its watcher looks at once.
        self._stirred: dict[str, asyncio.Event] = {}
        # Attempts consuming a local execution slot. Pool attempts only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.executor_inflight: dict[str, int] = {}
        self._io: ObjectIO | None = None
        # The core and its change index, whose disk cache is where `resolve_cache` says, else
        # beside `file://` state or in a temporary directory; None or False: no disk cache.
        root = resolve_cache if isinstance(resolve_cache, str) else cache_root(state.objects_url)
        self.core = Core(self, root if resolve_cache else None)
        # What fails now, by name — the eval loop, upkeep, a key index, the history, an
        # automation — each entry cleared by its own next success: the one place it shows.
        self.failing: dict[str, str] = {}
        self.history = history or History(state, clock=self.clock)
        self.history.lake.failing = self.failing
        self.upkeep = Upkeep(
            state,
            self.history,
            manifest,
            clock=self.clock,
            concurrency=maintenance_concurrency,
            retention_interval=retention_interval,
            failing=self.failing,
        )
        self._sensors_init(sensor_host)
        self._dynamic_dims = _dynamic_dims(manifest)

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
            self.state.record({"type": "EngineOutage", "down": down, "at": now})
        if m.deploy != self.manifest["deploy"] or m.manifest != self.manifest or m.project != self.project:
            self._register()

    def _register(self) -> None:
        """Record the served manifest, and the firings it leaves owed."""

        self.state.record(
            {
                "type": "ProjectRegistered",
                "deploy": self.manifest["deploy"],
                "manifest": self.manifest,
                "project": self.project,
                "at": self.clock(),
            }
        )
        if owed := self._owed_firings():
            self.state.record({"type": "FiringsOwed", "owed": owed, "at": self.clock()})

    def redeploy(self, manifest: dict) -> bool:
        """Serve another deploy of the project in place, as a local serve's
        reload does when its code changes: the deploy is recorded as any
        other. Attempts in flight finish under the deploy they launched
        with; new ones run the new code. Whether it changed anything."""

        if manifest["deploy"] == self.manifest["deploy"]:
            return False
        self.manifest = self.upkeep.manifest = manifest
        self._dynamic_dims = _dynamic_dims(manifest)
        self._register()
        return True

    def _owed_firings(self) -> dict[str, list[list[str]]]:
        """What the deploy just registered leaves each `OnChange` automation
        owing, once: for each target it changed — added (again), renamed,
        its declaration changed, or reset (`Model.changed`) — every
        current partition whose inputs have heads, so it is built now, not
        when its upstream next changes. Decided at the deploy and
        recorded, never re-checked by a tick: a run of it that fails is not
        resubmitted on every tick. Schedules, OnDeploy and sensors keep
        their own criteria."""

        planner, owed = self.planner(), {}
        changed = self.m.changed
        for name, auto in self.m.automations.items():
            if auto["trigger"]["kind"] != "onchange" or not auto["enabled"]:
                continue
            due = []
            for target in sorted(changed & set(auto["targets"])):
                try:
                    partitions = planner.partitions(target, "all")
                except ValueError:  # too many to list: its upstream's next changes reach them
                    continue
                due += [[target, s] for s in partitions if self._inputs_written(planner, target, s)]
            if due:
                owed[name] = due
        return owed

    @staticmethod
    def _inputs_written(planner, asset: str, partition: str) -> bool:
        """Whether every input of (asset, partition) has something to read: a
        head, or — across partitions (a fan-in) — at least one
        materialized upstream partition."""

        try:
            inputs = planner.inputs(asset, partition)
        except planning.UpstreamOnly:
            return False
        for input in inputs:
            if input.fan_in:
                if not planner.fan_in(input, materialized=True):
                    return False
            elif planner.head(input.output, input.partition) is None:
                return False
        return True

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
        self.tasks.every(
            "engine", self.eval_interval, self.tick, failing=self.failing, wake=self.state.changed
        )
        self.fence.spawn(self._fenced())
        self.upkeep.start()
        self.history.start()
        self._start_sensor_host()

    async def stop(self):
        await self.fence.close()
        await self._halt()

    async def _fenced(self) -> None:
        """Replaced (or broken): its successor owns everything now. The
        handlers' own authority checks stay: a cancel reaches a task only at
        its next await."""

        await self.state.ended.wait()
        log.warning("this engine lost the namespace's writer ownership: it stops acting")
        await self._halt()

    async def _halt(self) -> None:
        """Stop acting: the eval loop, the sensor host, the attempts'
        watchers, upkeep, the history, the key cache. The workers themselves
        keep running; whoever owns the namespace next adopts them."""

        await self.tasks.close()  # the eval loop, then the sensor host (its child stopped)
        await self.watchers.close()  # launched attempts keep running: the next engine adopts them
        await self.upkeep.stop()
        await self.history.stop()

    async def tick(self):
        """One evaluation pass: adoption, dispatch, automations, archiving."""

        self._adopt()
        await self._cover_tick()
        await core_full.settle(self)
        core_merge.start(self)
        self._cleanup_job()
        self._dispatch_due()
        self._sensor_sweep()
        self._automation_tick()
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
                mine = [self.watchers.get(k) for k in self.watchers if k[0] == run_id]
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
        """A run request becomes one task per (asset, partition) (§8). `by` says
        who asked (the API or CLI, or what the caller names); automation runs
        carry `automation` instead. `tags` label the run for finding it later.
        `skip_active` and `skip_missing_inputs` leave out the partitions already
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
        self.state.record({"type": "RunSubmitted", "run": run, "command": command_id})
        return self._run_view(self.m.runs.get(run["id"]) or run)

    def planner(self, projected: dict | None = None) -> planning.Planner:
        """Planning over the committed heads — with `projected` heads, which a
        sensor's commits are about to install, over them — and the engine's
        clock."""

        return planning.Planner(
            self.manifest,
            lambda o, s: self.m.heads.get((o, s)),
            self.m.heads_of,
            self.clock(),
            projected,
            self._complete_known,
            any_head=True,
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
            targets, partitions, mode, upstream, config, keys, active=self._partition_active, **options
        )

    def _partition_active(self, asset: str, partition: str) -> bool:
        return self._partition_active_claim(asset, partition) or self.m.is_pending(asset, partition)

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
        worker=None,
        end=None,
        reason=None,
        write=None,
        retry_for=None,
        keys=None,
        restart=False,
    ):
        """End an attempt. `worker` is what its worker reported — its result,
        or its last heartbeat: the events it recorded and what it used. `end`
        names how it ended if its outcome does not say (`aborted`, `lost`),
        `reason` why, and `writes` what is known of its writes
        (docs/lifecycle.md §2.3)."""
        self.core.plans.pop(claim["attempt"], None)
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
        elif task["asset"] in self.manifest["assets"]:
            # What it wrote is cleaned up once a worker that outlives it can no longer add to
            # it: the asset's timeout and cancel grace after its end (the sweep, §9.8).
            info = self.manifest["assets"][task["asset"]]
            event["late_writes"] = float(info.get("timeout") or 3600) + self.cancel_grace
        if more:
            event["more"] = True
        if restart:  # its asset read a dead write to its own output: its next attempt runs in full
            event["restart"] = True
        worker = worker_report(worker)  # values the model applies with no parsing
        if keys is not None:
            keys = worker_report({"keys": keys}).get("keys")
        if write not in (None, lifecycle.NONE, lifecycle.COMPLETE, lifecycle.WRITING):
            write = lifecycle.WRITING  # what a worker cannot say plainly is not known
        if worker.get("events") or worker.get("usage"):
            event["worker"] = {k: worker[k] for k in ("events", "usage") if worker.get(k)}
        if end is not None:
            event["end"] = end
        if reason is not None:
            event["reason"] = str(reason)[:200]
        if write is not None:
            event["write"] = write
        for field in ("cleaned_up", "cleanup_unresolved", "cleaned_to"):  # cleanup (§9.8)
            if worker.get(field):
                event[field] = worker[field]
        if worker.get("read"):
            event["read"] = worker["read"]  # what its inputs' reads saw, for lineage
        if keys:
            event["keys"] = keys  # a per-key attempt's keys by outcome
        self.state.record(event)
        if commit is not None:
            # What the commit let go of goes at once, if no reader needs it: a cleanup task.
            cleanup = task.get("cleanup") or {}
            handed = ((task.get("launched") or {}).get("prepared") or {}).get("cleanup") or {}
            if "output" in cleanup:  # a cleanup task: the rest, if its limit left some; else the job's
                if any(
                    len(i.get("cleanup") or ()) >= CLEANUPS
                    or len((i.get("deltas") or {}).get("deltas") or ()) >= CLEANUPS
                    for i in (handed.get("outputs") or {}).values()
                ):
                    self._submit_cleanups([(cleanup["output"], cleanup["partition"])])

    # -- dispatch ---------------------------------------------------------------

    def _dispatch_due(self):
        """Claim the tasks that are due, as far as the engine, their
        executors, their partitions, their assets' `concurrency=` and their outputs' merges allow.
        Those held back are recorded, with why, when that changes."""

        now = self.clock()
        engine_used = len(self.engine_inflight)
        executor_used = dict(self.executor_inflight)
        # Partitions of each asset claimed now, of its current life: what concurrency= caps.
        # An earlier life's attempt holds no name, but its partition stays its own until
        # it is ended: two attempts never own one partition's files at once.
        asset_used: dict[str, int] = {}
        retiring: dict[tuple, str] = {}
        for claim, claimed in ((c, self.m.task(t)) for t, c in self.m.claims.items()):
            if claimed is None:
                continue
            if self.m.earlier_life(claimed):
                retiring[(claimed["asset"], claimed["partition"])] = claim["attempt"]
            else:
                asset_used[claimed["asset"]] = asset_used.get(claimed["asset"], 0) + 1
        held = {}
        incomplete = core_full.waiting(self)
        planner = self.planner() if incomplete else None
        for task_id in self.m.due(now):
            task = self.m.task(task_id)
            if task is None or task["status"] != "queued" or task_id in self.m.claims:
                continue
            run = self.m.runs[self.m.task_run[task_id]]
            if run["status"] == "canceled" or run.get("paused"):
                continue
            try:
                spec = self._placement_spec(task)
                placement = self.registry.build(spec)
            except Exception as error:  # this task's, not the pass's: the others still dispatch
                log.exception("task %s cannot be placed", task_id)
                held[task_id] = ["invalid", str(error)[:200]]
                if task.get("held") == held[task_id]:
                    del held[task_id]
                continue
            executor = spec["executor"]
            limit = getattr(placement, "max_concurrent", None)
            is_pool = spec["kind"] == "Pool"
            key = (task["asset"], task["partition"])
            holder = self.m.claimed_partitions.get(key)
            if holder is None or self.m.claimed(holder) is None:
                holder = retiring.get(key)
            declared = self.manifest["assets"].get(task["asset"]) or {}  # a cleanup task's: none
            cap = declared.get("concurrency")
            if holder is not None:
                held[task_id] = ["claim", holder]
            elif (
                incomplete
                and (waiting := core_full.gate(planner, task, lambda o, p: (o, p) in incomplete)) is not None
            ):
                held[task_id] = waiting
            elif cap is not None and asset_used.get(task["asset"], 0) >= cap:
                held[task_id] = ["concurrency", task["asset"]]
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
            asset_used[task["asset"]] = asset_used.get(task["asset"], 0) + 1
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
                self._stirred.pop(attempt, None)
                self.live.pop(attempt, None)
                self.engine_inflight.discard(attempt)
                self.executor_inflight[executor] = max(0, self.executor_inflight.get(executor, 1) - 1)

        self.watchers.spawn(drive(), key=(run_id, attempt))

    def _adopt(self):
        """Wait again for attempts launched before a restart (§8): their
        claims are durable, but nothing in this process waits on them yet."""

        now = asyncio.get_running_loop().time()
        watched = {attempt for _, attempt in self.watchers}
        for task_id, claim in list(self.m.claims.items()):
            attempt = claim["attempt"]
            if not claim.get("launched") or attempt in watched:
                continue
            if self._crashes.get(attempt, (0, 0.0))[1] > now:  # its end failed: backing off
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

    def _placement_spec(self, task: dict) -> dict:
        """Where a task runs: its asset's placement; a cleanup task's, the
        project's default (K25)."""

        if task["asset"] == CLEANUP:
            return default_placement()
        return self.manifest["assets"][task["asset"]]["placement"]

    def _cleanup_job(self) -> None:
        """The cleanup job (§9.8): every `cleanup_interval`, a cleanup task for
        whatever is due anywhere — entries whose pins or grace have cleared
        since their commit, those of partitions that never run again, and
        output lives past their `cleanup_after`."""

        now = self.clock()
        if now >= self._cleanup_job_at:
            self._cleanup_job_at = now + self.cleanup_interval
            self._submit_cleanups()
        due = [
            k
            for k in self._cleanup_waiting
            if now >= self._cleanup_at.get(k, -math.inf) + self.cleanup_coalesce
        ]
        if due:
            self._cleanup_waiting.difference_update(due)
            self._cleanup_soon(due)

    def _submit_cleanups(self, partitions=None) -> int:
        """A cleanup task for each of `partitions` (output, partition) with
        entries due — all of them with none, and every output life past its
        `cleanup_after`, read by no pin older than the deploy that ended it.
        None where one is queued or running already, or an entry is stuck. It
        runs as any task does — placed, retried a bounded number of times, in
        the runs and their history — with no asset of the project's. Returns
        how many it submitted."""

        submitted = {
            json.dumps(t.get("cleanup"), sort_keys=True)
            for run in self.m.runs.values()
            for t in run["tasks"].values()
            if t["asset"] == CLEANUP and t["status"] not in TERMINAL_TASK
        }
        due, declared = [], {o["name"] for info in self.manifest["assets"].values() for o in info["outputs"]}
        for output, partition in (
            list({**self.m.cleanups, **self.m.cleaning}) if partitions is None else partitions
        ):
            if output in declared and (
                self._due_cleanups(output, partition, None) or self._due_step(output, partition, None)
            ):  # what a task could take
                due.append(({"output": output, "partition": partition}, f"{output}/{partition}", output))
        if partitions is None and self.m.retired:
            now, floor = self.clock(), self.m.pin_floor()
            for entry_id, entry in self.m.retired.items():
                if not entry.get("stuck") and entry["due"] <= now and floor >= entry["before"]:
                    due.append(({"life": entry_id}, entry_id, entry["output"]))
        made = 0
        for cleanup, partition, output in due:
            if json.dumps(cleanup, sort_keys=True) not in submitted:
                self._cleanup_task(cleanup, partition, output)
                made += 1
        return made

    def _cleanup_task(self, cleanup: dict, partition: str, output: str) -> None:
        now = self.clock()
        run_id = ulid(now)
        task_id = f"{run_id}/{CLEANUP}:{partition}"  # as any task's: run/asset:partition
        task = {
            "id": task_id,
            "run": run_id,
            "asset": CLEANUP,
            "partition": partition,  # its own: cleanup tasks of different partitions run at once
            "cleanup": cleanup,
            "status": "queued",
            "deps": [],
            "max_attempts": CLEANUP_TRIES,
            "retry": {"n": CLEANUP_TRIES - 1, "delay": CLEANUP_RETRY_DELAY, "backoff": "exponential"},
            "ready_at": now,
            "queued_at": now,
            "wait": 0.0,
        }
        run = {
            "id": run_id,
            "kind": "cleanup",
            "targets": [],
            # Its scope, for the runs list: one output partition, or a retired output life's whole.
            "partitions": [cleanup["partition"]] if "partition" in cleanup else "latest",
            "mode": "incremental",
            "upstream": False,
            "config": {},
            "keys": None,
            "automation": None,
            "by": "engine",
            "tags": {"cleanup": output},
            "status": "running",
            "paused": False,
            "created_at": now,
            "updated_at": now,
            "events": 0,
            "tasks": {task_id: task},
        }
        self.state.record({"type": "RunSubmitted", "run": run, "command": None})

    def _prepare_cleanup(self, task: dict, attempt: str) -> dict:
        """A cleanup task's spec: an output life's leftovers, or a partition's
        entries due now (at most `CLEANUPS`), with the output's asset, whose
        store and declaration delete them; nothing to read or write besides.
        Nothing due any more (a pin arrived, an operator cleared it): skipped."""

        prepared = {
            "outputs": {},
            "inputs": {},
            "plans": {},
            "cursor": None,
            "deploy_number": self.m.deploy_number,
        }
        cleanup = task["cleanup"]
        if "life" in cleanup:
            entry = self.m.retired.get(cleanup["life"])
            if entry is None:
                raise NonRetryable(f"cleanup {cleanup['life']}: an operator cleared it")
            return {**prepared, "cleanup": dict(entry)}
        output, partition = cleanup["output"], cleanup["partition"]
        owner = next(
            (
                a
                for a, info in self.manifest["assets"].items()
                for o in info["outputs"]
                if o["name"] == output
            ),
            None,
        )
        entries = self._due_cleanups(output, partition, attempt) if owner is not None else []
        step = self._due_step(output, partition, attempt) if owner is not None else None
        if not entries and step is None:
            return {**prepared, "skip": True}
        claim = self.m.claimed(attempt)
        if claim is not None and step is not None:
            # The deltas its spec hands it are read from now, not from `AttemptLaunched`
            # on: a step acknowledged meanwhile must not let collection take them.
            claim["cleanups"] = _delta_files(step["deltas"])
        info = {"cleanup": entries}
        if step is not None:
            info["deltas"] = step
        return {
            **prepared,
            "cleanup": {"asset": owner, "partition": partition, "outputs": {output: info}},
        }

    def _partition_active_claim(self, asset: str, partition: str) -> bool:
        attempt = self.m.claimed_partitions.get((asset, partition))
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
                if task["asset"] == CLEANUP:
                    prepared = self._prepare_cleanup(task, attempt)
                else:
                    prepared = await core_prepare.prepare(self.core, task, run, attempt)
            except (Retryable, NonRetryable, Conflict) as error:
                if getattr(
                    error, "replan", False
                ):  # an upstream became incomplete: planned again, no try spent
                    self._finish(
                        task,
                        claim,
                        "replanned",
                        error=error,
                        retryable=True,
                        delay=REPLAN_DELAY,
                        reason=core_full.INCOMPLETE,
                    )
                    return
                self._finish(
                    task,
                    claim,
                    "failed",
                    error=error,
                    retryable=not isinstance(error, NonRetryable) and getattr(error, "retryable", True),
                )
                return
            if prepared.get("skip"):  # its progress still moves to the heads
                try:
                    await self.commit_attempt(
                        attempt, prepared, {"outputs": {}, "skipped": True}, outcome="skipped"
                    )
                except Conflict as error:
                    if not getattr(error, "replan", False):
                        raise
                    self._finish(  # as `_settle` does: planned again, no try spent
                        task,
                        claim,
                        "replanned",
                        error=error,
                        retryable=True,
                        delay=REPLAN_DELAY,
                        reason="held back",
                    )
                return
            if prepared.get("skip"):
                commit = self._made(prepared)
                more = bool(prepared.get("more"))
                commit["final"] = not more
                self._finish(task, claim, "skipped", commit=commit, more=more)
                return
            try:  # under the deploy it was prepared under: nothing awaited since `prepare` returned
                stage = await self._launch(task, run, attempt, prepared, Generation(self.m, "it launched"))
            except Redeployed as error:  # its outputs' names may be another deploy's: planned again
                self._finish(
                    task,
                    claim,
                    "replanned",
                    error=error,
                    retryable=True,
                    delay=REPLAN_DELAY,
                    reason="redeployed",
                )
                return
            try:
                self._authority()  # `_launch` awaited: still ours?
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
            self.core.plans.pop(attempt, None)

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
                    self._authority()
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
        try:
            if claim.get("launched"):
                # A sealed result that could not be settled fails with its own evidence:
                # settling it again would fail again. A malformed file has none.
                try:
                    result = await self.state.attempt_result(task["run"], attempt)
                except lifecycle.Malformed:
                    result = None
                await self._fail(
                    task_id, attempt, f"engine: {error}", retryable=True, reason="engine", result=result
                )
            else:
                self._finish(task, claim, "failed", error=f"engine: {error}", retryable=True, reason="engine")
            self._crashes.pop(attempt, None)
        except Exception:
            # It could not be ended either (its store unreachable, say): adopted again,
            # but after a back-off, never in a loop.
            times, _ = self._crashes.get(attempt, (0, 0.0))
            wait = min(60.0, 0.5 * 2**times)
            self._crashes[attempt] = (times + 1, asyncio.get_running_loop().time() + wait)
            log.exception("attempt %s: ending it failed; adopted again in %.1fs", attempt, wait)

    # -- input resolution + Incremental plans (§5, §6, §8) --------------------------

    def _pin_inputs(self, task: dict, run: dict) -> dict:
        """The first half of preparing an attempt, either path's: its outputs'
        heads, whether it starts over (a full run, or a version bump), and a
        pin for every input that is not incremental, with the definition and
        context it is made under. The incremental inputs are listed, for the
        path's planner."""

        asset = self.manifest["assets"][task["asset"]]
        partition = task["partition"]
        full = run["mode"] == "full" or bool(task.get("restart"))
        heads = {}
        for output in asset["outputs"]:
            head = heads[output["name"]] = self.m.heads.get((output["name"], partition))
            # A version bump is an asset change like any other: what the old version
            # built is rebuilt, the next attempt starting over.
            if head is not None and head.get("version") not in (None, asset["version"]):
                full = True
        planner = self.planner()
        try:
            inputs = planner.inputs(task["asset"], partition)
        except planning.UpstreamOnly as error:
            raise Conflict(str(error)) from None
        pins = {}
        incremental = []
        for input in inputs:
            param, output = input.param, input.output
            load = (asset["inputs"].get(param) or {}).get("load", "data")  # registration decided it
            if input.kind == "in" and input.fan_in:
                refs = self._whole_fan_in(planner, input)
                pins[param] = {"refs": refs, "load": load}
                indexes = (
                    {k: self._whole_index(output, ref) for k, ref in refs.items()} if load == "data" else {}
                )
                if any(i is not None for i in indexes.values()):
                    pins[param]["indexes"] = {k: i for k, i in indexes.items() if i is not None}
            elif input.kind == "dep":
                # Across upstream-only dimensions, a dep pins the heads that exist and
                # agree with this partition's keys; otherwise its one projected head (§7).
                if input.fan_in:
                    refs = {k: h["ref"] for k, h in planner.fan_in(input, materialized=False).items()}
                else:
                    refs = {input.partition: self._pin_at(output, input.partition)}
                pins[param] = {"refs": refs}
            elif input.kind == "incremental":
                incremental.append(input)
            else:
                pins[param] = {"ref": self._pin_at(output, input.partition), "load": load}
                if load == "data" and (index := self._whole_index(output, pins[param]["ref"])) is not None:
                    pins[param]["index"] = index
        return {
            "asset": asset,
            "heads": heads,
            "full": full,
            "planner": planner,
            "inputs": inputs,
            "pins": pins,
            "incremental": incremental,
            "definition": self._definition(task["asset"], run),
            # The whole and dep versions read, which the commit records (its context).
            "context": self._context(planner, inputs),
        }

    def _prepared(
        self, task: dict, run: dict, attempt: str | None, base: dict, plans: dict, more, skip, each_page
    ) -> dict:
        """The second half of preparing an attempt, either path's: what it
        reads and writes, once its incremental inputs are planned."""

        asset, heads, full, pins = base["asset"], base["heads"], base["full"], base["pins"]
        partition = task["partition"]
        claim = self.m.claimed(attempt) if attempt is not None else None
        if claim is not None:
            # What this attempt reads stays endpoints of its upstreams until it finishes (§6).
            claim["reads"] = reads(plans)
        # A plain consumer never built is built, an empty input or not; a per-key asset
        # whose keys all failed so far has no head yet: nothing to wait for.
        if skip and each_page is None and not any(heads.values()):
            skip = False
        # A partition a dead writer left owing a repair launches all the same: its attempt repairs first.
        if skip and any(
            (o["name"], partition) in self.m.repairs
            for o in self.manifest["assets"][task["asset"]]["outputs"]
        ):
            skip = False
        # A full run's write is the whole content. A per-key batch's is not: it
        # patches by key, and a key that fails keeps its last good output (§5).
        reset = full and each_page is None
        cursor = None if reset else self.m.partition(task["asset"], partition).get("cursor")
        outputs = {}
        for output in asset["outputs"]:
            name, head = output["name"], heads[output["name"]]
            info = {
                # The committed head it writes over: its ref says where the content is.
                "head": head,
                # The contract it is launched under: settled, failed and cleaned up
                # by it, whatever is served by then.
                "contract": {
                    "store": output["store"],
                    "in_place": bool(self.manifest["stores"][output["store"]].get("in_place")),
                    "key": output.get("key"),
                    "incremental": output.get("incremental"),
                },
            }
            # Whether the write starts the content over: a first write (an output
            # reset by a move holds no head), or a full run.
            info["reset"] = reset or head is None
            # A full run in several batches (STO-8): whether an earlier batch of it committed
            # (the core's mark of a full run under way), and whether this batch ends the walk.
            # A store that others read in place swaps the old rows for the new at the last one.
            info["final"] = not more
            info["building"] = (
                bool(output.get("incremental"))
                and self.m.core.full_run(output_id(name, partition)) is not None
            )
            if is_log(output):
                # Where its commit goes: the engine names the layer, the worker writes it.
                found = self.m.core.output(self.core.named(name, partition))
                info["log"] = {
                    "pin": output_pin(self.core, name, partition),
                    "url": self.state.objects_url,
                    "path": self.core.path(found["life"], found["head"] + 1, attempt or "-"),
                    "commit": found["head"] + 1,
                    "live": found["live"],
                    "clear": info["reset"],  # a full run starts the log over
                }
            if output.get("key") is not None:
                info["core"] = output_pin(self.core, name, partition)
                if (name, partition) in self.m.repairs:
                    info["repairs"] = [version for version, _ in self.m.repairs[(name, partition)]]
                if output.get("dynamic_partitions") or name in self._dynamic_dims:
                    info["dynamic"] = True  # its elements, the engine's to work out at commit
            outputs[name] = info
        # The input versions its outputs will be built from, for the history (§7):
        # each pinned ref's generation (docs/versions.md §6).
        lineage = []
        for input in base["inputs"]:
            pin = pins.get(input.param) or {}
            refs = [pin["ref"]] if pin.get("ref") else list((pin.get("refs") or {}).values())
            for ref in refs:
                lineage.append([input.output, ref.get("partition") or "", ref.get("generation"), input.param])
        return {
            "version": asset["version"],
            "deploy_number": self.m.deploy_number,
            # The version of each whole or dep input read: a partition with no keyed
            # input whose inputs moved since is stale (a keyed one's layers carry it).
            "context": base["context"],
            "config": run.get("config") or {},
            "declaration": digest(self._declaration(task["asset"])),
            "prefixes": self._prefixes(pins, outputs, task),
            "inputs": pins,
            "lineage": lineage,
            "plans": plans,
            "more": more,
            "full": full,
            "skip": skip,
            "cursor": cursor,
            "definition": base["definition"],  # what its inputs' observations are made under
            "outputs": outputs,
        }

    def _prefixes(self, inputs: dict, outputs: dict, task: dict) -> list[str]:
        """What an attempt reads, as reader pins name it: the index prefix of
        every output partition it reads or writes — a value's or an unkeyed incremental
        output's too, whose data its partition's garbage names — and of any index
        its pins name (an outcome index). Collection elsewhere waits for no
        attempt that reads none of it."""

        found = set()

        def reads(output: str, partition: str) -> None:
            found.add(self.m.index(output, partition).prefix)
            if (held := self.m.core.output(output_id(output, partition))) is not None:
                found.add(f"core/{held['life']}/")  # its layers in the change index (`Core.path`)

        for name in outputs:
            reads(name, task["partition"])

        def walk(value) -> None:
            if isinstance(value, dict):
                if isinstance(value.get("prefix"), str):
                    found.add(value["prefix"])
                if isinstance(value.get("output"), str) and "handle" in value:  # a ref
                    reads(value["output"], value.get("partition") or "")
                for item in value.values():
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(inputs)
        walk(outputs)
        return sorted(found)

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
            info.pop("core", None)  # the output's layers: the spec's, never the journal's
            if "log" in info:  # its layers too; the commit needs where its layer was to go
                info["log"] = {k: v for k, v in info["log"].items() if k != "pin"}
            outputs[name] = info
        kept = {
            k: prepared.get(k)
            for k in (
                "version",
                "deploy_number",
                "context",
                "config",
                "declaration",
                "prefixes",
                "plans",
                "more",
                "full",
                "lineage",
                "definition",
                "cleanup",  # a cleanup task's entry (K25)
            )
        }
        if "gate" in prepared:  # the new path's: the upstream heads it read, checked again (`full.held`)
            kept["gate"] = prepared["gate"]
        return {**kept, "outputs": outputs}

    def _pin_at(self, output: str, upstream_partition: str):
        """Head ref for one upstream partition; sources synthesize theirs (§5, §8)."""

        head = self.m.heads.get((output, upstream_partition))
        if head is None:
            source = self.manifest["sources"].get(output)
            if source is not None and upstream_partition == "":
                return dict(source["head"])
            raise Retryable(f"input {output!r} has no head for partition {upstream_partition!r}")
        return head["ref"]

    def _whole_index(self, output: str, ref: dict) -> dict | None:
        """The pinned key index a whole read of a keyed output on an immutable
        store needs to name its objects (docs/lifecycle.md §9.8); for a log,
        the layers its rows are in."""

        record = self.manifest["outputs"].get(output) or {}
        store = self.manifest["stores"].get(record.get("store")) or {}
        if is_log(record):  # the core's to read, unless its store keeps the rows
            if store.get("in_place"):
                return None
            return {"log": output_pin(self.core, output, ref.get("partition") or "")}
        if record.get("key") is None or record.get("dynamic_partitions") or store.get("in_place"):
            return None
        return {"core": output_pin(self.core, output, ref.get("partition") or "")}

    def _log(self, input: planning.Input) -> bool:
        """Whether an input reads a log."""

        return is_log(self.manifest["outputs"].get(input.output))

    @staticmethod
    def _versioned(input: planning.Input) -> bool:
        """Whether staleness follows an input's version: a whole input or a dep,
        but not a partition set's implied dep (adding a key changes nothing
        already built), nor an incremental one, which its observations follow."""

        return input.kind != "incremental" and not input.set_dim

    def _input_refs(self, planner: planning.Planner, input: planning.Input) -> dict:
        """The head refs a whole or dep input reads, by partition key."""

        if input.kind == "in" and input.fan_in:
            return self._whole_fan_in(planner, input)
        if input.fan_in:
            return {k: h["ref"] for k, h in planner.fan_in(input, materialized=False).items()}
        try:
            return {input.partition: self._pin_at(input.output, input.partition)}
        except Retryable:
            return {}

    @staticmethod
    def _whole_fan_in(planner: planning.Planner, input: planning.Input) -> dict:
        """A whole fan-in pins every current upstream partition with a complete
        head (any head on the new core, whose gate holds a partial one) that agrees with this partition's shared keys, keyed by the dimensions
        it collapses (§7) — every dimension, with `all_partitions` — chosen
        among the heads that exist, never by expanding the partition domain."""

        return {input.key(s): h["ref"] for s, h in planner.fan_in(input, materialized=True).items()}

    # -- per-key batches (docs/per-key-processing.md §5, §9) ------------------------------

    def _definition(self, asset: str, run) -> str:
        """The definition its inputs' observations are made under:
        H(`model.declaration`, its inputs' bindings included, without
        patterns and batch size — and the run's config). A change makes the
        next run a full run (docs/observed-set.md, "Context and lives"), so
        binding an input to another output starts over. Not its inputs'
        versions: a whole or dep input that moves is an input change, each
        layer's context. Neither which store holds an output nor its name is
        in it, only the versions: a move resets the output (`Model._reset`);
        a rename keeps everything."""

        definition = self._declaration(asset)
        # New patterns are compared, a batch size only pages: neither starts anything over.
        definition["inputs"] = {
            p: {k: v for k, v in i.items() if k not in ("patterns", "batch_size")}
            for p, i in definition["inputs"].items()
        }
        return digest({**definition, "config": run.get("config") or {}})

    @staticmethod
    def _made(prepared: dict) -> dict:
        """What every commit says it was made under: the definition, the run's
        config, and the whole and dep versions read (staleness compares them)."""

        return {k: prepared[k] for k in ("definition", "config", "context") if k in prepared}

    def _declaration(self, asset: str) -> dict:
        """The asset's canonical definition (`model.declaration`): a change makes a
        full pass due, and an attempt claimed before it does not commit
        (docs/observed-set.md, "The commit check")."""

        return declaration(self.manifest, asset)

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
        """Install an attempt's result: heads, cursor, what its inputs observed
        (§8). A drained Each batch commits as the attempt it ended as:
        `canceled`, or `failed` (a timeout, retryable). Its observations are
        decided first; nothing awaited after them, the rest installs as one."""

        claim = self.m.claimed(attempt)
        if claim is None:
            raise LostOwnership(attempt)
        task = self.m.task(self.m.attempts[attempt])
        if prepared.get("cleanup") is not None:  # a cleanup task: its end, and what it cleaned up (K25)
            commit = {"cleaned": prepared["cleanup"].get("id")}  # an output life's
            self._finish(
                task,
                claim,
                outcome,
                commit=commit,
                error=error,
                retryable=retryable,
                delay=delay,
                worker=result,
            )
            return {"run": task["run"], "attempt": attempt, "outputs": {}}
        # Inputs may have moved since they were pinned: the attempt's output
        # derives from what it read (the spec records it), its positions
        # cover only the batch it was given, and a moved input changes the
        # next attempt's definition. Refusing here would only strand a write
        # a shared-table store has already made.
        self._commit_guards(task, prepared)
        settle = {"outcome": outcome, "error": error, "retryable": retryable, "delay": delay}
        outputs = result.get("outputs") or {}
        built = await core_commit.build(self.core, task, attempt, prepared, result, outputs)
        async with built.locks:  # the outputs it built: their commits are recorded in turn
            with built.generation:  # checked again where it is recorded (`core_commit.check`)
                if self.m.claimed(attempt) is not claim:
                    raise LostOwnership(attempt)
                self._commit_guards(task, prepared)  # again: the build awaited
                out = self._install(task, claim, attempt, prepared, result, built, **settle)
        await core_sources.record_loads(self, attempt, prepared, result)
        return out

    def _commit_guards(self, task: dict, prepared: dict) -> None:
        """Output heads must be unchanged since the claim, and no output it writes
        or reads incrementally reset since it launched (removed, or moved to
        another store): what it built belongs to the output's earlier life."""

        for output, info in (prepared.get("outputs") or {}).items():
            if commit_of(self.m.heads.get((output, task["partition"]))) != commit_of(info["head"]):
                raise Conflict(f"output {output} head changed since this attempt was claimed")
        upstreams = [p["output"] for p in (prepared.get("plans") or {}).values() if p]
        reset = [("asset", task["asset"])] + [
            ("output", o) for o in [*(prepared.get("outputs") or {}), *upstreams]
        ]
        for kind, name in reset:
            if self.m.reset_at.get((kind, name), 0) > prepared["deploy_number"]:
                raise Conflict(f"{kind} {name} was reset since this attempt launched")
        # Nor does a batch planned before an asset change finish the full pass due
        # after it: what it built is the old definition's (Positions.tla).
        current = self.manifest["assets"].get(task["asset"])
        if current is not None and prepared.get("declaration") not in (
            None,
            digest(self._declaration(task["asset"])),
        ):
            raise Conflict(f"asset {task['asset']} changed since this attempt launched")

    def _install(
        self, task, claim, attempt, prepared, result, built, *, outcome, error, retryable, delay
    ) -> dict:
        """The rest of committing an attempt, once its core commit is built
        (`built`): its outputs' heads and the commit, installed in one
        synchronous step."""

        from solera.sdk import UNSET

        outputs = result.get("outputs") or {}
        # Settled under the contract it was launched with, not today's manifest.
        declared = {name: info["contract"] for name, info in (prepared.get("outputs") or {}).items()}
        more = bool(prepared.get("more"))
        outcomes = None
        for plan in (prepared.get("plans") or {}).values():
            more = more or not (plan is None or plan["final"] or plan.get("done"))  # the walk goes on
        more = more or bool((built.progress or {}).get("held"))  # keys held back under a rebuild
        heads = {}
        for name, entry in outputs.items():
            if name not in declared:
                raise Conflict(f"result names undeclared output {name!r}", retryable=False)
            info = prepared["outputs"][name]
            decl, before = info["contract"], info["head"]
            if entry.get("unchanged"):
                if before is None:
                    raise Conflict(f"output {name}: unchanged, but there is no head", retryable=False)
                ref = before["ref"]
            else:
                ref = entry.get("ref")
                if ref is None or ref["partition"] != task["partition"]:
                    raise Conflict(f"output {name}: ref partition != {task['partition']!r}", retryable=False)
            head = {"ref": ref, "version": prepared["version"]}
            if decl.get("incremental"):
                counts = built.counts.get(name)
                schema = (ref.get("handle") or {}).get("schema")
                if (
                    counts is None
                    and before is not None
                    and schema == (before["ref"].get("handle") or {}).get("schema")
                ):
                    head["ref"] = before["ref"]  # it changed no key, nor its schema: the head stands
                head["commit_number"] = (
                    counts["commit"] if counts else int((before or {}).get("commit_number", 0))
                )
                if info.get("dynamic"):
                    head["partitions"] = built.elements.get(name, (before or {}).get("partitions") or [])
            heads[name] = head
            self.anomalies.pop((name, task["partition"]), None)  # raised again if a batch still finds one
        for name in set(declared) - set(outputs):
            # A per-key batch whose keys all failed writes nothing, and makes no head yet;
            # nor does a batch whose keys the input's patterns all left out.
            if prepared["outputs"][name]["head"] is None and outcomes is None and not result.get("skipped"):
                raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)
        commit = {"heads": heads, **self._made(prepared)}
        if built.progress is not None:
            commit["progress"] = {WALK: built.progress}
        commit["final"] = not more and outcome in SUCCESS  # a canceled run has no final commit
        if outcomes is not None:
            commit["outcomes"] = outcomes
            if result.get("key_outcomes"):  # the history reads them from the result (D178)
                commit["key_outcomes"] = True
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
        repaired = sorted(name for name, entry in outputs.items() if entry.get("repaired"))
        if repaired:
            commit["repaired"] = repaired
        if result.get("cursor", UNSET) is not UNSET:
            commit["cursor"] = result["cursor"]
        elif prepared.get("full"):
            commit["cursor"] = None  # a full run clears the committed cursor (§8)
        worked = bool((task.get("outcomes") or {}).get("succeeded"))
        if outcome == "succeeded" and result.get("skipped") and not more and not worked:
            outcome = "skipped"  # the patterns took none of its keys: it did nothing (per-key §11)
        if (why := core_full.held(self.m, prepared.get("gate") or ())) is not None:
            raise core_commit.HeldBack(
                f"{why[0]}: {why[1]}"
            )  # a head it read is partial: never, by `prepare`
        core_commit.check(self.core, built)  # checked, then recorded with nothing awaited between (rule 3)
        if (core := built.payload()) is not None:
            commit["core"] = core
        for param in built.walked:
            key = (task["asset"], task["partition"], param)
            self._walked[key] = self._walked.get(key, 0) + 1
        self._finish(
            task,
            claim,
            outcome,
            commit=commit,
            more=more and outcome in SUCCESS,
            worker=result,
            write=result.get("write"),
            error=error,
            retryable=retryable,
            delay=delay,
            keys=result.get("keys"),
        )
        return {"run": task["run"], "attempt": attempt, "outputs": outputs}

    # -- sources commit API (§5) ------------------------------------------------------

    async def commit_source(
        self,
        name: str,
        *,
        version=None,
        keys=None,
        upsert=None,
        remove=None,
        by: str | None = None,
        at: int | None = None,
    ):
        """Advance a source without moving data (§2.3, §6; docs/versions.md
        §2). A keyed source commit is checked against the source's key index
        like any write: a full map (`keys=`) replaces its content,
        `upsert`/`remove` patch it, and the changes become the commit's delta
        file. A map gives each key its version, and a key at the version
        its entry holds is unchanged; a list names keys with none, each a
        change. An unkeyed source takes a `version=`: the head's, no change.
        A commit that changes nothing is not a change.

        Every change is recorded as a run with no tasks (§7):
        `{"id", "source", "by", "batch", "upserted", "deleted"}` — or `version`
        for an unkeyed source. `by` says where the commit came from. `at`
        (the new path) makes `upsert` a load's report on the source's log at
        that commit (`tracking.sources.record_loads`)."""

        head, generation = self.m.heads.get((name, "")), Generation(self.m)
        # Its delta is named by nothing until recorded: its index is held as read
        # meanwhile, so the orphan collector leaves it be.
        with self.m.reading(self.m.index(name, "").prefix):
            event, ref = await self._prepare_commit(name, version, keys, upsert, remove, by, at=at)
            if event is None:
                return {"changed": False, "ref": ref}
            redeployed = generation.moved
            if commit_of(self.m.heads.get((name, ""))) != commit_of(head) or redeployed:
                await self._drop_prepared([event])
                raise Conflict(f"source {name!r} moved while committing; retry")
            event["at"] = self.clock()
            self.core.check_payload(event.get("core"))
            self.state.record(event)
        self._committed_keys([event])
        return {"changed": True, "ref": ref, "run": event["run"]["id"]}

    async def _prepare_commit(self, name, version, keys, upsert, remove, by, tags=None, at=None):
        """A source commit's `SourceCommitted`, its delta file written but
        nothing recorded, and the ref it installs; no event if it changes
        nothing. Whoever does not record it drops it (`_drop_prepared`).
        Its generation is the event counter it is prepared at, as an
        attempt's is its claim's: larger than any commit before it."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        # Adapters say "no removals" as an empty list: the same as none, for any source.
        remove = remove or None
        head = self.m.heads.get((name, ""))
        keyed = source.get("key") is not None
        if not keyed and (keys is not None or upsert is not None or remove is not None):
            raise ValueError(f"Source {name!r} is unkeyed; pass version=")
        generation = self.m.event_counter
        ref = {**(head["ref"] if head is not None else source["head"]), "generation": generation}
        run_id = ulid(self.clock())
        record = {
            "ref": ref,
            "run": run_id,
            "attempt": None,
            "version": None,
        }
        event = {"type": "SourceCommitted", "source": name, "head": record}
        run = {"id": run_id, "source": name, "by": by, **({"tags": tags} if tags else {})}
        if not keyed:
            if version is None:
                raise ValueError(f"Source {name!r} requires version=")
            if head is not None and head.get("version") == str(version):
                return None, head["ref"]
            record["version"] = run["version"] = str(version)
        else:
            # A dynamic partitions's elements carry an empty version: listed again, unchanged.
            listed = b"" if source.get("key") == "<partitions>" or name in self._dynamic_dims else None

            built = await core_sources.build(
                self.core,
                name,
                generation,
                run_id,
                keys,
                upsert,
                remove,
                "" if listed is not None else None,
                at,
            )
            if built is None:
                return None, head["ref"] if head is not None else source["head"]
            record["commit_number"] = run["commit_number"] = built["counts"]["commit"]
            if listed is not None:
                record["partitions"] = built["elements"]
            event["core"] = {
                "event": b64(built["event"]),
                "counts": {name: built["counts"]},
            }
            run["upserted"], run["deleted"] = built["upserted"], built["deleted"]
        ref["meta"] = {**(ref.get("meta") or {}), "source": True}
        event["run"] = run
        return event, ref

    def _committed_keys(self, events: list[dict]) -> None:
        """Warm the key cache with what recorded source commits installed."""

    async def _drop_prepared(self, events: list[dict]) -> None:
        """Delete the delta files of prepared commits never recorded."""

        paths = []
        for event in events:
            if "keys" in event:
                index = self.m.index(event["source"], "")
                paths += [index.path(n) for n in delta_names(event["keys"])]
        if paths:
            await self.state.delete_objects(paths)

    def _key_cache(self):
        """The engine's cache of layers, where the key service runs: what the
        engine's own reads and writes go through."""

        return None

    # -- key index upkeep (§6) --------------------------------------------------------

    def _key_io(self) -> ObjectIO:
        if self._io is None:
            self._io = ObjectIO(self.state.objects)
        return self._io

    async def list_keys(self, output: str, partition: str = "", *, after=None, offset=0, limit=1000) -> dict:
        """One page of an output's live keys, read from its key index."""

        if (output, partition) not in self.m.heads:
            raise KeyError(f"{output}/{partition}")
        found = self.m.core_output(output, partition)
        if found is None or is_log(self.manifest["outputs"].get(output)):  # a log has rows, no keys
            return {"total": 0, "keys": {}, "next": None}
        rows, cursor = [], after
        while len(rows) < offset + limit:
            page, cursor = await self.core.run(
                self.core.index.scan,
                found["layers"],
                found["head"],
                after=cursor,
                limit=offset + limit - len(rows),
            )
            rows += page
            if cursor is None:
                break
        return {"total": found["live"], "keys": {r[0]: r[2] for r in rows[offset:]}, "next": cursor}

    # -- automations (§9) ------------------------------------------------------------

    @staticmethod
    def _due_at(auto: dict) -> float | None:
        """When a schedule comes due (§9): `every` its interval, `cron` its next
        time, after it last fired — or, if it never has, after it was declared
        (`since`): a schedule waits for its next time, a new one too. `None`
        for the triggers that wait on an event, and for a disabled automation."""

        trigger = auto["trigger"]
        if not auto["enabled"]:
            return None
        last = auto["last_fired"] if auto["last_fired"] is not None else auto["since"]
        if trigger["kind"] == "every":
            return last + trigger["seconds"]
        if trigger["kind"] == "cron":
            zone = ZoneInfo(trigger.get("timezone") or "UTC")
            base = dt.datetime.fromtimestamp(last, zone)
            return croniter(trigger["expression"], base).get_next(dt.datetime).timestamp()
        return None

    def automation_view(self, auto: dict) -> dict:
        """An automation as the API shows it: a copy, with `next_at`, when its
        schedule next fires (now, if it is due), or null."""

        due = self._due_at(auto)
        return {**auto, "next_at": None if due is None else max(due, self.clock())}

    def _automation_tick(self):
        now = self.clock()
        fired = []
        for auto in list(self.m.automations.values()):
            if not auto["enabled"]:
                continue
            trigger = auto["trigger"]
            if trigger["kind"] in ("every", "cron"):
                if self._due_at(auto) <= now:
                    fired.append((auto, "schedule"))
            elif trigger["kind"] == "onchange":
                if auto["pending"]:
                    fired.append((auto, "onchange"))
            elif trigger["kind"] == "ondeploy":
                if auto.get("last_deploy") != self.manifest["deploy"]:
                    fired.append((auto, "ondeploy"))
        for auto, why in fired:
            if why == "onchange":
                self._fire_onchange(auto)
            elif why == "ondeploy":
                self._fire_ondeploy(auto)
            else:
                self._fire(auto, auto.get("partitions") or "latest")

    def _automation_run(self, auto, partitions, targets=None) -> dict | None:
        """The run a firing becomes, in the run's own vocabulary (§9); `None`
        if every partition is in flight or waits for its inputs."""

        return self._plan_run(
            targets or auto["targets"],
            partitions,
            auto.get("mode") or "incremental",
            auto.get("upstream"),
            auto.get("config"),
            auto.get("keys"),
            automation=auto["name"],
            skip_active=True,
            skip_missing_inputs=auto.get("skip_missing_inputs"),
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

    def _fire(self, auto, partitions):
        run = None
        try:
            run = self._automation_run(auto, partitions)
        except Exception as error:
            self.failing[f"automation {auto['name']}"] = str(error)
        else:
            self.failing.pop(f"automation {auto['name']}", None)
        self._fired(auto, run)

    def _fire_ondeploy(self, auto):
        """§9: fire once for the served deploy, then record it. A planning
        error leaves last_deploy unset so the next tick retries."""

        try:
            run = self._automation_run(auto, auto.get("partitions") or "latest")
        except Exception as error:
            self.failing[f"automation {auto['name']}"] = str(error)
            return
        self.failing.pop(f"automation {auto['name']}", None)
        self._fired(auto, run, deploy=self.manifest["deploy"])

    def _fire_onchange(self, auto):
        """One run per firing (§9). Each target's partitions are the automation's
        `partitions`, if it names them, else every changed upstream partition's
        projection onto the target (§7); planned together, a target that
        reads another waits for it. The changes it covers leave the pending
        set with it. A change waits — pending, never consumed — while work it
        is owed is already claimed or queued (that work would not see it, and
        the firing could not order after it), and while a target cannot read
        it yet (`Planner.visible`)."""

        planner, explicit = self.planner(), auto.get("partitions")
        consumed, selected = [], {}
        try:
            named = {t: planner.partitions(t, explicit) for t in auto["targets"]} if explicit else None
            for producer, partition in (list(p) for p in auto["pending"]):
                owed = named or {t: planner.reach(producer, partition, t) for t in auto["targets"]}
                if any(
                    self._partition_active(t, s) or self._backing_off(t, s)
                    for t, partitions in owed.items()
                    for s in partitions
                ):
                    continue
                if not all(planner.visible(producer, partition, t) for t in owed):
                    continue
                consumed.append([producer, partition])
                for target, partitions in owed.items():
                    selected.setdefault(target, set()).update(partitions)
            if not consumed:
                return
            selection = {t: sorted(partitions) for t, partitions in selected.items() if partitions}
            run = self._automation_run(auto, selection, sorted(selection)) if selection else None
        except Exception as error:
            self.failing[f"automation {auto['name']}"] = str(error)
            return  # the changes stay pending: the next tick replays them
        self.failing.pop(f"automation {auto['name']}", None)
        self._fired(auto, run, consumed=consumed)

    def _backing_off(self, asset: str, partition: str) -> bool:
        """Whether a partition whose runs keep failing waits before a change
        runs it again (F43): `CHANGE_BACKOFF` after its last failure,
        doubling with each failure in a row up to `CHANGE_BACKOFF_MAX`. Its
        changes stay pending, and it stays failed and visible; a run that
        succeeds — or one a user starts — ends the wait."""

        record = self.m.partition(asset, partition)
        failed = int(record.get("failed_in_row") or 0)
        if not failed:
            return False
        wait = min(CHANGE_BACKOFF_MAX, CHANGE_BACKOFF * 2 ** (failed - 1))
        return self.clock() < record["last"]["at"] + wait

    async def set_automation(self, name: str, enabled: bool):
        if name not in self.m.automations:
            raise KeyError(name)
        self.state.record({"type": "AutomationChanged", "name": name, "enabled": bool(enabled)})
        return self.m.automations[name]

    async def run_automation(self, name: str):
        auto = self.m.automations.get(name)
        if auto is None:
            raise KeyError(name)
        self._fire(auto, auto.get("partitions") or "latest")
        return self.m.automations[name]

    # -- run control ------------------------------------------------------------------

    def _control(self, run_id: str, action: str, by: str | None):
        event = {"type": "RunControlled", "run": run_id, "action": action, "at": self.clock()}
        self.state.record({**event, "by": by} if by else event)
        for run, attempt in self.watchers:
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
        partitions: dict[str, list[str]] = {}
        for task in sorted(run["tasks"].values(), key=lambda t: t["id"]):
            if task["status"] in ("failed", "canceled", "blocked"):
                partitions.setdefault(task["asset"], []).append(task["partition"])
        if not partitions:
            raise Conflict(f"run {run_id} has nothing to retry", retryable=False)
        # The request is the selected work's: key overrides only for the inputs it reads.
        read = {
            e["output"]
            for asset in partitions
            if asset in self.manifest["assets"]
            for e in self.manifest["assets"][asset]["inputs"].values()
            if e["kind"] == "incremental"
        }
        keys = {output: k for output, k in (run.get("keys") or {}).items() if output in read}
        return await self.submit(
            sorted(partitions),
            partitions=partitions,
            mode=run.get("mode") or "incremental",
            config=run.get("config"),
            keys=keys or None,
            by=by,
            tags=run.get("tags"),
            retry_of=run_id,
        )

    # -- finished runs -------------------------------------------------------------------

    def _archive_due(self):
        """Move finished runs from memory into the history, once none of
        their attempts is still in flight here (§7)."""

        busy = {run_id for run_id, _ in self.watchers}
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

    TASK_INTERNALS = (
        *("launched", "ready_at", "queued_at"),
        *("tries", "outcomes", "duration", "first_at", "last_at", "last", "error", "executor"),
    )

    def _task_view(self, task: dict, attempts: list[dict], live: bool) -> dict:
        view = {k: v for k, v in task.items() if k not in self.TASK_INTERNALS}
        claim = self.m.claims.get(task["id"]) if live else None
        view["status"] = claim["status"] if claim else task["status"]
        view["generation"] = view["attempt_count"] = len(attempts) + (1 if claim else 0)
        view["progress"] = history.progress(task)
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
                "outcome": a["outcome"],
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
                view["keys"] = a["keys"]  # a per-key attempt's keys by outcome
            if a.get("batch"):
                view["batch"] = a["batch"]  # a keyed attempt's batch
            view["handle"] = a.get("handle")  # its executor's, as recorded at launch (D170)
            out.append(view)
        claim = self.m.claims.get(task["id"]) if live else None
        if claim:
            launching = getattr(self.live.get(claim["attempt"]), "launching", False)  # not durable yet
            out.append(
                {
                    "id": claim["attempt"],
                    "task": task["id"],
                    "generation": len(out) + 1,
                    "outcome": "launching" if launching else claim["status"],
                    "started_at": claim["started_at"],
                    "handle": history.handle(task.get("launched")),
                    **(history.execution(task["launched"]["execution"]) if claim.get("launched") else {}),
                }
            )
            found = history.batch(((task.get("launched") or {}).get("prepared") or {}).get("plans"))
            if found:
                out[-1]["batch"] = found
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

    def head_view(self, output: str, partition: str, head: dict) -> dict:
        """A head as the API shows it: with its commit, its live keys if keyed
        (`key_count`, its index's), and whether it is of a complete pass —
        its partition's progress says (§7)."""

        owner = self.manifest["outputs"][output].get("asset")
        view = {**head, "asset": owner, "key_count": self.m.key_count(output, partition)}
        view["commit"] = f"{head['run']}/{head['attempt']}" if head.get("attempt") else None
        view["materialized"] = owner is None or self.planner().complete(
            owner, head["ref"].get("partition") or ""
        )
        return view

    def outcome_view(self, record: dict) -> dict:
        return {
            "last_outcome": record["outcome"],
            "last_attempt": f"{record['run']}/{record['attempt']}" if record.get("attempt") else None,
            "at": record["at"],
        }

    async def asset_detail(self, name: str, partition=""):
        asset = self.planner().asset_of(name)
        info = self.manifest["assets"][asset]
        heads = {
            o["name"]: [(s, self.head_view(o["name"], s, h)) for s, h in self.m.heads_of(o["name"])]
            for o in info["outputs"]
        }
        dims = self.planner().dims(asset)
        return {
            "asset": info,
            "heads": heads,
            "cursor": self.m.partition(asset, partition).get("cursor"),
            "current_keys": self.planner().dim_keys(dims) if dims else [],
            "repairs": {
                o["name"]: sorted(s for (n, s) in self.m.repairs if n == o["name"]) for o in info["outputs"]
            },
            "partitions": {
                s: self.outcome_view(r["last"]) for s, r in self.m.partitions.of(asset).items() if "last" in r
            },
        }

    async def catalog(self):
        assets = []
        for name, info in self.manifest["assets"].items():
            heads = {
                o["name"]: {s: self.head_view(o["name"], s, h) for s, h in self.m.heads_of(o["name"])}
                for o in info["outputs"]
            }
            assets.append({**info, "name": name, "heads": heads})
        return assets
