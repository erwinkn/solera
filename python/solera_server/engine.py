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
import secrets
from zoneinfo import ZoneInfo

from croniter import croniter
from obstore.exceptions import AlreadyExistsError
from solera import lifecycle
from solera.failed_keys import lower
from solera.ids import ulid, ulid_time
from solera.keys import Rows, SortedEntries
from solera.keys.index import (
    DeltaFiles,
    FileInfo,
    KeyIndex,
    Options,
    delta_keys,
    key_bytes,
    key_str,
)
from solera.keys.io import ObjectIO
from solera.sdk import default_placement, digest
from solera.tasks import Tasks

from . import history, planning
from .attempts import POOL_OFFERED_GRACE, Attempts, Live, current_names, worker_report
from .executors import PlacementContext, Registry
from .history import MAX_METADATA, History, RunFilter
from .keyservice import KeyService, cache_root
from .model import (
    CLEANUP,
    REPAIR_RUNS,
    REPAIR_SPACING,
    TERMINAL_RUN,
    TERMINAL_TASK,
    _delta_files,
    commit_of,
    declaration,
    reads,
)
from .observing import Observing
from .sensors import Sensors
from .staleness import Staleness
from .state import Conflict, LostOwnership, State
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
SOURCE_KEYS_RECORDED = 1000  # a source commit's run lists changed keys up to this many, else counts
GRACE_SECONDS = 5.0
CLEANUP_TRIES = 6  # a cleanup task's attempts, its retries backing off from a minute (K25)
CLEANUP_RETRY_DELAY = 60.0  # a failed cleanup task's first retry, then doubling
CHANGE_BACKOFF, CHANGE_BACKOFF_MAX = 60.0, 3600.0  # a failing partition's changes wait, doubling (F43)


class Retryable(RuntimeError):
    """A dispatch-time failure that the retry policy may absorb."""


class NonRetryable(RuntimeError):
    """A dispatch-time failure no retry will fix (§8: full run required, …)."""


def _batches(keys: int, limit: int) -> int:
    """Batches of `limit` a pass of `keys` is planned to take: at least one."""

    return max(1, -(-int(keys) // max(1, int(limit))))


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
        key_options: Options | None = None,
        maintenance_concurrency: int = 2,
        retention_interval: float = 60.0,
        history: History | None = None,
        resolve_cache: str | None | bool = True,
        sensor_host=None,
        cleanup_interval: float = CLEANUP_INTERVAL,
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
        self.local_app = None  # its routes for workers it runs in this process (`api.local_transport`)
        # Where workers reach this engine (docs/lifecycle.md §5); without one,
        # they report through `.beat` alone.
        self.engine_url = engine_url
        self.pool_offered_grace = pool_offered_grace
        self.secret: bytes | None = None  # signs attempt tokens; stable across restarts
        self.live: dict[str, Live] = {}  # attempt id -> what its worker reported
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
        # attempt id -> set when its run is controlled, so its watcher looks at once.
        self._stirred: dict[str, asyncio.Event] = {}
        # Attempts consuming a local execution slot. Pool attempts only poll
        # state — they run no local work and must not starve dispatch (§10).
        self.engine_inflight: set[str] = set()
        self.executor_inflight: dict[str, int] = {}
        self.key_options = key_options or Options()
        self._io: ObjectIO | None = None
        # The key cache and resolver (docs/resolved-commits.md §4–§5): where
        # `resolve_cache` says, else beside `file://` state or in a temporary
        # directory; None or False: off.
        self.keys: KeyService | None = None
        if resolve_cache:
            root = resolve_cache if isinstance(resolve_cache, str) else cache_root(state.objects_url)
            self.keys = KeyService(state.objects, root, options=self.key_options)
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
            key_options=self.key_options,
            concurrency=maintenance_concurrency,
            retention_interval=retention_interval,
            keys=self.keys,
            failing=self.failing,
        )
        self._sensors_init(sensor_host)
        self._dynamic_dims = {
            dim["output"]
            for a in manifest["assets"].values()
            for dim in ((a.get("partitions") or {}).get("dims") or {}).values()
            if dim["kind"] == "dynamic"
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
            self.state.record({"type": "EngineOutage", "down": down, "at": now})
        if m.deploy != self.manifest["deploy"] or m.manifest != self.manifest or m.project != self.project:
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

    def _owed_firings(self) -> dict[str, list[list[str]]]:
        """What the deploy just registered leaves each `OnChange` automation
        owing, once: for each target it changed — added (again), renamed,
        its declaration changed, or reset (`Model.changed_at`) — every
        current partition whose inputs have heads, so it is built now, not
        when its upstream next changes. Decided at the deploy and
        recorded, never re-checked by a tick: a run of it that fails is not
        resubmitted on every tick. Schedules, OnDeploy and sensors keep
        their own criteria."""

        planner, owed = self.planner(), {}
        changed = {a for a, n in self.m.changed_at.items() if n == self.m.event_counter}
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

        if self.keys is not None:
            try:
                self.keys.start()
            except Exception as e:  # an accelerator: without it, workers resolve their writes
                log.warning("key cache disabled: %s", e)
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
        if self.keys is not None:
            await self.keys.stop()

    async def tick(self):
        """One evaluation pass: adoption, dispatch, automations, archiving."""

        self._adopt()
        await self._cover_tick()
        self._cleanup_job()
        self._dispatch_due()
        self._sensor_sweep()
        self._automation_tick()
        await self._retry_tick()
        await self._repair_tick()
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
        repairs=None,
        worker=None,
        end=None,
        reason=None,
        write=None,
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
        elif task["asset"] in self.manifest["assets"]:
            # What it wrote is cleaned up once a worker that outlives it can no longer add to
            # it: the asset's timeout and cancel grace after its end (the sweep, §9.8).
            info = self.manifest["assets"][task["asset"]]
            event["late_writes"] = float(info.get("timeout") or 3600) + self.cancel_grace
        if more:
            event["more"] = True
        if repairs:
            event["intents"] = repairs
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
        prepared = (task.get("launched") or {}).get("prepared") or {}
        for field in ("cleaned_up", "cleanup_unresolved"):  # cleanup (§9.8)
            if worker.get(field):
                event[field] = current_names(prepared, worker[field])
        if worker.get("cleaned_files"):
            event["cleaned_files"] = worker["cleaned_files"]
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
                if (
                    sum(len(i.get("cleanup") or ()) for i in (handed.get("outputs") or {}).values())
                    >= CLEANUPS
                ):
                    self._submit_cleanups([(cleanup["output"], cleanup["partition"])])
            elif task["asset"] in self.manifest["assets"]:
                outputs = self.manifest["assets"][task["asset"]]["outputs"]
                self._submit_cleanups(
                    [(o["name"], task["partition"]) for o in outputs if self.m.immutable(o["name"])]
                )
        if self.keys is not None:
            for name, keys in ((commit or {}).get("keys") or {}).items() if outcome == "succeeded" else ():
                self._cache_commit(name, task["partition"], keys)
            self.keys.ended(claim["attempt"])

    def _cache_commit(self, name: str, partition: str, keys: dict | None) -> None:
        """Keep the engine's cache warm with what a commit installed (§5)."""

        if not keys or not keys.get("files"):
            return
        index = self.m.indexes.get((name, partition))
        if index is not None:
            files = [FileInfo.from_json(f) for f in keys["files"]]
            self.keys.committed(index.prefix, index.path, files, self.m.event_counter)

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
            written = [o["name"] for o in declared.get("outputs", ())]
            behind = self._merges_behind([*written, f"@{task['asset']}"], task["partition"])
            cap = declared.get("concurrency")
            if holder is not None:
                held[task_id] = ["claim", holder]
            elif cap is not None and asset_used.get(task["asset"], 0) >= cap:
                held[task_id] = ["concurrency", task["asset"]]
            elif behind is not None:
                held[task_id] = ["merges", behind]
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

    def _merges_behind(self, names: list[str], partition: str) -> str | None:
        """The first of the key indexes `names` in `partition` that upkeep has
        let fall far behind — twice the span cap, which forced merges
        otherwise hold — or None. Every index writer waits until merges catch
        up (writer backpressure, docs/key-index-design.md § Limits): a task
        writing one is held (`merges`), its outputs and a per-key asset's
        failure index alike, and a source commit is refused, retryable."""

        cap = 2 * self.key_options.fan_in
        for name in names:
            index = self.m.indexes.get((name, partition))
            if index is not None and len(index.spans) >= cap:
                return name
        return None

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

    def _submit_cleanups(self, partitions=None) -> None:
        """A cleanup task for each of `partitions` (output, partition) with
        entries due — all of them with none, and every output life past its
        `cleanup_after`, read by no pin older than the deploy that ended it.
        None where one is queued or running already, or an entry is stuck. It
        runs as any task does — placed, retried a bounded number of times, in
        the runs and their history — with no asset of the project's."""

        submitted = {
            json.dumps(t.get("cleanup"), sort_keys=True)
            for run in self.m.runs.values()
            for t in run["tasks"].values()
            if t["asset"] == CLEANUP and t["status"] not in TERMINAL_TASK
        }
        due, declared = [], {o["name"] for info in self.manifest["assets"].values() for o in info["outputs"]}
        for output, partition in list(self.m.cleanups) if partitions is None else partitions:
            if output in declared and self._due_cleanups(output, partition, None):  # what a task could take
                due.append(({"output": output, "partition": partition}, f"{output}/{partition}", output))
        if partitions is None and self.m.retired:
            now, floor = self.clock(), self.m.pin_floor()
            for entry_id, entry in self.m.retired.items():
                if not entry.get("stuck") and entry["due"] <= now and floor >= entry["before"]:
                    due.append(({"life": entry_id}, entry_id, entry["output"]))
        for cleanup, partition, output in due:
            if json.dumps(cleanup, sort_keys=True) not in submitted:
                self._cleanup_task(cleanup, partition, output)

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
            "partitions": "latest",
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
        if not entries:
            return {**prepared, "skip": True}
        claim = self.m.claimed(attempt)
        if claim is not None:
            # What its spec hands it is read from now, not from `AttemptLaunched` on: an
            # operator's clearing meanwhile must not let collection take it.
            claim["cleanups"] = _delta_files(entries)
        home = self.m.homes.get(output, output)
        return {
            **prepared,
            "cleanup": {
                "asset": owner,
                "partition": partition,
                "outputs": {output: {"cleanup": entries, "home": home}},
            },
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
                    prepared = self._prepare(task, run, attempt, await self._observe(task, run, attempt))
            except (Retryable, NonRetryable, Conflict) as error:
                self._finish(
                    task,
                    claim,
                    "failed",
                    error=error,
                    retryable=not isinstance(error, NonRetryable) and getattr(error, "retryable", True),
                )
                return
            if prepared.get("skip"):
                commit = {**await self._observations(task, prepared, {}), **self._made(prepared)}
                more = bool(prepared.get("more"))
                commit["final"] = not more
                self._finish(task, claim, "skipped", commit=commit, more=more)
                return
            stage = await self._launch(task, run, attempt, prepared)
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

    def _prepare(
        self, task: dict, run: dict, attempt: str | None = None, observing: dict | None = None
    ) -> dict:
        """Pin heads at attempt start; plan Incremental inputs — a keyed one's
        batch as `_observe` planned it — and decide skip (§8).

        Each output the attempt may write is pinned with its commit number and,
        when keyed, its key index: the worker works out the delta against
        exactly that state (§6)."""

        asset = self.manifest["assets"][task["asset"]]
        partition = task["partition"]
        observing = observing or {}
        full = run["mode"] == "full"
        heads = {}
        for output in asset["outputs"]:
            head = heads[output["name"]] = self.m.heads.get((output["name"], partition))
            # A version bump is an asset change like any other: what the old version
            # built is rebuilt, the next attempt starting over.
            if head is not None and head.get("version") not in (None, asset["version"]):
                full = True
        if observing:  # what its inputs observed decides: a full run's first batch only
            full = any(o["full"] for o in observing.values())
        planner = self.planner()
        try:
            inputs = planner.inputs(task["asset"], partition)
        except planning.UpstreamOnly as error:
            raise Conflict(str(error)) from None
        # Pass 1: pin every non-Incremental input.
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
        definition = self._definition(task["asset"], run)
        # The whole and dep versions read, which the commit records (its context).
        context = self._context(planner, inputs)
        # Pass 2: Incremental plans, as `_observe` planned them.
        plans, all_empty, each_page = {}, True, None
        for input in incremental:
            param, upstream_partition = input.param, input.partition
            ref = self._pin_at(input.output, upstream_partition)
            if self._keyed(input):
                pin, plan, empty = self._observed_pin(input, ref, observing[param])
            else:
                pin, plan, empty = self._commits_pin(input, ref, observing[param])
            if input.spec.get("each") is not None:
                pin, plan, empty = self._each_plan(
                    task, asset, input.spec, ref, pin, plan, empty, observing[param]
                )
                each_page = pin["each"]
            pins[param] = pin
            plans[param] = plan
            all_empty = all_empty and empty
        claim = self.m.claimed(attempt) if attempt is not None else None
        if claim is not None:
            # What this attempt reads stays endpoints of its upstreams until it finishes (§6).
            claim["reads"] = reads(plans)
        more = any(not (p["final"] or p.get("done") or p.get("retry")) for p in plans.values() if p)
        skip = bool(incremental) and all_empty and not more and not full
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
                    "writes": self.manifest["stores"][output["store"]]["writes"],
                    "key": output.get("key"),
                    "incremental": output.get("incremental"),
                },
            }
            # Whether the write starts the content over: a first write (an output
            # reset by a move holds no head), or a full run.
            info["reset"] = reset or head is None
            info["home"] = self.m.homes.get(name, name)  # where its store keeps it (K25)
            if output.get("incremental"):
                info["commit_number"] = int((head or {}).get("commit_number", -1)) + 1
            if output.get("key") is not None:
                info["index"] = self.m.index(name, partition).slice().to_json()
                if (name, partition) in self.m.repairs:
                    info["repairs"] = self.m.repairs[(name, partition)]
                if output.get("dynamic_partitions") or name in self._dynamic_dims:
                    info["partitions"] = list((head or {}).get("partitions") or ())
            outputs[name] = info
        # The input versions its outputs will be built from, for the history (§7):
        # each pinned ref's generation (docs/versions.md §6).
        lineage = []
        for input in inputs:
            pin = pins.get(input.param) or {}
            refs = [pin["ref"]] if pin.get("ref") else list((pin.get("refs") or {}).values())
            for ref in refs:
                lineage.append([input.output, ref.get("partition") or "", ref.get("generation"), input.param])
        return {
            "version": asset["version"],
            "deploy_number": self.m.deploy_number,
            # The version of each whole or dep input read: a partition with no keyed
            # input whose inputs moved since is stale (a keyed one's layers carry it).
            "context": context,
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
            "definition": definition,  # what its inputs' observations are made under
            "outputs": outputs,
            # A per-key batch's failure delta, for cleaning up if it never commits.
            "failures": None
            if each_page is None
            else {
                "prefix": self.m.index(f"@{task['asset']}", partition).prefix,
                "commit_number": each_page["commit_number"],
            },
        }

    def _prefixes(self, inputs: dict, outputs: dict, task: dict) -> list[str]:
        """What an attempt reads, as reader pins name it: the index prefix of
        every output partition it reads or writes — a value's or an unkeyed incremental
        output's too, whose data its partition's garbage names — and of any index
        its pins name (a failed keys). Collection elsewhere waits for no
        attempt that reads none of it."""

        found = {self.m.index(name, task["partition"]).prefix for name in outputs}

        def walk(value) -> None:
            if isinstance(value, dict):
                if isinstance(value.get("prefix"), str):
                    found.add(value["prefix"])
                if isinstance(value.get("output"), str) and "handle" in value:  # a ref
                    found.add(self.m.index(value["output"], value.get("partition") or "").prefix)
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
                "failures",
                "definition",
                "cleanup",  # a cleanup task's entry (K25)
            )
        }
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
        store needs to name its objects (docs/lifecycle.md §9.8)."""

        record = self.manifest["outputs"].get(output) or {}
        store = self.manifest["stores"].get(record.get("store")) or {}
        if (
            record.get("key") is None
            or record.get("dynamic_partitions")
            or store.get("writes") != "immutable"
        ):
            return None
        return self.m.index(output, ref.get("partition") or "").slice().to_json()

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
        head that agrees with this partition's shared keys, keyed by the dimensions
        it collapses (§7) — every dimension, with `all_partitions` — chosen
        among the heads that exist, never by expanding the partition domain."""

        return {input.key(s): h["ref"] for s, h in planner.fan_in(input, materialized=True).items()}

    # -- per-key batches (docs/per-key-processing.md §5, §9) ------------------------------

    def _forced_at(self, record: dict) -> int:
        return max((record.get("forced") or {}).values(), default=0)

    def _has_retries(self, record: dict | None) -> bool:
        """Whether a partition's failure record has keys to retry now: a pass in
        progress, a key due, a failed key not yet tried under this deploy,
        or a forced request newer than the last pass completed. The
        bounds are conservative: a pass may find nothing, which makes them
        exact (§9)."""

        if not record:
            return False
        due, deploy_min = record.get("due"), record.get("deploy_min")
        return bool(
            record.get("retry")
            or (due is not None and due <= self.clock())
            or (deploy_min is not None and deploy_min < self.m.deploy_number)
            or self._forced_at(record) > int(record.get("done_forced") or 0)
        )

    def _each_plan(self, task, asset, input, ref, pin, plan, empty, o):
        """A per-key input's batch: its owed keys, or the keys its failed keys
        has due again (`_retry`, as `_observe` read them). When both are
        pending they alternate — neither starves, and there is no fraction to
        tune (§9). A `keys=` run and a full run's first batch take no
        retries; that batch starts the failed keys over too (K47). Either
        carries its keys' prior failure records: its worker reads no index."""

        record = self.m.partition(task["asset"], task["partition"]).get("failures") or {}
        failures = self.m.index(f"@{task['asset']}", task["partition"])
        changes = not empty
        retries = o.get("retry") is not None
        if changes and retries:
            kind = "retry" if record.get("last") == "changes" else "changes"
        else:
            kind = "retry" if retries else "changes"
        forced = dict(record.get("forced") or {})
        current = self._forced_at(record)
        retry = record.get("retry")
        if retry is not None and (retry["deploy"] != self.m.deploy_number or retry["forced_at"] != current):
            retry = None  # its predicate's inputs moved: the pass starts over (§9)
        each = {
            "kind": kind,
            "concurrency": input["each"]["concurrency"],
            "deploy": self.m.deploy_number,
            "forced": forced,
            "forced_at": current,
            "now": self.clock(),
            "retries": asset.get("retries", {}).get("n", 0),
            "failures": failures.slice().to_json(),
            "commit_number": int(record.get("commit_number", -1)) + 1,
            "pass_after": (retry or {}).get("after"),
        }
        if kind == "retry":
            if retry is None:
                retry = {
                    "pass": int(record.get("passes") or 0) + 1,
                    "deploy": self.m.deploy_number,
                    "forced_at": current,
                    "after": None,
                    "due_acc": None,
                    "deploy_acc": None,
                }
            each["pass_after"] = retry["after"]
            each["priors"], each["rest"] = o["retry"]["priors"], o["retry"]["rest"]
            # Each due key at its version now, observed outright: a point each.
            pin = {
                "ref": ref,
                "index": pin["index"],
                "batch": {"keys": o["retry"]["keys"], "retry": {"after": o["retry"]["after"]}},
                "each": each,
            }
            if input.get("patterns") is not None:
                pin["patterns"] = input["patterns"]  # a due key the input no longer takes goes
            plan = {
                **plan,
                "named": True,
                "retry": True,
                "keys": {},
                "classes": {},
                "each": {"kind": "retry", "pass": retry, "changes": changes},
            }
            return pin, plan, False
        batch = {"kind": "changes", "retries": retries, "pass": retry}
        each["priors"] = o["priors"]
        if plan["full"]:  # the failed keys start over: the commit replaces the failure index
            each["start_over"] = batch["start_over"] = True
        return {**pin, "each": each}, {**plan, "each": batch}, empty

    def _each_commit(self, task, plan: dict, result: dict) -> tuple[dict, bool]:
        """What a per-key batch commits to its failure record, and whether the
        task has more to do (§9): the outcome counts move by the batch's
        transitions; the bounds are lowered by the records it wrote; a retry
        batch advances its pass and folds the range it walked into the pass's
        accumulators, which become the exact bounds when the pass completes;
        a change batch folds what it wrote behind the pass's place."""

        batch = plan["each"]
        record = self.m.partition(task["asset"], task["partition"]).get("failures") or {}
        if batch.get("start_over"):  # the index starts over: nothing of the record before carries
            record = {"commit_number": record.get("commit_number", -1), "forced": record.get("forced") or {}}
        run = self.m.runs.get(task["run"]) or {}
        report = result.get("failures") or {}
        counts = dict(record.get("counts") or {})
        for name, delta in (report.get("counts") or {}).items():
            counts[name] = counts.get(name, 0) + int(delta)
        counts = {k: v for k, v in counts.items() if v}
        due = lower(record.get("due"), report.get("due"))
        deploy_min = lower(record.get("deploy_min"), report.get("deploy_min"))
        commit = {
            "keys": report.get("keys") or {"files": []},
            "commit_number": int(record.get("commit_number", -1)) + 1,
            "counts": counts,
            "last": batch["kind"],
            # The configuration the partition runs under, for the runs retries start (§9).
            "config": run.get("config") or {},
        }
        retry, more = batch.get("pass"), False
        delivered = (result.get("delivered") or {}).get(plan.get("param") or "", {})
        # A forced request newer than the pass in progress, or than the last one done,
        # is owed a pass: this run takes it rather than waiting for unrelated activity.
        forced_after = self._forced_at(record) > int(
            (retry or {}).get("forced_at", record.get("done_forced") or 0)
        )
        if batch["kind"] == "retry":
            walked = report.get("range") or {}
            retry = {
                **retry,
                "due_acc": lower(retry.get("due_acc"), walked.get("due")),
                "deploy_acc": lower(retry.get("deploy_acc"), walked.get("deploy_min")),
            }
            after = delivered.get("after")
            if after is None:  # the pass is complete: its accumulators are the exact bounds
                due, deploy_min = retry["due_acc"], retry["deploy_acc"]
                commit.update({"passes": retry["pass"], "done_forced": retry["forced_at"], "retry": None})
                more = bool(batch.get("changes")) or forced_after  # owed keys are never dropped
            else:
                commit["retry"] = {**retry, "after": after}
                more = True
        else:
            if retry is not None:
                fold = report.get("fold") or {}
                commit["retry"] = {
                    **retry,
                    "due_acc": lower(retry.get("due_acc"), fold.get("due")),
                    "deploy_acc": lower(retry.get("deploy_acc"), fold.get("deploy_min")),
                }
            # Keys this batch left due at once are retried in the same run.
            more = (
                bool(batch.get("retries"))
                or (report.get("due") is not None and report["due"] <= self.clock())
                or forced_after
            )
        commit.update({"due": due, "deploy_min": deploy_min})
        if batch.get("start_over"):
            commit["start_over"] = True
        return commit, more

    def _due_cleanups(self, output: str, partition: str, attempt: str | None) -> list[dict]:
        """The cleanup of an immutable output's partition that no reader can
        still need: every entry let go of before the oldest reader pin but this
        attempt's own (a cleanup task's, which reads none of it), and past its
        `after`. At most `CLEANUPS` of them, for one cleanup task (§9.8)."""

        entries = self.m.cleanups.get((output, partition))
        if not entries:
            return []
        floor = self.m.pin_floor(but=attempt, path=self.m.index(output, partition).prefix)
        now = self.clock()
        return [e for e in entries if e["n"] <= floor and e.get("after", 0) <= now and not e.get("stuck")][
            :CLEANUPS
        ]

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
        (docs/observed-set.md, "The commit check"). A rename keeps everything."""

        return declaration(self.manifest, asset, self.m.homes)

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

        from solera.sdk import UNSET

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
        # Output heads must be unchanged since the claim, and no output it writes or
        # reads incrementally reset since it launched (removed, or moved to another
        # store): what it built belongs to the output's earlier life.
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
        observations = await self._observations(task, prepared, result)
        if self.m.claimed(attempt) is not claim:
            raise LostOwnership(attempt)
        outputs = current_names(prepared, result.get("outputs") or {})
        # Settled under the contract it was launched with, not today's manifest.
        declared = {name: info["contract"] for name, info in (prepared.get("outputs") or {}).items()}
        more = bool(prepared.get("more"))
        failures = None
        for param, plan in (prepared.get("plans") or {}).items():
            if plan is None:
                continue
            if "each" in plan:
                failures, each_more = self._each_commit(task, {**plan, "param": param}, result)
                more = more or each_more
            more = more or not (plan["final"] or plan.get("done") or plan.get("retry"))  # the walk goes on
        heads, keys = {}, {}
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
            head = {"ref": ref, "asset": task["asset"], "version": prepared["version"]}
            if decl.get("key") is not None:
                delta = entry.get("keys")
                if delta is None and not entry.get("unchanged"):
                    raise Conflict(f"keyed output {name}: the result carries no key delta", retryable=False)
                head["commit_number"] = int((before or {}).get("commit_number", -1))
                if delta is not None:
                    if delta["files"]:
                        head["commit_number"] = int(info["commit_number"])
                    keys[name] = {**delta, "commit_number": head["commit_number"]}
                if "partitions" in info:
                    head["partitions"] = entry.get("partitions", info["partitions"])
            elif decl.get("incremental"):
                if info["reset"]:  # starts over at its commit, whatever its content
                    head["commit_number"] = head["base"] = int(info["commit_number"])
                elif before["ref"].get("generation") == ref.get("generation"):  # appended nothing
                    head["commit_number"], head["base"] = (
                        before.get("commit_number", -1),
                        before.get("base", 0),
                    )
                else:
                    head["commit_number"], head["base"] = int(info["commit_number"]), before.get("base", 0)
            heads[name] = head
        for name in set(declared) - set(outputs):
            # A per-key batch whose keys all failed writes nothing, and makes no head yet;
            # nor does a batch whose keys the input's patterns all left out.
            if prepared["outputs"][name]["head"] is None and failures is None and not result.get("skipped"):
                raise Conflict(f"omitted output {name} has no head to keep (§2)", retryable=False)
        commit = {"heads": heads, **observations, **self._made(prepared)}
        commit["final"] = not more and outcome == "succeeded"  # a canceled run has no final commit
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
        repaired = sorted(
            name
            for name, entry in outputs.items()
            if ((prepared.get("outputs") or {}).get(name) or {}).get("repairs") and not entry.get("unchanged")
        )
        if repaired:
            commit["repaired"] = repaired
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
            write=result.get("write"),
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
        nothing. Whoever does not record it drops it (`_drop_prepared`).
        Its generation is the event counter it is prepared at, as an
        attempt's is its claim's: larger than any commit before it."""

        source = self.manifest["sources"].get(name)
        if source is None:
            raise KeyError(name)
        if self._merges_behind([name], "") is not None:
            raise Conflict(
                f"source {name!r}: its key index is far behind on merges; retry once upkeep catches up"
            )
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
            "asset": None,
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

            def versions(given):
                if isinstance(given, dict):
                    return {str(k): (key_bytes(str(v)) if v is not None else None) for k, v in given.items()}
                return {str(k): listed for k in given or []}

            if keys is not None:
                new, removes, replace = versions(keys), [], True
            else:
                new = versions(upsert)
                removes, replace = [str(k) for k in remove or [] if str(k) not in new], False
            commit_number = int((head or {}).get("commit_number", -1)) + 1
            attempt = ulid(self.clock())
            sorted_run = SortedEntries.of(
                [key_bytes(k) for k in new], list(new.values()), [key_bytes(k) for k in removes]
            )
            with self.m.reading(self.m.index(name, "").prefix):  # outlives merges meanwhile
                pinned = self.m.index(name, "").slice()
                index = KeyIndex(self._key_io(), None, pinned, self.key_options)
                files = await self._resolve_source(
                    index, pinned, sorted_run, replace, commit_number, attempt, generation
                )
                if files is not None:
                    files, changed = files
                elif replace:
                    files, changed = await index.replace(
                        Rows.pairs([(key_bytes(k), r) for k, r in new.items()]),
                        commit_number,
                        attempt,
                        collect=2 * SOURCE_KEYS_RECORDED,
                        generation=generation,
                    )
                else:
                    files, changed = await index.resolve(
                        sorted_run,
                        commit_number=commit_number,
                        attempt=attempt,
                        generation=generation,
                        collect=2 * SOURCE_KEYS_RECORDED,
                    )
            if not files.files:
                return None, head["ref"] if head is not None else source["head"]
            record["commit_number"] = commit_number
            if listed is not None:
                before = set((head or {}).get("partitions") or ())
                record["partitions"] = sorted(set(new) if replace else (before - set(removes)) | set(new))
            event["keys"] = {**files.to_json(), "commit_number": commit_number}
            run["commit_number"] = commit_number
            counts = (sum(f.entries for f in files.files) - files.removed, files.removed)
            for field, keys, count in zip(
                ("upserted", "deleted"), changed or (None, None), counts, strict=True
            ):
                shown = keys is not None and len(keys) <= SOURCE_KEYS_RECORDED
                run[field] = [key_str(k) for k in keys] if shown else count
        meta = dict(ref.get("meta") or {})
        meta["source"] = True
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
                    self._key_io(), None, self.m.index(event["source"], "").slice(), self.key_options
                )
                paths += [index.path(f["name"]) for f in event["keys"]["files"]]
        if paths:
            await self.state.delete_objects(paths)

    async def _resolve_source(self, index, pinned, run, replace, commit_number, attempt, generation):
        """A small source commit through the warm resolver, in process
        (docs/resolved-commits.md §4): its files and changed keys, or None when
        the cache cannot answer and the commit resolves cold."""

        from solera.keys.resolver import Limits

        lim = Limits()
        size = len(run) + (pinned.count if replace else 0)
        if self.keys is None or size > (lim.max_entries if replace else lim.max_keys):
            return None
        name = f"{commit_number:012d}-{attempt}.0000"
        answer, delta = await self.keys.direct(
            pinned,
            "replace" if replace else "patch",
            run,
            generation,
            commit_number,
            index.path(name),
            self.m.event_counter,
        )
        if answer["result"] == "empty":
            return DeltaFiles([], 0, 0, generation), ([], [])
        if answer["result"] != "delta":
            return None
        await index.io.write(index.path(name), delta)
        files = DeltaFiles([FileInfo.describe(name, delta)], answer["added"], answer["removed"], generation)
        return files, delta_keys(delta)

    # -- key index upkeep (§6) --------------------------------------------------------

    def _key_io(self) -> ObjectIO:
        if self._io is None:
            self._io = ObjectIO(self.state.objects)
        return self._io

    async def list_keys(self, output: str, partition: str = "", *, after=None, offset=0, limit=1000) -> dict:
        """One page of an output's live keys, read from its key index."""

        if (output, partition) not in self.m.heads:
            raise KeyError(f"{output}/{partition}")
        state = self.m.indexes.get((output, partition))
        if state is None:
            return {"total": 0, "keys": {}, "next": None}
        start = key_bytes(after) if after is not None else None
        with self.m.reading(state.prefix):  # its files outlive merges until the page is read
            index = KeyIndex(self._key_io(), None, state.slice(), self.key_options)
            keys, generations, _, nxt = await index.page(start, offset + limit)
        return {
            "total": state.count,
            # Each key's version: the generation that last wrote it (docs/versions.md).
            "keys": {key_str(k): g for k, g in list(zip(keys, generations, strict=True))[offset:]},
            "next": key_str(nxt) if nxt is not None else None,
        }

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

    async def _retry_tick(self):
        """The retry clock (docs/per-key-processing.md §9): an automated Each
        asset whose failed keys has keys due again runs for that partition, even
        when nothing upstream changed. An asset run by hand picks them up on
        its next run. A partition whose inputs have no head waits for them: its
        run could not plan, and would be submitted again every tick."""

        automated = {t for auto in self.m.automations.values() if auto["enabled"] for t in auto["targets"]}
        for (asset, partition), state in list(self.m.partitions.items()):
            if asset not in automated or asset not in self.manifest["assets"] or "failures" not in state:
                continue
            if self._partition_active(asset, partition) or self.m.is_pending(asset, partition):
                continue
            if self._has_retries(state["failures"]):
                await self.submit_retries(asset, [partition], "retry clock", skip_missing_inputs=True)

    async def _repair_tick(self):
        """The repair clock (docs/lifecycle.md §9.6): a partition a dead writer left
        owing a repair, once nothing else will run it (its task out of retries,
        or canceled), runs again on its own — its consumers' reads wait on the
        repair. At most `REPAIR_RUNS` runs, `REPAIR_SPACING` apart and doubling;
        then it waits, stuck and visible (`repairs_view`), for a run of a
        user's or a trigger's."""

        owner = {o["name"]: a for a, info in self.manifest["assets"].items() for o in info["outputs"]}
        now = self.clock()
        for output, partition in list(self.m.repairs):
            asset = owner.get(output)
            if (
                asset is None
                or self._partition_active(asset, partition)
                or self.m.is_pending(asset, partition)
            ):
                continue
            runs, at = self.m.repair_runs(output, partition)
            if runs >= REPAIR_RUNS or (at is not None and now < at + REPAIR_SPACING * 2 ** (runs - 1)):
                continue
            self.state.record(
                {"type": "RepairRunSubmitted", "output": output, "partition": partition, "at": now}
            )
            await self.submit(
                [asset], partitions=[partition], skip_active=True, skip_missing_inputs=True, by="repair clock"
            )

    async def submit_retries(
        self, asset: str, partitions, by: str | None, skip_missing_inputs: bool = False
    ) -> list[dict]:
        """Runs for a per-key asset's partitions that have keys to retry, each under
        the configuration its partition last ran with (kept on its failure record):
        a retry under another configuration would read other inputs, and its
        new definition would redeliver every key (§9). Partitions already active
        are left to the run they are in."""

        by_config: dict[str, list[str]] = {}
        for partition in partitions:
            config = (self.m.partition(asset, partition).get("failures") or {}).get("config") or {}
            by_config.setdefault(json.dumps(config, sort_keys=True), []).append(partition)
        runs = []
        for config, group in sorted(by_config.items()):
            run = await self.submit(
                [asset],
                partitions=sorted(group),
                config=json.loads(config),
                skip_active=True,
                skip_missing_inputs=skip_missing_inputs,
                by=by,
            )
            if run is not None:
                runs.append(run)
        return runs

    def retry_keys(self, asset: str, classes, partition: str | None = None, by: str | None = None) -> dict:
        """`solera keys retry`: a forced request for a per-key asset's failing
        keys of `classes` (`failed`, `rejected`, `canceled`, `retrying`,
        `timed_out`, or `all`), every partition or one. Its event
        counter is its identity; a retry pass takes each such key once (§9)."""

        from solera.failed_keys import NAMES

        if not any(
            e.get("each") for e in (self.manifest["assets"].get(asset) or {}).get("inputs", {}).values()
        ):
            raise ValueError(f"{asset} has no per-key input: it keeps no failing keys")
        classes = sorted(set(NAMES.values()) if "all" in classes else set(classes))
        unknown = set(classes) - set(NAMES.values())
        if unknown or not classes:
            raise ValueError(f"unknown key classes: {sorted(unknown) or classes}")
        self.state.record(
            {
                "type": "KeysRetryRequested",
                "asset": asset,
                "partition": partition,
                "classes": classes,
                "by": by,
            }
        )
        partitions = sorted(
            s for s, r in self.m.partitions.of(asset).items() if "failures" in r and partition in (None, s)
        )
        return {"asset": asset, "classes": classes, "partitions": partitions}

    def _automation_run(self, auto, partitions, targets=None) -> dict | None:
        """The run a firing becomes, in the run's own vocabulary (§9); `None`
        if every partition is in flight or waits for its inputs."""

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

    def head_view(self, head: dict) -> dict:
        """A head as the API shows it: with its commit, and whether it is of a
        complete pass — its partition's progress says (§7)."""

        view = dict(head)
        view["commit"] = f"{head['run']}/{head['attempt']}" if head.get("attempt") else None
        owner = head.get("asset")
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
            o["name"]: [(s, self.head_view(h)) for s, h in self.m.heads_of(o["name"])]
            for o in info["outputs"]
        }
        observed = {
            param: self._observed_at(rec)
            for param, rec in (self.m.partition(asset, partition).get("observed") or {}).items()
        }
        dims = self.planner().dims(asset)
        return {
            "asset": info,
            "heads": heads,
            "cursor": self.m.partition(asset, partition).get("cursor"),
            "observed": observed,  # each input's oldest commit observed
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
                o["name"]: {s: self.head_view(h) for s, h in self.m.heads_of(o["name"])}
                for o in info["outputs"]
            }
            assets.append({**info, "name": name, "heads": heads})
        return assets
