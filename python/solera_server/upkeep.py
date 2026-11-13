"""Storage upkeep (docs/object-store-state.md §6, §7, §11): background work
that keeps the object store tidy. The engine never waits on it.

- garbage: files nothing references any more (the layers a merge replaced,
  `tracking.merge`) are deleted once the events that let go of them are
  durable, and no attempt that may read them runs;
- retention: finished runs every asset they ran has let go of are deleted;
- liveness: while runs are in progress, a note every `ALIVE_SECONDS` that
  the engine is up, so the next one knows when it went down.

The history's lake flushes and merges on its own (lake.py).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math

from solera.tasks import Tasks

from . import history
from .model import CLEANUP

log = logging.getLogger(__name__)

ALIVE = "engine/alive.json"  # when the engine last said it was up, while runs were in progress
ALIVE_SECONDS = 30.0


class Upkeep:
    def __init__(
        self,
        state,
        history,
        manifest: dict,
        *,
        clock,
        concurrency: int = 2,
        retention_interval: float = 60.0,
        interval: float = 1.0,
        failing: dict[str, str] | None = None,
    ):
        self.state, self.history, self.manifest, self.clock = state, history, manifest, clock
        self.concurrency = concurrency
        self.retention_interval, self.interval = retention_interval, interval
        self.tasks = Tasks("upkeep")  # its tick
        self.jobs = Tasks("upkeep jobs")  # the merge running (`tracking.merge`)
        self.failing = {} if failing is None else failing  # what fails now, by name: the engine's
        self._swept = -math.inf
        self._alive = -math.inf
        self.retiring = asyncio.Lock()  # one retirement at a time
        self._purging = asyncio.Lock()

    @property
    def m(self):
        return self.state.model

    def start(self) -> None:
        self.tasks.every("upkeep", self.interval, self.tick, failing=self.failing)

    async def stop(self) -> None:
        await self.tasks.close()
        await self.jobs.close()
        with contextlib.suppress(Exception):
            await self.say_alive(force=True)

    async def tick(self) -> None:
        await self.say_alive()
        await self.collect()
        await self.purge()
        await self.sweep()

    async def say_alive(self, force=False) -> None:
        now = self.clock()
        if self.m.runs and (force or now - self._alive >= ALIVE_SECONDS):
            self._alive = now
            await self.state.put_object(ALIVE, json.dumps({"at": now}).encode())

    # -- garbage ---------------------------------------------------------------------

    async def collect(self) -> None:
        """Delete the files nothing references, once no reader pinned before
        they were let go of — an attempt, a delta pass over several batches, a sensor
        tick — still reads. Both are event counters of the model's order,
        never wall clocks: two engines' clocks may disagree, the order they
        replay may not."""

        if not self.m.garbage:
            return
        floors, read = self.m.floors(), self.m.cleanup_reads()  # pending cleanups still read them
        # The engine's own fills and fetches of index files hold back index files only.
        cache = math.inf

        def due_now(path: str, n: int) -> bool:
            if path in read or (path.startswith("keys/") and n > cache):
                return False
            return n <= self.m.pin_floor(path=path, floors=floors)

        due = [path for path, n in self.m.garbage if due_now(path, n)]
        if not due:
            return
        await self.state.durable()  # a replay must never reference them again
        await self._delete(due)
        self.history.lake.evict(due)  # their cached copies live exactly as long
        self.state.record({"type": "FilesCleanedUp", "paths": due})

    async def _delete(self, paths: list[str]) -> None:
        from obstore.exceptions import NotFoundError

        try:
            await self.state.delete_objects(paths)
        except (NotFoundError, FileNotFoundError):
            for path in paths:
                with contextlib.suppress(NotFoundError, FileNotFoundError):
                    await self.state.delete_objects([path])

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

    async def sweep(self) -> None:
        """Every `retention_interval`, delete finished runs every asset they
        ran has let go of: a run is kept while any of its assets keeps it,
        or keeps everything."""

        now = self.clock()
        if now - self._swept < self.retention_interval:
            return
        self._swept = now
        self.expire_ticks()
        policies = {name: self.m.policy(name) for name in self.manifest["assets"]}
        keeps = {name: int(p["runs"]) for name, p in policies.items() if p and p.get("runs")}
        nth = await self.history.nth_newest(keeps)
        horizons = {name: self._horizon(p, nth.get(name)) for name, p in policies.items()}
        default = self._horizon(self.m.policy(None), None)
        finite = {name: h for name, h in horizons.items() if h is not None}
        if default is not None:  # the engine's cleanup runs: no asset of the project's, so its default
            finite[CLEANUP] = default
        await self.delete_runs(await self.history.expired(finite, default))

    async def delete_runs(self, runs: list[tuple[str, str | None]]) -> None:
        """Delete finished runs, `(id, status)`. Retirement comes first and
        is for good: `RunsDeleted` drops their history and is made durable
        before any of their files go, so a replaced engine deletes nothing."""

        async with self.retiring:
            runs = [(r, s) for r, s in runs if r not in self.m.runs and r not in self.m.deleted]
            # Every run's directory: a skipped run may have launched (an attempt
            # whose patterns took no key), and listing an empty one costs a LIST.
            self.history.delete([r for r, _ in runs], [r for r, _ in runs])
        await self.purge()

    async def purge(self) -> None:
        """Delete the directories of retired runs: their attempt files and logs."""

        async with self._purging:
            if not self.m.deleted:
                return
            await self.state.durable()
            for run_id in list(self.m.deleted):
                await self.state.delete_run(run_id)
                self.state.record({"type": "RunsPurged", "runs": [run_id]})

    def expire_ticks(self) -> None:
        """Drop the `ticks` files whose newest row is over a day old
        (docs/lifecycle.md §11.5)."""

        if self.history.lake.job is not None:  # a merge may be reading them
            return
        horizon = self.clock() - history.TICKS_KEPT
        old = [
            f["path"]
            for f in self.m.history.files.get("ticks", ())
            if f["at"][1] is not None and f["at"][1] < horizon
        ]
        if old:
            self.state.record({"type": "HistoryCompacted", "changes": [{"table": "ticks", "removed": old}]})
