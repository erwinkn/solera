"""Storage upkeep (docs/object-store-state.md §6, §7, §11): background work
that keeps the object store tidy. The engine never waits on it.

- key indexes: delta logs are truncated to what consumers still read, small
  files compacted, and inexact counts recounted, on worker threads;
- garbage: files nothing references any more are deleted once the events
  that let go of them are durable, and no attempt that may read them runs;
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

from solera.keys.index import IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO, key_cache

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
        key_options: Options | None = None,
        recount_interval: float = 3600.0,
        concurrency: int = 2,
        retention_interval: float = 60.0,
        interval: float = 1.0,
    ):
        self.state, self.history, self.manifest, self.clock = state, history, manifest, clock
        self.key_options = key_options or Options()
        self.recount_interval, self.concurrency = recount_interval, concurrency
        self.retention_interval, self.interval = retention_interval, interval
        self.jobs: dict[tuple, asyncio.Task] = {}  # compactions and recounts running
        self.last_error: str | None = None
        self._recounted: dict[tuple, float] = {}  # when each index was last recounted
        self._checked: dict[tuple, IndexState] = {}  # the state last found needing nothing
        self._swept = -math.inf
        self._alive = -math.inf
        self._task: asyncio.Task | None = None

    @property
    def m(self):
        return self.state.model

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        jobs = [j for j in (self._task, *self.jobs.values()) if j is not None]
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        self._task = None
        with contextlib.suppress(Exception):
            await self.say_alive(force=True)

    async def _run(self) -> None:
        while True:
            try:
                await self.tick()
                self.last_error = None
            except Exception as error:
                self.last_error = f"{type(error).__name__}: {error}"
                log.exception("storage upkeep failed")
            await asyncio.sleep(self.interval)

    async def tick(self) -> None:
        await self.say_alive()
        self.truncate()
        self.maintain()
        await self.collect()
        await self.sweep()

    async def say_alive(self, force=False) -> None:
        now = self.clock()
        if self.m.runs and (force or now - self._alive >= ALIVE_SECONDS):
            self._alive = now
            await self.state.put_object(ALIVE, json.dumps({"at": now}).encode())

    # -- key indexes (§6) --------------------------------------------------------------

    def truncate(self) -> None:
        """Drop the delta log batches no consumer's watermark and no attempt
        in progress still reads."""

        needed: dict[tuple, int] = {}
        for wm in self.m.watermarks.values():
            if "up" in wm:
                key = (wm["output"], wm["up"])
                needed[key] = min(needed.get(key, math.inf), int(wm["batch"]))
        for claim in self.m.claims.values():
            for output, up, first in claim.get("reads") or ():
                needed[(output, up)] = min(needed.get((output, up), math.inf), int(first))
        truncations = []
        for (output, scope), index in self.m.indexes.items():
            if not index.log:
                continue
            below = needed.get((output, scope), index.log[-1][0] + 1)
            if index.log[0][0] < below:
                truncations.append(
                    {
                        "type": "IndexTruncated",
                        "output": output,
                        "scope": scope,
                        "below": below,
                        "at": self.clock(),
                    }
                )
        if truncations:
            self.state.record(*truncations)

    def maintain(self) -> None:
        """Start compactions and recounts, `concurrency` at a time."""

        now = self.clock()
        for key, index in list(self.m.indexes.items()):
            if len(self.jobs) >= self.concurrency:
                break
            if key in self.jobs or self._checked.get(key) is index:
                continue
            plan = KeyIndex(None, None, index, self.key_options).plan_compaction()
            if plan is not None:
                self._start(key, index, recount=False)
            elif not index.count_exact:
                if now - self._recounted.get(key, -math.inf) >= self.recount_interval:
                    self._start(key, index, recount=True)
            else:
                self._checked[key] = index

    def _start(self, key: tuple, index: IndexState, *, recount: bool) -> None:
        job = asyncio.create_task(self._maintenance(key, index, recount))
        self.jobs[key] = job
        job.add_done_callback(lambda _t: self.jobs.pop(key, None))

    async def _maintenance(self, key: tuple, index: IndexState, recount: bool) -> None:
        """One compaction or recount, run on a worker thread with its own event
        loop so merging never blocks the engine."""

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
                self.state.record(
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
            await self._delete([index.path(n) for n in created])
            return
        self.state.record(
            {
                "type": "IndexCompacted",
                "output": output,
                "scope": scope,
                "added": [f.to_json() for f in added],
                "removed": removed,
                "at": self.clock(),
            }
        )

    # -- garbage ---------------------------------------------------------------------

    async def collect(self) -> None:
        """Delete the files nothing references, once no attempt that started
        before they were let go of is still running."""

        if not self.m.garbage:
            return
        oldest = min((c["started_at"] for c in self.m.claims.values()), default=math.inf)
        due = [path for path, at in self.m.garbage if at < oldest]
        if not due:
            return
        await self.state.durable()  # a replay must never reference them again
        await self._delete(due)
        self.state.record({"type": "GarbageDeleted", "paths": due})

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
        await self.delete_runs(doomed)

    async def delete_runs(self, runs: list[tuple[str, str | None]]) -> None:
        """Delete finished runs, `(id, status)`: their attempt files and logs,
        then their history."""

        for run_id, status in runs:
            if status != "skipped":  # a skipped run launched nothing
                await self.state.delete_run(run_id)
        self.history.delete([run_id for run_id, _ in runs])
