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
import re
import time

from solera.ids import ulid
from solera.keys.index import IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO

from . import history

log = logging.getLogger(__name__)

GATES = "control/gates/"  # per day, the gates of runs deleted that day (docs/lifecycle.md §2.4)
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
        gate_days: float = 30.0,
        interval: float = 1.0,
        keys=None,
    ):
        self.state, self.history, self.manifest, self.clock = state, history, manifest, clock
        self.keys = keys  # the engine's key cache: compaction outputs go into it as written
        self.key_options = key_options or Options()
        self.recount_interval, self.concurrency = recount_interval, concurrency
        self.retention_interval, self.interval = retention_interval, interval
        self.jobs: dict[tuple, asyncio.Task] = {}  # compactions and recounts running
        self.last_error: str | None = None
        self._recounted: dict[tuple, float] = {}  # when each index was last recounted
        self._checked: dict[tuple, IndexState] = {}  # the state last found needing nothing
        self._swept = -math.inf
        self.gate_days = gate_days
        self._alive = -math.inf
        self._task: asyncio.Task | None = None
        self.retiring = asyncio.Lock()  # one retirement at a time
        self._purging = asyncio.Lock()

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
        await self.purge()
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

        options, objects, service = self.key_options, self.state.objects, self.keys
        # An immutable store's compaction lists what its merge dropped: those
        # entries name objects that collection then discards (docs/lifecycle.md §9.8).
        garbage = self.m.immutable(key[0])

        def work():
            async def go(local):
                keys = KeyIndex(ObjectIO(objects, local=local), None, index, options)
                if service is not None:
                    keys.on_write = lambda path, f, data: service.installed(index.prefix, f, path, data)
                return await (keys.recount() if recount else keys.compact(garbage=garbage))

            # One warm copy serves every engine reader: an index the engine's cache
            # holds is read from its local files, the store otherwise.
            pin = service.pinned(index) if service is not None else None
            if pin is None:
                return asyncio.run(go(None))
            try:
                return asyncio.run(go(pin.handles))
            except ValueError as e:
                bad = re.match(r"local file (\S+): ", str(e))
                if bad is None:
                    raise
                service.corrupt(bad.group(1))  # dropped, fetched again by a fill; this run reads the store
            finally:
                service.unpin(pin)
            return asyncio.run(go(None))

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
            # Exact for the state it pinned; commits since add their `added - removed`.
            self._recounted[key] = self.clock()
            if current is not None and current.prefix == index.prefix:  # still the same index
                self.state.record(
                    {
                        "type": "IndexRecounted",
                        "output": output,
                        "scope": scope,
                        "live": result,
                        "pinned_count": index.count,
                        "pinned_inexact": index.inexact,
                    }
                )
            return
        if result is None:
            return
        added, removed, dropped = result
        if current is None or not set(removed) <= {f.name for f in current.files}:
            created = [f.name for f in added if f.name not in removed]
            await self._delete(
                [index.path(n) for n in created] + [f"{index.prefix}{g.name}.kg" for g in dropped]
            )
            return
        event = {
            "type": "IndexCompacted",
            "output": output,
            "scope": scope,
            "added": [f.to_json() for f in added],
            "removed": removed,
            "at": self.clock(),
        }
        if dropped:
            event["garbage"] = [g.to_json() for g in dropped]
        self.state.record(event)
        if self.keys is not None:  # published: no new snapshot reads its inputs
            # ...but a delta still in the log stays for change windows, until collected.
            kept = {f.name for f in added} | (self.m.indexes.get(key) or current).referenced()
            self.keys.retired([index.path(n) for n in removed if n not in kept])

    # -- garbage ---------------------------------------------------------------------

    async def collect(self) -> None:
        """Delete the files nothing references, once no reader pinned before
        they were let go of — an attempt, a paged delta window, a sensor
        tick — still reads. Both are positions in the model's event order,
        never wall clocks: two engines' clocks may disagree, the order they
        replay may not."""

        if not self.m.garbage:
            return
        oldest, read = self.m.pin_floor(), self.m.discard_reads()  # pending discards still read them
        if self.keys is not None:  # and the engine's own fills and fetches of index files
            oldest = min(oldest, self.keys.floor())
        due = [path for path, n in self.m.garbage if n <= oldest and path not in read]
        if not due:
            return
        await self.state.durable()  # a replay must never reference them again
        await self._delete(due)
        self.history.lake.evict(due)  # their cached copies live exactly as long
        self.state.record({"type": "GarbageDeleted", "paths": due})
        if self.keys is not None:  # what the engine's cache held of them goes too
            self.keys.retired(due)

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
        await self.expire_gates()
        self.expire_ticks()
        policies = {name: self.m.policy(name) for name in self.manifest["assets"]}
        keeps = {name: int(p["runs"]) for name, p in policies.items() if p and p.get("runs")}
        nth = await self.history.nth_newest(keeps)
        horizons = {name: self._horizon(p, nth.get(name)) for name, p in policies.items()}
        default = self._horizon(self.m.policy(None), None)
        finite = {name: h for name, h in horizons.items() if h is not None}
        await self.delete_runs(await self.history.expired(finite, default))

    async def delete_runs(self, runs: list[tuple[str, str | None]]) -> None:
        """Delete finished runs, `(id, status)`. Retirement comes first and
        is for good: `RunsDeleted` drops their history and is made durable
        before any of their files go, so a replaced engine deletes nothing."""

        async with self.retiring:
            runs = [(r, s) for r, s in runs if r not in self.m.runs and r not in self.m.retired]
            # Every run's directory: a skipped run may have launched (an attempt
            # whose patterns took no key), and listing an empty one costs a LIST.
            self.history.delete([r for r, _ in runs], [r for r, _ in runs])
        await self.purge()

    async def purge(self) -> None:
        """Delete the directories of retired runs: their attempt files and logs."""

        async with self._purging:
            if not self.m.retired:
                return
            await self.state.durable()
            for run_id in list(self.m.retired):
                gates = await self.state.delete_run(run_id)
                if gates:  # tombstones outlive the run (docs/lifecycle.md §2.4): note where
                    day = time.strftime("%Y-%m-%d", time.gmtime(self.clock()))
                    note = f"{GATES}{day}/{ulid(self.clock())}.json"
                    await self.state.create_object(note, json.dumps(gates).encode())
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

    async def expire_gates(self) -> None:
        """Delete the gates retired runs left, `gate_days` after the day
        their run was deleted."""

        horizon = time.strftime("%Y-%m-%d", time.gmtime(self.clock() - self.gate_days * 86400))
        for note in await self.state.list_objects(GATES):
            if note[len(GATES) :].split("/", 1)[0] >= horizon:
                continue
            gates = json.loads(await self.state.get_object(note) or b"[]")
            await self._delete(gates)
            await self._delete([note])
