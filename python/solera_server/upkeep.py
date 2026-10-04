"""Storage upkeep (docs/object-store-state.md §6, §7, §11): background work
that keeps the object store tidy. The engine never waits on it.

- key indexes: adjacent spans merged, as the merge policy plans them
  against the commits readers need (docs/key-index-design.md), on worker
  threads;
- garbage: files nothing references any more are deleted once the events
  that let go of them are durable, and no attempt that may read them runs;
  merge outputs no state ever referenced (their merge failed, or its engine
  stopped before publishing) are found by listing, every `ORPHAN_SECONDS`;
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

from solera.keys import LocalError
from solera.keys.index import IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.tasks import Tasks

from . import history

log = logging.getLogger(__name__)

ALIVE = "engine/alive.json"  # when the engine last said it was up, while runs were in progress
ALIVE_SECONDS = 30.0
MERGE_ATTEMPTS = 3  # attempts per input set before merging an index stops, alarmed (the write bound's R)
ORPHAN_SECONDS = 600.0


class Upkeep:
    def __init__(
        self,
        state,
        history,
        manifest: dict,
        *,
        clock,
        key_options: Options | None = None,
        concurrency: int = 2,
        retention_interval: float = 60.0,
        interval: float = 1.0,
        keys=None,
        failing: dict[str, str] | None = None,
    ):
        self.state, self.history, self.manifest, self.clock = state, history, manifest, clock
        self.keys = keys  # the engine's key cache: merge outputs go into it as written
        self.key_options = key_options or Options()
        self.concurrency = concurrency
        self.retention_interval, self.interval = retention_interval, interval
        self.tasks = Tasks("upkeep")  # its tick
        self.jobs = Tasks("upkeep jobs")  # merges running, by (index key, lane)
        self.failing = {} if failing is None else failing  # what fails now, by name: the engine's
        self._checked: dict[tuple, IndexState] = {}  # the state last found needing nothing
        self._attempts: dict[tuple, int] = {}  # merge attempts, by index key and input spans
        self.stopped: set[tuple] = set()  # indexes whose merges failed `MERGE_ATTEMPTS` times
        self._busy: dict[tuple, frozenset] = {}  # the input spans of each merge running, by (key, lane)
        self._swept = -math.inf
        self._alive = -math.inf
        self._orphans_at = -math.inf
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
        self.maintain()
        await self.collect()
        await self.collect_orphans()
        await self.purge()
        await self.sweep()

    async def say_alive(self, force=False) -> None:
        now = self.clock()
        if self.m.runs and (force or now - self._alive >= ALIVE_SECONDS):
            self._alive = now
            await self.state.put_object(ALIVE, json.dumps({"at": now}).encode())

    # -- key indexes (§6) --------------------------------------------------------------

    def maintain(self) -> None:
        """Start span merges, `concurrency` at a time: per index, one into the
        base and one among the other spans, whose inputs never overlap."""

        for key, index in list(self.m.indexes.items()):
            if len(self.jobs) >= self.concurrency:
                break
            if self._checked.get(key) is index or key in self.stopped:
                continue
            endpoints = self.m.endpoints(*key)
            planned = False
            for lane in ("base", "tail"):
                if (key, lane) in self.jobs or len(self.jobs) >= self.concurrency:
                    continue
                other = self._busy.get((key, "tail" if lane == "base" else "base"), frozenset())
                plan = KeyIndex(None, None, index, self.key_options).plan_merge(
                    endpoints, lane=lane, busy=other
                )
                if plan is None:
                    continue
                lo, count = plan
                inputs = tuple((sp.a, sp.b) for sp in index.spans[lo : lo + count])
                tries = self._attempts.get((key, inputs), 0)
                if tries >= MERGE_ATTEMPTS:
                    self.stopped.add(key)
                    self.failing[f"key index {key[0]}/{key[1]} merges"] = (
                        f"merging spans {inputs} failed {tries} times: this index merges no more until restart"
                    )
                    break
                self._attempts[(key, inputs)] = tries + 1
                self._busy[(key, lane)] = frozenset(inputs)
                self.jobs.spawn(self._merge(key, lane, index, plan, endpoints), key=(key, lane))
                planned = True
            if not planned and not any((key, lane) in self.jobs for lane in ("base", "tail")):
                self._checked[key] = index

    async def _merge(self, key: tuple, lane: str, index: IndexState, plan, endpoints: set[int]) -> None:
        """One span merge, run on a worker thread with its own event loop so
        merging never blocks the engine, then published through the journal
        if the index is still the life it was planned against and holds its
        inputs; else its output is deleted. Writes are exact, so a merge lets
        go of no object the deltas did not already list."""

        options, objects, service = self.key_options, self.state.objects, self.keys

        def work():
            async def go(local):
                keys = KeyIndex(ObjectIO(objects, local=local), None, index, options)
                if service is not None:
                    keys.on_write = lambda path, f, data: service.installed(index.prefix, f, path, data)
                return await keys.merge(plan, endpoints)

            # One warm copy serves every engine reader: an index the engine's cache
            # holds is read from its local files, the store otherwise.
            with service.open(index) if service is not None else contextlib.nullcontext() as local:
                if local is not None:
                    try:
                        return asyncio.run(go(local.handles))
                    except LocalError as e:
                        service.corrupt(e.path)  # dropped, fetched again by a fill; this run reads the store
            return asyncio.run(go(None))

        try:
            try:
                merged = await asyncio.to_thread(work)
            except Exception as error:
                self.failing[f"key index {key[0]}/{key[1]}"] = f"{type(error).__name__}: {error}"
                log.exception("key index merge failed for %s", key)
                return
            self.failing.pop(f"key index {key[0]}/{key[1]}", None)
            if merged is None:  # a rewrite that would not drop a quarter: nothing to publish,
                self._attempts.pop(
                    (key, tuple((sp.a, sp.b) for sp in index.spans[plan[0] : plan[0] + plan[1]])), None
                )
                if self.m.indexes.get(key) is index:  # nor to plan again until the index changes
                    self._checked[key] = index
                return
            output, partition = key
            current = self.m.indexes.get(key)
            if (
                current is None
                or current.life != index.life
                or not current.holds(merged.inputs, merged.names)
            ):
                await self._delete([index.path(f.name) for f in merged.span.files])
                return
            self.state.record(
                {
                    "type": "IndexMerged",
                    "output": output,
                    "partition": partition,
                    "life": index.life,
                    "prefix": index.prefix,
                    "inputs": [list(r) for r in merged.inputs],
                    "names": merged.names,
                    "span": merged.span.to_json(),
                    "read": merged.read,
                    "written": merged.written,
                    "at": self.clock(),
                }
            )
            self._attempts.pop((key, tuple(merged.inputs)), None)
            if self.keys is not None:  # published: no new snapshot reads its inputs
                kept = (self.m.indexes.get(key) or current).referenced()
                self.keys.retired([index.path(n) for names in merged.names for n in names if n not in kept])
        finally:
            self._busy.pop((key, lane), None)

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
        cache = self.keys.floor() if self.keys is not None else math.inf

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
        if self.keys is not None:  # what the engine's cache held of them goes too
            self.keys.retired(due)

    async def collect_orphans(self) -> None:
        """Every `ORPHAN_SECONDS`, delete the merge outputs (`m…` files) that
        nothing names (docs/key-index-design.md § Lifecycles): no index's
        current spans, no file let go of and still awaiting its readers, no
        pending cleanup, and no merge running here. A published output is in
        an index until a later event lets go of it, so a pinned snapshot never
        names an orphan. Delta files are not merge outputs: a dead attempt's
        are its repair intent's."""

        now = self.clock()
        if now - self._orphans_at < ORPHAN_SECONDS:
            return
        self._orphans_at = now
        named = {index.path(n) for index in self.m.indexes.values() for n in index.referenced()}
        named |= {path for path, _ in self.m.garbage} | set(self.m.cleanup_reads())
        running = {
            (self.m.indexes[key].prefix, ins[0][0], ins[-1][1])
            for (key, _), ins in self._running()
            if key in self.m.indexes
        }
        orphans = []
        for path in await self.state.list_objects("keys/"):
            prefix, _, name = path.rpartition("/")
            if not name.startswith("m") or not name.endswith(".kx") or path in named:
                continue
            try:  # m{a:012d}-{b:012d}-{stamp}.{n:04d}.kx
                a, b = int(name[1:13]), int(name[14:26])
            except ValueError:
                continue
            if (prefix + "/", a, b) not in running:
                orphans.append(path)
        if orphans:
            log.info("deleting %d orphaned merge outputs", len(orphans))
            await self._delete(orphans)
            if self.keys is not None:
                self.keys.retired(orphans)

    def _running(self):
        """The merges running here: `((index key, lane), sorted input spans)`."""

        return [(k, sorted(ins)) for k, ins in self._busy.items() if ins]

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
        # A run whose attempt a read-ahead names is kept: its spec lists the keys (K45).
        named = {run for position in self.m.positions() for _, run, _ in position.get("ahead", ())}
        await self.delete_runs([r for r in await self.history.expired(finite, default) if r[0] not in named])

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
