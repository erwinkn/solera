"""Storage upkeep (docs/object-store-state.md §6, §7, §11): background work
that keeps the object store tidy. The engine never waits on it.

- key indexes: adjacent layers merged by the rule (docs/key-index-design.md
  § Compaction), on worker threads; the cut raised as readers move on;
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

from solera.keys.io import ObjectIO
from solera.keys.layers import LayerIndex, LayerState, delta_names, epoch_of
from solera.tasks import Tasks

from . import history
from .model import CLEANUP

log = logging.getLogger(__name__)

ALIVE = "engine/alive.json"  # when the engine last said it was up, while runs were in progress
ALIVE_SECONDS = 30.0
MERGE_ATTEMPTS = 3  # attempts per input set before merging an index stops, alarmed (the write bound's R)
ORPHAN_SECONDS = 600.0


def _attempt_of(name: str) -> str | None:
    """The attempt a commit's delta file names (`{commit:012d}-{attempt}-{n}.lay`,
    `{commit:012d}-{attempt}.lix`), or None: not a delta's."""

    stem, _, ext = name.rpartition(".")
    parts = stem.split("-")
    if ext == "lay" and len(parts) == 3 and parts[2].isdigit():
        commit, attempt = parts[0], parts[1]
    elif ext == "lix" and len(parts) == 2:
        commit, attempt = parts
    else:
        return None
    return attempt if len(commit) == 12 and commit.isdigit() and attempt else None


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
        keys=None,
        failing: dict[str, str] | None = None,
    ):
        self.state, self.history, self.manifest, self.clock = state, history, manifest, clock
        self.keys = keys  # the engine's key cache: merge outputs go into it as written
        self.concurrency = concurrency
        self.retention_interval, self.interval = retention_interval, interval
        self.tasks = Tasks("upkeep")  # its tick
        self.jobs = Tasks("upkeep jobs")  # merges running, by (index key, lane)
        self.failing = {} if failing is None else failing  # what fails now, by name: the engine's
        self._checked: dict[tuple, LayerState] = {}  # the state last found needing nothing
        self._busy: dict[
            tuple, frozenset
        ] = {}  # the input layers (ids) of each merge running, by (key, lane)
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
        """Raise each index's cut to the oldest commit its readers hold (its
        head when none holds one), and
        start merges, `concurrency` at a time: per index, one into the base
        and one among the tiers, whose inputs never overlap. Once an input
        set's merge was uploaded `MERGE_ATTEMPTS` times in the index's
        current life, none published, the index merges no more, alarmed: the
        count is durable (`Model.merges`), so neither a restart nor a
        takeover resets it, and a new life starts afresh. (Not the next
        smaller set: a store that fails every upload would pay the budget
        once per candidate set.)"""

        for key, index in list(self.m.indexes.items()):
            oldest = self.m.oldest_observed(*key)
            if oldest is None:  # no reader holds a commit of it: none needs a flip
                oldest = index.head
            if oldest > index.cut:
                self.state.record(
                    {
                        "type": "IndexCut",
                        "output": key[0],
                        "partition": key[1],
                        "life": index.life,
                        "cut": oldest,
                    }
                )
                index = self.m.indexes[key]
            if len(self.jobs) >= self.concurrency:
                break
            if self._checked.get(key) is index:
                continue
            rec = self.m.merge_record(key, index.life)
            spent = {k for k, n in rec["attempts"].items() if n >= MERGE_ATTEMPTS}
            if spent:
                self.failing[f"key index {key[0]}/{key[1]} merges"] = (
                    f"a merge of {sorted(spent)[0]!r} was uploaded {MERGE_ATTEMPTS} times, none published: "
                    "the index merges no more in this life"
                )
                self._checked[key] = index
                continue
            self.failing.pop(f"key index {key[0]}/{key[1]} merges", None)
            planned = False
            for lane in ("base", "tier"):
                if (key, lane) in self.jobs or len(self.jobs) >= self.concurrency:
                    continue
                other = self._busy.get((key, "tier" if lane == "base" else "base"), frozenset())
                plan = index.plan(busy=other, stopped=spent, lane=lane)
                if plan is None:
                    continue
                _, lo, count = plan
                self._busy[(key, lane)] = frozenset(x.id for x in index.layers[lo : lo + count])
                self.jobs.spawn(self._merge(key, lane, index, lo, count), key=(key, lane))
                planned = True
            if not planned and not any((key, lane) in self.jobs for lane in ("base", "tier")):
                self._checked[key] = index

    async def _merge(self, key: tuple, lane: str, index: LayerState, lo: int, count: int) -> None:
        """One merge, run on a worker thread with its own event loop so merging
        never blocks the engine, then published through the journal if the
        index is still the life it was planned against and holds its inputs;
        else its output is deleted. Every upload is counted, durably, before
        it starts. The engine's cache serves the inputs it holds and takes
        the outputs as written."""

        objects, service = self.state.objects, self.keys
        epoch = self.state.journal.epoch
        output, partition = key
        ins = index.layers[lo : lo + count]
        cache = getattr(service, "cache", None)

        def work():
            async def go():
                return await LayerIndex(ObjectIO(objects), index, cache=cache).merge(lo, count, epoch=epoch)

            return asyncio.run(go())

        try:
            try:
                self.state.record(
                    {
                        "type": "MergeAttempted",
                        "output": output,
                        "partition": partition,
                        "life": index.life,
                        "inputs": index.attempt_key(ins),
                        "at": self.clock(),
                    }
                )
                await self.state.durable()  # counted before anything is uploaded
                ids, layer = await asyncio.to_thread(work)
            except Exception as error:
                self.failing[f"key index {key[0]}/{key[1]}"] = f"{type(error).__name__}: {error}"
                log.exception("key index merge failed for %s", key)
                return
            self.failing.pop(f"key index {key[0]}/{key[1]}", None)
            current = self.m.indexes.get(key)
            if current is None or current.life != index.life or not current.holds(ids):
                await self._delete([index.path(n) for n in layer.names()])
                return
            self.state.record(
                {
                    "type": "IndexMerged",
                    "output": output,
                    "partition": partition,
                    "life": index.life,
                    "prefix": index.prefix,
                    "ids": ids,
                    "layer": layer.to_json(),
                    "at": self.clock(),
                }
            )
            if service is not None:  # published: no new read opens its inputs
                kept = (self.m.indexes.get(key) or current).referenced()
                service.retired([index.path(n) for x in ins for n in x.names() if n not in kept])
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
        """Every `ORPHAN_SECONDS`, find the index files nothing names
        (docs/key-index-design.md § Lifecycles) and record them as garbage
        (`OrphansFound`): deleted like any garbage, after a journal write — a
        fenced engine's fails, and it deletes nothing — and past every pin.
        Nothing names a file when no index's layers, garbage, pending cleanup
        or repair intent does, and:

        - a merge output (`{life}/l{a}-{b}-e{epoch}-{id}…`): of this engine's
          epoch or an earlier one, never a later one's (a takeover), and no
          merge running here writes it;
        - a commit's delta (`{commit}-{attempt}-{n}.lay`): its attempt holds
          no claim (it ended without committing), and no reader here holds
          its index (a source commit resolving)."""

        now = self.clock()
        if now - self._orphans_at < ORPHAN_SECONDS:
            return
        self._orphans_at = now
        listed = await self.state.list_objects("keys/")
        # Judged after the listing, with no await before the record: a merge of
        # this engine's that published meanwhile is named, one still running is.
        m = self.m
        named = {index.path(n) for index in m.indexes.values() for n in index.referenced()}
        named |= {path for path, _ in m.garbage} | set(m.cleanup_reads())
        for key, intents in m.repairs.items():
            index = m.indexes.get(key)
            if index is not None:
                named |= {index.path(n) for intent in intents for n in delta_names(intent)}
        own, running = self.state.journal.epoch, set()
        for (key, _), ids in self._running():
            index = m.indexes.get(key)
            held = [x for x in index.layers if x.id in ids] if index is not None else []
            if held:
                running.add(f"{index.prefix}{index.life}/l{held[0].a:012d}-{held[-1].b:012d}-e{own}-")
        claimed = {c.get("attempt") for c in m.claims.values()}
        read = [prefixes for _, prefixes in m.readers.values()]
        orphans = []
        for path in listed:
            if path in named:
                continue
            epoch = epoch_of(path)
            if epoch is not None:
                if epoch <= own and not any(path.startswith(r) for r in running):
                    orphans.append(path)
                continue
            prefix, name = path.rsplit("/", 1)
            attempt = _attempt_of(name)
            if attempt is None or attempt in claimed:
                continue
            if any(ps is None or f"{prefix}/" in ps for ps in read):
                continue
            orphans.append(path)
        if orphans:
            log.info("found %d orphaned index files", len(orphans))
            self.state.record({"type": "OrphansFound", "paths": orphans})

    def _running(self):
        """The merges running here: `((index key, lane), input layer ids)`."""

        return [(k, ins) for k, ins in self._busy.items() if ins]

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
