"""The engine's state (docs/object-store-state.md §4, §5): what it holds, the
events that change it, and `apply`.

The model is plain data changed only by `apply(event)`, so replaying the
journal reproduces it exactly. It has three layers:

- **Durable**: the project, heads, key indexes, each asset partition's record
  (cursor, last outcome, completeness, what its inputs observed, failing keys),
  automation state, active runs
  (tasks nested inside, each launched attempt on its task), outputs owing a repair, idempotency
  receipts, files awaiting deletion, and the run history's files and the
  rows not yet flushed to them (§7). `snapshot()` serializes exactly this,
  and `restore()` loads it.
- **Derived**: the ready queue, pending-per-partition, dependency counters, run
  roll-ups, and the claims, claims and pool work of launched attempts.
  Rebuilt by `restore()`, maintained by `apply()`.
- **Memory only**: the claim an attempt holds while it prepares, before it
  is launched (a restart simply dispatches its task again).

Events carry every timestamp they need; `apply` never reads a clock.
`applied` counts the events applied: the model's own clock, the same in
every engine that replays the journal. What must not be compared across
hosts' wall clocks is ordered by it: an attempt pins the files it may read
at its claim's event counter, and a file let go of at a later one waits for it.
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import math

from solera.keys.layers import DeltaFiles, Layer, LayerState, delta_names, index_prefix

from . import history, observed
from .lake import LakeState

TERMINAL_TASK = frozenset({"succeeded", "skipped", "failed", "blocked", "canceled"})
TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
# What an automation's record keeps of its own; the rest is its manifest entry.
AUTOMATION_STATE = ("enabled", "since", "last_fired", "last_run", "last_deploy", "pending")
CLEANUP = "@cleanup"  # a cleanup task's asset: none of the project's (K25)
BAD_OUTCOME = frozenset({"failed", "blocked", "canceled"})
MAX_RECEIPTS = 10_000  # idempotency receipts kept for replayed submissions
STUCK_AFTER = 3  # misses before a clean up entry is stuck (docs/lifecycle.md §9.8)
REPAIR_RUNS = 3  # runs the repair clock gives a partition a dead writer left owing a repair
REPAIR_SPACING = 60.0  # seconds between them: 60, then 120 (the spacing doubles)


def _nest(flat: dict, depth: int) -> dict:
    """{(a, b): v} -> {a: {b: v}} (depth 2), {(a, b, c): v} -> {a: {b: {c: v}}} (depth 3)."""

    out: dict = {}
    for key, value in flat.items():
        node = out
        for part in key[:-1]:
            node = node.setdefault(part, {})
        node[key[-1]] = value
    return out


class Grouped(dict):
    """A map keyed by `(name, partition)` that also holds each name's entries
    together: `of(name)` finds them without looking at any other name's."""

    def __init__(self, items=()):
        super().__init__()
        self._of: dict[str, dict] = {}
        for key, value in dict(items).items():
            self[key] = value

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._of.setdefault(key[0], {})[key[1]] = value

    def __delitem__(self, key):
        super().__delitem__(key)
        group = self._of[key[0]]
        del group[key[1]]
        if not group:
            del self._of[key[0]]

    def pop(self, key, *default):
        if key not in self:
            if default:
                return default[0]
            raise KeyError(key)
        value = self[key]
        del self[key]
        return value

    def of(self, name: str) -> dict:
        """`{partition: value}` of one name."""

        return self._of.get(name, {})


def _flatten(nested: dict, depth: int) -> dict:
    out = {}

    def walk(node, prefix):
        if len(prefix) == depth - 1:
            for k, v in node.items():
                out[(*prefix, k)] = v
            return
        for k, v in node.items():
            walk(v, (*prefix, k))

    walk(nested or {}, ())
    return out


def _renumbered(entries: list[dict]) -> list[dict]:
    """Two names' clean up entries as one partition's: in order, ids unique again."""

    out, seen = [], {}
    for d in sorted(entries, key=lambda d: d["n"]):
        ordinal = seen[d["n"]] = seen.get(d["n"], -1) + 1
        out.append({**d, "id": f"{d['n']}.{ordinal}"})
    return out


def commit_of(head: dict | None) -> tuple | None:
    """What identifies the commit that installed a head — not the figures
    upkeep corrects on it: whether a writer came in
    between is a question of this alone."""

    if head is None:
        return None
    return (
        head.get("run"),
        head.get("attempt"),
        head["ref"].get("generation"),
        head.get("commit_number"),
        head.get("n"),
    )


def _delta(keys: dict, generation: int | None) -> DeltaFiles:
    """A commit's delta, at `generation`, the one its writes carried, when
    its head records it."""

    delta = DeltaFiles.from_json(keys)
    return delta if generation is None else dataclasses.replace(delta, generation=int(generation))


def _delta_files(deltas) -> frozenset[str]:
    """The delta files of queued deltas (`Model.cleaning`): a cleanup step's input."""

    return frozenset(f"{d['prefix']}{name}" for d in deltas for name in d["files"])


class Model:
    def __init__(self):
        self.restore(None)

    # -- snapshot / restore ---------------------------------------------------------------

    def snapshot(self, *, copied: bool = True) -> dict:
        """The state, as a checkpoint holds it. `copied=False` hands out the
        model's own structures — for a caller that encodes them at once, before
        any later event changes them (the journal's checkpoint: no deep copy
        under its lock)."""

        data = {
            "event_counter": self.event_counter,
            "deploy": self.deploy,
            "deploy_number": self.deploy_number,
            "manifest": self.manifest,
            "project": self.project,
            "heads": _nest(self.heads, 2),
            "indexes": _nest({k: v.to_json() for k, v in self.indexes.items()}, 2),
            "merges": _nest(self.merges, 2),
            "garbage": self.garbage,
            "deleted": self.deleted,
            "partitions": _nest(self.partitions, 2),
            "repairs": _nest(self.repairs, 2),
            "cleanups": _nest(self.cleanups, 2),
            "cleaning": _nest(self.cleaning, 2),
            "retired": dict(self.retired),
            "homes": dict(self.homes),
            "reset_at": _nest(self.reset_at, 2),
            "automations": {
                name: {k: auto[k] for k in AUTOMATION_STATE if k in auto}
                for name, auto in self.automations.items()
            },
            "sensors": self.sensors,
            "runs": self.runs,
            "receipts": list(self.receipts.items()),
            "history": self.history.to_json(),
        }
        return copy.deepcopy(data) if copied else data

    def restore(self, snap: dict | None) -> None:
        snap = copy.deepcopy(snap) if snap else {}
        # durable
        self.event_counter: int = snap.get("event_counter") or 0
        self.deploy = snap.get("deploy")
        # how many deploys this namespace has served: what gives stored outcomes
        # one try per deploy (per-key §13)
        self.deploy_number: int = snap.get("deploy_number") or 0
        self.manifest = snap.get("manifest")
        self.project = snap.get("project")
        self.heads = Grouped(_flatten(snap.get("heads"), 2))
        self.indexes: dict[tuple, LayerState] = {
            k: LayerState.from_json(v) for k, v in _flatten(snap.get("indexes"), 2).items()
        }
        # (output, partition) -> what merges of the index's current life tried:
        # `life`, and `attempts` by input layers (uploads, failed or
        # abandoned, none published), keyed by LayerState.attempt_key.
        self.merges: dict[tuple, dict] = _flatten(snap.get("merges"), 2)
        # [path, n]: files nothing references since the n-th event applied
        self.garbage: list[list] = snap.get("garbage") or []
        # deleted runs whose directories are still to be deleted (§11)
        self.deleted: list[str] = snap.get("deleted") or []
        # (asset, partition) -> the partition's record (§5): its `cursor`; `last`, its last
        # terminal outcome;
        # `observed` {input: what it observed — a keyed input's observation record, an
        # unkeyed one's last commit read}; the `definition` they were observed under;
        # and a per-key asset's `outcomes` record
        # (docs/per-key-processing.md §9), whose index lives in `indexes` under
        # ("@asset", partition). A rename moves it, retirement trims it: one record.
        self.partitions = Grouped(_flatten(snap.get("partitions"), 2))
        # (output, partition) -> intents of attempts that died while writing it (§8)
        self.repairs: dict[tuple, list] = _flatten(snap.get("repairs"), 2)
        # (output, partition) -> cleanup of an immutable store, each entry at the
        # event counter that let go of it: for the partition's next attempt to clean up
        # once no reader pins it (docs/lifecycle.md §9.8)
        self.cleanups: dict[tuple, list] = _flatten(snap.get("cleanups"), 2)
        # (output, partition) -> an immutable keyed output's cleanup queue: the deltas
        # committed and not yet cleaned that name what they replaced, in commit order,
        # each kept until a cleanup step walks past it. Its front is the cleanup
        # cursor (docs/lifecycle.md §9.8)
        self.cleaning: dict[tuple, list] = _flatten(snap.get("cleaning"), 2)
        # Output lives removed or moved away, whose data a cleanup task deletes (K25):
        # id -> where it was, what it was, and when it may go.
        self.retired: dict[str, dict] = dict(snap.get("retired") or {})
        # Output -> the name its life began under: where its store keeps every partition
        # of it, renamed or not (K25). A rename carries it; a reset starts a new one.
        self.homes: dict[str, str] = dict(snap.get("homes") or {})
        # ("output" | "asset", name) -> the deploy number that last reset it:
        # removed, or (an output) moved to another store. An attempt launched
        # under an earlier one commits nothing of it.
        self.reset_at: dict[tuple, int] = _flatten(snap.get("reset_at"), 2)
        # name -> its manifest entry and its state (`AUTOMATION_STATE`): only the state
        # is checkpointed, the entry is the manifest's
        declared = (self.manifest or {}).get("automations") or {}
        self.automations: dict[str, dict] = {
            name: {**declared.get(name, {}), **state}
            for name, state in (snap.get("automations") or {}).items()
        }
        # sensor -> {cursor, accepted}: the last tick that changed something (docs/lifecycle.md §11.4)
        self.sensors: dict[str, dict] = snap.get("sensors") or {}
        self.runs: dict[str, dict] = snap.get("runs") or {}
        self.receipts: dict[str, str] = dict(snap.get("receipts") or [])
        # the run history (§7): per table, its files and the rows awaiting a flush
        self.history = LakeState(history.TABLES, snap.get("history"))
        # derived from launched attempts, plus memory-only claims of attempts preparing
        # task id -> {attempt, started_at, pin, status, launched?, reads?}; `pin`: `applied` at the claim
        self.claims: dict[str, dict] = {}
        self.attempts: dict[str, str] = {}  # attempt id -> task id, while claimed
        self.claimed_partitions: dict[tuple, str] = {}  # (asset, partition) -> the attempt that claims it
        self.pool: dict[str, dict] = {}  # attempt id -> pool work
        # sensor -> the tick dispatched and not yet decided: {tick, cursor, snapshot, pin, ...}
        self.ticks: dict[str, dict] = {}
        self.readers: dict[object, tuple] = {}  # the engine's own readers: (event counter, domains)
        # the assets the last deploy changed: added (again), renamed, their declarations
        # changed, or reset. Memory only: the engine reads it as it records the deploy,
        # for what its OnChange automations owe (`FiringsOwed`, an event of its own).
        self.changed: set[str] = set()
        self._reindex()

    def _reindex(self) -> None:
        """Rebuild the derived indexes from the durable runs and the manifest."""

        self.task_run: dict[str, str] = {}
        self.queue: dict[str, float] = {}
        self.pending: dict[tuple, set] = {}
        self.dependents: dict[str, set] = {}
        self.unfinished: dict[str, dict] = {}
        self.run_left: dict[str, list] = {}  # run id -> [left, bad]
        self.archivable: set[str] = set()
        for run_id, run in self.runs.items():
            self._index_run(run_id, run)

    def _index_run(self, run_id: str, run: dict) -> None:
        tasks = run["tasks"]
        left = bad = 0
        for tid, task in tasks.items():
            self.task_run[tid] = run_id
            status = task["status"]
            if task.get("launched"):
                self._hold(task)
            if status in TERMINAL_TASK:
                bad += status in BAD_OUTCOME
                continue
            left += 1
            self.pending.setdefault((task["asset"], task["partition"]), set()).add(tid)
            if status == "queued":
                self.queue[tid] = task["ready_at"]
            elif status == "waiting":
                n_left = n_bad = 0
                for dep in task["deps"]:
                    dep_status = tasks[dep]["status"] if dep in tasks else "failed"
                    if dep_status in TERMINAL_TASK:
                        n_bad += dep_status in BAD_OUTCOME
                    else:
                        n_left += 1
                        self.dependents.setdefault(dep, set()).add(tid)
                self.unfinished[tid] = {"left": n_left, "bad": n_bad}
        self.run_left[run_id] = [left, bad]
        if run["status"] in TERMINAL_RUN:
            self.archivable.add(run_id)

    def _unindex_run(self, run_id: str) -> None:
        run = self.runs.get(run_id)
        for tid, task in (run or {}).get("tasks", {}).items():
            self.task_run.pop(tid, None)
            self.queue.pop(tid, None)
            self.dependents.pop(tid, None)
            self.unfinished.pop(tid, None)
            self._release_claim(tid)
            bucket = self.pending.get((task["asset"], task["partition"]))
            if bucket:
                bucket.discard(tid)
                if not bucket:
                    del self.pending[(task["asset"], task["partition"])]
        self.run_left.pop(run_id, None)
        self.archivable.discard(run_id)

    # -- reads ---------------------------------------------------------------------------

    def task(self, task_id: str) -> dict | None:
        run_id = self.task_run.get(task_id)
        return self.runs[run_id]["tasks"][task_id] if run_id else None

    def status_of(self, task: dict) -> str:
        """The task's live status: its durable one, or its claim's."""

        claim = self.claims.get(task["id"])
        return claim["status"] if claim else task["status"]

    def index(self, output: str, partition: str) -> LayerState:
        """The output's key index, or an empty one where a new index would go."""

        found = self.indexes.get((output, partition))
        if found is not None:
            return found
        return LayerState(prefix=index_prefix(output, partition), life=str(self.event_counter))

    def key_count(self, output: str, partition: str) -> int | None:
        """A keyed output's live keys, as its index counts them (None: unkeyed)."""

        index = self.indexes.get((output, partition))
        return None if index is None else index.count

    def observed_commits(self, output: str, partition: str) -> list[int]:
        """The commits of an upstream index its readers hold: every commit its
        consumers' observation records were observed at — their bases' and
        ranges' heads, in its current life — and the head each batch in
        flight classed its keys at."""

        state = self.indexes.get((output, partition))
        life = state.life if state is not None else ""
        commits = []
        for record in self.partitions.values():
            for rec in (record.get("observed") or {}).values():
                if rec.get("upstream") == [output, partition] and observed.lives(rec) == {life}:
                    commits += [layer["endpoint"] for layer in [rec["base"], *rec["ranges"]]]
        for claim in self.claims.values():
            commits += [
                read[2] for read in claim.get("reads") or () if (read[0], read[1]) == (output, partition)
            ]
        return [int(c) for c in commits if c is not None]

    def oldest_observed(self, output: str, partition: str) -> int | None:
        """The oldest commit of an upstream index any reader holds (None:
        none): the index's cut never passes it (A31 R3)."""

        return min(self.observed_commits(output, partition), default=None)

    def heads_of(self, output: str) -> list[tuple[str, dict]]:
        return sorted(self.heads.of(output).items())

    def partition(self, asset: str, partition: str) -> dict:
        """An asset partition's record, empty where it has none: to read."""

        return self.partitions.get((asset, partition)) or {}

    def _partition(self, asset: str, partition: str) -> dict:
        """An asset partition's record, to change: made if it has none."""

        key = (asset, partition)
        if key not in self.partitions:
            self.partitions[key] = {}
        return self.partitions[key]

    def _ended(self, task: dict, outcome: str, run: str, attempt: str | None, at: float) -> None:
        record = self._partition(task["asset"], task["partition"])
        record["last"] = {"outcome": outcome, "run": run, "attempt": attempt, "at": at}
        if outcome == "failed":  # its runs failing in a row: changes back off (F43)
            record["failed_in_row"] = int(record.get("failed_in_row") or 0) + 1
        elif outcome in ("succeeded", "skipped"):
            record.pop("failed_in_row", None)

    def pending_partitions(self, asset: str) -> set[str]:
        return {partition for (a, partition), ids in self.pending.items() if a == asset and ids}

    def is_pending(self, asset: str, partition: str) -> bool:
        return bool(self.pending.get((asset, partition)))

    def repair_runs(self, output: str, partition: str) -> tuple[int, float | None]:
        """How many runs the repair clock gave this partition's repair, and when
        the last began. Kept on the intents, so a rename carries it and the
        repair's commit drops it."""

        intents = self.repairs.get((output, partition)) or ()
        runs = max((int(i.get("runs", 0)) for i in intents), default=0)
        return runs, max((i["tried_at"] for i in intents if "tried_at" in i), default=None)

    def due(self, now: float) -> list[str]:
        """Queued, unclaimed tasks ready by `now`, oldest first."""

        return [tid for tid, at in sorted(self.queue.items(), key=lambda kv: (kv[1], kv[0])) if at <= now]

    # -- claims ------------------------------------------------------------------------

    def claim(self, task_id: str, attempt: str, now: float) -> None:
        """Claim a task for an attempt that is preparing: memory only, until
        `AttemptLaunched` makes it durable."""

        task = self.task(task_id)
        self.claims[task_id] = {
            "attempt": attempt,
            "started_at": now,
            # Not durable until its launch is: an engine replaced before then leaves
            # the number to the next one, which may hand it out again. Harmless only
            # because nothing outside learns of an attempt before its launch is
            # durable, so no write was made under it.
            "generation": self.event_counter,
            "status": "running",
        }
        self.attempts[attempt] = task_id
        self.claimed_partitions[(task["asset"], task["partition"])] = attempt
        self.queue.pop(task_id, None)

    def pins(self, but: str | None = None) -> list[tuple[int, tuple[str, ...] | None]]:
        """Every reader pin (docs/lifecycle.md §9.8): its event counter, and
        the index prefixes of the output partitions it reads — `None`, all of
        them. The attempts claimed (but attempt `but`), by what they read and
        write (`None` while one prepares); the delta passes delivered over
        several attempts and the pattern change drains reading one snapshot over
        several (per-key-processing.md §11), by their upstream; the sensor
        ticks in flight, by their sources; the engine's own readers."""

        out = [
            (c["generation"], c.get("prefixes"))
            for c in self.claims.values()
            if c["attempt"] != but and "generation" in c
        ]
        for tick in self.ticks.values():
            out.append(
                (tick["pin"], tuple(self.index(source, "").prefix for source in tick.get("snapshot") or ()))
            )
        out += self.readers.values()
        return out

    def floors(self, but: str | None = None) -> tuple[float, dict[str, int]]:
        """The reader pins reduced, once: the oldest pin of the readers of
        everything, and per domain the oldest pin of its readers."""

        low, by = math.inf, {}
        for n, prefixes in self.pins(but):
            if prefixes is None:
                low = min(low, n)
            for domain in prefixes or ():
                if n < by.get(domain, math.inf):
                    by[domain] = n
        return low, by

    def pin_floor(self, but: str | None = None, path: str | None = None, floors=None) -> float:
        """The oldest pin of a reader that may read `path` (any reader, with
        none): what was let go of at or before it is read by no one. One slow
        reader holds back only what it reads. Domains are directories (they
        end in `/`), so a path's are found one `/` at a time."""

        low, by = self.floors(but) if floors is None else floors
        if path is None:
            return min(low, *by.values()) if by else low
        end = path.find("/")
        while end != -1:
            low = min(low, by.get(path[: end + 1], math.inf))
            end = path.find("/", end + 1)
        return low

    @contextlib.contextmanager
    def reading(self, *prefixes: str):
        """Pin what the index state holds now — of `domains` (index prefixes,
        `history/`), all of it with none — for as long as the block reads it:
        an engine listing, a source commit's resolution, a history query.
        Memory only, like the reads."""

        token = object()
        self.readers[token] = (self.event_counter, tuple(prefixes) or None)
        try:
            yield
        finally:
            del self.readers[token]

    def release(self, task_id: str, attempt: str) -> None:
        """Drop the memory-only claim of an attempt that was never launched."""

        claim = self.claims.get(task_id)
        if claim is not None and claim["attempt"] == attempt and not claim.get("launched"):
            self._release_claim(task_id, attempt)

    def _hold(self, task: dict) -> None:
        """The claim, claim and pool work of a task's launched attempt."""

        launched = task["launched"]
        attempt = launched["attempt"]
        pool = launched.get("pool")
        status = "running" if pool is None else "claimable"
        self.claims[task["id"]] = {
            "attempt": attempt,
            "started_at": launched["started_at"],
            "generation": launched["generation"],
            "status": status,
            "launched": True,
            "reads": reads(launched["prepared"].get("plans") or {}),
            "prefixes": tuple(launched["prepared"].get("prefixes") or ()),
            "cleanups": _delta_files(  # the deltas a cleanup task's step reads (§9.8)
                d
                for info in ((launched["prepared"].get("cleanup") or {}).get("outputs") or {}).values()
                for d in (info.get("deltas") or {}).get("deltas") or ()
            ),
        }
        self.attempts[attempt] = task["id"]
        if not self.earlier_life(task):  # it holds no name a later life may claim
            self.claimed_partitions[(task["asset"], task["partition"])] = attempt
        self.queue.pop(task["id"], None)
        if pool is not None:  # discoverable until it ends; its claim is the worker's (§10)
            self.pool[attempt] = {
                "attempt": attempt,
                "task": task["id"],
                "run": self.task_run[task["id"]],
                "asset": task["asset"],
                "partition": task["partition"],
                "pool": pool["name"],
                "needs": pool.get("needs") or {},
                "created_at": launched["at"],
            }

    def claimed(self, attempt: str) -> dict | None:
        """The live claim behind an attempt. The claim decides, not
        `claimed_partitions`: that index gates dispatch for an asset's
        current life only, while an earlier life's attempt, still in flight
        after a reset, must settle all the same."""

        task_id = self.attempts.get(attempt)
        claim = self.claims.get(task_id) if task_id else None
        if claim is None or claim["attempt"] != attempt or self.task(task_id) is None:
            return None
        return claim

    def earlier_life(self, task: dict) -> bool:
        """Whether a task's launched attempt is of an earlier life of its
        asset: launched before the deploy that removed the asset (the
        commit's rule: it commits nothing of the new one)."""

        deploy = ((task.get("launched") or {}).get("prepared") or {}).get("deploy_number")
        return deploy is not None and self.reset_at.get(("asset", task["asset"]), 0) > deploy

    def _release_claim(self, task_id: str, attempt: str | None = None) -> None:
        claim = self.claims.get(task_id)
        if claim is None or (attempt is not None and claim["attempt"] != attempt):
            return
        del self.claims[task_id]
        self.attempts.pop(claim["attempt"], None)
        self.pool.pop(claim["attempt"], None)
        for key, holder in list(self.claimed_partitions.items()):
            if holder == claim["attempt"]:
                del self.claimed_partitions[key]

    # -- apply -------------------------------------------------------------------------

    def apply(self, event: dict) -> None:
        self.event_counter += 1
        getattr(self, f"_on_{event['type']}")(event)

    def _on_ProjectRegistered(self, e):
        manifest = e["manifest"]
        if e["deploy"] != self.deploy:
            self.deploy_number += 1
        previous = (self.manifest or {}).get("outputs") or {}
        stores_before = (self.manifest or {}).get("stores") or {}
        sources_before = set((self.manifest or {}).get("sources") or ())
        assets_before = set((self.manifest or {}).get("assets") or ())
        declared_before = {a: declaration(self.manifest, a, self.homes) for a in assets_before}
        self.deploy, self.manifest, self.project = e["deploy"], manifest, e.get("project")
        renamed, output_map = self._apply_aliases(manifest)
        self._reconcile_tasks(manifest, renamed, output_map, e["at"])
        carried = {old for olds in renamed.values() for old in olds}  # by an alias: not removed
        homes = dict(self.homes)  # the reset starts new lives: the old ones' homes, for their cleanup
        reset = self._reset(
            {output_map.get(name, name): o.get("store") for name, o in previous.items()},
            assets_before - carried,
        )
        # What an output life left in the store it was removed or moved from: a cleanup
        # task deletes it, once due and no pin predates the reset (K25). A source's data
        # is not ours.
        for name, old in previous.items():
            now = output_map.get(name, name)
            moved = now not in manifest["outputs"] or manifest["outputs"][now].get("store") != old.get(
                "store"
            )
            if moved and name not in sources_before and old.get("store") in stores_before:
                self._retire_output(now, homes.get(now, now), old, stores_before[old["store"]], e["at"])
        self.changed = set()
        for asset in manifest["assets"]:
            olds = [a for a in renamed.get(asset, ()) if a in declared_before]  # renamed by this deploy
            before = declared_before.get(asset, declared_before.get(olds[0]) if olds else None)
            if before is None or olds or before != declaration(manifest, asset, self.homes) or asset in reset:
                self.changed.add(asset)
        self._unsubscribe()
        automations = {}
        for name, auto in manifest["automations"].items():
            existing = self.automations.get(name)
            if existing is None:
                owner, _, rest = name.partition(".")
                for old in renamed.get(owner, ()):
                    existing = self.automations.get(f"{old}.{rest}")
                    if existing is not None:
                        break
            # `since`: when it was declared, which a schedule's first time counts from.
            record = {
                **auto,
                "since": e["at"],
                "last_fired": None,
                "last_run": None,
                "last_deploy": None,
                "pending": [],
            }
            if existing is not None:
                record["enabled"] = existing["enabled"]
                if existing["trigger"] == auto["trigger"]:
                    for field in AUTOMATION_STATE[1:]:
                        record[field] = existing.get(field, record[field])
            automations[name] = record
        self.automations = automations
        self.sensors = {n: s for n, s in self.sensors.items() if n in (manifest.get("sensors") or {})}
        for name, source in manifest["sources"].items():
            if (name, "") not in self.heads:
                self.heads[(name, "")] = {
                    "ref": source["head"],
                    "run": None,
                    "attempt": None,
                    "at": e["at"],
                    "version": None,  # a source has no code version
                    "n": self.event_counter,
                }

    def _reconcile_tasks(self, manifest: dict, renamed: dict, output_map: dict, at: float) -> None:
        """Outstanding work under a new project. A task of a renamed asset
        carries on under its new name: its claim, and a launched
        attempt's output records — which keep the contract and the places
        it was launched with, and the names its worker knows them by
        (`as`) — go with it, so one writer owns the partition, and its commit
        lands under the new names. A task not yet launched of an asset that
        is gone is canceled, saying why — its dependents blocked, its run
        rolled up; a launched one settles its attempt first, then ends the
        same way (`_ready`): no task is ever queued for an asset the
        manifest lacks."""

        new_name = {old: new for new, olds in renamed.items() for old in olds}
        for run in list(self.runs.values()):
            if run["status"] in TERMINAL_RUN:
                continue
            for tid, task in sorted(run["tasks"].items()):
                if task["status"] in TERMINAL_TASK:
                    continue
                old, partition = task["asset"], task["partition"]
                if old in new_name:
                    new = task["asset"] = new_name[old]
                    bucket = self.pending.get((old, partition))
                    if bucket is not None and tid in bucket:
                        bucket.discard(tid)
                        if not bucket:
                            del self.pending[(old, partition)]
                        self.pending.setdefault((new, partition), set()).add(tid)
                    if (old, partition) in self.claimed_partitions:
                        # Never over a claim still held: an earlier life's left the index
                        # at its reset, so only two live assets merged by a rename meet here,
                        # and the one already under the name keeps it.
                        holder = self.claimed_partitions.pop((old, partition))
                        current = self.claimed_partitions.get((new, partition))
                        if current is None or self.claimed(current) is None:
                            self.claimed_partitions[(new, partition)] = holder
                    launched = task.get("launched")
                    if launched is not None:
                        outputs = launched["prepared"].get("outputs") or {}
                        launched["prepared"]["outputs"] = {
                            output_map.get(n, n): {**info, "as": info.get("as", n)}
                            if n in output_map
                            else info
                            for n, info in outputs.items()
                        }
                        if launched["attempt"] in self.pool:
                            self.pool[launched["attempt"]]["asset"] = new
                elif (
                    old not in manifest["assets"]
                    and old != CLEANUP
                    and not task.get("launched")
                    and tid not in self.claims
                ):
                    self._retire(run, task, at)

    def _retire(self, run: dict, task: dict, at: float) -> None:
        """End a task whose asset is no longer in the project: canceled, saying why."""

        task["status"], task["error"] = "canceled", f"asset {task['asset']!r} is no longer in the project"
        task.pop("held", None)
        self._stop_clock(task, at)
        self.queue.pop(task["id"], None)
        self.unfinished.pop(task["id"], None)
        self._finished(run, task, "canceled", None, at)

    def _subscribed(self, asset: str, input: str, rec: dict) -> bool:
        """Whether the project still declares the Incremental input a record
        is what it observed of: the same asset, parameter and upstream output."""

        spec = ((self.manifest or {}).get("assets") or {}).get(asset, {}).get("inputs", {}).get(input) or {}
        return spec.get("kind") == "incremental" and spec.get("output") == rec["upstream"][0]

    def _unsubscribe(self, asset: str | None = None, partition: str | None = None) -> None:
        """Retire what inputs the project no longer declares observed (a
        removed consumer, a renamed parameter, another upstream): records
        that would keep the upstream's endpoints for good. A partition with
        an attempt in flight keeps them until it settles: that attempt still
        reads them. With `asset` and `partition`, only that partition's — one
        whose attempt ended."""

        live = {(t["asset"], t["partition"]) for tid in self.claims if (t := self.task(tid)) is not None}
        keys = list(self.partitions) if asset is None else [(asset, partition)]
        for key in keys:
            if key in live or key not in self.partitions:
                continue
            held = self.partitions[key].get("observed")
            if not held:
                continue
            for input in [i for i, rec in held.items() if not self._subscribed(key[0], i, rec)]:
                del held[input]
            if not held:
                del self.partitions[key]["observed"]

    def _retire_output(self, output: str, home: str, old: dict, store: dict, at: float) -> None:
        """Record an output life's leftovers for a cleanup task (K25): the
        store it was on — a built-in one's class and config, else its name
        alone — its home, its declaration, and `before`, the first generation
        after this deploy: a later life of the name in that store keeps what
        it writes. Due `cleanup_after` from now: the output's, else its
        store's."""

        after = old.get("cleanup_after")
        after = store.get("cleanup_after") or 0.0 if after is None else after
        entry_id = f"{output}@{self.event_counter}"
        self.retired[entry_id] = {
            "id": entry_id,
            "output": output,
            "home": home,
            "store": old["store"],
            "built_in": store.get("built_in"),
            "decl": {k: old[k] for k in ("key", "incremental", "config") if k in old},
            "before": self.event_counter,
            "due": at + float(after),
        }

    def _reset(self, stores: dict[str, str], assets_before: set[str]) -> set[str]:
        """A deploy that removes an asset, or removes an output or declares it
        on another store than `stores` says it was on, resets it: what comes
        back under that name, or what the new store holds, is a new one (K10).
        An output's heads, key indexes (their files become garbage) and repair
        intents go now, and what its asset's partitions observed: its
        producer starts over, and every consumer's next run is a full run. A
        removed asset's partition records go — cursor, observations, failed
        keys — a job's included, which has no output. `reset_at` keeps the
        deploy number, by output and by asset; an attempt launched before the
        reset commits nothing of it (`Engine.commit_attempt`), so nothing
        waits for one in flight. History keeps the records, and pending
        cleanups stay: their objects are still owed. Returns the assets whose
        outputs were reset: changed, so the deploy owes their OnChange
        automations a firing (`FiringsOwed`)."""

        manifest = self.manifest or {}
        outputs, assets = manifest.get("outputs") or {}, manifest.get("assets") or {}
        for asset in assets_before - set(assets):
            self.reset_at[("asset", asset)] = self.deploy_number
            # Its attempts in flight are an earlier life's now: they settle (and commit
            # nothing), but no longer hold the name a later life may claim.
            for key in [k for k in self.claimed_partitions if k[0] == asset]:
                del self.claimed_partitions[key]
        reset = {
            name
            for name, store in stores.items()
            if name not in outputs or outputs[name].get("store") != store
        }
        for name in reset:
            self.reset_at[("output", name)] = self.deploy_number
        reset |= {k[0] for k in self.heads} - set(outputs)  # any other name no longer declared
        for name in reset | (set(self.homes) - set(outputs)):
            self.homes.pop(name, None)
        for name in outputs:
            self.homes.setdefault(name, name)
        producers = {outputs[name].get("asset") for name in reset if name in outputs} - {None}

        def gone(key) -> bool:
            if key[0].startswith("@"):  # a per-key asset's stored outcomes
                return key[0][1:] not in assets
            return key[0] in reset

        for key in [k for k in self.heads if gone(k)]:
            del self.heads[key]
        for key in [k for k in self.repairs if gone(k)]:  # what dead attempts meant to write
            index = self.index(*key)
            for intent in self.repairs.pop(key):
                self.garbage.extend([index.path(name), self.event_counter] for name in delta_names(intent))
        for key in [k for k in self.indexes if gone(k)]:
            index = self.indexes.pop(key)
            self.garbage.extend([index.path(name), self.event_counter] for name in sorted(index.referenced()))
        for key in list(self.partitions):
            if key[0] not in assets:
                del self.partitions[key]
                continue
            if key[0] in producers:  # a new life: never built, so missing, not stale
                for field in ("observed", "definition", "config", "context"):
                    self.partitions[key].pop(field, None)
            # What an input observed of a reset upstream stays, its next run a full run: a
            # keyed record's layers name the upstream's earlier life; an unkeyed one is marked.
            records = self.partitions[key].get("observed") or {}
            gone = [rec for rec in records.values() if rec["upstream"][0] in reset]
            for rec in gone:
                if "commit" in rec:
                    rec["reset"] = True
            if gone and key[0] not in producers:
                self._drop_outcomes(*key)  # failed against keys that are no longer the input's (K47)
        return producers

    def _apply_aliases(self, manifest) -> tuple[dict[str, list[str]], dict[str, str]]:
        """Move everything held under an asset's former names to its current
        one (§2): its partition records, pending automation entries, a per-key
        asset's stored outcomes, and — for outputs named after the asset — heads,
        key indexes, repair intents and pending cleanups. A new name never
        releases a write domain. An index keeps its files where they are (its
        `prefix`). Returns `{asset: [aliases]}`, for the automations and tasks
        to follow, and the outputs renamed with their assets, `{old: new}`."""

        assets = {n: a for n, a in manifest["assets"].items() if a.get("aliases")}
        renamed = {}
        for name, info in assets.items():
            renamed[name] = [a for a in info["aliases"] if a not in manifest["assets"]]
        asset_map = {old: new for new, olds in renamed.items() for old in olds}
        if not asset_map:
            return renamed, {}
        outputs = manifest["outputs"]
        output_map = {
            old: new
            for old, new in asset_map.items()
            if old not in outputs and (outputs.get(new) or {}).get("asset") == new
        }

        def move(table: dict, rename, slot: int, merge=None):
            for key in [k for k in table if k[slot] in rename]:
                target = (*key[:slot], rename[key[slot]], *key[slot + 1 :])
                if target not in table:
                    table[target] = table.pop(key)
                elif merge is not None:  # lists: both names' entries are owed
                    table[target] = merge(table[target] + table.pop(key))

        move(self.heads, output_map, 0)
        for old, new in output_map.items():
            if old in self.homes and new not in self.homes:
                self.homes[new] = self.homes.pop(old)
        move(self.indexes, output_map, 0)
        # A partition's record goes whole: a name that already has one keeps its own.
        move(self.partitions, asset_map, 0)
        # A per-key asset's stored outcomes (`@asset`), whose files stay under their prefix.
        move(self.indexes, {f"@{old}": f"@{new}" for old, new in asset_map.items()}, 0)
        move(self.repairs, output_map, 0, merge=list)
        move(self.cleanups, output_map, 0, merge=_renumbered)
        move(self.cleaning, output_map, 0, merge=lambda q: sorted(q, key=lambda d: d["n"]))
        for record in self.partitions.values():
            for rec in (record.get("observed") or {}).values():
                rec["upstream"][0] = output_map.get(rec["upstream"][0], rec["upstream"][0])
        for auto in self.automations.values():
            auto["pending"] = [[asset_map.get(a, a), s] for a, s in auto.get("pending") or []]
        return renamed, output_map

    def _on_RunSubmitted(self, e):
        run = e["run"]
        at = run["created_at"]
        self.runs[run["id"]] = run
        if e.get("command"):
            self.receipts[e["command"]] = run["id"]
            while len(self.receipts) > MAX_RECEIPTS:
                del self.receipts[next(iter(self.receipts))]
        self._index_run(run["id"], run)
        by = run.get("by") or "engine"
        self._event(run, "submitted", at, by=by, name=run.get("automation"))
        for task in run["tasks"].values():
            if task["status"] == "queued":
                self._event(run, "ready", at, task["id"])
        self._roll_up(run["id"], at)

    def _on_RunControlled(self, e):
        run = self.runs.get(e["run"])
        if run is None:
            return
        at, action, by = e["at"], e["action"], e.get("by") or "user"
        if action in ("pause", "resume"):
            run["paused"] = action == "pause"
            self._event(run, "paused" if run["paused"] else "resumed", at, by=by)
            for task in run["tasks"].values():
                if task["status"] != "queued":
                    continue
                if run["paused"]:
                    self._stop_clock(task, at)
                else:
                    task["queued_at"] = max(at, task["ready_at"])
        elif action == "cancel":
            # Every unfinished task is canceled together: no dependent is
            # blocked on the way, and nothing is left to roll up.
            run["status"] = "canceled"
            run["updated_at"] = at
            self._event(run, "canceled", at, by=by)
            for tid, task in run["tasks"].items():
                if task["status"] in TERMINAL_TASK:
                    continue
                attempt = (self.claims.get(tid) or {}).get("attempt")
                task["status"] = "canceled"
                task.pop("held", None)
                self._stop_clock(task, at)
                self._event(run, "canceled", at, tid)
                self._ended(task, "canceled", run["id"], attempt, at)
            # Claims go with their tasks, except those of launched attempts:
            # each is aborted, or — if it is already writing — waited for and
            # committed (§8).
            self._unindex_run(run["id"])
            self._index_run(run["id"], run)

    def _on_TasksHeld(self, e):
        """Why tasks ready to run are not claimed (`[reason, name]`): the
        engine is full, their executor is, another attempt holds their
        partition, or an output's merges are far behind. Only a change of
        reason is an event."""

        for tid, held in sorted(e["held"].items()):
            task = self.task(tid)
            if task is None or task["status"] != "queued" or task.get("held") == held:
                continue
            task["held"] = held
            self._event(self.runs[task["run"]], "held", e["at"], tid, reason=held[0], name=held[1])

    def _on_EngineOutage(self, e):
        """The engine was down from `down` (the last time it said it was
        alive) to `at`: every run in progress records the outage, and its
        tasks' wait leaves it out."""

        for run in self.runs.values():
            if run["status"] in TERMINAL_RUN:
                continue
            self._event(run, "outage", max(e["down"], run["created_at"]), until=e["at"])
            for task in run["tasks"].values():
                if task.get("queued_at") is not None:
                    task["wait"] += max(0.0, e["down"] - task["queued_at"])
                    task["queued_at"] = max(task["queued_at"], e["at"])

    def _claimed(self, run: dict, task: dict, attempt: str, at: float) -> None:
        """A task's attempt claimed it at `at`: its wait ends."""

        self._stop_clock(task, at)
        task.pop("held", None)
        self._event(run, "claimed", at, task["id"], attempt)

    def _on_AttemptLaunched(self, e):
        run = self.runs.get(e["run"])
        task = run["tasks"].get(e["task"]) if run else None
        if task is None or task["status"] in TERMINAL_TASK:
            return
        launched = {k: e[k] for k in ("attempt", "started_at", "generation", "at", "execution", "prepared")}
        if e.get("pool"):
            launched["pool"] = e["pool"]
        task["launched"] = launched
        if task["status"] == "queued":
            task["status"] = "running"
        self._hold(task)
        self._claimed(run, task, e["attempt"], e["started_at"])
        self._event(run, "launched", e["at"], task["id"], e["attempt"], name=e["execution"]["executor"])

    def _on_AttemptPlaced(self, e):
        """Where a launched attempt runs: its placement handle (§10)."""

        task = self.task(self.attempts.get(e["attempt"], ""))
        launched = (task or {}).get("launched")
        if launched is not None and launched["attempt"] == e["attempt"]:
            launched["handle"] = e["handle"]

    def _on_AttemptFinished(self, e):
        self._attempt_finished(e)

    def _attempt_finished(self, e) -> dict | None:
        run = self.runs.get(e["run"])
        task = run["tasks"].get(e["task"]) if run else None
        if task is None:
            return None
        self._release_claim(task["id"], e["attempt"])
        self._unsubscribe(task["asset"], task["partition"])  # what it read under an input since removed
        outcome, at = e["outcome"], e["finished_at"]
        prepared, execution = {}, {}
        launched = task.get("launched")
        # The generation its writes carried (§9.7), as its launch recorded it: the
        # refs it installs carry it. An attempt never launched wrote nothing.
        generation = launched["generation"] if (launched or {}).get("attempt") == e["attempt"] else None
        if (launched or {}).get("attempt") == e["attempt"]:
            del task["launched"]
            prepared = launched["prepared"]
            execution = history.execution(launched["execution"])
            if task["status"] == "running":
                task["status"] = "queued"  # until the outcome below says otherwise
        else:
            launched = None
            self._claimed(run, task, e["attempt"], e["started_at"])
        times = self._attempt_events(run, task, e, launched)
        for output, intent in (e.get("intents") or {}).items():
            if self.reset_at.get(("output", output), 0) > prepared.get("deploy_number", self.deploy_number):
                # Reset since it launched: what it meant to write was the old output's,
                # which owes no repair. Its files go, from where it wrote them.
                prefix = ((prepared.get("outputs") or {}).get(output) or {}).get("prefix")
                index = LayerState(prefix=prefix) if prefix else self.index(output, task["partition"])
                self.garbage.extend([index.path(name), self.event_counter] for name in delta_names(intent))
                continue
            intents = self.repairs.setdefault((output, task["partition"]), [])
            intents.append({**intent, "run": e["run"], "attempt": e["attempt"]})
        self._cleaned_up((task.get("cleanup") or {}).get("partition", task["partition"]), e, prepared)
        if launched is not None and not e.get("commit"):
            self._abandoned(task["partition"], e["attempt"], launched, at + float(e.get("late_writes") or 0))
        usage = (e.get("worker") or {}).get("usage") or {}
        summary = {
            "id": e["attempt"],
            "outcome": outcome,
            "started_at": e["started_at"],
            "finished_at": at,
            **history.phases(times, at),
            **{k: usage[k] for k in history.USAGE if usage.get(k) is not None},
            **execution,
        }
        if e.get("error"):
            summary["error"] = e["error"]
        if generation is not None:
            summary["generation"] = int(generation)
        commit = e.get("commit")
        if commit:
            summary["outputs"] = sorted(commit.get("heads", {}))
        if e.get("keys"):
            summary["keys"] = e["keys"]
        if found := history.batch(prepared.get("plans")):
            summary["batch"] = found
        self._tried(run, task, summary)
        if commit and outcome in ("canceled", "failed"):
            # A drained Each batch: what finished commits (docs/lifecycle.md §7).
            self._install(task, commit, e, prepared)
        if task["status"] in TERMINAL_TASK:
            # Its run was canceled while it ran. An attempt that was already
            # writing still commits: its data landed (§8).
            if outcome == "succeeded" and commit:
                self._install(task, commit, e, prepared)
            return task
        if outcome == "succeeded":
            self._install(task, commit or {}, e, prepared)
            if e.get("more"):
                self._ready(run, task, at)
            else:
                task["status"] = "succeeded"
                self._finished(run, task, "succeeded", e["attempt"], at)
        elif outcome == "skipped":
            self._install(task, commit or {}, e, prepared)
            if e.get("more"):  # a batch with nothing to load, its walk not done
                self._ready(run, task, at)
            else:
                task["status"] = "skipped"
                self._finished(run, task, "skipped", e["attempt"], at)
        elif outcome == "failed":
            failures = task["outcomes"].get("failed", 0)
            allowed = task["max_attempts"]
            transient = e.get("retry_for") is not None
            if transient:
                # A Transient error retries past `retries=`, for its `retry_for`
                # from the first one (docs/per-key-processing.md §8).
                since = task.setdefault("transient_since", at)
                transient = at < since + float(e["retry_for"])
            if e.get("retryable") and (failures < allowed or transient):
                self._ready(run, task, at, float(e.get("delay") or 0))
            else:
                task["status"] = "failed"
                if task["asset"] == CLEANUP:
                    # Failed for good: kept, shown with why, never run again until an
                    # operator clears it (`solera cleanups OUTPUT --clear`, K25).
                    self._stuck(task, prepared, str(e.get("error") or "failed"))
                self._finished(run, task, "failed", e["attempt"], at)
        elif outcome == "canceled" and commit:
            task["status"] = "canceled"  # a drained batch the user stopped: it does not resume
            self._finished(run, task, "canceled", e["attempt"], at)
        elif outcome == "canceled":
            self._ready(run, task, at)
        else:
            raise ValueError(f"unknown attempt outcome {outcome!r}")
        return task

    def _tried(self, run: dict, task: dict, summary: dict) -> None:
        """An ended attempt: its row goes to the history now, and its task
        keeps only what scheduling and the task's own row need — however
        many batches a task runs, it holds no list of them."""

        task["tries"] = task.get("tries", 0) + 1
        outcomes = task.setdefault("outcomes", {})
        outcomes[summary["outcome"]] = outcomes.get(summary["outcome"], 0) + 1
        task["duration"] = task.get("duration", 0.0) + history.span(summary)
        task.setdefault("first_at", summary["started_at"])
        task["last_at"] = summary["finished_at"]
        task["last"] = {k: summary[k] for k in ("id", "outcome", "error", "outputs") if k in summary}
        if summary.get("error"):
            task["error"] = summary["error"]
        if summary.get("executor"):
            task["executor"] = summary["executor"]
        self._record("attempts", history.attempt_row(run["id"], task, summary, task["tries"]))

    def _attempt_events(self, run: dict, task: dict, e: dict, launched: dict | None) -> dict[str, float]:
        """Record what the worker says of an ended attempt, then how it
        ended. The worker's clock is not the engine's: its times are kept
        between the moment it could have started and the attempt's end, in
        order. Returns when each event first happened."""

        tid, attempt, end = task["id"], e["attempt"], e["finished_at"]
        times = {"claimed": e["started_at"]}
        if launched is not None:
            times["launched"] = launched["at"]
            at = launched["at"]
            for event in (e.get("worker") or {}).get("events") or ():
                at = min(max(at, float(event["at"])), end)
                times.setdefault(event["type"], at)
                self._event(
                    run,
                    event["type"],
                    at,
                    tid,
                    attempt,
                    by="worker",
                    name=event.get("name"),
                    rows=event.get("rows"),
                )
        closing = e.get("end") or {"succeeded": "committed"}.get(e["outcome"], e["outcome"])
        self._event(run, closing, end, tid, attempt, reason=e.get("reason"))
        return times

    def _install(self, task: dict, commit: dict, e: dict, prepared: dict) -> None:
        """Install a commit: heads, key indexes, the partition's record — under the
        contract its attempt was launched with (`prepared`). An output whose
        ref's generation moved changed (docs/versions.md); each such version
        enters the history, with what it was built from (its `lineage`, with
        what its reads saw: `history.read_lineage`)."""

        if task["asset"] == CLEANUP:  # a cleanup task: its output life's leftovers are gone (K25)
            self.retired.pop(commit.get("cleaned"), None)
            return
        asset, partition, at = task["asset"], task["partition"], e["finished_at"]
        reads = history.read_lineage(prepared.get("lineage"), e.get("read"))
        contracts = prepared.get("outputs") or {}
        changed = []
        for name, head in commit.get("heads", {}).items():
            before, keys = self.heads.get((name, partition)), (commit.get("keys") or {}).get(name)
            prefix = contracts[name].get("prefix")  # where its delta files are
            if before is None or before["ref"].get("generation") != head["ref"].get("generation"):
                changed.append(name)
            if contracts[name]["contract"]["writes"] == "immutable":
                self._superseded(name, partition, before, head, keys, prefix)
            self.heads[(name, partition)] = {**head, "run": e["run"], "attempt": e["attempt"], "at": at}
            self._commit_keys(name, partition, keys, prefix, head["ref"].get("generation"))
            if name in commit.get("repaired", ()):
                # The commit's delta took in what the dead attempts left (§8):
                # their intent files are no longer needed.
                index = self.index(name, partition)
                for intent in self.repairs.pop((name, partition), ()):
                    self.garbage.extend(
                        [index.path(name), self.event_counter] for name in delta_names(intent)
                    )
        record = self._partition(asset, partition)
        for field in ("definition", "config", "context"):  # what it was made under: staleness compares
            if field in commit:
                record[field] = commit[field]
        if "cursor" in commit:
            if commit["cursor"] is None:
                record.pop("cursor", None)
            else:
                record["cursor"] = commit["cursor"]
        for input, ops in (commit.get("observed") or {}).items():
            records = record.setdefault("observed", {})
            if isinstance(ops, dict):  # an unkeyed input: the last commit it read
                records[input] = ops
            else:
                observed.apply(records.setdefault(input, {}), ops)  # a new record's begin with its reset
            if not self._subscribed(asset, input, records[input]):  # an input removed while it ran
                del records[input]
        for input, progress in (commit.get("progress") or {}).items():  # the run's walk (D155)
            task.setdefault("progress", {})[input] = progress
        if "outcomes" in commit:
            self._stored(asset, partition, commit["outcomes"])
        for row in commit.get("key_outcomes") or ():
            self._record(
                "key_outcomes",
                {
                    **row,
                    "run": e["run"],
                    "attempt": e["attempt"],
                    "asset": asset,
                    "partition": partition,
                    "at": at,
                },
            )
        for name in changed:
            head = self.heads[(name, partition)]
            self._record(
                "commits",
                history.commit_row(
                    name,
                    asset,
                    partition,
                    head,
                    keys=(commit.get("keys") or {}).get(name),
                    key_count=self.key_count(name, partition),
                    rows=(commit.get("rows") or {}).get(name),
                    metadata=(commit.get("metadata") or {}).get(name),
                    final=commit.get("final"),
                ),
            )
            for row in history.lineage(name, partition, head, reads):
                self._record("lineage", row)
        self._pend_onchange(asset, partition, changed)

    def _stored(self, asset: str, partition: str, f: dict) -> None:
        """A per-key batch's commit to its stored outcome: the stored outcomes'
        delta, and the counts, bounds and retry-pass state the engine worked
        out from it (docs/per-key-processing.md §9)."""

        record = self._partition(asset, partition).setdefault("outcomes", {"commit_number": -1, "forced": {}})
        if f.get("start_over"):  # a start-over's stored outcomes start over (K47): the batch's alone
            self._drop_outcomes(asset, partition)
            record = self._partition(asset, partition)["outcomes"] = {
                "commit_number": record["commit_number"],
                "forced": record.get("forced") or {},
            }
        keys = f.get("keys") or {}
        if delta_names(keys):
            name = f"@{asset}"
            index = self.index(name, partition).committed(f["commit_number"], DeltaFiles.from_json(keys))
            self.indexes[(name, partition)] = index
            record["commit_number"] = f["commit_number"]
        for field in ("counts", "due", "deploy_min", "retry", "passes", "done_forced", "last"):
            if field in f:
                record[field] = f[field]

    def _drop_outcomes(self, asset: str, partition: str) -> None:
        """A per-key partition's stored outcomes go: their index's files become
        garbage, and the next write starts a new index (a new life)."""

        index = self.indexes.pop((f"@{asset}", partition), None)
        if index is not None:
            self.garbage.extend([index.path(name), self.event_counter] for name in sorted(index.referenced()))
        self._partition(asset, partition).pop("outcomes", None)

    def _on_KeysRetryRequested(self, e):
        """`solera retry ASSET --failed …`: a forced request, identified by its
        event counter, for each class it names
        (docs/per-key-processing.md §9)."""

        for partition, record in self.partitions.of(e["asset"]).items():
            if "outcomes" in record and e.get("partition") in (None, partition):
                for name in e["classes"]:
                    record["outcomes"].setdefault("forced", {})[name] = self.event_counter

    def _pend_onchange(self, asset: str | None, partition: str, changed: list[str]) -> None:
        if not changed:
            return
        for auto in self.automations.values():
            trigger = auto["trigger"]
            if trigger["kind"] != "onchange" or not auto["enabled"]:
                continue
            watched = set(auto.get("watched") or trigger.get("outputs") or [])
            if watched & set(changed):
                entry = [asset, partition]
                if entry not in auto["pending"]:
                    auto["pending"].append(entry)

    def _on_FiringsOwed(self, e):
        """What a deploy left each `OnChange` automation owing: a firing per
        (asset, partition), as a change of the asset's own (`Engine._owed_firings`)."""

        for name, due in e["owed"].items():
            auto = self.automations.get(name)
            for entry in due if auto is not None else ():
                if entry not in auto["pending"]:
                    auto["pending"].append(entry)

    def _ready(self, run: dict, task: dict, at: float, delay: float = 0.0) -> None:
        """Queue a task to run from `at + delay`: its wait starts then,
        unless its run is paused. A task whose asset left the project while
        its attempt ran — a retry, a next batch — is retired instead."""

        if (
            self.manifest is not None
            and task["asset"] not in self.manifest["assets"]
            and task["asset"] != CLEANUP
        ):
            self._retire(run, task, at)
            return
        due = at + delay
        task["status"] = "queued"
        task["ready_at"] = due
        task["queued_at"] = None if run.get("paused") else due
        self.queue[task["id"]] = due
        if delay:
            self._event(run, "retry_scheduled", at, task["id"], until=due)
        else:
            self._event(run, "ready", at, task["id"])

    @staticmethod
    def _stop_clock(task: dict, at: float) -> None:
        """A task stops waiting to run: claimed, paused or canceled."""

        if task.get("queued_at") is not None:
            task["wait"] += max(0.0, at - task["queued_at"])
            task["queued_at"] = None

    def _event(self, run: dict, type_: str, at: float, task=None, attempt=None, *, by="engine", **fields):
        """Append to the run's timeline (§7): `run_timeline` rows, in order."""

        run["events"] += 1
        row = {"run": run["id"], "n": run["events"], "at": at, "type": type_, "task": task}
        self._record("run_timeline", {**row, "attempt": attempt, "by": by, **fields})

    def _finished(self, run: dict, task: dict, outcome: str, attempt, at, *, roll_up: bool = True) -> None:
        """A terminal transition: outcome, pending index, dependents, run roll-up."""

        tid = task["id"]
        self.queue.pop(tid, None)
        bucket = self.pending.get((task["asset"], task["partition"]))
        if bucket:
            bucket.discard(tid)
            if not bucket:
                del self.pending[(task["asset"], task["partition"])]
        self._ended(task, outcome, run["id"], attempt, at)
        self._event(run, outcome, at, tid)
        stats = self.run_left.setdefault(run["id"], [0, 0])
        if roll_up:
            stats[0] -= 1
        stats[1] += outcome in BAD_OUTCOME
        for dep_id in sorted(self.dependents.pop(tid, ())):
            counter = self.unfinished.get(dep_id)
            dep = run["tasks"].get(dep_id)
            if counter is None or dep is None:
                continue
            counter["left"] -= 1
            counter["bad"] += outcome in BAD_OUTCOME
            if counter["left"] > 0:
                continue
            del self.unfinished[dep_id]
            if dep["status"] != "waiting":
                continue
            if counter["bad"]:
                dep["status"] = "blocked"
                self._finished(run, dep, "blocked", None, at, roll_up=roll_up)
            else:
                self._ready(run, dep, at)
        if roll_up:
            self._roll_up(run["id"], at)

    def _roll_up(self, run_id: str, at: float) -> None:
        run = self.runs[run_id]
        if run["status"] in TERMINAL_RUN:
            return
        left, bad = self.run_left.get(run_id, [0, 0])
        run["updated_at"] = at
        if left <= 0:
            run["status"] = "failed" if bad else "succeeded"
            self.archivable.add(run_id)
            self._event(run, run["status"], at)
        else:
            run["status"] = "running"

    def _commit_keys(
        self, output: str, partition: str, keys: dict | None, prefix: str | None, generation: int | None
    ) -> None:
        """Add a commit's delta to the output's key index (§6), as its layer,
        at the generation its writes carried: even an empty one, so the layers
        keep no holes. An output's first index starts
        where its attempt wrote them, `prefix` — under the name it launched
        with, when a rename came since. The head carries the index's live key
        count."""

        if keys is not None:
            index = self.index(output, partition)
            if (output, partition) not in self.indexes and prefix is not None:
                index = LayerState(prefix=prefix, life=str(self.event_counter))
            self.indexes[(output, partition)] = index.committed(
                keys["commit_number"], _delta(keys, generation)
            )

    # -- cleanup of immutable stores (docs/lifecycle.md §9.8) -----------------------

    def immutable(self, output: str) -> bool:
        manifest = self.manifest or {}
        record = (manifest.get("outputs") or {}).get(output) or {}
        return ((manifest.get("stores") or {}).get(record.get("store")) or {}).get("writes") == "immutable"

    def cleanup_reads(self) -> set[str]:
        """The delta files cleanup still reads (they name what their changes
        replaced): kept while queued, and while a live attempt holds them in
        its spec — a step acknowledged by another meanwhile is still being
        read — even once the index lets go of them."""

        pending = _delta_files(d for queue in self.cleaning.values() for d in queue)
        return pending.union(*(claim.get("cleanups") or () for claim in self.claims.values()))

    def _collect(self, output: str, partition: str, entry: dict) -> None:
        """Queue cleanup let go of now: `n`, the event counter, is
        when it may be collected; `id`, unique in its partition, is what an
        attempt acknowledges — one event can let go of several entries."""

        entries = self.cleanups.setdefault((output, partition), [])
        ordinal = sum(1 for d in entries if d["n"] == self.event_counter)
        entries.append({"n": self.event_counter, "id": f"{self.event_counter}.{ordinal}", **entry})

    def _superseded(
        self, output: str, partition: str, before: dict | None, head: dict, keys: dict | None, prefix=None
    ) -> None:
        """What a commit on an immutable store let go of: the generations its
        delta's updates and removes replaced (its delta queued for the
        partition's cleanup cursor), a value's previous object, or — when an
        append output starts over — its earlier commits."""

        delta = DeltaFiles.from_json(keys) if keys else None
        if delta is not None and delta.part.entries > delta.added:  # an update or a remove
            index = self.index(output, partition)
            self.cleaning.setdefault((output, partition), []).append(
                {
                    "n": self.event_counter,
                    "life": index.life,
                    "commit": int(head["commit_number"]),
                    "generation": delta.generation,
                    "prefix": prefix or index.prefix,
                    "files": [f.name for f in delta.part.files],
                }
            )
        old, new = (
            ((before or {}).get("ref") or {}).get("handle") or {},
            (head.get("ref") or {}).get("handle") or {},
        )
        if old.get("mode") in ("value", "set") and old.get("path") != new.get("path"):
            self._collect(
                output, partition, {"kind": "version", "generation": int(before["ref"]["generation"])}
            )
        if old.get("mode") == "commits" and new.get("mode") == "commits":
            first, last = old["commits"]
            if int(new["commits"][0]) > int(first):
                self._collect(
                    output,
                    partition,
                    {"kind": "commits", "from": int(first), "to": int(new["commits"][0]) - 1},
                )

    def _abandoned(self, partition: str, attempt: str, launched: dict, after: float) -> None:
        """An attempt that ended without committing: whatever it wrote on an
        immutable store carries its generation, which no other attempt uses.
        One cleanup takes it all, run late (the sweep, §9.8): due `after`, its
        end plus the asset's timeout and cancel grace, by when a worker that
        outlives it and honours cancellation has written its last."""

        for name, info in (launched["prepared"].get("outputs") or {}).items():
            if info["contract"]["writes"] == "immutable":
                entry = {
                    "kind": "abandoned",
                    "attempt": attempt,
                    "generation": launched["generation"],
                    "after": after,
                }
                self._collect(name, partition, entry)

    def _cleaned_up(self, partition: str, e: dict, prepared: dict | None = None) -> None:
        """A cleanup task cleaned up a partition: its entries go, and the index-side
        files they were read from become garbage themselves. An entry whose
        names it could not read counts a miss; at `STUCK_AFTER` it is
        `stuck`: kept, and shown, but no longer handed out."""

        if e.get("commit") is not None:  # a cleanup task that reported nothing of an entry it was handed
            handed = ((prepared or {}).get("cleanup") or {}).get("outputs") or {}
            for output, info in handed.items():
                told = {
                    *((e.get("cleaned_up") or {}).get(output) or ()),
                    *((e.get("cleanup_unresolved") or {}).get(output) or ()),
                }
                silent = [d["id"] for d in info.get("cleanup") or () if d["id"] not in told]
                if silent:
                    e = {
                        **e,
                        "cleanup_unresolved": {
                            **(e.get("cleanup_unresolved") or {}),
                            output: [*((e.get("cleanup_unresolved") or {}).get(output) or ()), *silent],
                        },
                    }
        for output, done in (e.get("cleaned_up") or {}).items():
            self._drop_cleanups(output, partition, done)
        for output, to in (e.get("cleaned_to") or {}).items():  # a step: the cursor moves past `to`
            left = [d for d in self.cleaning.get((output, partition), []) if d["n"] > int(to)]
            if left:
                self.cleaning[(output, partition)] = left
            else:
                self.cleaning.pop((output, partition), None)
        for output, missed in (e.get("cleanup_unresolved") or {}).items():
            for d in self.cleanups.get((output, partition), []):
                if d["id"] in missed:
                    d["misses"] = d.get("misses", 0) + 1
                    if d["misses"] >= STUCK_AFTER:
                        d["stuck"] = True

    def _on_RepairRunSubmitted(self, e):
        """The repair clock ran a partition again for its repair: its intents count
        the run, so the clock stops after `REPAIR_RUNS` (`Engine._repair_tick`)."""

        runs, _ = self.repair_runs(e["output"], e["partition"])
        for intent in self.repairs.get((e["output"], e["partition"]), ()):
            intent["runs"], intent["tried_at"] = runs + 1, e["at"]

    def _stuck(self, task: dict, prepared: dict, why: str) -> None:
        """A cleanup task out of tries: its output life, or the entries it was
        handed, are `stuck` — kept, shown, never handed out again."""

        cleanup = task.get("cleanup") or {}
        if cleanup.get("life") in self.retired:
            self.retired[cleanup["life"]]["stuck"] = why
        handed = (prepared.get("cleanup") or {}).get("outputs") or {}
        for output, info in handed.items():
            ids = {d["id"] for d in info.get("cleanup") or ()}
            for d in self.cleanups.get((output, cleanup.get("partition")), []):
                if d["id"] in ids:
                    d["stuck"] = True
            step = {d["n"] for d in (info.get("deltas") or {}).get("deltas") or ()}  # a cursor step's
            for d in self.cleaning.get((output, cleanup.get("partition")), []):
                if d["n"] in step:
                    d["stuck"] = True

    def _drop_cleanups(self, output: str, partition: str, ids) -> None:
        left = [d for d in self.cleanups.get((output, partition), []) if d["id"] not in set(ids)]
        if left:
            self.cleanups[(output, partition)] = left
        else:
            self.cleanups.pop((output, partition), None)

    def _on_CleanupsCleared(self, e):
        """An operator gave up on stuck entries, a stuck cursor step's deltas
        (`delta:{n}`) included: their objects stay."""

        self._drop_cleanups(e["output"], e["partition"], e["ids"])
        key, gone = (e["output"], e["partition"]), set(e["ids"])
        left = [d for d in self.cleaning.get(key, []) if f"delta:{d['n']}" not in gone]
        if left:
            self.cleaning[key] = left
        else:
            self.cleaning.pop(key, None)
        for entry_id in e.get("retired") or ():  # a stuck cleanup task's output life (K25)
            self.retired.pop(entry_id, None)

    def _replace_index(self, key: tuple, index: LayerState) -> None:
        """Swap in a new index state; files it no longer references await deletion."""

        before = self.indexes[key]
        self.indexes[key] = index
        for name in sorted(before.referenced() - index.referenced()):
            self.garbage.append([before.path(name), self.event_counter])

    def merge_record(self, key: tuple, life: str) -> dict:
        """What merges of this index's life `life` tried: another life's count
        for nothing."""

        rec = self.merges.get(key)
        return rec if rec is not None and rec["life"] == life else {"life": life, "attempts": {}}

    def _merge_record(self, key: tuple, life: str) -> dict:
        rec = self.merge_record(key, life)
        self.merges[key] = rec
        return rec

    def _on_MergeAttempted(self, e):
        """A merge of these input files is about to upload: counted before it
        does, whatever comes of it."""

        rec = self._merge_record((e["output"], e["partition"]), e["life"])
        rec["attempts"][e["inputs"]] = rec["attempts"].get(e["inputs"], 0) + 1

    def _prune_merges(self, key: tuple) -> None:
        """Forget what merges tried of files the index no longer holds, and
        another life's record."""

        rec, index = self.merges.get(key), self.indexes.get(key)
        if rec is None:
            return
        if index is None or rec["life"] != index.life:
            del self.merges[key]
            return
        held = {x.id for x in index.layers}
        rec["attempts"] = {
            k: n for k, n in rec["attempts"].items() if all(i in held for i in k.split("|", 1)[-1].split(","))
        }

    def _on_IndexMerged(self, e):
        """A merge of adjacent layers published: installed if the index is the
        life it was planned against and still holds exactly its inputs; else
        refused, its output let go of (docs/key-index-design.md § Lifecycles).
        Names never repeat across lives (each carries the life, an epoch and
        a unique id), and the life check refuses an old life's merge of
        empty layers, whose ids could match."""

        key = (e["output"], e["partition"])
        layer = Layer.from_json(e["layer"])
        index = self.indexes.get(key)
        if index is None or index.life != e["life"] or not index.holds(e["ids"]):
            prefix = e.get("prefix") or (index.prefix if index is not None else "")
            self.garbage.extend([f"{prefix}{name}", self.event_counter] for name in layer.names())
            self._prune_merges(key)
            return
        self._replace_index(key, index.merged(e["ids"], layer))
        self._prune_merges(key)

    def _on_IndexCut(self, e):
        """The index's cut rose: the oldest P its readers hold (in-flight
        batches' heads included), so merges may drop flips at or below it."""

        key = (e["output"], e["partition"])
        index = self.indexes.get(key)
        if index is not None and index.life == e["life"]:
            self.indexes[key] = index.with_cut(int(e["cut"]))

    def _on_OrphansFound(self, e):
        """Index files nothing names (`Upkeep.collect_orphans`): garbage now,
        deleted once no pin predates this event."""

        known = {g[0] for g in self.garbage}
        self.garbage.extend([p, self.event_counter] for p in e["paths"] if p not in known)

    def _on_FilesCleanedUp(self, e):
        gone = set(e["paths"])
        self.garbage = [g for g in self.garbage if g[0] not in gone]

    def _on_SourceCommitted(self, e):
        before = self.heads.get((e["source"], ""))
        head = e["head"]
        self.heads[(e["source"], "")] = {**head, "at": e["at"], "n": self.event_counter}
        self._commit_keys(e["source"], "", e.get("keys"), None, head["ref"].get("generation"))
        run = e.get("run")
        if run is not None:
            self._record("runs", history.source_run_row(run, e["at"]))
            self._record(
                "run_timeline",
                {
                    "run": run["id"],
                    "n": 1,
                    "at": e["at"],
                    "type": "committed",
                    "by": run.get("by"),
                    "name": e["source"],
                },
            )
            installed = {**self.heads[(e["source"], "")], "run": run["id"], "attempt": None}
            version = {"version": head["version"]} if head.get("version") is not None else None
            self._record(
                "commits",
                history.commit_row(
                    e["source"],
                    None,
                    "",
                    installed,
                    keys=e.get("keys"),
                    key_count=self.key_count(e["source"], ""),
                    listed=run,
                    metadata=version,
                ),
            )
        if before is None or before["ref"].get("generation") != head["ref"].get("generation"):
            self._pend_onchange(None, "", [e["source"]])

    def _on_SensorAdvanced(self, e):
        self.sensors[e["sensor"]] = {"cursor": e.get("cursor"), "accepted": e["accepted"]}

    def _on_AutomationChanged(self, e):
        auto = self.automations.get(e["name"])
        if auto is not None:
            auto["enabled"] = bool(e["enabled"])

    def _on_AutomationFired(self, e):
        auto = self.automations.get(e["name"])
        if auto is None:
            return
        auto["last_fired"] = e["at"]
        if e.get("run"):
            auto["last_run"] = e["run"]
        if "deploy" in e:
            auto["last_deploy"] = e["deploy"]
        consumed = e.get("consumed") or []
        auto["pending"] = [p for p in auto["pending"] if p not in consumed]

    def policy(self, asset: str | None) -> dict | None:
        """The retention policy that applies to an asset: its own, else the
        project's; `None` keeps everything (§11)."""

        manifest = self.manifest or {}
        own = ((manifest.get("assets") or {}).get(asset) or {}).get("retention") if asset else None
        policy = own or manifest.get("retention")
        if not policy or policy.get("forever"):
            return None
        return policy

    def _on_RunArchived(self, e):
        run = self.runs.get(e["run"])
        if run is None:
            return
        self._unindex_run(e["run"])
        del self.runs[e["run"]]
        for table, rows in history.run_rows(run).items():
            for row in rows:
                self._record(table, row)

    # -- history (§7) ------------------------------------------------------------------

    def _record(self, table: str, row: dict) -> None:
        self.history.append(table, row)

    def _on_HistoryFlushed(self, e):
        self.history.flushed(e["files"], e["upto"])

    def _on_HistoryCompacted(self, e):
        self.garbage.extend([path, self.event_counter] for path in self.history.compacted(e["changes"]))

    def _on_RunsDeleted(self, e):
        """Runs retire for good: their history goes, and their directories
        are deleted only from here on — `RunsPurged` once they are."""

        self.history.forget(set(e["runs"]), e["at"])
        self.deleted.extend(r for r in e.get("files") or () if r not in self.deleted)

    def _on_RunsPurged(self, e):
        gone = set(e["runs"])
        self.deleted = [r for r in self.deleted if r not in gone]


def declaration(manifest: dict, asset: str, homes: dict | None = None) -> dict:
    """An asset's definition, in its one canonical form: what its
    outputs are built under. Its version; its inputs and deps — what each
    reads, by that output's home, so a rename changes nothing; how (kind,
    flags, patterns, batch size); and its store's version; its outputs'
    declarations (key, incremental, migrations, config) and store versions,
    not their names, nor which store holds them — that is the reset rule's
    (§2). A difference is an asset change; its docs, automations,
    placement, retries, timeout, tags or retention are not. What inputs
    observe is made under it without patterns and batch size
    (`Engine._definition`): new patterns are compared, and a batch size
    only pages."""

    homes = homes or {}
    entry, stores, outputs = manifest["assets"][asset], manifest["stores"], manifest["outputs"]

    def read(name: str) -> dict:
        return {"output": homes.get(name, name), "store_version": stores[outputs[name]["store"]]["version"]}

    inputs = {
        param: {**{k: v for k, v in i.items() if k not in ("meta", "output")}, **read(i["output"])}
        for param, i in entry["inputs"].items()
    }
    deps = sorted((read(d) for d in entry["deps"]), key=lambda d: d["output"])
    written = [
        {
            **{k: v for k, v in o.items() if k not in ("name", "store")},
            "store_version": stores[o["store"]]["version"],
        }
        for o in entry["outputs"]
    ]
    return {
        "version": entry["version"],
        "inputs": inputs,
        "deps": deps,
        "deps_all_partitions": sorted(homes.get(d, d) for d in entry.get("deps_all_partitions") or ()),
        "outputs": written,
    }


def reads(plans: dict) -> list[tuple]:
    """What an attempt's plans read of their upstream indexes until the
    claim goes: `(output, partition, head)`, the head a keyed batch classes
    its keys at and its commit records."""

    return [
        (p["output"], p["upstream_partition"], p["head"])
        for p in plans.values()
        if p and p["kind"] == "observed"
    ]
