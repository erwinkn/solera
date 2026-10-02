"""The engine's state (docs/object-store-state.md §4, §5): what it holds, the
events that change it, and `apply`.

The model is plain data changed only by `apply(event)`, so replaying the
journal reproduces it exactly. It has three layers:

- **Durable**: the project, heads, key indexes, cursors, watermarks,
  per-scope outcomes, automation state, active runs (tasks nested inside,
  each launched attempt on its task), unsettled outputs, idempotency
  receipts, files awaiting deletion, and the run history's files and the
  rows not yet flushed to them (§7). `snapshot()` serializes exactly this,
  and `restore()` loads it.
- **Derived**: the ready queue, pending-per-scope, dependency counters, run
  roll-ups, and the claims, scope locks and pool work of launched attempts.
  Rebuilt by `restore()`, maintained by `apply()`.
- **Memory only**: the claim an attempt holds while it prepares, before it
  is launched (a restart simply dispatches its task again).

Events carry every timestamp they need; `apply` never reads a clock.
`applied` counts the events applied: the model's own clock, the same in
every engine that replays the journal. What must not be compared across
hosts' wall clocks is ordered by it: an attempt pins the files it may read
at its claim's position, and a file let go of at a later one waits for it.
"""

from __future__ import annotations

import copy
import math

from solera.keys.index import DeltaFiles, FileInfo, IndexState, index_prefix

from . import history
from .lake import LakeState

TERMINAL_TASK = frozenset({"succeeded", "skipped", "failed", "blocked", "canceled"})
TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
BAD_OUTCOME = frozenset({"failed", "blocked", "canceled"})
MAX_RECEIPTS = 10_000  # idempotency receipts kept for replayed submissions
STUCK_AFTER = 3  # misses before a discard entry is stuck (docs/lifecycle.md §9.8)


def delta_reads(plans: dict) -> list[tuple]:
    """The delta logs an attempt's Incremental plans read: `(output, scope,
    first batch)` — kept until its claim goes (§6)."""

    return [(p["output"], p["up"], p["from"]) for p in plans.values() if p and "from" in p]


def _nest(flat: dict, depth: int) -> dict:
    """{(a, b): v} -> {a: {b: v}} (depth 2), {(a, b, c): v} -> {a: {b: {c: v}}} (depth 3)."""

    out: dict = {}
    for key, value in flat.items():
        node = out
        for part in key[:-1]:
            node = node.setdefault(part, {})
        node[key[-1]] = value
    return out


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


class Model:
    def __init__(self):
        self.restore(None)

    # -- snapshot / restore ---------------------------------------------------------------

    def snapshot(self) -> dict:
        return copy.deepcopy(
            {
                "applied": self.applied,
                "writer": self.writer,
                "revision": self.revision,
                "epoch": self.epoch,
                "manifest": self.manifest,
                "project": self.project,
                "heads": _nest(self.heads, 2),
                "indexes": _nest({k: v.to_json() for k, v in self.indexes.items()}, 2),
                "garbage": self.garbage,
                "retired": self.retired,
                "cursors": _nest(self.cursors, 2),
                "watermarks": _nest(self.watermarks, 3),
                "outcomes": _nest(self.outcomes, 2),
                "unsettled": _nest(self.unsettled, 2),
                "holds": _nest(self.holds, 2),
                "discards": _nest(self.discards, 2),
                "failures": _nest(self.failures, 2),
                "automations": self.automations,
                "sensors": self.sensors,
                "runs": self.runs,
                "receipts": list(self.receipts.items()),
                "history": self.history.to_json(),
            }
        )

    def restore(self, snap: dict | None) -> None:
        snap = copy.deepcopy(snap) if snap else {}
        # durable
        self.applied: int = snap.get("applied") or 0
        self.writer = snap.get("writer")
        self.revision = snap.get("revision")
        # how many project revisions this namespace has served: the revision
        # epoch, which gives failed keys one try per deploy (per-key §13)
        self.epoch: int = snap.get("epoch") or 0
        self.manifest = snap.get("manifest")
        self.project = snap.get("project")
        self.heads: dict[tuple, dict] = _flatten(snap.get("heads"), 2)
        self.indexes: dict[tuple, IndexState] = {
            k: IndexState.from_json(v) for k, v in _flatten(snap.get("indexes"), 2).items()
        }
        # [path, n]: files nothing references since the n-th event applied
        self.garbage: list[list] = snap.get("garbage") or []
        # deleted runs whose directories are still to be deleted (§11)
        self.retired: list[str] = snap.get("retired") or []
        self.cursors: dict[tuple, object] = _flatten(snap.get("cursors"), 2)
        self.watermarks: dict[tuple, dict] = _flatten(snap.get("watermarks"), 3)
        self.outcomes: dict[tuple, dict] = _flatten(snap.get("outcomes"), 2)
        # (output, scope) -> intents of attempts that died while writing it (§8)
        self.unsettled: dict[tuple, list] = _flatten(snap.get("unsettled"), 2)
        # (asset, scope) -> the ended attempt whose writes may still land on an
        # overwrite store: no attempt runs there until it is released (docs/lifecycle.md §9.9)
        self.holds: dict[tuple, dict] = _flatten(snap.get("holds"), 2)
        # (output, scope) -> data garbage of an immutable store, each entry at the
        # event position that let go of it: for the scope's next attempt to discard
        # once no reader pins it (docs/lifecycle.md §9.8)
        self.discards: dict[tuple, list] = _flatten(snap.get("discards"), 2)
        # (asset, scope) -> an Each asset's failure record (docs/per-key-processing.md §9):
        # its index lives in `indexes` under ("@asset", scope)
        self.failures: dict[tuple, dict] = _flatten(snap.get("failures"), 2)
        self.automations: dict[str, dict] = snap.get("automations") or {}
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
        self.locks: dict[tuple, str] = {}  # (asset, scope) -> attempt id
        self.pool: dict[str, dict] = {}  # attempt id -> pool work
        # sensor -> the tick dispatched and not yet decided: {tick, cursor, snapshot, pin, ...}
        self.ticks: dict[str, dict] = {}
        self._reindex()

    def _reindex(self) -> None:
        """Rebuild the derived indexes from the durable runs and the manifest."""

        self._consumed = self._consumed_outputs(self.manifest)

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
            self.pending.setdefault((task["asset"], task["scope"]), set()).add(tid)
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
            bucket = self.pending.get((task["asset"], task["scope"]))
            if bucket:
                bucket.discard(tid)
                if not bucket:
                    del self.pending[(task["asset"], task["scope"])]
        self.run_left.pop(run_id, None)
        self.archivable.discard(run_id)

    @staticmethod
    def _consumed_outputs(manifest) -> set[str]:
        """Outputs some asset reads through an Incremental edge: only their
        indexes keep a delta log (§6)."""

        return {
            edge["output"]
            for asset in ((manifest or {}).get("assets") or {}).values()
            for edge in asset["inputs"].values()
            if edge["kind"] == "incremental"
        }

    # -- reads ---------------------------------------------------------------------------

    def task(self, task_id: str) -> dict | None:
        run_id = self.task_run.get(task_id)
        return self.runs[run_id]["tasks"][task_id] if run_id else None

    def status_of(self, task: dict) -> str:
        """The task's live status: its durable one, or its claim's."""

        claim = self.claims.get(task["id"])
        return claim["status"] if claim else task["status"]

    def index(self, output: str, scope: str) -> IndexState:
        """The output's key index, or an empty one where a new index would go."""

        found = self.indexes.get((output, scope))
        return found if found is not None else IndexState(prefix=index_prefix(output, scope))

    def heads_of(self, output: str) -> list[tuple[str, dict]]:
        return sorted((scope, head) for (o, scope), head in self.heads.items() if o == output)

    def outcomes_of(self, asset: str) -> dict[str, dict]:
        return {scope: rec for (a, scope), rec in self.outcomes.items() if a == asset}

    def pending_scopes(self, asset: str) -> set[str]:
        return {scope for (a, scope), ids in self.pending.items() if a == asset and ids}

    def is_pending(self, asset: str, scope: str) -> bool:
        return bool(self.pending.get((asset, scope)))

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
            "pin": self.applied,
            "status": "running",
        }
        self.attempts[attempt] = task_id
        self.locks[(task["asset"], task["scope"])] = attempt
        self.queue.pop(task_id, None)

    def pin_floor(self, but: str | None = None) -> float:
        """The oldest reader pin (docs/lifecycle.md §9.8): of the attempts
        claimed (but attempt `but`), the delta windows delivered over several
        attempts, and the sensor ticks in flight. What was let go of at or
        before it is read by no one."""

        pins = [c["pin"] for c in self.claims.values() if c["attempt"] != but and "pin" in c]
        pins += [wm["pin"] for wm in self.watermarks.values() if wm.get("pin") is not None]
        pins += [t["pin"] for t in self.ticks.values()]
        return min(pins, default=math.inf)

    def release(self, task_id: str, attempt: str) -> None:
        """Drop the memory-only claim of an attempt that was never launched."""

        claim = self.claims.get(task_id)
        if claim is not None and claim["attempt"] == attempt and not claim.get("launched"):
            self._release_claim(task_id, attempt)

    def _hold(self, task: dict) -> None:
        """The claim, scope lock and pool work of a task's launched attempt."""

        launched = task["launched"]
        attempt = launched["attempt"]
        pool = launched.get("pool")
        status = "running" if pool is None else "claimable"
        self.claims[task["id"]] = {
            "attempt": attempt,
            "started_at": launched["started_at"],
            "pin": launched["pin"],
            "status": status,
            "launched": True,
            "reads": delta_reads(launched["prepared"].get("plans") or {}),
        }
        self.attempts[attempt] = task["id"]
        self.locks[(task["asset"], task["scope"])] = attempt
        self.queue.pop(task["id"], None)
        if pool is not None:  # discoverable until it ends; its claim is the worker's (§10)
            self.pool[attempt] = {
                "attempt": attempt,
                "task": task["id"],
                "run": self.task_run[task["id"]],
                "asset": task["asset"],
                "scope": task["scope"],
                "pool": pool["name"],
                "needs": pool.get("needs") or {},
                "created_at": launched["at"],
            }

    def claimed(self, attempt: str) -> dict | None:
        """The live claim behind an attempt, if it still holds its scope."""

        task_id = self.attempts.get(attempt)
        claim = self.claims.get(task_id) if task_id else None
        if claim is None or claim["attempt"] != attempt:
            return None
        task = self.task(task_id)
        if task is None or self.locks.get((task["asset"], task["scope"])) != attempt:
            return None
        return claim

    def _release_claim(self, task_id: str, attempt: str | None = None) -> None:
        claim = self.claims.get(task_id)
        if claim is None or (attempt is not None and claim["attempt"] != attempt):
            return
        del self.claims[task_id]
        self.attempts.pop(claim["attempt"], None)
        self.pool.pop(claim["attempt"], None)
        for key, holder in list(self.locks.items()):
            if holder == claim["attempt"]:
                del self.locks[key]

    # -- apply -------------------------------------------------------------------------

    def apply(self, event: dict) -> None:
        self.applied += 1
        getattr(self, f"_on_{event['type']}")(event)

    def _on_WriterStarted(self, e):
        self.writer = e.get("writer")

    def _on_ProjectRegistered(self, e):
        manifest = e["manifest"]
        if e["revision"] != self.revision:
            self.epoch += 1
        self.revision, self.manifest, self.project = e["revision"], manifest, e.get("project")
        self._consumed = self._consumed_outputs(manifest)
        renamed = self._apply_aliases(manifest)
        automations = {}
        for name, auto in manifest["automations"].items():
            existing = self.automations.get(name)
            if existing is None:
                owner, _, rest = name.partition(".")
                for old in renamed.get(owner, ()):
                    existing = self.automations.get(f"{old}.{rest}")
                    if existing is not None:
                        break
            record = {**auto, "last_at": None, "last_run": None, "last_revision": None, "pending": []}
            if existing is not None:
                record["enabled"] = existing["enabled"]
                if existing["trigger"] == auto["trigger"]:
                    for field in ("last_at", "last_run", "last_revision", "pending"):
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
                    "complete": True,
                    "asset": None,
                    "version": None,
                    "n": self.applied,
                }

    def _apply_aliases(self, manifest) -> dict[str, list[str]]:
        """Move everything held under an asset's former names to its current
        one (§2): cursors, watermarks, outcomes, pending automation entries,
        holds on its scopes, and — for outputs named after the asset — heads,
        key indexes, unsettled intents and pending discards. A new name never
        releases a write domain. An index keeps its files where they are (its
        `prefix`). Returns `{asset: [aliases]}` for the automations to follow."""

        assets = {n: a for n, a in manifest["assets"].items() if a.get("aliases")}
        renamed = {}
        for name, info in assets.items():
            renamed[name] = [a for a in info["aliases"] if a not in manifest["assets"]]
        asset_map = {old: new for new, olds in renamed.items() for old in olds}
        if not asset_map:
            return renamed
        outputs = manifest["outputs"]
        output_map = {
            old: new
            for old, new in asset_map.items()
            if old not in outputs and (outputs.get(new) or {}).get("asset") == new
        }

        def move(table: dict, rename, position: int, merge=None):
            for key in [k for k in table if k[position] in rename]:
                target = (*key[:position], rename[key[position]], *key[position + 1 :])
                if target not in table:
                    table[target] = table.pop(key)
                elif merge is not None:  # lists: both names' entries are owed
                    table[target] = merge(table[target] + table.pop(key))

        move(self.heads, output_map, 0)
        move(self.indexes, output_map, 0)
        move(self.cursors, asset_map, 0)
        move(self.outcomes, asset_map, 0)
        move(self.watermarks, asset_map, 0)
        move(self.holds, asset_map, 0)
        move(self.unsettled, output_map, 0, merge=list)
        move(self.discards, output_map, 0, merge=lambda entries: sorted(entries, key=lambda d: d["n"]))
        for head in self.heads.values():
            if head.get("asset") in asset_map:
                head["asset"] = asset_map[head["asset"]]
        for wm in self.watermarks.values():
            if wm.get("output") in output_map:
                wm["output"] = output_map[wm["output"]]
        for auto in self.automations.values():
            auto["pending"] = [[asset_map.get(a, a), s] for a, s in auto.get("pending") or []]
        return renamed

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

    def _on_RunReopened(self, e):
        run = e["run"]
        # Its history rows describe how it ended, and it is running again.
        # Its events stay: the timeline goes on.
        self.history.forget({run["id"]}, e["at"], history.RUN_TABLES)
        if run["id"] in self.runs:
            self._unindex_run(run["id"])
        self.runs[run["id"]] = run
        self._index_run(run["id"], run)
        self._roll_up(run["id"], e["at"])

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
                self.outcomes[(task["asset"], task["scope"])] = {
                    "outcome": "canceled",
                    "run": run["id"],
                    "attempt": attempt,
                    "at": at,
                }
            # Claims go with their tasks, except those of launched attempts:
            # each is aborted, or — if it is already writing — waited for and
            # committed (§8).
            self._unindex_run(run["id"])
            self._index_run(run["id"], run)
        elif action == "retry":
            # Failed tasks run again; blocked ones wait on their dependencies again.
            self._event(run, "retried", at, by=by)
            for task in run["tasks"].values():
                if task["status"] == "failed":
                    task["retried"] = task.get("retried", 0) + 1
                    self._ready(run, task, at)
                elif task["status"] == "blocked":
                    task["status"] = "waiting"
            run["status"] = "running"
            run["paused"] = False
            self._unindex_run(run["id"])
            self._index_run(run["id"], run)
            for tid, counter in list(self.unfinished.items()):
                task = run["tasks"].get(tid)
                if task is None or task["status"] != "waiting" or counter["left"] > 0:
                    continue
                del self.unfinished[tid]
                if counter["bad"]:
                    task["status"] = "blocked"
                    self._finished(run, task, "blocked", None, at)
                else:
                    self._ready(run, task, at)
            self._roll_up(run["id"], at)

    def _on_TasksHeld(self, e):
        """Why tasks ready to run are not claimed (`[reason, name]`): the
        engine is full, their executor is, or another attempt holds their
        scope. Only a change of reason is an event."""

        for tid, held in sorted(e["held"].items()):
            task = self.task(tid)
            if task is None or task["status"] != "queued" or task.get("held") == held:
                continue
            task["held"] = held
            self._event(self.runs[task["run"]], "held", e["at"], tid, reason=held[0], name=held[1])

    def _on_EngineRestarted(self, e):
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
        launched = {k: e[k] for k in ("attempt", "started_at", "pin", "at", "execution", "prepared")}
        if e.get("pool"):
            launched["pool"] = e["pool"]
        task["launched"] = launched
        if task["status"] == "queued":
            task["status"] = "running"
        self._hold(task)
        self._claimed(run, task, e["attempt"], e["started_at"])
        self._event(run, "launched", e["at"], task["id"], e["attempt"], name=e["execution"]["executor"])

    def _on_ScopeReleased(self, e):
        """A held scope runs again: the grace passed, the writer's result
        established its completion, or an operator released it."""

        hold = self.holds.get((e["asset"], e["scope"]))
        if hold is None or hold["attempt"] != e["attempt"]:
            return
        del self.holds[(e["asset"], e["scope"])]
        run = self.runs.get(hold["run"])
        if run is not None:
            task = run["tasks"].get(f"{hold['run']}/{e['asset']}:{e['scope']}")
            self._event(run, "released", e["at"], task and task["id"], e["attempt"], reason=e["by"])

    def _on_AttemptPlaced(self, e):
        """Where a launched attempt runs: its placement handle (§10)."""

        task = self.task(self.attempts.get(e["attempt"], ""))
        launched = (task or {}).get("launched")
        if launched is not None and launched["attempt"] == e["attempt"]:
            launched["handle"] = e["handle"]

    def _on_AttemptFinished(self, e):
        run = self.runs.get(e["run"])
        task = run["tasks"].get(e["task"]) if run else None
        if task is None:
            return
        self._release_claim(task["id"], e["attempt"])
        outcome, at = e["outcome"], e["finished_at"]
        reads, execution = [], {}
        launched = task.get("launched")
        if (launched or {}).get("attempt") == e["attempt"]:
            del task["launched"]
            reads = launched["prepared"].get("lineage") or []
            execution = history.execution(launched["execution"])
            if task["status"] == "running":
                task["status"] = "queued"  # until the outcome below says otherwise
        else:
            launched = None
            self._claimed(run, task, e["attempt"], e["started_at"])
        times = self._attempt_events(run, task, e, launched)
        for output, intent in (e.get("unsettled") or {}).items():
            intents = self.unsettled.setdefault((output, task["scope"]), [])
            intents.append({**intent, "run": e["run"], "attempt": e["attempt"]})
        self._discarded(task["scope"], e)
        if launched is not None and not e.get("commit"):
            self._abandoned(task["scope"], e["attempt"], launched)
        if e.get("hold"):
            self.holds[(task["asset"], task["scope"])] = {
                "attempt": e["attempt"],
                "run": e["run"],
                "mode": e["hold"],
                "at": at,
            }
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
        commit = e.get("commit")
        if commit:
            summary["outputs"] = {
                name: h["ref"].get("version") for name, h in commit.get("heads", {}).items()
            }
        task["attempts"].append(summary)
        if e.get("keys"):
            summary["keys"] = e["keys"]
        if commit and outcome in ("canceled", "failed"):
            # A drained Each page: what finished commits (docs/lifecycle.md §7).
            self._install(task, commit, e, reads)
        if task["status"] in TERMINAL_TASK:
            # Its run was canceled while it ran. An attempt that was already
            # writing still commits: its data landed (§8).
            if outcome == "succeeded" and commit:
                self._install(task, commit, e, reads)
            return
        if outcome == "succeeded":
            self._install(task, commit or {}, e, reads)
            if e.get("more"):
                self._ready(run, task, at)
            else:
                task["status"] = "succeeded"
                self._finished(run, task, "succeeded", e["attempt"], at)
        elif outcome == "skipped":
            self._install(task, commit or {}, e, reads)
            task["status"] = "skipped"
            self._finished(run, task, "skipped", e["attempt"], at)
        elif outcome == "failed":
            failures = sum(1 for a in task["attempts"] if a["outcome"] == "failed")
            allowed = task["max_attempts"] * (1 + task.get("retried", 0))
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
                self._finished(run, task, "failed", e["attempt"], at)
        elif outcome == "canceled" and commit:
            task["status"] = "canceled"  # a drained page the user stopped: it does not resume
            self._finished(run, task, "canceled", e["attempt"], at)
        elif outcome == "canceled":
            self._ready(run, task, at)
        else:
            raise ValueError(f"unknown attempt outcome {outcome!r}")

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

    def _install(self, task: dict, commit: dict, e: dict, reads=()) -> None:
        """Install a commit: heads, key indexes, cursor, watermarks. Each
        output version it makes enters the history, with what it was built
        from (`reads`: `[output, scope, version, param]`)."""

        asset, scope, at = task["asset"], task["scope"], e["finished_at"]
        changed = []
        for name, head in commit.get("heads", {}).items():
            before = self.heads.get((name, scope))
            if before is None or before["ref"].get("version") != head["ref"].get("version"):
                changed.append(name)
            self._superseded(name, scope, before, head, (commit.get("keys") or {}).get(name))
            self.heads[(name, scope)] = {**head, "run": e["run"], "attempt": e["attempt"], "at": at}
            self._commit_keys(name, scope, (commit.get("keys") or {}).get(name))
            if name in commit.get("settled", ()):
                # The commit's delta took in what the dead attempts left (§8):
                # their intent files are no longer needed.
                index = self.index(name, scope)
                for intent in self.unsettled.pop((name, scope), ()):
                    self.garbage.extend([index.path(f["name"]), self.applied] for f in intent["files"])
        if "cursor" in commit:
            if commit["cursor"] is None:
                self.cursors.pop((asset, scope), None)
            else:
                self.cursors[(asset, scope)] = commit["cursor"]
        for edge, wm in commit.get("watermarks", {}).items():
            self.watermarks[(asset, edge, scope)] = wm
        if "failures" in commit:
            self._failures(asset, scope, commit["failures"])
        for row in commit.get("key_outcomes") or ():
            self._record(
                "key_outcomes",
                {**row, "run": e["run"], "attempt": e["attempt"], "asset": asset, "scope": scope, "at": at},
            )
        for name in changed:
            head = self.heads[(name, scope)]
            self._record(
                "materializations",
                history.materialization(
                    name,
                    asset,
                    scope,
                    head,
                    keys=(commit.get("keys") or {}).get(name),
                    rows=(commit.get("rows") or {}).get(name),
                    metadata=(commit.get("metadata") or {}).get(name),
                ),
            )
            for row in history.lineage(name, scope, head, reads):
                self._record("lineage", row)
        self._pend_onchange(asset, scope, changed)

    def _failures(self, asset: str, scope: str, f: dict) -> None:
        """An Each page's commit to its failure record: the failure index's
        delta, and the counts, bounds and retry-pass state the engine worked
        out from it (docs/per-key-processing.md §9)."""

        record = self.failures.setdefault((asset, scope), {"batch": -1, "forced": {}})
        keys = f.get("keys") or {}
        if keys.get("files"):
            name = f"@{asset}"
            index = self.index(name, scope).committed(f["batch"], DeltaFiles.from_json(keys), keep_log=False)
            self.indexes[(name, scope)] = index
            record["batch"] = f["batch"]
        for field in ("counts", "due", "epoch_min", "retry", "passes", "done_forced", "last"):
            if field in f:
                record[field] = f[field]

    def _on_KeysRetryRequested(self, e):
        """`solera retry ASSET --failed …`: a forced request, identified by its
        position in the event order, for each class it names
        (docs/per-key-processing.md §9)."""

        for (asset, scope), record in self.failures.items():
            if asset == e["asset"] and (e.get("scope") is None or e["scope"] == scope):
                for name in e["classes"]:
                    record.setdefault("forced", {})[name] = self.applied

    def _pend_onchange(self, asset: str | None, scope: str, changed: list[str]) -> None:
        if not changed:
            return
        for auto in self.automations.values():
            trigger = auto["trigger"]
            if trigger["kind"] != "onchange" or not auto["enabled"]:
                continue
            watched = set(auto.get("watched") or trigger.get("outputs") or [])
            if watched & set(changed):
                entry = [asset, scope]
                if entry not in auto["pending"]:
                    auto["pending"].append(entry)

    def _ready(self, run: dict, task: dict, at: float, delay: float = 0.0) -> None:
        """Queue a task to run from `at + delay`: its wait starts then,
        unless its run is paused."""

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
        """Append to the run's timeline (§7): `run_events` rows, in order."""

        run["events"] += 1
        row = {"run": run["id"], "n": run["events"], "at": at, "type": type_, "task": task}
        self._record("run_events", {**row, "attempt": attempt, "by": by, **fields})

    def _finished(self, run: dict, task: dict, outcome: str, attempt, at, *, roll_up: bool = True) -> None:
        """A terminal transition: outcome, pending index, dependents, run roll-up."""

        tid = task["id"]
        self.queue.pop(tid, None)
        bucket = self.pending.get((task["asset"], task["scope"]))
        if bucket:
            bucket.discard(tid)
            if not bucket:
                del self.pending[(task["asset"], task["scope"])]
        self.outcomes[(task["asset"], task["scope"])] = {
            "outcome": outcome,
            "run": run["id"],
            "attempt": attempt,
            "at": at,
        }
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

    def _commit_keys(self, output: str, scope: str, keys: dict | None) -> None:
        """Add a commit's delta files to the output's key index (§6): into the
        levels, and into the delta log if anything reads it incrementally.
        The head carries the index's live key count."""

        if keys is not None:
            index = self.index(output, scope)
            if keys["files"]:
                index = index.committed(
                    keys["batch"], DeltaFiles.from_json(keys), keep_log=output in self._consumed
                )
            self.indexes[(output, scope)] = index
        index = self.indexes.get((output, scope))
        if index is not None:
            self.heads[(output, scope)]["count"] = index.count

    # -- data garbage of immutable stores (docs/lifecycle.md §9.8) -----------------------

    def immutable(self, output: str) -> bool:
        manifest = self.manifest or {}
        record = (manifest.get("outputs") or {}).get(output) or {}
        return ((manifest.get("stores") or {}).get(record.get("store")) or {}).get("writes") == "immutable"

    def discard_reads(self) -> set[str]:
        """The index files pending discard entries still read (their delta
        files name the predecessors): kept until the entries are done, even
        once the index lets go of them."""

        return {
            f"{d['prefix']}{name}.kx"
            for entries in self.discards.values()
            for d in entries
            if d["kind"] == "delta"
            for name in d["files"]
        }

    def _collect(self, output: str, scope: str, entry: dict) -> None:
        self.discards.setdefault((output, scope), []).append({"n": self.applied, **entry})

    def _superseded(
        self, output: str, scope: str, before: dict | None, head: dict, keys: dict | None
    ) -> None:
        """What a commit let go of: each changed key's predecessor (named in
        its delta files), a value's previous object, or — when an append
        output starts over — its earlier batches."""

        if not self.immutable(output):
            return
        if keys and keys.get("files"):
            prefix = self.index(output, scope).prefix
            self._collect(
                output,
                scope,
                {"kind": "delta", "prefix": prefix, "files": [f["name"] for f in keys["files"]]},
            )
        old, new = (
            ((before or {}).get("ref") or {}).get("handle") or {},
            (head.get("ref") or {}).get("handle") or {},
        )
        if old.get("mode") in ("value", "set") and old.get("path") != new.get("path"):
            self._collect(output, scope, {"kind": "items", "items": [["path", old["path"]]]})
        if old.get("mode") == "batches" and new.get("mode") == "batches":
            first, last = old["batches"]
            if int(new["batches"][0]) > int(first):
                self._collect(
                    output,
                    scope,
                    {"kind": "items", "items": [["batches", int(first), int(new["batches"][0]) - 1]]},
                )

    def _abandoned(self, scope: str, attempt: str, launched: dict) -> None:
        """An attempt that ended without committing: whatever it wrote on an
        immutable store carries its generation, which no other attempt uses."""

        for name, info in (launched["prepared"].get("outputs") or {}).items():
            if self.immutable(name):
                entry = {
                    "kind": "abandoned",
                    "attempt": attempt,
                    "generation": launched["pin"],
                    "batch": info.get("batch"),
                }
                if info.get("prefix") is not None:
                    entry["prefix"] = info["prefix"]
                self._collect(name, scope, entry)

    def _discarded(self, scope: str, e: dict) -> None:
        """A worker discarded data garbage: its entries go, and the index-side
        files they were read from become garbage themselves. An entry whose
        names it could not read counts a miss; at `STUCK_AFTER` it is
        `stuck`: kept, and shown, but no longer handed out."""

        for output, done in (e.get("discarded") or {}).items():
            self._drop_discards(output, scope, done)
        for output, missed in (e.get("discard_unresolved") or {}).items():
            for d in self.discards.get((output, scope), []):
                if d["n"] in missed:
                    d["misses"] = d.get("misses", 0) + 1
                    if d["misses"] >= STUCK_AFTER:
                        d["stuck"] = True
        self.garbage.extend([path, self.applied] for path in e.get("discarded_files") or ())

    def _drop_discards(self, output: str, scope: str, ns) -> None:
        left = [d for d in self.discards.get((output, scope), []) if d["n"] not in set(ns)]
        if left:
            self.discards[(output, scope)] = left
        else:
            self.discards.pop((output, scope), None)

    def _on_DiscardsCleared(self, e):
        """An operator gave up on stuck entries: their objects stay."""

        self._drop_discards(e["output"], e["scope"], e["n"])

    def _replace_index(self, key: tuple, index: IndexState) -> None:
        """Swap in a new index state; files it no longer references await deletion."""

        before = self.indexes[key]
        self.indexes[key] = index
        for name in sorted(before.referenced() - index.referenced()):
            self.garbage.append([before.path(name), self.applied])

    def _on_IndexCompacted(self, e):
        key = (e["output"], e["scope"])
        if key not in self.indexes:
            return
        if e.get("garbage"):  # the entries the merge dropped: their objects (§9.8)
            prefix = self.indexes[key].prefix
            self._collect(
                *key, {"kind": "sidecar", "prefix": prefix, "files": [g["name"] for g in e["garbage"]]}
            )
        index = self.indexes[key].compacted([FileInfo.from_json(f) for f in e["added"]], e["removed"])
        self._replace_index(key, index)

    def _on_IndexRecounted(self, e):
        key = (e["output"], e["scope"])
        if key in self.indexes:
            index = self.indexes[key] = self.indexes[key].recounted(
                e["live"], e["pinned_count"], e["pinned_inexact"]
            )
            if key in self.heads:
                self.heads[key]["count"] = index.count

    def _on_IndexTruncated(self, e):
        key = (e["output"], e["scope"])
        if key in self.indexes:
            self._replace_index(key, self.indexes[key].truncated(e["below"]))

    def _on_GarbageDeleted(self, e):
        gone = set(e["paths"])
        self.garbage = [g for g in self.garbage if g[0] not in gone]

    def _on_SourceCommitted(self, e):
        before = self.heads.get((e["source"], ""))
        head = e["head"]
        self.heads[(e["source"], "")] = {**head, "at": e["at"], "n": self.applied}
        self._commit_keys(e["source"], "", e.get("keys"))
        run = e.get("run")
        if run is not None:
            self._record("runs", history.commit_row(run, e["at"]))
            self._record(
                "run_events",
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
            self._record(
                "materializations",
                history.materialization(e["source"], None, "", installed, keys=e.get("keys"), listed=run),
            )
        if before is None or before["ref"].get("version") != head["ref"].get("version"):
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
        auto["last_at"] = e["at"]
        if e.get("run"):
            auto["last_run"] = e["run"]
        if "revision" in e:
            auto["last_revision"] = e["revision"]
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
        self.garbage.extend([path, self.applied] for path in self.history.compacted(e["changes"]))

    def _on_RunsDeleted(self, e):
        """Runs retire for good: their history goes, and their directories
        are deleted only from here on — `RunsPurged` once they are."""

        self.history.forget(set(e["runs"]), e["at"])
        self.retired.extend(r for r in e.get("files") or () if r not in self.retired)

    def _on_RunsPurged(self, e):
        gone = set(e["runs"])
        self.retired = [r for r in self.retired if r not in gone]
