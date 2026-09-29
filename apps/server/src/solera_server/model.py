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
  is launched (a restart simply dispatches its task again), pool leases, and
  workers.

Events carry every timestamp they need; `apply` never reads a clock.
"""

from __future__ import annotations

import copy

from solera.keys.index import DeltaFiles, FileInfo, IndexState, index_prefix

from . import history
from .lake import LakeState

TERMINAL_TASK = frozenset({"succeeded", "skipped", "failed", "blocked", "canceled"})
TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
BAD_OUTCOME = frozenset({"failed", "blocked", "canceled"})
MAX_RECEIPTS = 10_000  # idempotency receipts kept for replayed submissions


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
                "writer": self.writer,
                "revision": self.revision,
                "manifest": self.manifest,
                "project": self.project,
                "heads": _nest(self.heads, 2),
                "indexes": _nest({k: v.to_json() for k, v in self.indexes.items()}, 2),
                "garbage": self.garbage,
                "cursors": _nest(self.cursors, 2),
                "watermarks": _nest(self.watermarks, 3),
                "outcomes": _nest(self.outcomes, 2),
                "unsettled": _nest(self.unsettled, 2),
                "automations": self.automations,
                "runs": self.runs,
                "receipts": list(self.receipts.items()),
                "history": self.history.to_json(),
            }
        )

    def restore(self, snap: dict | None) -> None:
        snap = copy.deepcopy(snap) if snap else {}
        # durable
        self.writer = snap.get("writer")
        self.revision = snap.get("revision")
        self.manifest = snap.get("manifest")
        self.project = snap.get("project")
        self.heads: dict[tuple, dict] = _flatten(snap.get("heads"), 2)
        self.indexes: dict[tuple, IndexState] = {
            k: IndexState.from_json(v) for k, v in _flatten(snap.get("indexes"), 2).items()
        }
        self.garbage: list[list] = snap.get("garbage") or []  # [path, at]: unreferenced index files
        self.cursors: dict[tuple, object] = _flatten(snap.get("cursors"), 2)
        self.watermarks: dict[tuple, dict] = _flatten(snap.get("watermarks"), 3)
        self.outcomes: dict[tuple, dict] = _flatten(snap.get("outcomes"), 2)
        # (output, scope) -> intents of attempts that died while writing it (§8)
        self.unsettled: dict[tuple, list] = _flatten(snap.get("unsettled"), 2)
        self.automations: dict[str, dict] = snap.get("automations") or {}
        self.runs: dict[str, dict] = snap.get("runs") or {}
        self.receipts: dict[str, str] = dict(snap.get("receipts") or [])
        # the run history (§7): per table, its files and the rows awaiting a flush
        self.history = LakeState(history.TABLES, snap.get("history"))
        # derived from launched attempts, plus memory-only claims of attempts preparing
        self.claims: dict[str, dict] = {}  # task id -> {attempt, started_at, status, launched?}
        self.attempts: dict[str, str] = {}  # attempt id -> task id, while claimed
        self.locks: dict[tuple, str] = {}  # (asset, scope) -> attempt id
        self.pool: dict[str, dict] = {}  # attempt id -> pool work
        # memory only
        self.workers: dict[str, dict] = {}
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
        self.claims[task_id] = {"attempt": attempt, "started_at": now, "status": "running"}
        self.attempts[attempt] = task_id
        self.locks[(task["asset"], task["scope"])] = attempt
        self.queue.pop(task_id, None)

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
        status = "running" if pool is None or launched.get("worker") else "claimable"
        self.claims[task["id"]] = {
            "attempt": attempt,
            "started_at": launched["started_at"],
            "status": status,
            "launched": True,
        }
        self.attempts[attempt] = task["id"]
        self.locks[(task["asset"], task["scope"])] = attempt
        self.queue.pop(task["id"], None)
        if pool is not None:
            record = self.pool.get(attempt) or {"lease_until": None}
            self.pool[attempt] = {
                **record,
                "attempt": attempt,
                "task": task["id"],
                "run": self.task_run[task["id"]],
                "asset": task["asset"],
                "scope": task["scope"],
                "pool": pool["name"],
                "needs": pool.get("needs") or {},
                "status": "claimed" if launched.get("worker") else "queued",
                "claimed_by": launched.get("worker"),
                "claimed_at": launched.get("claimed_at"),
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
        getattr(self, f"_on_{event['type']}")(event)

    def _on_WriterStarted(self, e):
        self.writer = e.get("writer")

    def _on_ProjectRegistered(self, e):
        manifest = e["manifest"]
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
                }

    def _apply_aliases(self, manifest) -> dict[str, list[str]]:
        """Move everything held under an asset's former names to its current
        one (§2): cursors, watermarks, outcomes, pending automation entries,
        and — for outputs named after the asset — heads and key indexes. An
        index keeps its files where they are (its `prefix`). Returns
        `{asset: [aliases]}` for the automations to follow."""

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

        def move(table: dict, rename, position: int):
            for key in [k for k in table if k[position] in rename]:
                target = (*key[:position], rename[key[position]], *key[position + 1 :])
                if target not in table:
                    table[target] = table.pop(key)

        move(self.heads, output_map, 0)
        move(self.indexes, output_map, 0)
        move(self.cursors, asset_map, 0)
        move(self.outcomes, asset_map, 0)
        move(self.watermarks, asset_map, 0)
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
        self.runs[run["id"]] = run
        if e.get("command"):
            self.receipts[e["command"]] = run["id"]
            while len(self.receipts) > MAX_RECEIPTS:
                del self.receipts[next(iter(self.receipts))]
        self._index_run(run["id"], run)
        self._roll_up(run["id"], e["run"]["created_at"])

    def _on_RunReopened(self, e):
        run = e["run"]
        # Its history rows describe how it ended, and it is running again.
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
        at, action = e["at"], e["action"]
        if action in ("pause", "resume"):
            run["paused"] = action == "pause"
        elif action == "cancel":
            # Every unfinished task is canceled together: no dependent is
            # blocked on the way, and nothing is left to roll up.
            run["status"] = "canceled"
            run["updated_at"] = at
            for tid, task in run["tasks"].items():
                if task["status"] in TERMINAL_TASK:
                    continue
                attempt = (self.claims.get(tid) or {}).get("attempt")
                task["status"] = "canceled"
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
            for task in run["tasks"].values():
                if task["status"] == "failed":
                    task["status"] = "queued"
                    task["ready_at"] = at
                    task["retried"] = task.get("retried", 0) + 1
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
                    self._requeue(task, at)
            self._roll_up(run["id"], at)

    def _on_AttemptLaunched(self, e):
        run = self.runs.get(e["run"])
        task = run["tasks"].get(e["task"]) if run else None
        if task is None or task["status"] in TERMINAL_TASK:
            return
        launched = {k: e[k] for k in ("attempt", "started_at", "at", "execution", "prepared")}
        if e.get("pool"):
            launched["pool"] = e["pool"]
        task["launched"] = launched
        if task["status"] == "queued":
            task["status"] = "running"
        self._hold(task)

    def _on_AttemptClaimed(self, e):
        """A pool worker took a launched attempt (§10)."""

        task = self.task(self.attempts.get(e["attempt"], ""))
        launched = (task or {}).get("launched")
        if launched is None or launched["attempt"] != e["attempt"]:
            return
        launched["worker"], launched["claimed_at"] = e["worker"], e["at"]
        self._hold(task)

    def _on_AttemptFinished(self, e):
        run = self.runs.get(e["run"])
        task = run["tasks"].get(e["task"]) if run else None
        if task is None:
            return
        self._release_claim(task["id"], e["attempt"])
        reads = []
        if (task.get("launched") or {}).get("attempt") == e["attempt"]:
            reads = task["launched"]["prepared"].get("lineage") or []
            del task["launched"]
            if task["status"] == "running":
                task["status"] = "queued"  # until the outcome below says otherwise
        for output, intent in (e.get("unsettled") or {}).items():
            intents = self.unsettled.setdefault((output, task["scope"]), [])
            intents.append({**intent, "run": e["run"], "attempt": e["attempt"]})
        outcome, at = e["outcome"], e["finished_at"]
        summary = {
            "id": e["attempt"],
            "outcome": outcome,
            "started_at": e.get("started_at"),
            "finished_at": at,
        }
        if e.get("error"):
            summary["error"] = e["error"]
        commit = e.get("commit")
        if commit:
            summary["outputs"] = {
                name: h["ref"].get("version") for name, h in commit.get("heads", {}).items()
            }
        task["attempts"].append(summary)
        if task["status"] in TERMINAL_TASK:
            # Its run was canceled while it ran. An attempt that was already
            # writing still commits: its data landed (§8).
            if outcome == "succeeded" and commit:
                self._install(task, commit, e, reads)
            return
        if outcome == "succeeded":
            self._install(task, commit or {}, e, reads)
            if e.get("more"):
                self._requeue(task, at)
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
            if e.get("retryable") and failures < allowed:
                self._requeue(task, at + float(e.get("delay") or 0))
            else:
                task["status"] = "failed"
                self._finished(run, task, "failed", e["attempt"], at)
        elif outcome in ("expired", "canceled"):
            self._requeue(task, at)
        else:
            raise ValueError(f"unknown attempt outcome {outcome!r}")

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
            self.heads[(name, scope)] = {**head, "run": e["run"], "attempt": e["attempt"], "at": at}
            self._commit_keys(name, scope, (commit.get("keys") or {}).get(name))
            if name in commit.get("settled", ()):
                # The commit's delta took in what the dead attempts left (§8):
                # their intent files are no longer needed.
                index = self.index(name, scope)
                for intent in self.unsettled.pop((name, scope), ()):
                    self.garbage.extend([index.path(f["name"]), at] for f in intent["files"])
        if "cursor" in commit:
            if commit["cursor"] is None:
                self.cursors.pop((asset, scope), None)
            else:
                self.cursors[(asset, scope)] = commit["cursor"]
        for edge, wm in commit.get("watermarks", {}).items():
            self.watermarks[(asset, edge, scope)] = wm
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

    def _requeue(self, task: dict, ready_at: float) -> None:
        task["status"] = "queued"
        task["ready_at"] = ready_at
        self.queue[task["id"]] = ready_at

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
                self._requeue(dep, at)
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

    def _replace_index(self, key: tuple, index: IndexState, at: float) -> None:
        """Swap in a new index state; files it no longer references await deletion."""

        before = self.indexes[key]
        self.indexes[key] = index
        for name in sorted(before.referenced() - index.referenced()):
            self.garbage.append([before.path(name), at])

    def _on_IndexCompacted(self, e):
        key = (e["output"], e["scope"])
        if key not in self.indexes:
            return
        index = self.indexes[key].compacted(
            [FileInfo.from_json(f) for f in e["added"]], e["removed"], recount=e.get("recount")
        )
        self._replace_index(key, index, e["at"])
        if key in self.heads:
            self.heads[key]["count"] = index.count

    def _on_IndexTruncated(self, e):
        key = (e["output"], e["scope"])
        if key in self.indexes:
            self._replace_index(key, self.indexes[key].truncated(e["below"]), e["at"])

    def _on_GarbageDeleted(self, e):
        gone = set(e["paths"])
        self.garbage = [g for g in self.garbage if g[0] not in gone]

    def _on_SourceCommitted(self, e):
        before = self.heads.get((e["source"], ""))
        head = e["head"]
        self.heads[(e["source"], "")] = {**head, "at": e["at"]}
        self._commit_keys(e["source"], "", e.get("keys"))
        run = e.get("run")
        if run is not None:
            self._record("runs", history.commit_row(run, e["at"]))
            installed = {**self.heads[(e["source"], "")], "run": run["id"], "attempt": None}
            self._record(
                "materializations",
                history.materialization(e["source"], None, "", installed, keys=e.get("keys"), listed=run),
            )
        if before is None or before["ref"].get("version") != head["ref"].get("version"):
            self._pend_onchange(None, "", [e["source"]])

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
        for table, rows in history.run_rows(run, self.manifest).items():
            for row in rows:
                self._record(table, row)

    # -- history (§7) ------------------------------------------------------------------

    def _record(self, table: str, row: dict) -> None:
        self.history.append(table, row)

    def _on_HistoryFlushed(self, e):
        self.history.flushed(e["files"], e["upto"])

    def _on_HistoryCompacted(self, e):
        self.garbage.extend([path, e["at"]] for path in self.history.compacted(e["changes"]))

    def _on_RunsDeleted(self, e):
        self.history.forget(set(e["runs"]), e["at"])
