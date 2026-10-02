"""Run history (docs/object-store-state.md §7): what ran and what it made, as
Parquet files on the object store, queried with DuckDB.

Eight tables, each row about one:

    run_events        thing that happened to a run, one of its tasks or attempts
    runs              finished run or source commit: how it was asked for, how it ended
    tasks             task of a finished run: timings, retries, executor
    attempts          attempt of a finished run: its phases, and the versions it committed
    materializations  output version a commit installed, with its metadata
    lineage           input version an output version was built from
    key_outcomes      key an Each attempt processed: what it came to (per-key-processing.md §10)
    ticks             sensor tick (lifecycle.md §11): buffered, never journaled

`run_events` is the timeline: the engine's events and the worker's, appended
as they are applied. The timings in `runs`, `tasks` and `attempts` summarize
it. Rows are born in the model: `apply` derives them from the events that
finish things — a run archived, a commit installed, a source committed — and
appends them to the history's `LakeState`. How they reach Parquet files, and how the
files are merged and rewritten, is the lake's business (`lake.py`): this
module knows the tables, their rows and the questions asked of them.

Queries also see the runs still in progress, so a run shows up the moment it
is submitted.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field

from .lake import Lake, Table

TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
BAD_TASK = frozenset({"failed", "blocked", "canceled"})


TABLES = {
    "run_events": Table(
        "run",
        "at",
        {
            "run": "VARCHAR",
            "n": "INTEGER",  # its order within the run: events at the same moment keep theirs
            "at": "DOUBLE",
            "type": "VARCHAR",
            "task": "VARCHAR",  # null for the run's own events
            "attempt": "VARCHAR",  # null for a run's or task's own events
            "by": "VARCHAR",  # engine | worker | who asked
            "name": "VARCHAR",  # what it is about: an input, output, executor, host, mark...
            "reason": "VARCHAR",
            "until": "DOUBLE",  # the end of what it announces: a retry's due time, an outage
            "rows": "BIGINT",
        },
    ),
    "runs": Table(
        "id",
        "created_at",
        {
            "id": "VARCHAR",
            "created_at": "DOUBLE",
            "finished_at": "DOUBLE",
            "status": "VARCHAR",  # a run that wrote nothing is "skipped"
            "trigger": "VARCHAR",  # manual | automation | sensor | commit
            "automation": "VARCHAR",
            "by": "VARCHAR",
            "retry_of": "VARCHAR",  # the finished run an explicit retry ran again
            "source": "VARCHAR",
            "targets": "VARCHAR[]",
            "assets": "VARCHAR[]",
            "committed": "VARCHAR[]",
            "mode": "VARCHAR",
            "partitions": "VARCHAR",  # a named selection, or a JSON list of scopes
            "upstream": "BOOLEAN",
            "tags": "MAP(VARCHAR, VARCHAR)",
            "task_count": "INTEGER",
            "failed_count": "INTEGER",
            "error": "VARCHAR",
            "config": "VARCHAR",  # JSON object
            "keys": "VARCHAR",  # JSON object: the keys asked for, per incremental edge
        },
    ),
    "tasks": Table(
        "run",
        "created_at",
        {
            "id": "VARCHAR",
            "run": "VARCHAR",
            "asset": "VARCHAR",
            "scope": "VARCHAR",
            "status": "VARCHAR",
            "created_at": "DOUBLE",
            "started_at": "DOUBLE",
            "finished_at": "DOUBLE",
            "attempts": "INTEGER",
            "wait": "DOUBLE",  # seconds ready to run, and not: paused, or the engine down
            "duration": "DOUBLE",  # seconds, summed over its attempts
            "error": "VARCHAR",
            "deps": "VARCHAR[]",  # the tasks it waited on
            "max_attempts": "INTEGER",
            "retry_delay": "DOUBLE",  # seconds; no retry policy if null
            "retry_backoff": "VARCHAR",
            "executor": "VARCHAR",  # where its last attempt ran
        },
    ),
    "attempts": Table(
        "run",
        "started_at",
        {
            "id": "VARCHAR",
            "run": "VARCHAR",
            "task": "VARCHAR",
            "asset": "VARCHAR",
            "scope": "VARCHAR",
            "n": "INTEGER",
            "outcome": "VARCHAR",
            "started_at": "DOUBLE",
            "finished_at": "DOUBLE",
            "duration": "DOUBLE",
            # seconds in each phase: from its first event to the next phase's
            "preparing": "DOUBLE",  # claimed: inputs pinned, the spec written
            "provisioning": "DOUBLE",  # launched: a machine found, the harness started
            "importing": "DOUBLE",  # booted: the project imported
            "loading": "DOUBLE",  # imported: the inputs loaded
            "computing": "DOUBLE",  # computing: the asset's function ran
            "writing": "DOUBLE",  # computed: the outputs stored
            "settling": "DOUBLE",  # finished: the result committed
            "peak_memory": "BIGINT",  # bytes; measured in a process of its own only
            "cpu_seconds": "DOUBLE",
            "error": "VARCHAR",
            "executor": "VARCHAR",  # null if never launched: skipped, or failed preparing
            "cpu": "INTEGER",  # requested
            "memory": "BIGINT",  # bytes, requested
            "gpu": "INTEGER",  # requested; a named GPU type counts one
            "options": "MAP(VARCHAR, VARCHAR)",  # its other placement options: image, GPU type
            "outputs": "MAP(VARCHAR, VARCHAR)",  # output -> the version it committed
            "keys": "MAP(VARCHAR, BIGINT)",  # an Each attempt's keys by outcome: ok, failed…
        },
    ),
    "materializations": Table(
        "run",
        "at",
        {
            "output": "VARCHAR",
            "asset": "VARCHAR",
            "scope": "VARCHAR",
            "version": "VARCHAR",
            "store": "VARCHAR",
            "run": "VARCHAR",
            "attempt": "VARCHAR",
            "at": "DOUBLE",
            "batch": "BIGINT",
            "added": "BIGINT",
            "removed": "BIGINT",
            "added_keys": "VARCHAR[]",  # a source commit's keys, listed up to 1,000
            "removed_keys": "VARCHAR[]",
            "rows": "BIGINT",
            "complete": "BOOLEAN",
            "metadata": "VARCHAR",  # JSON object
        },
    ),
    "lineage": Table(
        "run",
        "at",
        {
            "output": "VARCHAR",
            "scope": "VARCHAR",
            "version": "VARCHAR",
            "run": "VARCHAR",
            "attempt": "VARCHAR",
            "at": "DOUBLE",
            "input": "VARCHAR",
            "input_scope": "VARCHAR",
            "input_version": "VARCHAR",
            "param": "VARCHAR",
        },
    ),
    # Sensor ticks (docs/lifecycle.md §11.5): buffered in memory, never
    # journaled; what a tick caused is a run tagged with it, kept with runs.
    "ticks": Table(
        "sensor",
        "started_at",
        {
            "sensor": "VARCHAR",
            "tick": "VARCHAR",
            "started_at": "DOUBLE",
            "ended_at": "DOUBLE",
            "host": "VARCHAR",
            "outcome": "VARCHAR",  # skipped | advanced | committed | requested | refused | failed
            "error": "VARCHAR",
            "runs": "VARCHAR[]",  # the runs it requested
        },
    ),
}
VOLATILE = ("ticks",)  # tables whose rows are never journaled
TICKS_KEPT = 86400.0  # seconds a tick row is kept
TABLES["key_outcomes"] = Table(
    "run",
    "at",
    {
        "run": "VARCHAR",
        "attempt": "VARCHAR",  # attempts.id
        "asset": "VARCHAR",
        "scope": "VARCHAR",
        "key": "VARCHAR",
        "revision": "VARCHAR",  # the upstream version it processed: its text, or a digest's hex
        "outcome": "VARCHAR",  # ok, removed, unmatched, rejected, failed, retrying, canceled, timed_out
        "error": "VARCHAR",  # class and message
        "duration": "DOUBLE",  # seconds in the call
        "at": "DOUBLE",
    },
)
MAX_METADATA = 64 << 10  # bytes of JSON per output version


# -- rows -----------------------------------------------------------------------------


EXECUTION = ("executor", "cpu", "memory", "gpu", "options")


def execution(spec: dict) -> dict:
    """Where a launched attempt ran, as its row holds it: the executor, the
    resources it requested, and its other options verbatim. Unset fields
    are left out: `Local()()` is just `{"executor": "local"}`."""

    options = dict(spec.get("placement") or {})
    cpu, memory, gpu = options.pop("cpu", None), options.pop("memory", None), options.get("gpu")
    if not isinstance(gpu, str):
        options.pop("gpu", None)
    record = {
        "executor": spec["executor"],
        "cpu": cpu,
        "memory": memory,
        "gpu": 1 if isinstance(gpu, str) else gpu,
        "options": {k: str(v) for k, v in options.items()},
    }
    return {k: v for k, v in record.items() if v not in (None, {})}


# An attempt's phases, and the events that start them: each lasts until the
# next of these the attempt reached, or until it ended.
PHASES = {
    "preparing": "claimed",
    "provisioning": "launched",
    "importing": "booted",
    "loading": "imported",
    "computing": "computing",
    "writing": "computed",
    "settling": "finished",
}
USAGE = ("peak_memory", "cpu_seconds")


def phases(times: dict[str, float], end: float) -> dict[str, float]:
    """Seconds in each phase an attempt reached, from `times` — when each
    event that starts one happened — and when it ended."""

    reached = [(phase, times[event]) for phase, event in PHASES.items() if event in times]
    ends = [at for _, at in reached[1:]] + [end]
    return {phase: max(0.0, stop - start) for (phase, start), stop in zip(reached, ends, strict=True)}


def span(attempt: dict) -> float:
    start, end = attempt.get("started_at"), attempt.get("finished_at")
    return max(0.0, end - start) if start is not None and end is not None else 0.0


def attempt_row(run_id: str, task: dict, summary: dict, n: int) -> dict:
    """An ended attempt's `attempts` row, written as it ends: a task in
    progress keeps only aggregates of the attempts behind it."""

    return {
        "id": summary["id"],
        "run": run_id,
        "task": task["id"],
        "asset": task["asset"],
        "scope": task["scope"],
        "n": n,
        "outcome": summary["outcome"],
        "started_at": summary.get("started_at"),
        "finished_at": summary.get("finished_at"),
        "duration": span(summary),
        **{k: summary.get(k) for k in (*PHASES, *USAGE)},
        "error": summary.get("error"),
        **{k: summary.get(k) for k in EXECUTION},
        "options": summary.get("options") or {},
        "outputs": summary.get("outputs") or {},
        "keys": summary.get("keys") or {},
    }


def attempt_summary(row: dict) -> dict:
    """An `attempts` row as the views show an attempt: `attempt_row` backwards."""

    attempt = {k: row[k] for k in ("id", "outcome", "started_at", "finished_at")}
    attempt |= {k: row[k] for k in (*PHASES, *USAGE) if row[k] is not None}
    attempt |= {k: row[k] for k in EXECUTION if row[k]}
    if row["error"]:
        attempt["error"] = row["error"]
    if row["outputs"]:
        attempt["outputs"] = dict(row["outputs"])
    if row.get("keys"):
        attempt["keys"] = dict(row["keys"])
    return attempt


def _json(value) -> str | None:
    return None if value is None else json.dumps(value, sort_keys=True)


def run_rows(run: dict, *, live: bool = False) -> dict[str, list[dict]]:
    """A run's rows in `runs` and `tasks` (its attempts wrote theirs as they
    ended). `live` describes a run still in progress: no finish time."""

    tasks = run["tasks"]
    task_rows = []
    for tid in sorted(tasks):
        task = tasks[tid]
        launched = (task.get("launched") or {}).get("execution")
        retry = task.get("retry") or {}
        task_rows.append(
            {
                "id": tid,
                "run": run["id"],
                "asset": task["asset"],
                "scope": task["scope"],
                "status": task["status"],
                "created_at": run["created_at"],
                "started_at": task.get("first_at"),
                "finished_at": task.get("last_at"),
                "attempts": task.get("tries", 0),
                "wait": task["wait"],
                "duration": task.get("duration", 0.0),
                "error": task.get("error"),
                "deps": task["deps"],
                "max_attempts": task["max_attempts"],
                "retry_delay": retry.get("delay"),
                "retry_backoff": retry.get("backoff"),
                "executor": launched["executor"] if launched else task.get("executor"),
            }
        )
    status = run["status"]
    quiet = all(
        t["status"] == "skipped" and set(t.get("outcomes") or ()) <= {"skipped"} for t in tasks.values()
    )
    if live:
        status = "paused" if run.get("paused") else status
    elif status == "succeeded" and quiet:
        status = "skipped"  # it launched nothing and wrote nothing
    failed = [r for r in task_rows if r["status"] in BAD_TASK]
    partitions = run.get("partitions")
    row = {
        "id": run["id"],
        "created_at": run["created_at"],
        "finished_at": None if live else run.get("updated_at"),
        "status": status,
        "trigger": "automation" if run.get("automation") else "sensor" if run.get("sensor") else "manual",
        "automation": run.get("automation"),
        "by": run.get("by"),
        "retry_of": run.get("retry_of"),
        "source": None,
        "targets": list(run.get("targets") or ()),
        "assets": sorted({t["asset"] for t in tasks.values()}),
        "committed": sorted({t["asset"] for t in tasks.values() if t["status"] == "succeeded"}),
        "mode": run.get("mode"),
        "partitions": partitions
        if isinstance(partitions, str) or partitions is None
        else json.dumps(partitions),
        "upstream": bool(run.get("upstream")),
        "tags": dict(run.get("tags") or {}),
        "task_count": len(tasks),
        "failed_count": len(failed),
        "error": next((r["error"] for r in failed if r["error"]), None),
        "config": _json(run.get("config")),
        "keys": _json(run.get("keys")),
    }
    return {"runs": [row], "tasks": task_rows}


def run_record(rows: dict[str, list[dict]], events: int = 0) -> dict:
    """A finished run as the model held it, from its rows — `run_rows` (or
    `commit_row`) backwards — and how many events it has had."""

    [row] = rows["runs"]
    if row["trigger"] == "commit":
        record = {"id": row["id"], "source": row["source"], "by": row["by"]}
        for m in rows.get("materializations") or ():
            if m["batch"] is None:
                record["version"] = m["version"]
            else:
                record["batch"] = m["batch"]
                record["upserted"] = m["added_keys"] if m["added_keys"] is not None else m["added"]
                record["deleted"] = m["removed_keys"] if m["removed_keys"] is not None else m["removed"]
        return record
    tasks = {}
    for t in rows["tasks"]:
        tasks[t["id"]] = {
            "id": t["id"],
            "run": row["id"],
            "asset": t["asset"],
            "scope": t["scope"],
            "status": t["status"],
            "deps": list(t["deps"] or ()),
            "max_attempts": t["max_attempts"],
            "retry": None
            if t["retry_delay"] is None
            else {"n": t["max_attempts"] - 1, "delay": t["retry_delay"], "backoff": t["retry_backoff"]},
            "wait": t["wait"],
        }
    partitions = row["partitions"]
    return {
        "id": row["id"],
        "targets": list(row["targets"] or ()),
        "partitions": json.loads(partitions) if partitions and partitions.startswith("[") else partitions,
        "mode": row["mode"],
        "upstream": row["upstream"],
        "config": None if row["config"] is None else json.loads(row["config"]),
        "keys": None if row["keys"] is None else json.loads(row["keys"]),
        "automation": row["automation"],
        "by": row["by"],
        **({"retry_of": row["retry_of"]} if row.get("retry_of") else {}),
        "tags": dict(row["tags"] or {}),
        "status": "succeeded" if row["status"] == "skipped" else row["status"],
        "paused": False,
        "created_at": row["created_at"],
        "updated_at": row["finished_at"],
        "events": events,
        "tasks": tasks,
    }


def commit_row(run: dict, at: float) -> dict:
    """The `runs` row of a source commit (§5): `run` is its record."""

    return {
        "id": run["id"],
        "created_at": at,
        "finished_at": at,
        "status": "succeeded",
        "trigger": "commit",
        "automation": None,
        "by": run.get("by"),
        "source": run["source"],
        "targets": [run["source"]],
        "assets": [],
        "committed": [],
        "mode": "commit",
        "partitions": None,
        "upstream": False,
        "tags": run.get("tags") or {},
        "task_count": 0,
        "failed_count": 0,
        "error": None,
        "config": None,
        "keys": None,
    }


def materialization(output, asset, scope, head, *, keys=None, rows=None, metadata=None, listed=None) -> dict:
    """The row of an output version a commit installed: `head` is the head
    as installed, `keys` the commit's key delta for the output, `listed` a
    source commit's record, which lists the keys it changed."""

    listed = listed or {}

    count = head.get("count")
    return {
        "output": output,
        "asset": asset,
        "scope": scope,
        "version": head["ref"].get("version"),
        "store": head["ref"].get("store"),
        "run": head.get("run"),
        "attempt": head.get("attempt"),
        "at": head["at"],
        "batch": head.get("batch"),
        "added": (keys or {}).get("added"),
        "removed": (keys or {}).get("removed"),
        # listed up to 1,000, counted past that
        "added_keys": listed.get("upserted") if isinstance(listed.get("upserted"), list) else None,
        "removed_keys": listed.get("deleted") if isinstance(listed.get("deleted"), list) else None,
        "rows": count if count is not None else rows,
        "complete": bool(head.get("complete", True)),
        "metadata": metadata or None,
    }


def lineage(output, scope, head, reads) -> list[dict]:
    """One row per input version the attempt behind `head` read: `reads`
    holds `[output, scope, version, param]` as pinned in its spec (§8)."""

    base = {
        "output": output,
        "scope": scope,
        "version": head["ref"].get("version"),
        "run": head.get("run"),
        "attempt": head.get("attempt"),
        "at": head["at"],
    }
    return [
        {**base, "input": i, "input_scope": s, "input_version": v, "param": p} for i, s, v, p in reads or ()
    ]


# -- filters ------------------------------------------------------------------------------


@dataclass
class RunFilter:
    """Which runs a listing, facet count or histogram covers. Values within a
    field match any; fields must all match. Tags are `key=value` (or just
    `key`): run tags must all match, and `asset_tag` keeps runs that ran an
    asset carrying any of them. Skipped runs are left out unless `status`
    asks for them."""

    status: list[str] = field(default_factory=list)
    asset: list[str] = field(default_factory=list)
    asset_tag: list[str] = field(default_factory=list)
    trigger: list[str] = field(default_factory=list)
    automation: list[str] = field(default_factory=list)
    by: list[str] = field(default_factory=list)
    source: list[str] = field(default_factory=list)
    tag: list[str] = field(default_factory=list)
    q: str | None = None
    since: float | None = None
    until: float | None = None


FACETS = {
    "status": "status",
    "trigger": "trigger",
    "automation": "automation",
    "by": '"by"',
    "source": "source",
    "asset": "unnest(assets)",
    "tag": "unnest(map_entries(tags))",
}


def _tag(spec: str) -> tuple[str, str | None]:
    key, eq, value = spec.partition("=")
    return key, value if eq else None


def tagged_assets(manifest: dict, specs: list[str]) -> list[str]:
    """Assets whose `tags` match any of `key=value` (or `key`)."""

    out = []
    for name, asset in (manifest.get("assets") or {}).items():
        tags = asset.get("tags") or {}
        for key, value in map(_tag, specs):
            if key in tags and (value is None or tags[key] == value):
                out.append(name)
                break
    return sorted(out)


def run_where(f: RunFilter, manifest: dict, skip: str | None = None) -> tuple[str, list]:
    """SQL for a filter over `runs`, leaving out field `skip` (a facet counts
    every value of its own field)."""

    clauses, params = [], []
    for name, column in (
        ("trigger", "trigger"),
        ("automation", "automation"),
        ("by", '"by"'),
        ("source", "source"),
    ):
        values = getattr(f, name)
        if values and name != skip:
            clauses.append(f"list_contains(?::VARCHAR[], {column})")
            params.append(list(values))
    if skip != "status":
        if f.status:
            clauses.append("list_contains(?::VARCHAR[], status)")
            params.append(list(f.status))
        else:
            clauses.append("status <> 'skipped'")
    if f.asset and skip != "asset":
        clauses.append("list_has_any(assets, ?::VARCHAR[])")
        params.append(list(f.asset))
    if f.asset_tag:
        clauses.append("list_has_any(assets, ?::VARCHAR[])")
        params.append(tagged_assets(manifest, f.asset_tag))
    if f.tag and skip != "tag":
        for key, value in map(_tag, f.tag):
            if value is None:
                clauses.append("map_contains(tags, ?)")
                params.append(key)
            else:
                clauses.append("tags[?] = ?")
                params.extend([key, value])
    if f.q:
        clauses.append("(error ILIKE ? OR id ILIKE ?)")
        params.extend([f"%{f.q}%", f"{f.q}%"])
    if f.since is not None:
        clauses.append("created_at >= ?")
        params.append(float(f.since))
    if f.until is not None:
        clauses.append("created_at < ?")
        params.append(float(f.until))
    return " AND ".join(clauses) or "true", params


BUCKETS = (60, 300, 900, 3600, 3 * 3600, 6 * 3600, 12 * 3600, 86400, 7 * 86400, 30 * 86400)


def bucket_for(span: float, target: int = 60) -> int:
    """The smallest bucket that fits `span` seconds into `target` bars."""

    return next((b for b in BUCKETS if span / b <= target), BUCKETS[-1])


def _dicts(cursor) -> list[dict]:
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


# -- the history --------------------------------------------------------------------------


class History:
    """The run history of one namespace: its lake, and the questions asked
    of it. Memory only; the model holds the rest."""

    def __init__(self, state, *, clock=None, **lake):
        self.state = state
        self.lake = Lake(
            state,
            TABLES,
            lambda: state.model.history,
            name="History",
            clock=clock,
            volatile=VOLATILE,
            pin=lambda: state.model.reading(),
            **lake,
        )
        self.clock = self.lake.clock

    @property
    def m(self):
        return self.state.model

    def start(self) -> None:
        self.lake.start()

    async def stop(self) -> None:
        await self.lake.stop()

    def tick(self, row: dict) -> None:
        """A sensor tick's row: written with the next flush, lost if the
        engine stops first."""

        self.lake.buffer("ticks", row)

    async def ticks(self, sensor: str, limit: int = 100) -> list[dict]:
        """A sensor's latest tick rows, newest first."""

        def work(con):
            return _dicts(
                con.execute(
                    "SELECT * FROM ticks WHERE sensor = ? ORDER BY started_at DESC LIMIT ?", [sensor, limit]
                )
            )

        return await self.lake.query(
            work, ("ticks",), key=sensor, extra={"ticks": self.lake.unwritten("ticks")}
        )

    def delete(self, runs: list[str], files: list[str] = ()) -> None:
        """Forget finished runs: their rows go now, or from the files that
        hold them when those are next rewritten. Of `files`, the run
        directories are deleted next (§11)."""

        if runs:
            event = {"type": "RunsDeleted", "runs": sorted(runs), "at": self.clock()}
            self.state.record({**event, "files": sorted(files)} if files else event)

    async def query(
        self,
        work,
        tables=tuple(TABLES),
        *,
        since: float | None = None,
        until: float | None = None,
        run: str | None = None,
        live: bool = True,
    ):
        """Run `work(con)` against a view per table — with, if `live`, the
        runs in progress. Files outside `since`/`until`, or unable to hold
        `run`, are left out."""

        extra = {table: self.lake.unwritten(table) for table in VOLATILE}
        if live:
            m = self.m
            for run_id in list(m.runs):
                for table, rows in run_rows(m.runs[run_id], live=True).items():
                    extra.setdefault(table, []).extend(rows)
        return await self.lake.query(work, tables, since=since, until=until, key=run, extra=extra)

    async def run(self, run_id: str) -> dict | None:
        """A finished run as the model held it, rebuilt from its rows."""

        def work(con):
            found = {}
            for table, order in (("runs", "id"), ("tasks", "id"), ("materializations", "output")):
                found[table] = _dicts(
                    con.execute(
                        f'SELECT * FROM {table} WHERE "{TABLES[table].key}" = ? ORDER BY {order}', [run_id]
                    )
                )
            [(found["events"],)] = con.execute(
                "SELECT max(n) FROM run_events WHERE run = ?", [run_id]
            ).fetchall()
            return found

        found = await self.query(
            work, ("runs", "tasks", "materializations", "run_events"), run=run_id, live=False
        )
        return run_record(found, found.pop("events") or 0) if found["runs"] else None

    async def attempts(self, run_id: str) -> dict[str, list[dict]]:
        """A run's ended attempts by task, in order — in progress or finished."""

        def work(con):
            return _dicts(con.execute("SELECT * FROM attempts WHERE run = ? ORDER BY task, n", [run_id]))

        out: dict[str, list[dict]] = {}
        for row in await self.query(work, ("attempts",), run=run_id, live=False):
            out.setdefault(row["task"], []).append(attempt_summary(row))
        return out

    async def events(self, run_id: str) -> list[dict]:
        """A run's timeline: its events in the order they happened."""

        def work(con):
            return _dicts(con.execute('SELECT * FROM run_events WHERE run = ? ORDER BY "at", n', [run_id]))

        return await self.query(work, ("run_events",), run=run_id, live=False)

    async def runs(
        self,
        f: RunFilter,
        *,
        before: str | None = None,
        anchor: str | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> dict:
        """Runs matching `f`, newest first, `limit` at a time. Two ways to
        page: follow `next` (the `before` cursor of the following page), or
        jump by `offset`, pinning `anchor` (the newest run id of the first
        page) so new runs don't shift later pages. `total` counts every
        match."""

        where, params = run_where(f, self.m.manifest or {})
        if before:
            where += " AND id < ?"
            params.append(before)
        if anchor:
            where += " AND id <= ?"
            params.append(anchor)
        columns = ", ".join(f'"{c}"' for c in TABLES["runs"].columns if c not in ("config", "keys"))

        def work(con):
            found = _dicts(
                con.execute(
                    f"SELECT {columns} FROM runs WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
                    [*params, limit + 1, offset],
                )
            )
            total = con.execute(f"SELECT count(*) FROM runs WHERE {where}", params).fetchone()[0]
            return found, total

        found, total = await self.query(work, ("runs",), since=f.since, until=f.until)
        more = len(found) > limit
        found = found[:limit]
        for row in found:
            text = row["partitions"]
            if text and text.startswith("["):
                row["partitions"] = json.loads(text)
        return {"runs": found, "next": found[-1]["id"] if more and found else None, "total": total}

    async def facets(self, f: RunFilter, *, top: int = 50) -> dict:
        """For each facet, how many runs each value has — counted with every
        other field of `f` applied, so values of one field can be combined."""

        manifest = self.m.manifest or {}

        def work(con):
            out = {}
            for name, expr in FACETS.items():
                where, params = run_where(f, manifest, skip=name)
                if name == "tag":
                    sql = (
                        f"SELECT e.key || '=' || e.value AS value, count(*) AS n FROM "
                        f"(SELECT {expr} AS e FROM runs WHERE {where}) GROUP BY 1 ORDER BY n DESC, value LIMIT ?"
                    )
                else:
                    sql = (
                        f"SELECT value, count(*) AS n FROM (SELECT {expr} AS value FROM runs WHERE {where}) "
                        "WHERE value IS NOT NULL GROUP BY 1 ORDER BY n DESC, value LIMIT ?"
                    )
                out[name] = [{"value": v, "count": n} for v, n in con.execute(sql, [*params, top]).fetchall()]
            return out

        return await self.query(work, ("runs",), since=f.since, until=f.until)

    async def histogram(self, f: RunFilter, *, bars: int = 60) -> dict:
        """Runs per time bucket and status, across `f.since`..`f.until` (or
        the runs' own span)."""

        where, params = run_where(f, self.m.manifest or {})
        now = self.clock()

        def work(con):
            lo, hi = con.execute(
                f"SELECT min(created_at), max(created_at) FROM runs WHERE {where}", params
            ).fetchone()
            start = f.since if f.since is not None else lo
            end = f.until if f.until is not None else now
            if start is None:
                return {"bucket": BUCKETS[0], "since": None, "until": end, "bars": []}
            bucket = bucket_for(max(end - start, 1), bars)
            rows = con.execute(
                f"SELECT floor(created_at / {bucket}) * {bucket} AS t, status, count(*) FROM runs "
                f"WHERE {where} GROUP BY 1, 2 ORDER BY 1",
                params,
            ).fetchall()
            out: dict[float, dict] = {}
            for t, status, n in rows:
                out.setdefault(t, {"t": t, "counts": {}})["counts"][status] = n
            return {
                "bucket": bucket,
                "since": math.floor(start / bucket) * bucket,
                "until": end,
                "bars": list(out.values()),
            }

        return await self.query(work, ("runs",), since=f.since, until=f.until)

    async def tasks(
        self,
        *,
        asset: str | None = None,
        scope: str | None = None,
        status: list[str] | None = None,
        run: str | None = None,
        since: float | None = None,
        until: float | None = None,
        before: str | None = None,
        limit: int = 100,
    ) -> dict:
        clauses, params = ["true"], []
        for column, value in (("asset", asset), ("scope", scope), ("run", run)):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if status:
            clauses.append("list_contains(?::VARCHAR[], status)")
            params.append(list(status))
        if since is not None:
            clauses.append("created_at >= ?")
            params.append(since)
        if until is not None:
            clauses.append("created_at < ?")
            params.append(until)
        if before:
            clauses.append("id < ?")
            params.append(before)
        where = " AND ".join(clauses)

        def work(con):
            return _dicts(
                con.execute(
                    f"SELECT * FROM tasks WHERE {where} ORDER BY id DESC LIMIT ?", [*params, limit + 1]
                )
            )

        found = await self.query(work, ("tasks",), since=since, until=until, run=run)
        more = len(found) > limit
        found = found[:limit]
        return {"tasks": found, "next": found[-1]["id"] if more and found else None}

    async def stats(
        self,
        *,
        since: float | None = None,
        until: float | None = None,
        asset: str | None = None,
        scope: str | None = None,
    ) -> dict:
        """Operations at a glance, from finished tasks: per asset and per
        executor, how many ran and failed, how long they took and waited
        (p50, p95), and the compute their attempts requested. A task counts
        under the executor of its last attempt; compute, under each
        attempt's. `scope` narrows to one partition of `asset`."""

        clauses, params = ["status NOT IN ('waiting', 'queued', 'running')"], []
        if since is not None:
            clauses.append("created_at >= ?")
            params.append(since)
        if until is not None:
            clauses.append("created_at < ?")
            params.append(until)
        if asset is not None:
            clauses.append("asset = ?")
            params.append(asset)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        where = " AND ".join(clauses)
        measures = """
            count(*) FILTER (WHERE status <> 'skipped') AS tasks,
            count(*) FILTER (WHERE status = 'skipped') AS skipped,
            count(*) FILTER (WHERE status = 'failed') AS failed,
            quantile_cont(duration, 0.5) FILTER (WHERE status = 'succeeded') AS p50,
            quantile_cont(duration, 0.95) FILTER (WHERE status = 'succeeded') AS p95,
            quantile_cont(wait, 0.5) FILTER (WHERE started_at IS NOT NULL) AS wait_p50,
            quantile_cont(wait, 0.95) FILTER (WHERE started_at IS NOT NULL) AS wait_p95,
            sum(duration) / 3600 AS hours
        """
        compute = """
            sum(a.duration * a.cpu) / 3600 AS cpu_hours,
            sum(a.duration * a.memory) / 3600 / 1e9 AS gb_hours,
            sum(a.duration * a.gpu) / 3600 AS gpu_hours
        """

        def work(con):
            con.execute(f"CREATE TEMP TABLE picked AS SELECT * FROM tasks WHERE {where}", params)
            result = {}
            for group, name in (("asset", "assets"), ("executor", "executors")):
                result[name] = _dicts(
                    con.execute(f"""
                        WITH t AS (
                            SELECT {group}, {measures} FROM picked
                            WHERE {group} IS NOT NULL GROUP BY 1
                        ), c AS (
                            SELECT a.{group}, {compute}
                            FROM attempts a JOIN picked p ON a.run = p.run AND a.task = p.id
                            GROUP BY 1
                        )
                        SELECT * FROM t LEFT JOIN c USING ({group}) ORDER BY 1
                    """)
                )
            return result

        # Not pruned by `until`: an attempt may start after it, its task before.
        return await self.query(work, ("tasks", "attempts"), since=since, live=False)

    async def materializations(
        self,
        *,
        outputs: list[str] | None = None,
        scope: str | None = None,
        before: str | None = None,
        limit: int = 200,
    ) -> dict:
        """Output versions, newest first, with their metadata. `next` is the
        `before` cursor of the following page: `[at, output, scope]` as JSON,
        since one commit makes several versions at the same moment."""

        clauses, params = ["true"], []
        if outputs is not None:
            clauses.append("list_contains(?::VARCHAR[], output)")
            params.append(list(outputs))
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        until = None
        if before:
            at, output, at_scope = json.loads(before)
            clauses.append('("at" < ? OR ("at" = ? AND (output > ? OR (output = ? AND scope > ?))))')
            params.extend([at, at, output, output, at_scope])
            until = math.nextafter(at, math.inf)
        where = " AND ".join(clauses)

        def work(con):
            return _dicts(
                con.execute(
                    f'SELECT * FROM materializations WHERE {where} ORDER BY "at" DESC, output, scope LIMIT ?',
                    [*params, limit + 1],
                )
            )

        found = await self.query(work, ("materializations",), until=until, live=False)
        more = len(found) > limit
        found = found[:limit]
        for row in found:
            if isinstance(row["metadata"], str):
                row["metadata"] = json.loads(row["metadata"])
        last = found[-1] if more and found else None
        cursor = json.dumps([last["at"], last["output"], last["scope"]]) if last else None
        return {"materializations": found, "next": cursor}

    async def key_outcomes(
        self,
        asset: str,
        *,
        scope: str | None = None,
        key: str | None = None,
        q: str | None = None,
        outcomes: list[str] | None = None,
        run: str | None = None,
        before: str | None = None,
        limit: int = 100,
    ) -> dict:
        """What the keys of an `Each` asset came to (per-key-processing.md §10),
        newest first: `key` exactly, or keys containing `q` (any case). Rows
        are appended when a page commits, so a run in progress shows the pages
        it has committed. `next` is the `before` cursor of the following
        page: `[at, scope, key]` as JSON."""

        clauses, params = ["asset = ?"], [asset]
        for column, value in (("scope", scope), ("key", key), ("run", run)):
            if value is not None:
                clauses.append(f'"{column}" = ?')
                params.append(value)
        if q:
            clauses.append("contains(lower(key), lower(?))")
            params.append(q)
        if outcomes:
            clauses.append("list_contains(?::VARCHAR[], outcome)")
            params.append(list(outcomes))
        until = None
        if before:
            at, at_scope, at_key = json.loads(before)
            clauses.append('("at" < ? OR ("at" = ? AND (scope > ? OR (scope = ? AND key > ?))))')
            params.extend([at, at, at_scope, at_scope, at_key])
            until = math.nextafter(at, math.inf)
        where = " AND ".join(clauses)

        def work(con):
            return _dicts(
                con.execute(
                    f'SELECT * FROM key_outcomes WHERE {where} ORDER BY "at" DESC, scope, key LIMIT ?',
                    [*params, limit + 1],
                )
            )

        found = await self.query(work, ("key_outcomes",), until=until, run=run, live=False)
        more = len(found) > limit
        found = found[:limit]
        last = found[-1] if more and found else None
        cursor = json.dumps([last["at"], last["scope"], last["key"]]) if last else None
        return {"outcomes": found, "next": cursor}

    async def lineage(
        self, output: str, scope: str, version: str, *, downstream: bool = False, depth: int = 5
    ) -> dict:
        """The versions `output@scope:version` was built from (or, with
        `downstream`, those built from it), `depth` steps out: `edges` go from
        input to output, and `nodes` say when and by which run each version
        was made."""

        made, read = ("output", "scope", "version"), ("input", "input_scope", "input_version")
        near, far = (read, made) if downstream else (made, read)
        join = " AND ".join(f"l.{c} = w.{w}" for c, w in zip(near, made, strict=True))
        sql = f"""
            WITH RECURSIVE walk(output, scope, version, depth) AS (
                SELECT ?::VARCHAR, ?::VARCHAR, ?::VARCHAR, 0
                UNION
                SELECT l.{far[0]}, l.{far[1]}, l.{far[2]}, w.depth + 1
                FROM lineage l JOIN walk w ON {join}
                WHERE w.depth < ?
            )
            SELECT DISTINCT l.input, l.input_scope, l.input_version, l.output, l.scope, l.version, l.param, l.run
            FROM lineage l JOIN walk w ON {join}
            WHERE w.depth < ?
        """

        def work(con):
            edges = con.execute(sql, [output, scope, version, depth, depth]).fetchall()
            keys = {(output, scope, version)}
            for i, i_s, i_v, o, s, v, _, _ in edges:
                keys.update({(i, i_s, i_v), (o, s, v)})
            nodes = {}
            if keys:
                con.execute("CREATE TEMP TABLE wanted (output VARCHAR, scope VARCHAR, version VARCHAR)")
                con.executemany("INSERT INTO wanted VALUES (?, ?, ?)", sorted(keys))
                for row in _dicts(
                    con.execute(
                        "SELECT m.output, m.scope, m.version, m.asset, m.run, m.attempt, m.at, m.rows "
                        "FROM materializations m JOIN wanted USING (output, scope, version)"
                    )
                ):
                    nodes[(row["output"], row["scope"], row["version"])] = row
            return edges, keys, nodes

        edges, keys, nodes = await self.query(work, ("lineage", "materializations"), live=False)
        m = self.m
        out_nodes = []
        for key in sorted(keys, key=lambda k: (k[0], k[1], k[2] or "")):
            node = dict(nodes.get(key) or {"output": key[0], "scope": key[1], "version": key[2]})
            head = m.heads.get((key[0], key[1]))
            node["current"] = head is not None and head["ref"].get("version") == key[2]
            out_nodes.append(node)
        return {
            "root": {"output": output, "scope": scope, "version": version},
            "direction": "downstream" if downstream else "upstream",
            "nodes": out_nodes,
            "edges": [
                {
                    "from": {"output": i, "scope": i_s, "version": i_v},
                    "to": {"output": o, "scope": s, "version": v},
                    "param": p,
                    "run": r,
                }
                for i, i_s, i_v, o, s, v, p, r in edges
            ],
        }

    # -- retention (§11) -------------------------------------------------------------------

    async def nth_newest(self, keeps: dict[str, int]) -> dict[str, float]:
        """For each asset, when the `keeps[asset]`-th newest run that
        committed to it was created — if it has that many."""

        if not keeps:
            return {}

        def work(con):
            con.execute("CREATE TEMP TABLE keeps (asset VARCHAR, keep INTEGER)")
            con.executemany("INSERT INTO keeps VALUES (?, ?)", sorted(keeps.items()))
            return con.execute(
                """
                SELECT r.asset, r.created_at FROM (
                    SELECT asset, created_at,
                           row_number() OVER (PARTITION BY asset ORDER BY created_at DESC, id DESC) AS n
                    FROM (SELECT unnest(committed) AS asset, created_at, id FROM runs)
                ) r JOIN keeps k ON r.asset = k.asset AND r.n = k.keep
                """
            ).fetchall()

        return dict(await self.query(work, ("runs",), live=False))

    async def older_than(self, moment: float, limit: int = 10_000) -> list[tuple[str, float, list[str], str]]:
        """Finished runs created before `moment`, oldest first:
        `(id, created_at, assets, status)`."""

        def work(con):
            return con.execute(
                "SELECT id, created_at, assets, status FROM runs WHERE created_at < ? ORDER BY id LIMIT ?",
                [moment, limit],
            ).fetchall()

        return await self.query(work, ("runs",), until=moment, live=False)

    async def prunable(self, *, before=None, asset=None, keep=None) -> list[tuple[str, str]]:
        """Finished runs created before `before`, of `asset` if given, except
        the `keep` newest of them: `(id, status)`, oldest first."""

        where, params = ("list_contains(assets, ?)", [asset]) if asset is not None else ("true", [])

        def work(con):
            return con.execute(
                f"""
                SELECT id, status FROM (
                    SELECT id, created_at, status, row_number() OVER (ORDER BY id DESC) AS n
                    FROM runs WHERE {where}
                ) WHERE n > ? AND created_at < ? ORDER BY id
                """,
                [*params, int(keep or 0), math.inf if before is None else float(before)],
            ).fetchall()

        return await self.query(work, ("runs",), live=False)
