"""Run history (docs/object-store-state.md §7): what ran and what it made, as
Parquet files on the object store, queried with DuckDB.

Five tables, each row about one:

    runs              finished run or source commit, with its full record
    tasks             task of a finished run: timings, attempts, executor
    attempts          attempt of a finished run
    materializations  output version a commit installed, with its metadata
    lineage           input version an output version was built from

Rows are born in the model: `apply` derives them from the events that finish
things — a run archived, a commit installed, a source committed — and appends
them to the history's `LakeState`. How they reach Parquet files, and how the
files are merged and rewritten, is the lake's business (`lake.py`): this
module knows the tables, their rows and the questions asked of them.

Queries also see the runs still in progress, so a run shows up the moment it
is submitted.
"""

from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass, field

from .lake import Lake, Table

TERMINAL_RUN = frozenset({"succeeded", "failed", "canceled"})
BAD_TASK = frozenset({"failed", "blocked", "canceled"})


TABLES = {
    "runs": Table(
        "id",
        "created_at",
        {
            "id": "VARCHAR",
            "created_at": "DOUBLE",
            "finished_at": "DOUBLE",
            "status": "VARCHAR",  # a run that wrote nothing is "skipped"
            "trigger": "VARCHAR",  # manual | automation | commit
            "automation": "VARCHAR",
            "by": "VARCHAR",
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
            "record": "VARCHAR",  # the run as JSON: tasks, attempts, everything
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
            "ready_at": "DOUBLE",  # when its dependencies were done
            "started_at": "DOUBLE",
            "finished_at": "DOUBLE",
            "attempts": "INTEGER",
            "duration": "DOUBLE",  # seconds, summed over its attempts
            "error": "VARCHAR",
            "executor": "VARCHAR",
            "cpu": "DOUBLE",
            "memory": "DOUBLE",
            "gpu": "DOUBLE",
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
            "error": "VARCHAR",
            "executor": "VARCHAR",
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
}
RUN_TABLES = ("runs", "tasks", "attempts")  # replaced when a run is reopened
MAX_METADATA = 64 << 10  # bytes of JSON per output version


# -- rows -----------------------------------------------------------------------------


def _executor(placement: dict | None) -> dict:
    placement = placement or {}
    kind = placement.get("kind") or "Local"
    env = placement.get("environment") or {}
    options = placement.get("placement") or {}
    label = kind if not env else f"{kind}({'/'.join(str(v) for _, v in sorted(env.items()))})"
    gpu = options.get("gpu")
    return {
        "executor": label,
        "cpu": options.get("cpu"),
        "memory": options.get("memory"),
        "gpu": float(gpu) if isinstance(gpu, (int, float)) else 1.0 if gpu else None,
    }


def _span(attempt: dict) -> float:
    start, end = attempt.get("started_at"), attempt.get("finished_at")
    return max(0.0, end - start) if start is not None and end is not None else 0.0


def run_rows(run: dict, manifest: dict | None, *, live: bool = False) -> dict[str, list[dict]]:
    """A run's rows in `runs`, `tasks` and `attempts`. `live` describes a run
    still in progress: no record, and no finish time."""

    assets = (manifest or {}).get("assets") or {}
    tasks = run["tasks"]
    ends = {tid: t["attempts"][-1].get("finished_at") for tid, t in tasks.items() if t["attempts"]}
    task_rows, attempt_rows = [], []
    for tid in sorted(tasks):
        task = tasks[tid]
        attempts = task["attempts"]
        executor = _executor((assets.get(task["asset"]) or {}).get("placement"))
        done = [ends[d] for d in task["deps"] if ends.get(d) is not None]
        task_rows.append(
            {
                "id": tid,
                "run": run["id"],
                "asset": task["asset"],
                "scope": task["scope"],
                "status": task["status"],
                "created_at": run["created_at"],
                "ready_at": max([run["created_at"], *done]),
                "started_at": attempts[0].get("started_at") if attempts else None,
                "finished_at": attempts[-1].get("finished_at") if attempts else None,
                "attempts": len(attempts),
                "duration": sum(_span(a) for a in attempts),
                "error": next((a["error"] for a in reversed(attempts) if a.get("error")), None),
                **executor,
            }
        )
        for n, a in enumerate(attempts, 1):
            attempt_rows.append(
                {
                    "id": a["id"],
                    "run": run["id"],
                    "task": tid,
                    "asset": task["asset"],
                    "scope": task["scope"],
                    "n": n,
                    "outcome": a["outcome"],
                    "started_at": a.get("started_at"),
                    "finished_at": a.get("finished_at"),
                    "duration": _span(a),
                    "error": a.get("error"),
                    "executor": executor["executor"],
                }
            )
    status = run["status"]
    quiet = all(
        t["status"] == "skipped" and all(a["outcome"] == "skipped" for a in t["attempts"])
        for t in tasks.values()
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
        "trigger": "automation" if run.get("automation") else "manual",
        "automation": run.get("automation"),
        "by": run.get("by"),
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
        "record": None if live else run,
    }
    return {"runs": [row], "tasks": task_rows, "attempts": attempt_rows}


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
        "tags": {},
        "task_count": 0,
        "failed_count": 0,
        "error": None,
        "record": run,
    }


def materialization(output, asset, scope, head, *, keys=None, rows=None, metadata=None) -> dict:
    """The row of an output version a commit installed: `head` is the head
    as installed, `keys` the commit's key delta for the output."""

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


def legacy_rows(run: dict, manifest: dict | None) -> dict[str, list[dict]]:
    """Rows for a `run.json` written before the history existed: its run,
    tasks and attempts, and one materialization per new output version its
    attempts committed. Lineage was not recorded then."""

    from solera.ids import ulid_time

    if "source" in run:
        at = ulid_time(run["id"])
        rows = {"runs": [commit_row(run, at)]}
        if run.get("version") is not None or run.get("batch") is not None:
            added, removed = run.get("upserted"), run.get("deleted")
            rows["materializations"] = [
                {
                    "output": run["source"],
                    "asset": None,
                    "scope": "",
                    "version": run.get("version"),
                    "store": None,
                    "run": run["id"],
                    "attempt": None,
                    "at": at,
                    "batch": run.get("batch"),
                    "added": len(added) if isinstance(added, list) else added,
                    "removed": len(removed) if isinstance(removed, list) else removed,
                    "rows": None,
                    "complete": True,
                    "metadata": None,
                }
            ]
        return rows
    rows = run_rows(run, manifest)
    seen, made = set(), []
    for tid in sorted(run["tasks"]):
        task = run["tasks"][tid]
        for a in task["attempts"]:
            for output, ref in (a.get("outputs") or {}).items():
                key = (output, task["scope"], ref.get("version"))
                if key in seen:
                    continue
                seen.add(key)
                head = {"ref": ref, "run": run["id"], "attempt": a["id"], "at": a.get("finished_at")}
                made.append(materialization(output, task["asset"], task["scope"], head))
    rows["materializations"] = made
    return rows


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
        self.lake = Lake(state, TABLES, lambda: state.model.history, name="History", clock=clock, **lake)
        self.clock = self.lake.clock

    @property
    def m(self):
        return self.state.model

    async def tick(self) -> None:
        await self.lake.tick()

    async def stop(self) -> None:
        await self.lake.stop()

    async def delete(self, runs: list[str]) -> None:
        """Forget finished runs: their rows go now, or from the files that
        hold them when those are next rewritten."""

        if runs:
            await self.state.emit({"type": "RunsDeleted", "runs": sorted(runs), "at": self.clock()})

    async def backfill(self) -> None:
        """Import the `runs/{run}/run.json` records written before the history
        existed, once, then delete them."""

        m = self.m
        if m.history.imported:
            return
        paths = [p for p in await self.state.list_objects("runs/") if p.endswith("/run.json")]
        known = set(m.runs) | {row["id"] for row in m.history.buffered("runs")}
        rows: dict[str, list[dict]] = {}
        for i in range(0, len(paths), 64):
            chunk = paths[i : i + 64]
            for data in await asyncio.gather(*(self.state.get_object(p) for p in chunk)):
                if data is None:
                    continue
                run = json.loads(data)
                if run["id"] in known:
                    continue
                for table, found in legacy_rows(run, m.manifest).items():
                    rows.setdefault(table, []).extend(found)
        files = {table: await self.lake.write(table, found) for table, found in rows.items()}
        await self.lake.imported(files)
        if paths:
            await self.state.delete_objects(paths)

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

        extra = {}
        if live:
            m = self.m
            for run_id in list(m.runs):
                for table, rows in run_rows(m.runs[run_id], m.manifest, live=True).items():
                    extra.setdefault(table, []).extend(rows)
        return await self.lake.query(work, tables, since=since, until=until, key=run, extra=extra)

    async def run(self, run_id: str) -> dict | None:
        """A finished run's record: what `run.json` used to hold."""

        for row in reversed(self.m.history.buffered("runs")):
            if row["id"] == run_id:
                return row["record"]

        def work(con):
            found = con.execute("SELECT record FROM runs WHERE id = ?", [run_id]).fetchall()
            return found[-1][0] if found else None

        record = await self.query(work, ("runs",), run=run_id, live=False)
        return json.loads(record) if record else None

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
        columns = ", ".join(f'"{c}"' for c in TABLES["runs"].columns if c != "record")

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
        (p50, p95), and the compute they used. `scope` narrows to one
        partition of `asset`."""

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
            quantile_cont(greatest(started_at - ready_at, 0), 0.5) FILTER (WHERE started_at IS NOT NULL) AS wait_p50,
            quantile_cont(greatest(started_at - ready_at, 0), 0.95) FILTER (WHERE started_at IS NOT NULL) AS wait_p95,
            sum(duration) / 3600 AS hours,
            sum(duration * cpu) / 3600 AS cpu_hours,
            sum(duration * memory) / 3600 / 1e9 AS gb_hours,
            sum(duration * gpu) / 3600 AS gpu_hours
        """

        def work(con):
            by_asset = _dicts(
                con.execute(
                    f"SELECT asset, {measures} FROM tasks WHERE {where} GROUP BY 1 ORDER BY 1", params
                )
            )
            by_executor = _dicts(
                con.execute(
                    f"SELECT executor, {measures} FROM tasks WHERE {where} GROUP BY 1 ORDER BY 1", params
                )
            )
            return {"assets": by_asset, "executors": by_executor}

        return await self.query(work, ("tasks",), since=since, until=until, live=False)

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
