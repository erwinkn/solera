"""Append-only tables on the object store, queried with DuckDB
(docs/object-store-state.md §7).

Rows are appended to a `LakeState`, the durable part held by the model: as
events are applied, each row joins its table's buffer, which the checkpoint
carries. A `Lake`, memory only, does the rest:

- flush: once a buffer holds `flush_rows` rows, or its oldest row has waited
  `flush_seconds`, each table's rows go out as one Parquet file under
  `{prefix}/{table}/`;
- merge: small files are rewritten into bigger ones, `merge_width` neighbours
  of one size tier at a time, on a worker thread;
- forget: rows are deleted by key — at once from the buffer, and from files
  once they are rewritten; until then, each file lists the keys it hides;
- query: a DuckDB connection with a view per table over its files (cached on
  local disk: they never change), its buffer — mirrored in an in-memory
  DuckDB table — and any rows the caller adds.

Nothing outside this module knows how rows are buffered, flushed or merged.
"""

from __future__ import annotations

import asyncio
import bisect
import contextlib
import json
import logging
import math
import os
import tempfile
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from urllib.parse import unquote, urlsplit

import duckdb
from solera.ids import ulid

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Table:
    key: str  # the column rows are forgotten by
    time: str  # the column files are sorted and pruned by
    columns: dict[str, str]  # name -> DuckDB type


class LakeState:
    """A lake's durable part: per table, the files written and the buffered
    rows, `[seq, values]` with values in column order. Changed only by the
    model, as it applies events."""

    def __init__(self, schema: dict[str, Table], snap: dict | None = None):
        snap = snap or {}
        self.schema = schema
        self.files: dict[str, list[dict]] = snap.get("files") or {}
        self.seq: int = snap.get("seq") or 0
        self.rows: dict[str, list[list]] = snap.get("rows") or {}
        # memory only: bumped when rows leave a buffer other than by a flush
        self.generation: dict[str, int] = {}

    def to_json(self) -> dict:
        return {
            "files": self.files,
            "rows": {table: rows for table, rows in self.rows.items() if rows},
            "seq": self.seq,
        }

    def append(self, table: str, row: dict) -> None:
        self.seq += 1
        self.rows.setdefault(table, []).append([self.seq, [row.get(c) for c in self.schema[table].columns]])

    def forget(self, keys: set[str], at: float, tables=None) -> None:
        """Drop the rows keyed by `keys`: buffered ones now, those in files
        once the files are rewritten — until then, the files hide them."""

        for table in tables or self.schema:
            spec = self.schema[table]
            column = list(spec.columns).index(spec.key)
            rows = self.rows.get(table)
            if rows:
                kept = [r for r in rows if r[1][column] not in keys]
                if len(kept) != len(rows):
                    self.rows[table] = kept
                    self.generation[table] = self.generation.get(table, 0) + 1
            for f in self.files.get(table, ()):
                lo, hi = f["keys"]
                hit = sorted(k for k in keys if lo is not None and lo <= k <= hi)
                if hit:
                    f["hidden"] = sorted({*(f.get("hidden") or ()), *hit})
                    f.setdefault("hidden_at", at)

    def flushed(self, files: dict[str, dict], upto: dict[str, int]) -> None:
        for table, f in files.items():
            self.files.setdefault(table, []).append(f)
            self.rows[table] = [r for r in self.rows.get(table, ()) if r[0] > upto[table]]

    def compacted(self, changes: list[dict]) -> list[str]:
        """Swap merged files in, each where the first it replaces was; the
        paths replaced, now garbage."""

        gone = []
        for change in changes:
            files = self.files.get(change["table"], [])
            removed = set(change["removed"])
            at = next((i for i, f in enumerate(files) if f["path"] in removed), len(files))
            kept = [f for f in files if f["path"] not in removed]
            if change.get("added"):
                kept.insert(at, change["added"])
            self.files[change["table"]] = kept
            gone.extend(sorted(removed))
        return gone


class Lake:
    """A lake's write path and queries. Memory only: `held()` returns the
    `LakeState`, and every change to it goes through an event the model
    applies: `{name}Flushed` and `{name}Compacted`."""

    def __init__(
        self,
        state,
        schema: dict[str, Table],
        held,
        *,
        name: str,
        clock=None,
        cache: str | None = None,
        flush_rows: int = 2_000,
        flush_seconds: float = 60.0,
        purge_seconds: float = 3600.0,
        merge_width: int = 4,
        base_rows: int = 1_000,
        final_rows: int = 1_000_000,
        volatile: tuple[str, ...] = (),
        pin=contextlib.nullcontext,
    ):
        self.state, self.schema, self.held = state, schema, held
        # files go under `{prefix}/{table}/`; events are `{name}Flushed` and so on
        self.name, self.prefix = name, name.lower()
        self.clock = clock or time.time
        self.flush_rows, self.flush_seconds = flush_rows, flush_seconds
        self.purge_seconds = purge_seconds
        self.merge_width, self.base_rows, self.final_rows = merge_width, base_rows, final_rows
        u = urlsplit(state.objects_url)
        # `file://` state is read in place; anything else is copied to local disk.
        self.root = Path(unquote(u.path)) if u.scheme == "file" else None
        if cache is None and self.root is None:
            if u.scheme == "memory":
                cache = tempfile.mkdtemp(prefix=f"solera-{self.prefix}-")
            else:
                digest = sha256(state.objects_url.encode()).hexdigest()[:16]
                cache = os.path.join(tempfile.gettempdir(), f"solera-{self.prefix}", digest)
        self.cache = Path(cache) if cache else None
        self.check_seconds = min(flush_seconds, 1.0)
        self._task: asyncio.Task | None = None  # the background loop, once started
        self.job: asyncio.Task | None = None  # the merge under way
        self.last_error: str | None = None
        self._db = None  # in-memory DuckDB mirroring the buffers
        self._mirrored: dict[str, tuple] = {}  # table -> (state, generation, first seq, last seq)
        # rows of `volatile` tables: memory only, never journaled, until a flush writes them
        self.volatile: dict[str, list[list]] = {table: [] for table in volatile}
        self.pin = pin  # a context in which the files a query chose are not collected

    # -- write path ----------------------------------------------------------------------

    def start(self) -> None:
        """Flush and merge in the background from here on."""

        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.check_seconds)
            try:
                await self.tick()
            except Exception as error:
                self.last_error = f"{self.prefix}: {type(error).__name__}: {error}"
                log.exception("%s flush failed", self.prefix)

    async def tick(self) -> None:
        await self.flush()
        self.maintain()

    async def stop(self) -> None:
        jobs = [j for j in (self._task, self.job) if j is not None]
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        self._task = None

    def buffer(self, table: str, row: dict) -> None:
        """Buffer a row of a volatile table: no event, so a crash loses it."""

        self.volatile[table].append([row.get(c) for c in self.schema[table].columns])

    def unwritten(self, table: str) -> list[dict]:
        names = list(self.schema[table].columns)
        return [dict(zip(names, values, strict=True)) for values in self.volatile.get(table, ())]

    async def flush(self, force: bool = False) -> None:
        """Write the buffered rows out, one file per table, once there are
        `flush_rows` of them or the oldest has waited `flush_seconds`."""

        lake = self.held()
        pending = {t: rows for t, rows in lake.rows.items() if rows}
        volatile = {t: list(rows) for t, rows in self.volatile.items() if rows}
        count = sum(len(rows) for rows in pending.values()) + sum(len(rows) for rows in volatile.values())
        if not count:
            return
        now = self.clock()
        oldest = min(
            [self._time(t, rows[0][1]) or now for t, rows in pending.items()]
            + [self._time(t, rows[0]) or now for t, rows in volatile.items()]
        )
        if not force and count < self.flush_rows and now - oldest < self.flush_seconds:
            return
        upto = {t: rows[-1][0] for t, rows in pending.items()}
        batches = {t: [values for _, values in rows] for t, rows in pending.items()}
        for t, rows in volatile.items():
            batches[t] = batches.get(t, []) + rows
            upto.setdefault(t, 0)
        files = {}
        try:
            for table, rows in batches.items():
                files[table] = await self._write(table, rows)
        except BaseException:
            await self._discard([f["path"] for f in files.values()])
            raise
        # Rows forgotten meanwhile must not come back with this file.
        for table, rows in pending.items():
            left = sum(1 for seq, _ in self.held().rows.get(table, ()) if seq <= upto[table])
            if left != len(rows):
                await self._discard([f["path"] for f in files.values()])
                return
        self.state.record({"type": f"{self.name}Flushed", "files": files, "upto": upto})
        for table, rows in volatile.items():
            del self.volatile[table][: len(rows)]

    def _time(self, table: str, values: list):
        spec = self.schema[table]
        return values[list(spec.columns).index(spec.time)]

    async def _write(self, table: str, rows: list[list]) -> dict:
        path = f"{self.prefix}/{table}/{ulid(self.clock())}.parquet"
        data, stats = await asyncio.to_thread(self._encode, table, rows)
        await self.state.create_object(path, data)
        self._keep(path, data)
        return {"path": path, **stats, "bytes": len(data)}

    def _encode(self, table: str, rows: list[list]) -> tuple[bytes, dict]:
        """Rows as a Parquet file, sorted by the table's time column."""

        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "rows.json")
            self._ndjson(source, table, rows)
            con = duckdb.connect()
            try:
                return self._copy(con, table, self._json_sql(table, [source]), tmp)
            finally:
                con.close()

    def _ndjson(self, path: str, table: str, rows, seqs=None) -> None:
        names = list(self.schema[table].columns)
        with open(path, "w") as out:
            for i, values in enumerate(rows):
                row = dict(zip(names, values, strict=True))
                if seqs is not None:
                    row["_seq"] = seqs[i]
                out.write(json.dumps(row, allow_nan=False, default=str))
                out.write("\n")

    def _copy(self, con, table: str, sql: str, tmp: str) -> tuple[bytes, dict]:
        spec = self.schema[table]
        target = os.path.join(tmp, "out.parquet")
        con.execute(
            f'COPY (SELECT * FROM ({sql}) ORDER BY "{spec.time}") TO {_sql_str(target)} '
            "(FORMAT parquet, COMPRESSION zstd)"
        )
        rows, lo, hi, first, last = con.execute(
            f'SELECT count(*), min("{spec.time}"), max("{spec.time}"), min("{spec.key}"), max("{spec.key}") '
            f"FROM read_parquet({_sql_str(target)})"
        ).fetchone()
        stats = {"rows": rows, "at": [lo, hi], "keys": [first, last]}
        return Path(target).read_bytes(), stats

    async def _discard(self, paths: list[str]) -> None:
        with contextlib.suppress(Exception):
            await self.state.delete_objects(paths)
        for path in paths:
            self._evict(path)

    def maintain(self) -> None:
        """Start merging small files, and rewriting those hiding forgotten
        rows, unless that is already under way."""

        if self.job is not None:
            return
        plan = self.plan()
        if not plan:
            return
        self.job = asyncio.create_task(self._compact(plan))

        def done(_job):
            self.job = None

        self.job.add_done_callback(done)

    def plan(self) -> list[tuple[str, list[dict]]]:
        """Groups of files to rewrite as one: `merge_width` neighbours of one
        size tier (rows grow by `merge_width` per tier, up to `final_rows`),
        and any file that has hidden rows for `purge_seconds` or hides more
        than a thousand keys."""

        now = self.clock()
        out = []
        for table, files in self.held().files.items():
            grouped = set()
            run, tier = [], None
            for f in files:
                t = self._tier(f["rows"])
                if t is None or t != tier:
                    run, tier = [], t
                if t is None:
                    continue
                run.append(f)
                if len(run) == self.merge_width:
                    out.append((table, [dict(x) for x in run]))
                    grouped.update(x["path"] for x in run)
                    run, tier = [], None
            for f in files:
                hidden = f.get("hidden")
                if not hidden or f["path"] in grouped:
                    continue
                if len(hidden) >= 1000 or now - (f.get("hidden_at") or now) >= self.purge_seconds:
                    out.append((table, [dict(f)]))
        return out

    def _tier(self, rows: int) -> int | None:
        if rows >= self.final_rows:
            return None
        return int(math.log(max(rows, 1) / self.base_rows, self.merge_width)) if rows > self.base_rows else 0

    async def _compact(self, plan) -> None:
        try:
            for _, group in plan:
                await self._fetch([f["path"] for f in group])
            results = await asyncio.to_thread(self._merge, plan)
            changes, created = [], []
            for (table, group), (data, stats) in zip(plan, results, strict=True):
                added = None
                if stats["rows"]:
                    path = f"{self.prefix}/{table}/{ulid(self.clock())}.parquet"
                    await self.state.create_object(path, data)
                    self._keep(path, data)
                    created.append(path)
                    added = {"path": path, **stats, "bytes": len(data)}
                changes.append({"table": table, "removed": [f["path"] for f in group], "added": added})
            # Unless every file it replaces is unchanged — nothing hidden in
            # it since — the rewrite is stale.
            current = {f["path"]: f for files in self.held().files.values() for f in files}
            for _, group in plan:
                for f in group:
                    now = current.get(f["path"])
                    if now is None or len(now.get("hidden") or ()) != len(f.get("hidden") or ()):
                        await self._discard(created)
                        return
            self.state.record({"type": f"{self.name}Compacted", "changes": changes, "at": self.clock()})
            for change in changes:
                for path in change["removed"]:
                    self._evict(path)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.last_error = f"{self.prefix}: {type(error).__name__}: {error}"
            log.exception("%s compaction failed", self.prefix)

    def _merge(self, plan) -> list[tuple[bytes, dict]]:
        out = []
        for table, group in plan:
            with tempfile.TemporaryDirectory() as tmp:
                con = duckdb.connect()
                try:
                    files = [(self._local(f["path"]), f.get("hidden") or []) for f in group]
                    out.append(self._copy(con, table, self._files_sql(table, files), tmp))
                finally:
                    con.close()
        return out

    # -- local files -------------------------------------------------------------------------

    def _local(self, path: str) -> str:
        return str((self.root if self.root is not None else self.cache) / path)

    def _keep(self, path: str, data: bytes) -> None:
        """Cache a file this process just wrote."""

        if self.root is not None:
            return
        target = Path(self._local(path))
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_bytes(data)
        tmp.replace(target)

    def _evict(self, path: str) -> None:
        if self.root is None:
            with contextlib.suppress(OSError):
                os.unlink(self._local(path))

    async def _fetch(self, paths: list[str]) -> None:
        if self.root is not None:
            return
        missing = [p for p in paths if not os.path.exists(self._local(p))]
        for i in range(0, len(missing), 16):
            chunk = missing[i : i + 16]
            for path, data in zip(
                chunk, await asyncio.gather(*(self.state.get_object(p) for p in chunk)), strict=True
            ):
                if data is not None:
                    self._keep(path, data)

    # -- queries -------------------------------------------------------------------------------

    def _columns_sql(self, table: str) -> str:
        return ", ".join(f"{_sql_str(c)}: {_sql_str(t)}" for c, t in self.schema[table].columns.items())

    def _json_sql(self, table: str, paths: list[str]) -> str:
        listed = ", ".join(_sql_str(p) for p in paths)
        return (
            f"SELECT * FROM read_json([{listed}], format='newline_delimited', "
            f"columns={{{self._columns_sql(table)}}})"
        )

    def _files_sql(self, table: str, files: list[tuple[str, list[str]]], extra: list[str] = ()) -> str:
        """One table over Parquet `files` — `(path, hidden keys)` — and any
        `extra` SELECTs, with every column, typed."""

        spec = self.schema[table]
        parts = [
            "SELECT " + ", ".join(f'NULL::{t} AS "{c}"' for c, t in spec.columns.items()) + " WHERE false"
        ]
        clean = [p for p, hidden in files if not hidden]
        if clean:
            listed = ", ".join(_sql_str(p) for p in clean)
            parts.append(f"SELECT * FROM read_parquet([{listed}], union_by_name=true)")
        for path, hidden in files:
            if hidden:
                keys = ", ".join(_sql_str(k) for k in hidden)
                parts.append(
                    f'SELECT * FROM read_parquet({_sql_str(path)}) WHERE "{spec.key}" NOT IN ({keys})'
                )
        parts.extend(extra)
        union = " UNION ALL BY NAME ".join(f"({p})" for p in parts)
        return f"SELECT {', '.join(f'"{c}"' for c in spec.columns)} FROM ({union})"

    def _database(self):
        if self._db is None:
            self._db = duckdb.connect()
        return self._db

    def _mirror(self, table: str) -> None:
        """Bring the in-memory copy of `table`'s buffer up to date: drop what
        was flushed, add what was appended — or, after rows were forgotten
        or the state replaced, copy it anew."""

        lake, db = self.held(), self._database()
        rows = lake.rows.get(table) or []
        buffer = f'"{table}__buffer"'
        generation = lake.generation.get(table, 0)
        seen = self._mirrored.get(table)
        if seen is None:
            columns = ", ".join(f'"{c}" {t}' for c, t in self.schema[table].columns.items())
            db.execute(f"CREATE TABLE {buffer} ({columns}, _seq BIGINT)")
        if seen is None or seen[0] is not lake or seen[1] != generation:
            db.execute(f"DELETE FROM {buffer}")
            last = 0
        else:
            last = seen[3]
            if rows and rows[0][0] > seen[2]:
                db.execute(f"DELETE FROM {buffer} WHERE _seq < ?", [rows[0][0]])
            elif not rows and seen[2] <= seen[3]:
                db.execute(f"DELETE FROM {buffer}")
        start = bisect.bisect_right(rows, last, key=lambda r: r[0])
        new = rows[start:]
        if new:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "rows.json")
                self._ndjson(path, table, [v for _, v in new], [s for s, _ in new])
                columns = self._columns_sql(table)
                db.execute(
                    f"INSERT INTO {buffer} SELECT * FROM read_json({_sql_str(path)}, "
                    f"format='newline_delimited', columns={{{columns}, '_seq': 'BIGINT'}})"
                )
        first = rows[0][0] if rows else lake.seq + 1
        self._mirrored[table] = (lake, generation, first, max(last, rows[-1][0] if rows else last))

    async def query(
        self,
        work,
        tables,
        *,
        since: float | None = None,
        until: float | None = None,
        key: str | None = None,
        extra: dict[str, list[dict]] | None = None,
    ):
        """Run `work(con)` on a worker thread against a DuckDB connection with
        a view per table: its files, its buffer, and `extra` rows (objects).
        Files outside `since`/`until`, or unable to hold `key`, are left out.

        One snapshot, taken before the first await: the files chosen, their
        hidden keys and the buffers (kept by the transaction) agree, so a
        flush during the download can neither hide a row nor show it twice.
        The files chosen are pinned until the query ends."""

        with self.pin():
            lake = self.held()
            files, chosen = {}, []
            for table in tables:
                picked = []
                for f in lake.files.get(table, ()):
                    lo, hi = f["at"]
                    if since is not None and hi is not None and hi < since:
                        continue
                    if until is not None and lo is not None and lo >= until:
                        continue
                    if key is not None and not (f["keys"][0] <= key <= f["keys"][1]):
                        continue
                    picked.append(f)
                files[table] = [(self._local(f["path"]), list(f.get("hidden") or ())) for f in picked]
                chosen += [f["path"] for f in picked]
            for table in tables:
                self._mirror(table)
            con = self._database().cursor()
            try:
                con.execute("BEGIN TRANSACTION")
                for table in tables:
                    con.execute(f'SELECT 1 FROM "{table}__buffer" LIMIT 0').fetchall()
                extra = {t: rows for t, rows in (extra or {}).items() if rows and t in tables}
                await self._fetch(chosen)
            except BaseException:
                con.close()
                raise
            return await asyncio.to_thread(self._answer, con, work, tables, files, extra)

    def _answer(self, con, work, tables, files, extra):

        with tempfile.TemporaryDirectory() as tmp:
            try:
                for table in tables:
                    parts = [f'SELECT * EXCLUDE (_seq) FROM "{table}__buffer"']
                    if table in extra:
                        path = os.path.join(tmp, f"{table}.json")
                        names = list(self.schema[table].columns)
                        self._ndjson(path, table, [[r.get(c) for c in names] for r in extra[table]])
                        parts.append(self._json_sql(table, [path]))
                    con.execute(f"CREATE TEMP VIEW {table} AS {self._files_sql(table, files[table], parts)}")
                return work(con)
            finally:
                con.close()


def _sql_str(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"
