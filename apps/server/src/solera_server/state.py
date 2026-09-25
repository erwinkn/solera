"""The engine's state on an object store (docs/object-store-state.md): the
in-memory model (model.py), made durable by the journal (journal.py), next to
the objects attempts, runs and data live in.

Everything lives under `{root}/{namespace}/`:

    control/      the journal and checkpoints (journal.py)
    keys/         key index files (solera.keys, §6)
    runs/{run}/   run.json once finished; per attempt `{attempt}.json` (spec,
                  then spec + result + log index) and `{attempt}.log`
    data/ blobs/  store data

`emit()` applies events to the model at once — so the engine checks a
precondition and changes state in one synchronous step, with nothing
interleaved — and returns once they are durable.
"""

from __future__ import annotations

import json
import re
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import obstore
from obstore.exceptions import NotFoundError
from obstore.store import LocalStore, MemoryStore

from .journal import Fenced, Journal
from .model import Model

RECENT_RUNS = 500  # finished runs that wrote nothing, kept in memory for the console


class Unavailable(RuntimeError):
    """This writer was replaced (fenced) or cannot make state durable; restart required."""


class LostOwnership(Exception):
    """The attempt no longer owns its scope (fencing, §8)."""


class Conflict(Exception):
    """A commit precondition failed (moved head, stale claim).

    retryable=True for races (a head moved under the attempt); False for
    violations no retry can fix (undeclared output, missing prior head)."""

    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


def esc(value) -> str:
    return quote(str(value), safe="")


def unesc(value: str) -> str:
    return unquote(value)


def open_store(url: str, namespace: str):
    """The object store rooted at `{url}/{namespace}`, and the URL workers use for it."""

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", namespace):
        raise ValueError("Namespace must contain 1–64 letters, digits, underscores or hyphens")
    u = urlsplit(url)
    if u.query or u.fragment or u.username or u.password:
        raise ValueError("Credentials and query parameters do not belong in storage URLs")
    if u.scheme == "file":
        if u.netloc not in ("", "localhost") or not u.path.startswith("/"):
            raise ValueError("File storage requires an absolute local path")
        root = Path(unquote(u.path)).resolve() / namespace
        root.mkdir(parents=True, exist_ok=True)
        return LocalStore(root, mkdir=True), root.as_uri()
    if u.scheme == "s3" and u.netloc:
        prefix = u.path.strip("/")
        if ".." in prefix.split("/"):
            raise ValueError("Invalid object prefix")
        base = "/".join(filter(None, (prefix, namespace)))
        objects_url = f"s3://{u.netloc}/{base}"
        return obstore.store.from_url(objects_url), objects_url
    if u.scheme == "memory":
        return MemoryStore(), "memory:///"
    raise ValueError("Use file:///absolute/path, s3://bucket/prefix, or memory:///")


class State:
    """The model, its journal, and the object store — one per namespace."""

    def __init__(
        self, store, *, url: str, namespace: str, objects_url: str, journal: Journal, model: Model, clock
    ):
        self.objects = store
        self.url, self.namespace, self.objects_url = url, namespace, objects_url
        self.journal, self.model, self.clock = journal, model, clock
        self.recent: OrderedDict[str, dict] = OrderedDict()

    @classmethod
    async def open(
        cls,
        url: str,
        namespace: str = "default",
        *,
        clock=None,
        flush_interval: float = 1.0,
        min_checkpoint: int = 256 << 10,
        writer: bool = True,
    ) -> State:
        """Load the newest checkpoint, replay the journal, and fence out any
        earlier writer: from here on this process is the namespace's writer.
        `writer=False` only reads: nothing is fenced and `emit` fails."""

        clock = clock or time.time
        store, objects_url = open_store(url, namespace)
        model = Model()
        journal = Journal(
            store, "control", flush_interval=flush_interval, min_checkpoint=min_checkpoint, clock=clock
        )
        await journal.open(model.restore, model.apply, model.snapshot, writer=writer)
        return cls(
            store,
            url=url,
            namespace=namespace,
            objects_url=objects_url,
            journal=journal,
            model=model,
            clock=clock,
        )

    @property
    def poisoned(self) -> bool:
        return self.journal.fenced

    async def emit(self, *events: dict) -> None:
        """Apply events to the model now; return once they are durable."""

        if self.journal.fenced:
            raise Unavailable("This writer was replaced; restart required")
        for event in events:
            self.model.apply(event)
        try:
            await self.journal.durable(*events)
        except Fenced as error:
            raise Unavailable("This writer was replaced; restart required") from error

    async def close(self) -> None:
        await self.journal.close()

    # -- runs and attempts (§7, §8) -------------------------------------------------

    def _run_path(self, run_id: str) -> str:
        return f"runs/{esc(run_id)}/run.json"

    def attempt_path(self, run_id: str, attempt: str) -> str:
        return f"runs/{esc(run_id)}/{esc(attempt)}"

    async def archive(self, run_id: str, *, committed=(), write: bool = True) -> None:
        """Write a finished run to `runs/{run}/run.json`, then drop it from
        memory. A run that wrote nothing (every task skipped) is not written:
        it stays in the recent list only."""

        run = self.model.runs.get(run_id)
        if run is None:
            return
        if write:
            data = json.dumps(run, sort_keys=True, allow_nan=False).encode()
            await obstore.put_async(self.objects, self._run_path(run_id), data, mode="overwrite")
        else:
            self.recent[run_id] = run
            while len(self.recent) > RECENT_RUNS:
                self.recent.popitem(last=False)
        await self.emit({"type": "RunArchived", "run": run_id, "committed": sorted(committed)})

    async def archived(self, run_id: str) -> dict | None:
        if run_id in self.recent:
            return self.recent[run_id]
        data = await self.get_object(self._run_path(run_id))
        return json.loads(data) if data is not None else None

    async def archived_ids(self) -> list[str]:
        """Every run with a directory under `runs/` (ULIDs sort by creation time)."""

        out = []
        result = await obstore.list_with_delimiter_async(self.objects, "runs/")
        for prefix in result["common_prefixes"]:
            out.append(unesc(prefix.rstrip("/").rsplit("/", 1)[-1]))
        return sorted(out)

    async def delete_run(self, run_id: str) -> None:
        """Delete a run's record, attempt files and logs."""

        from solera.stores import remove_empty_dirs

        self.recent.pop(run_id, None)
        await self.delete_objects(await self.list_objects(f"runs/{esc(run_id)}/"))
        remove_empty_dirs(self.objects, [f"runs/{esc(run_id)}"])

    async def attempt_record(self, run_id: str, attempt: str) -> dict | None:
        data = await self.get_object(f"{self.attempt_path(run_id, attempt)}.json")
        return json.loads(data) if data is not None else None

    async def attempt_finished(self, run_id: str, attempt: str) -> bool:
        record = await self.attempt_record(run_id, attempt)
        return record is not None and "result" in record

    async def attempt_log(self, run_id: str, attempt: str, tail: int | None = None) -> bytes:
        """An attempt's log as JSON lines: the joined log, reading only the
        blocks the last `tail` lines are in, or the chunks shipped so far
        while it runs (§8)."""

        import gzip

        base = self.attempt_path(run_id, attempt)
        record = await self.attempt_record(run_id, attempt)
        index = (record or {}).get("log")
        if index is not None:
            if not index["blocks"]:
                return b""
            start = 0
            if tail is not None:
                lines = 0
                for offset, count, _ in reversed(index["blocks"]):
                    start, lines = offset, lines + count
                    if lines >= tail:
                        break
            data = bytes(
                await obstore.get_range_async(self.objects, f"{base}.log", start=start, end=index["bytes"])
            )
            text = gzip.decompress(data)
        else:
            name = f"{esc(attempt)}.log."
            chunks = [
                p
                for p in await self.list_objects(f"runs/{esc(run_id)}/")
                if p.rsplit("/", 1)[-1].startswith(name)
            ]
            text = b"".join([gzip.decompress(await self.get_object(p) or b"") for p in sorted(chunks)])
        if tail is not None:
            text = b"".join(text.splitlines(keepends=True)[-tail:])
        return text

    # -- objects ---------------------------------------------------------------------------

    async def put_object(self, key: str, value: bytes):
        await obstore.put_async(self.objects, key, value, mode="overwrite", use_multipart=False)

    async def create_object(self, key: str, value: bytes):
        await obstore.put_async(self.objects, key, value, mode="create", use_multipart=False)

    async def get_object(self, key: str) -> bytes | None:
        try:
            result = await obstore.get_async(self.objects, key)
        except (NotFoundError, FileNotFoundError):
            return None
        return bytes(await result.bytes_async())

    async def list_objects(self, prefix: str) -> list[str]:
        out = []
        async for batch in obstore.list(self.objects, prefix=prefix):
            out.extend(meta["path"] for meta in batch)
        return sorted(out)

    async def list_object_meta(self, prefix: str) -> list[dict]:
        out = []
        async for batch in obstore.list(self.objects, prefix=prefix):
            out.extend(dict(meta) for meta in batch)
        return sorted(out, key=lambda m: m["path"])

    async def delete_objects(self, keys: list[str]):
        for i in range(0, len(keys), 1000):
            await obstore.delete_async(self.objects, keys[i : i + 1000])
