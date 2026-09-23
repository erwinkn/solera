"""The engine's state on an object store (docs/object-store-state.md): the
in-memory model (model.py), made durable by the journal (journal.py), next to
the objects attempts, runs and data live in.

Everything lives under `{root}/{namespace}/`:

    control/      the journal and checkpoints (journal.py)
    runs/{run}/   finished runs (run.json) and attempt files
    specs/ results/ logs/ deltas/ data/ blobs/   attempt I/O and store data

`emit()` applies events to the model at once — so the engine checks a
precondition and changes state in one synchronous step, with nothing
interleaved — and returns once they are durable.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import obstore
from obstore.exceptions import NotFoundError
from obstore.store import LocalStore, MemoryStore

from .journal import Fenced, Journal
from .model import Model


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

    # -- finished runs -----------------------------------------------------------------

    def _run_path(self, run_id: str) -> str:
        return f"runs/{esc(run_id)}/run.json"

    async def archive(self, run_id: str) -> None:
        """Write a finished run to `runs/{run}/run.json`, then drop it from memory."""

        run = self.model.runs.get(run_id)
        if run is None:
            return
        data = json.dumps(run, sort_keys=True, allow_nan=False).encode()
        await obstore.put_async(self.objects, self._run_path(run_id), data, mode="overwrite")
        await self.emit({"type": "RunArchived", "run": run_id})

    async def archived(self, run_id: str) -> dict | None:
        data = await self.get_object(self._run_path(run_id))
        return json.loads(data) if data is not None else None

    async def archived_ids(self) -> list[str]:
        """Every archived run id (ULIDs sort by creation time)."""

        out = []
        result = await obstore.list_with_delimiter_async(self.objects, "runs/")
        for prefix in result["common_prefixes"]:
            out.append(unesc(prefix.rstrip("/").rsplit("/", 1)[-1]))
        return sorted(out)

    # -- objects ---------------------------------------------------------------------------

    async def put_object(self, key: str, value: bytes):
        await obstore.put_async(self.objects, key, value, mode="overwrite", use_multipart=False)

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

    # -- the delta log (until the key index replaces it) ---------------------------------

    async def delta(self, output: str, scope: str, batch: int) -> dict | None:
        from cursus.stores import delta_path

        data = await self.get_object(delta_path(output, scope, batch))
        return json.loads(data) if data is not None else None

    async def delta_key_map(self, output: str, scope: str, hi: int) -> dict[str, str]:
        """The live key map of a keyed incremental output at delta `hi`."""

        keys: dict[str, str] = {}
        for b in range(0, int(hi) + 1):
            delta = await self.delta(output, scope, b)
            if delta is None:
                continue
            for key in delta.get("deleted") or []:
                keys.pop(str(key), None)
            keys.update({str(k): str(v) for k, v in (delta.get("upserted") or {}).items()})
        return keys
