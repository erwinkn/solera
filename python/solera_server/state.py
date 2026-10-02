"""The engine's state on an object store (docs/object-store-state.md): the
in-memory model (model.py), made durable by the journal (journal.py), next to
the objects attempts, run history and data live in.

Everything lives under `{root}/{namespace}/`:

    control/      the journal and checkpoints (journal.py)
    keys/         key index files (solera.keys, §6)
    history/      the run history: `{table}/{id}.parquet` (history.py, §7)
    runs/{run}/   per attempt `{attempt}.json` (spec, then spec + result +
                  log index) and `{attempt}.log`
    data/ blobs/  store data

`record()` applies events to the model at once — so the engine checks a
precondition and changes state in one synchronous step, with nothing
interleaved — and the journal makes them durable in the background.
`durable()` waits for that, for the few things that act on the outside
world on the strength of an event.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import obstore
from obstore.exceptions import NotFoundError
from obstore.store import LocalStore, MemoryStore
from solera import lifecycle
from solera.objects import create

from .journal import Fenced, Journal, encode
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
        self.changed = asyncio.Event()  # set by every record, for whoever waits on changes

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
        `writer=False` only reads: nothing is fenced and `record` fails."""

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

    def record(self, *events: dict, lazy: bool = False) -> None:
        """Apply events to the model now, and make them durable in the
        background: the one way state changes. A `lazy` event waits for the
        next one to be written with it.

        The whole batch is encoded first, so one the journal cannot hold
        (`ValueError`) changes nothing; then the model applies decoded
        copies. Neither the caller's events nor the model's objects are
        ever the journal's: what is replayed is what was recorded."""

        if self.journal.fenced:
            raise Unavailable("This writer was replaced; restart required")
        encoded = [encode(event) for event in events]
        for data in encoded:
            self.model.apply(json.loads(data))
        self.journal.append(*encoded, lazy=lazy)
        self.changed.set()

    @property
    def recorded(self) -> int:
        return self.journal.appended

    async def durable(self) -> None:
        """Return once everything recorded so far is durable — for what acts
        on the outside world on the strength of it (docs/object-store-state.md §3)."""

        try:
            await self.journal.durable()
        except Fenced as error:
            raise Unavailable("This writer was replaced; restart required") from error

    async def close(self) -> None:
        await self.journal.close()

    # -- attempts (§8) -----------------------------------------------------------------

    def attempt_path(self, run_id: str, attempt: str) -> str:
        return f"runs/{esc(run_id)}/{esc(attempt)}"

    async def delete_run(self, run_id: str) -> list[str]:
        """Delete a run's attempt objects and logs, except its gates, which
        outlive it as tombstones (docs/lifecycle.md §2.4). Returns them."""

        from solera.stores import remove_empty_dirs

        paths = await self.list_objects(f"runs/{esc(run_id)}/")
        gates = [p for p in paths if p.endswith(lifecycle.GATE)]
        await self.delete_objects([p for p in paths if not p.endswith(lifecycle.GATE)])
        if not gates:
            remove_empty_dirs(self.objects, [f"runs/{esc(run_id)}"])
        return gates

    async def attempt_spec(self, run_id: str, attempt: str) -> dict | None:
        data = await self.get_object(f"{lifecycle.base(run_id, attempt)}{lifecycle.SPEC}")
        return json.loads(data) if data is not None else None

    async def attempt_result(self, run_id: str, attempt: str) -> dict | None:
        data = await self.get_object(f"{lifecycle.base(run_id, attempt)}{lifecycle.RESULT}")
        return json.loads(data) if data is not None else None

    async def attempt_finished(self, run_id: str, attempt: str) -> bool:
        return await self.attempt_result(run_id, attempt) is not None

    async def attempt_log(self, run_id: str, attempt: str, tail: int | None = None) -> bytes:
        """An attempt's log as JSON lines: from the chunks its result lists
        and its tail, reading only the chunks the last `tail` lines are in;
        while it runs, the chunks shipped so far (docs/lifecycle.md §2.1)."""

        base = lifecycle.base(run_id, attempt)
        result = await self.attempt_result(run_id, attempt)
        index = (result or {}).get("log")
        if index is not None:
            chunks, lines = list(index["chunks"]), 0
            if tail is not None:
                if index.get("tail"):
                    lines = len(lifecycle.log_text([], index["tail"]).splitlines())
                kept = []
                for entry in reversed(chunks):
                    if lines >= tail:
                        break
                    kept.insert(0, entry)
                    lines += entry[1]
                chunks = kept
            data = [await self.get_object(f"{base}{lifecycle.chunk(n)}") or b"" for n, _, _ in chunks]
            text = lifecycle.log_text(data, index.get("tail"))
        else:
            prefix = f"{base}.log."
            paths = [p for p in await self.list_objects(f"runs/{esc(run_id)}/") if p.startswith(prefix)]
            text = lifecycle.log_text([await self.get_object(p) or b"" for p in sorted(paths)], None)
        if tail is not None:
            text = b"".join(text.splitlines(keepends=True)[-tail:])
        return text

    # -- objects ---------------------------------------------------------------------------

    async def put_object(self, key: str, value: bytes):
        await obstore.put_async(self.objects, key, value, mode="overwrite", use_multipart=False)

    async def create_object(self, key: str, value: bytes):
        """Raises `AlreadyExistsError` if another writer's object is at `key`."""

        await create(self.objects, key, value)

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

    async def delete_objects(self, keys: list[str]):
        for i in range(0, len(keys), 1000):
            await obstore.delete_async(self.objects, keys[i : i + 1000])
