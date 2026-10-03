"""What an attempt's inputs read (docs/stores.md, "What a read sees";
docs/versions.md §6).

A snapshot store (FileStore, S3Store) reads what the engine pinned,
exactly. A store that reads the current rows (`reads`, PostgresStore) may
read a newer write: its loads go through one reader per store — one
moment, the inputs read together — and each says which generation's write
it saw, which lineage records. `ctx.load` reads no input of the
attempt's: it is not recorded."""

from __future__ import annotations

import contextlib
from typing import Any

from solera.sdk import is_ref_type


class Observed:
    def __init__(self):
        self.read: dict[tuple[str, str], dict] = {}
        self._stack = contextlib.AsyncExitStack()
        self._readers: dict[str, Any] = {}

    async def load(self, store, ref, t, selection):
        if not callable(getattr(store, "reads", None)) or is_ref_type(t):
            return await store.load(ref, t, selection)
        reader = self._readers.get(ref.store)
        if reader is None:
            reader = self._readers[ref.store] = await self._stack.enter_async_context(store.reads())
        value, generation = await reader.load(ref, t, selection)
        # One moment reads a partition once: the first read of it is what lineage records.
        self.read.setdefault(
            (ref.output, ref.partition or ""),
            {"output": ref.output, "partition": ref.partition or "", "generation": generation},
        )
        return value

    async def close(self) -> None:
        """The moment ends: the readers' transactions close. A load after
        opens another."""

        self._readers = {}
        stack, self._stack = self._stack, contextlib.AsyncExitStack()
        await stack.aclose()

    def report(self) -> list[dict]:
        """For the result: per partition read, the generation it saw."""

        return list(self.read.values())
