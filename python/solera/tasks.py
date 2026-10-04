"""A component's background tasks (the background-task audit, P1).

asyncio keeps only weak references to tasks: one nobody holds can be
collected mid-run and its work dropped without a word. A `Tasks` holds
each task it spawns until it ends, logs an exception the moment the task
dies of one, and cancels what is left in the order it was spawned — never
a set's order, which no seed pins. One per component, each closed where
the component stops, so a process's shutdown order stays its own."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Hashable, Iterator

log = logging.getLogger(__name__)


class Tasks:
    def __init__(self, name: str):
        self.name = name
        self._tasks: dict[Hashable, asyncio.Task] = {}  # insertion-ordered

    def spawn(self, coro: Coroutine, key: Hashable | None = None, *, awaited: bool = False) -> asyncio.Task:
        """Run `coro` as a task held until it ends. A `key` names it, for
        `get` and `in`, while it runs; without one, the task is its own key.
        `awaited`: its exception reaches whoever awaits it (a shared fill,
        a fetch read later), so it is not logged here as well."""

        try:
            loop = asyncio.get_running_loop()  # none from a thread: RuntimeError, as create_task
            if key is not None and key in self._tasks:
                raise ValueError(f"{self.name}: {key!r} is running already")
        except (RuntimeError, ValueError):
            coro.close()  # never to run: no "never awaited" warning
            raise
        task = loop.create_task(coro, name=f"{self.name}:{key}")
        self._tasks[task if key is None else key] = task
        task.add_done_callback(lambda t, k=(task if key is None else key): self._ended(k, t, awaited))
        return task

    def _ended(self, key: Hashable, task: asyncio.Task, awaited: bool) -> None:
        if self._tasks.get(key) is task:
            del self._tasks[key]
        if not awaited and not task.cancelled() and (error := task.exception()) is not None:
            log.error("%s failed", task.get_name(), exc_info=error)

    def get(self, key: Hashable) -> asyncio.Task | None:
        return self._tasks.get(key)

    def __contains__(self, key: Hashable) -> bool:
        return key in self._tasks

    def __iter__(self) -> Iterator[Hashable]:
        return iter(list(self._tasks))

    def values(self) -> list[asyncio.Task]:
        """The tasks still running, in the order spawned."""

        return list(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)

    async def close(self) -> None:
        """Cancel every task left, in the order spawned, and wait for them."""

        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
