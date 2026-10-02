"""How a worker reaches the engine (docs/lifecycle.md §5): over HTTPS, or in
process for workers the engine runs itself. Every call is a signal, never
the only copy of a fact: a failed call costs latency, and the worker falls
back to its `.worker` object (§6).

A channel answers `start` and `beat` with `{"cancel": record | None}`,
raises `Ended` when the engine says the attempt is over for this
invocation, and any other exception when it cannot be reached."""

from __future__ import annotations

import asyncio

from solera.lifecycle import Ended


class HttpChannel:
    """The engine's HTTPS routes, with the attempt's token."""

    def __init__(self, url: str, project: str, attempt: str, token: str, timeout: float = 10.0):
        import httpx

        self.base = f"{url.rstrip('/')}/api/projects/{project}/attempts/{attempt}"
        self.client = httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=timeout)

    def _post(self, route: str, body: dict) -> dict:
        response = self.client.post(f"{self.base}/{route}", json=body)
        if response.status_code == 409:
            raise Ended((response.json().get("detail") or "ended") if response.content else "ended")
        response.raise_for_status()
        return response.json() if response.content else {}

    def beat(self, body: dict) -> dict:  # from the reporting thread
        return self._post("beat", body)

    async def start(self, body: dict) -> dict:
        return await asyncio.to_thread(self._post, "start", body)

    async def logs(self, body: dict) -> dict:
        return await asyncio.to_thread(self._post, "logs", body)

    async def finished(self, body: dict) -> None:
        await asyncio.to_thread(self._post, "finished", body)

    def close(self) -> None:
        self.client.close()


class LocalChannel:
    """The engine's handlers, called in process: for workers an engine runs
    on its own event loop (tests, the inline placement)."""

    def __init__(self, engine, attempt: str):
        self.engine, self.attempt = engine, attempt
        self.loop = asyncio.get_running_loop()

    def beat(self, body: dict) -> dict:  # from the reporting thread: run on the engine's loop
        future = asyncio.run_coroutine_threadsafe(self.engine.attempt_beat(self.attempt, body), self.loop)
        return future.result(timeout=10)

    async def start(self, body: dict) -> dict:
        return await self.engine.attempt_start(self.attempt, body)

    async def logs(self, body: dict) -> dict:
        return await self.engine.attempt_logs(self.attempt, body)

    async def finished(self, body: dict) -> None:
        await self.engine.attempt_finished(self.attempt, body)

    def close(self) -> None:
        pass
