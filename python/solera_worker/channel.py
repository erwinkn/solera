"""How a worker reaches the engine (docs/lifecycle.md §5): over HTTPS, or in
process for workers the engine runs itself; and how a pool host asks for
work (§10). Every call is a signal, never the only copy of a fact: a failed
call costs latency, and the worker falls back to its `.beat` object and its
control file (§6, §2.4).

A channel answers `start` and `beat` with `{"cancel": record | None}`,
raises `Ended` when the engine says the attempt is over for this
worker, and any other exception when it cannot be reached."""

from __future__ import annotations

import asyncio

from solera.lifecycle import Ended


class HttpChannel:
    """The engine's HTTPS routes, with the attempt's token."""

    def __init__(self, url: str, project: str, attempt: str, token: str, timeout: float = 10.0):
        import httpx

        self.base = f"{url.rstrip('/')}/api/projects/{project}/attempts/{attempt}"
        self.client = httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=timeout)

    def _post(self, route: str, body: dict, timeout: float | None = None) -> dict:
        extra = {"timeout": timeout} if timeout is not None else {}
        response = self.client.post(f"{self.base}/{route}", json=body, **extra)
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

    async def finished(self, body: dict) -> dict:
        # The answer waits for the commit, to name what is due for cleaning up.
        return await asyncio.to_thread(self._post, "finished", body, 30.0)

    async def cleaned_up(self, body: dict) -> None:
        await asyncio.to_thread(self._post, "cleaned_up", body)

    def _resolve(self, body: bytes) -> bytes:
        from solera.keys.resolver import CONTENT_TYPE

        response = self.client.post(
            f"{self.base}/resolve", content=body, headers={"Content-Type": CONTENT_TYPE}, timeout=30.0
        )
        if response.status_code == 409:
            raise Ended("ended")
        response.raise_for_status()
        return response.content

    async def resolve(self, body: bytes) -> bytes:
        """A small write's delta, from the engine's cache (docs/resolved-commits.md §4)."""

        return await asyncio.to_thread(self._resolve, body)

    def close(self) -> None:
        self.client.close()


class HttpPoolChannel:
    """Pool discovery (docs/lifecycle.md §10): which attempts wait on a pool
    that fit this host, with the pool token."""

    def __init__(self, server: str, project: str, pool: str, token: str | None):
        import httpx

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.path = f"/api/projects/{project}/pools/{pool}/work"
        self.client = httpx.AsyncClient(base_url=server, headers=headers, timeout=60)

    async def work(self, host: str, capacity: dict) -> list[dict]:
        response = await self.client.get(self.path, params={"wait": 30, "host": host, **capacity})
        response.raise_for_status()
        return response.json()["work"]

    async def close(self) -> None:
        await self.client.aclose()


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

    async def finished(self, body: dict) -> dict:
        return await self.engine.attempt_finished(self.attempt, body)

    async def cleaned_up(self, body: dict) -> None:
        await self.engine.attempt_cleaned_up(self.attempt, body)

    async def resolve(self, body: bytes) -> bytes:
        return await self.engine.attempt_resolve(self.attempt, body)

    def close(self) -> None:
        pass
