"""How a worker reaches the engine (docs/lifecycle.md §5): an attempt's
routes, a pool host asking for work (§10), and a sensor host asking for
ticks (§11). Every call is a signal, never the only copy of a fact: a failed
call costs latency, and the worker falls back to its `.beat` object and its
control file (§6, §2.4).

One channel per kind of call builds the requests; a transport carries
them. `HttpTransport` goes over HTTPS. `LocalTransport` hands the same
request — method, path, token, body — to the engine's own app in this
process, for workers the engine runs itself: the same routes, the same
authentication, the same handlers, no network.

A channel answers `start` and `beat` with `{"cancel": record | None}`,
raises `Ended` when the engine says the attempt is over for this worker
(`409`), and any other exception when it cannot be reached."""

from __future__ import annotations

import asyncio

from solera.lifecycle import Ended


class HttpTransport:
    """The engine's routes over HTTPS, with a token. `transport`: an httpx
    transport to send through instead of the network (tests)."""

    def __init__(self, url: str, token: str | None, timeout: float = 10.0, transport=None):
        import httpx

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        kw = {"base_url": url.rstrip("/"), "headers": headers, "timeout": timeout}
        if transport is not None:
            kw["transport"] = transport
        self.client = httpx.Client(**kw)  # for the reporting thread's beats
        self.async_client = httpx.AsyncClient(**kw)

    def send(self, method: str, path: str, **kw):
        return self.client.request(method, path, **kw)

    async def asend(self, method: str, path: str, **kw):
        return await self.async_client.request(method, path, **kw)

    async def close(self) -> None:
        self.client.close()
        await self.async_client.aclose()


class LocalTransport:
    """The engine's app in this process (an ASGI app), with a token. `send`,
    from another thread, runs the request on the loop the transport was made
    on: the engine's."""

    def __init__(self, app, token: str | None, timeout: float = 10.0):
        import httpx

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.loop, self.timeout = asyncio.get_running_loop(), timeout
        self.async_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://engine", headers=headers
        )

    def send(self, method: str, path: str, **kw):
        future = asyncio.run_coroutine_threadsafe(self.asend(method, path, **kw), self.loop)
        return future.result(timeout=kw.get("timeout") or self.timeout)

    async def asend(self, method: str, path: str, **kw):
        return await self.async_client.request(method, path, **kw)

    async def close(self) -> None:
        await self.async_client.aclose()


def _answer(response):
    """The engine's answer, or `Ended` (`409`), or the HTTP error."""

    if response.status_code == 409:
        raise Ended((response.json().get("detail") or "ended") if response.content else "ended")
    response.raise_for_status()
    return response


def _json(response) -> dict:
    response = _answer(response)
    return response.json() if response.content else {}


class AttemptChannel:
    """An attempt's routes (§5.1), with the attempt's own token."""

    def __init__(self, transport, project: str, attempt: str):
        self.transport, self.base = transport, f"/api/projects/{project}/attempts/{attempt}"

    def beat(self, body: dict) -> dict:  # from the reporting thread
        return _json(self.transport.send("POST", f"{self.base}/beat", json=body))

    async def start(self, body: dict) -> dict:
        return await self._post("start", body)

    async def logs(self, body: dict) -> dict:
        return await self._post("logs", body)

    async def finished(self, body: dict) -> None:
        await self._post("finished", body)

    async def resolve(self, body: bytes) -> bytes:
        """A small write's delta, from the engine's cache (docs/resolved-commits.md §4)."""

        from solera.keys.resolver import CONTENT_TYPE

        headers = {"Content-Type": CONTENT_TYPE}
        response = await self.transport.asend(
            "POST", f"{self.base}/resolve", content=body, headers=headers, timeout=30.0
        )
        return _answer(response).content

    async def close(self) -> None:
        await self.transport.close()

    async def _post(self, route: str, body: dict, timeout: float | None = None) -> dict:
        extra = {"timeout": timeout} if timeout is not None else {}
        return _json(await self.transport.asend("POST", f"{self.base}/{route}", json=body, **extra))


class PoolChannel:
    """Pool discovery (§10): which attempts wait on a pool that fit this
    host, with the pool token."""

    def __init__(self, transport, project: str, pool: str):
        self.transport, self.path = transport, f"/api/projects/{project}/pools/{pool}/work"

    async def work(self, host: str, capacity: dict) -> list[dict]:
        params = {"wait": 30, "host": host, **capacity}
        return _json(await self.transport.asend("GET", self.path, params=params))["work"]

    async def close(self) -> None:
        await self.transport.close()


class SensorChannel:
    """A sensor host's routes (§11.2), with the pool token or the engine's own
    host's."""

    def __init__(self, transport, project: str):
        self.transport, self.base = transport, f"/api/projects/{project}/sensors"

    async def next(self, executor: str, deploy: str, host: str, slots: int, build: str | None = None) -> dict:
        params = {"executor": executor, "deploy": deploy, "host": host, "slots": slots, "wait": 30}
        if build:
            params["build"] = build  # how this host computed its deploy, for the engine's warning
        return _json(await self.transport.asend("GET", f"{self.base}/next", params=params))

    async def post(self, sensor: str, tick: str, outcome: dict) -> dict:
        return _json(await self.transport.asend("POST", f"{self.base}/{sensor}/ticks/{tick}", json=outcome))

    async def close(self) -> None:
        await self.transport.close()
