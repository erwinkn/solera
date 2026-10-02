"""A sensor host (docs/lifecycle.md §11.2): a long-lived process with the
project loaded, so a tick costs a function call. It long-polls the engine
for due ticks, runs each body on a thread of its own within the sensor's
timeout, and posts the outcome. A host whose tick overran, or that ran
`max_ticks`, exits for its supervisor to start a fresh one."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import socket
import threading
import traceback

from solera.lifecycle import Ended
from solera.sdk import Project, Tick
from solera.stores import resolve_env

OVERRAN = 3  # the exit code of a host that gave up on a tick


class SensorContext:
    """The `ctx` a sensor body gets: its `cursor`, and the `snapshot` of the
    sources it may commit to (a keyed one's pinned index included)."""

    def __init__(self, tick: dict):
        self.sensor, self.tick = tick["sensor"], tick["tick"]
        self.cursor, self.snapshot = tick.get("cursor"), tick.get("snapshot") or {}


class HttpSensorChannel:
    def __init__(self, server: str, project: str, token: str | None):
        import httpx

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.base = f"/api/projects/{project}/sensors"
        self.client = httpx.AsyncClient(base_url=server, headers=headers, timeout=60)

    async def next(self, executor: str, revision: str, host: str, slots: int) -> dict:
        params = {"executor": executor, "revision": revision, "host": host, "slots": slots, "wait": 30}
        response = await self.client.get(f"{self.base}/next", params=params)
        response.raise_for_status()
        return response.json()

    async def post(self, sensor: str, tick: str, outcome: dict) -> dict:
        response = await self.client.post(f"{self.base}/{sensor}/ticks/{tick}", json=outcome)
        if response.status_code == 409:
            raise Ended(response.json().get("detail") or "refused")
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self.client.aclose()


class LocalSensorChannel:
    """The engine's handlers, called in process (tests)."""

    def __init__(self, engine):
        self.engine = engine

    async def next(self, executor: str, revision: str, host: str, slots: int) -> dict:
        return await self.engine.sensor_next(executor, revision, host, slots, 30)

    async def post(self, sensor: str, tick: str, outcome: dict) -> dict:
        try:
            return await self.engine.sensor_post(sensor, tick, outcome)
        except self.engine.Conflict as error:
            raise Ended(str(error)) from error

    async def close(self) -> None:
        pass


async def run_sensor_host(
    channel,
    project: Project,
    executor: str,
    *,
    concurrency: int = 4,
    max_ticks: int = 10_000,
    host=None,
    stale_wait: float = 30.0,
) -> int:
    """Run ticks until `max_ticks` ran, one overran, or the engine serves
    another revision (then after `stale_wait`: a host started afresh loads
    the code as it is now); returns the exit code."""

    revision = project.manifest["revision"]
    host = host or f"{socket.gethostname()}:{os.getpid()}"
    running: set[asyncio.Task] = set()
    ran, overran = 0, asyncio.Event()

    async def one(tick: dict) -> None:
        sensor = project.sensors[tick["sensor"]]
        try:
            value = await _call(sensor, project, SensorContext(tick), tick["timeout"])
            if value is not None and not isinstance(value, Tick):
                raise TypeError(f"{sensor.name} returned {type(value).__name__}, not a Tick or None")
            outcome = value.to_json() if value is not None else {}
        except TimeoutError:
            overran.set()  # the engine drops it; its thread runs on, so this host goes
            return
        except Exception as error:
            outcome = {"error": "".join(traceback.format_exception_only(error)).strip()}
        for attempt in range(3):
            try:
                await channel.post(tick["sensor"], tick["tick"], outcome)
                return
            except Ended:
                return  # refused, or decided already
            except Exception:
                await asyncio.sleep(0.5 * 2**attempt)

    while ran < max_ticks and not overran.is_set():
        slots = min(concurrency - len(running), max_ticks - ran)
        if slots <= 0:
            await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            continue
        poll, gave_up = (
            asyncio.create_task(channel.next(executor, revision, host, slots)),
            asyncio.create_task(overran.wait()),
        )
        await asyncio.wait({poll, gave_up}, return_when=asyncio.FIRST_COMPLETED)
        gave_up.cancel()
        if not poll.done():  # a tick overran: stop asking
            poll.cancel()
            break
        try:
            answer = poll.result()
        except Exception:
            await asyncio.sleep(1.0)  # the engine may be restarting
            continue
        if answer["revision"] != revision:  # the engine serves other code: start afresh, later
            await asyncio.sleep(stale_wait)
            break
        for tick in answer["ticks"]:
            ran += 1
            task = asyncio.create_task(one(tick))
            running.add(task)
            task.add_done_callback(running.discard)
    if running:
        await asyncio.wait(running)
    return OVERRAN if overran.is_set() else 0


async def _call(sensor, project: Project, ctx: SensorContext, timeout: float):
    """The body on a daemon thread: one that overruns cannot be stopped, but
    it does not keep the host's process from exiting."""

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    kwargs = {p: resolve_env(project.resources[p]) for p in sensor.params}
    if "ctx" in inspect.signature(sensor.fn).parameters:
        kwargs["ctx"] = ctx

    def settle(value, error):
        if not future.done():
            future.set_exception(error) if error is not None else future.set_result(value)

    def work():
        value, error = None, None
        try:
            value = sensor.fn(**kwargs)
            if inspect.isawaitable(value):
                value = asyncio.run(value)
        except BaseException as e:
            error = e
        with contextlib.suppress(RuntimeError):  # the loop closed meanwhile
            loop.call_soon_threadsafe(settle, value, error)

    threading.Thread(target=work, name=f"sensor {sensor.name}", daemon=True).start()
    return await asyncio.wait_for(asyncio.shield(future), timeout)
