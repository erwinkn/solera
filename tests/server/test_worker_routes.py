"""The worker's channels and the server's routes agree (docs/lifecycle.md
§5, §10, §11): every request a channel makes is routed by the server and
admitted with the token its worker holds, and refused without it — over
both transports. In process, each call goes to the app's handlers; over
HTTP, what each call puts on the wire is replayed through the app. Both
sides come from code: a route renamed on either side fails here."""

import ast
import asyncio
import inspect
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import solera_worker
from solera import lifecycle
from solera_server.api import create_app
from solera_server.sensors import HOST_TOKEN
from solera_worker import channel as channels
from solera_worker.channel import AttemptChannel, HttpTransport, LocalTransport, PoolChannel, SensorChannel

SECRET = b"s" * 32
SAMPLES = {"dict": {}, "bytes": b"", "str": "x", "int": 1}
TOKENS = {
    "attempt": lifecycle.token(SECRET, "a1"),
    "pool": "pool",
    "host": lifecycle.token(SECRET, HOST_TOKEN),
}


class Engine:
    """What the app asks of an engine on a worker's routes; every handler
    answers at once."""

    def __init__(self):
        self.manifest, self.secret, self.state = {"name": "p"}, SECRET, SimpleNamespace(recorded=0)

    def __getattr__(self, name):
        async def answer(*args, **kwargs):
            return b"" if name == "attempt_resolve" else {"work": []} if name == "pool_work" else {}

        return answer


def built(transport, tokens=TOKENS):
    """Every channel, each over `transport(token)`, with the token its worker holds."""

    return [
        AttemptChannel(transport(tokens["attempt"]), "p", "a1"),
        PoolChannel(transport(tokens["pool"]), "p", "gpu"),
        SensorChannel(transport(tokens["pool"]), "p"),  # a pool host's sensors
        SensorChannel(transport(tokens["host"]), "p"),  # the engine's own host
    ]


def calls(cls) -> list[str]:
    """A channel's calls: its public methods but `close`."""

    return sorted(n for n, f in vars(cls).items() if inspect.isfunction(f) and n[0] != "_" and n != "close")


async def invoke(channel, name: str):
    """One call, with the arguments its signature asks for; a blocking one
    from a thread of its own, as the reporting thread makes it."""

    method = getattr(channel, name)
    args = [
        SAMPLES[p.annotation] for p in inspect.signature(method).parameters.values() if p.default is p.empty
    ]
    return (
        await method(*args) if inspect.iscoroutinefunction(method) else await asyncio.to_thread(method, *args)
    )


def served_app(monkeypatch):
    monkeypatch.setenv("SOLERA_POOL_TOKEN", "pool")
    engine = Engine()
    app = create_app(engine=engine, token="admin")
    app.state.engine = engine
    return app


def test_every_worker_client_is_checked_here():
    """The only places in the worker package that make an HTTP client are the
    two transports, and every channel is driven below."""

    making = set()
    for path in Path(solera_worker.__file__).parent.glob("*.py"):
        for node in ast.parse(path.read_text()).body:
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and ast.unparse(call.func) in (
                    "httpx.Client",
                    "httpx.AsyncClient",
                ):
                    making.add(node.name)
    assert making == {"HttpTransport", "LocalTransport"}
    defined = {n for n, c in vars(channels).items() if inspect.isclass(c) and n.endswith("Channel")}
    assert defined == {type(c).__name__ for c in built(lambda token: None)}


async def test_in_process_every_call_is_served_with_its_token(monkeypatch):
    app = served_app(monkeypatch)
    for channel in built(lambda token: LocalTransport(app, token)):
        for name in calls(type(channel)):
            await invoke(channel, name)  # an error answer would raise
    for channel in built(lambda token: LocalTransport(app, None)):
        for name in calls(type(channel)):
            with pytest.raises(httpx.HTTPStatusError) as refused:
                await invoke(channel, name)
            assert refused.value.response.status_code == 401, (type(channel).__name__, name)


async def test_over_http_every_request_is_served_with_its_token(monkeypatch):
    app = served_app(monkeypatch)
    sent: list[httpx.Request] = []

    def record(request):
        sent.append(request)
        return httpx.Response(200, json={"work": []})

    for channel in built(
        lambda token: HttpTransport("http://engine", token, transport=httpx.MockTransport(record))
    ):
        for name in calls(type(channel)):
            before = len(sent)
            await invoke(channel, name)
            assert len(sent) == before + 1, (type(channel).__name__, name)  # one request a call
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://engine") as server:
        for request in sent:
            route = f"{request.method} {request.url.path}"

            async def replay(*kept, request=request):
                headers = {k: v for k, v in request.headers.items() if k in kept}
                url = request.url
                return await server.request(
                    request.method, url.path, params=url.params, content=request.content, headers=headers
                )

            answer = await replay("authorization", "content-type")
            assert answer.is_success, (route, answer.status_code, answer.text)
            assert (await replay("content-type")).status_code == 401, route
