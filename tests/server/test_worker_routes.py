"""The worker's HTTP clients and the server's routes agree (docs/lifecycle.md
§5, §10, §11): every request a worker-side client makes is routed by the
server and admitted with the token that client holds, and refused without
it. Both sides come from code: each client's methods are called through a
recording transport, and what they sent is replayed through the app. A
route renamed on either side fails here; in-process workers, which call
the engine directly, never would."""

import ast
import inspect
from pathlib import Path
from types import SimpleNamespace

import httpx
import solera_worker
from solera import lifecycle
from solera_server.api import create_app
from solera_server.sensors import HOST_TOKEN
from solera_worker.channel import HttpChannel, HttpPoolChannel, LocalChannel
from solera_worker.sensors import HttpSensorChannel, LocalSensorChannel

SECRET = b"s" * 32
SAMPLES = {"dict": {}, "bytes": b"", "str": "x", "int": 1}


class Engine:
    """What the app asks of an engine on a worker's routes; every handler
    answers at once."""

    def __init__(self):
        self.manifest, self.secret, self.state = {"name": "p"}, SECRET, SimpleNamespace(recorded=0)

    def __getattr__(self, name):
        async def answer(*args, **kwargs):
            return b"" if name == "attempt_resolve" else {"work": []} if name == "pool_work" else {}

        return answer


def calls(cls) -> list[str]:
    """A client's calls: its public methods but `close`."""

    return sorted(n for n, f in vars(cls).items() if inspect.isfunction(f) and n[0] != "_" and n != "close")


async def sent(client) -> list[httpx.Request]:
    """Every call of `client`, with arguments its signature asks for, through
    a transport that records each request: one per call."""

    requests = []

    def record(request):
        requests.append(request)
        return httpx.Response(200, json={"work": []})

    real = client.client
    transport = httpx.MockTransport(record)
    if isinstance(real, httpx.Client):
        client.client = httpx.Client(transport=transport, headers=real.headers)
    else:
        client.client = httpx.AsyncClient(transport=transport, base_url=real.base_url, headers=real.headers)
    for name in calls(type(client)):
        method, before = getattr(client, name), len(requests)
        params = inspect.signature(method).parameters.values()
        result = method(*(SAMPLES[p.annotation] for p in params if p.default is p.empty))
        if inspect.isawaitable(result):
            await result
        assert len(requests) == before + 1, f"{type(client).__name__}.{name}"
    return requests


def clients(attempt_token, pool_token, host_token):
    """Every worker-side client, each with the token its worker holds."""

    return [
        HttpChannel("http://test", "p", "a1", attempt_token),
        HttpPoolChannel("http://test", "p", "gpu", pool_token),
        HttpSensorChannel("http://test", "p", pool_token),  # a pool host's sensors
        HttpSensorChannel("http://test", "p", host_token),  # the engine's own host
    ]


def test_every_worker_client_is_checked_here():
    """Each place in the worker package that makes an HTTP client is a client
    driven below, and each has an in-process twin with the same calls."""

    making = set()
    for path in Path(solera_worker.__file__).parent.glob("*.py"):
        for node in ast.parse(path.read_text()).body:
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and ast.unparse(call.func) in (
                    "httpx.Client",
                    "httpx.AsyncClient",
                ):
                    making.add(node.name)
    assert making == {type(c).__name__ for c in clients("", "", "")}
    assert calls(HttpChannel) == calls(LocalChannel)
    assert calls(HttpSensorChannel) == calls(LocalSensorChannel)


async def replay(server, request, *kept):
    """`request` sent to the app with only the `kept` headers."""

    headers = {k: v for k, v in request.headers.items() if k in kept}
    url = request.url
    return await server.request(
        request.method, url.path, params=url.params, content=request.content, headers=headers
    )


async def test_the_server_serves_every_worker_request_with_its_token(monkeypatch):
    monkeypatch.setenv("SOLERA_POOL_TOKEN", "pool")
    engine = Engine()
    app = create_app(engine=engine, token="admin")
    app.state.engine = engine
    tokens = (lifecycle.token(SECRET, "a1"), "pool", lifecycle.token(SECRET, HOST_TOKEN))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as server:
        for client in clients(*tokens):
            for request in await sent(client):
                route = f"{type(client).__name__}: {request.method} {request.url.path}"
                answer = await replay(server, request, "authorization", "content-type")
                assert answer.is_success, (route, answer.status_code, answer.text)
                assert (await replay(server, request, "content-type")).status_code == 401, route
