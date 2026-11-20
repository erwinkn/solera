"""Who may call the server (docs/access.md): the admin token, the project's
own sign-in, and --insecure, each a name and a role; the worker channels
keep their own credentials and never see the sign-in."""

import logging

import httpx
import pytest
from solera import Identity, Project, RegistrationError, lifecycle
from solera_server.api import create_app
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.sensors import HOST_TOKEN
from solera_server.state import State

from tests.server.test_api import build_project

ROLES = {"reader": "read", "writer": "write", "boss": "admin"}


@pytest.fixture
async def engine(tmp_path):
    project = build_project()
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    runtime = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
    )
    await runtime.initialize()
    yield runtime
    await state.close()


def by_header(request):
    """A sign-in as a proxy would make one: the caller's name in a header
    (a real one verifies its signature first; docs/access.md)."""

    name = request.headers.get("X-User")
    return Identity(name, ROLES[name]) if name in ROLES else None


async def by_header_async(request):
    return by_header(request)


def client_for(engine, **options):
    app = create_app(engine=engine, **options)
    app.state.engine = engine
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def as_user(name):
    return {"X-User": name}


ADMIN = {"Authorization": "Bearer s3cret"}


@pytest.mark.parametrize("signin", [by_header, by_header_async])
async def test_each_role_reads_starts_and_deletes_as_its_role_allows(engine, signin):
    async with client_for(engine, token="s3cret", signin=signin) as client:
        base = f"/api/projects/{engine.manifest['name']}"
        start = {"targets": ["daily"], "partitions": ["2026-09-18"]}
        started = await client.post(f"{base}/runs", json=start, headers=ADMIN)
        assert started.status_code == 201
        run = started.json()["id"]

        for name in ROLES:
            assert (await client.get(f"{base}/runs", headers=as_user(name))).status_code == 200

        refused = await client.post(f"{base}/runs", json=start, headers=as_user("reader"))
        assert refused.status_code == 403
        assert refused.json()["detail"] == "This needs the write role; reader has read."
        refused = await client.delete(f"{base}/runs/{run}", headers=as_user("reader"))
        assert refused.status_code == 403 and "write role" in refused.json()["detail"]
        refused = await client.post(f"{base}/runs:prune", json={"dry_run": True}, headers=as_user("reader"))
        assert refused.status_code == 403

        assert (await client.post(f"{base}/runs", json=start, headers=as_user("writer"))).status_code == 201
        assert (await client.post(f"{base}/runs", json=start, headers=as_user("boss"))).status_code == 201
        # a destructive route: past the role check, whatever the engine then says of the run
        assert (await client.delete(f"{base}/runs/{run}", headers=as_user("writer"))).status_code != 403

        # nobody the sign-in knows, and no token: 401, as before
        assert (await client.get(f"{base}/runs", headers=as_user("stranger"))).status_code == 401
        assert (await client.get(f"{base}/runs")).status_code == 401


async def test_the_admin_token_does_everything_and_an_unknown_bearer_does_not(engine):
    async with client_for(engine, token="s3cret", signin=by_header) as client:
        base = f"/api/projects/{engine.manifest['name']}"
        start = {"targets": ["daily"], "partitions": ["2026-09-18"]}
        assert (await client.post(f"{base}/runs", json=start, headers=ADMIN)).status_code == 201
        assert (
            await client.post(f"{base}/runs:prune", json={"dry_run": True}, headers=ADMIN)
        ).status_code == 200
        wrong = {"Authorization": "Bearer wrong"}
        assert (await client.get(f"{base}/runs", headers=wrong)).status_code == 401
        # the token wins over whatever else the request says
        me = await client.get("/api/me", headers={**ADMIN, **as_user("reader")})
        assert me.json() == {"name": "admin", "role": "admin"}


async def test_a_sign_in_that_fails_or_makes_a_bad_identity_refuses(engine, caplog):
    def broken(request):
        # as a parser would: its message quotes what it was reading
        raise RuntimeError(f"cannot read the session {request.cookies['session']!r}")

    def bad_role(request):
        return Identity("eve", "owner")

    def not_an_identity(request):
        return {"name": "eve", "role": "admin"}

    for signin in (broken, bad_role, not_an_identity):
        caplog.clear()
        async with client_for(engine, token="s3cret", signin=signin) as client:
            with caplog.at_level(logging.ERROR, logger="solera_server.api"):
                refused = await client.get("/api/me", headers={"Cookie": "session=abc123"})
            assert refused.status_code == 500
            assert refused.json() == {"detail": "The sign-in failed; see the server's log"}
            [record] = caplog.records
            logged = record.getMessage()
            # what failed and where, never the error's message: it may quote the cookie
            assert "GET /api/me" in logged and "Error" in logged and "  File " in logged
            assert "abc123" not in caplog.text
            # the admin token does not depend on it
            assert (await client.get("/api/me", headers=ADMIN)).status_code == 200


async def test_the_sign_in_is_never_asked_on_a_worker_channel(engine, monkeypatch):
    monkeypatch.setenv("SOLERA_POOL_TOKEN", "pool")
    asked = []

    def everyone_is_admin(request):
        asked.append(request.url.path)
        return Identity("eve", "admin")

    p = engine.manifest["name"]
    async with client_for(engine, token="s3cret", signin=everyone_is_admin) as client:
        attempt = f"/api/projects/{p}/attempts/a1/beat"
        pool = f"/api/projects/{p}/pools/gpu/work?wait=0"
        sensor = f"/api/projects/{p}/sensors/next?executor=Local&deploy=d&wait=0"
        for path, method in ((attempt, "POST"), (pool, "GET"), (sensor, "GET")):
            assert (await client.request(method, path, json={})).status_code == 401
        assert asked == []
        # each with its own credential, as before
        pool_ok = await client.get(pool, headers={"Authorization": "Bearer pool"})
        assert pool_ok.status_code == 200
        host = lifecycle.token(engine.secret, HOST_TOKEN)
        assert (await client.get(sensor, headers={"Authorization": f"Bearer {host}"})).status_code == 200
        assert asked == []
        # anything else is the sign-in's to answer
        assert (await client.get(f"/api/projects/{p}/runs")).status_code == 200
        assert asked == [f"/api/projects/{p}/runs"]


async def test_by_is_the_signed_in_name_and_the_bodys_for_the_admin_token(engine):
    async with client_for(engine, token="s3cret", signin=by_header) as client:
        base = f"/api/projects/{engine.manifest['name']}"
        start = {"targets": ["daily"], "partitions": ["2026-09-18"], "by": "somebody else"}
        signed = (await client.post(f"{base}/runs", json=start, headers=as_user("writer"))).json()
        assert signed["by"] == "writer"
        token = (await client.post(f"{base}/runs", json=start, headers=ADMIN)).json()
        assert token["by"] == "somebody else"
        unnamed = (await client.post(f"{base}/runs", json={**start, "by": None}, headers=ADMIN)).json()
        assert unnamed["by"] == "api"
        commit = {"upsert": ["u-1"], "by": "somebody else"}
        committed = await client.post(
            f"{base}/sources/uploads/commit", json=commit, headers=as_user("writer")
        )
        run = (await client.get(f"{base}/runs/{committed.json()['run']}", headers=ADMIN)).json()
        assert run["request"]["by"] == "writer"
        # an automation fired by hand is that person's run too; fired by the token, nobody's
        autos = (await client.get(f"{base}/automations", headers=ADMIN)).json()["automations"]
        hourly = next(a["name"] for a in autos if a["trigger"]["kind"] == "every")
        for headers, by in ((as_user("writer"), "writer"), (ADMIN, None)):
            fired = await client.post(f"{base}/automations/{hourly}/run-now", headers=headers)
            run = (await client.get(f"{base}/runs/{fired.json()['last_run']}", headers=ADMIN)).json()
            assert run["request"]["by"] == by and run["request"]["automation"] == hourly
            await client.post(f"{base}/runs/{fired.json()['last_run']}/cancel", headers=ADMIN)


async def test_a_change_from_another_sites_page_is_refused(engine):
    """A cookie sign-in and --insecure both let a browser in without a
    header a page must add: another site's page must not start runs with it."""

    start = {"targets": ["daily"], "partitions": ["2026-09-18"]}
    for options, headers in (
        ({"signin": by_header}, {"X-User": "writer"}),
        ({"insecure": True}, {}),
    ):
        async with client_for(engine, **options) as client:
            runs = f"/api/projects/{engine.manifest['name']}/runs"
            for theirs in (
                {"Sec-Fetch-Site": "cross-site"},
                {"Sec-Fetch-Site": "same-site"},
                {"Origin": "https://elsewhere.example"},
            ):
                refused = await client.post(runs, json=start, headers=headers | theirs)
                assert refused.status_code == 403, theirs
                assert "console's own page" in refused.json()["detail"]
                assert (await client.get(runs, headers=headers | theirs)).status_code == 200  # a read may
            for ours in (
                {"Sec-Fetch-Site": "same-origin", "Origin": "https://elsewhere.example"},
                {"Origin": f"http://{client.base_url.host}"},
                {},  # a script: no browser, nothing to trick
            ):
                assert (await client.post(runs, json=start, headers=headers | ours)).status_code == 201, ours


async def test_an_older_browsers_origin_is_compared_whole_and_through_a_proxy(engine):
    """Without Sec-Fetch-Site the Origin decides: the http page of the same
    host is another site, and behind a proxy the address is the forwarded one."""

    start = {"targets": ["daily"], "partitions": ["2026-09-18"]}
    app = create_app(engine=engine, signin=by_header)
    app.state.engine = engine
    transport = httpx.ASGITransport(app=app)
    runs = f"/api/projects/{engine.manifest['name']}/runs"
    async with httpx.AsyncClient(transport=transport, base_url="https://solera.example") as client:
        for origin, status in (("https://solera.example", 201), ("http://solera.example", 403)):
            sent = await client.post(runs, json=start, headers=as_user("writer") | {"Origin": origin})
            assert sent.status_code == status, origin
    async with httpx.AsyncClient(transport=transport, base_url="http://backend:8000") as client:
        proxied = as_user("writer") | {"X-Forwarded-Proto": "https", "X-Forwarded-Host": "console.example"}
        for origin, status in (
            ("https://console.example", 201),
            ("http://console.example", 403),
            ("http://backend:8000", 403),
            ("https://elsewhere.example", 403),
        ):
            sent = await client.post(runs, json=start, headers=proxied | {"Origin": origin})
            assert sent.status_code == status, origin


@pytest.fixture
async def signed_in_engine(tmp_path):
    """An engine serving a project whose sign-in is `by_header`."""

    project = build_project(sign_in=by_header)
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    runtime = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
    )
    await runtime.initialize()
    yield runtime
    await state.close()


def test_a_projects_sign_in_is_any_function_and_no_part_of_its_deploy():
    """The server imports the project itself, so the sign-in is just the
    function: a lambda or a closure does. It is not code the attempts run,
    so it leaves the deploy as it was."""

    def nested(request):
        return None

    for function in (by_header, nested, lambda request: None):
        assert build_project(sign_in=function).sign_in is function
    assert build_project(sign_in=by_header).manifest == build_project().manifest
    with pytest.raises(RegistrationError, match="sign_in"):
        Project(sign_in="tests.server.test_access:by_header")


SIGNED_IN = """
from solera import Identity, Project, asset


@asset
def numbers():
    return [1]


project = Project(
    assets=[numbers],
    sign_in=lambda request: Identity(request.headers["X-User"], "read") if "X-User" in request.headers else None,
)
"""


async def test_the_server_serves_with_the_projects_sign_in(tmp_path, monkeypatch):
    """No token and no --insecure: the project's sign-in is enough to start."""

    monkeypatch.delenv("SOLERA_API_TOKEN", raising=False)
    monkeypatch.setenv("SOLERA_DATA_URL", (tmp_path / "data").as_uri())
    (tmp_path / "signed_in.py").write_text(SIGNED_IN)
    app = create_app(state_url=tmp_path.as_uri(), namespace="test", project=str(tmp_path / "signed_in.py"))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            me = await client.get("/api/me", headers={"X-User": "ana@example.com"})
            assert me.json() == {"name": "ana@example.com", "role": "read"}
            assert (await client.get("/api/me")).json() == {"detail": "Authentication required"}


async def test_the_app_for_in_process_workers_has_no_callers(signed_in_engine):
    """`local_transport`'s app serves workers alone: the project's sign-in,
    which opens the served app, opens nothing on it."""

    from solera_server.api import local_transport

    local_transport(signed_in_engine, "a-worker's-token")
    transport = httpx.ASGITransport(app=signed_in_engine.local_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.get("/api/me", headers=as_user("boss"))).status_code == 401
        runs = f"/api/projects/{signed_in_engine.manifest['name']}/runs"
        start = {"targets": ["daily"], "partitions": ["2026-09-18"]}
        assert (await client.post(runs, json=start, headers=as_user("writer"))).status_code == 401


async def test_me_says_who_the_caller_is(engine):
    async with client_for(engine, token="s3cret", signin=by_header) as client:
        assert (await client.get("/api/me", headers=as_user("reader"))).json() == {
            "name": "reader",
            "role": "read",
        }
        assert (await client.get("/api/me", headers=ADMIN)).json() == {"name": "admin", "role": "admin"}
        assert (await client.get("/api/me")).status_code == 401
    async with client_for(engine, insecure=True) as client:
        assert (await client.get("/api/me")).json() == {"name": "local", "role": "admin"}
    # --insecure with a sign-in: whom it names is named, anyone else is local
    async with client_for(engine, insecure=True, signin=by_header) as client:
        assert (await client.get("/api/me", headers=as_user("reader"))).json()["name"] == "reader"
        assert (await client.get("/api/me")).json()["name"] == "local"


async def test_static_files_and_health_are_served_as_before(engine):
    async with client_for(engine, token="s3cret", signin=by_header) as client:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/")).status_code == 200
        assert (await client.get("/runs/abc")).status_code == 200  # a console route: the shell
        assert (await client.get("/api/nowhere")).status_code == 401  # unknown API path: still checked
        assert (await client.get("/api/nowhere", headers=as_user("reader"))).status_code == 404
