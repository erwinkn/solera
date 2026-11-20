"""Signing in through an OpenID Connect provider (solera_server/openid.py):
the code flow against a fake provider that signs real EdDSA ID tokens, then
the server's own session; and each way a sign-in is refused."""

import base64
import hashlib
import json
import time
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from solera import Identity, OpenID, Project, asset
from solera_server import openid
from solera_server.api import create_app

from tests.server.test_access import engine  # noqa: F401  (a fixture)

ISSUER = "https://id.example"


class Provider:
    """An OpenID provider: discovery, keys, and a token endpoint that hands
    out an ID token for the code its authorize page would have given."""

    def __init__(self):
        self.key = Ed25519PrivateKey.generate()
        self.claims: dict = {}  # added to, or overriding, each ID token's
        self.authorized: dict = {}  # the authorize request's parameters

    def token(self, nonce: str, **claims) -> str:
        now = int(time.time())
        body = {"iss": ISSUER, "aud": "solera", "sub": "u1", "iat": now, "exp": now + 300, "nonce": nonce}
        body |= {"email": "ana@example.com", "name": "Ana"} | self.claims | claims
        return jwt.encode(body, self.key, algorithm="EdDSA", headers={"kid": "k1"})

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/.well-known/openid-configuration":
            return httpx.Response(
                200,
                json={
                    "issuer": ISSUER,
                    "authorization_endpoint": f"{ISSUER}/authorize",
                    "token_endpoint": f"{ISSUER}/token",
                    "jwks_uri": f"{ISSUER}/jwks",
                },
            )
        if request.url.path == "/jwks":
            public = json.loads(jwt.algorithms.OKPAlgorithm.to_jwk(self.key.public_key()))
            return httpx.Response(200, json={"keys": [public | {"kid": "k1", "use": "sig", "alg": "EdDSA"}]})
        if request.url.path == "/token":
            form = parse_qs(request.content.decode())
            expected = base64.b64encode(b"solera:shh").decode()
            if request.headers.get("Authorization") != f"Basic {expected}" or form["code"] != ["the-code"]:
                return httpx.Response(400, json={"error": "invalid_grant"})
            verifier = form["code_verifier"][0].encode()
            challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier).digest()).rstrip(b"=").decode()
            if challenge != self.authorized["code_challenge"]:
                return httpx.Response(400, json={"error": "invalid_grant", "error_description": "PKCE"})
            if form["redirect_uri"] != [self.authorized["redirect_uri"]]:
                return httpx.Response(400, json={"error": "invalid_grant"})
            return httpx.Response(200, json={"id_token": self.token(self.authorized["nonce"])})
        return httpx.Response(404)


def roles(claims: dict) -> Identity | None:
    if claims["email"].endswith("@example.com"):
        return Identity(claims["email"], "write")
    return None


@pytest.fixture
def provider():
    return Provider()


@pytest.fixture
def app(engine, provider):  # noqa: F811
    config = OpenID(issuer=ISSUER, client_id="solera", client_secret="shh", identity=roles)
    flow = openid.Flow(config, client=httpx.AsyncClient(transport=httpx.MockTransport(provider.handle)))
    app = create_app(engine=engine, signin=flow)
    app.state.engine = engine
    return app


def browser(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://solera.test")


async def start(client, provider, next="/runs") -> str:
    """Begin a sign-in, as the console does on a 401; the state it sent."""

    sent = await client.get("/auth/sign-in", params={"next": next})
    assert sent.status_code == 302
    authorize = urlsplit(sent.headers["location"])
    assert f"{authorize.scheme}://{authorize.netloc}{authorize.path}" == f"{ISSUER}/authorize"
    provider.authorized = {k: v[0] for k, v in parse_qs(authorize.query).items()}
    assert provider.authorized["redirect_uri"] == "http://solera.test/auth/callback"
    assert provider.authorized["code_challenge_method"] == "S256"
    assert "openid" in provider.authorized["scope"].split()
    return provider.authorized["state"]


async def test_a_browser_signs_in_and_then_carries_a_session(app, provider):
    async with browser(app) as client:
        refused = await client.get("/api/me")
        assert refused.status_code == 401 and refused.json()["sign_in"] == "/auth/sign-in"
        state = await start(client, provider)
        back = await client.get("/auth/callback", params={"code": "the-code", "state": state})
        assert back.status_code == 302 and back.headers["location"] == "/runs", back.text
        assert "httponly" in back.headers["set-cookie"].lower()
        assert (await client.get("/api/me")).json() == {"name": "ana@example.com", "role": "write"}
        started = await client.post(
            "/api/projects/example/runs",
            json={"targets": ["daily"], "partitions": ["2026-09-18"]},
            headers={"Sec-Fetch-Site": "same-origin"},
        )
        assert started.status_code == 201 and started.json()["by"] == "ana@example.com"
        await client.get("/auth/sign-out")
        assert (await client.get("/api/me")).status_code == 401


async def test_a_callback_this_browser_did_not_start_is_refused(app, provider):
    async with browser(app) as client:
        await start(client, provider)
        forged = await client.get("/auth/callback", params={"code": "the-code", "state": "someone-elses"})
        assert forged.status_code == 400
    async with browser(app) as stranger:  # no sign-in started: no flow cookie at all
        assert (
            await stranger.get("/auth/callback", params={"code": "the-code", "state": "x"})
        ).status_code == 400
        assert (await stranger.get("/api/me")).status_code == 401


async def test_an_id_token_for_another_client_or_sign_in_is_refused(app, provider):
    for claims in (
        {"aud": "another-app"},
        {"iss": "https://elsewhere.example"},
        {"exp": int(time.time()) - 600},
    ):
        provider.claims = claims
        async with browser(app) as client:
            state = await start(client, provider)
            back = await client.get("/auth/callback", params={"code": "the-code", "state": state})
            assert back.status_code == 400, claims
            assert (await client.get("/api/me")).status_code == 401
    provider.claims = {"nonce": "an-old-one"}
    async with browser(app) as client:
        state = await start(client, provider)
        assert (
            await client.get("/auth/callback", params={"code": "the-code", "state": state})
        ).status_code == 400


async def test_someone_the_identity_refuses_gets_no_session(app, provider):
    provider.claims = {"email": "eve@elsewhere.example"}
    async with browser(app) as client:
        state = await start(client, provider)
        back = await client.get("/auth/callback", params={"code": "the-code", "state": state})
        assert back.status_code == 403 and "eve@elsewhere.example" in back.text
        assert (await client.get("/api/me")).status_code == 401


async def test_a_session_cookie_cannot_be_forged_or_kept_past_its_end(app, provider):
    async with browser(app) as client:
        state = await start(client, provider)
        await client.get("/auth/callback", params={"code": "the-code", "state": state})
        cookie = client.cookies[openid.SESSION]
        body, signature = cookie.rsplit(".", 1)
        claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        promoted = base64.urlsafe_b64encode(json.dumps(claims | {"role": "admin"}).encode()).rstrip(b"=")
        client.cookies.set(openid.SESSION, f"{promoted.decode()}.{signature}")
        assert (await client.get("/api/me")).status_code == 401
    key = openid._key(type("R", (), {"app": app})())
    lapsed = openid._write(key, {"name": "ana@example.com", "role": "write", "exp": time.time() - 1})
    async with browser(app) as client:
        client.cookies.set(openid.SESSION, lapsed)
        assert (await client.get("/api/me")).status_code == 401


async def test_after_signing_in_the_browser_stays_on_this_server(app, provider):
    for asked in ("//evil.example/x", "https://evil.example", "/\\evil.example"):
        async with browser(app) as client:
            state = await start(client, provider, next=asked)
            back = await client.get("/auth/callback", params={"code": "the-code", "state": state})
            assert back.headers["location"] == "/", asked


async def test_a_project_without_openid_has_no_sign_in_pages(engine):  # noqa: F811
    app = create_app(engine=engine, token="t")
    app.state.engine = engine
    async with browser(app) as client:
        assert (await client.get("/auth/sign-in")).status_code == 404
        assert "sign_in" not in (await client.get("/api/me")).json()


def test_a_project_takes_an_openid_as_its_sign_in():
    @asset
    def numbers():
        return [1]

    config = OpenID(issuer=ISSUER, client_id="solera", client_secret="shh", identity=roles)
    assert Project(assets=[numbers], sign_in=config).sign_in is config
    assert "shh" not in repr(config)
