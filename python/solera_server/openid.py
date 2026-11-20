"""Signing in through an OpenID Connect provider (`Project(sign_in=OpenID(...))`,
docs/access.md): the authorization code flow, with PKCE, and then a session
of the server's own.

    browser ─GET /auth/sign-in?next=/runs─▶ 302 to the provider's authorize page
            (state, nonce and PKCE verifier kept in a signed cookie, 10 minutes)
            ─▶ the provider signs the person in ─▶ 302 /auth/callback?code&state
    server  ─POST token endpoint (code, verifier, client secret)─▶ ID token
            checks its signature (the provider's JWKS), issuer, audience, expiry
            and nonce; `identity(claims)` makes the caller
    browser ◀─ session cookie (signed, `hours` long) and 302 back to /runs

Every request after that is a cookie check: the provider is not asked again
until the session ends. Its role is the one `identity` gave at sign-in.

Both cookies are signed with a key derived from the engine's secret, which is
kept in the state and lasts across restarts, so a restart signs nobody out."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from urllib.parse import urlencode

import httpx
import jwt
from solera.access import Identity, OpenID
from starlette.requests import Request
from starlette.responses import PlainTextResponse, RedirectResponse, Response

from . import auth

SESSION, FLOW = "solera_session", "solera_sign_in"
FLOW_SECONDS = 600
PATHS = {"sign_in": "/auth/sign-in", "callback": "/auth/callback", "sign_out": "/auth/sign-out"}
# Asymmetric algorithms only: a provider's public keys verify its tokens, and
# HS256 with a public key as the secret is the classic forgery.
ALGORITHMS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"]


class Flow:
    """The sign-in an `OpenID` declares: called with each request, it reads
    the session cookie; its routes run the sign-in itself."""

    def __init__(self, config: OpenID, client: httpx.AsyncClient | None = None):
        self.config = config
        self.client = client or httpx.AsyncClient(timeout=10, follow_redirects=False)
        self._provider: dict | None = None
        self._keys: jwt.PyJWKSet | None = None

    # -- each request -----------------------------------------------------------------

    def __call__(self, request: Request) -> Identity | None:
        session = _read(_key(request), request.cookies.get(SESSION))
        return Identity(session["name"], session["role"]) if session else None

    # -- the routes -------------------------------------------------------------------

    async def sign_in(self, request: Request) -> Response:
        provider = await self.provider()
        flow = {
            "state": secrets.token_urlsafe(24),
            "nonce": secrets.token_urlsafe(24),
            "verifier": secrets.token_urlsafe(48),
            "next": _local(request.query_params.get("next")),
            "exp": time.time() + FLOW_SECONDS,
        }
        challenge = _b64(hashlib.sha256(flow["verifier"].encode()).digest())
        query = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": _callback(request),
            "scope": " ".join(self.config.scopes),
            "state": flow["state"],
            "nonce": flow["nonce"],
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        response = RedirectResponse(
            f"{provider['authorization_endpoint']}?{urlencode(query)}", status_code=302
        )
        _set(response, request, FLOW, _write(_key(request), flow), FLOW_SECONDS, path="/auth")
        return response

    async def callback(self, request: Request) -> Response:
        flow = _read(_key(request), request.cookies.get(FLOW))
        state = request.query_params.get("state")
        if flow is None or state is None or not hmac.compare_digest(state.encode(), flow["state"].encode()):
            # A link someone else started, or one past its ten minutes.
            return PlainTextResponse("This sign-in expired or was not started here. Sign in again.", 400)
        if "code" not in request.query_params:
            error = request.query_params.get("error", "no code")
            return PlainTextResponse(f"The provider did not sign you in: {error}", 400)
        try:
            claims = await self.verify(await self.exchange(request, flow), flow["nonce"])
        except (httpx.HTTPError, RuntimeError) as error:
            return PlainTextResponse(f"The provider could not be reached to finish signing in: {error}", 502)
        except jwt.InvalidTokenError as error:
            return PlainTextResponse(f"The provider's answer did not check out: {error}", 400)
        identity = self.config.identity(claims)
        if identity is None:
            who = claims.get("email") or claims.get("sub")
            return PlainTextResponse(f"{who} may not use this server.", 403)
        if not isinstance(identity, Identity):
            raise TypeError(f"OpenID identity returned {type(identity).__name__}, not an Identity or None")
        session = {
            "name": identity.name,
            "role": identity.role,
            "exp": time.time() + self.config.hours * 3600,
        }
        response = RedirectResponse(flow["next"], status_code=302)
        _set(response, request, SESSION, _write(_key(request), session), int(self.config.hours * 3600))
        response.delete_cookie(FLOW, path="/auth")
        return response

    async def sign_out(self, request: Request) -> Response:
        response = RedirectResponse("/", status_code=302)
        response.delete_cookie(SESSION, path="/")
        return response

    # -- the provider -----------------------------------------------------------------

    async def provider(self) -> dict:
        """The provider's endpoints (OpenID discovery), read once."""

        if self._provider is None:
            url = f"{self.config.issuer.rstrip('/')}/.well-known/openid-configuration"
            response = await self.client.get(url)
            response.raise_for_status()
            self._provider = response.json()
        return self._provider

    async def exchange(self, request: Request, flow: dict) -> str:
        """The code for an ID token, at the token endpoint."""

        provider = await self.provider()
        form = {
            "grant_type": "authorization_code",
            "code": request.query_params["code"],
            "redirect_uri": _callback(request),
            "code_verifier": flow["verifier"],
        }
        methods = provider.get("token_endpoint_auth_methods_supported", ["client_secret_basic"])
        credentials = None
        if "client_secret_basic" in methods:
            credentials = (self.config.client_id, self.config.client_secret)
        else:
            form |= {"client_id": self.config.client_id, "client_secret": self.config.client_secret}
        response = await self.client.post(provider["token_endpoint"], data=form, auth=credentials)
        if response.status_code != 200:
            raise RuntimeError(f"the token endpoint answered {response.status_code}: {response.text[:300]}")
        return response.json()["id_token"]

    async def verify(self, token: str, nonce: str) -> dict:
        """The ID token's claims, once its signature, issuer, audience, expiry
        and nonce are the ones this sign-in expects."""

        provider = await self.provider()
        kid = jwt.get_unverified_header(token).get("kid")
        key = await self.key(kid)
        claims = jwt.decode(
            token,
            key.key,
            algorithms=ALGORITHMS,
            audience=self.config.client_id,
            issuer=provider["issuer"],
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
            leeway=30,
        )
        if not hmac.compare_digest(str(claims.get("nonce", "")).encode(), nonce.encode()):
            raise jwt.InvalidTokenError("the ID token's nonce is not this sign-in's")
        return claims

    async def key(self, kid: str | None) -> jwt.PyJWK:
        """The provider's signing key `kid`: its key set is read again when a
        token names a key it lacks, as after a rotation."""

        for fresh in (False, True):
            if self._keys is None or fresh:
                response = await self.client.get((await self.provider())["jwks_uri"])
                response.raise_for_status()
                self._keys = jwt.PyJWKSet.from_dict(response.json())
            for found in self._keys.keys:
                if kid is None or found.key_id == kid:
                    return found
        raise jwt.InvalidTokenError(f"the provider has no signing key {kid!r}")


def _key(request: Request) -> bytes:
    secret = request.app.state.engine.secret
    if secret is None:
        raise RuntimeError("the engine has no secret yet")
    return hmac.new(secret, b"solera sign-in cookies", hashlib.sha256).digest()


def _write(key: bytes, value: dict) -> str:
    body = _b64(json.dumps(value, separators=(",", ":")).encode())
    return f"{body}.{_b64(hmac.new(key, body.encode(), hashlib.sha256).digest())}"


def _read(key: bytes, cookie: str | None) -> dict | None:
    """A cookie's value if this server signed it and it has not expired."""

    if not cookie or "." not in cookie:
        return None
    body, signature = cookie.rsplit(".", 1)
    if not hmac.compare_digest(signature, _b64(hmac.new(key, body.encode(), hashlib.sha256).digest())):
        return None
    value = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    return value if value.get("exp", 0) > time.time() else None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _local(target: str | None) -> str:
    """Where to land after signing in: a path on this server, never another
    site (`//elsewhere`, `/\\elsewhere`), which would make the sign-in an
    open redirect."""

    if not target or not target.startswith("/") or target.startswith(("//", "/\\")):
        return "/"
    return target


def _callback(request: Request) -> str:
    scheme, host = auth.origin(request)
    return f"{scheme}://{host}{PATHS['callback']}"


def _set(response: Response, request: Request, name: str, value: str, seconds: int, path: str = "/") -> None:
    secure = auth.origin(request)[0] == "https"
    response.set_cookie(name, value, max_age=seconds, path=path, secure=secure, httponly=True, samesite="lax")
