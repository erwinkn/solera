"""Who is calling (docs/access.md): a name and one of three roles, each
including the one before. The server recognises the admin token and
`--insecure` itself; anything else is the project's own sign-in
(`Project(sign_in=...)`), called with each request the built-ins did not
recognise. The three worker channels (attempts, pools, sensors) are not
callers: they keep their own credentials (`api.py`)."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from solera.access import ROLES, Identity
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request

SignIn = Callable[[Request], Identity | None | Awaitable[Identity | None]]


def needs(method: str) -> str:
    """The role a request needs, by its method alone, so no route can be
    left out: reading is a GET, and everything that changes something is
    not. A POST that only reads (a prune's dry run) needs `write` too."""

    return "read" if method in ("GET", "HEAD") else "write"


def cross_site(request: Request) -> bool:
    """Whether a browser sent this from another site's page. A sign-in that
    rides on a cookie, and --insecure, which asks for nothing, would both
    let any page the person has open start a run in their name; no page
    but the console's own has a reason to. A browser says where a request
    comes from (`Sec-Fetch-Site`, and `Origin` before that existed); a
    script or the CLI sends neither, and is not a browser to be tricked."""

    site = request.headers.get("Sec-Fetch-Site")
    if site is not None:
        return site not in ("same-origin", "none")
    origin = request.headers.get("Origin")
    if origin is None:
        return False
    # The whole origin, scheme included: the http page of the same host is
    # another site. Behind a proxy the server sees the proxy's scheme and
    # perhaps its host, so what the proxy forwarded speaks first. A page
    # cannot forge those: a cross-site request with a header of its own
    # choosing is asked about first (CORS), and this server never agrees.
    headers = request.headers
    scheme = headers.get("X-Forwarded-Proto", request.url.scheme).split(",")[0].strip()
    host = headers.get("X-Forwarded-Host", headers.get("Host", "")).split(",")[0].strip()
    theirs = urlsplit(origin)
    return (theirs.scheme.lower(), theirs.netloc.lower()) != (scheme.lower(), host.lower())


def allows(identity: Identity, role: str) -> bool:
    return ROLES.index(identity.role) >= ROLES.index(role)


async def sign_in(function: SignIn, request: Request) -> Identity | None:
    """The deployment's verdict on `request`. A plain function runs in a
    thread, as Starlette runs a plain route, so one that fetches its keys
    holds up nothing else. Whatever it returns that is not an identity or
    None is its bug, raised like any error it raises."""

    if inspect.iscoroutinefunction(function):
        found = await function(request)
    else:
        found = await run_in_threadpool(function, request)
        if inspect.isawaitable(found):  # a callable object whose __call__ is async
            found = await found
    if found is not None and not isinstance(found, Identity):
        raise TypeError(f"the sign-in returned {type(found).__name__}, not an Identity or None")
    return found
