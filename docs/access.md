# Who can call the server

Every request under `/api/` comes from a caller the server recognises, or it
is refused with a 401. A caller has a name and a role. The console, the
`solera` CLI and scripts all go through the same check.

## How a caller is recognised

The server tries these in order and stops at the first that knows the caller.

1. **The admin token.** A request with `Authorization: Bearer $SOLERA_API_TOKEN`
   is `admin`, named `admin`. It is the way in before anything else is set up.
2. **The project's sign-in.** `Project(sign_in=function)` gives a function
   that is called with each request the token did not match. It returns an
   `Identity`, or `None` when it does not know the caller. It may be `async`;
   a plain one runs in a thread.
3. **`--insecure`.** On a loopback-only server, anyone else is `admin`, named
   `local`.

If the sign-in raises, or returns a role that does not exist, the request is
refused with a 500. The server's log gets the error's type and where it was
raised, not its message, which could quote the cookie or token it was reading.

`solera serve` imports the project into its own process, so the sign-in is
any function, a lambda included. A local reload switches to new code without
a restart, but it keeps the sign-in the server started with: a change to the
sign-in takes effect when the server restarts.

Workers are not callers. A worker's own routes (an attempt's, a pool's, a
sensor host's) take their own tokens, as `docs/lifecycle.md` §5 describes. The
sign-in is never asked about them, so it cannot open them.

## Roles

Each role can do everything the one before it can.

| Role | Can |
|---|---|
| `read` | Every `GET`: runs, assets, keys, logs, history, the engine's state. |
| `write` | Anything that changes something: start, cancel, retry, pause, resume, delete and prune runs; retry keys; clear cleanups; turn automations on and off and run them; commit to a source. |
| `admin` | Nothing more yet. |

The role a request needs depends only on its method: `GET` and `HEAD` need
`read`, anything else needs `write`. A caller without it gets a 403 that says
which role the action needs and which one they have. The console hides the
controls a `read` caller cannot use.

A run records who asked for it in `by`. For a caller the sign-in named, `by` is
that name, whatever the request says. For the admin token and `--insecure`,
`by` is what the request says (`console`, `cli`, `api`).

## Changes come from the console's own page

A request that changes something is refused with a 403 when a browser sent it
from another site's page. A sign-in that rides on a cookie would otherwise let
any page a signed-in person has open start a run in their name. The server
reads `Sec-Fetch-Site`, or from an older browser `Origin`, which it compares
with its own address: `X-Forwarded-Proto` and `X-Forwarded-Host` when a proxy
sets them, else the request's own scheme and `Host`. A script or the CLI sends
neither header and is not affected.

## A sign-in behind a proxy

A proxy such as Cloudflare Access signs people in, then sends each request on
with a signed header naming them. The function checks the signature and maps
the email to a role:

```python
# acme/access.py
import jwt  # PyJWT
from solera import Identity

TEAM = "https://acme.cloudflareaccess.com"
AUDIENCE = "your Access application's AUD tag"
ROLES = {"ana@acme.com": "admin", "bo@acme.com": "write"}  # everyone else at acme.com reads
keys = jwt.PyJWKClient(f"{TEAM}/cdn-cgi/access/certs")  # fetched once, then cached


def access(request):
    assertion = request.headers.get("Cf-Access-Jwt-Assertion")
    if assertion is None:
        return None
    try:
        key = keys.get_signing_key_from_jwt(assertion).key
        claims = jwt.decode(assertion, key, algorithms=["RS256"], audience=AUDIENCE, issuer=TEAM)
    except jwt.PyJWKClientConnectionError:  # the keys cannot be fetched: refuse, loudly
        raise
    except (jwt.InvalidTokenError, jwt.PyJWKClientError):  # forged, expired, another application's, an unknown key
        return None
    email = str(claims.get("email", "")).lower()
    return Identity(email, ROLES.get(email, "read")) if email.endswith("@acme.com") else None
```

The project names it with `Project(..., sign_in=access)`. Keys that cannot be
fetched raise, so the request is refused rather than let through.

## Cookies

A sign-in that reads a cookie needs nothing more: the console's requests are
same-origin, and browsers send cookies with those.
