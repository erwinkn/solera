"""Who is calling the server (docs/access.md): a name and one of three roles,
each including the one before. A project's sign-in (`Project(sign_in=...)`)
makes one from a request."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

ROLES = ("read", "write", "admin")


@dataclass(frozen=True)
class Identity:
    name: str  # who: an email, a token's name
    role: str  # "read", "write" or "admin"

    def __post_init__(self):
        # A sign-in that builds a bad identity fails here, where it is made,
        # and its request is refused: a role nobody defined grants nothing.
        if not isinstance(self.name, str) or not self.name:
            raise ValueError(f"an identity needs a name, not {self.name!r}")
        if self.role not in ROLES:
            raise ValueError(f"{self.name}: role {self.role!r} is not one of {', '.join(ROLES)}")


@dataclass(frozen=True, kw_only=True)
class OpenID:
    """A sign-in through an OpenID Connect provider (docs/access.md): the
    server sends a browser that has no session to `issuer` to sign in, and
    keeps a session of its own for `hours` afterwards. `identity` makes the
    caller from the provider's ID token claims (`email`, `name`, and any the
    provider adds), or refuses them with None. The provider must accept
    `{server}/auth/callback` as this client's return address."""

    issuer: str
    client_id: str
    client_secret: str = field(repr=False)
    identity: Callable[[dict], Identity | None]
    scopes: tuple[str, ...] = ("openid", "email", "profile")
    hours: float = 8
