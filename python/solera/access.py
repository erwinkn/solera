"""Who is calling the server (docs/access.md): a name and one of three roles,
each including the one before. A project's sign-in (`Project(sign_in=...)`)
makes one from a request."""

from __future__ import annotations

from dataclasses import dataclass

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
