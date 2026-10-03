"""Records the engine and the worker share (docs/lifecycle.md §2): where an
attempt's objects live, the cancel record, write-completion evidence, the
gate, and the per-attempt token. One implementation, imported by both."""

from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
from dataclasses import dataclass
from urllib.parse import quote

# -- objects (§2.1) ------------------------------------------------------------------


def base(run: str, attempt: str) -> str:
    """`runs/{run}/{attempt}`: every object of an attempt is this plus a suffix."""

    return f"runs/{quote(str(run), safe='')}/{quote(str(attempt), safe='')}"


SPEC = ".spec"  # immutable, the engine's
WORKER = ".worker"  # the claim; then the owner's reports while HTTP fails
RESULT = ".result"  # immutable, sealed: its existence means the worker is done
GATE = ".writing"  # the gate; outlives its run (§2.4)


def chunk(n: int) -> str:
    return f".log.{n:06d}"


# -- write-completion evidence (§2.3) ------------------------------------------------

NONE, COMPLETE = "none", "complete"  # and WRITING, as the gate says: a call may have landed

# -- the gate ------------------------------------------------------------------------

WRITING, ABORTED, CLOSED = "writing", "aborted", "closed"


def gate(state: str, worker_id: str | None = None, intents: dict | None = None) -> bytes:
    body: dict = {"state": state}
    if worker_id is not None:
        body["worker_id"] = worker_id
    if intents is not None:
        body["intents"] = intents
    return json.dumps(body, sort_keys=True).encode()


# -- the cancel record (§2.2) --------------------------------------------------------

PHASES = ("requested", "forced")
REASONS = ("provisioning", "timeout", "user")  # in rising precedence


@dataclass(frozen=True)
class Cancel:
    """What the engine decided to stop, latched: `phase` only advances, and a
    reason of higher precedence replaces a lower one."""

    phase: str
    reason: str
    since: int  # the event position at which the engine latched it

    def __post_init__(self):
        if self.phase not in PHASES or self.reason not in REASONS:
            raise ValueError(f"invalid cancel record: {self}")

    def stronger(self, other: Cancel | None) -> Cancel:
        """The latched record after seeing `other` too."""

        if other is None:
            return self
        phase = max(self.phase, other.phase, key=PHASES.index)
        reason = max(self.reason, other.reason, key=REASONS.index)
        for record in (self, other):
            if (record.phase, record.reason) == (phase, reason):
                return record
        return Cancel(phase, reason, max(self.since, other.since))

    def to_json(self) -> dict:
        return {"phase": self.phase, "reason": self.reason, "since": self.since}

    @classmethod
    def from_json(cls, data: dict | None) -> Cancel | None:
        return None if data is None else cls(data["phase"], data["reason"], int(data["since"]))


def latch(current: Cancel | None, seen: Cancel | None) -> Cancel | None:
    if seen is None:
        return current
    return seen.stronger(current)


# -- the attempt token (§5.2) --------------------------------------------------------


def token(secret: bytes, attempt: str) -> str:
    return hmac.new(secret, f"attempt:{attempt}".encode(), hashlib.sha256).hexdigest()


def valid(secret: bytes, attempt: str, presented: str) -> bool:
    return hmac.compare_digest(token(secret, attempt).encode(), presented.encode())


class Ended(Exception):
    """The engine says this attempt is over for this invocation (`409`):
    it ended, or another invocation owns it. Write nothing more."""

    def __init__(self, reason: str = "ended"):
        super().__init__(reason)
        self.reason = reason


def log_text(chunks: list[bytes], tail: str | None) -> bytes:
    """A log's JSON lines from its chunks' bytes and the result's `tail`."""

    text = b"".join(gzip.decompress(c) for c in chunks)
    if tail:
        text += gzip.decompress(base64.b64decode(tail))
    return text
