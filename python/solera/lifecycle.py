"""Records the engine and the worker share (docs/lifecycle.md §2): where an
attempt's objects live, its control file, the cancel record,
write-completion evidence, and the per-attempt token. One implementation,
imported by both."""

from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
from dataclasses import dataclass
from urllib.parse import quote

from solera.objects import read

# -- objects (§2.1) ------------------------------------------------------------------


def base(run: str, attempt: str) -> str:
    """`runs/{run}/{attempt}`: every object of an attempt is this plus a suffix."""

    return f"runs/{quote(str(run), safe='')}/{quote(str(attempt), safe='')}"


SPEC = ".spec"  # immutable, the engine's
CONTROL = ".control"  # who owns the attempt, the gate, the sealed result or the end (§2.4)
BEAT = ".beat"  # the owner's reports while HTTP fails: evidence, never a decision


def chunk(n: int) -> str:
    return f".log.{n:06d}"


# -- write-completion evidence (§2.3) ------------------------------------------------

NONE, COMPLETE = "none", "complete"  # and WRITING, as the gate says: a call may have landed

# -- the control file (§2.4) ---------------------------------------------------------

# Created `open` by the engine before the launch; then only swapped (`If-Match`):
# `owned` and `writing` (the gate) by the first worker, `sealed` (its result) by it,
# or `ended` by the engine. `sealed` and `ended` are final.
OPEN, OWNED, WRITING, SEALED, ENDED = "open", "owned", "writing", "sealed", "ended"
FINAL = (SEALED, ENDED)


def control(state: str, **fields) -> bytes:
    """A control file body: its state and what the state adds. Every body
    names its writer (`worker_id` or `engine`), so no two writers' bodies
    are the same bytes: `swap` relies on that to settle a lost answer."""

    return json.dumps({"state": state, **fields}, sort_keys=True, allow_nan=False).encode()


class Malformed(ValueError):
    """A control file no writer of this version would write: its version
    (`etag`) is kept, so the engine can still end it."""

    def __init__(self, reason: str, etag: str):
        super().__init__(f"malformed control file: {reason}")
        self.etag = etag


# What each state's writers put in a body (§2.4), and nothing else.
FIELDS = {
    OPEN: {"state", "engine"},
    OWNED: {"state", "worker_id", "host", "pid", "at"},
    WRITING: {"state", "worker_id", "intents"},
    SEALED: {"state", "worker_id", "result"},
    ENDED: {"state", "engine", "write", "intents"},
}


def check_control(data: bytes, etag: str) -> dict:
    """A control file's body, or `Malformed`: a JSON object in a known state,
    with what that state adds and no field it cannot have (an `open`
    file naming a worker would have the engine wait on one that never was)."""

    try:
        body = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        raise Malformed("not JSON", etag) from None
    if not isinstance(body, dict):
        raise Malformed("not an object", etag)
    state = body.get("state")
    if state not in FIELDS:
        raise Malformed(f"unknown state {state!r}", etag)
    if extra := sorted(set(body) - FIELDS[state]):
        raise Malformed(f"{state} with fields it cannot have: {', '.join(extra)}", etag)
    if state in (OWNED, WRITING, SEALED) and not isinstance(body.get("worker_id"), str):
        raise Malformed(f"{state} names no worker", etag)
    if state == WRITING and not isinstance(body.get("intents", {}), dict):
        raise Malformed("writing with intents that are not an object", etag)
    if state == SEALED and not isinstance(body.get("result"), dict):
        raise Malformed("sealed with no result", etag)
    return body


async def read_control(store, run: str, attempt: str) -> tuple[dict, str] | None:
    """The attempt's control file and its version, or `None` if there is
    none. Raises `Malformed` for one no writer of this version would write."""

    found = await read(store, f"{base(run, attempt)}{CONTROL}")
    return (check_control(*found), found[1]) if found is not None else None


# -- the cancel record (§2.2) --------------------------------------------------------

PHASES = ("requested", "forced")
REASONS = ("provisioning", "timeout", "user")  # in rising precedence


@dataclass(frozen=True)
class Cancel:
    """What the engine decided to stop, latched: `phase` only advances, and a
    reason of higher precedence replaces a lower one."""

    phase: str
    reason: str
    since: int  # the event counter at which the engine latched it

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
    """The engine says this attempt is over for this worker (`409`):
    it ended, or another worker owns it. Write nothing more."""

    def __init__(self, reason: str = "ended"):
        super().__init__(reason)
        self.reason = reason


def log_text(chunks: list[bytes], tail: str | None) -> bytes:
    """A log's JSON lines from its chunks' bytes and the result's `tail`."""

    text = b"".join(gzip.decompress(c) for c in chunks)
    if tail:
        text += gzip.decompress(base64.b64decode(tail))
    return text
