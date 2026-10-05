"""The failed keys's record, transitions and eligibility
(docs/per-key-processing.md §9, the authoritative definition). The engine
and the worker both call these; nothing else decides them.

A per-key asset keeps, per partition, a key index of the keys that did not
succeed: `key → record`, the record packed into the entry's payload. A key
that succeeds, is removed upstream or stops matching gets a tombstone.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

from .errors import backoff

REJECTED, FAILED, RETRYING, CANCELED, TIMED_OUT = 1, 2, 3, 4, 5
NAMES = {
    REJECTED: "rejected",
    FAILED: "failed",
    RETRYING: "retrying",
    CANCELED: "canceled",
    TIMED_OUT: "timed_out",
}
MESSAGE_MAX = 200  # bytes of an error message kept with its record

# What a per-key call came to — the outcome a transition takes.
OK, REMOVED, UNMATCHED = "ok", "removed", "unmatched"
GONE = (OK, REMOVED, UNMATCHED)
KINDS = {
    "rejected": REJECTED,
    "failed": FAILED,
    "transient": RETRYING,
    "canceled": CANCELED,
    "timed_out": TIMED_OUT,
}


@dataclass(frozen=True)
class Record:
    """One failing key. Times are whole seconds; `deploy` and `forced` are the
    engine's event counters the last try ran under, copied from the spec — never a
    worker's clock — so whether a key had its deploy or forced retry is
    decided causally. `last`, `next_at` and `until` are worker times, for
    display and scheduling only."""

    outcome: int
    tries: int
    deploy: int
    forced: int
    since: int
    last: int
    next_at: int  # a retrying or timed-out key's due time; 0 otherwise
    until: int  # when a retrying key turns failed; 0 otherwise
    upstream: int  # the generation of the upstream key that failed: its version
    message: str = ""

    @property
    def name(self) -> str:
        return NAMES[self.outcome]

    def encode(self) -> bytes:
        out = bytearray([self.outcome])
        for n in (
            self.tries,
            self.deploy,
            self.forced,
            self.since,
            self.last,
            self.next_at,
            self.until,
            self.upstream,
        ):
            _varint(out, n)
        message = _clip(self.message)
        _varint(out, len(message))
        out += message
        return bytes(out)

    @classmethod
    def decode(cls, data: bytes) -> Record:
        outcome, pos, ints = data[0], 1, []
        for _ in range(8):
            n, pos = _read_varint(data, pos)
            ints.append(n)
        n, pos = _read_varint(data, pos)
        return cls(outcome, *ints, bytes(data[pos : pos + n]).decode(errors="replace"))


@dataclass(frozen=True)
class Outcome:
    """What one key came to: `kind` is `ok`, `removed`, `unmatched`, or an
    error's class (`rejected`, `failed`, `transient`) or interruption
    (`canceled`, `timed_out`); `upstream` the generation of the upstream
    key it ran at."""

    kind: str
    upstream: int = 0
    message: str = ""
    retry_after: float | None = None
    retry_for: float | None = None


def transition(
    prior: Record | None, outcome: Outcome, *, now: float, deploy: int, forced: int, retries: int
) -> Record | None:
    """The key's record after `outcome` (§9's transition table): `None` for
    no record — nothing, or a tombstone where `prior` existed. `deploy` and
    `forced` are the batch's event counters; `retries` the asset's `retries=`,
    which bounds timeouts."""

    if outcome.kind in GONE:
        return None
    code, t = KINDS[outcome.kind], int(now)  # `since` and `last`: whole seconds, for display
    fresh = prior is None or prior.upstream != outcome.upstream
    counted = 0 if code == CANCELED else 1  # a cancel interrupted the try: it does not count
    tries = counted if fresh else prior.tries + counted
    since = t if fresh else prior.since
    record = Record(code, tries, deploy, forced, since, t, 0, 0, outcome.upstream, outcome.message)
    # Deadlines are computed from the exact time, then rounded up: a budget or a wait
    # is never shortened by the rounding.
    if code == RETRYING:
        if not fresh and prior.outcome == RETRYING:
            until = prior.until
        else:
            until = math.ceil(now + float(outcome.retry_for or 0))
        if now >= until:
            return replace(record, outcome=FAILED)  # its retry_for ran out
        wait = outcome.retry_after if outcome.retry_after is not None else backoff(tries)
        return replace(record, next_at=math.ceil(now + wait), until=until)
    if code == TIMED_OUT:
        if tries > retries:
            return replace(record, outcome=FAILED)  # always outlives the timeout: stop cycling
        return replace(record, next_at=math.ceil(now + backoff(tries)))
    return record


def eligible(record: Record, now: float, deploy: int, forced: dict[str, int]) -> bool:
    """Whether a retry pass takes `record`'s key (§9). Each clause retires
    itself: a retried key's `next_at` moves on, its `deploy` becomes the
    pass's, its `forced` the pass's event counter. A canceled key matches only a
    forced request."""

    return (
        (record.outcome in (RETRYING, TIMED_OUT) and record.next_at <= now)
        or (record.outcome == FAILED and record.deploy < deploy)
        or record.forced < int(forced.get(record.name, 0))
    )


def minima(records) -> tuple[int | None, int | None]:
    """`(due, deploy)`: the earliest `next_at` of retrying and timed-out
    records, and the lowest `deploy` of failed ones — `None` where there are
    none."""

    due = deploy = None
    for r in records:
        if r is None:
            continue
        if r.outcome in (RETRYING, TIMED_OUT):
            due = r.next_at if due is None else min(due, r.next_at)
        elif r.outcome == FAILED:
            deploy = r.deploy if deploy is None else min(deploy, r.deploy)
    return due, deploy


def lower(a: int | float | None, b: int | float | None):
    """The lower of two bounds, `None` meaning none."""

    return b if a is None else a if b is None else min(a, b)


def _clip(message: str) -> bytes:
    data = message.encode(errors="replace")
    if len(data) <= MESSAGE_MAX:
        return data
    return data[:MESSAGE_MAX].decode(errors="ignore").encode()


def _varint(out: bytearray, n: int) -> None:
    n = max(int(n), 0)
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    n = shift = 0
    while True:
        b = data[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if b < 0x80:
            return n, pos
        shift += 7
