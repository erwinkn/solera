"""The failure index's record, transitions and eligibility
(docs/per-key-processing.md §9, the authoritative definition). The engine
and the worker both call these; nothing else decides them.

An `Each` asset keeps, per scope, a key index of the keys that did not
succeed: `key → record`, the record packed into the entry's version. A key
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
    """One failing key. Times are whole seconds; `epoch` and `forced` are the
    engine's positions the last try ran under, copied from the spec — never a
    worker's clock — so whether a key had its deploy or forced retry is
    decided causally. `last`, `next_at` and `until` are worker times, for
    display and scheduling only."""

    outcome: int
    tries: int
    epoch: int
    forced: int
    since: int
    last: int
    next_at: int  # a retrying or timed-out key's due time; 0 otherwise
    until: int  # when a retrying key turns failed; 0 otherwise
    revision: bytes  # the upstream version that failed
    message: str = ""

    @property
    def name(self) -> str:
        return NAMES[self.outcome]

    def encode(self) -> bytes:
        out = bytearray([self.outcome])
        for n in (self.tries, self.epoch, self.forced, self.since, self.last, self.next_at, self.until):
            _varint(out, n)
        message = _clip(self.message)
        for blob in (self.revision, message):
            _varint(out, len(blob))
            out += blob
        return bytes(out)

    @classmethod
    def decode(cls, data: bytes) -> Record:
        outcome, pos, ints = data[0], 1, []
        for _ in range(7):
            n, pos = _read_varint(data, pos)
            ints.append(n)
        blobs = []
        for _ in range(2):
            n, pos = _read_varint(data, pos)
            blobs.append(bytes(data[pos : pos + n]))
            pos += n
        return cls(outcome, *ints, blobs[0], blobs[1].decode(errors="replace"))


@dataclass(frozen=True)
class Outcome:
    """What one key came to: `kind` is `ok`, `removed`, `unmatched`, or an
    error's class (`rejected`, `failed`, `transient`) or interruption
    (`canceled`, `timed_out`); `revision` the upstream version it ran at."""

    kind: str
    revision: bytes = b""
    message: str = ""
    retry_after: float | None = None
    retry_for: float | None = None


def transition(
    prior: Record | None, outcome: Outcome, *, now: float, epoch: int, forced: int, retries: int
) -> Record | None:
    """The key's record after `outcome` (§9's transition table): `None` for
    no record — nothing, or a tombstone where `prior` existed. `epoch` and
    `forced` are the page's positions; `retries` the asset's `retries=`,
    which bounds timeouts."""

    if outcome.kind in GONE:
        return None
    code, t = KINDS[outcome.kind], int(now)  # `since` and `last`: whole seconds, for display
    fresh = prior is None or prior.revision != outcome.revision
    counted = 0 if code == CANCELED else 1  # a cancel interrupted the try: it does not count
    tries = counted if fresh else prior.tries + counted
    since = t if fresh else prior.since
    record = Record(code, tries, epoch, forced, since, t, 0, 0, outcome.revision, outcome.message)
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


def eligible(record: Record, now: float, epoch: int, forced: dict[str, int]) -> bool:
    """Whether a retry pass takes `record`'s key (§9). Each clause retires
    itself: a retried key's `next_at` moves on, its `epoch` becomes the
    pass's, its `forced` the pass's position. A canceled key matches only a
    forced request."""

    return (
        (record.outcome in (RETRYING, TIMED_OUT) and record.next_at <= now)
        or (record.outcome == FAILED and record.epoch < epoch)
        or record.forced < int(forced.get(record.name, 0))
    )


def minima(records) -> tuple[int | None, int | None]:
    """`(due, epoch)`: the earliest `next_at` of retrying and timed-out
    records, and the lowest `epoch` of failed ones — `None` where there are
    none."""

    due = epoch = None
    for r in records:
        if r is None:
            continue
        if r.outcome in (RETRYING, TIMED_OUT):
            due = r.next_at if due is None else min(due, r.next_at)
        elif r.outcome == FAILED:
            epoch = r.epoch if epoch is None else min(epoch, r.epoch)
    return due, epoch


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
