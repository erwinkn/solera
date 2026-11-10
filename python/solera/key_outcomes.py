"""The stored outcomes' record, transitions and eligibility
(docs/per-key-processing.md §9, the authoritative definition). The engine
and the worker both call these; nothing else decides them. The transition
table, the backoff and eligibility are the tracking core's
(`native/tracking/src/outcomes/transition.rs`): one definition, which
this module and the core's retries both use.

A per-key asset keeps, per partition, a key index of the keys that did not
succeed: `key → record`, the record packed into the entry's payload. A key
that succeeds, is removed upstream or stops matching gets a tombstone.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import _native

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
class StoredOutcome:
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
    def decode(cls, data: bytes) -> StoredOutcome:
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
    prior: StoredOutcome | None, outcome: Outcome, *, now: float, deploy: int, forced: int, retries: int
) -> StoredOutcome | None:
    """The key's record after `outcome` (§9's transition table): `None` for
    no record — nothing, or a tombstone where `prior` existed. `deploy` and
    `forced` are the batch's event counters; `retries` the asset's `retries=`,
    which bounds timeouts."""

    if outcome.kind in GONE:
        return None
    fresh = prior is None or prior.upstream != outcome.upstream
    before = None
    if prior is not None:
        before = (
            prior.outcome,
            prior.tries,
            prior.deploy,
            prior.forced,
            prior.since,
            prior.last,
            prior.next_at,
            prior.until,
            prior.message.encode(errors="replace"),
        )
    after = _native.outcome_transition(
        before,
        fresh,
        outcome.kind,
        outcome.message.encode(errors="replace"),
        None if outcome.retry_after is None else float(outcome.retry_after),
        None if outcome.retry_for is None else float(outcome.retry_for),
        float(now),
        int(deploy),
        int(forced),
        int(retries),
    )
    if after is None:
        return None
    *counts, message = after
    return StoredOutcome(*counts, outcome.upstream, message.decode(errors="replace"))


def eligible(record: StoredOutcome, now: float, deploy: int, forced: dict[str, int]) -> bool:
    """Whether a retry pass takes `record`'s key (§9). Each clause retires
    itself: a retried key's `next_at` moves on, its `deploy` becomes the
    pass's, its `forced` the pass's event counter. A canceled key matches only a
    forced request."""

    requests = [0] + [int(forced.get(NAMES[code], 0)) for code in range(1, 6)]
    return _native.outcome_eligible(
        record.outcome, record.next_at, record.deploy, record.forced, float(now), int(deploy), requests
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
