"""The failure record, its transitions and eligibility (docs/per-key-processing.md §9)."""

from solera.failures import (
    CANCELED,
    FAILED,
    REJECTED,
    RETRYING,
    TIMED_OUT,
    Outcome,
    Record,
    eligible,
    lower,
    minima,
    transition,
)


def step(prior, kind, now=1000, epoch=3, forced=0, retries=2, revision=b"r1", **kw):
    return transition(
        prior,
        Outcome(kind, revision, kw.pop("message", ""), **kw),
        now=now,
        epoch=epoch,
        forced=forced,
        retries=retries,
    )


def test_record_round_trips_and_clips_its_message():
    r = Record(FAILED, 300, 7, 4031, 10, 20, 0, 0, b"\x00\xffrev", "é" * 300)
    back = Record.decode(r.encode())
    assert back.tries == 300 and back.forced == 4031 and back.revision == b"\x00\xffrev"
    assert len(back.message.encode()) <= 200 and back.message == "é" * 100


def test_transition_table():
    assert step(None, "ok") is None  # none → ok: nothing
    fresh = step(None, "failed", message="ValueError: x")
    assert (fresh.outcome, fresh.tries, fresh.since, fresh.last, fresh.epoch) == (FAILED, 1, 1000, 1000, 3)
    again = step(fresh, "failed", now=2000, epoch=4)
    assert (again.tries, again.since, again.last, again.epoch) == (2, 1000, 2000, 4)
    assert step(again, "ok") is None and step(again, "removed") is None  # tombstones
    # Another revision is a fresh record.
    assert step(again, "failed", revision=b"r2").tries == 1
    # Another class keeps `since`, counts the try.
    rejected = step(again, "rejected", now=3000)
    assert (rejected.outcome, rejected.tries, rejected.since) == (REJECTED, 3, 1000)


def test_transient_backoff_and_budget():
    first = step(None, "transient", retry_for=7200)
    assert (first.outcome, first.next_at, first.until) == (RETRYING, 1060, 8200)  # one minute
    second = step(first, "transient", now=1100, retry_for=7200)
    assert (second.tries, second.until, second.next_at) == (2, 8200, 1220)  # doubles; until kept
    told = step(second, "transient", now=1300, retry_after=5, retry_for=7200)
    assert told.next_at == 1305
    assert step(told, "transient", now=8200, retry_for=7200).outcome == FAILED  # retry_for ran out


def test_interruptions():
    canceled = step(None, "canceled")
    assert (canceled.outcome, canceled.tries, canceled.next_at) == (CANCELED, 0, 0)
    failed = step(None, "failed")
    assert step(failed, "canceled").tries == 1  # a cancel does not count a try
    timed = step(None, "timed_out")
    assert (timed.outcome, timed.tries, timed.next_at) == (TIMED_OUT, 1, 1060)
    timed = step(step(timed, "timed_out"), "timed_out")
    assert timed.outcome == FAILED  # past retries=2: it stops cycling


def test_eligibility_is_causal():
    retrying = step(None, "transient", retry_for=7200)
    assert not eligible(retrying, 1059, 3, {}) and eligible(retrying, 1060, 3, {})
    failed = step(None, "failed", epoch=3)
    assert not eligible(failed, 10**9, 3, {}) and eligible(failed, 0, 4, {})  # one try per deploy
    # Forced requests are positions: a try under position 4031 satisfies it, whatever the clocks.
    tried = step(None, "failed", epoch=3, forced=4031, now=10**9)
    assert not eligible(tried, 0, 3, {"failed": 4031}) and eligible(tried, 0, 3, {"failed": 4032})
    assert not eligible(tried, 0, 3, {"rejected": 9999})  # another class's request
    canceled = step(None, "canceled", forced=10)
    assert not eligible(canceled, 10**9, 99, {}) and eligible(canceled, 0, 3, {"canceled": 11})


def test_minima_and_bounds():
    records = [step(None, "transient", now=100, retry_for=99999), step(None, "failed", epoch=2), None]
    assert minima(records) == (160, 2)
    assert minima([]) == (None, None)
    assert lower(None, 5) == 5 and lower(3, None) == 3 and lower(3, 5) == 3
