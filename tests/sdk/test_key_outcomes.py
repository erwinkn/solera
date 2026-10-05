"""The stored outcome, its transitions and eligibility (docs/per-key-processing.md §9)."""

from solera.key_outcomes import (
    CANCELED,
    FAILED,
    REJECTED,
    RETRYING,
    TIMED_OUT,
    Outcome,
    StoredOutcome,
    eligible,
    lower,
    minima,
    transition,
)


def step(prior, kind, now=1000, deploy=3, forced=0, retries=2, upstream=11, **kw):
    return transition(
        prior,
        Outcome(kind, upstream, kw.pop("message", ""), **kw),
        now=now,
        deploy=deploy,
        forced=forced,
        retries=retries,
    )


def test_record_round_trips_and_clips_its_message():
    r = StoredOutcome(FAILED, 300, 7, 4031, 10, 20, 0, 0, 1 << 40, "é" * 300)
    back = StoredOutcome.decode(r.encode())
    assert back.tries == 300 and back.forced == 4031 and back.upstream == 1 << 40
    assert len(back.message.encode()) <= 200 and back.message == "é" * 100


def test_transition_table():
    assert step(None, "ok") is None  # none → ok: nothing
    fresh = step(None, "failed", message="ValueError: x")
    assert (fresh.outcome, fresh.tries, fresh.since, fresh.last, fresh.deploy) == (FAILED, 1, 1000, 1000, 3)
    again = step(fresh, "failed", now=2000, deploy=4)
    assert (again.tries, again.since, again.last, again.deploy) == (2, 1000, 2000, 4)
    assert step(again, "ok") is None and step(again, "removed") is None  # tombstones
    # Another upstream generation — the key written since — is a fresh record.
    assert step(again, "failed", upstream=12).tries == 1
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
    failed = step(None, "failed", deploy=3)
    assert not eligible(failed, 10**9, 3, {}) and eligible(failed, 0, 4, {})  # one try per deploy
    # Forced requests are event counters: a try under event counter 4031 satisfies it, whatever the clocks.
    tried = step(None, "failed", deploy=3, forced=4031, now=10**9)
    assert not eligible(tried, 0, 3, {"failed": 4031}) and eligible(tried, 0, 3, {"failed": 4032})
    assert not eligible(tried, 0, 3, {"rejected": 9999})  # another class's request
    canceled = step(None, "canceled", forced=10)
    assert not eligible(canceled, 10**9, 99, {}) and eligible(canceled, 0, 3, {"canceled": 11})


def test_minima_and_bounds():
    records = [step(None, "transient", now=100, retry_for=99999), step(None, "failed", deploy=2), None]
    assert minima(records) == (160, 2)
    assert minima([]) == (None, None)
    assert lower(None, 5) == 5 and lower(3, None) == 3 and lower(3, 5) == 3


def test_deadlines_are_never_shortened_by_rounding():
    """Review 11: a budget or a wait is computed from the exact time, then rounded up."""

    half = transition(
        None, Outcome("transient", 11, retry_for=0.5), now=1000.5, deploy=0, forced=0, retries=0
    )
    assert half.outcome == RETRYING and half.until == 1001
    soon = transition(
        None,
        Outcome("transient", 11, retry_after=1, retry_for=60),
        now=1000.9,
        deploy=0,
        forced=0,
        retries=0,
    )
    assert soon.next_at == 1002  # not 1001, a tenth of a second later


def test_backoff_saturates():
    from solera.errors import BACKOFF_MAX, backoff

    assert backoff(1025) == BACKOFF_MAX and backoff(10**9) == BACKOFF_MAX
