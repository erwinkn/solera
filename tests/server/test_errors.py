"""Error classes at the attempt level (docs/per-key-processing.md §8)."""

import httpx
import pytest
from solera import Abort, Failed, Rejected, Transient
from solera.errors import ABORT, FAILED, REJECTED, TRANSIENT, backoff, classify, seconds
from solera.sdk import Project, RegistrationError, Retry, asset

from .engines import drive, make_engine, status_of


class Unprocessable(Rejected):
    pass


class Throttled(Transient):
    retry_for = "2h"


def test_classify_by_method_resolution_order():
    assert classify(Unprocessable("empty"))[0] == REJECTED
    assert classify(ValueError("x"))[0] == FAILED
    assert classify(Failed("x"))[0] == FAILED
    assert classify(Abort("x"))[0] == ABORT
    kind, timing = classify(Throttled(retry_after=30))
    assert kind == TRANSIENT and timing == {"retry_after": 30.0, "retry_for": 7200.0}
    kind, timing = classify(Transient("x", retry_for="90m"))
    assert timing == {"retry_after": None, "retry_for": 5400.0}

    class Mine(httpx.TimeoutException):
        pass

    mapping = {httpx.TimeoutException: Throttled, KeyError: Abort}
    assert classify(Mine("t"), mapping) == (TRANSIENT, {"retry_after": None, "retry_for": 7200.0})
    assert classify(KeyError("k"), mapping)[0] == ABORT
    # A Solera class the error subclasses comes before a mapped base further up.

    class Both(Rejected, KeyError):
        pass

    assert classify(Both("k"), mapping)[0] == REJECTED


def test_durations_and_backoff():
    assert seconds(30) == 30 and seconds("2h") == 7200 and seconds("1.5m") == 90 and seconds("250ms") == 0.25
    with pytest.raises(ValueError):
        seconds("soon")
    with pytest.raises(ValueError):
        Transient(retry_for="later")
    assert [backoff(n) for n in (1, 2, 3)] == [60, 120, 240]
    assert backoff(20) == 6 * 3600


def test_errors_mapping_is_checked_at_registration():
    with pytest.raises(RegistrationError, match="not Rejected"):
        Project(errors={KeyError: ValueError})
    with pytest.raises(RegistrationError, match="not an exception type"):
        Project(errors={"KeyError": Abort})


async def test_rejected_fails_without_retries(state):
    calls = {"n": 0}

    @asset(retries=Retry(3, delay=0.01))
    def bad():
        calls["n"] += 1
        raise Unprocessable("empty file")

    project = Project(assets=[bad])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["bad"]))
    assert status_of(detail) == "failed" and calls["n"] == 1
    assert "Unprocessable: empty file" in detail["attempts"][detail["tasks"][0]["id"]][0]["error"]


async def test_failed_and_abort_follow_retries(state):
    calls = {"failed": 0, "abort": 0}

    @asset(retries=Retry(1, delay=0.01))
    def unclassified():
        calls["failed"] += 1
        raise ValueError("bug")

    @asset(retries=Retry(1, delay=0.01))
    def aborts():
        calls["abort"] += 1
        raise Abort("credentials")

    project = Project(assets=[unclassified, aborts])
    engine = make_engine(state, project)
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["unclassified"]))) == "failed"
    assert status_of(await drive(engine, await engine.submit(["aborts"]))) == "failed"
    assert calls == {"failed": 2, "abort": 2}


async def test_transient_retries_past_retries_within_its_budget(state):
    """A Transient error is retried after its `retry_after` even with no
    retries left, until `retry_for` has passed since the first one."""

    calls = {"n": 0}

    @asset(retries=Retry(0))
    def throttled():
        calls["n"] += 1
        if calls["n"] < 4:
            raise Throttled("429", retry_after=0.01)
        return ["ok"]

    project = Project(assets=[throttled])
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["throttled"]))
    assert status_of(detail) == "succeeded" and calls["n"] == 4


async def test_transient_gives_up_after_retry_for(state):
    calls = {"n": 0}

    @asset(retries=Retry(0))
    def flapping():
        calls["n"] += 1
        raise Transient("down", retry_after=0.05, retry_for=0.2)

    project = Project(assets=[flapping], errors={})
    engine = make_engine(state, project)
    await engine.initialize()
    detail = await drive(engine, await engine.submit(["flapping"]))
    assert status_of(detail) == "failed"
    assert 2 <= calls["n"] <= 6


async def test_mapped_exception_is_transient(state):
    calls = {"n": 0}

    @asset(retries=Retry(0))
    def timeouts():
        calls["n"] += 1
        if calls["n"] < 2:
            raise httpx.ReadTimeout("slow")
        return ["ok"]

    class Quick(Transient):
        retry_for = "1h"

    project = Project(assets=[timeouts], errors={httpx.TimeoutException: Quick})
    engine = make_engine(state, project)
    await engine.initialize()
    # The mapped class has no retry_after: the backoff's first wait is a minute.
    run = await engine.submit(["timeouts"])
    with pytest.raises(TimeoutError):
        await engine.run_until(run["id"], 1)
    detail = await engine.run_detail(run["id"])
    assert calls["n"] == 1 and detail["tasks"][0]["status"] == "queued"
