"""Error classes at the attempt level (docs/per-key-processing.md §8)."""

import httpx
import pytest
from solera import Abort, Failed, Rejected, Transient
from solera.errors import ABORT, FAILED, REJECTED, TRANSIENT, backoff, classify, seconds
from solera.sdk import Project, RegistrationError, Retry, asset

from .engines import make_engine


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
        await engine.run_until(run["id"], 1)  # deliberately short: the backoff waits a minute
    detail = await engine.run_detail(run["id"])
    assert calls["n"] == 1 and detail["tasks"][0]["status"] == "queued"
