"""§7: partition dimensions — static, time, key-set — and key generation."""

import datetime as dt

import pytest
from solera.sdk import (
    Project,
    RegistrationError,
    StaticPartitions,
    TimePartitions,
    asset,
    canonical_partition,
    split_partition,
)

AS_OF = dt.datetime(2026, 9, 19, 12, 0, 0, tzinfo=dt.UTC)


def test_daily_keys():
    """§7: every='1d' yields half-open daily windows keyed YYYY-MM-DD."""

    tp = TimePartitions(start="2026-09-17", every="1d")
    assert tp.keys(AS_OF) == ["2026-09-17", "2026-09-18"]
    assert tp.latest(AS_OF) == "2026-09-18"
    assert tp.window("2026-09-18") == ("2026-09-18T00:00:00+00:00", "2026-09-19T00:00:00+00:00")


def test_minute_keys():
    """§7: every='15m' yields quarter-hour windows."""

    tp = TimePartitions(start="2026-09-19T10:00:00", every="15m")
    keys = tp.keys(AS_OF)
    assert keys[0] == "2026-09-19T10:00"
    assert keys[-1] == "2026-09-19T11:45"
    assert len(keys) == 8


def test_cron_every():
    """§7: a cron 'every' makes calendar partitions; format is required."""

    # Mondays at 00:00 UTC: windows close at the next fire, so by Sep 19
    # (a Saturday) the Sep 14 window is still open.
    tp = TimePartitions(start="2026-08-31", every="0 0 * * 1", format="%Y-%m-%d")
    keys = tp.keys(AS_OF)
    assert keys == ["2026-08-31", "2026-09-07"]
    with pytest.raises(RegistrationError):
        TimePartitions(start="2024-01-01", every="0 0 * * 1")


def test_end_offset_excludes_recent_windows():
    """§7: end_offset (a duration) shifts the newest complete window back."""

    tp = TimePartitions(start="2026-09-16", every="1d", end_offset="1d")
    assert tp.keys(AS_OF) == ["2026-09-16", "2026-09-17"]
    assert tp.latest(AS_OF) == "2026-09-17"


def test_end_caps_the_set():
    """§7: an explicit end caps the set even when now is later."""

    tp = TimePartitions(start="2026-09-17", every="1d", end="2026-09-19")
    assert tp.keys(AS_OF) == ["2026-09-17", "2026-09-18"]


def test_timezone_alignment():
    """§7: windows align to the declared timezone, not UTC."""

    tp = TimePartitions(start="2026-09-17", every="1d", timezone="America/New_York")
    start, end = tp.window("2026-09-18")
    assert start == "2026-09-18T00:00:00-04:00"
    assert end == "2026-09-19T00:00:00-04:00"


def test_canonical_key_encoding():
    """§7: multi-dimension keys are `dim=key` sorted by dimension name."""

    dims = {"site": {"kind": "static", "keys": ["Richmond"]}, "day": {"kind": "time"}}
    key = canonical_partition(dims, {"site": "Richmond", "day": "2026-09-19"})
    assert key == "day=2026-09-19,site=Richmond"
    assert split_partition(dims, key) == {"site": "Richmond", "day": "2026-09-19"}
    single = {"site": {"kind": "static", "keys": ["Richmond"]}}
    assert canonical_partition(single, {"site": "Richmond"}) == "Richmond"
    assert split_partition(single, "Richmond") == {"site": "Richmond"}


def test_shared_dimension_identity():
    """§7: two assets bound to the same declaration share the dimension."""

    sites = StaticPartitions(["a", "b"])

    @asset(partitions={"site": sites})
    def up():
        return []

    @asset(partitions={"site": sites}, inputs={"up": "up"})
    def down(up: list):
        return up

    project = Project(assets=[up, down])
    assert project.manifest["assets"]["down"]["partitions"]["dims"]["site"]["kind"] == "static"
