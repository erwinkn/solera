"""Events on the tracking core's state: refused whole, never half applied."""

import pytest
from solera._native import Tracking


def test_an_event_that_changes_one_edge_twice_is_refused_and_leaves_the_state_as_it_was():
    t = Tracking()
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    before = t.snapshot()
    both = Tracking.join([Tracking.event_edge_close(b"e"), Tracking.event_reset(b"e", False)])
    with pytest.raises(ValueError, match="two parts of one event change edge e"):
        t.apply(both)
    assert t.snapshot() == before
    t.apply(Tracking.event_edge_close(b"e"))


def test_a_page_of_owed_keys_holds_at_least_one_key():
    t = Tracking()
    t.apply(Tracking.event_edge_open(b"e", b"items", True))
    with pytest.raises(ValueError, match="at least one key"):
        t.owed(b"e", {}, limit=0)
