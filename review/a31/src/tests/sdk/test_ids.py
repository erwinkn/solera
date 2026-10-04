"""ULIDs sort in creation order, within a millisecond too."""

from solera.ids import ulid, ulid_time


def test_ids_made_in_one_millisecond_sort_as_made():
    ids = [ulid(1_000_000.0) for _ in range(100)]
    assert ids == sorted(ids) and len(set(ids)) == 100
    assert {ulid_time(i) for i in ids} == {1_000_000.0}


def test_a_clock_that_went_back_keeps_its_own_time():
    later, earlier = ulid(2_000.0), ulid(1_000.0)
    assert ulid_time(earlier) == 1_000.0 and earlier < later
