"""The contract is complete: every entry is tested or pending with a reason,
and no contract test reads today's internals."""

from tests.contract import check


def test_every_entry_has_a_test_or_a_pending_reason():
    print(check.summary())
    assert not check.problems(), "\n".join(check.problems())
