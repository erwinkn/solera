"""A panic in the native extension is an `Exception`, not pyo3's
`PanicException` — a `BaseException` that every `except Exception` in the
engine and the worker would let through, ending the process."""

import pytest
from solera import _native


@pytest.mark.parametrize("parallel", [False, True], ids=["caller", "rayon"])
def test_a_native_panic_is_an_exception(parallel):
    with pytest.raises(RuntimeError, match="solera._native panicked: an invariant broke") as raised:
        _native._panic("an invariant broke", parallel=parallel)
    assert isinstance(raised.value, Exception)
    assert _native.filter_nbits(100, 14) > 0  # and the module works on
