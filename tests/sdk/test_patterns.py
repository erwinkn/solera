"""Key patterns on inputs (docs/per-key-processing.md §11)."""

import pytest
from solera import Incremental, Regex
from solera.patterns import Matcher, spec
from solera.sdk import RegistrationError


def test_include_exclude_and_names():
    m = Matcher(spec(["XRF/**", "XRF Data/**"], {"archive": "**/archive/**", "old": Regex(r".*\bold\b.*")}))
    assert m("XRF/run 1.csv") and m("XRF Data/x.csv")
    assert not m("XRF/archive/run.csv") and m.excluded_by("XRF/archive/run.csv") == "archive"
    assert not m("XRF/an old run.csv") and m("XRF/Gold_ore.csv")  # words, not substrings
    assert Matcher(None)("anything") and Matcher(spec(exclude=["*.tmp"]))("a.csv")
    assert Matcher(spec(exclude=["*.tmp"])).excluded_by("x.tmp") == "exclude[0]"


def test_edges_carry_patterns():
    assert Incremental("f").spec("x").get("patterns") is None
    e = Incremental("f", include="a/**", exclude={"tmp": "**/*.tmp"}, each=True).spec("x")
    assert e["patterns"] == {"include": [{"glob": "a/**"}], "exclude": [["tmp", {"glob": "**/*.tmp"}]]}
    with pytest.raises(RegistrationError):
        Incremental("f", include=[], each=True)
    with pytest.raises(RegistrationError):
        Incremental("f", exclude=[""])
