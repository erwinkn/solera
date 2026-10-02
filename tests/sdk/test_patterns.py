"""Key patterns on edges (docs/per-key-processing.md §11)."""

import pytest
from solera import Each, Incremental, Regex
from solera.patterns import Matcher, spec
from solera.sdk import RegistrationError


def test_globs():
    m = Matcher(spec("ICP/Results/**/*.csv"))
    assert m("ICP/Results/a.csv") and m("ICP/Results/2026/09/a.csv")
    assert not m("ICP/Results/a.xlsx") and not m("XRF/ICP/Results/a.csv")
    assert Matcher(spec("*.csv"))("a.csv") and not Matcher(spec("*.csv"))("dir/a.csv")
    assert Matcher(spec("f?.csv"))("f1.csv") and not Matcher(spec("f?.csv"))("f/.csv")
    assert Matcher(spec("a+b(c).csv"))("a+b(c).csv")  # literal characters stay literal


def test_include_exclude_and_names():
    m = Matcher(spec(["XRF/**", "XRF Data/**"], {"archive": "**/archive/**", "old": Regex(r".*\bold\b.*")}))
    assert m("XRF/run 1.csv") and m("XRF Data/x.csv")
    assert not m("XRF/archive/run.csv") and m.excluded_by("XRF/archive/run.csv") == "archive"
    assert not m("XRF/an old run.csv") and m("XRF/Gold_ore.csv")  # words, not substrings
    assert Matcher(None)("anything") and Matcher(spec(exclude=["*.tmp"]))("a.csv")
    assert Matcher(spec(exclude=["*.tmp"])).excluded_by("x.tmp") == "exclude[0]"


def test_edges_carry_patterns():
    assert Incremental("f").spec("x").get("patterns") is None
    e = Each("f", include="a/**", exclude={"tmp": "**/*.tmp"}).spec("x")
    assert e["patterns"] == {"include": [{"glob": "a/**"}], "exclude": [["tmp", {"glob": "**/*.tmp"}]]}
    with pytest.raises(RegistrationError):
        Each("f", include=[])
    with pytest.raises(RegistrationError):
        Incremental("f", exclude=[""])
