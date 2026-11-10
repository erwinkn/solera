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


def test_regex_is_rusts_dialect_matched_against_the_whole_key():
    """`Regex` is the Rust `regex` dialect, matched natively by the planner's
    matcher: what it lacks is refused when the pattern is declared."""
    for unsupported in [r"(?!archive/).*\.csv", r".*(?<=\.csv)", r"(a)\1"]:
        with pytest.raises(ValueError, match="lookaround and backreferences are not supported"):
            Regex(unsupported)
    word = Matcher(spec(include=Regex(r"\w+")))
    assert word("e\u0301")  # Rust's \w takes combining marks; Python's re does not
    assert not word("a/b") and not Matcher(spec(include=Regex("ab")))("abc")
    assert Matcher(spec(include=Regex("a.b")))("a\nb")
