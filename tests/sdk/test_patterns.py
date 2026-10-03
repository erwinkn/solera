"""Key patterns on inputs (docs/per-key-processing.md §11)."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
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


def test_one_exclude_pattern_is_a_pattern_not_its_characters():
    """A string `exclude=` is one pattern, as a string `include=` is: `'k1*'`
    leaves out `k1…` only, never every key through its `'*'`."""

    assert spec(exclude="k1*") == spec(exclude=["k1*"])
    taken = Matcher(spec(exclude="k1*"))
    assert taken("k2") and taken("a") and not taken("k10")
    assert Matcher(spec(include="k*", exclude=Regex("k1.*")))("k2")


def _glob(glob: str, key: str) -> bool:
    """A reference matcher, independent of the regex translation: `**/`
    takes whole directories (or none), `**` anything, `*` and `?` stay
    within one directory, every other character is itself."""

    from functools import cache

    @cache
    def match(g: int, k: int) -> bool:
        if g == len(glob):
            return k == len(key)
        if glob.startswith("**/", g):
            return match(g + 3, k) or any(key[j] == "/" and match(g + 3, j + 1) for j in range(k, len(key)))
        if glob.startswith("**", g):
            return any(match(g + 2, j) for j in range(k, len(key) + 1))
        if glob[g] == "*":
            return any(match(g + 1, j) for j in range(k, len(key) + 1) if "/" not in key[k:j])
        if k == len(key):
            return False
        if glob[g] == "?":
            return key[k] != "/" and match(g + 1, k + 1)
        return glob[g] == key[k] and match(g + 1, k + 1)

    return match(0, 0)


_globs = st.lists(
    st.sampled_from(["a", "b", "/", ".", "*", "**", "**/", "?", "[", "\\", "\n"]), max_size=8
).map("".join)
_keys = st.text(st.sampled_from(["a", "b", "/", ".", "*", "?", "[", "\\", "\n"]), max_size=8)


@settings(max_examples=500, deadline=None)
@given(glob=_globs.filter(bool), key=_keys)
def test_a_glob_matches_as_the_reference_does(glob, key):
    assert Matcher(spec(glob))(key) == _glob(glob, key)


@settings(max_examples=200, deadline=None)
@given(
    include=st.one_of(st.none(), st.lists(_globs.filter(bool), min_size=1, max_size=3)),
    exclude=st.lists(_globs.filter(bool), max_size=3),
    key=_keys,
)
def test_a_key_is_taken_when_an_include_and_no_exclude_matches(include, exclude, key):
    m = Matcher(spec(include, exclude))
    excluded = [n for n, g in enumerate(exclude) if _glob(g, key)]
    assert m.excluded_by(key) == (f"exclude[{excluded[0]}]" if excluded else None)
    assert m(key) == ((include is None or any(_glob(g, key) for g in include)) and not excluded)
