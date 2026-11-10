"""CHG: definition, pattern and context changes (docs/behaviors.md)."""

import asyncio

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from solera import Incremental, Regex
from solera.patterns import Matcher, spec
from solera.sdk import Output, Project, asset
from solera.stores import FileStore

from tests.contract.example import Example


def hold(ex: Example) -> tuple[asyncio.Event, asyncio.Event]:
    """`tally`'s next batch waits, once it has read its inputs, until
    released: (entered, release)."""

    entered, release = asyncio.Event(), asyncio.Event()

    async def held():
        entered.set()
        await release.wait()

    ex.during = held
    return entered, release


def delivered(ex: Example, name: str = "tally", since: int = 0) -> list[str]:
    """Every key the producer's batches delivered as changed since its
    `since`-th batch, in order: added, updated or removed."""

    return [k for b in ex.seen.get(name, [])[since:] for k in b.added + b.updated + b.removed]


async def test_chg_2_a_code_change_alone_is_a_new_deploy_not_a_definition_change(solera, monkeypatch):
    monkeypatch.setenv("SOLERA_BUILD", "one")
    ex = await Example(solera).start()
    ex.failing["k1"] = ValueError("bug")
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.run("copy")
    for _ in range(2):
        await solera.run("checks", expect=None)
    assert sorted(ex.calls["checks"]) == ["k1", "k2"]  # k1 failed, and is not tried again on this deploy
    names = ("items", "tally", "copy", "checks")
    before = {name: await solera.stale(name) for name in names}
    assert before["tally"] == before["copy"] == []
    first = (await solera.get("/manifest"))["deploy"]

    monkeypatch.setenv("SOLERA_BUILD", "two")  # a helper module edited: the declaration is the same
    await ex.deploy()
    assert (await solera.get("/manifest"))["deploy"] != first
    assert {name: await solera.stale(name) for name in names} == before
    batches = len(ex.seen["tally"])
    await solera.run("tally", expect=None)
    assert delivered(ex, since=batches) == []
    for _ in range(2):
        await solera.run("checks", expect=None)
    assert sorted(ex.calls["checks"]) == ["k1", "k1", "k2"]  # one more try on the new deploy, and only one


async def test_chg_3_a_batch_launched_before_a_definition_change_does_not_commit(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    entered, release = hold(ex)
    run = await solera.submit("tally")
    await solera.until(entered.is_set)
    await ex.deploy(version="2")
    release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [attempts] = detail["attempts"].values()
    assert (await solera.head("tally"))["commit"] != f"{run['id']}/{attempts[0]['id']}"
    held, *after = ex.seen["tally"]
    assert after and after[-1].added == ["k1", "k2"]  # still owed, under version 2
    assert await ex.count() == 2 and await ex.stale() == []


async def test_chg_3_a_batch_launched_before_an_upstream_reset_does_not_commit(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.feed({"k3": "1"})
    entered, release = hold(ex)
    run = await solera.submit("tally")
    await solera.until(entered.is_set)
    assert ex.seen["tally"][-1].added == ["k3"]
    await ex.deploy(items_store="other")  # `items` is a new output
    await ex.feed({"k2": "2"}, removes=["k1"])  # its first write: k2, k3
    release.set()
    detail = await solera.wait(run)
    assert detail["request"]["status"] == "succeeded"
    [attempts] = detail["attempts"].values()
    assert (await solera.head("tally"))["commit"] != f"{run['id']}/{attempts[0]['id']}"
    last = ex.seen["tally"][-1]
    assert last.reset and last.added == ["k2", "k3"]  # the new `items`, read in full
    assert await ex.count() == 2 and await ex.stale() == []


async def test_chg_5_a_first_include_removes_what_it_leaves_out(solera):
    ex = await Example(solera).start()
    await ex.feed({"keep/a": "1", "drop/b": "1"})
    await ex.run("tally")
    await ex.run("copy")
    await ex.deploy(include=["keep/*"])
    assert await ex.stale() == ["input changed"]
    [batch] = await ex.run("tally")
    assert batch.classes == ([], [], ["drop/b"])
    await ex.run("copy")
    assert await ex.count() == 1 and await solera.rows("copy") == {"keep/a": "1"}
    assert await ex.stale() == []


async def test_chg_6_a_key_added_while_the_patterns_exclude_it_is_owed_nothing(solera):
    ex = await Example(solera).start(include=["keep/*", "drop/*"])
    await ex.feed({"keep/a": "1"})
    await ex.run("tally")
    await ex.deploy(include=["keep/*"])
    await ex.feed({"drop/b": "1"})
    assert await ex.stale() == []
    await solera.run("tally", expect=None)
    assert "drop/b" not in delivered(ex)
    assert await ex.count() == 1 and await ex.stale() == []


async def test_chg_7_widening_to_a_key_that_never_existed_owes_nothing(solera):
    ex = await Example(solera).start(include=["k1"])
    await ex.feed({"k1": "1"})
    await ex.run("tally")
    await ex.deploy(include=["k1", "k9"])
    assert await ex.stale() == []
    batches = len(ex.seen["tally"])
    await solera.run("tally", expect=None)
    assert delivered(ex, since=batches) == [] and await ex.count() == 1


@pytest.mark.parametrize("case", ["once", "repeated", "absent key", "partial"])
async def test_chg_8_a_keys_run_around_a_pattern_change_is_counted_once(solera, case):
    """Widen `k1` to `k*`, then `keys=` runs, then a default run: k2 is
    added once. `partial`: before the widening, a `keys=(k1)` run left k2
    owed under `[k1, k2]`."""

    ex = await Example(solera).start(include=["k1", "k2"] if case == "partial" else ["k1"])
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally", **({"keys": {"items": ["k1"]}} if case == "partial" else {}))
    await ex.deploy(include=["k*"])
    for _ in range(2 if case == "repeated" else 1):
        await solera.run("tally", keys={"items": ["k9" if case == "absent key" else "k2"]})
    await ex.run("tally")
    assert sorted(delivered(ex)) == ["k1", "k2", "k3"]
    assert await ex.count() == 3 and await ex.stale() == []


async def test_chg_8_a_keys_run_after_an_unprocessed_change_and_a_widening_is_counted_once(solera):
    ex = await Example(solera).start(include=["k1"])
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.feed({"k1": "2"})  # owed, not yet processed
    await ex.deploy(include=["k*"])
    await solera.run("tally", keys={"items": ["k2"]})
    await ex.run("tally")
    assert sorted(k for b in ex.seen["tally"] for k in b.added) == ["k1", "k2"]
    assert [k for b in ex.seen["tally"] for k in b.updated] == ["k1"]
    assert await ex.count() == 2 and await ex.stale() == []


async def test_chg_9_a_commit_of_keys_its_consumers_exclude_leaves_them_fresh(solera):
    ex = await Example(solera).start(include=["k*"])
    await ex.feed({"k1": "1", "k2": "1"})
    for name in ("tally", "copy", "checks"):
        await solera.run(name)
    await ex.feed({"x1": "1"})  # `items` commits x1 alone
    for name in ("tally", "copy", "checks"):
        assert await solera.stale(name) == [], name
    assert await solera.stale_keys("checks") == {}
    await ex.feed({"k1": "2"})
    assert await solera.stale("copy") == ["input changed"]
    assert list(await solera.stale_keys("checks")) == ["k1"]


async def test_chg_9_a_per_key_consumer_is_fresh_while_only_keys_it_excludes_are_stale_upstream(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await solera.run("checks")
    await solera.run("filtered")
    await ex.feed({"k2": "2"})
    assert list(await solera.stale_keys("checks")) == ["k2"]
    assert await solera.stale("filtered") == [] and await solera.stale_keys("filtered") == {}
    await ex.feed({"k1": "2"})
    assert await solera.stale("filtered") != []


async def test_chg_10_a_moved_whole_input_owes_every_key_processed_under_its_old_version(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await solera.run("checks")
    await ex.run("tally")
    await ex.factor("w2")
    for name in ("checks", "tally"):
        assert await solera.stale(name) == ["input changed"], name  # not "definition changed"
    owed = await solera.stale_keys("checks")
    assert sorted(owed) == ["k1", "k2"]
    assert {reason["kind"] for reasons in owed.values() for reason in reasons} == {"input changed"}

    await solera.run("checks", keys={"items": ["k1"]})
    assert list(await solera.stale_keys("checks")) == ["k2"]
    assert await solera.rows("checks", column="factor") == {"k1": "w2", "k2": "w1"}
    [batch] = await ex.run("tally", keys={"items": ["k1"]})
    assert batch.context == {"factor": "w2"}
    assert await ex.stale() == ["input changed"]  # k2, processed under w1

    await ex.factor("w1")
    assert list(await solera.stale_keys("checks")) == ["k1"]
    assert await ex.stale() == ["input changed"]  # k1 now
    await solera.run("checks")
    await ex.run("tally")
    assert await solera.rows("checks", column="factor") == {"k1": "w1", "k2": "w1"}
    assert await solera.stale("checks") == [] and await ex.stale() == []
    assert await ex.count() == 2


async def test_chg_11_a_whole_input_written_again_with_the_same_content_is_a_change(solera):
    calls = []

    @asset(outputs=Output("feed", key="id"))
    def feed():
        return [{"id": "a"}]

    @asset
    def settings():
        return {"rate": 2}

    @asset(inputs={"feed": Incremental(), "settings": "settings"})
    def reader(feed: list, settings: dict):
        calls.append(len(feed))
        return []

    await solera.deploy(
        Project(assets=[feed, settings, reader], default_store=FileStore(solera.root / "data"))
    )
    await solera.run("reader", upstream=True)
    commits = [(await solera.head("settings"))["commit"]]
    for _ in range(2):
        await solera.run("settings")  # the same content, written again
        commits.append((await solera.head("settings"))["commit"])
        assert await solera.read("settings") == {"rate": 2}
        assert await solera.stale("reader") == ["input changed"]
        await solera.run("reader")
    assert len(set(commits)) == 3
    assert calls == [1, 1, 1]  # each new version of `settings`: `feed` read again
    assert await solera.stale("reader") == []


# -- patterns: the SDK alone --------------------------------------------------------


def test_chg_12_globs_match_by_path_segments():
    m = Matcher(spec("ICP/Results/**/*.csv"))
    assert m("ICP/Results/a.csv") and m("ICP/Results/2026/09/a.csv")
    assert not m("ICP/Results/a.xlsx") and not m("XRF/ICP/Results/a.csv")
    assert Matcher(spec("*.csv"))("a.csv") and not Matcher(spec("*.csv"))("dir/a.csv")
    assert Matcher(spec("f?.csv"))("f1.csv") and not Matcher(spec("f?.csv"))("f/.csv")
    assert Matcher(spec("a+b(c).csv"))("a+b(c).csv")  # literal characters stay literal


def test_chg_12_one_exclude_pattern_is_a_pattern_not_its_characters():
    """A string `exclude=` is one pattern, as a string `include=` is: `'k1*'`
    leaves out `k1…` only, never every key through its `'*'`."""

    assert spec(exclude="k1*") == spec(exclude=["k1*"])
    taken = Matcher(spec(exclude="k1*"))
    assert taken("k2") and taken("a") and not taken("k10")
    assert Matcher(spec(include="k*", exclude=Regex("k1.*")))("k2")


def test_chg_12_a_word_rule_wants_a_segment_anchored_pattern():
    """`**/*old*` is characters, not a word: it takes `Gold_ore.csv`."""

    assert Matcher(spec("**/*old*"))("Gold_ore.csv") and Matcher(spec("**/*old*"))("an/old.csv")
    words = Matcher(spec(Regex(r"(.*/)?old[ _.].*")))
    assert words("an/old.csv") and not words("Gold_ore.csv")


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
def test_chg_12_a_glob_matches_as_the_reference_does(glob, key):
    assert Matcher(spec(glob))(key) == _glob(glob, key)


@settings(max_examples=200, deadline=None)
@given(
    include=st.one_of(st.none(), st.lists(_globs.filter(bool), min_size=1, max_size=3)),
    exclude=st.lists(_globs.filter(bool), max_size=3),
    key=_keys,
)
def test_chg_12_a_key_is_taken_when_an_include_and_no_exclude_matches(include, exclude, key):
    m = Matcher(spec(include, exclude))
    excluded = [n for n, g in enumerate(exclude) if _glob(g, key)]
    assert m.excluded_by(key) == (f"exclude[{excluded[0]}]" if excluded else None)
    assert m(key) == ((include is None or any(_glob(g, key) for g in include)) and not excluded)
