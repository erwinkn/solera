"""CHG: definition, pattern and context changes (docs/behaviors.md)."""

import asyncio
import os
import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from solera import Incremental, Regex
from solera.patterns import Matcher, spec
from solera.sdk import In, Loaded, Output, Project, Source, asset, source
from solera.stores import FileStore

from tests.contract.example import Example
from tests.contract.harness import Outside, SourceStore, rebuild
from tests.contract.test_aut import automated, built_patterned, patterned


def hold(ex: Example) -> tuple[asyncio.Event, asyncio.Event]:
    """`tally`'s next batch waits, once it has read its inputs, until
    released: (entered, release)."""

    entered, release = asyncio.Event(), asyncio.Event()

    async def held():
        entered.set()
        await release.wait()

    ex.during = held
    return entered, release


async def obsolete_committed_nothing(solera, detail: dict) -> None:
    """`tally`'s run's first attempt, the one held across the change, made
    no commit: every commit of the run is a later attempt's."""

    held, *later = next(iter(detail["attempts"].values()))
    assert "commit" not in held and later
    commits = await solera.commits("tally", detail["request"]["id"])
    assert commits and held["id"] not in {c["attempt"] for c in commits}


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
    await obsolete_committed_nothing(solera, detail)
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
    await obsolete_committed_nothing(solera, detail)
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


# -- target (PR 2) --


# -- CHG-1 (target) ---------------------------------------------------------------------


async def test_chg_1_a_version_bump_makes_it_stale_and_its_next_run_a_full_run(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally")
    await ex.deploy(batch_size=2, version="2")
    assert await ex.stale() == ["definition changed"]
    batches = await ex.run("tally")
    assert [(b.reset, b.added) for b in batches] == [(True, ["k1", "k2"]), (False, ["k3"])]
    assert await ex.count() == 3 and await ex.stale() == []


async def test_chg_1_a_runs_config_is_part_of_the_definition(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    [batch] = await ex.run("tally", config={"rate": 2})
    assert batch.reset and batch.added == ["k1", "k2"]
    assert await ex.count() == 2


def stored(root, store, calls: list) -> Project:
    """`files` (keyed), and `copy`, incremental over it, writing to `store`."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": "a", "v": "1"}, {"id": "b", "v": "1"}]

    @asset(inputs={"files": Incremental()}, outputs=Output("copy", key="id", store="s"))
    def copy(ctx, files: list):
        calls.append((ctx.batch["files"].reset, sorted(r["id"] for r in files)))
        return rebuild(ctx.batch["files"], files)

    return Project(assets=[files, copy], stores={"s": store}, default_store=FileStore(root / "data"))


async def test_chg_1_a_stores_version_is_part_of_the_definition(solera):
    """The store `copy` writes to says its data changed (a new version):
    `copy` is stale, and its next run starts over."""

    calls, store = [], FileStore(solera.root / "s")
    await solera.deploy(stored(solera.root, store, calls))
    await solera.run("copy", upstream=True)
    store.version = "next"
    await solera.deploy(stored(solera.root, store, calls))
    assert await solera.stale("copy") == ["definition changed"]
    await solera.run("copy", upstream=True)
    assert calls[-1] == (True, ["a", "b"])


@pytest.mark.postgres
async def test_chg_1_a_stores_config_is_not_part_of_the_definition(solera, monkeypatch):
    """The DSN of the Postgres store holding `files` moves to another
    variable naming the same database (a rotated password, a new host for
    the same data): nothing is owed, and nothing runs again."""

    dsn = os.environ.get("SOLERA_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    import psycopg
    from solera_postgres import PostgresStore

    schema, calls = f"contract_{uuid.uuid4().hex[:10]}", []
    monkeypatch.setenv("CONTRACT_ROTATED_DSN", dsn)

    def project(variable):
        @asset(
            outputs=Output("files", key="id", store="pg", schema=schema, columns={"id": "text", "v": "text"})
        )
        def files():
            return [{"id": "a", "v": "1"}]

        @asset(
            inputs={"files": Incremental()},
            outputs=Output("copy", key="id", store="pg", schema=schema, columns={"id": "text", "v": "text"}),
        )
        def copy(ctx, files: list):
            calls.append(sorted(r["id"] for r in files))
            return rebuild(ctx.batch["files"], files)

        return Project(assets=[files, copy], stores={"pg": PostgresStore(f"env:{variable}")})

    try:
        await solera.deploy(project("SOLERA_TEST_DATABASE_URL"))
        await solera.run("copy", upstream=True)
        await solera.deploy(project("CONTRACT_ROTATED_DSN"))
        for name in ("files", "copy"):
            assert await solera.stale(name) == [], name
        await solera.run("copy")
        assert calls == [["a"]] and await solera.rows("copy") == {"a": "1"}
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# -- CHG-4 (target) ---------------------------------------------------------------------


async def test_chg_4_widening_patterns_owes_the_keys_that_join_and_nothing_else(solera):
    ex = await Example(solera).start(include=["k1"])
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally")
    await ex.deploy(include=["k*"])
    assert await ex.stale() == ["input changed"]
    [batch] = await ex.run("tally")
    assert (batch.classes, batch.reset) == ((["k2", "k3"], [], []), False)
    assert await ex.count() == 3 and await ex.stale() == []


async def test_chg_4_narrowing_patterns_owes_the_removal_of_every_held_key(solera):
    ex = await Example(solera).start(include=["k*"])
    await ex.feed({"k1": "1", "k2": "1", "k3": "1"})
    await ex.run("tally")
    await ex.deploy(include=["z*"])
    assert await ex.stale() == ["input changed"]
    [batch] = await ex.run("tally")
    assert (batch.classes, batch.reset) == (([], [], ["k1", "k2", "k3"]), False)
    assert await ex.count() == 0 and await ex.stale() == []


async def test_chg_4_a_pattern_change_that_owes_something_fires_onchange(solera):
    """`tally`, automated by `OnChange`, takes k1, then k*: its automation
    fires once and adds k2 and k3; narrowed to z*, it fires again and
    removes all three."""

    calls = []
    await built_patterned(solera, calls, include=["k1"])
    for include, call in ((["k*"], (["k2", "k3"], [], False)), (["z*"], ([], ["k1", "k2", "k3"], False))):
        fired = len(await automated(solera))
        await solera.deploy(patterned(solera.root, calls, include=include))
        await solera.until(lambda call=call: calls[-1] == call)
        await solera.quiet()
        assert len(await automated(solera)) == fired + 1
        assert await solera.stale("tally") == []


# -- CHG-13 (target) --------------------------------------------------------------------


async def test_chg_13_a_whole_input_moving_mid_run_owes_the_keys_processed_before_it_moved(solera):
    """`factor` moves from w1 to w2 while `checks` is in its first batch
    (k1): its second batch (k2) reads w2, and after the run k1 alone is
    owed again, for its input."""

    outside, seen, steps = Outside(), [], []

    @source
    async def factor(keys, ctx):
        return Loaded(outside.knob, version=outside.knob)

    @asset(
        inputs={"row": Incremental("feed", each=True, batch_size=1, concurrency=1), "factor": In()},
        outputs=Output("checks", key="id"),
    )
    async def checks(ctx, row: list, factor: str):
        seen.append((ctx.key, factor))
        if steps:
            await steps.pop(0)()
        return [{"v": row[0]["v"], "factor": factor}]

    await solera.deploy(
        Project(
            assets=[checks],
            sources=[Source("feed", key="id", store="ext"), factor],
            stores={"ext": SourceStore(solera.root / "ext", outside)},
            default_store=FileStore(solera.root / "data"),
        )
    )

    async def move():
        outside.knob = "w2"
        await solera.commit_source("factor", version="w2")

    await solera.commit_source("factor", version="w1")
    outside.feed.update(k1="1", k2="1")
    await solera.commit_source("feed", upsert={"k1": "1", "k2": "1"})
    steps.append(move)
    await solera.run("checks")
    assert seen == [("k1", "w1"), ("k2", "w2")]
    assert await solera.rows("checks", column="factor") == {"k1": "w1", "k2": "w2"}
    owed = await solera.stale_keys("checks")
    assert list(owed) == ["k1"] and [r["kind"] for r in owed["k1"]] == ["input changed"]
    await solera.run("checks")
    assert seen[-1] == ("k1", "w2") and await solera.stale("checks") == []
