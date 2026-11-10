"""HIS: history and lineage (docs/behaviors.md)."""

from solera.sdk import In, Incremental, Output, Project, Result, Retry, Source, asset

from tests.contract.example import Example
from tests.contract.test_key import Checks


@asset(tags={"team": "growth"})
def orders(ctx):
    n = ctx.config.get("n", 3)
    ctx.mark("counted")
    ctx.metadata(rows=n, source="shop")
    return [{"id": i} for i in range(n)]


@asset(inputs={"orders": In()}, retries=Retry(n=0))
def revenue(orders: list, ctx):
    if ctx.config.get("fail"):
        raise RuntimeError("upstream timeout")
    return Result({"revenue": len(orders)}, metadata={"revenue": {"total": len(orders) * 10}})


@asset(outputs=Output("ranked", key="id"))
def ranked():
    return [{"id": "a"}, {"id": "b"}]


SHOP = Project(assets=[orders, revenue, ranked], sources=[Source("uploads", key="id")])


async def run(solera, targets, **fields) -> dict:
    """A run a minute after the last: its request, as it ended."""

    solera.advance(60)
    return (await solera.run(targets, expect=None, **fields))["request"]


async def commits(solera, name: str, **params) -> list[dict]:
    return (await solera.get(f"/assets/{name}/history", **params))["commits"]


# -- HIS-1 ------------------------------------------------------------------------


async def test_his_1_versions_carry_metadata_and_lineage(solera):
    await solera.deploy(SHOP)
    await run(solera, "revenue", upstream=True, config={"n": 4})
    await run(solera, "ranked")
    [made] = await commits(solera, "orders")
    assert made["metadata"] == {"rows": 4, "source": "shop"} and made["rows"] == 4
    [made] = await commits(solera, "revenue")
    assert made["metadata"] == {"total": 40}
    [made] = await commits(solera, "ranked")
    assert made["key_count"] == 2 and made["rows"] == 2  # its live keys; the rows it wrote

    for n in (5, 6):
        await run(solera, "orders", config={"n": n})
    made, paged, cursor = await commits(solera, "orders"), [], None
    while True:  # one version at a time walks them all
        page = await solera.get("/assets/orders/history", limit=1, **({"before": cursor} if cursor else {}))
        paged += page["commits"]
        if (cursor := page["next"]) is None:
            break
    assert paged == made and [c["rows"] for c in made] == [6, 5, 4]

    built_from = (await solera.head("orders"))["ref"]["generation"]
    await run(solera, "revenue")
    up = await solera.get("/outputs/revenue/lineage")
    assert [
        (e["from"]["output"], e["from"]["generation"], e["to"]["output"], e["param"]) for e in up["edges"]
    ] == [("orders", built_from, "revenue", "orders")]
    assert all(n["current"] for n in up["nodes"])
    down = await solera.get("/outputs/orders/lineage", direction="downstream")
    assert {n["output"] for n in down["nodes"]} == {"orders", "revenue"}


async def test_his_1_runs_filter_facets_and_pages(solera):
    await solera.deploy(SHOP)
    ok = await run(solera, "orders", tags={"env": "prod"})
    bad = await run(solera, "revenue", config={"fail": True}, tags={"env": "dev"})
    assert bad["status"] == "failed"
    third = await run(solera, "orders", tags={"env": "prod", "who": "ci"})

    async def ids(**params):
        return [r["id"] for r in await solera.runs(**params)]

    assert await ids() == [third["id"], bad["id"], ok["id"]]
    assert await ids(status="failed") == [bad["id"]]
    assert await ids(tag="env=prod") == [third["id"], ok["id"]]
    assert await ids(tag=["env=prod", "who"]) == [third["id"]]
    assert await ids(q="timeout") == [bad["id"]]
    assert await ids(asset_tag="team=growth") == [third["id"], ok["id"]]
    assert await ids(since=ok["created_at"] + 1) == [third["id"], bad["id"]]

    page = await solera.get("/runs", limit=2)
    assert [r["id"] for r in page["runs"]] == [third["id"], bad["id"]] and page["next"] == bad["id"]
    rest = await solera.get("/runs", before=page["next"], limit=2)
    assert [r["id"] for r in rest["runs"]] == [ok["id"]] and rest["next"] is None
    assert page["total"] == 3 and rest["total"] == 1
    row = page["runs"][1]
    assert row["error"] and "upstream timeout" in row["error"] and row["failed_count"] == 1

    facets = await solera.get("/runs:facets", status="failed")
    # A facet ignores its own field: every status is still counted.
    assert {f["value"]: f["count"] for f in facets["status"]} == {"succeeded": 2, "failed": 1}
    assert {f["value"]: f["count"] for f in facets["asset"]} == {"revenue": 1}
    assert {f["value"]: f["count"] for f in facets["tag"]} == {"env=dev": 1}

    histogram = await solera.get("/runs:histogram", since=ok["created_at"] - 1)
    assert sum(sum(b["counts"].values()) for b in histogram["bars"]) == 3

    # Numbered pages pin the newest run of the first page: a run arriving
    # meanwhile doesn't shift the second page.
    await run(solera, "orders")
    second = await solera.get("/runs", anchor=third["id"], offset=2, limit=2)
    assert [r["id"] for r in second["runs"]] == [ok["id"]] and second["total"] == 3


async def test_his_1_a_run_has_one_timeline_and_says_where_each_attempt_ran(solera):
    await solera.deploy(SHOP)
    detail = await solera.run("revenue", upstream=True)
    run_id = detail["request"]["id"]
    events = await solera.get(f"/runs/{run_id}/events")
    assert [e["n"] for e in events] == list(range(1, len(events) + 1))
    assert all(e["run"] == run_id for e in events)
    ran = [a for found in detail["attempts"].values() for a in found]
    assert {e["task"] for e in events} - {None} == {t["id"] for t in detail["tasks"]}
    assert {e["attempt"] for e in events} - {None} == {a["id"] for a in ran}
    assert [e["at"] for e in events] == sorted(e["at"] for e in events)
    assert {a["executor"] for a in ran} == {"local"}
    tasks = (await solera.get("/tasks", run=run_id))["tasks"]
    assert sorted(t["asset"] for t in tasks) == ["orders", "revenue"]
    assert {t["executor"] for t in tasks} == {"local"}


async def test_his_1_a_run_reads_the_same_once_archived(solera):
    await solera.deploy(SHOP)
    failed = await run(solera, "revenue", upstream=True, config={"fail": True}, tags={"env": "prod"})
    assert failed["status"] == "failed"
    await solera.commit_source("uploads", upsert=["a", "b"])

    async def history():
        return (
            await solera.get(f"/runs/{failed['id']}"),
            await solera.get(f"/runs/{failed['id']}/events"),
            await solera.get("/tasks", run=failed["id"]),
            await solera.get("/runs"),
            await solera.get("/assets/orders/history"),
        )

    ended = await history()
    await solera.settle(0.3)
    assert await history() == ended
    await solera.restart()
    assert await history() == ended
    [commit, _] = ended[3]["runs"]
    assert (commit["origin"], commit["source"], commit["status"]) == ("commit", "uploads", "succeeded")


# -- HIS-2 ------------------------------------------------------------------------


async def test_his_2_a_task_waits_only_while_it_could_run(solera):
    """A task's wait leaves out its run being paused, and the engine being down."""

    await solera.deploy(SHOP)
    submitted = await solera.submit("orders")
    solera.advance(10)
    await solera.post(f"/runs/{submitted['id']}/pause")
    solera.advance(20)
    await solera.post(f"/runs/{submitted['id']}/resume")
    solera.advance(10)
    await solera.outage(60)
    solera.advance(10)
    detail = await solera.wait(submitted)
    assert detail["request"]["status"] == "succeeded"
    [task] = detail["tasks"]
    assert 30 <= task["wait"] < 31
    [row] = (await solera.get("/tasks", run=submitted["id"]))["tasks"]
    assert 30 <= row["wait"] < 31


# -- HIS-4 ------------------------------------------------------------------------


async def test_his_4_a_commit_row_names_the_batch_that_made_it(solera):
    """`batch` on a history commit row is `{index, count}` of the attempt
    that committed it, with `final` on the last: null for an unbatched task."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": 1} for k in "abc"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    def copy(ctx, files: list):
        return [{"id": r["id"], "v": r["v"]} for r in files]

    await solera.deploy(Project(assets=[files, copy]))
    await solera.run("copy", upstream=True)
    copies = sorted(await commits(solera, "copy"), key=lambda r: r["generation"])
    assert [r["batch"] for r in copies] == [{"index": i, "count": 3} for i in range(3)]
    assert [r["final"] for r in copies] == [False, False, True]
    assert [r["batch"] for r in await commits(solera, "files")] == [None]


# -- target (PR 2) ---------------------------------------------------------------

# -- HIS-3 (target) ----------------------------------------------------------------


async def test_his_3_lineage_records_the_version_each_input_was_read_at(solera):
    """`tally` reads `items` (FileStore) and `factor`, a source the outside
    moved to w2 before any commit said so: its lineage names the version of
    `items` it planned and the version of `factor` it was served, and no edge
    for what `ctx.load()` read."""

    ex = await Example(solera).start()
    await ex.feed({"k1": "1"})
    await ex.run("tally")
    ex.outside.knob = "w2"
    await ex.feed({"k1": "2"})
    [batch] = await ex.run("tally")
    assert batch.context == {"factor": "w2"}
    made = (await solera.head("tally"))["ref"]["generation"]
    lineage = await solera.get("/outputs/tally/lineage", depth=1)
    edges = {
        e["param"]: e["from"]
        for e in lineage["edges"]
        if e["to"] == {"output": "tally", "partition": "", "generation": made}
    }
    assert sorted(edges) == ["factor", "items"]  # ctx.load is not lineage
    assert edges["items"]["generation"] == (await solera.head("items"))["ref"]["generation"]
    factor = await solera.head("factor")
    assert (factor["version"], edges["factor"]["generation"]) == ("w2", factor["ref"]["generation"])


# -- HIS-5 (target) ----------------------------------------------------------------


async def test_his_5_a_removed_key_outcome_stays_known_while_its_run_is_in_history(solera):
    """`checks` delivers k2's removal in run R; `feed` moves on and upkeep
    merges its history: while R is retained, k2's outcome is still `removed`."""

    c = await Checks(solera).deploy()
    await c.feed(k1="1", k2="1")
    await c.run()
    await c.feed(removes=["k2"])
    await c.run()
    assert (await solera.outcomes("checks"))["k2"]["outcome"] == "removed"
    for v in ("2", "3"):
        await c.feed(k1=v)
        await c.run()
        await solera.upkeep()
    [k2] = (await solera.get("/assets/checks/outcomes", key="k2"))["keys"]
    assert k2["outcome"] == "removed"
    assert (await solera.outcomes("checks"))["k2"]["outcome"] == "removed"


# -- HIS-6 (target) ----------------------------------------------------------------


async def test_his_6_the_per_key_log_keeps_failures_newest_first_not_ok_calls_and_expires_with_its_run(
    solera,
):
    """Twelve keys succeed and `bad` fails, in two runs (a new deploy retries
    it): the log holds bad's two failures, newest first, with their errors;
    no ok call. Deleting the first run takes its failure with it."""

    c = await Checks(solera).deploy()
    c.failing["bad"] = RuntimeError("unexpected header")
    await c.feed(bad="1", **{f"k{i:02}": "1" for i in range(12)})
    await c.run()
    await c.deploy(build="2")
    await c.feed(**{f"k{i:02}": "2" for i in range(12)})
    await c.run()
    first, second = [r["id"] for r in reversed(await solera.runs(asset="checks"))]
    log = await solera.key_log("checks")
    assert [(r["run"], r["key"], r["outcome"]) for r in log] == [
        (second, "bad", "failed"),
        (first, "bad", "failed"),
    ]
    assert all("unexpected header" in r["error"] for r in log)
    assert await solera.key_log("checks", outcome="ok") == []
    await solera.request("DELETE", f"/runs/{first}")
    assert [r["run"] for r in await solera.key_log("checks")] == [second]
