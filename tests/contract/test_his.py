"""HIS: history and lineage (docs/behaviors.md)."""

from solera.sdk import In, Incremental, Output, Project, Result, Retry, Source, asset


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
    that committed it, with `final` on the last: null for an unbatched task.
    `count` is the batches planned, an estimate (INC-10): the row carries the
    one its attempt reported, whatever the run realised."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return [{"id": k, "v": 1} for k in "abc"]

    @asset(inputs={"files": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    def copy(ctx, files: list):
        return [{"id": r["id"], "v": r["v"]} for r in files]

    await solera.deploy(Project(assets=[files, copy]))
    detail = await solera.run("copy", upstream=True)
    [task] = [t for t in detail["tasks"] if t["asset"] == "copy"]
    reported = {a["id"]: a["batch"] for a in detail["attempts"][task["id"]]}
    copies = sorted(await commits(solera, "copy"), key=lambda r: r["generation"])
    assert [r["batch"]["index"] for r in copies] == [0, 1, 2]
    for row in copies:  # the estimate the attempt reported, carried as it was
        made = reported[row["attempt"]]
        assert isinstance(row["batch"]["count"], int) and row["batch"]["count"] >= 1
        assert row["batch"] == {"index": made["index"], "count": made["count"]}
    assert [r["final"] for r in copies] == [False, False, True]
    assert [r["batch"] for r in await commits(solera, "files")] == [None]
