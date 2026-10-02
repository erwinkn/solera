"""Delivery progress (§5, §6; per-key §11): where an Incremental edge is in
delivering its upstream, and the one transition a delivered page makes.

An attempt is given a page of a plan; its commit `advance`s the edge's
watermark by what the page delivered. A plan is one of three kinds:

- `keys`: a keyed upstream, read through its index — a `full` delivery of
  the whole index (from batch `from` on), or a delta window `from`..`to`;
  paged by key, the page reporting `after`, the last key it delivered
  (`None`: the window is done). A pattern transition (`rescope`) finishes
  its old changes, then pages through a membership `diff`.
- `batches`: a batch-mode upstream, `lo`..`hi` of the batches up to `head`,
  `full` from the reset's `base`; a page always ends at its boundary.
- `held`: a page that moves no watermark of its own — an Each retry or
  reconcile page (its progress is the failure record's) or a `keys=`
  override. Its `watermark` is kept as it is.

Whatever the kind, a delivery's mode (`full`), boundary and page plan
(`page` of `pages`) are decided when it starts and kept on the watermark
until its last page.

Watermark: {"batch", "until"?, "after", "full", "fingerprint", "output",
"up", "pass"?, "page"?, "pages"?, "patterns"?, "rescope"?, "cleanup"?,
"reconcile"?, "pin"?}.
"""

from __future__ import annotations


def kind(plan: dict) -> str:
    """A plan's kind. One prepared before kinds were named — kept on a task
    launched across an upgrade — moved its watermark to `update`, or was keyed."""

    if "kind" in plan:
        return plan["kind"]
    return "held" if "update" in plan else "keys"


def advance(plan: dict, after: str | None = None) -> dict | None:
    """The watermark after a page of `plan` was delivered, ending at `after`
    (a keyed page; `None`: its window is done) — `None` if it moves none."""

    if kind(plan) == "held":
        return plan.get("watermark", plan.get("update"))
    if kind(plan) == "batches":
        more = plan["hi"] < plan["head"]
        wm = {**_base(plan), "batch": max(plan["lo"], plan["hi"] + 1), "after": None}
        wm["full"] = bool(plan["full"]) and more
        if more:
            wm.update(page=int(plan["page"]) + 1, pages=int(plan["pages"]))
        return wm
    wm = _keys(plan, after)
    if after is not None and plan.get("pages") is not None:
        wm.update(page=int(plan["page"]) + 1, pages=int(plan["pages"]))
    return wm


def continues(plan: dict, after: str | None, wm: dict | None) -> bool:
    """Whether the delivery has more for the same task to do after this page:
    a key window not done, a membership diff still owed, a cleanup begun."""

    if kind(plan) == "batches":
        return plan["hi"] < plan["head"]
    if kind(plan) == "held":
        return False
    return (
        after is not None
        or (plan.get("rescope") is not None and not plan.get("diff"))
        or "reconcile" in (wm or {})
    )


def _base(plan: dict) -> dict:
    base = {k: plan[k] for k in ("output", "up", "fingerprint")}
    if plan.get("pass") is not None:  # the run whose reset began this pass
        base["pass"] = plan["pass"]
    return base


def _keys(plan: dict, after: str | None) -> dict:
    """A keyed page's transition. A delta window delivered over several
    attempts keeps the reader pin of the attempt that began it: its later
    pages still read versions as of then (lifecycle.md §9.8)."""

    base = _base(plan)
    if plan.get("cleanup") and plan["full"]:  # a full Each delivery owes a cleanup (§11)
        base["cleanup"] = True
    if plan.get("patterns") is not None:  # what the edge delivers under (per-key §11)
        base["patterns"] = plan["patterns"]
    rescope = plan.get("rescope")
    if plan.get("diff"):  # a rescope's membership diff (per-key §11)
        if after is None:  # done: the new patterns from the cutover on
            done = {**base, "batch": plan["batch"], "after": None, "full": False}
            done.pop("patterns", None)
            return {**done, "patterns": rescope["to"]} if rescope["to"] is not None else done
        return {
            **base,
            "batch": plan["batch"],
            "after": None,
            "full": False,
            "rescope": {**rescope, "after": after},
        }
    if rescope is not None:
        base["rescope"] = rescope
    if plan["full"]:
        if after is None:
            done = {**base, "batch": plan["from"], "after": None, "full": False}
            done.pop("cleanup", None)
            if plan.get("each") is not None and plan.get("cleanup"):
                # An Each output may hold keys the delivery no longer names — gone
                # upstream, or left out by the patterns: reconcile them next (§11).
                done["reconcile"] = {"after": None}
            return done
        return {**base, "batch": plan["from"], "after": after, "full": True}
    if after is None:
        return {**base, "batch": max(plan["from"], plan["to"] + 1), "after": None, "full": False}
    paged = {**base, "batch": plan["from"], "until": plan["to"], "after": after, "full": False}
    return {**paged, "pin": plan["pin"]} if plan.get("pin") is not None else paged
