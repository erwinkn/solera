"""Delivery progress (§5, §6; per-key §11): where an Incremental edge is in
delivering its upstream, and the one transition a delivered page makes.

Each (asset, edge, scope) keeps a **watermark**:

    {
      "kind": "keys" | "batches",   # a keyed upstream, read by key; or batch by batch
      "output", "up",               # the upstream output and scope it reads
      "fingerprint",                # the interpretation it was delivered under (§6)
      "pass": run id,               # the run whose reset began the current pass
      "next": int,                  # the first upstream batch not yet delivered
      "delivery": {                 # a delivery under way, paged over attempts
        "mode": "full" | "delta" | "diff",
        "from": int, "to": int,     # its boundary, decided when it starts
        "at": key | batch | None,   # its position: the last key delivered, the next batch
        "page": int, "pages": int,  # where the next page sits in its plan
        "pin": int,                 # a delta window's reader pin (lifecycle.md §9.8)
        "cleanup": bool,            # a full Each delivery owes a reconcile after (§11)
      },
      "patterns": ...,              # keys: the patterns it delivers under (per-key §11)
      "rescope": {"old", "new", "cutover", "snapshot", "pin"},  # a pattern transition
      "reconcile": {"after": key},  # an Each output's cleanup after a full delivery
    }

The modes:

- `full`: everything — a keyed upstream's whole index, which resumes as
  deltas from `from` (the head's next batch when it started); a batch
  upstream's batches from its reset's `base` to `to`.
- `delta`: the batches `from`..`to` past the last delivery.
- `diff`: a pattern transition's membership diff over the index as of its
  cutover (keys).

A delivery's mode, boundary and page plan are decided when it starts and
kept until its last page; `next` then moves past it. An attempt is given a
**plan** — `kind`, the `watermark` it carries forward, and the `delivery`
its page is on (`hi`, for batches: the page's last batch); a `held` plan,
a page that moves no watermark of its own (an Each retry or reconcile
page); or a `selection`, a run's `keys=` selection, which reads the keys it
names and moves neither the watermark nor the scope's progress, whatever
they are.
"""

from __future__ import annotations


def advance(plan: dict, after: str | None = None) -> dict | None:
    """The watermark after a page of `plan` was delivered — `after`, a key
    page's last key (`None`: the delivery is done) — or `None` if it moves
    none."""

    if plan["kind"] == "selection":
        return None
    if plan["kind"] == "held":
        return plan["watermark"]
    wm, d = dict(plan["watermark"]), plan["delivery"]
    if plan["kind"] == "batches":
        if plan["hi"] < d["to"]:
            wm["delivery"] = {**d, "at": plan["hi"] + 1, "page": d["page"] + 1}
        else:
            wm.pop("delivery", None)
            wm["next"] = max(int(d["at"]), d["to"] + 1)
        return wm
    if after is not None:  # the delivery continues from the next key
        wm["delivery"] = {**d, "at": after, "page": d["page"] + 1}
        return wm
    wm.pop("delivery", None)
    if d["mode"] == "diff":  # the transition is done: the new patterns from the cutover on
        rescope = wm.pop("rescope")
        wm.pop("patterns", None)
        if rescope["new"] is not None:
            wm["patterns"] = rescope["new"]
    elif d["mode"] == "full":
        wm["next"] = d["from"]
        if d.get("cleanup"):
            # An Each output may hold keys the delivery no longer names — gone
            # upstream, or left out by the patterns: reconcile them next (§11).
            wm["reconcile"] = {"after": None}
    else:
        wm["next"] = max(d["from"], d["to"] + 1)
    return wm


def continues(plan: dict, after: str | None, wm: dict | None) -> bool:
    """Whether the task has more to deliver after this page: a delivery not
    done, a pattern transition's diff still owed, a cleanup begun — or a
    delivery done behind the upstream `head` the page was planned against.
    A delivery's boundary is fixed when it starts, so one resumed after the
    upstream moved (a full delivery interrupted, then a change it was fired
    for) ends short of that change: the task goes on to it, as a delta."""

    if plan["kind"] in ("held", "selection"):
        return False
    # Behind: known only once the page's watermark is (`wm`, after `advance`).
    behind = wm is not None and "delivery" not in wm and int(wm["next"]) <= int(plan.get("head", -1))
    wm = wm or {}
    if plan["kind"] == "batches":
        return plan["hi"] < plan["delivery"]["to"] or behind
    return after is not None or "rescope" in wm or "reconcile" in wm or behind


def selects(plans: dict) -> bool:
    """Whether an attempt reads a `keys=` selection: it then leaves the
    scope's progress as it was."""

    return any(p and p["kind"] == "selection" for p in plans.values())


def outstanding(wm: dict) -> bool:
    """Whether an edge still owes its scope delivery: one under way, a
    pattern transition's diff, an Each cleanup."""

    return "delivery" in wm or "rescope" in wm or "reconcile" in wm


def needs(wm: dict) -> int:
    """The first batch of the upstream's delta log the edge still reads."""

    d = wm.get("delivery")
    return int(d["from"]) if d is not None and d.get("from") is not None else int(wm["next"])


def pins(wm: dict) -> list[int]:
    """The reader pins an edge holds: a delta window paged over attempts
    reads versions as of its first page; a pattern transition, its cutover's
    index (lifecycle.md §9.8)."""

    out = []
    if (wm.get("delivery") or {}).get("pin") is not None:
        out.append(wm["delivery"]["pin"])
    if wm.get("rescope") is not None:
        out.append(wm["rescope"]["pin"])
    return out


def reads(plans: dict) -> list[tuple]:
    """The delta logs an attempt's plans read: `(output, scope, first batch)`
    — kept until its claim goes (§6)."""

    return [
        (p["watermark"]["output"], p["watermark"]["up"], p["delivery"]["from"])
        for p in plans.values()
        if p and p["kind"] == "keys" and p["delivery"].get("from") is not None
    ]
