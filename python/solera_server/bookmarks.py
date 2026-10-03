"""Pass progress (§5, §6; per-key §11): where an Incremental edge is in
delivering its upstream, and the one transition a delivered page makes.

Each (asset, edge, partition) keeps a **bookmark**:

    {
      "kind": "keys" | "batches",   # a keyed upstream, read by key; or batch by batch
      "output", "upstream_partition", # the upstream output and partition it reads
      "fingerprint",                # the interpretation it was delivered under (§6)
      "reset_by": run id,           # the run whose reset began the current pass
      "next": int,                  # the first upstream batch not yet delivered
      "pass": {                 # a pass under way, paged over attempts
        "mode": "full" | "delta" | "diff",
        "from": int, "to": int,     # its boundary, decided when it starts
        "at": key | batch | None,   # its position: the last key delivered, the next batch
        "page": int, "pages": int,  # where the next page sits in its plan
        "pin": int,                 # a delta pass's reader pin (lifecycle.md §9.8)
        "reconcile": bool,          # a full Each pass owes a reconcile after (§11)
      },
      "patterns": ...,              # keys: the patterns it delivers under (per-key §11)
      "pattern change": {"old", "new", "at", "snapshot", "pin"},  # a pattern change
      "reconcile": {"after": key},  # an Each output's cleanup after a full pass
    }

The modes:

- `full`: everything — a keyed upstream's whole index, which resumes as
  deltas from `from` (the head's next batch when it started); a batch
  upstream's batches from its reset's `base` to `to`.
- `delta`: the batches `from`..`to` past the last pass.
- `diff`: a pattern change's membership diff over the index as of its
  pattern change (keys).

A pass's mode, boundary and page plan are decided when it starts and
kept until its last page; `next` then moves past it. An attempt is given a
**plan** — `kind`, the `bookmark` it carries forward, and the `pass`
its page is on (`hi`, for batches: the page's last batch); a `held` plan,
a page that moves no bookmark of its own (an Each retry or reconcile
page); or a `selection`, a run's `keys=` selection, which reads the keys it
names and moves neither the bookmark nor the partition's progress, whatever
they are.
"""

from __future__ import annotations


def advance(plan: dict, after: str | None = None) -> dict | None:
    """The bookmark after a page of `plan` was delivered — `after`, a key
    page's last key (`None`: the pass is done) — or `None` if it moves
    none."""

    if plan["kind"] == "selection":
        return None
    if plan["kind"] == "held":
        return plan["bookmark"]
    wm, d = dict(plan["bookmark"]), plan["pass"]
    if plan["kind"] == "commits":
        if plan["hi"] < d["to"]:
            wm["pass"] = {**d, "at": plan["hi"] + 1, "page": d["page"] + 1}
        else:
            wm.pop("pass", None)
            wm["next"] = max(int(d["at"]), d["to"] + 1)
        return wm
    if after is not None:  # the pass continues from the next key
        wm["pass"] = {**d, "at": after, "page": d["page"] + 1}
        return wm
    wm.pop("pass", None)
    if d["mode"] == "diff":  # the transition is done: the new patterns from the pattern change on
        pattern_change = wm.pop("pattern_change")
        wm.pop("patterns", None)
        if pattern_change["new"] is not None:
            wm["patterns"] = pattern_change["new"]
    elif d["mode"] == "full":
        wm["next"] = d["from"]
        if d.get("reconcile"):
            # An Each output may hold keys the pass no longer names — gone
            # upstream, or left out by the patterns: reconcile them next (§11).
            wm["reconcile"] = {"after": None}
    else:
        wm["next"] = max(d["from"], d["to"] + 1)
    return wm


def continues(plan: dict, after: str | None, wm: dict | None) -> bool:
    """Whether the task has more to deliver after this page: a pass not
    done, a pattern change's diff still owed, a cleanup begun — or a
    pass done behind the upstream `head` the page was planned against.
    A pass's boundary is fixed when it starts, so one resumed after the
    upstream moved (a full pass interrupted, then a change it was fired
    for) ends short of that change: the task goes on to it, as a delta."""

    if plan["kind"] in ("held", "selection"):
        return False
    # Behind: known only once the page's bookmark is (`wm`, after `advance`).
    behind = wm is not None and "pass" not in wm and int(wm["next"]) <= int(plan.get("head", -1))
    wm = wm or {}
    if plan["kind"] == "commits":
        return plan["hi"] < plan["pass"]["to"] or behind
    return after is not None or "pattern_change" in wm or "reconcile" in wm or behind


def selects(plans: dict) -> bool:
    """Whether an attempt reads a `keys=` selection: it then leaves the
    partition's progress as it was."""

    return any(p and p["kind"] == "selection" for p in plans.values())


def outstanding(wm: dict) -> bool:
    """Whether an edge still owes its partition pass: one under way, a
    pattern change's diff, an Each cleanup."""

    return "pass" in wm or "pattern_change" in wm or "reconcile" in wm


def needs(wm: dict) -> int:
    """The first batch of the upstream's delta log the edge still reads."""

    d = wm.get("pass")
    return int(d["from"]) if d is not None and d.get("from") is not None else int(wm["next"])


def pins(wm: dict) -> list[int]:
    """The reader pins an edge holds: a delta pass paged over attempts
    reads versions as of its first page; a pattern change, its pattern change's
    index (lifecycle.md §9.8)."""

    out = []
    if (wm.get("pass") or {}).get("pin") is not None:
        out.append(wm["pass"]["pin"])
    if wm.get("pattern_change") is not None:
        out.append(wm["pattern_change"]["pin"])
    return out


def reads(plans: dict) -> list[tuple]:
    """The delta logs an attempt's plans read: `(output, partition, first batch)`
    — kept until its claim goes (§6)."""

    return [
        (p["bookmark"]["output"], p["bookmark"]["upstream_partition"], p["pass"]["from"])
        for p in plans.values()
        if p and p["kind"] == "keys" and p["pass"].get("from") is not None
    ]
