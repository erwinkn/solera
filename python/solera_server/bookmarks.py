"""Pass progress (§5, §6; per-key §11): where an Incremental input is in
delivering its upstream, and the one transition a delivered batch makes.

Each (asset, input, partition) keeps a **bookmark**:

    {
      "kind": "keys" | "commits",   # a keyed upstream, read by key; or commit by commit
      "output", "upstream_partition", # the upstream output and partition it reads
      "fingerprint",                # the declaration it was delivered under (§6)
      "reset_by": run id,           # the run whose reset began the current pass
      "next": int,                  # the first upstream commit not yet delivered
      "pass": {                     # a pass under way, in batches over attempts
        "mode": "full" | "delta" | "diff",
        "from": int, "to": int,     # its boundary, decided when it starts
        "at": key | commit | None,  # its position: the last key delivered, the next commit
        "batch": int, "batches": int,  # the next batch's index, and how many are planned
        "pin": int,                 # a delta pass's reader pin (lifecycle.md §9.8)
        "reconcile": bool,          # a full Each pass owes a reconcile after (§11)
      },
      "patterns": ...,              # keys: the patterns it delivers under (per-key §11)
      "pattern_change": {"old", "new", "at", "snapshot", "pin"},
      "reconcile": {"after": key},  # an Each output's cleanup after a full pass
    }

The modes:

- `full`: everything — a keyed upstream's whole index, which resumes as
  deltas from `from` (the head's next commit when it started); an unkeyed
  upstream's commits from its reset's `base` to `to`.
- `delta`: the commits `from`..`to` past the last pass.
- `diff`: a pattern change's membership diff over the index as of its
  pattern change (keys).

A pass's mode, boundary and batch plan are decided when it starts and
kept until its last batch; `next` then moves past it. An attempt is given a
**plan** — `kind`, the `bookmark` it carries forward, and the `pass`
its batch is on (`hi`, for commits: the batch's last commit); a `held` plan,
a batch that moves no bookmark of its own (an Each retry or reconcile
batch); or a `selection`, a run's `keys=` selection, which reads the keys it
names and moves neither the bookmark nor the partition's progress, whatever
they are.
"""

from __future__ import annotations


def advance(plan: dict, after: str | None = None) -> dict | None:
    """The bookmark after a batch of `plan` was delivered — `after`, a key
    batch's last key (`None`: the pass is done) — or `None` if it moves
    none."""

    if plan["kind"] == "selection":
        return None
    if plan["kind"] == "held":
        return plan["bookmark"]
    wm, d = dict(plan["bookmark"]), plan["pass"]
    if plan["kind"] == "commits":
        if plan["hi"] < d["to"]:
            wm["pass"] = {**d, "at": plan["hi"] + 1, "batch": d["batch"] + 1}
        else:
            wm.pop("pass", None)
            wm["next"] = max(int(d["at"]), d["to"] + 1)
        return wm
    if after is not None:  # the pass continues from the next key
        wm["pass"] = {**d, "at": after, "batch": d["batch"] + 1}
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
    """Whether the task has more to deliver after this batch: a pass not
    done, a pattern change's diff still owed, a cleanup begun — or a
    pass done behind the upstream `head` the batch was planned against.
    A pass's boundary is fixed when it starts, so one resumed after the
    upstream moved (a full pass interrupted, then a change it was fired
    for) ends short of that change: the task goes on to it, as a delta."""

    if plan["kind"] in ("held", "selection"):
        return False
    # Behind: known only once the batch's bookmark is (`wm`, after `advance`).
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
    """Whether an input still owes its partition pass: one under way, a
    pattern change's diff, an Each cleanup."""

    return "pass" in wm or "pattern_change" in wm or "reconcile" in wm


def needs(wm: dict) -> int:
    """The first commit of the upstream's delta log the input still reads."""

    d = wm.get("pass")
    return int(d["from"]) if d is not None and d.get("from") is not None else int(wm["next"])


def pins(wm: dict) -> list[int]:
    """The reader pins an input holds: a delta pass in batches over attempts
    reads versions as of its first batch; a pattern change, its snapshot's
    index (lifecycle.md §9.8)."""

    out = []
    if (wm.get("pass") or {}).get("pin") is not None:
        out.append(wm["pass"]["pin"])
    if wm.get("pattern_change") is not None:
        out.append(wm["pattern_change"]["pin"])
    return out


def reads(plans: dict) -> list[tuple]:
    """The delta logs an attempt's plans read: `(output, partition, first commit)`
    — kept until its claim goes (§6)."""

    return [
        (p["bookmark"]["output"], p["bookmark"]["upstream_partition"], p["pass"]["from"])
        for p in plans.values()
        if p and p["kind"] == "keys" and p["pass"].get("from") is not None
    ]
