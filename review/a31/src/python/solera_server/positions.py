"""Pass progress (§5, §6; per-key §11): where an Incremental input is in
delivering its upstream, and the one transition a delivered batch makes.

Each (asset, input, partition) keeps a **position**:

    {
      "kind": "keys" | "commits",   # a keyed upstream, read by key; or commit by commit
      "output", "upstream_partition", # the upstream output and partition it reads
      "fingerprint",                # the declaration it was delivered under (§6)
      "reset_by": run id,           # the run whose reset began the current pass
      "next": int,                  # the first upstream commit not yet delivered
      "pass": {                     # a pass under way, in batches over attempts
        "mode": "full" | "delta" | "diff",
        "from": int, "to": int,     # its boundary, decided when it starts
        "at": key | commit | None,  # its place: the last key delivered, the next commit
        "batch": int, "batches": int,  # the next batch's index, and how many are planned
        "pin": int,                 # its reader pin: a delta's files, a full pass's snapshot (lifecycle.md §9.8)
        "reconcile": bool,          # a full per-key pass owes a reconcile after (§11)
        "began": int,               # a full pass: the claim generation that started it
      },
      "began": int,                 # the last full pass's start, kept after it ends (d)
      "patterns": ...,              # keys: the patterns it delivers under (per-key §11)
      "pattern_change": {"old", "new", "at", "snapshot", "pin"},
      "reconcile": {"after": key},  # a per-key output's cleanup after a full pass
      "ahead": [[commit, run, attempt, ...], ...],  # keys: the read-ahead (below)
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
**plan** — `kind`, the `position` it carries forward, and the `pass`
its batch is on (`hi`, for commits: the batch's last commit); a `held` plan,
a batch that moves no position of its own (a per-key retry or reconcile
batch); or a `selection`, a run's `keys=` selection, which reads the keys it
names as of one upstream commit and moves neither `next` nor the
partition's progress.

**The read-ahead** (docs/positions-from-reads.md, K45, K47). A selection
adds `[commit, run, attempt]` to `ahead` — a paged selection's later
attempts join its run's entry, so the cap counts runs: it delivered, as of upstream
commit `commit`, the keys the attempt's sealed result lists, live or
removed. The next pass skips a changed key some entry read at or after
its last change, classes one changed since by what was delivered (K44),
and the entries a new
`next` passes collapse into the snapshot. A per-key input keeps the same
record: no per-key one.
"""

from __future__ import annotations


def advance(plan: dict, after: str | None = None) -> dict | None:
    """The position after a batch of `plan` was delivered — `after`, a key
    batch's last key (`None`: the pass is done) — or `None` if it moves
    none."""

    if plan["kind"] == "selection":
        position = plan.get("position")
        if position is None:  # no snapshot to be ahead of
            return None
        if plan.get("covers"):  # nothing is left undelivered: the record collapses to a new snapshot
            done = {k: v for k, v in position.items() if k not in ("pass", "ahead")}
            return {**done, "next": max(int(position["next"]), int(plan["head"]) + 1)}
        ahead = list(position.get("ahead", ()))
        if ahead and ahead[-1][1] == plan["run"]:  # a paged selection's next page: its run's entry
            commit, run, *attempts = ahead.pop()
            entry = [max(int(commit), int(plan["head"])), run, *attempts, plan["attempt"]]
        else:
            entry = [plan["head"], plan["run"], plan["attempt"]]
        position = {**position, "ahead": [*ahead, entry]}
        if "pass" in position and not plan.get("merge"):
            # a full pass this run started or continued: its next delivery is not its first
            position["pass"] = {**position["pass"], "batch": position["pass"]["batch"] + 1}
        return position
    if plan["kind"] == "held":
        return plan["position"]
    position, d = dict(plan["position"]), plan["pass"]
    if plan["kind"] == "commits":
        if plan["hi"] < d["to"]:
            position["pass"] = {**d, "at": plan["hi"] + 1, "batch": d["batch"] + 1}
        else:
            position.pop("pass", None)
            position["next"] = max(int(d["at"]), d["to"] + 1)
        return position
    if after is not None:  # the pass continues from the next key
        position["pass"] = {**d, "at": after, "batch": d["batch"] + 1}
        return position
    position.pop("pass", None)
    if d["mode"] == "diff":  # the transition is done: the new patterns from the pattern change on
        pattern_change = position.pop("pattern_change")
        position.pop("patterns", None)
        if pattern_change["new"] is not None:
            position["patterns"] = pattern_change["new"]
    elif d["mode"] == "full":
        position["next"] = d["from"]
        if d.get("reconcile"):
            # A per-key output may hold keys the pass no longer names — gone
            # upstream, or left out by the patterns: reconcile them next (§11).
            position["reconcile"] = {"after": None}
    else:
        position["next"] = max(d["from"], d["to"] + 1)
    return collapse(position)


def collapse(position: dict) -> dict:
    """The read-ahead entries the snapshot does not cover yet: one read as
    of a commit before `next` says nothing the snapshot does not."""

    ahead = [e for e in position.get("ahead", ()) if int(e[0]) >= int(position["next"])]
    if ahead:
        return {**position, "ahead": ahead}
    position.pop("ahead", None)
    return position


def continues(plan: dict, after: str | None, position: dict | None) -> bool:
    """Whether the task has more to deliver after this batch: a pass not
    done, a pattern change's diff still owed, a cleanup begun — or a
    pass done behind the upstream `head` the batch was planned against.
    A pass's boundary is fixed when it starts, so one resumed after the
    upstream moved (a full pass interrupted, then a change it was fired
    for) ends short of that change: the task goes on to it, as a delta."""

    if plan["kind"] in ("held", "selection"):
        return False
    # Behind: known only once the batch's position is (after `advance`).
    behind = (
        position is not None and "pass" not in position and int(position["next"]) <= int(plan.get("head", -1))
    )
    if plan["kind"] == "commits":
        return plan["hi"] < plan["pass"]["to"] or behind
    return after is not None or outstanding(position or {}) or behind


def selects(plans: dict) -> bool:
    """Whether an attempt reads a `keys=` selection: it then leaves the
    partition's progress as it was."""

    return any(p and p["kind"] == "selection" for p in plans.values())


def outstanding(position: dict) -> bool:
    """Whether an input still owes its partition pass: one under way, a
    pattern change's diff, a per-key cleanup."""

    return "pass" in position or "pattern_change" in position or "reconcile" in position


def pins(position: dict) -> list[int]:
    """The reader pins an input holds: a delta pass in batches over attempts
    reads versions as of its first batch; a pattern change, its snapshot's
    index (lifecycle.md §9.8)."""

    out = []
    if (position.get("pass") or {}).get("pin") is not None:
        out.append(position["pass"]["pin"])
    if position.get("pattern_change") is not None:
        out.append(position["pattern_change"]["pin"])
    return out


def reads(plans: dict) -> list[tuple]:
    """What an attempt's plans read of their upstream indexes: `(output,
    partition, first, end)`, `first` the commit its read starts at and `end`
    the head + 1 the plan was cut at, where the position may land (a pass's
    batch, a keys= selection, a pattern change's diff, a retry pass that may
    cover what is left). Both stay endpoints of the upstream index until the
    claim goes (docs/key-index-design.md § Endpoints). A plan that cannot
    move a keyed position reserves nothing: an unkeyed upstream's, one with
    no head to land at (a reconcile, a retry that cannot cover), or one with
    no position (a keys= selection while a pattern change decides
    membership)."""

    out = []
    for p in plans.values():
        if not p or p["kind"] not in ("keys", "selection", "held") or p.get("head") is None:
            continue
        position = p.get("position")
        if position is None:
            continue
        first = (p.get("pass") or {}).get("from")
        first = int(position["next"]) if first is None else int(first)
        out.append((position["output"], position["upstream_partition"], first, int(p["head"]) + 1))
    return out
