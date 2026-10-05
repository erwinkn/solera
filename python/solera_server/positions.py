"""An unkeyed Incremental input's progress (§5, §6): where it is in
delivering its upstream's commits, and the one transition a delivered
batch makes. A keyed input keeps an observation record instead
(docs/observed-set.md, `observing`).

Each (asset, input, partition) reading an unkeyed upstream keeps a
**position**:

    {
      "kind": "commits",
      "output", "upstream_partition", # the upstream output and partition it reads
      "fingerprint",                # the declaration it was delivered under (§6)
      "reset_by": run id,           # the run whose reset began the current pass
      "next": int,                  # the first upstream commit not yet delivered
      "pass": {                     # a pass under way, in batches over attempts
        "mode": "full" | "delta",
        "from": int, "to": int,     # its boundary, decided when it starts
        "at": int,                  # its place: the next commit
        "batch": int, "batches": int,  # the next batch's index, and how many are planned
      },
      "seen": ...,                  # the whole and dep versions its last full pass began under
    }

`full` delivers the upstream's commits from its reset's `base` to `to`;
`delta`, the commits `from`..`to` past the last pass. A pass's mode,
boundary and batch plan are decided when it starts and kept until its
last batch; `next` then moves past it.
"""

from __future__ import annotations


def advance(plan: dict) -> dict:
    """The position after a batch of a `commits` plan was delivered."""

    position, d = dict(plan["position"]), plan["pass"]
    if plan["hi"] < d["to"]:
        position["pass"] = {**d, "at": plan["hi"] + 1, "batch": d["batch"] + 1}
    else:
        position.pop("pass", None)
        position["next"] = max(int(d["at"]), d["to"] + 1)
    return position


def continues(plan: dict, position: dict | None = None) -> bool:
    """Whether the task has more to deliver after this batch: a pass not
    done, or — known once its `position` is — one done behind the upstream
    `head` the batch was planned against. A pass's boundary is fixed when it
    starts, so one resumed after the upstream moved ends short of that
    change: the task goes on to it, as a delta."""

    behind = (
        position is not None and "pass" not in position and int(position["next"]) <= int(plan.get("head", -1))
    )
    return plan["hi"] < plan["pass"]["to"] or behind


def selects(plans: dict) -> bool:
    """Whether an attempt reads keys a `keys=` run names: it then leaves the
    partition's progress as it was."""

    return any(
        p and p["kind"] == "observed" and p.get("named") and not p.get("retry") for p in plans.values()
    )


def outstanding(position: dict) -> bool:
    """Whether an input still owes its partition a pass under way."""

    return "pass" in position


def reads(plans: dict) -> list[tuple]:
    """What an attempt's plans read of their upstream indexes until the
    claim goes: `(output, partition, head)`, the head a keyed batch classes
    its keys at and its commit records."""

    return [
        (p["output"], p["upstream_partition"], p["head"])
        for p in plans.values()
        if p and p["kind"] == "observed"
    ]
