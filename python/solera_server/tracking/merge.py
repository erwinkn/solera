"""Merging an output's layers (docs/tracking-design.md §13). Every commit of a
keyed output or a log adds one layer file to its list. Left alone, the list
grows by one object per commit: every plan and read opens more files, and
every attempt's spec carries a longer list. A merge replaces adjacent layers
with one that holds the same entries, by the index's size rule (four layers
of like size, and never more than 32 in a list), so a list stays a few
dozen files however many commits it holds.

One merge runs at a time, off the event loop. It is built from the layers
as they were when it was picked; commits that land meanwhile only append,
so its event still applies, and the core refuses it if the output was
retired or reset. The files it replaced become garbage, deleted once no
attempt or engine read pinned before the merge is still running
(`Upkeep.collect`).

A merge here keeps every entry: nothing is folded into a base, so an
output's whole history stays readable. Folding it below what readers still
need comes with cleanup (build plan B9).
"""

from __future__ import annotations

import logging

from solera._native import retain_merge, retain_merge_plan, retain_merges_due

from ..state import Conflict
from . import Core, Generation

log = logging.getLogger(__name__)

JOB = "merge"  # the one merge running, among upkeep's jobs


def start(engine) -> None:
    """Start the next merge that is due, unless one is running."""

    jobs = engine.upkeep.jobs
    if JOB in jobs:
        return
    due = retain_merges_due(engine.m.core)
    if due:
        jobs.spawn(merge(engine.core, due[0]), JOB)


async def merge(core: Core, id_: bytes) -> bool:
    """Merge the layers the size rule picks on output `id_`, and record it.
    False when there was nothing to do, or the output moved on meanwhile."""

    m, generation = core.m, Generation(core.m)
    life = (m.core.output(id_) or {}).get("life")
    # Held at commit 0: every entry is kept, none folds into a base.
    plan = retain_merge_plan(m.core, id_, [(id_, 0, "run", b"history")])
    if plan is None:
        return False
    core.attach(id_)
    path = core.path(life, 0, "merge")
    found = await core.run(retain_merge, m.core, plan, path)
    try:
        generation.check()  # a deploy may have retired the output and named another
        core.record(found["event"], garbage=found["replaced"])
    except Conflict:
        await core.engine.state.delete_objects([path])  # no state names it
        return False
    log.info("merged %d layers of %s", len(found["replaced"]), id_.decode(errors="replace"))
    return True
