"""A keyed source commit on the new tracking core (docs/tracking-design.md
§4; `versions.md` §2): the delta half of `Engine._prepare_commit`.

The engine is the only writer of a source's log. A full map (`keys=`) is a
replacement: a key it does not name is removed. A patch (`upsert=`,
`remove=`) names its changes. A key given with a version word is a change
only if the word differs from the head's; a key given bare is a change, at
the commit's generation. A commit that changes nothing writes nothing.
"""

from __future__ import annotations

import contextlib

from solera._native import encode_changes

from ..state import Conflict
from . import Core

SOURCE_KEYS_RECORDED = 1000  # a source commit's run lists changed keys up to this many, else counts
PAGE = 10_000  # history entries a tick's staleness check reads at a time


async def build(
    core: Core, name: str, generation: int, run: str, keys, upsert, remove, listed: str | None, at: int | None
) -> dict | None:
    """Build a keyed source's next commit: `{"event", "minted", "counts",
    "upserted", "deleted", "elements"}`, or None when it changes nothing.
    Its layer is named by this build alone (`Core.path`); the caller
    checks and records the event, or leaves the layer an orphan (build plan
    B9 sweeps them). With `at`, it is a load's report (SRC-12): a key counts
    only if the log holds the same version for it at `at` and at the head."""

    def versions(given) -> dict:
        if isinstance(given, dict):
            return {str(k): (str(v) if v is not None else listed) for k, v in given.items()}
        return {str(k): listed for k in given or []}

    if keys is not None:
        changes, mode = encode_changes(versions(keys)), "replace"
    else:
        new = versions(upsert)
        changes = encode_changes(new, [str(k) for k in remove or [] if str(k) not in new])
        mode = "patch" if at is None else "report"
    minted: list = []
    id_ = core.output_id(name, "", minted)
    core.attach(id_)
    out = core.m.core_output(name, "") or {"head": 0, "layers": []}
    found = await core.run(
        core.tracking.build_commit,
        id_.encode(),
        changes,
        version=generation,
        path=core.path(id_, out["head"] + 1, run),
        time_ms=int(core.engine.clock() * 1000),
        producer=f"source:{generation}".encode(),
        mode=mode,
        at=at,
        collect=max(2 * SOURCE_KEYS_RECORDED, 1_000_000 if listed is not None else 0),
    )
    if found is None:
        return None
    built = {
        "event": found["event"],
        "minted": minted,
        "counts": {k: found[k] for k in ("commit", "live", "added", "updated", "removed")} | {"id": id_},
    }
    for field, changed, count in (
        ("upserted", found["upserted_keys"], found["added"] + found["updated"]),
        ("deleted", found["removed_keys"], found["removed"]),
    ):
        built[field] = changed if changed is not None and len(changed) <= SOURCE_KEYS_RECORDED else count
    if listed is not None:
        from .commit import elements

        built["elements"] = await elements(core, name, "", out, found)
    return built


async def moved(core: Core, name: str, since: int, commit: dict) -> bool:
    """Whether a sensor tick's commit to keyed source `name` is stale (D193):
    a key it reports has an entry in a commit after `since`, the commit the
    tick observed, removals included. A full map reports every key, so any
    commit since makes it stale."""

    found = core.m.core_output(name, "")
    head = found["head"] if found is not None else 0
    if head == since:
        return False
    if commit.get("keys") is not None:
        return True
    keys = {str(k) for k in commit.get("upsert") or ()} | {str(k) for k in commit.get("remove") or ()}
    if not keys:
        return False
    core.attach(core.m.core_outputs[(name, "")])
    after = None
    while True:
        rows, after = await core.run(
            core.index.history,
            found["layers"],
            commits=(since + 1, head),
            hi=max(keys),
            after=after,
            limit=PAGE,
        )
        if any(key in keys for key, _ in rows):
            return True
        if after is None:
            return False


async def record_loads(engine, attempt: str, prepared: dict, result: dict) -> None:
    """A batch that loaded a source's key at another version than its log
    says records it in the log (SRC-12, D193): a commit of the source by the
    load, as a report against the batch's head, so a key the log changed
    since then is dropped (it knows better). A key found absent is not: the
    log lists it until a commit says otherwise (SRC-5). Recorded after the
    batch's commit; lost in a crash between the two, or refused because the
    source moved meanwhile, the key is owed again, as on today's path."""

    delivered = result.get("delivered") or {}
    for param, plan in (prepared.get("plans") or {}).items():
        if (
            (plan or {}).get("kind") != "core"
            or plan.get("done")
            or plan["output"] not in engine.manifest["sources"]
        ):
            continue
        upsert = {k: v for k, v in (delivered.get(param) or {}).get("loaded") or () if v is not None}
        if upsert:
            with contextlib.suppress(Conflict):
                await engine.commit_source(
                    plan["output"], upsert=upsert, by=f"load {attempt}", at=plan["head"]
                )
