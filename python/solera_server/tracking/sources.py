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


async def build(
    core: Core, name: str, generation: int, keys, upsert, remove, listed: str | None
) -> dict | None:
    """Build a keyed source's next commit: `{"event", "minted", "counts",
    "upserted", "deleted", "elements"}`, or None when it changes nothing.
    Its layer is written; the caller checks and records the event, or
    leaves the layer an orphan (build plan B9 sweeps them)."""

    def versions(given) -> dict:
        if isinstance(given, dict):
            return {str(k): (str(v) if v is not None else listed) for k, v in given.items()}
        return {str(k): listed for k in given or []}

    if keys is not None:
        changes, mode = encode_changes(versions(keys)), "replace"
    else:
        new = versions(upsert)
        changes = encode_changes(new, [str(k) for k in remove or [] if str(k) not in new])
        mode = "patch"
    minted: list = []
    id_ = core.output_id(name, "", minted)
    core.attach(id_)
    out = core.m.core_output(name, "") or {"head": 0, "layers": []}
    found = await core.run(
        core.tracking.build_commit,
        id_.encode(),
        changes,
        version=generation,
        path=core.path(id_, out["head"] + 1, f"g{generation}"),
        time_ms=int(core.engine.clock() * 1000),
        producer=f"source:{generation}".encode(),
        mode=mode,
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
    """Whether a sensor tick's commit to source `name` is stale (D193): a key
    it reports changed since the tick observed the source at commit `since`.
    A full map reports every key, so any commit since makes it stale."""

    found = core.m.core_output(name, "")
    head = found["head"] if found is not None else 0
    if head == since:
        return False
    if commit.get("keys") is not None:
        return True
    keys = sorted({str(k) for k in commit.get("upsert") or ()} | {str(k) for k in commit.get("remove") or ()})
    if not keys:
        return False
    core.attach(core.m.core_outputs[(name, "")])
    now, _ = await core.run(core.index.get, found["layers"], head, keys)
    then, _ = await core.run(core.index.get, found["layers"], since, keys) if since else ([], None)
    return any(row[1] > since for row in now) or bool({row[0] for row in then} - {row[0] for row in now})


async def record_loads(engine, attempt: str, prepared: dict, result: dict) -> None:
    """A batch that loaded a source's key at another version than its log
    says records it in the log (SRC-12, D193): a commit of the source by the
    load, unless the log changed the key since the batch's head (it then
    knows better). A key found absent is not: the log lists it until a
    commit says otherwise (SRC-5). Recorded after the batch's commit; lost in a crash
    between the two, the key is owed again, as on today's path."""

    core, delivered = engine.core, result.get("delivered") or {}
    for param, plan in (prepared.get("plans") or {}).items():
        if (
            (plan or {}).get("kind") != "core"
            or plan.get("done")
            or plan["output"] not in engine.manifest["sources"]
        ):
            continue
        loaded = [kv for kv in (delivered.get(param) or {}).get("loaded") or () if kv[1] is not None]
        found = core.m.core_output(plan["output"], "")
        if not loaded or found is None:
            continue
        keys = sorted(k for k, _ in loaded)
        core.attach(core.m.core_outputs[(plan["output"], "")])
        now, _ = await core.run(core.index.get, found["layers"], found["head"], keys)
        then, _ = await core.run(core.index.get, found["layers"], plan["head"], keys)
        moved = {r[0] for r in now} ^ {r[0] for r in then} | {r[0] for r in now if r[1] > plan["head"]}
        upsert = {k: v for k, v in loaded if v is not None and k not in moved}
        if upsert:
            with contextlib.suppress(Conflict):  # the log moved meanwhile: it knows better
                await engine.commit_source(plan["output"], upsert=upsert, by=f"load {attempt}")
