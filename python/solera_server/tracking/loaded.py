"""What a batch must not process (docs/tracking-design.md §8 rule 4 and
§10 rule 4, one rule). A table is read in place, so a batch may load a key
at another version than the one its plan read from the upstream's log. Its
worker asks, as it loads, which of those keys to hold back:

- the upstream is rebuilding (a full run under way), or started over since
  the batch planned: every such key is held back, since the rows are from
  the middle of a rebuild;
- the upstream's log now lists the key at the version loaded (its producer
  committed since the plan), or absent as loaded: processed as loaded;
- an attempt of the producer is live: the version is its uncommitted
  write, or its removal. Processed as loaded; the key settles when that
  attempt commits;
- anything else is an anomaly: a dead writer's version, a row that
  vanished, a version no attempt explains. It is held back, and an alarm
  names the output, the key and, where known, the attempt.

A key held back keeps what the consumer held of it and stays owed. The
answer is advice the commit checks again: a rebuild that begins after it
still refuses the batch (`progress_ops`).
"""

from __future__ import annotations

from . import Core


def rebuilding(core: Core, output: str, partition: str, head: int) -> bool:
    """Whether an output is mid full run, or started over after commit `head`."""

    m = core.m
    if m.partial(output, partition, m.axis_head(output, partition)):
        return True
    found = m.core_output(output, partition) or {"layers": ()}
    return any(clear > head for layer in found["layers"] for clear in layer["clears"])


async def held(core: Core, prepared: dict, found: dict) -> dict:
    """Per keyed input of a batch, the keys of `found[input]` (`[key,
    version]`, None: absent) it must hold back."""

    engine, m, out = core.engine, core.m, {}
    for param, loaded in found.items():
        plan = (prepared.get("plans") or {}).get(param) or {}
        if plan.get("kind") != "core" or not loaded:
            continue
        output, partition = plan["output"], plan["upstream_partition"]
        if rebuilding(core, output, partition, plan["head"]):
            out[param] = sorted(key for key, _ in loaded)
            continue
        log, listed = {}, m.core_output(output, partition)
        if listed is not None:
            rows, _ = await core.run(
                core.index.get, listed["layers"], listed["head"], sorted(key for key, _ in loaded)
            )
            log = {row[0]: row[2] for row in rows}
        producer = (engine.manifest["outputs"].get(output) or {}).get("asset")
        claim = m.claimed(m.claimed_partitions.get((producer, partition)) or "")
        writing = (None, claim["generation"]) if claim is not None else ()
        dead = dict(map(tuple, m.repairs.get((output, partition), ())))
        out[param] = []
        for key, version in loaded:
            if log.get(key) == version or version in writing:
                continue
            out[param].append(key)
            engine.anomalies.setdefault((output, partition), {})[key] = {
                "output": output,
                "partition": partition,
                "key": key,
                "version": version,
                "attempt": dead.get(version),
            }
        out[param].sort()
    return out
