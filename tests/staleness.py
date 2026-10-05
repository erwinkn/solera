"""Staleness at every level (K43, K46; docs/observed-set.md), for the
simulation and the property tests: the engine's answers, behind
placeholder names, and a reference model of what they must be.

The rules:

- Output unit: a key within a partition if the output is keyed, else the
  partition. Input unit, per input: one upstream key for an each=True
  input; otherwise the upstream partition(s) it reads.
- What a consumer partition observed of a keyed input is its observed set,
  key -> (the version it processed, the whole and dep versions it was
  processed under). It owes every key that set holds otherwise than the
  upstream does now, under its patterns and the whole and dep versions now:
  a key it lacks, one it holds that the upstream removed or its patterns no
  longer take, one at another version or under another `knob`. "Changed"
  is the net difference: a key added and removed again, or updated and
  reverted to the version observed, owes nothing.
- An output unit is stale ("input changed") while it owes something —
  with each=True, the keys it owes; otherwise its keys go stale together.
  "Definition changed" while its asset's definition is not the one its
  last commit ran under. Staleness is transitive (K46): a unit is also
  stale when an upstream unit it depends on is ("upstream stale"). Each
  stale status says every reason that holds. Roll-ups use "any".
- Runs: a default run delivers what is owed. `keys=` delivers the keys it
  names as the upstream has them — unchanged ones too — and never touches
  a key it does not name (R2). A definition change or an upstream reset
  makes the next run a full run: its first batch starts over — a plain
  consumer from nothing, an each=True one from what it holds, at no
  upstream version, so every key is owed — and its later batches go on
  from there, `keys=` runs included.
- An output reset itself starts empty: a `keys=` run leaves just its keys;
  the next default run converges (R6).

Every engine call the tests make about staleness is in this module, so
adapting to the names the engine uses is one edit here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

# -- the engine's answers --------------------------------------------------------------


async def stale_keys(engine, asset: str, partition: str = "") -> set[str] | None:
    """The stale keys of a keyed asset's partition, every page of them
    (a non-each output's are all its keys, or none); None for an unkeyed
    output, which has no keys."""

    keys, after = set(), None
    while True:
        page = await engine.stale_keys(asset, partition, after=after)
        if not page.get("tracked", True):
            return None
        keys.update(k["key"] for k in page["keys"])
        after = page.get("next")
        if after is None:
            return keys


async def partition_stale(engine, asset: str, partition: str = "") -> bool:
    """Whether the partition statuses (API, CLI) report it `stale`."""

    rows = (await engine.partition_statuses([asset], every=False))[asset]
    return any(r["partition"] == partition and r["status"] == "stale" for r in rows)


async def stale_partitions(engine, asset: str) -> set[str]:
    rows = (await engine.partition_statuses([asset], every=False))[asset]
    return {r["partition"] for r in rows if r["status"] == "stale"}


async def stale_reasons(engine, asset: str, partition: str = "") -> set[str]:
    """Why the partition statuses report it `stale` (K46); empty if not."""

    rows = (await engine.partition_statuses([asset], every=False))[asset]
    for row in rows:
        if row["partition"] == partition:
            return set(row["reasons"]) if row["status"] == "stale" else set()
    return set()


async def asset_stale(engine, asset: str) -> bool:
    """The asset rollup's own flag (asset_statuses, the console's graph)."""

    return bool((await engine.asset_statuses())[asset]["stale"])


# -- the reference -------------------------------------------------------------------

INPUT, UPSTREAM, DEFINITION = "input changed", "upstream stale", "definition changed"


def everything(key: str) -> bool:
    return True


@dataclass
class EachAsset:
    """An each=True output's partition: whether it is built (without a commit
    it is `missing`, never `stale`), and its observed set of `items` — key
    -> (the generation it read, the `knob` it read it under); a key it
    holds at no upstream version (a held base, a full run under way) reads
    (None, None). `made`: the asset change its last commit ran under;
    `reset`: its upstream was reset since."""

    takes: Callable[[str], bool] = everything
    built: bool = False
    seen: dict[str, tuple] = field(default_factory=dict)
    changed_at: int = 0  # its asset's last change
    made: int = 0
    reset: bool = False
    whole: bool = False  # no key decodes from the empty base: complete


@dataclass
class ByPartition:
    """A plain incremental output's partition: whether it is built, its
    observed set of `items` (key -> the generation it read), the keys it
    holds (`keys`, keyed), and like an `EachAsset` `made` and `reset`."""

    takes: Callable[[str], bool] = everything
    built: bool = False
    seen: dict[str, int] = field(default_factory=dict)
    keys: set[str] = field(default_factory=set)
    changed_at: int = 0
    made: int = 0
    reset: bool = False
    whole: bool = False


class Reference:
    """`feed` (a keyed source, its keys versioned) -> `items` (plain
    incremental, run by hand) and `fchecks` (each=True); `items` ->
    `checks` (each=True, with the source `knob` a dep), `copy` (plain
    incremental, keyed) and `count` (plain incremental, unkeyed). What the
    observed set says of each after any history of feed commits, runs of
    `items`, its resets, `knob` changes, `checks`' own resets, asset
    changes, keys= runs and default runs (docs/observed-set.md): a
    consumer owes every key its observed set holds otherwise than `items`
    does now, under its patterns and `knob`; its definition is stale while
    its last commit ran under an older one; either change, or its
    upstream's reset, makes its next run a full run."""

    def __init__(self, takes: Callable[[str], bool] = everything):
        self.now = 0
        self.feed: dict[str, str] = {}  # key -> its version
        self.feed_read: dict[str, str] = {}  # `feed` as `items` last read it
        self.up: dict[str, int] = {}  # `items`: key -> the counter of its write (its generation)
        self.fchecks = EachAsset()  # each=True over `feed`: seen = key -> (version read, None)
        self.knob = 0
        self.checks = EachAsset(takes=takes)
        self.others = {"copy": ByPartition(takes=takes), "count": ByPartition()}

    def _tick(self) -> int:
        self.now += 1
        return self.now

    # history

    def commit_feed(self, upserts: set[str] | dict[str, str], removes: set[str]) -> None:
        """Upserts at the versions given, or at fresh ones (a set)."""

        t = self._tick()
        versions = upserts if isinstance(upserts, dict) else dict.fromkeys(upserts, f"v{t}")
        for k in removes - set(versions):
            self.feed.pop(k, None)
        self.feed.update(versions)

    def run_items(self) -> None:
        """`items` reads the net delta of `feed` since it last read it."""

        t = self._tick()
        for k in sorted(set(self.feed) | set(self.feed_read)):
            if self.feed.get(k) != self.feed_read.get(k):
                if k in self.feed:
                    self.up[k] = t
                else:
                    self.up.pop(k, None)
        self.feed_read = dict(self.feed)

    def commit(self, upserts: set[str], removes: set[str]) -> None:
        """A feed commit, and the run of `items` that takes it."""

        self.commit_feed(upserts, removes)
        self.run_items()

    def reset_upstream(self) -> None:
        """`items` moved, and rebuilt from `feed`: every key at a new version,
        in a new life; its consumers' next runs are full runs."""

        t = self._tick()
        self.up = dict.fromkeys(self.feed, t)
        self.feed_read = dict(self.feed)
        for o in (self.checks, *self.others.values()):
            o.reset = o.built

    def change_knob(self) -> None:
        self.knob = self._tick()

    def reset_checks(self) -> None:
        """`checks` itself reset (R6): empty, never built."""

        self.checks = EachAsset(takes=self.checks.takes)

    def change_asset(self, name: str) -> None:
        o = self.checks if name == "checks" else self.others[name]
        o.changed_at = self._tick()

    # runs

    def _full(self, o) -> bool:
        return o.built and (o.reset or o.made != o.changed_at)

    def _commit(self, o) -> None:
        o.built, o.reset, o.made = True, False, o.changed_at

    def run_keys(self, keys: set[str], name: str = "checks"):
        """A keys= run: each named key the patterns take is observed as
        `items` holds it — written if present, removed if held — and one
        they leave out goes if held. With a full run due, its batch starts
        over first: a plain consumer from nothing, `checks` from what it
        holds, at no upstream version. On `checks` returns the keys it
        writes; on a plain asset, (what reaches it, whether it starts over)."""

        o = self.checks if name == "checks" else self.others[name]
        start_over = self._full(o)
        if start_over:
            if name == "checks":  # what it holds, its base: complete
                o.seen, o.whole = dict.fromkeys(o.seen, (None, None)), True
            else:  # from nothing: no key covered yet
                o.seen, o.keys, o.whole = {}, set(), False
        written, delivered = set(), set()
        for k in keys:
            held = k in o.seen
            if o.takes(k) and k in self.up:
                o.seen[k] = (self.up[k], self.knob) if name == "checks" else self.up[k]
                written.add(k)
                delivered.add(k)
            elif held:
                del o.seen[k]
                delivered.add(k)
        if name != "checks":
            o.keys = (o.keys | written) - (delivered - written)
        self._commit(o)
        return written if name == "checks" else (delivered, start_over)

    def run_default(self, name: str):
        """A default run: everything owed — after a full run's start-over,
        every key `items` has under the patterns, and on `checks` every key
        it held, now owed. On `checks` returns the keys it writes; on a
        plain asset, (what reaches it, whether it starts over)."""

        o = self.checks if name == "checks" else self.others[name]
        start_over = self._full(o)
        if start_over:
            if name == "checks":
                o.seen = dict.fromkeys(o.seen, (None, None))
            else:
                o.seen, o.keys = {}, set()
        o.whole = True  # its walk covers every key
        owed = self._owed(o, name)
        for k in owed:
            if k in self.up and o.takes(k):
                o.seen[k] = (self.up[k], self.knob) if name == "checks" else self.up[k]
            else:
                o.seen.pop(k, None)
        written = {k for k in owed if k in o.seen}
        if name != "checks":
            o.keys = (o.keys | written) - (owed - written)
        self._commit(o)
        return written if name == "checks" else (owed, start_over)

    def run_full(self) -> set[str]:
        """A full run of `checks` (`mode="full"`): every key it holds, or
        `items` has under its patterns, again. Returns the keys it writes."""

        self.checks.reset = self.checks.built  # its start-over, as a reset's
        return self.run_default("checks")

    def run_fchecks(self, keys: set[str] | None = None) -> None:
        """A run of `fchecks`, each=True over `feed`: what it owes (default), or
        the named keys (keys=), made to match `feed`."""

        f = self.fchecks
        f.built = True
        for k in set(self.feed) | set(f.seen) if keys is None else keys:
            if k in self.feed:
                f.seen[k] = (self.feed[k], None)
            else:
                f.seen.pop(k, None)

    # answers

    def _owed(self, o, name: str) -> set[str]:
        """The keys its observed set holds otherwise than `items` now — under
        its patterns, and on `checks` under `knob` too."""

        out = set()
        for k in set(self.up) | set(o.seen):
            now = self.up.get(k) if o.takes(k) else None
            if name == "checks":
                now = None if now is None else (now, self.knob)
            if o.seen.get(k) != now:
                out.add(k)
        return out

    def items_stale(self) -> bool:
        return self.feed != self.feed_read

    def fchecks_stale_keys(self) -> set[str]:
        f = self.fchecks
        return {k for k in set(self.feed) | set(f.seen) if f.seen.get(k, (None,))[0] != self.feed.get(k)}

    def _input_owed(self, o, name: str) -> set[str]:
        """What staleness says an input owes: its observed set compared with
        `items` now — after an upstream reset, against nothing, or on
        `checks` against what it holds, at no upstream version."""

        if o.reset:
            fresh = EachAsset(seen=dict.fromkeys(o.seen, (None, None))) if name == "checks" else ByPartition()
            fresh.takes = o.takes
            return self._owed(fresh, name)
        return self._owed(o, name)

    def stale_keys(self) -> set[str]:
        """`checks`' stale keys: those it owes; its definition changed, every
        key `items` has under its patterns and every key it holds; and,
        while `items` is stale, every one of those (`items` is stale by
        partition)."""

        c = self.checks
        keys = self._input_owed(c, "checks")
        if self.definition_changed("checks") or self.items_stale():
            keys |= {k for k in self.up if c.takes(k)} | set(c.seen)
        return keys

    def stale_keys_of(self, name: str) -> set[str]:
        """The stale keys of a keyed asset: per key for `checks`; for `copy`
        all its keys, or none."""

        if name == "checks":
            return self.stale_keys()
        return set(self.others[name].keys) if self.stale(name) else set()

    # each reason its own predicate (Erwin's ruling): those that hold are
    # reported, and nothing remembers why a unit went stale

    def definition_changed(self, name: str) -> bool:
        o = self.checks if name == "checks" else self.others[name]
        return o.built and o.made != o.changed_at

    def reasons(self, name: str) -> set[str]:
        """Why `name` is stale (K46): the reasons whose predicate holds."""

        if name == "items":
            return {INPUT} if self.items_stale() else set()
        if name == "fchecks":
            return {INPUT} if self.fchecks.built and self.fchecks_stale_keys() else set()
        o = self.checks if name == "checks" else self.others[name]
        if not o.built:
            return set()  # never built: `missing`, not `stale`
        why = {INPUT} if self._input_owed(o, name) else set()
        why |= {DEFINITION} if self.definition_changed(name) else set()
        return why | ({UPSTREAM} if self.items_stale() else set())

    def stale(self, name: str) -> bool:
        return bool(self.reasons(name))

    def complete(self, name: str) -> bool:
        """Whether a consumer's content is complete: built, and no key decodes
        from the empty base — a default run walked every key since its
        last start-over from nothing; a per-key consumer's start-over keeps
        what it holds as its base."""

        o = self.checks if name == "checks" else self.others[name]
        return o.built and o.whole


class Holdings:
    """What the consumer holds, from its batches alone: each batch's classes
    must agree with it — `added` a key it lacks, `updated` and `removed` one
    it holds — and a start-over (`full and first`) empties it. The count it
    keeps is its size. A disagreement is recorded, not raised: raised in the
    producer, it would only fail the attempt."""

    def __init__(self):
        self.held: set[str] = set()
        self.trace: list[tuple] = []
        self.wrong: list[str] = []

    def apply(self, batch) -> None:
        if batch.full and batch.first:
            self.held.clear()
        added, updated, removed = set(batch.added), set(batch.updated), set(batch.removed)
        self.trace.append((sorted(added), sorted(updated), sorted(removed)))
        if added & self.held:
            self.wrong.append(f"added again: {sorted(added & self.held)}")
        if (updated | removed) - self.held:
            self.wrong.append(f"never held: {sorted((updated | removed) - self.held)}")
        self.held = (self.held | added) - removed

    def check(self, keys: set[str]) -> None:
        assert not self.wrong and self.held == keys, (self.wrong, self.trace)


class ObservedSets:
    """The literal observed set (docs/observed-set.md, D126): per consumer
    partition and keyed input, key -> (version, context), what each batch
    actually gave its producer — `added`, `updated` and `unchanged` keys at
    the version they were served at, `removed` keys gone. Observed absent
    and never observed are one: not in the dict. A plain dict, updated as
    the spec says; the engine's decode of its observation record must equal
    it after every commit (`engine.observed`)."""

    def __init__(self):
        self.sets: dict[tuple, dict[str, tuple]] = {}

    def apply(self, consumer: str, partition: str, param: str, batch, context: dict | None = None) -> None:
        held = self.sets.setdefault((consumer, partition, param), {})
        for key in (*batch.added, *batch.updated, *getattr(batch, "unchanged", ())):
            held[key] = (batch.served.get(key), dict(context or {}))
        for key in batch.removed:
            held.pop(key, None)

    def of(self, consumer: str, partition: str, param: str) -> dict[str, tuple]:
        return dict(self.sets.get((consumer, partition, param), {}))


async def decoded(engine, consumer: str, partition: str, param: str) -> dict[str, tuple]:
    """The engine's decode of a keyed input's observation record, as the
    reference keeps it: key -> (version, context), present keys only."""

    found = await engine.observed(consumer, partition, param)
    return {k: (o.version, dict(o.context)) for k, o in found.items()}


async def head_versions(engine, output: str, partition: str = "") -> dict:
    """Every key of an output at its head, at its version: a source's word, else its generation."""

    from solera.keys.delta import delta, version_of

    index, _ = engine._upstream(output, partition)
    out, after = {}, None
    while True:
        page = await delta(index, None, None, after=after)
        out |= {d.key: version_of(d.generation, d.payload) for d in page.diffs}
        if page.cursor is None:
            return out
        after = page.cursor


def agree(decoded: dict, literal: dict, head: dict) -> bool:
    """Whether a decode is the literal observed set as the index can know it
    (docs/observed-set.md, "What the index can say"): the same keys under
    the same contexts, each at its literal version — or at None, a version
    since replaced, where the literal one is not the head's."""

    if decoded.keys() != literal.keys():
        return False
    return all(
        d[1] == literal[k][1] and (d[0] == literal[k][0] or (d[0] is None and literal[k][0] != head.get(k)))
        for k, d in decoded.items()
    )
