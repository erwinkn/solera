"""Reads of a key index, checked against the fold of their commits.

A reader reads Δ(P, H) — every key whose state differs between commits P
and H, with its presence at both ends and its version at H — or looks keys
up at the head (docs/key-index-design.md). The index serves these from
stamped layers, which merges rewrite and whose flips at or below the cut
they drop. A read the index refuses (a P below the cut, an H inside a merged
layer) is wrong too, and so is the cut that passed a commit a reader holds.
Which commits readers hold comes from the events, not from the engine's own
`Model.oldest_observed`, so that a reader it misses is still checked:
attempts in flight from their launches to their ends; observation records
from the model's, their life (resets, renames) being the model's alone to
fold.

So each read is checked against the fold of the commits the reader's own
index holds. Each commit's entries are taken from its delta files when the
commit is installed (`LayerState.committed`). Each merge's inputs are
recorded when it is published (`LayerState.merged`). Any index state can
then be traced back to its original commits, however merged, and folded up
to any commit, without the layers or merges that served the read. A read
that disagrees is kept in `wrong`; the `reads_at_endpoints_are_exact`
invariant raises it."""

from __future__ import annotations

import contextlib
from pathlib import Path

Entry = tuple[int, bool, bytes | None]  # generation, removed, payload


def _payload(p) -> bytes | None:
    return None if p is None else bytes(p)


def _paths(state, layer) -> tuple[str, ...]:
    return tuple(state.path(n) for n in layer.names())


class Reads:
    def __init__(self, root: Path):
        self.root = root  # the namespace's directory: where index files live
        self.leaves: dict[tuple[str, ...], tuple[int, dict[bytes, Entry]]] = {}  # a commit's files
        self.merges: dict[tuple[str, ...], list[tuple[str, ...]]] = {}  # a merge's files -> its inputs'
        self.wrong: list[str] = []
        self.checked = 0  # reads compared with their fold

    # -- what installs and merges say ------------------------------------------------------

    def installed(self, state, commit_number: int, delta) -> None:
        from solera import _native

        paths = tuple(state.path(n) for n in delta.part.names())
        if not paths or paths in self.leaves:
            return
        entries: dict[bytes, Entry] = {}
        for f in delta.part.files:
            try:
                data = (self.root / state.path(f.name)).read_bytes()
            except FileNotFoundError:
                return  # unknown: reads over this commit go unchecked
            for e in _native.layers_decode(data, commit_number, delta.generation):
                entries[bytes(e[0])] = (delta.generation, not e[1], _payload(e[6]))
        self.leaves[paths] = (commit_number, entries)

    def merged(self, state, ids, out) -> None:
        by_id = {x.id: x for x in state.layers}
        self.merges[_paths(state, out)] = [_paths(state, by_id[i]) for i in ids if i in by_id]

    # -- the commits readers hold, apart from the engine's own word ------------------------

    @staticmethod
    def launched(model, e) -> None:
        """An attempt's reads, from its launch: the head each keyed batch
        classes its keys at."""

        held = model.__dict__.setdefault("_sim_claims", {})
        out = []
        for plan in ((e.get("prepared") or {}).get("plans") or {}).values():
            if plan and plan.get("kind") == "observed":
                out.append(((plan["output"], plan["upstream_partition"]), int(plan["head"])))
        held[e["attempt"]] = (e["run"], out)

    @staticmethod
    def ended(model, e) -> None:
        held = model.__dict__.setdefault("_sim_claims", {})
        if e["type"] == "AttemptFinished":
            held.pop(e["attempt"], None)
        else:  # RunArchived: its attempts go with it
            for attempt in [a for a, (run, _) in held.items() if run == e["run"]]:
                del held[attempt]

    @staticmethod
    def holders(model, key) -> set[int]:
        """The commits of `key`'s index readers hold: attempts in flight, from
        their launches (not the engine's claims), and observation records,
        from the model's (a record's life is the model's own to fold)."""

        out: set[int] = set()
        for _, reads in model.__dict__.get("_sim_claims", {}).values():
            out.update(head for k, head in reads if k == key)
        state = model.indexes.get(key)
        for record in model.partitions.values():
            for rec in (record.get("observed") or {}).values():
                if tuple(rec["upstream"]) != key or state is None:
                    continue
                for layer in [rec["base"], *rec["ranges"]]:
                    if layer["endpoint"] is not None and layer["life"] == state.life:
                        out.add(int(layer["endpoint"]))
        return out

    def cut_kept(self, key, cut: int, held: set[int]) -> None:
        """A cut never passes a commit a reader holds: below the cut, flips
        go, and Δ from there is refused."""

        below = sorted(c for c in held if c < cut)
        if below:
            self.wrong.append(f"{key}: the cut moved to {cut}, past commit {below[0]}, which a reader holds")

    # -- the fold ----------------------------------------------------------------------------

    def _commits(self, paths: tuple[str, ...]) -> list[tuple[int, dict[bytes, Entry]]] | None:
        if paths in self.leaves:
            return [self.leaves[paths]]
        if paths in self.merges:
            out = []
            for child in self.merges[paths]:
                sub = self._commits(child)
                if sub is None:
                    return None
                out += sub
            return out
        return None

    def commits(self, state) -> list[tuple[int, dict[bytes, Entry]]] | None:
        """The original commits an index state holds, oldest first; None if
        one of its layers cannot be traced back. An empty delta holds none."""

        out = []
        for layer in state.layers:
            paths = _paths(state, layer)
            if not paths:
                continue
            sub = self._commits(paths)
            if sub is None:
                return None
            out += sub
        return sorted(out, key=lambda c: c[0])

    @staticmethod
    def fold(commits, upto: int | None) -> dict[bytes, Entry]:
        """Each key's newest entry in the commits at or before `upto` (all: None)."""

        state: dict[bytes, Entry] = {}
        for c, entries in commits:
            if upto is None or c <= upto:
                state.update(entries)
        return state

    def _wrong(self, index, call: str, args, why: str) -> None:
        self.wrong.append(f"{index.state.prefix} {call}{args}: {why}")

    # -- the reads ---------------------------------------------------------------------------

    @contextlib.contextmanager
    def served(self, index, call: str, args):
        """A read the index refuses is wrong too: a commit a reader holds must
        stay readable (the cut keeps it; a batch reads the state it pinned)."""

        from solera.keys.layers import CutError, NotHeld

        try:
            yield
        except (CutError, NotHeld) as error:
            self._wrong(index, call, args, f"not served: {error}")
            raise

    def delta(self, index, p, keys, after, upto, take, glob, result) -> None:
        """Every key that differs between P and H: live at one end and not the
        other, or live at both with another version at H. A filtered read
        (`take`, `glob`) is checked for what it returns, not for what it
        leaves out."""

        commits = self.commits(index.state)
        if commits is None:
            return
        self.checked += 1
        before = {} if p is None else self.fold(commits, p)
        now = self.fold(commits, None)

        def live(state, k):
            return k in state and not state[k][1]

        def differs(k):
            was, is_ = live(before, k), live(now, k)
            return was != is_ or (is_ and before[k][0] != now[k][0])

        rows, cursor = result
        got = [bytes(r[0]) for r in rows]
        args = (p, after, len(keys) if keys is not None else None)
        if got != sorted(got) or len(set(got)) != len(got):
            self._wrong(index, "delta", args, f"keys out of order: {got[:5]}")
            return
        for k, was, is_, g, pl in rows:
            k = bytes(k)
            want = (live(before, k), live(now, k))
            if (bool(was), bool(is_)) != want or not differs(k):
                self._wrong(index, "delta", args, f"{k!r} as {(was, is_)}, the fold {want}")
                return
            if is_ and (int(g), _payload(pl)) != (now[k][0], now[k][2]):
                self._wrong(index, "delta", args, f"{k!r} at {g}, the fold at {now[k][0]}")
                return
        if take is not None or glob is not None:
            return
        if keys is not None:
            want = sorted({bytes(k) for k in keys if differs(bytes(k))})
        else:
            stop = bytes(cursor) if cursor is not None else None
            want = sorted(
                k
                for k in set(before) | set(now)
                if differs(k)
                and (after is None or k > bytes(after))
                and (upto is None or k <= bytes(upto))
                and (stop is None or k <= stop)
            )
        if got != want:
            self._wrong(index, "delta", args, f"keys {got[:5]}, the fold {want[:5]}")

    def lookup(self, index, keys, result) -> None:
        commits = self.commits(index.state)
        if commits is None:
            return
        self.checked += 1
        state = self.fold(commits, None)
        want = {k: (state[k][0], state[k][2]) for k in map(bytes, keys) if k in state and not state[k][1]}
        got = {bytes(k): (int(g), _payload(p)) for k, (g, p) in result.items()}
        if got != want:
            self._wrong(
                index,
                "lookup",
                (len(keys),),
                f"{sorted(got.items())[:3]}, the fold {sorted(want.items())[:3]}",
            )


def patches(reads: Reads) -> list[tuple]:
    """What `World._install` patches so that every install, merge, cut and
    read goes through `reads`."""

    from solera.keys.layers import LayerIndex, LayerState
    from solera_server.model import Model

    committed, merged, on_cut, apply = (
        LayerState.committed,
        LayerState.merged,
        Model._on_IndexCut,
        Model.apply,
    )
    delta, lookup = LayerIndex.delta, LayerIndex.lookup

    def committing(state, commit_number, files):
        out = committed(state, commit_number, files)
        reads.installed(state, commit_number, files)
        return out

    def merging(state, ids, out):
        reads.merged(state, ids, out)
        return merged(state, ids, out)

    def applying(model, e):
        if e["type"] == "AttemptLaunched":
            reads.launched(model, e)
        elif e["type"] in ("AttemptFinished", "RunArchived"):
            reads.ended(model, e)
        return apply(model, e)

    def cutting(model, e):
        key = (e["output"], e["partition"])
        reads.cut_kept(key, int(e["cut"]), reads.holders(model, key))
        on_cut(model, e)

    async def reading(index, p, *, keys=None, after=None, upto=None, glob=None, take=None, **kw):
        with reads.served(index, "delta", (p, after)):
            result = await delta(index, p, keys=keys, after=after, upto=upto, glob=glob, take=take, **kw)
        reads.delta(index, p, keys, after, upto, take, glob, result)
        return result

    async def looking(index, keys):
        result = await lookup(index, keys)
        reads.lookup(index, keys, result)
        return result

    return [
        (LayerState, "committed", committing),
        (LayerState, "merged", merging),
        (Model, "_on_IndexCut", cutting),
        (Model, "apply", applying),
        (LayerIndex, "delta", reading),
        (LayerIndex, "lookup", looking),
    ]
