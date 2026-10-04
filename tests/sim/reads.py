"""Reads at an endpoint, checked against the fold of their commits.

A reader reads a key index at an endpoint: a position's `next`, a claim's
pin, a full pass's snapshot. `page` and `lookup` read the state after commit
`at - 1`; `changes_page` reads what commits `[first, last]` changed, and
how. A read the index cannot serve at all (a merge dropped the endpoint) is
wrong too, and so is the merge that dropped it: every endpoint a reader
holds must still start a span or segment after a merge. Which endpoints
readers hold comes from the events, not from the engine's own list
(`Model.endpoints`), so that a reader that list misses is still checked:
attempts in flight from their launches to their ends; positions from the
model's records, their life (resets, renames, pattern changes) being the
model's alone to fold. The index serves these from spans, which merges rewrite. A merge that
ignores an endpoint still reads well at the head, and only a reader at that
endpoint sees the wrong state.

So each read is checked against the fold of the commits the reader's own
index holds. Each commit's entries are taken from its delta files when the
commit is installed (`IndexState.committed`). Each merge's inputs are
recorded when it is published (`IndexState.merged`). Any index state can then
be traced back to its original commits, however merged, and folded up to any
endpoint, without the spans or merges that served the read. A read that
disagrees is kept in `wrong`; the `reads_at_endpoints_are_exact` invariant
raises it."""

from __future__ import annotations

import contextlib
from pathlib import Path

Entry = tuple[int, bool, bytes | None]  # generation, deleted, payload


def _payload(p) -> bytes | None:
    return None if p is None else bytes(p)


class Reads:
    def __init__(self, root: Path):
        self.root = root  # the namespace's directory: where index files live
        self.leaves: dict[tuple[str, ...], tuple[int, dict[bytes, Entry]]] = {}  # a commit's files
        self.merges: dict[tuple[str, ...], list[tuple[str, ...]]] = {}  # a merge's files -> its inputs'
        self.wrong: list[str] = []
        self.checked = 0  # reads compared with their fold

    # -- what installs and merges say ------------------------------------------------------

    def installed(self, state, commit_number: int, delta) -> None:
        paths = tuple(state.path(f.name) for f in delta.files)
        if not paths or paths in self.leaves:
            return
        from solera_worker.worker import _file_entries

        entries: dict[bytes, Entry] = {}
        for path in paths:
            try:
                data = (self.root / path).read_bytes()
            except FileNotFoundError:
                return  # unknown: reads over this commit go unchecked
            for key, generation, deleted, payload, _ in _file_entries(data):
                entries[bytes(key)] = (int(generation), bool(deleted), _payload(payload))
        self.leaves[paths] = (commit_number, entries)

    # -- the endpoints readers hold, apart from the engine's own list ----------------------

    @staticmethod
    def launched(model, e) -> None:
        """An attempt's reads, from its launch: what each plan reads of its
        upstream index, from where to where (`first`, `end`)."""

        held = model.__dict__.setdefault("_sim_claims", {})
        out = []
        for plan in ((e.get("prepared") or {}).get("plans") or {}).values():
            if not plan or plan.get("kind") not in ("keys", "selection", "held") or plan.get("head") is None:
                continue
            position = plan.get("position")
            if position is None:
                continue
            first = (plan.get("pass") or {}).get("from")
            first = int(position["next"]) if first is None else int(first)
            out.append(((position["output"], position["upstream_partition"]), first, int(plan["head"]) + 1))
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
        """The endpoints of `key`'s index readers hold: attempts in flight,
        from their launches (not the engine's claims), and positions, from the
        model's records (a position's life — resets, renames, pattern changes —
        is the model's own to fold)."""

        out: set[int] = set()
        for _, reads in model.__dict__.get("_sim_claims", {}).values():
            out.update(x for k, first, end in reads if k == key for x in (first, end))
        for position in model.positions():
            if (position["output"], position["upstream_partition"]) != key:
                continue
            out.add(int(position["next"]))
            d = position.get("pass") or {}
            if d.get("from") is not None:
                out.add(int(d["from"]))
            if d.get("to") is not None:
                out.add(int(d["to"]) + 1)
            if position.get("pattern_change") is not None:
                out.add(int(position["pattern_change"]["at"]) + 1)
        return out

    def kept(self, key, before, after, endpoints) -> None:
        """A merge just installed keeps every endpoint a reader holds: one it
        drops is one no read can be exact at (W36's planted bug)."""

        if after is before:
            return  # refused: nothing changed
        for e in sorted(endpoints):
            if 0 < e <= after.head and after.generation(e) is None and before.generation(e) is not None:
                self.wrong.append(
                    f"{after.prefix}: a merge dropped endpoint {e}, which a reader of {key} holds"
                )
                return

    def merged(self, state, inputs, out) -> None:
        spans = {(s.a, s.b): s for s in state.spans}
        self.merges[tuple(state.path(f.name) for f in out.files)] = [
            tuple(state.path(f.name) for f in spans[tuple(r)].files) for r in inputs if tuple(r) in spans
        ]

    # -- the fold ----------------------------------------------------------------------------

    def _commits(self, paths: tuple[str, ...]) -> list[tuple[int, dict[bytes, Entry]]] | None:
        if not paths:
            return []
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
        one of its spans cannot be traced back. A slice (a catch-up's pin:
        the spans overlapping its range) holds only its own."""

        out = []
        for span in state.spans:
            sub = self._commits(tuple(state.path(f.name) for f in span.files))
            if sub is None:
                return None
            out += sub
        return sorted(out, key=lambda c: c[0])

    @staticmethod
    def fold(commits, below: int | None) -> dict[bytes, Entry]:
        """Each key's newest entry in the commits before `below` (all: None)."""

        state: dict[bytes, Entry] = {}
        for c, entries in commits:
            if below is None or c < below:
                state.update(entries)
        return state

    @staticmethod
    def _below(state, at: int | None) -> int | None:
        return None if at is None or at > state.head else at

    def _wrong(self, index, call: str, args, why: str) -> None:
        self.wrong.append(f"{index.prefix} {call}{args}: {why}")

    # -- the reads ---------------------------------------------------------------------------

    @contextlib.contextmanager
    def served(self, index, call: str, args):
        """A read at an endpoint the index cannot serve is wrong too: an
        endpoint a reader holds must stay readable (a merge keeps it)."""

        try:
            yield
        except LookupError as error:
            self._wrong(index, call, args, f"not served: {error}")
            raise

    @staticmethod
    def whole(state) -> bool:
        return not state.spans or state.spans[0].a == 0

    def page(self, index, after, limit, at, result) -> None:
        commits = self.commits(index.state)
        if commits is None or not self.whole(index.state):
            return
        self.checked += 1
        state = self.fold(commits, self._below(index.state, at))
        keys, generations, payloads, cursor = result
        want = sorted(
            k for k, (_, deleted, _) in state.items() if not deleted and (after is None or k > after)
        )
        keys = [bytes(k) for k in keys]
        if keys != want[: len(keys)] or (cursor is None and len(keys) != len(want)):
            self._wrong(index, "page", (after, limit, at), f"keys {keys[:5]}, the fold {want[:5]}")
            return
        for k, g, p in zip(keys, generations, payloads, strict=True):
            if (int(g), _payload(p)) != (state[k][0], state[k][2]):
                self._wrong(index, "page", (after, limit, at), f"{k!r} at {g}, the fold at {state[k][0]}")
                return

    def lookup(self, index, keys, at, result) -> None:
        commits = self.commits(index.state)
        if commits is None or not self.whole(index.state):
            return
        self.checked += 1
        state = self.fold(commits, self._below(index.state, at))
        want = {}
        for k in map(bytes, keys):
            if k in state and not state[k][1]:  # live at the endpoint
                want[k] = (state[k][0], state[k][2])
        got = {bytes(k): (int(g), _payload(p)) for k, (g, p) in result.items()}
        if got != want:
            self._wrong(
                index,
                "lookup",
                (len(keys), at),
                f"{sorted(got.items())[:3]}, the fold {sorted(want.items())[:3]}",
            )

    def changes(self, index, first, last, after, limit, keys, until, page) -> None:
        """Every key with a version in `[first, last]`, classed by the net
        rule (docs/key-index-design.md § changes) from whether it is live at
        each end: added, updated, removed, or neither — live at neither, or
        at both with equal payloads (a source's versions). A neither is
        delivered to no consumer, and a merge that folds its versions away
        drops it from the page too: it may be listed, the others must be. A
        slice cannot tell what was live before it: there, the keys and their
        states at `last` are checked, not classes."""

        commits = self.commits(index.state)
        if commits is None:
            return
        self.checked += 1
        whole = self.whole(index.state)
        before, now = self.fold(commits, first), self.fold(commits, last + 1)

        def live(state, k):
            return k in state and not state[k][1]

        changed = {k for c, entries in commits if first <= c <= last for k in entries}
        if keys is not None:
            changed &= {bytes(k) for k in keys}
        changed = {k for k in changed if (after is None or k > after) and (until is None or k < until)}
        got = [bytes(k) for k in page.keys]
        args = (first, last, after, limit)

        def cls(k):
            was, is_ = live(before, k), live(now, k)
            same = was and is_ and before[k][2] is not None and before[k][2] == now[k][2]
            return 0 if is_ and not was else 2 if was and not is_ else 1 if is_ and not same else 3

        cut = got[-1] if got and page.cursor is not None else None  # where this page stops
        needed = sorted(
            k
            for k in changed
            if (cut is None or k <= cut) and (not whole and live(now, k) or whole and cls(k) != 3)
        )
        want = needed
        if got != sorted(got) or not set(got) <= changed or not set(needed) <= set(got):
            self._wrong(index, "changes", args, f"keys {got[:5]}, the fold {want[:5]}")
            return
        for i, k in enumerate(got):
            g, deleted, p = now[k]
            seen = (int(page.generations[i]), bool(page.deleted[i]))
            if seen != (g, deleted) or (not deleted and _payload(page.payloads[i]) != p):
                self._wrong(index, "changes", args, f"{k!r} at {seen}, the fold at {(g, deleted)}")
                return
            if not whole:
                continue  # its class needs what came before the slice
            if page.classes[i] != cls(k):
                self._wrong(index, "changes", args, f"{k!r} classed {page.classes[i]}, the fold {cls(k)}")
                return


def patches(reads: Reads) -> list[tuple]:
    """What `World._install` patches so that every install, merge and read
    at an endpoint goes through `reads`."""

    from solera.keys.index import IndexState, KeyIndex
    from solera_server.model import Model

    committed, merged, on_merged, apply = (
        IndexState.committed,
        IndexState.merged,
        Model._on_IndexMerged,
        Model.apply,
    )
    page, lookup, changes_page = KeyIndex.page, KeyIndex.lookup, KeyIndex.changes_page

    def committing(state, commit_number, delta):
        out = committed(state, commit_number, delta)
        reads.installed(state, commit_number, delta)
        return out

    def merging(state, inputs, out):
        reads.merged(state, inputs, out)
        return merged(state, inputs, out)

    def applying(model, e):
        if e["type"] == "AttemptLaunched":
            reads.launched(model, e)
        elif e["type"] in ("AttemptFinished", "RunArchived"):
            reads.ended(model, e)
        return apply(model, e)

    def merge_installed(model, e):
        key = (e["output"], e["partition"])
        before = model.indexes.get(key)
        endpoints = reads.holders(model, key) if before is not None else set()
        on_merged(model, e)
        if before is not None:
            reads.kept(key, before, model.indexes.get(key), endpoints)

    async def paging(index, after, limit, at=None):
        with reads.served(index, "page", (after, limit, at)):
            result = await page(index, after, limit, at)
        reads.page(index, after, limit, at, result)
        return result

    async def looking(index, keys, at=None):
        with reads.served(index, "lookup", (len(keys), at)):
            result = await lookup(index, keys, at)
        reads.lookup(index, keys, at, result)
        return result

    async def changing(index, first, last, after, limit, *, keys=None, until=None):
        with reads.served(index, "changes", (first, last, after, limit)):
            result = await changes_page(index, first, last, after, limit, keys=keys, until=until)
        reads.changes(index, first, last, after, limit, keys, until, result)
        return result

    return [
        (IndexState, "committed", committing),
        (IndexState, "merged", merging),
        (Model, "_on_IndexMerged", merge_installed),
        (Model, "apply", applying),
        (KeyIndex, "page", paging),
        (KeyIndex, "lookup", looking),
        (KeyIndex, "changes_page", changing),
    ]
