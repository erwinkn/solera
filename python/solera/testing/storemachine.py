"""Generated conformance for a store: random sequences of what the engine and
its workers do to one output, checked after every step against what the
engine's key index would hold (docs/stores.md, "The invariants"). The named
scenarios of `solera.testing.stores` are the readable spec; this machine
looks for the sequences they do not spell out. It needs Hypothesis:

    from solera.testing.storemachine import stateful

    TestMyStore = stateful(lambda: Worker(MyStore(dsn), fresh_output)).TestCase

One run is one output — `Harness.output` gives a fresh one per run — and
the machine plays the engine for it:

- **attempts.** `begin` starts the next attempt, its generation larger than
  every one before (a fenced store's `acquire` comes first, as the engine
  guarantees). An attempt writes a replacement, a patch or a replacement
  resolved against the index (its keys, and removes of the rest), and the
  write commits or is abandoned (its worker died, its answer was lost). An
  attempt makes one write, which may be retried: a name carries its
  generation, so it never writes a key twice with other content. An earlier
  attempt may write late (a stale writer) and a second worker of the
  current one may try (a duplicate).
- **readers.** A reader pins the committed content and reads it later;
  a by-key reader loads `dict[str, T]` under `Keys`, as per-key incremental does.
- **collection** (immutable stores). The names no committed index and no
  pinned reader references — superseded, abandoned, or never written —
  are cleaned up, twice over.

After every step: the committed content reads back exactly (an immutable
store through the index's keys; a fenced one whole, but for rows a dead
writer left that the next commit settles); every pinned reader reads what
it pinned. Unlike the scenarios, a step here assumes no particular store:
what it expects follows from the kind."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass

from ..sdk import Ref
from ..stores import KeyedWrite, Keys, Patch, StoreError, in_place, prepare_for
from .stores import Harness, Ledger, context, said

KEYS = ["a", "b", "c", "d"]


@dataclass
class Attempt:
    generation: int
    worker_id: str
    last: tuple | None = None  # its write call, to retry: (keyed write, prior, written ref)
    fenced: bool = False  # it holds the partition: acquired once the partition existed, or wrote


@dataclass
class Pin:
    ref: Ref
    entries: dict[str, int]  # key -> generation, as the index held them
    content: dict[str, str]  # key -> its `v`, as committed


def stateful(make_harness: Callable[[], Harness]):
    """A Hypothesis `RuleBasedStateMachine` class over the store
    `make_harness()` gives, one worker per run."""

    from hypothesis import strategies as st
    from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

    class StoreMachine(RuleBasedStateMachine):
        def __init__(self):
            super().__init__()
            self.loop = asyncio.new_event_loop()
            self.h = make_harness()
            self.store = self.h.store
            self.kind = "fenced" if in_place(self.store) else "immutable"
            self.out = self.h.output(key="id")
            self.ledger = Ledger()  # what the index holds
            self.content: dict[str, str] = {}  # each committed key's `v`
            self.head: Ref | None = None
            self.generation = 0
            self.current: Attempt | None = None
            self.stale: list[Attempt] = []
            self.dirty: set[str] = set()  # keys a dead writer may have changed in place (fenced)
            self.exists = False  # the partition exists in the store: something wrote it
            self.fence = 0  # the newest generation holding the partition (fenced)
            self.written: set[tuple[str, int]] = set()  # (key, generation) names ever written
            self.pins: list[Pin] = []

        def teardown(self):
            self.loop.close()

        def run(self, coro):
            return self.loop.run_until_complete(coro)

        # -- attempts ---------------------------------------------------------------------

        @rule()
        def begin(self):
            """The engine starts the next attempt on the partition."""

            if self.current is not None:
                self.stale.append(self.current)
            self.generation += 1
            self.current = Attempt(self.generation, f"i{self.generation}")
            if self.kind == "fenced":
                self.run(self.store.written_at([], self.head, self._context(self.current)))
                if self.exists:  # a partition not written yet is acquired by its first write
                    self.current.fenced, self.fence = True, self.generation

        @precondition(lambda self: self.current is not None and self.current.last is None)
        @rule(
            kind=st.sampled_from(["replace", "patch", "changed"]),
            keys=st.sets(st.sampled_from(KEYS), max_size=3),
            removes=st.sets(st.sampled_from(KEYS), max_size=2),
            version=st.sampled_from(["1", "2", "3"]),
            commits=st.booleans(),
        )
        def write(self, kind, keys, removes, version, commits):
            """The current attempt writes; the write commits, or its attempt
            dies after it (an abandoned write, which the next one settles)."""

            attempt = self.current
            if self.kind == "fenced" and self.dirty and kind == "patch":
                kind = "replace"  # what a dead writer left is settled by the next whole write here
            rows = [{"id": k, "v": version} for k in sorted(keys)]
            keyed, upserts, gone, whole = self._resolve(kind, rows, sorted(removes - keys))
            if not upserts and not gone and not (whole and self.ledger.entries):
                return  # nothing changes against the index: the worker stores nothing
            prior = self.head
            written = self.run(self.store.store(keyed, prior, self._context(attempt)))
            want = (sorted(upserts, key=str.encode), sorted(gone, key=str.encode), whole)
            if said(written) != want:
                raise AssertionError(f"a write said it changed {said(written)}; it changed {want}")
            self.exists, attempt.fenced, self.fence = True, True, max(self.fence, attempt.generation)
            attempt.last = (keyed, prior, written.ref)
            touched = set(upserts) | set(gone) | (set(self.ledger.entries) if whole else set())
            self.written |= {(key, attempt.generation) for key in upserts}
            if not commits:
                self.current = None  # its worker died: the next attempt follows
                if self.kind == "fenced":
                    self.dirty |= touched
                return
            if whole:
                self.ledger.entries.clear()
                self.content.clear()
            for key in gone:
                self.ledger.entries.pop(key, None)
                self.content.pop(key, None)
            self.ledger.entries.update(dict.fromkeys(upserts, attempt.generation))
            self.content.update(upserts)
            self.head = written.ref
            self.dirty -= touched

        @precondition(lambda self: self.current is not None and self.current.last is not None)
        @rule()
        def retry(self):
            """The current attempt's last call, sent again: the same content,
            the same ref."""

            keyed, prior, first = self.current.last
            again = self.run(self.store.store(keyed, prior, self._context(self.current)))
            if again.ref != first:
                raise AssertionError(f"a retried write gave {again.ref}, the first {first}")

        @precondition(lambda self: self.stale)
        @rule(which=st.integers(0, 10), version=st.sampled_from(["0", "9"]), patch=st.booleans())
        def stale_write(self, which, version, patch):
            """An attempt the engine gave up on writes late — its one write, or
            that write once more: a fenced store refuses it once a newer
            attempt holds the partition, an immutable one lets it write names
            nobody reads. (Before the partition exists nothing may hold it: the
            stale write may land, uncommitted, and the next commit, a first
            write, replaces it whole.)"""

            attempt = self.stale[which % len(self.stale)]
            if attempt.last is not None and self.kind == "immutable":
                keyed, prior, _ = attempt.last  # it wrote already: the same call, late
                self.run(self.store.store(keyed, prior, self._context(attempt)))
                return
            rows = [{"id": k, "v": version} for k in KEYS[:2]]
            keyed, upserts, _, _ = self._resolve("patch" if patch else "replace", rows, [])
            attempt.last = (keyed, self.head, None)
            if self.kind == "fenced":
                if attempt.generation < self.fence:
                    with _refused("a stale writer"):
                        self.run(self.store.store(keyed, self.head, self._context(attempt)))
                    return
                try:  # nothing holds the partition yet: refusing early is as good
                    self.run(self.store.store(keyed, self.head, self._context(attempt)))
                except StoreError:
                    return
                self.exists, attempt.fenced, self.fence = True, True, attempt.generation
                self.dirty |= set(upserts)
                return
            self.run(self.store.store(keyed, self.head, self._context(attempt)))
            self.written |= {(key, attempt.generation) for key in upserts}

        @precondition(lambda self: self.kind == "fenced" and self.current is not None and self.current.fenced)
        @rule()
        def duplicate(self):
            """A second worker of the current attempt: refused, both to
            acquire and to write, once the attempt holds the partition."""

            twin = Attempt(self.current.generation, self.current.worker_id + "-twin")
            with _refused("a duplicate worker's acquire"):
                self.run(self.store.written_at([], self.head, self._context(twin)))
            rows = [{"id": "a", "v": "0"}]
            keyed, _, _, _ = self._resolve("patch", rows, [])
            with _refused("a duplicate worker's write"):
                self.run(self.store.store(keyed, self.head, self._context(twin)))

        # -- readers and collection ---------------------------------------------------

        @precondition(lambda self: self.kind == "immutable" and self.head is not None)
        @rule()
        def pin(self):
            """A reader pins the committed content, to read it later."""

            self.pins.append(Pin(self.head, dict(self.ledger.entries), dict(self.content)))

        @precondition(lambda self: self.pins)
        @rule(which=st.integers(0, 10))
        def unpin(self, which):
            self.pins.pop(which % len(self.pins))

        @precondition(lambda self: self.kind == "immutable" and self.head is not None)
        @rule(take=st.integers(0, 2**16), twice=st.booleans())
        def cleanup(self, take, twice):
            """Collection: names no index and no pinned reader references —
            superseded, abandoned or never written — are cleaned up."""

            live = set(self.ledger.entries.items())
            for pin in self.pins:
                live |= set(pin.entries.items())
            unused = sorted(self.written - live)
            chosen = [n for i, n in enumerate(unused) if take >> (i % 16) & 1]
            versions = [*chosen, ("zz", 99_999)]  # and one never written
            for _ in range(2 if twice else 1):
                for k, g in versions:
                    self.run(self.store.cleanup(self.out, partition="", key=k, generation=g))

        @precondition(lambda self: self.head is not None and self.ledger.entries)
        @rule(keys=st.sets(st.sampled_from(KEYS), min_size=1))
        def read_by_key(self, keys):
            """A per-key incremental read: `dict[str, T]` under `Keys`, each key's group."""

            selection = Keys(
                {k: g for k, g in self.ledger.entries.items() if k in keys and k not in self.dirty}
            )
            if not selection.generations:
                return
            found = self.run(self.store.read(self.head, dict[str, list[dict]], selection))
            got = {k: sorted(str(r["v"]) for r in group) for k, group in found.value.items()}
            want = {k: [self.content[k]] for k in selection.generations}
            if got != want:
                raise AssertionError(f"a by-key load gave {got}; the index holds {want}")
            if dict(found.versions) != selection.generations:
                raise AssertionError(
                    f"a by-key load said versions {found.versions}; the index holds {selection}"
                )

        # -- invariants -------------------------------------------------------------------

        @invariant()
        def committed_content_reads_back(self):
            if self.head is None:
                return
            want = sorted(self.content.items())
            versions = dict(self.ledger.entries)
            if self.kind == "immutable":
                got, read = self._read(self.head, self.ledger.keys())
            else:
                got, read = self._read(self.head, None)
                got = [p for p in got if p[0] not in self.dirty]
                want = [p for p in want if p[0] not in self.dirty]
                read = {k: v for k, v in read.items() if k not in self.dirty}
                versions = {k: v for k, v in versions.items() if k not in self.dirty}
            if got != want:
                raise AssertionError(f"the committed content reads {got}; the index holds {want}")
            if read != versions:
                raise AssertionError(
                    f"the committed content reads at versions {read}; the index holds {versions}"
                )

        @invariant()
        def pinned_readers_read_what_they_pinned(self):
            for pin in self.pins:
                got, read = self._read(pin.ref, Keys(dict(pin.entries)))
                want = sorted(pin.content.items())
                if got != want or read != pin.entries:
                    raise AssertionError(
                        f"a pinned reader reads {got} at {read}; it pinned {want} at {pin.entries}"
                    )

        # -- helpers ----------------------------------------------------------------------

        def _context(self, attempt: Attempt):
            return context(self.out, attempt.generation, attempt.worker_id)

        def _resolve(self, kind: str, rows: list[dict], removes: list[str]):
            """A write as the worker hands it to the store, resolved against
            the index — every key it writes is a change (docs/versions.md):
            `(KeyedWrite, upserts {key: v}, removed keys, whole)`."""

            entries = self.ledger.entries
            upserts = {r["id"]: r["v"] for r in rows}
            if kind == "replace" or self.head is None:
                prepared = prepare_for(self.store, rows, self.out)
                return KeyedWrite(prepared, reset=True, value=rows), upserts, set(), True
            if kind == "patch":
                prepared = prepare_for(self.store, Patch(rows, remove=removes), self.out)
                gone = {k for k in removes if k in entries}
                keyed = KeyedWrite(prepared, upserts=frozenset(upserts), removes=frozenset(gone), value=rows)
                return keyed, upserts, gone, False
            prepared = prepare_for(self.store, rows, self.out)
            gone = set(entries) - set(upserts)
            keyed = KeyedWrite(prepared, upserts=frozenset(upserts), removes=frozenset(gone), value=rows)
            return keyed, upserts, gone, False

        def _read(self, ref: Ref, selection) -> tuple[list[tuple[str, str]], dict]:
            """The rows `read` gives, and the version it says each key was read at."""

            found = self.run(self.store.read(ref, list[dict], selection))
            return sorted((str(r["id"]), str(r["v"])) for r in found.value), dict(found.versions)

    return StoreMachine


@contextlib.contextmanager
def _refused(what: str):
    try:
        yield
    except StoreError:
        return
    raise AssertionError(f"the store accepted {what}, which it must refuse")
