"""A key index over a history of commits, against a dict per commit: for
tests of what reads it (Δ, the observed set's planning)."""

from obstore.store import MemoryStore
from solera.keys import SortedEntries
from solera.keys.delta import Diff
from solera.keys.index import IndexState, KeyIndex, Options, key_bytes
from solera.keys.io import ObjectIO


class History:
    """Commits against a dict: `states[c]` is the dict, key -> generation,
    after commit c."""

    def __init__(self):
        self.io, self.state, self.states = ObjectIO(MemoryStore()), IndexState(), {}
        self.commit_number = 0

    def index(self) -> KeyIndex:
        return KeyIndex(self.io, "keys/out/_", self.state, Options(block_size=512, max_file_bytes=4096))

    async def commit(self, upserts, removes=()) -> None:
        idx, gen = self.index(), self.commit_number + 1
        removes = sorted(set(removes))
        keys = sorted(set(upserts) - set(removes))
        entries = SortedEntries.of([key_bytes(k) for k in keys], None, [key_bytes(k) for k in removes])
        files, _ = await idx.resolve(
            entries, commit_number=self.commit_number, attempt=f"a{gen}", generation=gen, collect=10**6
        )
        self.state = self.state.committed(self.commit_number, files)
        after = {**self.states.get(self.commit_number - 1, {}), **dict.fromkeys(keys, gen)}
        for k in removes:
            after.pop(k, None)
        self.states[self.commit_number] = after
        self.commit_number += 1

    async def merge_all(self, endpoints: set[int]) -> None:
        idx = self.index()
        while (plan := idx.plan_merge(endpoints)) is not None:
            out = await idx.merge(plan, endpoints)
            if out is None:
                return
            self.state = self.state.merged(out.inputs, out.span)
            idx = self.index()

    def expected(self, p, h, keys=None, take=None) -> list[Diff]:
        old, new = ({} if p is None else self.states[p]), self.states[h]
        out = []
        for k in sorted(set(keys) if keys is not None else set(old) | set(new)):
            if (take is None or take(k)) and ((k in old) != (k in new) or old.get(k) != new.get(k)):
                out.append(Diff(k, k in old, k in new, new.get(k), None))
        return out
