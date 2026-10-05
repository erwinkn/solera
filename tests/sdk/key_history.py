"""A key index over a history of commits, against a dict per commit: for
tests of what reads it (Δ, the observed set's planning)."""

from obstore.store import MemoryStore
from solera.keys import SortedEntries
from solera.keys.delta import Diff
from solera.keys.io import ObjectIO
from solera.keys.layers import LayerIndex, LayerState, key_bytes


class History:
    """Commits against a dict: `states[c]` is the dict, key -> generation,
    after commit c."""

    def __init__(self):
        self.io, self.states = ObjectIO(MemoryStore()), {}
        self.state = LayerState(prefix="keys/out/_/", life="l1")
        self.commit_number = 0

    def index(self) -> LayerIndex:
        return LayerIndex(self.io, self.state)

    async def commit(self, upserts, removes=()) -> None:
        gen = self.commit_number + 1
        removes = sorted(set(removes))
        keys = sorted(set(upserts) - set(removes))
        entries = SortedEntries.of([key_bytes(k) for k in keys], None, [key_bytes(k) for k in removes])
        files, _ = await self.index().write_patch(
            entries, name=f"{self.commit_number:012d}-a{gen}", generation=gen
        )
        self.state = self.state.committed(self.commit_number, files)
        after = {**self.states.get(self.commit_number - 1, {}), **dict.fromkeys(keys, gen)}
        for k in removes:
            after.pop(k, None)
        self.states[self.commit_number] = after
        self.commit_number += 1

    async def merge_all(self, cut: int = -1) -> None:
        """Merges under the rule until none is due, with flips kept from `cut` on."""

        self.state = self.state.with_cut(cut)
        while (plan := self.state.plan()) is not None:
            _, lo, count = plan
            ids, out = await self.index().merge(lo, count, epoch=1)
            self.state = self.state.merged(ids, out)

    def expected(self, p, h, keys=None, take=None) -> list[Diff]:
        old, new = ({} if p is None else self.states[p]), self.states[h]
        out = []
        for k in sorted(set(keys) if keys is not None else set(old) | set(new)):
            if (take is None or take(k)) and ((k in old) != (k in new) or old.get(k) != new.get(k)):
                out.append(Diff(k, k in old, k in new, new.get(k), None))
        return out
