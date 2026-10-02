"""What an attempt's inputs read (docs/stores.md, "What a read sees").

A snapshot store (FileStore, S3Store) reads the version the engine pinned,
exactly. A store that reads the current rows (`reads`, PostgresStore) may
read a newer one: its loads go through one reader per store — one moment,
the inputs read together — and each says which generation's write it saw.
When that is not the pinned head's generation, the keys a page read are
versioned as they were read, so lineage holds what was read, not what was
pinned. Only then: a read of the pinned generation costs nothing more."""

from __future__ import annotations

import contextlib
from typing import Any

from solera.sdk import is_ref_type
from solera.stores import Keys, prepare_for


class Observed:
    def __init__(self, spec: dict, project):
        self.project = project
        self.pinned: dict[tuple[str, str], int] = {}
        for pin in spec["inputs"].values():
            if pin.get("ref") and pin.get("generation") is not None:
                ref = pin["ref"]
                self.pinned[(ref["output"], ref.get("partition") or "")] = int(pin["generation"])
            for key, ref in (pin.get("refs") or {}).items():
                if (pin.get("generations") or {}).get(key) is not None:
                    self.pinned[(ref["output"], ref.get("partition") or "")] = int(pin["generations"][key])
        self.read: dict[tuple[str, str], dict] = {}
        self._stack = contextlib.AsyncExitStack()
        self._readers: dict[str, Any] = {}

    async def load(self, store, ref, t, selection):
        if not callable(getattr(store, "reads", None)) or is_ref_type(t):
            return await store.load(ref, t, selection)
        reader = self._readers.get(ref.store)
        if reader is None:
            reader = self._readers[ref.store] = await self._stack.enter_async_context(store.reads())
        value, generation = await reader.load(ref, t, selection)
        self._saw(store, ref, value, selection, generation)
        return value

    async def close(self) -> None:
        """The moment ends: the readers' transactions close. A load after
        opens another."""

        self._readers = {}
        stack, self._stack = self._stack, contextlib.AsyncExitStack()
        await stack.aclose()

    def report(self) -> list[dict]:
        """For the result: per slice read, the generation it saw, and the
        versions of the keys a page read when that was not the pinned one."""

        return list(self.read.values())

    def _saw(self, store, ref, value, selection, generation: int | None) -> None:
        slice_ = (ref.output, ref.partition or "")
        entry = self.read.get(slice_)
        if entry is not None:
            if entry.get("generation") != generation:  # two moments saw two versions: claim neither
                entry.update(generation=None, mixed=True)
                entry.pop("keys", None)
            return
        entry = self.read[slice_] = {"output": ref.output, "scope": slice_[1], "generation": generation}
        if generation is None or generation == self.pinned.get(slice_) or not isinstance(selection, Keys):
            return
        decl = self._declared(ref.output)
        if decl is None or decl.key is None:
            return
        try:
            versions = dict(prepare_for(store, value, decl).entries())
        except Exception:  # a form the store cannot version back: the generation alone
            return
        # A key the page asked for and did not find did not exist at that generation.
        entry["keys"] = {k: (versions[k].hex() if k in versions else None) for k in selection.revisions}

    def _declared(self, output: str):
        for asset in self.project.assets.values():
            for o in asset.outputs:
                if (o.name or asset.name) == output:
                    return o
        return None
