"""Reading a source through its loader (docs/stores.md, "Sources: how data is
loaded"): its function (`@source`), or its store's `serve`. Either says, of
every key it serves, the version it served it at; a key it leaves out was
observed absent."""

from __future__ import annotations

import inspect
from collections.abc import Mapping

from solera.sdk import Loaded, Source, is_ref_type
from solera.stores import MISSING, Keys, StoreError, by_key_type, frames


class SourceContext:
    """A loader's `ctx`: the source it loads, and the partition."""

    def __init__(self, source: str, partition: str):
        self.source, self.partition = source, partition


class SourceLoader:
    """A source read through its loader, shaped as a store's `load`: its rows
    as the parameter's type asks, and in `served` the version each key was
    served at (None: it had none), across every load of this reader."""

    def __init__(self, source: Source, store=None):
        self.source, self.store, self.served = source, store, {}

    async def load(self, ref, t, selection):
        if is_ref_type(t):
            return ref
        keys = sorted(selection.generations) if isinstance(selection, Keys) else None
        ctx = SourceContext(self.source.name, ref.partition or "")
        load = self.source.loader if self.source.loader is not None else self.store.serve
        answer = load(keys, ctx) if self.source.loader is not None else load(self.source, keys, ctx)
        if inspect.isawaitable(answer):
            answer = await answer
        if self.source.key is None:
            return answer.value if isinstance(answer, Loaded) else answer
        if not isinstance(answer, Mapping):
            raise StoreError(
                f"source {self.source.name!r}: a keyed loader returns {{key: Loaded(row, version=…)}}"
            )
        groups: dict[str, list] = {}
        for key in answer if keys is None else keys:
            loaded = answer.get(key)
            if loaded is None:  # observed absent
                self.served[key] = None
                continue
            if not isinstance(loaded, Loaded) or loaded.version is None:
                raise StoreError(
                    f"source {self.source.name!r}: key {key!r} came without the version it was served at: "
                    "return Loaded(row, version=…)"
                )
            self.served[key] = str(loaded.version)
            groups[key] = loaded.value if isinstance(loaded.value, list) else [loaded.value]
        inner = by_key_type(t)
        if inner is not MISSING:  # dict[str, T]: each key's group, as T
            return {k: frames.materialize(rows, inner) for k, rows in groups.items()}
        return frames.materialize([row for rows in groups.values() for row in rows], t)


def reader(project, ref):
    """Where an input's data comes from: a source's loader, or its store."""

    source, store = project.sources.get(ref.output), project.stores.get(ref.store)
    if source is not None and (source.loader is not None or callable(getattr(store, "serve", None))):
        return SourceLoader(source, store)
    return project.stores[ref.store]
