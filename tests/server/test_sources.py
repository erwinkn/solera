"""D147: a source's data is loaded through a function (`@source`) or its
store's `serve`, which says the version each key was served at; the batch
carries it as `served` (docs/stores.md, "Sources: how data is loaded")."""

from solera.sdk import Loaded, Ref, source


async def test_a_key_the_loader_leaves_out_was_observed_absent():
    from solera.stores import Keys
    from solera_worker.sources import SourceLoader

    @source(key="id")
    async def files(keys, ctx):
        return {"a": Loaded({"id": "a"}, version="e1")}

    reader = SourceLoader(files)
    rows = await reader.load(Ref("files", "default", {}, ""), list, Keys({"a": 1, "b": 2}))
    assert rows == [{"id": "a"}] and reader.served == {"a": "e1", "b": None}
