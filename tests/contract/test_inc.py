"""INC: incremental delivery and classification (docs/behaviors.md)."""

from tests.contract.example import Example


async def test_inc_1_a_first_run_delivers_every_upstream_key_as_added(solera):
    ex = await Example(solera).start(batch_size=2)
    await ex.feed({f"k{i}": "1" for i in range(1, 6)})
    batches = await ex.run("tally")
    assert [b.added for b in batches] == [["k1", "k2"], ["k3", "k4"], ["k5"]]
    assert not any(b.updated or b.removed for b in batches)
    assert await ex.count() == 5


async def test_inc_2_a_keys_class_is_the_net_difference_with_upstream_now(solera):
    ex = await Example(solera).start()
    await ex.feed({"k1": "1", "k2": "1"})
    await ex.run("tally")
    await ex.feed({"k1": "2", "k3": "1"}, removes=["k2"])
    [batch] = await ex.run("tally")
    assert batch.classes == (["k3"], ["k1"], ["k2"])
    assert batch.rows == ["k1", "k3"]
    assert await ex.count() == 2
