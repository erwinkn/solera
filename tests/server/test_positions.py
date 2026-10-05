"""An unkeyed input's pass progress (review round 3, system S1): one
transition, `advance`, over an explicit position — a pass keeps its mode,
boundary and batch plan until its last batch, then `next` moves past it.
And what a keyed batch reserves: its head."""

from solera_server.positions import advance, continues, reads

CARRIED = {"output": "log", "upstream_partition": "", "fingerprint": "f", "reset_by": "r1"}


def test_a_batch_delivery_keeps_its_mode_to_its_boundary():
    full = {"mode": "full", "from": 0, "to": 2, "at": 0, "batch": 0, "batches": 3}
    first = {
        "kind": "commits",
        "position": {"kind": "commits", **CARRIED, "next": 0},
        "pass": full,
        "hi": 0,
    }
    position = advance(first)
    assert position["next"] == 0 and position["pass"] == {**full, "at": 1, "batch": 1}
    assert continues(first, position)
    last = {**first, "pass": {**full, "at": 2, "batch": 2}, "hi": 2}
    done = advance(last)
    assert done == {"kind": "commits", **CARRIED, "next": 3} and not continues(last, done)


def test_a_keyed_batch_reserves_the_head_it_classed_at():
    plan = {"kind": "observed", "output": "items", "upstream_partition": "", "head": 5}
    assert reads({"item": plan}) == [("items", "", 5)]
    assert reads({"log": {"kind": "commits"}, "none": None}) == []
