"""Pass progress (review round 3, system S1): one transition, `advance`,
over an explicit position — a pass keeps its mode, boundary and batch
plan until its last batch, then `next` moves past it."""

from solera_server.positions import advance, continues, pins, reads

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
    assert continues(first, None, position)
    last = {**first, "pass": {**full, "at": 2, "batch": 2}, "hi": 2}
    done = advance(last)
    assert done == {"kind": "commits", **CARRIED, "next": 3} and not continues(last, None, done)


def test_a_keyed_pass_goes_by_key_then_moves_next():
    wm0 = {"kind": "keys", **CARRIED, "next": 5}
    full = {"mode": "full", "from": 8, "at": None, "batch": 0, "batches": 2, "reconcile": True}
    plan = {"kind": "keys", "position": wm0, "pass": full}
    position = advance(plan, "k9")
    assert position == {**wm0, "pass": {**full, "at": "k9", "batch": 1}} and continues(plan, "k9", position)
    done = advance({**plan, "pass": position["pass"]}, None)
    assert done == {**wm0, "next": 8, "reconcile": {"after": None}}  # a per-key cleanup owed

    delta = {"mode": "delta", "from": 5, "to": 7, "at": None, "batch": 0, "batches": 2, "pin": 40}
    paged = advance({"kind": "keys", "position": wm0, "pass": delta}, "k1")
    assert paged["pass"]["at"] == "k1" and pins(paged) == [40]
    assert advance({"kind": "keys", "position": wm0, "pass": delta}, None) == {**wm0, "next": 8}


def test_a_pattern_transition_ends_on_the_new_patterns():
    pattern_change = {"old": ["a/**"], "new": ["b/**"], "at": 4, "snapshot": {}, "pin": 12}
    wm0 = {"kind": "keys", **CARRIED, "next": 5, "patterns": ["a/**"], "pattern_change": pattern_change}
    diff = {"mode": "diff", "at": None, "batch": 0, "batches": 1}
    assert pins(wm0) == [12]
    done = advance({"kind": "keys", "position": wm0, "pass": diff}, None)
    assert done == {"kind": "keys", **CARRIED, "next": 5, "patterns": ["b/**"]} and pins(done) == []


def test_a_held_batch_moves_no_position():
    kept = {"kind": "keys", **CARRIED, "next": 3}
    assert advance({"kind": "held", "position": kept}) == kept
    assert advance({"kind": "held", "position": None}) is None


def test_every_plan_that_may_move_a_position_reserves_where_it_lands():
    """A17: what a claim reserves of its upstream index is what its plan may
    move the position to. A retry pass that may cover what is left lands at
    its head + 1 (R5): without that endpoint a merge meanwhile forces a full
    pass. A keys= selection while a pattern change decides membership has
    no position (R4), and a retry that cannot cover has no head: neither
    reserves anything."""

    position = {"output": "items", "upstream_partition": "", "next": 3}
    assert reads({"item": {"kind": "held", "position": position, "head": 5}}) == [("items", "", 3, 6)]
    assert reads({"item": {"kind": "held", "position": position}}) == []
    assert reads({"item": {"kind": "selection", "position": None, "head": 5}}) == []
