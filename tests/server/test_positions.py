"""Pass progress (review round 3, system S1): one transition, `advance`,
over an explicit position — a pass keeps its mode, boundary and batch
plan until its last batch, then `next` moves past it."""

from solera_server.positions import advance, continues, pins

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
