"""Pass progress (review round 3, system S1): one transition, `advance`,
over an explicit bookmark — a pass keeps its mode, boundary and page
plan until its last page, then `next` moves past it."""

from solera_server.bookmarks import advance, continues, needs, pins

CARRIED = {"output": "log", "upstream_partition": "", "fingerprint": "f", "reset_by": "r1"}


def test_a_batch_delivery_keeps_its_mode_to_its_boundary():
    full = {"mode": "full", "from": 0, "to": 2, "at": 0, "page": 0, "pages": 3}
    first = {
        "kind": "commits",
        "bookmark": {"kind": "commits", **CARRIED, "next": 0},
        "pass": full,
        "hi": 0,
    }
    wm = advance(first)
    assert wm["next"] == 0 and wm["pass"] == {**full, "at": 1, "page": 1}
    assert continues(first, None, wm)
    last = {**first, "pass": {**full, "at": 2, "page": 2}, "hi": 2}
    done = advance(last)
    assert done == {"kind": "commits", **CARRIED, "next": 3} and not continues(last, None, done)


def test_a_key_delivery_pages_by_key_then_moves_next():
    wm0 = {"kind": "keys", **CARRIED, "next": 5}
    full = {"mode": "full", "from": 8, "at": None, "page": 0, "pages": 2, "reconcile": True}
    plan = {"kind": "keys", "bookmark": wm0, "pass": full}
    wm = advance(plan, "k9")
    assert wm == {**wm0, "pass": {**full, "at": "k9", "page": 1}} and continues(plan, "k9", wm)
    assert needs(wm) == 8  # a full pass resumes as deltas from its `from`
    done = advance({**plan, "pass": wm["pass"]}, None)
    assert done == {**wm0, "next": 8, "reconcile": {"after": None}}  # an Each cleanup owed

    delta = {"mode": "delta", "from": 5, "to": 7, "at": None, "page": 0, "pages": 2, "pin": 40}
    paged = advance({"kind": "keys", "bookmark": wm0, "pass": delta}, "k1")
    assert paged["pass"]["at"] == "k1" and pins(paged) == [40] and needs(paged) == 5
    assert advance({"kind": "keys", "bookmark": wm0, "pass": delta}, None) == {**wm0, "next": 8}


def test_a_pattern_transition_ends_on_the_new_patterns():
    pattern_change = {"old": ["a/**"], "new": ["b/**"], "at": 4, "snapshot": {}, "pin": 12}
    wm0 = {"kind": "keys", **CARRIED, "next": 5, "patterns": ["a/**"], "pattern_change": pattern_change}
    diff = {"mode": "diff", "at": None, "page": 0, "pages": 1}
    assert pins(wm0) == [12]
    done = advance({"kind": "keys", "bookmark": wm0, "pass": diff}, None)
    assert done == {"kind": "keys", **CARRIED, "next": 5, "patterns": ["b/**"]} and pins(done) == []


def test_a_held_page_moves_no_watermark():
    kept = {"kind": "keys", **CARRIED, "next": 3}
    assert advance({"kind": "held", "bookmark": kept}) == kept
    assert advance({"kind": "held", "bookmark": None}) is None
