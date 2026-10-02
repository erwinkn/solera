"""Delivery progress (review round 3, system S1): one transition, `advance`,
for every kind of plan — a delivery keeps its mode and page plan until its
last page."""

from solera_server.delivery import advance, continues

BASE = {"output": "log", "up": "", "fingerprint": "f", "pass": "r1"}


def test_a_batch_delivery_keeps_its_mode_to_its_last_page():
    first = {"kind": "batches", **BASE, "lo": 0, "hi": 0, "head": 2, "full": True, "page": 0, "pages": 3}
    wm = advance(first)
    assert wm == {**BASE, "batch": 1, "after": None, "full": True, "page": 1, "pages": 3}
    assert continues(first, None, wm)
    last = {**first, "lo": 2, "hi": 2, "page": 2}
    assert advance(last) == {**BASE, "batch": 3, "after": None, "full": False}  # done: a delta next
    assert not continues(last, None, advance(last))


def test_a_key_delivery_pages_by_key():
    full = {"kind": "keys", **BASE, "full": True, "from": 5, "after": None, "page": 0, "pages": 2}
    wm = advance(full, "k9")
    assert wm == {**BASE, "batch": 5, "after": "k9", "full": True, "page": 1, "pages": 2}
    assert continues(full, "k9", wm)
    assert advance({**full, "page": 1}, None) == {**BASE, "batch": 5, "after": None, "full": False}
    delta = {"kind": "keys", **BASE, "full": False, "from": 5, "to": 7, "after": None, "pin": 40}
    assert advance(delta, "k1") == {**BASE, "batch": 5, "until": 7, "after": "k1", "full": False, "pin": 40}
    assert advance(delta, None) == {**BASE, "batch": 8, "after": None, "full": False}


def test_a_held_page_moves_no_watermark_and_old_plans_still_settle():
    kept = {**BASE, "batch": 3, "after": None, "full": False}
    assert advance({"kind": "held", "watermark": kept}) == kept
    assert advance({"kind": "held", "watermark": None}) is None
    # Prepared before kinds were named, settled after an upgrade.
    assert advance({"update": kept, "more": False}) == kept
    assert advance({**BASE, "full": False, "from": 5, "to": 7, "after": None}, None)["batch"] == 8
