"""The observed set's planning over Δ (solera_server.owed): random histories
of upstream commits, runs walking their batches from their progress (some
of them keys= lists or "all"), commits landing between batches, and the
patterns and context changing between runs. Each batch's classes update
the literal observed set; after every batch commit the record decodes to
it, and a run that saw no commit meanwhile leaves nothing owed and the set
equal to upstream (docs/observed-set.md)."""

import random

import pytest
from solera.patterns import Matcher
from solera_server import observed, owed

from tests.sdk.key_history import History

KEYS = [f"k{i:02d}" for i in range(30)]
PATTERNS = [
    None,
    {"include": [{"glob": "k1*"}, {"glob": "k2*"}]},
    {"exclude": [["odd", {"glob": "k*[13579]"}]]},
]


async def commit(h: History, rng: random.Random) -> None:
    live = list(h.states.get(h.commit_number - 1, {}))
    upserts = rng.sample(KEYS, rng.randrange(1, 8))
    await h.commit(upserts, rng.sample(live, min(len(live), rng.randrange(0, 4))))


def check(h: History, rec: dict, truth: dict) -> None:
    def at(endpoint, key):
        return h.states[endpoint].get(key)

    assert {k: observed.decode(rec, k, at) for k in KEYS if observed.decode(rec, k, at)} == truth


@pytest.mark.parametrize("seed", range(30))
async def test_runs_keep_the_record_equal_to_the_observed_set(seed):
    rng = random.Random(seed)
    h = History()
    rec, truth = observed.record("life"), {}
    patterns, context = None, {"factor": "w1"}
    for _ in range(3):
        await commit(h, rng)
    for _ in range(8):
        if rng.random() < 0.3:
            patterns = rng.choice(PATTERNS)
        if rng.random() < 0.2:
            context = {"factor": rng.choice(["w1", "w2"])}
        mode = rng.choice([None, None, None, "all", sorted(rng.sample(KEYS, 5))])
        size, progress, quiet = rng.choice([1, 3, 7, 100]), None, True
        while True:
            now = owed.Now(h.commit_number - 1, patterns, context, "life")
            b = await owed.batch(h.index(), rec, now, size, after=progress, keys=mode)
            ops = await owed.commit_ops(h.index(), rec, now, b, named=isinstance(mode, list))
            observed.apply(rec, ops)
            for o in b.keys:
                if o.cls == "removed":
                    truth.pop(o.key, None)
                else:
                    truth[o.key] = (o.new, context)
            check(h, rec, truth)
            if b.final:
                break
            progress = b.end
            if rng.random() < 0.3:  # the upstream moves on mid-run
                await commit(h, rng)
                quiet = False
        if mode is None and quiet:
            now = owed.Now(h.commit_number - 1, patterns, context, "life")
            assert await owed.owed(h.index(), rec, now) == []
            take = Matcher(patterns)
            assert truth == {k: (g, context) for k, g in h.states[h.commit_number - 1].items() if take(k)}
        if rng.random() < 0.7:
            await commit(h, rng)
    # A last default run, nothing committed meanwhile: whatever the history, it settles.
    now, progress = owed.Now(h.commit_number - 1, patterns, context, "life"), None
    while True:
        b = await owed.batch(h.index(), rec, now, 5, after=progress)
        observed.apply(rec, await owed.commit_ops(h.index(), rec, now, b))
        for o in b.keys:
            if o.cls == "removed":
                truth.pop(o.key, None)
            else:
                truth[o.key] = (o.new, context)
        check(h, rec, truth)
        if b.final:
            break
        progress = b.end
    assert await owed.owed(h.index(), rec, now) == []
    take = Matcher(patterns)
    assert truth == {k: (g, context) for k, g in h.states[h.commit_number - 1].items() if take(k)}
    assert rec["ranges"] == [] and rec["base"]["endpoint"] == now.head  # folded into one base


async def test_a_batch_takes_its_size_of_owed_keys_and_covers_up_to_the_last():
    h = History()
    await h.commit([f"k{i}" for i in range(6)])
    rec = observed.record("life")
    now = owed.Now(0, None, {}, "life")
    b = await owed.batch(h.index(), rec, now, 4)
    assert [o.key for o in b.keys] == ["k0", "k1", "k2", "k3"] and b.end == "k3" and not b.final
    assert {o.cls for o in b.keys} == {"added"}
    observed.apply(rec, await owed.commit_ops(h.index(), rec, now, b))
    b = await owed.batch(h.index(), rec, now, 4, after=b.end)
    assert [o.key for o in b.keys] == ["k4", "k5"] and b.end is None and b.final


async def test_a_named_key_held_as_it_is_is_unchanged():
    h = History()
    await h.commit(["k1", "k2"])
    rec = observed.record("life")
    now = owed.Now(0, None, {}, "life")
    observed.apply(rec, await owed.commit_ops(h.index(), rec, now, await owed.batch(h.index(), rec, now, 10)))
    b = await owed.batch(h.index(), rec, now, 10, keys=["k1", "k9"])
    assert [(o.key, o.cls) for o in b.keys] == [("k1", "unchanged")]  # k9: absent, never held
