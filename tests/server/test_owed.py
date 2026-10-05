"""The observed set's planning over Δ (solera_server.owed): random histories
of upstream commits, runs walking their batches from their progress (some
of them keys= lists or "all"), commits landing between batches, and the
patterns and context changing between runs. Each batch's classes update
the literal observed set; after every batch commit the record decodes to
it — a key changed since it was observed at a version since replaced
(None), the index keeping no older ones — and a run that saw no commit
meanwhile leaves nothing owed and the set equal to upstream
(docs/observed-set.md)."""

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


def at_h(index, h: int):
    """`index`, asserting its lookups are at H, `h`: a key view at an older
    commit is one the index does not serve (A31 R2) — a lookup reads its
    state's head, and Δ reads the state at H."""

    lookup = index.lookup

    async def checked_lookup(keys):
        assert index.state.head == h, f"a lookup at {index.state.head}, under H {h}"
        return await lookup(keys)

    index.lookup = checked_lookup
    return index


def check(h: History, rec: dict, truth: dict) -> None:
    head = h.states[h.commit_number - 1]

    def at(endpoint, key):  # as the index answers: no version at an older commit
        then = h.states[endpoint].get(key)
        return then if then == head.get(key) else (observed.OLDER if then is not None else None)

    decoded = {k: found for k in KEYS if (found := observed.decode(rec, k, at)) is not None}
    assert decoded.keys() == truth.keys()
    for k, (version, context) in decoded.items():
        assert context == truth[k][1], k
        assert version == truth[k][0] or (version is None and truth[k][0] != head.get(k)), k


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
            b = await owed.batch(at_h(h.index(), now.head), rec, now, size, after=progress, keys=mode)
            if rng.random() < 0.3:  # the upstream moves on while the batch runs
                await commit(h, rng)
                quiet = False
            ops = await owed.commit_ops(at_h(h.index(), now.head), rec, now, b, named=isinstance(mode, list))
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


async def test_a_held_base_owes_each_held_key_an_update_or_a_removal():
    """A per-key consumer's full run compares against what it holds: a key
    there is present at no upstream version — updated if upstream has it,
    removed if not — and every other upstream key is added."""

    upstream, held = History(), History()
    await upstream.commit(["k1", "k2"])
    await held.commit(["k2", "k3"])
    rec = observed.record("life", held=True)
    now = owed.Now(0, None, {}, "life")
    owes = await owed.owed(upstream.index(), rec, now, held=[held.index()])
    assert [(o.key, o.cls) for o in owes] == [("k1", "added"), ("k2", "updated"), ("k3", "removed")]
    b = await owed.batch(upstream.index(), rec, now, 10, held=[held.index()])
    observed.apply(rec, await owed.commit_ops(upstream.index(), rec, now, b, held=[held.index()]))
    assert rec["ranges"] == [] and rec["base"]["endpoint"] == 0 and "held" not in rec["base"]
