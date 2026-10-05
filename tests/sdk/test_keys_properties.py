"""Properties of what the key index exchanges, against what was given:
Hypothesis draws keys — empty and long ones, keys sharing long prefixes —
and payloads, empty and absent: a write's sorted entries survive their
transport (one delta file, `SortedEntries.encode`/`decode`), and the
resolver's framing takes a body only when it holds together."""

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from solera import _native

SETTINGS = settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])

# Keys from a small alphabet share prefixes (prefix compression restarts per
# block); some are long, one may be empty.
keys_ = st.one_of(
    st.binary(max_size=6).map(lambda b: bytes(c % 3 + 97 for c in b)),
    st.binary(min_size=200, max_size=600),
    st.builds(lambda n, tail: b"p" * n + tail, st.integers(0, 300), st.binary(max_size=3)),
)
payloads_ = st.one_of(st.none(), st.just(b""), st.binary(max_size=40), st.binary(min_size=500, max_size=900))


@SETTINGS
@given(
    upserts=st.dictionaries(keys_, payloads_, max_size=30),
    removes=st.sets(keys_, max_size=10),
)
def test_sorted_entries_round_trip(upserts, removes):
    removes -= set(upserts)
    keys = list(upserts)
    run = _native.SortedEntries.of(keys, [upserts[k] for k in keys], sorted(removes))
    assert (len(run), run.upserts, run.removes) == (len(upserts) + len(removes), len(upserts), len(removes))
    want = sorted([(k, 0, 0, p) for k, p in upserts.items()] + [(k, 0, 1, None) for k in removes])
    k, g, f, p = run.entries()
    assert list(zip(k, g, f, p, strict=True)) == want
    assert _native.SortedEntries.decode(run.encode(block_size=256)).entries() == run.entries()


_names = st.sampled_from(["a", "b", "c"])
_outputs_ = st.lists(
    st.fixed_dictionaries(
        {"name": st.one_of(_names, st.integers(0, 2))},
        optional={"offset": st.one_of(st.integers(-2, 12), st.just("0")), "size": st.integers(-1, 12)},
    ),
    max_size=3,
)


@SETTINGS
@given(
    head=st.one_of(
        st.fixed_dictionaries({"outputs": _outputs_}),
        st.just([]),
        st.dictionaries(st.text(max_size=3), st.none()),
    ),
    payload=st.binary(max_size=24),
    cut=st.integers(0, 3),
    noise=st.binary(max_size=8),
)
def test_a_resolve_body_is_framed_exactly_or_malformed(head, payload, cut, noise):
    """Whatever a worker sends, the resolver's framing takes it only when
    each output's payload lies right after the one before and they fill
    the body; anything else is `Malformed` (or a version it does not
    speak), never another exception."""

    from solera.keys.resolver import Malformed, UnsupportedVersion, _outputs, frame, unframe

    bodies = [frame(head, [payload]) if isinstance(head, dict) else frame({}, [])]
    bodies += [bodies[0][: len(bodies[0]) - cut], bodies[0][:5] + noise, noise]
    for body in bodies:
        try:
            header, payloads = unframe(body)
            outputs = _outputs(header, payloads)
        except (Malformed, UnsupportedVersion):
            continue
        assert b"".join(bytes(p) for _, p in outputs) == bytes(payloads)
        assert len({o["name"] for o, _ in outputs}) == len(outputs)
