"""The `.kx` key index format (docs/key-index-format.md) against a dict:
Hypothesis draws entries — empty and long keys, keys sharing long
prefixes, generations up to 2**64 - 1, empty and absent payloads, tiny
blocks — and the native extension and the pure-Python reference must
each read back what either wrote, and merge newest-wins as the dict does.
`test_keys_format.py` holds the named cases; this file holds the rest."""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from solera import _native

from . import keys_reference as _python
from .keys_driver import drive
from .test_keys_format import blocks_of, decode_all

_MODULES = {"python": _python, "native": _native}
CROSS = [
    pytest.param(w, r, id=f"{wn}-writes-{rn}-reads")
    for wn, w in _MODULES.items()
    for rn, r in _MODULES.items()
]
SETTINGS = settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])

# Keys from a small alphabet share prefixes (prefix compression restarts per
# block); some are long, one may be empty.
keys_ = st.one_of(
    st.binary(max_size=6).map(lambda b: bytes(c % 3 + 97 for c in b)),
    st.binary(min_size=200, max_size=600),
    st.builds(lambda n, tail: b"p" * n + tail, st.integers(0, 300), st.binary(max_size=3)),
)
generations_ = st.one_of(st.integers(0, 3), st.integers(0, 2**64 - 1))
payloads_ = st.one_of(st.none(), st.just(b""), st.binary(max_size=40), st.binary(min_size=500, max_size=900))


@st.composite
def entry_maps(draw, max_size=40):
    """key -> (generation, deleted, payload, predecessor): one file's entries."""

    keys = draw(st.sets(keys_, max_size=max_size))
    out = {}
    for key in keys:
        deleted = draw(st.booleans())
        payload = None if deleted else draw(payloads_)
        out[key] = (draw(generations_), int(deleted), payload, draw(st.one_of(st.none(), generations_)))
    return out


codecs_ = st.sampled_from([0, 1])
block_sizes = st.sampled_from([1, 64, 4096])


def encode(impl, entries: dict, *, predecessors=True, **kw) -> bytes:
    keys = sorted(entries)
    return impl.encode_file(
        keys,
        [entries[k][0] for k in keys],
        bytes(entries[k][1] for k in keys),
        payloads=[entries[k][2] for k in keys],
        predecessors=[entries[k][3] for k in keys] if predecessors else None,
        **kw,
    )


def newest_wins(files: list[dict], drop_deleted: bool) -> dict:
    """The merge of `files`, newest first: (generation, deleted, payload) per key."""

    out = {}
    for entries in reversed(files):
        out.update({k: v[:3] for k, v in entries.items()})
    return {k: v for k, v in out.items() if not (drop_deleted and v[1])}


@pytest.mark.parametrize("writer,reader", CROSS)
@SETTINGS
@given(entries=entry_maps(), codec=codecs_, block_size=block_sizes)
def test_a_file_reads_back_what_was_written(writer, reader, entries, codec, block_size):
    data = encode(writer, entries, codec=codec, block_size=block_size)
    tail, k, g, f, p = decode_all(reader, data)
    keys = sorted(entries)
    assert k == keys
    assert list(zip(g, f, p, strict=True)) == [entries[key][:3] for key in keys]
    assert tail["entries"] == len(keys)
    if keys:
        assert (tail["min_key"], tail["max_key"]) == (keys[0], keys[-1])
    _, blocks = blocks_of(data)
    predecessors = [x for b in blocks for x in reader.decode_block(b, tail["codec"])[4]]
    assert predecessors == [entries[key][3] for key in keys]


@pytest.mark.parametrize("writer,reader", CROSS)
@SETTINGS
@given(entries=entry_maps(), probes=st.sets(keys_, max_size=12), codec=codecs_, block_size=block_sizes)
def test_a_lookup_finds_exactly_the_written_keys(writer, reader, entries, probes, codec, block_size):
    tail, blocks = blocks_of(encode(writer, entries, codec=codec, block_size=block_size))
    probe = sorted(probes | set(list(entries)[:6]))
    found, gens, dels, pays = reader.lookup(blocks, tail["codec"], probe)
    for key, hit, g, d, p in zip(probe, found, gens, dels, pays, strict=True):
        want = entries.get(key)
        assert (hit, g, d, p) == ((1, *want[:3]) if want else (0, 0, 0, None)), key


@pytest.mark.parametrize("writer,reader", CROSS)
@SETTINGS
@given(
    files=st.lists(entry_maps(max_size=15), min_size=1, max_size=4),
    bounds=st.tuples(st.one_of(st.none(), keys_), st.one_of(st.none(), keys_)),
    drop_deleted=st.booleans(),
    block_size=block_sizes,
    data=st.data(),
)
def test_a_range_merge_is_newest_wins(writer, reader, files, bounds, drop_deleted, block_size, data):
    codecs = [data.draw(codecs_) for _ in files]
    runs = [
        blocks_of(encode(writer, f, codec=c, block_size=block_size))[1]
        for f, c in zip(files, codecs, strict=True)
    ]
    after, upto = bounds
    k, g, f, p = reader.merge_range(runs, codecs, after, upto, drop_deleted)
    want = {
        key: v
        for key, v in newest_wins(files, drop_deleted).items()
        if (after is None or key > after) and (upto is None or key <= upto)
    }
    assert k == sorted(want)
    assert list(zip(g, f, p, strict=True)) == [want[key] for key in k]


def merge_spans(impl, files: list[bytes], *, base: bool, **kw) -> list[bytes]:
    if impl is _python:
        return _python.merge_spans(files, base=base, **kw)
    return drive(_native.Merge.spans(len(files), endpoints=[], base=base, **kw), [[f] for f in files])


@pytest.mark.parametrize("writer,reader", CROSS)
@SETTINGS
@given(
    files=st.lists(entry_maps(max_size=25), min_size=1, max_size=4),
    base=st.booleans(),
    max_file_bytes=st.sampled_from([1, 2_000, 64 * 2**20]),
)
def test_a_span_merge_without_endpoints_keeps_the_newest_in_consecutive_files(
    writer, reader, files, base, max_file_bytes
):
    """With no live endpoint inside, merged spans hold every key's newest
    version, carrying its oldest version's predecessor — into the base, live
    keys only and no predecessor — split into consecutive key ranges. A
    file's generations sit above every older file's, as spans' do."""

    files = [
        {k: ((len(files) - i) << 40 | g % (1 << 40), d, p, b) for k, (g, d, p, b) in f.items()}
        for i, f in enumerate(files)
    ]
    encoded = [encode(writer, f, block_size=256) for f in files]
    out = merge_spans(reader, encoded, base=base, block_size=256, max_file_bytes=max_file_bytes)
    keys, rest, preds = [], [], []
    for data in out:
        _, k, g, f, p = decode_all(reader, data)
        assert k, "a merge writes no empty file"
        keys += k
        rest += zip(g, f, p, strict=True)
        _, blocks = blocks_of(data)
        preds += [x for b in blocks for x in reader.decode_block(b, 1)[4]]
    want = newest_wins(files, base)
    assert keys == sorted(want)  # consecutive and non-overlapping, in order
    assert rest == [want[k] for k in keys]
    oldest = {k: e[3] for f in files for k, e in f.items()}  # the last file holding a key is its oldest
    assert preds == [None if base else oldest[k] for k in keys]


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
    assert _native.SortedEntries.decode(run.encode()).entries() == run.entries()


def resolved(files: list[dict], upserts: dict, removes: set, replace: bool, generation: int) -> dict:
    """What a write does to each key (native/src/delta.rs): key ->
    (generation, deleted, payload, predecessor) for every key it changes."""

    held = {k: v for k, v in newest_wins(files, drop_deleted=True).items()}
    out = {}
    if replace:
        removes = set(held) - set(upserts)
    for key in sorted(set(upserts) | removes):
        old = held.get(key)
        before = old[0] if old else None
        if key in removes:
            if old is not None:
                out[key] = (generation, 1, None, before)
        elif old is None:
            out[key] = (generation, 0, upserts[key], None)
        elif not (upserts[key] is not None and old[2] == upserts[key]):
            out[key] = (generation, 0, upserts[key], before)
    return out


@SETTINGS
@given(
    files=st.lists(entry_maps(max_size=40), max_size=4),
    upserts=st.dictionaries(keys_, st.one_of(st.none(), st.sampled_from([b"", b"v1", b"v2"])), max_size=6),
    removes=st.sets(keys_, max_size=4),
    replace=st.booleans(),
    generation=generations_,
)
def test_a_resolve_writes_what_changed(files, upserts, removes, replace, generation):
    """A write resolved against an index — by point lookups when it is
    small beside the index, by a merge otherwise — holds every key it
    changes and no other, each with the generation the key had before."""

    import tempfile

    removes = set() if replace else removes - set(upserts)
    with tempfile.TemporaryDirectory() as tmp:
        runs = []
        for n, f in enumerate(files):
            path = f"{tmp}/{n}"
            _native.build_local(encode(_native, f, block_size=256), f"f{n}", bytes(16), path, 2**30)
            runs.append([_native.LocalFile(path)])
        keys = list(upserts)
        run = _native.SortedEntries.of(keys, [upserts[k] for k in keys], sorted(removes))
        out, added, removed, changed = _native.Snapshot(runs).resolve(
            run, replace=replace, generation=generation, block_size=256
        )
    got = {}
    for data in out:
        tail, blocks = blocks_of(data)
        for b in blocks:
            got.update({k: v for k, *v in zip(*_native.decode_block(b, tail["codec"]), strict=True)})
    got = {k: tuple(v) for k, v in got.items()}
    want = resolved(files, upserts, removes, replace, generation)
    assert got == want
    held = newest_wins(files, drop_deleted=True)
    assert added == sum(1 for k, v in want.items() if not v[1] and k not in held)
    assert removed == sum(1 for v in want.values() if v[1])
    assert changed == len(want) - added - removed


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
