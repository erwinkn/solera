"""A write's changes file: the transport form a worker hands the engine
(N13), encoded from the write's rows and removes a block at a time."""

import pytest


def _changes(o, size):
    """The changes file as encode_changes writes it, a block of size changes at
    a time: the reference batch._encode must match byte for byte."""
    import heapq
    import itertools

    from solera._native import encode_changes
    from solera.keys.layers import key_str

    word = "" if o.output.is_dynamic_partitions else None
    upserts = ((key_str(k), False) for chunk, _ in o.prepared.rows.chunks(size) for k in chunk)
    removes = () if o.replace else ((k, True) for k in sorted(o.prepared.removes))
    header = encode_changes([])
    out = [header]
    for chunk in itertools.batched(heapq.merge(upserts, removes), size):
        written = [k for k, removed in chunk if not removed]
        out.append(
            encode_changes(
                written if word is None else dict.fromkeys(written, word),
                [k for k, removed in chunk if removed],
            )[len(header) :]
        )
    return out


def _encoded(o, size, monkeypatch):
    import io

    from solera_worker import batch

    parts = []

    class Parts(io.BytesIO):
        def write(self, b):
            parts.append(bytes(b))
            return super().write(b)

    monkeypatch.setattr(batch, "CHANGES_CHUNK", size)
    assert batch._encode(o, Parts()) == len(b"".join(parts))
    return parts


def test_a_removal_heavy_write_is_encoded_a_chunk_of_changes_at_a_time(monkeypatch):
    from types import SimpleNamespace

    from solera.sdk import Output
    from solera.stores import Patch, prepare

    output = Output("items", key="id")
    prepared = prepare(Patch([{"id": "k050"}], remove=[f"k{i:03d}" for i in range(100)]), output)
    o = SimpleNamespace(output=output, prepared=prepared, replace=False)
    parts = _encoded(o, 7, monkeypatch)
    assert len(parts) == 1 + 15 and parts == _changes(o, 7)


@pytest.mark.parametrize("size", [1, 3, 64])
@pytest.mark.parametrize("replace", [False, True])
@pytest.mark.parametrize("case", ["records", "ints", "removes only", "empty", "partitions", "keys"])
def test_a_writes_changes_file_is_the_one_encode_changes_writes(monkeypatch, case, replace, size):
    import random
    from types import SimpleNamespace

    from solera.sdk import KEYS, DynamicPartitions, Output
    from solera.stores import Patch, prepare

    rng = random.Random(f"{case}{replace}{size}")
    names = [f"k{i}" for i in rng.sample(range(500), 120)] + ["é", "z", "a b", ""]
    written, removed = names[:60], names[40:]
    output = Output("items", key="id")
    if case == "records":
        write = Patch([{"id": k, "n": 1} for k in written] + [{"id": written[0], "n": 2}], remove=removed)
    elif case == "ints":
        write = Patch([{"id": i} for i in rng.sample(range(10_000), 50)], remove=[str(i) for i in range(30)])
    elif case == "removes only":
        write = Patch([], remove=removed)
    elif case == "empty":
        write = []
    elif case == "partitions":
        output = DynamicPartitions("items")
        write = written
    else:
        output = Output("items", key=KEYS)
        write = Patch({k: 1 for k in written}, remove=removed)
    o = SimpleNamespace(output=output, prepared=prepare(write, output), replace=replace)
    assert _encoded(o, size, monkeypatch) == _changes(o, size)


def test_a_change_names_each_key_once_in_byte_order():
    from solera._native import Rows, encode_changes

    rows = Rows.records([{"id": "b"}, {"id": 7}, {"id": "b"}], "id")
    assert b"".join(rows.changes(["c", "a"], "w")) == encode_changes({"7": "w", "b": "w"}, ["a", "c"])
    n = len(encode_changes([]))
    assert list(rows.changes(["c", "a"], "w", 2)) == [
        encode_changes([]),
        encode_changes({"7": "w"}, ["a"])[n:],
        encode_changes({"b": "w"}, ["c"])[n:],
    ]
    for removes, size in [(["b"], 64), (["b"], 1), (["a", "a"], 64)]:
        with pytest.raises(ValueError, match="both written and removed|removed twice"):
            b"".join(rows.changes(removes, size=size))
    with pytest.raises(ValueError, match="removed twice"):
        Rows.records([], "id").changes(["a", "a"], size=1)
    with pytest.raises(ValueError, match="at least one"):
        rows.changes([], size=0)
    with pytest.raises(TypeError):
        rows.changes([None])
    with pytest.raises(TypeError):
        rows.changes("ac")
    assert b"".join(rows.changes(iter({"c", "a"}), "w")) == encode_changes({"7": "w", "b": "w"}, ["a", "c"])
    # A failed iterator ends: it never hands out the blocks after the one that failed.
    blocks = Rows.records([{"id": k} for k in "abc"], "id").changes(["b"], size=4)
    next(blocks)
    with pytest.raises(ValueError, match="both written and removed"):
        next(blocks)
    assert list(blocks) == []
    # A key that is not UTF-8 text is refused, written or removed, as encode_changes refuses it.
    with pytest.raises(ValueError, match="UTF-8"):
        b"".join(Rows.records([{"id": "\udcff"}], "id").changes([]))
    with pytest.raises(UnicodeEncodeError):
        Rows.records([], "id").changes(["\udcfe"])
    with pytest.raises(UnicodeEncodeError):
        encode_changes(["\udcff"])
