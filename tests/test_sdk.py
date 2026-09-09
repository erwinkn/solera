import pytest

from data_orchestrator import (
    ByKey,
    DailyPartitions,
    Definitions,
    Output,
    ReplaceKeys,
    Upsert,
    asset,
)
from data_orchestrator.planner import plan
from data_orchestrator.stores import FileStore, apply_operation


def test_decorator_and_dependency_inference():
    @asset
    def source():
        return 1

    @asset
    def final(source):
        return source + 1

    definitions = Definitions([final, source])
    manifest = definitions.manifest()
    assert [p["key"] for p in manifest["producers"]] == ["source", "final"]
    assert final(2) == 3
    assert len(plan(manifest, ["final"])) == 2


def test_cycle_and_missing_dependency():
    @asset
    def one(two):
        return two

    @asset
    def two(one):
        return one

    with pytest.raises(ValueError, match="cycle"):
        Definitions([one, two]).manifest()
    with pytest.raises(ValueError, match="Unknown upstream"):
        Definitions([one]).manifest()


def test_duplicate_outputs():
    @asset(outputs={"shared": Output()})
    def one():
        return 1

    @asset(outputs={"shared": Output()})
    def two():
        return 2

    with pytest.raises(ValueError, match="Duplicate asset"):
        Definitions([one, two]).manifest()


def test_empty_ownership_replacement():
    assert apply_operation([{"file": "a"}, {"file": "b"}], ReplaceKeys("file", ["a"], [])) == [
        {"file": "b"}
    ]
    with pytest.raises(ValueError, match="ownership"):
        apply_operation([], ReplaceKeys("file", ["a"], [{"file": "b"}]))


def test_upsert_validation():
    assert apply_operation([{"id": "a", "v": 1}], Upsert("id", [{"id": "b", "v": 2}], ["a"])) == [
        {"id": "b", "v": 2}
    ]
    with pytest.raises(ValueError, match="Duplicate"):
        apply_operation([], Upsert("id", [{"id": "a"}, {"id": "a"}]))
    with pytest.raises(ValueError, match="both"):
        apply_operation([], Upsert("id", [{"id": "a"}], ["a"]))


def test_partition_bounds():
    partitions = DailyPartitions("2024-01-01")
    assert partitions.keys("2024-02-28", "2024-03-01") == ["2024-02-28", "2024-02-29", "2024-03-01"]
    for start, end in [
        ("2023-12-31", "2024-01-01"),
        ("2024-02-02", "2024-02-01"),
        ("2024-01-01", "2025-12-31"),
    ]:
        with pytest.raises(ValueError):
            partitions.keys(start, end)
    with pytest.raises(ValueError):
        ByKey("source", batch_size=0)


def test_file_store_is_immutable_and_validates_integrity(tmp_path):
    store = FileStore(str(tmp_path))
    reference = store.stage([{"id": 1}])
    assert store.stage([{"id": 1}]) == reference
    assert store.load(reference) == [{"id": 1}]
    assert len(list(tmp_path.iterdir())) == 1
    with pytest.raises(ValueError):
        store.load({"id": "../outside"})
    (tmp_path / f"{reference['id']}.json").write_text("[]")
    with pytest.raises(ValueError, match="integrity"):
        store.load(reference)
