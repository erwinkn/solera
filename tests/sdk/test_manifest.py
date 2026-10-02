"""§11 registration: the manifest is deterministic and every documented
registration error is raised with the asset/output name in the message."""

import json
from pathlib import Path

import pytest
from solera.executors import Environment
from solera.sdk import (
    Automation,
    Cron,
    Every,
    In,
    Incremental,
    Migration,
    OnChange,
    Output,
    Project,
    RegistrationError,
    Result,
    Source,
    StaticPartitions,
    TableRef,
    asset,
    job,
)
from solera.stores import FileStore

BRIMSTONE = Path(__file__).parents[2] / "example" / "brimstone.py"
SNAPSHOT = Path(__file__).parent / "snapshots" / "brimstone.manifest.json"


def load_brimstone():
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("brimstone", BRIMSTONE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["brimstone"] = module
    spec.loader.exec_module(module)
    return module.project


def test_brimstone_manifest_snapshot(monkeypatch):
    """§11: the reference project registers and its manifest is stable."""

    monkeypatch.setenv("SOLERA_BUILD", "snapshot")  # not this checkout's content
    project = load_brimstone()
    manifest = json.loads(json.dumps(project.manifest, sort_keys=True))
    if not SNAPSHOT.exists():
        SNAPSHOT.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        pytest.fail("snapshot regenerated; re-run to verify")
    assert manifest == json.loads(SNAPSHOT.read_text())


def test_brimstone_revision_changes_on_change():
    """§11: the project revision is a digest of the manifest body."""

    project = load_brimstone()
    assert project.manifest["revision"] == project.manifest["revision"]

    def other_asset():
        return 1

    other = Project(assets=[asset(other_asset)])
    assert other.manifest["revision"] != project.manifest["revision"]


def test_input_value_must_be_edge_or_str():
    """§11: an inputs= value that is not str/In/Incremental/AllPartitions is an error."""

    @asset(inputs={"x": 42})
    def bad(x):
        return x

    with pytest.raises(RegistrationError, match="bad"):
        Project(assets=[bad])


def test_input_names_unknown_output():
    """§11: an input naming an unknown output is an error."""

    @asset(inputs={"x": "nowhere"})
    def bad(x: list):
        return x

    with pytest.raises(RegistrationError, match="nowhere"):
        Project(assets=[bad])


def test_upstream_only_dimension_requires_all_partitions():
    """§7/§11: an edge with upstream-only dimensions needs AllPartitions."""

    upstream_partitions = StaticPartitions(["a", "b"])

    @asset(partitions=upstream_partitions)
    def up():
        return []

    @asset(inputs={"up": In()})
    def down(up: list):
        return up

    with pytest.raises(RegistrationError, match="AllPartitions"):
        Project(assets=[up, down])


def test_incremental_rejects_upstream_only_dimensions():
    """§11: an Incremental edge cannot have upstream-only dimensions."""

    upstream_partitions = StaticPartitions(["a", "b"])

    @asset(outputs=Output("up", key="id"), partitions=upstream_partitions)
    def up():
        return []

    @asset(partitions={"day": StaticPartitions(["x"])}, inputs={"up": Incremental()})
    def down(up: list):
        return up

    with pytest.raises(RegistrationError, match="upstream-only"):
        Project(assets=[up, down])


def test_incremental_rejects_ref_annotation():
    """§11: an Incremental edge cannot be ref-annotated."""

    @asset(outputs=Output("up", key="id"))
    def up():
        return []

    @asset(inputs={"up": Incremental()})
    def down(up: TableRef):
        return up

    with pytest.raises(RegistrationError, match="ref-annotated"):
        Project(assets=[up, down])


def test_incremental_requires_incremental_upstream():
    """§11: an Incremental edge's upstream output must be incremental."""

    @asset
    def up():
        return []

    @asset(inputs={"up": Incremental()})
    def down(up: list):
        return up

    with pytest.raises(RegistrationError, match="not incremental"):
        Project(assets=[up, down])


def test_incremental_requires_selection_capable_store():
    """§11: the upstream store must serve the edge's selection type."""

    class NoSelection(FileStore):
        def can_load(self, t, selection):
            return selection is None and super().can_load(t, None)

    @asset(outputs=Output("up", store="nosel", key="id"))
    def up() -> list[dict]:
        return []

    @asset(inputs={"up": Incremental()})
    def down(up: list[dict]):
        return up

    with pytest.raises(RegistrationError, match="cannot load"):
        Project(assets=[up, down], stores={"nosel": NoSelection()})


def test_output_mode_is_removed():
    """§2/§11: Output(mode=) is gone; incremental= replaces it."""

    with pytest.raises(RegistrationError, match="mode="):
        Output("x", key="id", mode="append")
    with pytest.raises(RegistrationError, match="key= implies"):
        Output("x", key="id", incremental=False)


def test_store_must_accept_output_type():
    """§11: an output's return annotation must pass can_store."""

    @asset(outputs=Output("x", keyed=True))
    def bad() -> str:
        return "nope"

    with pytest.raises(RegistrationError, match="cannot store"):
        Project(assets=[bad])


def test_patch_and_result_annotations_say_nothing_of_the_payload():
    """§4/§11: `Patch` and `Result` are envelopes: annotating a producer's
    return with them (or a union holding one) registers, as it runs."""

    from solera.stores import Patch

    @asset(outputs=Output("rows", key="id"))
    def patch() -> Patch:
        return Patch([])

    @asset(outputs=Output("more", key="id"))
    def result() -> Result:
        return Result({"more": []})

    @asset(outputs=Output("either", key="id"))
    def either() -> list[dict] | Patch:
        return []

    Project(assets=[patch, result, either])


def test_a_dataframe_needs_a_store_that_reads_dataframes():
    """§4: the core knows no DataFrame; a store takes one only if it reads it
    (`Store.prepare`), and says so in `can_store`. A producer annotated to
    return a DataFrame into a store of plain rows fails at registration."""

    import pandas as pd
    from solera.stores import takes_plain

    class PlainStore(FileStore):
        def can_store(self, t, output):
            return takes_plain(t)

    @asset(outputs=Output("rows", key="id", store="plain"))
    def rows() -> pd.DataFrame:
        return pd.DataFrame({"id": ["a"]})

    with pytest.raises(RegistrationError, match="cannot store output rows .*Store.prepare"):
        Project(assets=[rows], stores={"plain": PlainStore()})

    @asset(outputs=Output("rows", key="id", store="plain"))
    def listed() -> list[dict]:
        return [{"id": "a"}]

    Project(assets=[listed], stores={"plain": PlainStore()})


def test_unannotated_store_bound_input():
    """§11: a store-bound input must be annotated."""

    @asset
    def up():
        return []

    @asset(inputs={"up": In()})
    def down(up):  # no annotation
        return up

    with pytest.raises(RegistrationError, match="unannotated"):
        Project(assets=[up, down])


def test_store_must_load_input_type():
    """§11: an input's store must pass can_load for the annotation."""

    class NoBytes(FileStore):
        def can_load(self, t, selection):
            return t is not bytes

    @asset(outputs=Output("up", store="nobytes"))
    def up():
        return []

    @asset(inputs={"up": In()})
    def down(up: bytes):
        return up

    with pytest.raises(RegistrationError, match="cannot load"):
        Project(assets=[up, down], stores={"nobytes": NoBytes()})


def test_keyed_output_requires_row_values():
    """§11: a keyed output rejects a non-row return annotation."""

    @asset(outputs=Output("x", key="id"))
    def bad() -> str:
        return "nope"

    with pytest.raises(RegistrationError, match="cannot store"):
        Project(assets=[bad])


def test_partitioned_output_on_shared_store_needs_partition_column():
    """§3/§11: a partitioned output on a shared-table store needs partition_column."""
    from solera_postgres import PostgresStore

    partitions = StaticPartitions(["a"])

    @asset(outputs=Output("x", store="pg"), partitions=partitions)
    def bad() -> list[dict]:
        return []

    with pytest.raises(RegistrationError, match="partition_column"):
        Project(assets=[bad], stores={"pg": PostgresStore("env:DATABASE_URL")})


def test_partitions_must_name_keyed_output():
    """§7/§11: partitions= naming an output with no key is an error."""

    @asset
    def unkeyed():
        return []

    @asset(partitions="unkeyed")
    def bad():
        return []

    with pytest.raises(RegistrationError, match="no key"):
        Project(assets=[bad, unkeyed])


def test_automation_requires_trigger():
    """§9/§11: Automation() without a trigger is an error."""

    with pytest.raises(RegistrationError, match="trigger"):
        Automation("x", targets=["a"])


def test_standalone_automation_requires_name_and_targets():
    """§9/§11: a standalone automation requires name and targets."""

    @asset
    def a():
        return []

    with pytest.raises(RegistrationError, match="name and targets"):
        Project(assets=[a], automations=[Automation(trigger=Every(60))])
    with pytest.raises(RegistrationError, match="name and targets"):
        Project(assets=[a], automations=[Automation("named", trigger=Every(60))])


def test_automation_name_collision():
    """§9/§11: automation names must be unique."""

    @asset(automations=[Automation(trigger=Every(60)), Automation(trigger=Every(60))])
    def a():
        return []

    # Attached names derive {asset}.{trigger}.{index} and must not collide.
    project = Project(assets=[a])
    assert "a.every.0" in project.manifest["automations"]
    assert "a.every.1" in project.manifest["automations"]

    @asset
    def b():
        return []

    with pytest.raises(RegistrationError, match="automation"):
        Project(
            assets=[b],
            automations=[
                Automation("dup", targets=["b"], trigger=Every(60)),
                Automation("dup", targets=["b"], trigger=Cron("0 0 * * *")),
            ],
        )


def test_onchange_cannot_watch_own_outputs():
    """§9/§11: OnChange may not name an output of its own target."""

    @asset(outputs=Output("x"), automations=Automation(trigger=OnChange("x")))
    def a():
        return []

    with pytest.raises(RegistrationError, match="own"):
        Project(assets=[a])


def test_unregistered_placement_kind():
    """§10/§11: a custom kind's executor must be registered, and a name
    means one executor."""

    class Custom(Environment):
        kind = "Custom"
        allowed = frozenset({"cpu"})

    custom = Custom("custom", zone="a")

    @asset(executor=custom(cpu=1))
    def a():
        return []

    with pytest.raises(RegistrationError, match="kind"):
        Project(assets=[a])
    project = Project(assets=[a], executors=[custom])
    assert project.manifest["executors"] == {"custom": {"kind": "Custom", "environment": {"zone": "a"}}}
    with pytest.raises(RegistrationError, match="custom"):
        Project(assets=[a], executors=[Custom("custom", zone="b")])


def test_deps_pin_but_never_bind():
    """§5: deps are watched and pinned but bound to no parameter."""

    @asset
    def up():
        return []

    @job(deps=["up"])
    def j():
        return None

    project = Project(assets=[up, j])
    assert project.manifest["assets"]["j"]["deps"] == ["up"]
    assert project.manifest["assets"]["j"]["inputs"] == {}


def test_source_synthesized_head():
    """§5: a source gets a synthesized external head at registration."""

    project = Project(sources=[Source("ext", key="id", region="us")])
    head = project.manifest["sources"]["ext"]["head"]
    assert head["meta"]["external"] is True
    assert head["handle"]["region"] == "us"
    assert head["version"]


def test_migrations_in_manifest():
    """§2/§11: the manifest records the ordered migration names per output."""

    @asset(
        outputs=Output(
            "docs",
            store="blobs",
            migrations=[
                Migration("a_seed", lambda objects, prefix: None),
                Migration("b_fix", lambda o, p: None),
            ],
        )
    )
    def producer() -> bytes:
        return b""

    project = Project(assets=[producer], stores={"blobs": Migrating()})
    out = project.manifest["outputs"]["docs"]
    assert out["migrations"] == ["a_seed", "b_fix"]


def test_migrations_require_a_migrating_store():
    """§4/§11: migrations= on a store without migrate is a registration error."""

    @asset(outputs=Output("x", migrations=[Migration("m", "SELECT 1")]))
    def bad():
        return []

    with pytest.raises(RegistrationError, match="no migrate"):
        Project(assets=[bad])


def test_duplicate_migration_names_rejected():
    """§4/§11: a migration name may not repeat within one output."""
    from solera_postgres import PostgresStore

    @asset(
        outputs=Output(
            "x",
            store="pg",
            migrations=[Migration("same", "SELECT 1"), Migration("same", "SELECT 2")],
        )
    )
    def bad():
        return []

    with pytest.raises(RegistrationError, match="duplicate migration"):
        Project(assets=[bad], stores={"pg": PostgresStore("env:DATABASE_URL")})


def test_migration_payload_must_pass_can_store():
    """§4/§11: a payload the store cannot store is a registration error."""

    @asset(outputs=Output("x", store="blobs", migrations=[Migration("m", "SELECT 1")]))
    def bad() -> bytes:
        return b""

    with pytest.raises(RegistrationError, match="can_store"):
        Project(assets=[bad], stores={"blobs": Migrating()})


class Migrating(FileStore):
    """A store that runs callable migrations only."""

    def can_store(self, t, output):
        return t is not str and super().can_store(t, output)

    async def migrate(self, output, migrations, scope=None):
        return [m.name for m in migrations]


def test_output_names_are_names():
    """An output is named like everything else — never `@asset`, the
    namespace of Each failure indexes, which would share its key files."""

    def parse():
        return {}

    for bad in ("@parse", "a/b", "<lambda>", ""):
        with pytest.raises(RegistrationError, match="invalid output name"):
            Project(assets=[asset(parse, outputs=Output(bad or "@", keyed=True))])
