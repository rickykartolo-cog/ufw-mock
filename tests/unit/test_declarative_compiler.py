from pathlib import Path

import pytest

from ufw_mock.declarative.compiler import (
    DatasetDef,
    SdpPipelineCompiler,
    dataset_name_for_task,
    normalize_path,
    slugify,
)
from ufw_mock.declarative.registrar import publish_legacy_paths
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.task import Task
from ufw_mock.runtime.validation_registry import ValidationRegistry
from ufw_mock.types import DeclarativeDatasetKind, EdgeDirection, EdgeProtocol, TaskType, WriteMode


def task(task_id: str, source: str, target: str, **properties) -> Task:
    return Task(
        id=task_id,
        type=TaskType.TRANSFORM,
        source={"edge_node": "files", "path": source},
        target={"edge_node": "files", "path": target},
        properties=properties,
    )


def pipeline(tasks: list[Task]) -> Pipeline:
    return Pipeline(name="test", tasks=tasks)


def test_slugify_and_dataset_override() -> None:
    assert slugify("Customer Raw-2024") == "customer_raw_2024"
    assert dataset_name_for_task(task("Task 1", "in", "out")) == "task_1"
    assert dataset_name_for_task(task("Task 1", "in", "out", dataset_name="Curated.Customers")) == (
        "curated_customers"
    )


def test_slug_collision_is_hard_error() -> None:
    config = pipeline(
        [
            task("A-B", "in-a", "out-a"),
            task("A_B", "in-b", "out-b"),
        ]
    )
    with pytest.raises(ValueError, match="Dataset name collision"):
        SdpPipelineCompiler(config).compile()


def test_path_resolution_normalizes_trailing_and_relative_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = pipeline(
        [
            task("ingest", "./input/", "./raw/"),
            task("transform", "raw", "curated"),
        ]
    )
    resolutions = SdpPipelineCompiler(config).resolve_paths()
    assert resolutions[0].kind == "EXTERNAL"
    assert resolutions[1].kind == "INTERNAL"
    assert resolutions[1].producer_task_id == "ingest"
    assert resolutions[1].resolved_path == str((tmp_path / "raw").resolve())
    assert normalize_path(f"file://{tmp_path}/raw/") == str((tmp_path / "raw").resolve())


def test_near_miss_and_later_producer_are_external(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = pipeline(
        [
            task("first", "never-produced", "later-output"),
            task("second", "raw/", "current-output"),
        ]
    )
    resolutions = SdpPipelineCompiler(config).resolve_paths()
    assert resolutions[0].kind == "EXTERNAL"
    assert resolutions[1].kind == "EXTERNAL"


def test_latest_earlier_producer_wins_for_reused_path() -> None:
    config = pipeline(
        [
            task("first", "input-a", "shared"),
            task("second", "input-b", "shared"),
            task("consumer", "shared", "output"),
        ]
    )
    resolutions = SdpPipelineCompiler(config).resolve_paths()
    assert resolutions[2].kind == "INTERNAL"
    assert resolutions[2].producer_task_id == "second"


def test_compiler_emits_internal_upstream_and_validate_passthrough() -> None:
    config = pipeline(
        [
            task("ingest", "input", "raw"),
            Task(
                id="validate",
                type=TaskType.VALIDATE,
                source={"edge_node": "files", "path": "raw"},
                target={"edge_node": "files", "path": ""},
                transformations=[{"name": "rule", "type": "unique", "input_cols": ["id"]}],
            ),
        ]
    )
    definitions = SdpPipelineCompiler(config).compile()
    assert [(item.name, item.upstream) for item in definitions] == [
        ("ingest", ()),
        ("validate", ("ingest",)),
        ("validate_dq_failures", ("validate",)),
        ("validate_dq_summary", ("validate_dq_failures",)),
    ]
    assert definitions[1].kind.value == "materialized_view"


def test_validate_without_rules_has_no_dq_datasets() -> None:
    config = pipeline(
        [
            Task(
                id="validate",
                type=TaskType.VALIDATE,
                source={"edge_node": "files", "path": "input"},
                target={"edge_node": "files", "path": ""},
            )
        ]
    )
    assert [definition.name for definition in SdpPipelineCompiler(config).compile()] == ["validate"]


def test_validate_generated_dataset_names_collide_with_user_names() -> None:
    config = pipeline(
        [
            Task(
                id="validate",
                type=TaskType.VALIDATE,
                source={"edge_node": "files", "path": "input"},
                target={"edge_node": "files", "path": ""},
                transformations=[{"name": "rule", "type": "not_null", "input_cols": ["id"]}],
            ),
            task("validate_dq_failures", "other", "output"),
        ]
    )
    with pytest.raises(ValueError, match="generated dataset"):
        SdpPipelineCompiler(config).compile()


def test_validation_registry_exposes_predicates_and_rejects_custom_without_one() -> None:
    registry = ValidationRegistry()
    for key in ("not_null", "range", "regex", "unique"):
        assert callable(registry.get_predicate(key))

    registry.register("custom", lambda _df, _rule: None)
    with pytest.raises(NotImplementedError, match="does not expose"):
        registry.get_predicate("custom")


def test_custom_validator_without_predicate_is_rejected_by_compiler() -> None:
    config = pipeline(
        [
            Task(
                id="validate",
                type=TaskType.VALIDATE,
                source={"edge_node": "files", "path": "input"},
                target={"edge_node": "files", "path": ""},
                transformations=[{"name": "custom-rule", "type": "custom", "input_cols": ["id"]}],
            )
        ]
    )
    compiler = SdpPipelineCompiler(config)
    compiler.validation_registry.register("custom", lambda _df, _rule: None)
    with pytest.raises(NotImplementedError, match="custom"):
        compiler.compile()


@pytest.mark.parametrize("mode", [WriteMode.MERGE, WriteMode.IGNORE, WriteMode.ERROR_IF_EXISTS])
def test_unsupported_write_modes_name_task(mode: WriteMode) -> None:
    config = pipeline(
        [
            Task(
                id="bad-write",
                type=TaskType.INGEST,
                source={"edge_node": "files", "path": "in"},
                target={"edge_node": "files", "path": "out", "mode": mode},
            )
        ]
    )
    with pytest.raises(NotImplementedError, match="bad-write"):
        SdpPipelineCompiler(config).compile()


def test_streaming_kind_is_reserved_for_phase_four() -> None:
    config = pipeline([task("stream", "in", "out", dataset="streaming_table")])
    with pytest.raises(NotImplementedError, match="phase 4"):
        SdpPipelineCompiler(config).compile()


def test_streaming_edge_defaults_to_streaming_kind() -> None:
    config = Pipeline(
        name="streaming-edge",
        edge_nodes=[
            {
                "name": "events",
                "direction": EdgeDirection.INBOUND,
                "protocol": EdgeProtocol.KAFKA,
            }
        ],
        tasks=[
            Task(
                id="events",
                type=TaskType.INGEST,
                source={"edge_node": "events", "path": "topic"},
                target={"edge_node": "files", "path": "out"},
            )
        ],
    )
    with pytest.raises(NotImplementedError, match="phase 4"):
        SdpPipelineCompiler(config).compile()


def test_declarative_configuration_rejects_static_warehouse_key() -> None:
    with pytest.raises(ValueError, match="spark.sql.warehouse.dir"):
        Pipeline(
            name="bad-config",
            declarative={
                "storage": "file:///tmp/storage",
                "configuration": {"spark.sql.warehouse.dir": "/tmp/warehouse"},
            },
            tasks=[task("one", "in", "out")],
        )


def test_legacy_publishing_keys_definitions_by_task_id(monkeypatch, tmp_path: Path) -> None:
    class FakeReader:
        def __init__(self) -> None:
            self.read_names: list[str] = []

        def table(self, name: str) -> object:
            self.read_names.append(name)
            return object()

    class FakeSpark:
        def __init__(self) -> None:
            self.read = FakeReader()

    class FakeFormat:
        def __init__(self) -> None:
            self.writes: list[str] = []

        def write(self, _df: object, path: str, _target: object) -> None:
            self.writes.append(path)

    fake_format = FakeFormat()
    monkeypatch.setattr("ufw_mock.formats.get_format", lambda _format: fake_format)
    config = Pipeline(
        name="publish-alignment",
        edge_nodes=[
            {
                "name": "files",
                "direction": EdgeDirection.INBOUND,
                "protocol": EdgeProtocol.FILE,
                "properties": {"base_path": str(tmp_path)},
            }
        ],
        tasks=[
            {
                "id": "first",
                "type": "INGEST",
                "source": {"edge_node": "files", "path": "input-a"},
                "target": {"edge_node": "files", "path": "first"},
            },
            {
                "id": "second",
                "type": "INGEST",
                "source": {"edge_node": "files", "path": "input-b"},
                "target": {"edge_node": "files", "path": "second"},
            },
        ],
    )
    definitions = [
        DatasetDef(
            name="_dq_summary",
            kind=DeclarativeDatasetKind.MATERIALIZED_VIEW,
            task_id="_dq_summary",
            upstream=(),
            options={},
            builder=lambda _spark: None,
            source_path="",
            target_path="",
            publish_legacy_path=False,
        ),
        *[
            DatasetDef(
                name=task_id,
                kind=DeclarativeDatasetKind.MATERIALIZED_VIEW,
                task_id=task_id,
                upstream=(),
                options={},
                builder=lambda _spark: None,
                source_path="",
                target_path=target,
                publish_legacy_path=True,
            )
            for task_id, target in (("first", "first"), ("second", "second"))
        ],
    ]

    fake_spark = FakeSpark()
    status, errors = publish_legacy_paths(fake_spark, config, definitions)

    assert status == "succeeded"
    assert errors == []
    assert fake_spark.read.read_names == ["first", "second"]
    assert fake_format.writes == [str(tmp_path / "first"), str(tmp_path / "second")]
