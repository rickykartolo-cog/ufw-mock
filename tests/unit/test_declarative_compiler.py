from pathlib import Path

import pytest

from ufw_mock.declarative.compiler import (
    SdpPipelineCompiler,
    dataset_name_for_task,
    normalize_path,
    slugify,
)
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.task import Task
from ufw_mock.types import EdgeDirection, EdgeProtocol, TaskType, WriteMode


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
    ]
    assert definitions[1].kind.value == "materialized_view"


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
