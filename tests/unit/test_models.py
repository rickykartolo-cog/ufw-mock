import pytest

from ufw_mock.models.edge_node import EdgeNode
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.task import Task
from ufw_mock.models.transformation import Transformation
from ufw_mock.types import EdgeDirection, EdgeProtocol, StorageFormat, TaskType, WriteMode


def test_pipeline_model():
    pipeline = Pipeline(
        name="test-pipeline",
        tasks=[
            Task(
                id="t1",
                type=TaskType.INGEST,
                source=Source(edge_node="inbound", path="raw"),
                target=Target(edge_node="core", path="curated"),
            )
        ],
    )
    assert pipeline.name == "test-pipeline"
    assert len(pipeline.tasks) == 1
    assert pipeline.tasks[0].type == TaskType.INGEST


def test_edge_node_model():
    edge = EdgeNode(
        name="inbound-crm",
        direction=EdgeDirection.INBOUND,
        protocol=EdgeProtocol.FILE,
    )
    assert edge.name == "inbound-crm"
    assert edge.direction == EdgeDirection.INBOUND


def test_source_target_defaults():
    source = Source(edge_node="inbound", path="data")
    assert source.format == StorageFormat.PARQUET
    target = Target(edge_node="outbound", path="data")
    assert target.mode == WriteMode.OVERWRITE


def test_transformation_model():
    transform = Transformation(name="upper", type="uppercase", input_cols=["country"])
    assert transform.name == "upper"
    assert transform.type == "uppercase"


def test_pipeline_min_tasks_validation():
    with pytest.raises(ValueError):
        Pipeline(name="empty", tasks=[])
