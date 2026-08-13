import pytest

from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.task import Task
from ufw_mock.runtime.dag_resolver import (
    CyclicDependencyError,
    DuplicateTaskIdError,
    UnknownDependencyError,
    build_dependency_graph,
    has_declared_dependencies,
    resolve_execution_order,
)
from ufw_mock.types import TaskType


def make_task(task_id, depends_on=None, output_dataset=None, source_path="in", target_path="out"):
    return Task(
        id=task_id,
        type=TaskType.TRANSFORM,
        source=Source(edge_node="inbound", path=source_path),
        target=Target(edge_node="core", path=target_path),
        depends_on=depends_on or [],
        output_dataset=output_dataset,
    )


def test_task_dependency_fields_default_to_empty():
    task = make_task("t1")
    assert task.depends_on == []
    assert task.output_dataset is None


def test_sequential_fallback_without_dependencies():
    tasks = [make_task("a"), make_task("b"), make_task("c")]
    assert has_declared_dependencies(tasks) is False
    assert [t.id for t in resolve_execution_order(tasks)] == ["a", "b", "c"]


def test_topological_order_of_multi_task_dag():
    tasks = [
        make_task("publish", depends_on=["validate"]),
        make_task("validate", depends_on=["transform"]),
        make_task("transform", depends_on=["ingest"]),
        make_task("ingest"),
    ]
    assert [t.id for t in resolve_execution_order(tasks)] == [
        "ingest",
        "transform",
        "validate",
        "publish",
    ]


def test_independent_branches_keep_authoring_order():
    tasks = [
        make_task("join", depends_on=["left", "right"]),
        make_task("left"),
        make_task("right"),
    ]
    order = [t.id for t in resolve_execution_order(tasks)]
    assert order.index("left") < order.index("join")
    assert order.index("right") < order.index("join")
    assert order[:2] == ["left", "right"]


def test_output_dataset_creates_implicit_dependency():
    tasks = [
        make_task("downstream", source_path="curated.customers"),
        make_task("upstream", output_dataset="curated.customers", target_path="curated.customers"),
    ]
    assert [t.id for t in resolve_execution_order(tasks)] == ["upstream", "downstream"]


def test_cycle_detection():
    tasks = [
        make_task("a", depends_on=["b"]),
        make_task("b", depends_on=["a"]),
    ]
    with pytest.raises(CyclicDependencyError) as exc_info:
        resolve_execution_order(tasks)
    assert "a" in str(exc_info.value) and "b" in str(exc_info.value)


def test_self_dependency_is_a_cycle():
    with pytest.raises(CyclicDependencyError):
        resolve_execution_order([make_task("a", depends_on=["a"])])


def test_unknown_dependency_error():
    tasks = [make_task("a", depends_on=["missing"])]
    with pytest.raises(UnknownDependencyError) as exc_info:
        resolve_execution_order(tasks)
    assert "missing" in str(exc_info.value)


def test_duplicate_task_ids_rejected():
    with pytest.raises(DuplicateTaskIdError):
        resolve_execution_order([make_task("a"), make_task("a")])


def test_build_dependency_graph_edges():
    tasks = [make_task("a"), make_task("b", depends_on=["a"])]
    assert build_dependency_graph(tasks) == {"a": set(), "b": {"a"}}
