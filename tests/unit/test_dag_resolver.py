import pytest

from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.task import Task
from ufw_mock.runtime.dag_resolver import (
    CyclicDependencyError,
    DuplicateDatasetError,
    DuplicateTaskIdError,
    UnknownDependencyError,
    build_dependency_graph,
    has_declared_dependencies,
    resolve_execution_order,
)
from ufw_mock.types import TaskType


def make_task(task_id, depends_on=None, output_dataset=None, source_path="in", target_path="out"):
    kwargs = {}
    if depends_on is not None:
        kwargs["depends_on"] = depends_on
    if output_dataset is not None:
        kwargs["output_dataset"] = output_dataset
    return Task(
        id=task_id,
        type=TaskType.TRANSFORM,
        source=Source(edge_node="inbound", path=source_path),
        target=Target(edge_node="core", path=target_path),
        **kwargs,
    )


def test_task_dependency_fields_default_to_empty():
    first = make_task("t1")
    second = make_task("t2")
    assert first.depends_on == []
    assert first.output_dataset is None
    first.depends_on.append("t0")
    assert second.depends_on == []


def test_sequential_fallback_without_dependencies():
    tasks = [make_task("publish"), make_task("ingest"), make_task("clean")]
    assert has_declared_dependencies(tasks) is False
    assert resolve_execution_order(tasks) == tasks


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
        make_task("join", depends_on=["zeta", "alpha"]),
        make_task("zeta"),
        make_task("alpha"),
    ]
    order = [t.id for t in resolve_execution_order(tasks)]
    assert order == ["zeta", "alpha", "join"]


def test_output_dataset_creates_implicit_dependency():
    tasks = [
        make_task("downstream", source_path="curated.customers"),
        make_task("upstream", output_dataset="curated.customers"),
    ]
    assert build_dependency_graph(tasks) == {"downstream": {"upstream"}, "upstream": set()}
    assert [t.id for t in resolve_execution_order(tasks)] == ["upstream", "downstream"]


def test_cycle_detection():
    tasks = [
        make_task("clean", depends_on=["publish"]),
        make_task("publish", depends_on=["clean"]),
    ]
    with pytest.raises(CyclicDependencyError) as exc_info:
        resolve_execution_order(tasks)
    assert "['clean', 'publish']" in str(exc_info.value)


def test_self_dependency_is_a_cycle():
    with pytest.raises(CyclicDependencyError):
        resolve_execution_order([make_task("clean", depends_on=["clean"])])


def test_unknown_dependency_error():
    tasks = [make_task("clean", depends_on=["missing"])]
    with pytest.raises(UnknownDependencyError) as exc_info:
        resolve_execution_order(tasks)
    assert "missing" in str(exc_info.value)


def test_duplicate_task_ids_rejected():
    with pytest.raises(DuplicateTaskIdError) as exc_info:
        resolve_execution_order([make_task("clean"), make_task("clean")])
    assert "clean" in str(exc_info.value)


def test_dataset_reference_via_source_properties():
    downstream = make_task("downstream")
    downstream.source.properties["dataset"] = "curated.customers"
    tasks = [downstream, make_task("upstream", output_dataset="curated.customers")]
    assert [t.id for t in resolve_execution_order(tasks)] == ["upstream", "downstream"]


def test_duplicate_output_dataset_rejected():
    tasks = [
        make_task("first", output_dataset="curated.customers"),
        make_task("second", output_dataset="curated.customers"),
    ]
    with pytest.raises(DuplicateDatasetError) as exc_info:
        resolve_execution_order(tasks)
    assert "curated.customers" in str(exc_info.value)


def test_build_dependency_graph_edges():
    tasks = [make_task("a"), make_task("b", depends_on=["a"])]
    assert build_dependency_graph(tasks) == {"a": set(), "b": {"a"}}
