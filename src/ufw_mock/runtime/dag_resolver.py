"""Dependency resolution for declarative pipeline task graphs.

Tasks may declare upstream dependencies explicitly via ``depends_on`` or
implicitly by publishing a named dataset via ``output_dataset`` that a
downstream task references through its ``source``. When no dependency is
declared anywhere in the pipeline, the authoring order of ``pipeline.tasks``
is preserved.
"""

from ufw_mock.models.task import Task


class DependencyResolutionError(ValueError):
    """Base error raised when a task graph cannot be resolved."""


class UnknownDependencyError(DependencyResolutionError):
    """Raised when a task depends on an id that is not defined in the pipeline."""


class CyclicDependencyError(DependencyResolutionError):
    """Raised when the declared dependencies form a cycle."""


class DuplicateTaskIdError(DependencyResolutionError):
    """Raised when two tasks share the same id."""


class DuplicateDatasetError(DependencyResolutionError):
    """Raised when two tasks declare the same output_dataset."""


def _dataset_references(task: Task) -> set[str]:
    """Dataset names a task reads from, derived from its source."""
    refs = {task.source.path}
    dataset = task.source.properties.get("dataset")
    if isinstance(dataset, str):
        refs.add(dataset)
    return refs


def build_dependency_graph(tasks: list[Task]) -> dict[str, set[str]]:
    """Map each task id to the set of task ids it depends on."""
    ids = [task.id for task in tasks]
    duplicates = {task_id for task_id in ids if ids.count(task_id) > 1}
    if duplicates:
        raise DuplicateTaskIdError(f"Duplicate task ids in pipeline: {sorted(duplicates)}")

    known = set(ids)
    producers: dict[str, str] = {}
    for task in tasks:
        if not task.output_dataset:
            continue
        existing = producers.get(task.output_dataset)
        if existing is not None:
            raise DuplicateDatasetError(
                f"Dataset '{task.output_dataset}' is produced by both '{existing}' and '{task.id}'"
            )
        producers[task.output_dataset] = task.id

    graph: dict[str, set[str]] = {}
    for task in tasks:
        upstream: set[str] = set()
        for dep in task.depends_on:
            if dep not in known:
                raise UnknownDependencyError(
                    f"Task '{task.id}' depends on unknown task id '{dep}'. "
                    f"Known task ids: {sorted(known)}"
                )
            upstream.add(dep)

        for ref in _dataset_references(task):
            producer = producers.get(ref)
            if producer is not None and producer != task.id:
                upstream.add(producer)

        graph[task.id] = upstream
    return graph


def has_declared_dependencies(tasks: list[Task]) -> bool:
    """Whether any dependency edge is declared in the pipeline."""
    return any(build_dependency_graph(tasks).values())


def resolve_execution_order(tasks: list[Task]) -> list[Task]:
    """Return tasks in dependency order.

    Falls back to authoring order when no dependencies are declared. Ties
    between independent tasks are broken by authoring order so runs stay
    deterministic.
    """
    graph = build_dependency_graph(tasks)
    if not any(graph.values()):
        return list(tasks)

    by_id = {task.id: task for task in tasks}
    remaining = {task_id: set(deps) for task_id, deps in graph.items()}
    ordered: list[Task] = []

    while remaining:
        ready = [task.id for task in tasks if task.id in remaining and not remaining[task.id]]
        if not ready:
            raise CyclicDependencyError(
                "Cyclic task dependencies detected among: " f"{sorted(remaining)}"
            )
        for task_id in ready:
            ordered.append(by_id[task_id])
            del remaining[task_id]
        for deps in remaining.values():
            deps.difference_update(ready)

    return ordered
