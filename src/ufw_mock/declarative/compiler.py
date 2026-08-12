from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

from pyspark.sql import DataFrame, SparkSession

from ufw_mock.formats import get_format
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.task import Task
from ufw_mock.runtime.edge_node_registry import EdgeNodeRegistry
from ufw_mock.runtime.transform_registry import TransformRegistry
from ufw_mock.types import DeclarativeDatasetKind, EdgeProtocol, TaskType, WriteMode


Builder = Callable[[SparkSession], DataFrame]


@dataclass(frozen=True)
class PathResolution:
    task_id: str
    source_path: str
    resolved_path: str
    kind: str
    producer_task_id: str | None = None


@dataclass(frozen=True)
class DatasetDef:
    name: str
    kind: DeclarativeDatasetKind
    task_id: str
    upstream: tuple[str, ...]
    options: dict[str, Any]
    builder: Builder
    source_path: str
    target_path: str
    publish_legacy_path: bool


def normalize_path(path: str) -> str:
    """Normalize local paths and URI paths for reliable producer matching."""
    parsed = urlsplit(path)
    if parsed.scheme in ("", "file"):
        from pathlib import Path

        local_path = Path(parsed.path if parsed.scheme else path).expanduser()
        if not local_path.is_absolute():
            local_path = Path.cwd() / local_path
        return str(local_path.resolve(strict=False))
    normalized_path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, parsed.query, parsed.fragment))


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    slug = re.sub(r"_+", "_", slug)
    if not slug:
        raise ValueError(f"Cannot derive a dataset name from {value!r}.")
    return slug


def dataset_name_for_task(task: Task) -> str:
    configured = task.properties.get("dataset_name")
    return slugify(str(configured if configured is not None else task.id))


class SdpPipelineCompiler:
    """Compile the existing Pipeline model into batch SDP dataset definitions."""

    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.edge_registry = EdgeNodeRegistry(pipeline.edge_nodes)
        self.transform_registry = TransformRegistry()

    def resolve_paths(self) -> list[PathResolution]:
        producers: dict[str, list[tuple[int, str]]] = {}
        for index, task in enumerate(self.pipeline.tasks):
            resolved = self._resolve_target(task)
            if task.target.path:
                producers.setdefault(resolved, []).append((index, task.id))

        resolutions: list[PathResolution] = []
        for index, task in enumerate(self.pipeline.tasks):
            source_path = self._resolve_source(task)
            earlier_producers = [
                producer
                for producer in producers.get(source_path, [])
                if producer[0] < index
            ]
            producer = max(earlier_producers, default=None, key=lambda item: item[0])
            is_internal = producer is not None
            resolutions.append(
                PathResolution(
                    task_id=task.id,
                    source_path=task.source.path,
                    resolved_path=source_path,
                    kind="INTERNAL" if is_internal else "EXTERNAL",
                    producer_task_id=producer[1] if is_internal else None,
                )
            )
        return resolutions

    def compile(self) -> list[DatasetDef]:
        names: dict[str, str] = {}
        for task in self.pipeline.tasks:
            name = dataset_name_for_task(task)
            previous = names.get(name)
            if previous is not None:
                raise ValueError(
                    f"Dataset name collision: tasks {previous!r} and {task.id!r} both resolve to {name!r}."
                )
            names[name] = task.id

        resolutions = {item.task_id: item for item in self.resolve_paths()}
        dataset_names = {task.id: dataset_name_for_task(task) for task in self.pipeline.tasks}
        definitions: list[DatasetDef] = []
        for task in self.pipeline.tasks:
            self._validate_task_mode(task)
            resolution = resolutions[task.id]
            upstream = (
                (dataset_names[resolution.producer_task_id],)
                if resolution.producer_task_id is not None
                else ()
            )
            kind = self._dataset_kind(task)
            builder = self._build_builder(task, resolution, dataset_names)
            definitions.append(
                DatasetDef(
                    name=dataset_names[task.id],
                    kind=kind,
                    task_id=task.id,
                    upstream=upstream,
                    options=self._dataset_options(task),
                    builder=builder,
                    source_path=task.source.path,
                    target_path=task.target.path,
                    publish_legacy_path=bool(
                        task.properties.get(
                            "publish_legacy_path",
                            self.pipeline.declarative.publish_legacy_paths
                            if self.pipeline.declarative
                            else True,
                        )
                    ),
                )
            )
        return definitions

    def _resolve_source(self, task: Task) -> str:
        edge = self.edge_registry.get_edge(task.source.edge_node)
        return normalize_path(edge.resolve(task.source.path))

    def _resolve_target(self, task: Task) -> str:
        edge = self.edge_registry.get_edge(task.target.edge_node)
        return normalize_path(edge.resolve(task.target.path))

    def _dataset_kind(self, task: Task) -> DeclarativeDatasetKind:
        configured = task.properties.get("dataset")
        if configured is None:
            edge_node = next(
                (node for node in self.pipeline.edge_nodes if node.name == task.source.edge_node),
                None,
            )
            streaming = bool(edge_node and edge_node.properties.get("streaming"))
            if edge_node and edge_node.protocol == EdgeProtocol.KAFKA:
                streaming = True
            configured = (
                DeclarativeDatasetKind.STREAMING_TABLE.value
                if streaming
                else DeclarativeDatasetKind.MATERIALIZED_VIEW.value
            )
        try:
            kind = DeclarativeDatasetKind(str(configured))
        except ValueError as exc:
            raise ValueError(f"Task {task.id!r} has unsupported declarative dataset kind {configured!r}.") from exc
        if kind == DeclarativeDatasetKind.STREAMING_TABLE:
            raise NotImplementedError(
                f"Task {task.id!r} resolves to a streaming_table; streaming SDP support is deferred to phase 4."
            )
        return kind

    def _validate_task_mode(self, task: Task) -> None:
        if task.target.mode in (WriteMode.MERGE, WriteMode.IGNORE, WriteMode.ERROR_IF_EXISTS):
            raise NotImplementedError(
                f"Task {task.id!r} uses write mode {task.target.mode.value!r}, which is unsupported "
                "in declarative mode (MERGE requires phase 4 Auto CDC; ignore/error_if_exists have no SDP analogue)."
            )

    def _dataset_options(self, task: Task) -> dict[str, Any]:
        options: dict[str, Any] = {}
        for key in ("comment", "partition_cols", "cluster_by", "table_properties", "format", "schema"):
            if key in task.properties:
                options[key] = task.properties[key]
        if "schema" not in options and task.source.schema_ref is not None:
            schema_ref = task.source.schema_ref
            if schema_ref.inline:
                options["schema"] = ", ".join(
                    f"`{name}` {data_type}" for name, data_type in schema_ref.inline.items()
                )
        if task.target.partition_by and "partition_cols" not in options:
            options["partition_cols"] = task.target.partition_by
        if "format" not in options:
            options["format"] = task.target.format.value
        return options

    def _build_builder(
        self,
        task: Task,
        resolution: PathResolution,
        dataset_names: dict[str, str],
    ) -> Builder:
        transformations = tuple(task.transformations)
        source = task.source
        edge_registry = self.edge_registry
        transform_registry = self.transform_registry
        upstream_name = (
            dataset_names[resolution.producer_task_id]
            if resolution.producer_task_id is not None
            else None
        )

        def builder(spark: SparkSession) -> DataFrame:
            if upstream_name is not None:
                df = spark.read.table(upstream_name)
            else:
                df = get_format(source.format).read(spark, resolution.resolved_path, source)
            if task.type != TaskType.VALIDATE:
                for transformation in transformations:
                    df = transform_registry.apply(df, transformation)
            # TODO(phase 3): attach DQ failure and summary datasets here.
            return df

        return builder
