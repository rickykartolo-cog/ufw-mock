from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from ufw_mock.declarative.compiler import DatasetDef
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.runtime.platform import PlatformAdapter


LOGGER = logging.getLogger(__name__)


@dataclass
class DeclarativeRunResult:
    graph_status: str
    publish_status: str = "not_requested"
    events: list[str] = field(default_factory=list)
    graph_id: str | None = None
    mapping: dict[str, str] = field(default_factory=dict)
    publish_errors: list[dict[str, str]] = field(default_factory=list)


def register_and_run(
    spark: SparkSession,
    pipeline: Pipeline,
    definitions: list[DatasetDef],
    *,
    dry: bool,
    full_refresh_all: bool = False,
) -> DeclarativeRunResult:
    """Build, register, and run one fresh SDP graph."""
    try:
        from pyspark import pipelines as dp
        from pyspark.pipelines.graph_element_registry import (
            graph_element_registration_context,
        )
        from pyspark.pipelines.spark_connect_graph_element_registry import (
            SparkConnectGraphElementRegistry,
        )
        from pyspark.pipelines.spark_connect_pipeline import (
            create_dataflow_graph,
            start_run,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Spark Declarative Pipelines requires the optional dependency "
            "'ufw-mock[declarative]' (pyspark[pipelines]>=4.1.0,<5.0)."
        ) from exc

    config = pipeline.declarative
    assert config is not None
    graph_id = create_dataflow_graph(
        spark,
        config.catalog,
        config.database,
        config.configuration,
    )
    registry = SparkConnectGraphElementRegistry(spark, graph_id)
    with graph_element_registration_context(registry):
        for definition in definitions:
            if definition.kind.value == "temporary_view":
                options = {
                    key: value
                    for key, value in definition.options.items()
                    if key in {"comment", "spark_conf"}
                }
                decorator = dp.temporary_view(name=definition.name, **options)
            else:
                decorator = dp.materialized_view(name=definition.name, **definition.options)
            decorator(lambda definition=definition: definition.builder(spark))

    events: list[str] = []
    for result in start_run(
        spark,
        graph_id,
        full_refresh=None,
        full_refresh_all=full_refresh_all,
        refresh=None,
        dry=dry,
        storage=config.storage,
    ):
        if "pipeline_event_result" in result:
            message = result["pipeline_event_result"].event.message
            events.append(message)
            LOGGER.info("SDP: %s", message)
        elif "pipeline_command_result" in result:
            LOGGER.info("SDP command completed for graph %s", graph_id)
    return DeclarativeRunResult(
        graph_status="dry_run_succeeded" if dry else "succeeded",
        events=events,
        graph_id=graph_id,
    )


def publish_legacy_paths(
    spark: SparkSession,
    pipeline: Pipeline,
    definitions: list[DatasetDef],
) -> tuple[str, list[dict[str, str]]]:
    """Export completed SDP tables through the existing format adapters."""
    from ufw_mock.formats import get_format
    from ufw_mock.runtime.edge_node_registry import EdgeNodeRegistry

    registry = EdgeNodeRegistry(pipeline.edge_nodes)
    definitions_by_task = {definition.task_id: definition for definition in definitions}
    errors: list[dict[str, str]] = []
    attempted = False
    for task in pipeline.tasks:
        definition = definitions_by_task.get(task.id)
        if definition is None:
            continue
        if not definition.target_path or not definition.publish_legacy_path:
            continue
        attempted = True
        try:
            edge = registry.get_edge(task.target.edge_node)
            path = edge.resolve(task.target.path)
            df = spark.read.table(definition.name)
            get_format(task.target.format).write(df, path, task.target)
        except Exception as exc:
            errors.append({"task": task.id, "path": task.target.path, "error": str(exc)})
            LOGGER.error("SDP legacy export failed for task %s: %s", task.id, exc)
    if not attempted:
        return "not_requested", errors
    return ("succeeded" if not errors else "partial_failure"), errors


def run_declarative_pipeline(
    pipeline: Pipeline,
    platform: PlatformAdapter,
    *,
    dry: bool = False,
    full_refresh_all: bool = False,
) -> dict[str, Any]:
    from ufw_mock.declarative.compiler import SdpPipelineCompiler

    definitions = SdpPipelineCompiler(pipeline).compile()
    spark = platform.get_spark_session(pipeline.name)
    result = register_and_run(
        spark,
        pipeline,
        definitions,
        dry=dry,
        full_refresh_all=full_refresh_all,
    )
    if not dry and result.graph_status == "succeeded":
        result.publish_status, result.publish_errors = publish_legacy_paths(
            spark,
            pipeline,
            definitions,
        )
    result.mapping = {
        definition.target_path: definition.name
        for definition in definitions
        if definition.target_path
    }
    return {
        "pipeline": pipeline.name,
        "graph_status": result.graph_status,
        "publish_status": result.publish_status,
        "events": result.events,
        "mapping": result.mapping,
        "publish_errors": result.publish_errors,
        "tasks": [{"id": definition.task_id, "dataset": definition.name} for definition in definitions],
    }
