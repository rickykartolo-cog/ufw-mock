"""Compile a UFW pipeline into Spark Declarative Pipelines (SDP) dataset definitions.

The real SDP decorator API (`pipelines`, historically `dlt`) is only importable
inside a Databricks/SDP pipeline context, so this module *compiles and emits*
dataset definitions as a contract artifact: a dependency-ordered graph plus a
generated Python module. The generated module reuses the existing
`TransformRegistry` for dataset bodies and maps `ValidationRegistry` validators
onto SDP expectations.
"""

import json
import re
from dataclasses import dataclass, field

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import SdpOptions
from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.task import Task
from ufw_mock.models.transformation import Transformation
from ufw_mock.runtime.validation_registry import ValidationRegistry
from ufw_mock.types import SdpExpectationAction, SdpMaterialization, TaskType

_MATERIALIZATION_DECORATORS = {
    SdpMaterialization.STREAMING_TABLE: "table",
    SdpMaterialization.MATERIALIZED_VIEW: "table",
    SdpMaterialization.TABLE: "table",
    SdpMaterialization.VIEW: "view",
}

_EXPECTATION_DECORATORS = {
    SdpExpectationAction.WARN: "expect_all",
    SdpExpectationAction.DROP: "expect_all_or_drop",
    SdpExpectationAction.FAIL: "expect_all_or_fail",
}


class SdpCompilationError(Exception):
    """Raised when a pipeline cannot be compiled into an SDP graph."""


@dataclass
class SdpExpectation:
    """An SDP expectation compiled from a VALIDATE transformation."""

    name: str
    action: SdpExpectationAction
    constraints: dict[str, str]

    @property
    def decorator(self) -> str:
        return _EXPECTATION_DECORATORS[self.action]


@dataclass
class SdpDataset:
    """A single SDP dataset definition compiled from a UFW task."""

    name: str
    task_id: str
    task_type: TaskType
    materialization: SdpMaterialization
    source: Source | None = None
    target: Target | None = None
    upstream: list[str] = field(default_factory=list)
    transformations: list[Transformation] = field(default_factory=list)
    expectations: list[SdpExpectation] = field(default_factory=list)
    validated_by: list[str] = field(default_factory=list)
    catalog: str | None = None
    schema_name: str | None = None

    @property
    def decorator(self) -> str:
        return _MATERIALIZATION_DECORATORS[self.materialization]

    @property
    def is_streaming(self) -> bool:
        return self.materialization == SdpMaterialization.STREAMING_TABLE

    @property
    def qualified_name(self) -> str:
        parts = [p for p in (self.catalog, self.schema_name, self.name) if p]
        return ".".join(parts)


@dataclass
class SdpPipelineGraph:
    """A dependency-ordered set of SDP dataset definitions."""

    pipeline_name: str
    datasets: list[SdpDataset]
    options: SdpOptions

    @property
    def dataset_names(self) -> list[str]:
        return [dataset.name for dataset in self.datasets]

    def get(self, name: str) -> SdpDataset:
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        raise KeyError(f"Unknown SDP dataset '{name}'. Known datasets: {self.dataset_names}")

    def roots(self) -> list[SdpDataset]:
        return [dataset for dataset in self.datasets if not dataset.upstream]

    def edges(self) -> list[tuple[str, str]]:
        return [
            (upstream, dataset.name) for dataset in self.datasets for upstream in dataset.upstream
        ]

    def to_python_module(self) -> str:
        """Emit the graph as a standalone SDP Python module."""
        return _emit_module(self)

    def register(self, sdp_module, spark, namespace: dict | None = None) -> dict:
        """Execute the emitted module against a real SDP module and Spark session.

        `sdp_module` is the SDP decorator module (`pipelines`/`dlt`), normally
        obtained from `SparkDeclarativePipelinesPlatform.get_sdp_module()`.
        """
        namespace = namespace if namespace is not None else {}
        namespace.update({"sdp": sdp_module, "spark": spark, "__name__": self.options.module_name})
        exec(compile(self.to_python_module(), f"<sdp:{self.pipeline_name}>", "exec"), namespace)
        return namespace


class SdpCompiler:
    """Translates a `Pipeline` into SDP dataset definitions."""

    def __init__(self, pipeline: Pipeline, options: SdpOptions | None = None) -> None:
        self.pipeline = pipeline
        self.options = options or SdpOptions()
        self.validation_registry = ValidationRegistry()

    def compile(self) -> SdpPipelineGraph:
        producers = self._producer_index()
        datasets: dict[str, SdpDataset] = {}
        validate_tasks: list[Task] = []

        for task in self.pipeline.tasks:
            if task.type == TaskType.VALIDATE:
                validate_tasks.append(task)
                continue
            dataset = self._build_dataset(task)
            upstream_task = producers.get(_location_key(task.source))
            if upstream_task and upstream_task != task.id:
                dataset.upstream = [self._dataset_name(upstream_task)]
            datasets[dataset.name] = dataset

        for task in validate_tasks:
            self._attach_expectations(task, datasets, producers)

        ordered = _topological_sort(datasets)
        return SdpPipelineGraph(self.pipeline.name, ordered, self.options)

    def _producer_index(self) -> dict[tuple[str, str], str]:
        """Map each written location to the id of the task that produces it."""
        producers: dict[tuple[str, str], str] = {}
        for task in self.pipeline.tasks:
            if task.type == TaskType.VALIDATE or not task.target.path:
                continue
            producers[_location_key(task.target)] = task.id
        return producers

    def _build_dataset(self, task: Task) -> SdpDataset:
        if task.type == TaskType.INGEST:
            materialization = self.options.ingest_materialization
        else:
            materialization = self.options.derived_materialization
        override = task.properties.get("sdp", {}).get("materialization")
        if override:
            materialization = SdpMaterialization(override)

        return SdpDataset(
            name=self._dataset_name(task.id),
            task_id=task.id,
            task_type=task.type,
            materialization=materialization,
            source=task.source,
            target=task.target,
            transformations=list(task.transformations),
            catalog=task.properties.get("sdp", {}).get("catalog", self.options.catalog),
            schema_name=task.properties.get("sdp", {}).get("schema", self.options.schema_name),
        )

    def _attach_expectations(
        self,
        task: Task,
        datasets: dict[str, SdpDataset],
        producers: dict[tuple[str, str], str],
    ) -> None:
        """Attach a VALIDATE task's expectations to the dataset it reads."""
        producer_task_id = producers.get(_location_key(task.source))
        if producer_task_id:
            dataset = datasets[self._dataset_name(producer_task_id)]
        else:
            # Nothing in the pipeline produces this location: expose it as a view
            # so the expectations still have a dataset to decorate.
            dataset = SdpDataset(
                name=self._dataset_name(task.id),
                task_id=task.id,
                task_type=task.type,
                materialization=SdpMaterialization.VIEW,
                source=task.source,
                target=task.target,
                catalog=self.options.catalog,
                schema_name=self.options.schema_name,
            )
            datasets[dataset.name] = dataset

        for transformation in task.transformations:
            dataset.expectations.append(self._build_expectation(transformation))
        if task.id not in dataset.validated_by:
            dataset.validated_by.append(task.id)

    def _build_expectation(self, transformation: Transformation) -> SdpExpectation:
        # Fail fast on validators the ValidationRegistry does not know about, so
        # the compiled contract stays consistent with sequential execution.
        self.validation_registry.get(transformation.type)
        builder = _EXPECTATION_BUILDERS.get(transformation.type)
        if builder is None:
            raise SdpCompilationError(
                f"Validator '{transformation.type}' has no SDP expectation mapping. "
                f"Supported: {sorted(_EXPECTATION_BUILDERS)}"
            )
        action = SdpExpectationAction(transformation.params.get("on_violation", "warn"))
        return SdpExpectation(
            name=transformation.name,
            action=action,
            constraints=builder(transformation),
        )

    def _dataset_name(self, task_id: str) -> str:
        override = self.options.dataset_names.get(task_id)
        return _sanitize(override or task_id)


def compile_pipeline(pipeline: Pipeline, options: SdpOptions | None = None) -> SdpPipelineGraph:
    """Compile a pipeline into an SDP dataset graph."""
    return SdpCompiler(pipeline, options).compile()


def _location_key(location: Source | Target) -> tuple[str, str]:
    return (location.edge_node, location.path.rstrip("/"))


def _sanitize(name: str) -> str:
    sanitized = re.sub(r"\W", "_", name).strip("_")
    if not sanitized:
        raise SdpCompilationError(f"Cannot derive an SDP dataset name from '{name}'.")
    if sanitized[0].isdigit():
        sanitized = f"ds_{sanitized}"
    return sanitized


def _required_cols(transformation: Transformation, validator: str) -> list[str]:
    cols = transformation.input_cols or []
    if not cols:
        raise SdpCompilationError(f"'{validator}' expectation requires input_cols.")
    return cols


def _not_null_constraints(transformation: Transformation) -> dict[str, str]:
    cols = _required_cols(transformation, "not_null")
    return {f"{col}_not_null": f"{col} IS NOT NULL" for col in cols}


def _unique_constraints(transformation: Transformation) -> dict[str, str]:
    cols = _required_cols(transformation, "unique")
    partition = ", ".join(cols)
    key = "_".join(cols)
    return {f"{key}_unique": f"COUNT(*) OVER (PARTITION BY {partition}) = 1"}


def _regex_constraints(transformation: Transformation) -> dict[str, str]:
    cols = _required_cols(transformation, "regex")
    pattern = transformation.params.get("pattern")
    if not pattern:
        raise SdpCompilationError("'regex' expectation requires params.pattern.")
    escaped = pattern.replace("'", "\\'")
    return {f"{col}_matches_pattern": f"{col} RLIKE '{escaped}'" for col in cols}


def _range_constraints(transformation: Transformation) -> dict[str, str]:
    cols = _required_cols(transformation, "range")
    min_val = transformation.params.get("min")
    max_val = transformation.params.get("max")
    if min_val is None or max_val is None:
        raise SdpCompilationError("'range' expectation requires params.min and params.max.")
    return {f"{col}_in_range": f"{col} BETWEEN {min_val} AND {max_val}" for col in cols}


_EXPECTATION_BUILDERS = {
    "not_null": _not_null_constraints,
    "unique": _unique_constraints,
    "regex": _regex_constraints,
    "range": _range_constraints,
}


def _topological_sort(datasets: dict[str, SdpDataset]) -> list[SdpDataset]:
    remaining = dict(datasets)
    resolved: list[SdpDataset] = []
    resolved_names: set[str] = set()

    while remaining:
        ready = [
            dataset
            for dataset in remaining.values()
            if all(name in resolved_names or name not in datasets for name in dataset.upstream)
        ]
        if not ready:
            raise SdpCompilationError(
                f"Cyclic data dependency between SDP datasets: {sorted(remaining)}"
            )
        for dataset in ready:
            resolved.append(dataset)
            resolved_names.add(dataset.name)
            del remaining[dataset.name]
    return resolved


_MODULE_HEADER = '''"""Spark Declarative Pipelines definitions generated by ufw-mock.

Pipeline: {pipeline_name}
Do not edit by hand: regenerate with `ufw-run --config <pipeline.json> --platform sdp`.
"""

import json

try:
    # `sdp` may be injected by the caller (see SdpPipelineGraph.register).
    sdp  # noqa: B018
except NameError:
    try:
        import pipelines as sdp  # type: ignore
    except ImportError:
        try:
            import dlt as sdp  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "Spark Declarative Pipelines engine is unavailable: neither 'pipelines' nor "
                "'dlt' can be imported. This module only runs inside a Databricks/SDP pipeline."
            ) from exc

from ufw_mock.models.transformation import Transformation
from ufw_mock.runtime.transform_registry import TransformRegistry

_TRANSFORM_REGISTRY = TransformRegistry()

# Task transformations are already DataFrame -> DataFrame, so the generated
# dataset bodies reuse the UFW transform registry verbatim.
_TRANSFORMATIONS = json.loads(
    r"""
{transformations}
"""
)


def _apply_transformations(df, dataset):
    for spec in _TRANSFORMATIONS.get(dataset, []):
        df = _TRANSFORM_REGISTRY.apply(df, Transformation.model_validate(spec))
    return df
'''


def _emit_module(graph: SdpPipelineGraph) -> str:
    transformations = {
        dataset.name: [t.model_dump(mode="json") for t in dataset.transformations]
        for dataset in graph.datasets
    }
    parts = [
        _MODULE_HEADER.format(
            pipeline_name=graph.pipeline_name,
            transformations=json.dumps(transformations, indent=4),
        )
    ]
    for dataset in graph.datasets:
        parts.append(_emit_dataset(dataset))
    return "\n".join(parts)


def _emit_dataset(dataset: SdpDataset) -> str:
    lines = [
        "",
        f"@sdp.{dataset.decorator}(",
        f"    name={dataset.qualified_name!r},",
        f"    comment={f'UFW task {dataset.task_id} ({dataset.task_type.value})'!r},",
        ")",
    ]
    for expectation in dataset.expectations:
        lines.append(f"@sdp.{expectation.decorator}({json.dumps(expectation.constraints)})")
    lines.append(f"def {dataset.name}():")
    lines.extend(f"    {line}" for line in _emit_body(dataset))
    lines.append("")
    return "\n".join(lines)


def _emit_body(dataset: SdpDataset) -> list[str]:
    if dataset.upstream:
        upstream = dataset.upstream[0]
        reader = "sdp.read_stream" if dataset.is_streaming else "sdp.read"
        body = [f"df = {reader}({upstream!r})"]
    else:
        source = dataset.source
        if source is None:
            raise SdpCompilationError(f"Dataset '{dataset.name}' has neither upstream nor source.")
        if dataset.is_streaming:
            # Auto Loader-style incremental read for pipeline entry points.
            body = [
                "df = (",
                "    spark.readStream.format('cloudFiles')",
                f"    .option('cloudFiles.format', {source.format.value!r})",
                f"    .load({source.path!r})",
                ")",
            ]
        else:
            body = [f"df = spark.read.format({source.format.value!r}).load({source.path!r})"]
    body.append(f"return _apply_transformations(df, {dataset.name!r})")
    return body
