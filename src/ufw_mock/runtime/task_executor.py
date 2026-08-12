from pyspark.sql import DataFrame

from ufw_mock.formats import get_format
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.task import Task
from ufw_mock.models.transformation import Transformation
from ufw_mock.runtime.edge_node_registry import EdgeNodeRegistry
from ufw_mock.runtime.platform import PlatformAdapter
from ufw_mock.runtime.transform_registry import TransformRegistry
from ufw_mock.runtime.validation_registry import ValidationError, ValidationRegistry


class TaskExecutor:
    """Executes a single task within a pipeline."""

    def __init__(self, platform: PlatformAdapter, edge_registry: EdgeNodeRegistry) -> None:
        self.platform = platform
        self.edge_registry = edge_registry
        self.transform_registry = TransformRegistry()
        self.validation_registry = ValidationRegistry()

    def execute(self, pipeline: Pipeline, task: Task) -> None:
        spark = self.platform.get_spark_session(pipeline.name)

        df = self._read(spark, task.source)

        if task.type.value == "VALIDATE":
            self._validate(df, task.transformations)
        else:
            df = self._transform(df, task.transformations)

        # VALIDATE tasks may not need to write; but if a target is provided, mirror input.
        if task.type.value != "VALIDATE" or task.target.path:
            self._write(df, task.target)

    def _read(self, spark, source: Source) -> DataFrame:
        edge = self.edge_registry.get_edge(source.edge_node)
        resolved_path = edge.resolve(source.path)
        fmt = get_format(source.format)
        return fmt.read(spark, resolved_path, source)

    def _transform(self, df: DataFrame, transformations: list[Transformation]) -> DataFrame:
        for transformation in transformations:
            df = self.transform_registry.apply(df, transformation)
        return df

    def _validate(self, df: DataFrame, transformations: list[Transformation]) -> None:
        if not transformations:
            return
        errors: list[str] = []
        for transformation in transformations:
            try:
                self.validation_registry.apply(df, transformation)
            except ValidationError as exc:
                errors.append(str(exc))
        if errors:
            raise ValidationError("Validation failed:\n" + "\n".join(errors))

    def _write(self, df: DataFrame, target: Target) -> None:
        edge = self.edge_registry.get_edge(target.edge_node)
        resolved_path = edge.resolve(target.path)
        fmt = get_format(target.format)
        fmt.write(df, resolved_path, target)
