from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ufw_mock.models.source_target import Source, Target


class Format(ABC):
    """Abstract storage format adapter."""

    @abstractmethod
    def read(self, spark: SparkSession, path: str, source: Source) -> DataFrame:
        """Read data from the given path/source."""

    @abstractmethod
    def write(self, df: DataFrame, path: str, target: Target) -> None:
        """Write data to the given path/target."""
