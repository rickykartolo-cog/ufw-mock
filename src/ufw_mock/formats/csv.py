from pyspark.sql import DataFrame, SparkSession

from ufw_mock.formats.base import Format
from ufw_mock.models.source_target import Source, Target


class CsvFormat(Format):
    """Read and write CSV files."""

    def read(self, spark: SparkSession, path: str, source: Source) -> DataFrame:
        reader = spark.read.format("csv").option("header", "true")
        for key, value in source.properties.items():
            reader = reader.option(key, value)
        return reader.load(path)

    def write(self, df: DataFrame, path: str, target: Target) -> None:
        writer = (
            df.write.format("csv")
            .mode(target.mode.value)
            .option("header", "true")
            .options(**target.properties)
        )
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        writer.save(path)
