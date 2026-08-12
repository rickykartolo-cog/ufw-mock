from pyspark.sql import DataFrame, SparkSession

from ufw_mock.formats.base import Format
from ufw_mock.models.source_target import Source, Target


class DeltaFormat(Format):
    """Read and write Delta Lake tables.

    In a real environment this requires the Delta Spark connector. The mock
    provides the adapter contract; local execution without the Delta jar will
    raise a clear error.
    """

    def _check_delta(self, spark: SparkSession) -> None:
        try:
            spark.version
        except Exception as exc:
            raise RuntimeError("Spark session is not available") from exc
        # Delta requires the delta-core / delta-spark jar. There is no robust
        # runtime check without importing Java classes; we rely on the write
        # operation itself to fail clearly if the jar is missing.

    def read(self, spark: SparkSession, path: str, source: Source) -> DataFrame:
        self._check_delta(spark)
        return spark.read.format("delta").options(**source.properties).load(path)

    def write(self, df: DataFrame, path: str, target: Target) -> None:
        self._check_delta(df.sparkSession)
        writer = df.write.format("delta").mode(target.mode.value).options(**target.properties)
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        writer.save(path)
