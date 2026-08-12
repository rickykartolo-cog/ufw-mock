from pyspark.sql import DataFrame, SparkSession

from ufw_mock.formats.base import Format
from ufw_mock.models.source_target import Source, Target


class IcebergFormat(Format):
    """Read and write Iceberg tables.

    In a real environment this requires the Iceberg Spark runtime jar and an
    Iceberg catalog configured on the Spark session. The mock provides the
    adapter contract; local execution without those dependencies will raise a
    clear error.
    """

    def _check_catalog(self, spark: SparkSession) -> None:
        catalogs = [key for key in spark.sparkContext.getConf().getAll() if "spark.sql.catalog" in key[0]]
        if not any("iceberg" in key[0].lower() for key in catalogs):
            raise NotImplementedError(
                "IcebergFormat requires an Iceberg catalog to be configured on the Spark session. "
                "For local development use ParquetFormat or install the Iceberg Spark runtime."
            )

    def read(self, spark: SparkSession, path: str, source: Source) -> DataFrame:
        self._check_catalog(spark)
        return spark.read.format("iceberg").options(**source.properties).load(path)

    def write(self, df: DataFrame, path: str, target: Target) -> None:
        self._check_catalog(df.sparkSession)
        writer = df.write.format("iceberg").mode(target.mode.value).options(**target.properties)
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        writer.save(path)
