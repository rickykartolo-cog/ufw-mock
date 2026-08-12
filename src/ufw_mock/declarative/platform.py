from __future__ import annotations

from pyspark.sql import SparkSession

from ufw_mock.runtime.platform import PlatformAdapter


class SparkDeclarativePlatform(PlatformAdapter):
    """Spark Connect platform for Spark Declarative Pipelines."""

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._spark: SparkSession | None = None

    def get_spark_session(self, app_name: str) -> SparkSession:
        if self._spark is not None:
            return self._spark
        try:
            import pyspark.pipelines
        except ImportError as exc:
            raise RuntimeError(
                "Spark Declarative Pipelines is unavailable. Install "
                "'ufw-mock[declarative]' to add pyspark[pipelines]."
            ) from exc
        remote = self.config.get("remote", "local")
        builder = SparkSession.builder.remote(remote).appName(app_name)
        for key, value in self.config.items():
            if key != "remote":
                builder = builder.config(key, value)
        self._spark = builder.getOrCreate()
        return self._spark

    def shutdown(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
