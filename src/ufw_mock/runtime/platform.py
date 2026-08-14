from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from ufw_mock.models.platform import Platform as PlatformConfig
from ufw_mock.models.platform import SdpOptions
from ufw_mock.types import PlatformType


class PlatformAdapter(ABC):
    """Abstract runtime platform adapter."""

    @abstractmethod
    def get_spark_session(self, app_name: str) -> SparkSession:
        """Return or create a SparkSession for this platform."""

    @abstractmethod
    def shutdown(self) -> None:
        """Clean up platform resources."""


class LocalPySparkPlatform(PlatformAdapter):
    """Local PySpark runtime."""

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._spark: SparkSession | None = None

    def get_spark_session(self, app_name: str) -> SparkSession:
        if self._spark is None or self._spark.sparkContext._jsc is None:
            builder = SparkSession.builder.appName(app_name)
            for key, value in self.config.items():
                builder = builder.config(key, value)
            self._spark = builder.getOrCreate()
        return self._spark

    def shutdown(self) -> None:
        if self._spark and not self._spark.sparkContext._jsc.sc().isStopped():
            self._spark.stop()
        self._spark = None


class DatabricksPlatform(PlatformAdapter):
    """Databricks runtime adapter (stub/contract for the mock)."""

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._spark: SparkSession | None = None

    def get_spark_session(self, app_name: str) -> SparkSession:
        # In Databricks, `spark` is already available in the notebook/job context.
        # The mock captures the contract; a real implementation would integrate with
        # Databricks Connect or the existing `spark` singleton.
        if self._spark is None:
            try:
                self._spark = SparkSession.builder.getOrCreate()
            except Exception as exc:
                raise RuntimeError(
                    "DatabricksPlatform requires a Databricks Spark session. "
                    "Run inside a Databricks notebook or job."
                ) from exc
        return self._spark

    def shutdown(self) -> None:
        # Do not stop the Databricks-managed Spark session.
        self._spark = None


class SparkDeclarativePipelinesPlatform(PlatformAdapter):
    """Spark Declarative Pipelines (SDP) runtime adapter (stub/contract for the mock).

    The real `dlt`/`pipelines` API is only importable inside an SDP pipeline
    context, so this adapter provides the Spark session contract and a guarded
    accessor for the SDP module. Pipelines targeting this platform are compiled
    into SDP dataset definitions rather than executed sequentially.
    """

    def __init__(self, config: dict | None = None, sdp_options: SdpOptions | None = None) -> None:
        self.config = config or {}
        self.sdp_options = sdp_options or SdpOptions()
        self._spark: SparkSession | None = None

    def get_spark_session(self, app_name: str) -> SparkSession:
        # Inside an SDP pipeline the session is created and owned by the engine.
        if self._spark is None:
            try:
                self._spark = SparkSession.builder.getOrCreate()
            except Exception as exc:
                raise RuntimeError(
                    "SparkDeclarativePipelinesPlatform requires a Spark session. "
                    "Run inside a Databricks/SDP pipeline."
                ) from exc
        return self._spark

    def get_sdp_module(self):
        """Return the SDP decorator module (`pipelines` or `dlt`).

        Raises a clear error when the SDP engine is unavailable, mirroring the
        Databricks adapter contract.
        """
        try:
            import pipelines as sdp  # type: ignore
        except ImportError:
            try:
                import dlt as sdp  # type: ignore
            except ImportError as exc:
                raise RuntimeError(
                    "Spark Declarative Pipelines engine is unavailable: neither 'pipelines' "
                    "nor 'dlt' can be imported. Dataset definitions can still be compiled and "
                    "emitted, but registration only works inside a Databricks/SDP pipeline."
                ) from exc
        return sdp

    def shutdown(self) -> None:
        # Do not stop the SDP-managed Spark session.
        self._spark = None


def get_platform(platform_config: PlatformConfig) -> PlatformAdapter:
    if platform_config.name == PlatformType.LOCAL_PYSPARK:
        return LocalPySparkPlatform(platform_config.config)
    if platform_config.name == PlatformType.DATABRICKS:
        return DatabricksPlatform(platform_config.config)
    if platform_config.name == PlatformType.SPARK_DECLARATIVE_PIPELINES:
        return SparkDeclarativePipelinesPlatform(platform_config.config, platform_config.sdp)
    raise ValueError(f"Unsupported platform: {platform_config.name}")
