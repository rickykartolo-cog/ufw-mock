from enum import Enum


class TaskType(str, Enum):
    INGEST = "INGEST"
    TRANSFORM = "TRANSFORM"
    VALIDATE = "VALIDATE"
    PUBLISH = "PUBLISH"


class EdgeDirection(str, Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class EdgeProtocol(str, Enum):
    FILE = "file"
    KAFKA = "kafka"
    REST = "rest"
    JDBC = "jdbc"


class StorageFormat(str, Enum):
    PARQUET = "parquet"
    ICEBERG = "iceberg"
    DELTA = "delta"
    JSON = "json"
    CSV = "csv"


class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"
    IGNORE = "ignore"
    ERROR_IF_EXISTS = "error_if_exists"


class PlatformType(str, Enum):
    LOCAL_PYSPARK = "local_pyspark"
    DATABRICKS = "databricks"
    CLOUDERA = "cloudera"
