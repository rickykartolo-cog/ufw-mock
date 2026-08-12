from ufw_mock.formats.base import Format
from ufw_mock.formats.csv import CsvFormat
from ufw_mock.formats.delta import DeltaFormat
from ufw_mock.formats.iceberg import IcebergFormat
from ufw_mock.formats.json import JsonFormat
from ufw_mock.formats.parquet import ParquetFormat
from ufw_mock.types import StorageFormat

_FORMATS: dict[StorageFormat, type[Format]] = {
    StorageFormat.PARQUET: ParquetFormat,
    StorageFormat.JSON: JsonFormat,
    StorageFormat.CSV: CsvFormat,
    StorageFormat.ICEBERG: IcebergFormat,
    StorageFormat.DELTA: DeltaFormat,
}


def get_format(storage_format: StorageFormat) -> Format:
    try:
        return _FORMATS[storage_format]()
    except KeyError as exc:
        raise ValueError(f"Unsupported storage format: {storage_format}") from exc


__all__ = [
    "Format",
    "ParquetFormat",
    "JsonFormat",
    "CsvFormat",
    "IcebergFormat",
    "DeltaFormat",
    "get_format",
]
