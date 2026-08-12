from typing import Any

from pydantic import BaseModel, Field

from ufw_mock.types import StorageFormat, WriteMode


class SchemaRef(BaseModel):
    """Reference to a schema definition, either inline or external."""

    catalog: str | None = None
    database: str | None = None
    table: str | None = None
    inline: dict[str, str] | None = None


class Source(BaseModel):
    """Input location and format for a task."""

    model_config = {"populate_by_name": True}

    edge_node: str = Field(..., description="Logical name of the edge node to read from.")
    path: str = Field(..., description="Path or identifier within the edge node.")
    format: StorageFormat = Field(default=StorageFormat.PARQUET)
    schema_ref: SchemaRef | None = Field(default=None, alias="schema")
    properties: dict[str, Any] = Field(default_factory=dict)


class Target(BaseModel):
    """Output location and format for a task."""

    model_config = {"populate_by_name": True}

    edge_node: str = Field(..., description="Logical name of the edge node to write to.")
    path: str = Field(..., description="Path or identifier within the edge node.")
    format: StorageFormat = Field(default=StorageFormat.PARQUET)
    mode: WriteMode = Field(default=WriteMode.OVERWRITE)
    partition_by: list[str] | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
