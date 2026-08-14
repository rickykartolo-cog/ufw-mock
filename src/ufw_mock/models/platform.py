from typing import Any

from pydantic import BaseModel, Field

from ufw_mock.types import PlatformType, SdpMaterialization


class SdpOptions(BaseModel):
    """Spark Declarative Pipelines (SDP) specific options."""

    model_config = {"populate_by_name": True}

    catalog: str | None = Field(default=None, description="Target catalog for emitted datasets.")
    schema_name: str | None = Field(
        default=None, alias="schema", description="Target schema/database for emitted datasets."
    )
    dataset_names: dict[str, str] = Field(
        default_factory=dict, description="Task id -> dataset name overrides."
    )
    ingest_materialization: SdpMaterialization = Field(
        default=SdpMaterialization.STREAMING_TABLE,
        description="Materialization used for INGEST tasks.",
    )
    derived_materialization: SdpMaterialization = Field(
        default=SdpMaterialization.MATERIALIZED_VIEW,
        description="Materialization used for TRANSFORM and PUBLISH tasks.",
    )
    module_name: str = Field(default="ufw_sdp_pipeline", description="Name of the emitted module.")


class Platform(BaseModel):
    """Runtime environment configuration."""

    name: PlatformType = Field(default=PlatformType.LOCAL_PYSPARK)
    config: dict[str, Any] = Field(default_factory=dict)
    sdp: SdpOptions = Field(default_factory=SdpOptions, description="SDP-specific options.")
