from typing import Any

from pydantic import BaseModel, Field

from ufw_mock.types import PlatformType


class Platform(BaseModel):
    """Runtime environment configuration."""

    name: PlatformType = Field(default=PlatformType.LOCAL_PYSPARK)
    config: dict[str, Any] = Field(default_factory=dict)
