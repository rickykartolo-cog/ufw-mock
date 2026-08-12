from typing import Any

from pydantic import BaseModel, Field

from ufw_mock.types import EdgeDirection, EdgeProtocol


class EdgeNode(BaseModel):
    """A contract boundary between source/target systems and the core platform."""

    name: str = Field(..., description="Logical name of the edge node.")
    direction: EdgeDirection = Field(..., description="Whether this edge receives or publishes data.")
    protocol: EdgeProtocol = Field(default=EdgeProtocol.FILE)
    owner: str = Field(default="central-data-platform")
    properties: dict[str, Any] = Field(default_factory=dict)
