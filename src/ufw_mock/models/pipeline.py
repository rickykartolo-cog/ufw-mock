from pydantic import BaseModel, Field

from ufw_mock.models.edge_node import EdgeNode
from ufw_mock.models.task import Task


class Pipeline(BaseModel):
    """A declarative, JSON-driven workflow of tasks."""

    name: str = Field(..., description="Unique pipeline name.")
    version: str = Field(default="1.0.0")
    owner: str = Field(default="central-data-platform")
    schedule: str | None = Field(default=None, description="Optional cron expression.")
    properties: dict = Field(default_factory=dict)
    edge_nodes: list[EdgeNode] = Field(default_factory=list)
    tasks: list[Task] = Field(..., min_length=1)
