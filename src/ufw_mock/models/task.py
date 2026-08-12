from pydantic import BaseModel, Field

from ufw_mock.models.source_target import Source, Target
from ufw_mock.models.transformation import Transformation
from ufw_mock.types import TaskType


class Task(BaseModel):
    """A single stage inside a pipeline."""

    id: str = Field(..., description="Unique task identifier within the pipeline.")
    type: TaskType = Field(..., description="Task category: INGEST, TRANSFORM, VALIDATE, or PUBLISH.")
    source: Source
    target: Target
    transformations: list[Transformation] = Field(default_factory=list)
    properties: dict = Field(default_factory=dict)
