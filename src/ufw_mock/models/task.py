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
    depends_on: list[str] = Field(
        default_factory=list,
        description="Ids of upstream tasks that must run before this task.",
    )
    output_dataset: str | None = Field(
        default=None,
        description="Name of the dataset this task produces, referenced by downstream tasks.",
    )
    properties: dict = Field(default_factory=dict)
