from typing import Any

from pydantic import BaseModel, Field


class Transformation(BaseModel):
    """A declarative transformation to be applied to a Spark DataFrame."""

    name: str = Field(..., description="Human-readable name for the transform step.")
    type: str = Field(..., description="Transform key, e.g. 'select', 'filter', 'custom.segment_by_risk'.")
    input_cols: list[str] | None = None
    output_col: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
