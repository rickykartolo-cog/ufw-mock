from pydantic import BaseModel, Field, model_validator

from ufw_mock.models.edge_node import EdgeNode
from ufw_mock.models.task import Task


class DeclarativeDQConfig(BaseModel):
    mode: str = Field(default="in_graph")
    fail_on_violation: bool = False

    @model_validator(mode="after")
    def validate_mode(self) -> "DeclarativeDQConfig":
        if self.mode != "in_graph":
            raise ValueError("declarative.dq.mode must be 'in_graph'.")
        return self


class DeclarativeConfig(BaseModel):
    storage: str = Field(..., min_length=1)
    catalog: str | None = None
    database: str | None = None
    configuration: dict[str, str] = Field(default_factory=dict)
    publish_legacy_paths: bool = True
    dq: DeclarativeDQConfig = Field(default_factory=DeclarativeDQConfig)

    @model_validator(mode="after")
    def validate_configuration(self) -> "DeclarativeConfig":
        disallowed = {"spark.sql.warehouse.dir"} & set(self.configuration)
        if disallowed:
            names = ", ".join(sorted(disallowed))
            raise ValueError(
                f"declarative.configuration contains disallowed static Spark config key(s): {names}"
            )
        return self


class Pipeline(BaseModel):
    """A declarative, JSON-driven workflow of tasks."""

    name: str = Field(..., description="Unique pipeline name.")
    version: str = Field(default="1.0.0")
    owner: str = Field(default="central-data-platform")
    schedule: str | None = Field(default=None, description="Optional cron expression.")
    properties: dict = Field(default_factory=dict)
    declarative: DeclarativeConfig | None = None
    edge_nodes: list[EdgeNode] = Field(default_factory=list)
    tasks: list[Task] = Field(..., min_length=1)
