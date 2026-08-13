import json

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform
from ufw_mock.runtime.dag_resolver import resolve_execution_order
from ufw_mock.runtime.edge_node_registry import EdgeNodeRegistry
from ufw_mock.runtime.platform import PlatformAdapter, get_platform
from ufw_mock.runtime.task_executor import TaskExecutor


class PipelineRunner:
    """Validates and executes a UFW pipeline definition."""

    def __init__(self, pipeline: Pipeline, platform_adapter: PlatformAdapter) -> None:
        self.pipeline = pipeline
        self.platform = platform_adapter
        self.edge_registry = EdgeNodeRegistry(pipeline.edge_nodes)
        self.task_executor = TaskExecutor(self.platform, self.edge_registry)

    def run(self) -> dict:
        summary = {"pipeline": self.pipeline.name, "tasks": []}
        for task in resolve_execution_order(self.pipeline.tasks):
            self.task_executor.execute(self.pipeline, task)
            summary["tasks"].append({"id": task.id, "status": "success"})
        summary["status"] = "success"
        return summary

    def __enter__(self) -> "PipelineRunner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.platform.shutdown()


def run_pipeline(config_path: str, platform_name: str = "local_pyspark") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    raw_platform = raw_config.get("platform", {})
    platform_name = raw_platform.get("name", platform_name)
    platform_config = Platform(name=platform_name, config=raw_platform.get("config", {}))
    pipeline = Pipeline.model_validate(raw_config)
    platform = get_platform(platform_config)

    with PipelineRunner(pipeline, platform) as runner:
        return runner.run()
