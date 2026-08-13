from ufw_mock.models.pipeline import Pipeline
from ufw_mock.runtime.pipeline_runner import PipelineRunner


class _RecordingExecutor:
    def __init__(self):
        self.executed = []

    def execute(self, pipeline, task):
        self.executed.append(task.id)


class _StubPlatform:
    def shutdown(self):
        pass


def _task(task_id, depends_on=None):
    return {
        "id": task_id,
        "type": "TRANSFORM",
        "source": {"edge_node": "inbound", "path": "in"},
        "target": {"edge_node": "core", "path": "out"},
        "depends_on": depends_on or [],
    }


def _runner(tasks):
    pipeline = Pipeline(name="p", tasks=tasks)
    runner = PipelineRunner(pipeline, _StubPlatform())
    runner.task_executor = _RecordingExecutor()
    return runner


def test_run_executes_in_dependency_order():
    runner = _runner([_task("publish", ["clean"]), _task("clean", ["ingest"]), _task("ingest")])
    summary = runner.run()
    assert runner.task_executor.executed == ["ingest", "clean", "publish"]
    assert summary["status"] == "success"
    assert summary["tasks"] == [
        {"id": "ingest", "status": "success"},
        {"id": "clean", "status": "success"},
        {"id": "publish", "status": "success"},
    ]


def test_run_preserves_authoring_order_without_dependencies():
    runner = _runner([_task("publish"), _task("ingest"), _task("clean")])
    runner.run()
    assert runner.task_executor.executed == ["publish", "ingest", "clean"]
