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
    runner = _runner([_task("c", ["b"]), _task("b", ["a"]), _task("a")])
    summary = runner.run()
    assert runner.task_executor.executed == ["a", "b", "c"]
    assert summary["status"] == "success"
    assert summary["tasks"] == [
        {"id": "a", "status": "success"},
        {"id": "b", "status": "success"},
        {"id": "c", "status": "success"},
    ]


def test_run_preserves_authoring_order_without_dependencies():
    runner = _runner([_task("a"), _task("b"), _task("c")])
    runner.run()
    assert runner.task_executor.executed == ["a", "b", "c"]
