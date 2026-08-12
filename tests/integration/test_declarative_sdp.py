import importlib.util

import pytest

from ufw_mock.declarative.compiler import SdpPipelineCompiler
from ufw_mock.declarative.registrar import run_declarative_pipeline
from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform
from ufw_mock.runtime.platform import get_platform


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("pyspark.pipelines") is None,
    reason="pyspark[pipelines] is not installed",
)


def test_programmatic_sdp_pipeline(tmp_path):
    pipeline = Pipeline(
        name="sdp-integration",
        declarative={"storage": (tmp_path / "storage").resolve().as_uri()},
        tasks=[
            {
                "id": "input",
                "type": "INGEST",
                "source": {"edge_node": "files", "path": str(tmp_path / "input")},
                "target": {"edge_node": "files", "path": str(tmp_path / "raw")},
                "properties": {"dataset_name": f"sdp_input_{tmp_path.name}"},
            },
            {
                "id": "output",
                "type": "TRANSFORM",
                "source": {"edge_node": "files", "path": str(tmp_path / "raw")},
                "target": {"edge_node": "files", "path": str(tmp_path / "output")},
                "properties": {"dataset_name": f"sdp_output_{tmp_path.name}"},
                "transformations": [
                    {"name": "select-id", "type": "select", "input_cols": ["id"]},
                ],
            },
        ],
    )
    from pyspark.sql import SparkSession

    spark_writer = SparkSession.builder.master("local[1]").appName("sdp-input").getOrCreate()
    spark_writer.createDataFrame([(1,), (2,)], ["id"]).write.mode("overwrite").parquet(
        str(tmp_path / "input")
    )
    spark_writer.stop()

    definitions = SdpPipelineCompiler(pipeline).compile()
    assert definitions[1].upstream == (definitions[0].name,)

    platform = get_platform(Platform(name="spark_declarative", config={"remote": "local"}))
    try:
        summary = run_declarative_pipeline(pipeline, platform)
        assert summary["graph_status"] == "succeeded"
        assert summary["publish_status"] == "succeeded"
        legacy_path = tmp_path / "output"
        assert list(legacy_path.glob("*.parquet"))
        spark = platform.get_spark_session("sdp-integration-check")
        rows = sorted(row.id for row in spark.read.parquet(str(legacy_path)).collect())
        assert rows == [1, 2]
    finally:
        platform.shutdown()


def _dq_pipeline(tmp_path, suffix: str, fail_on_violation: bool) -> Pipeline:
    return Pipeline(
        name=f"sdp-dq-{suffix}",
        declarative={
            "storage": (tmp_path / f"storage-{suffix}").resolve().as_uri(),
            "dq": {"fail_on_violation": fail_on_violation},
        },
        tasks=[
            {
                "id": "validate",
                "type": "VALIDATE",
                "source": {"edge_node": "files", "path": str(tmp_path / "dq-input")},
                "target": {"edge_node": "files", "path": ""},
                "properties": {"dataset_name": f"dq_validate_{suffix}"},
                "transformations": [
                    {"name": "unique-id", "type": "unique", "input_cols": ["id"]},
                    {
                        "name": "email-format",
                        "type": "regex",
                        "input_cols": ["email"],
                        "params": {"pattern": "^[^@]+@[^@]+$"},
                    },
                ],
            }
        ],
    )


def test_declarative_dq_is_non_blocking_and_can_gate_after_run(tmp_path):
    from pyspark.sql import SparkSession

    spark_writer = SparkSession.builder.master("local[1]").appName("sdp-dq-input").getOrCreate()
    spark_writer.createDataFrame(
        [(1, "good@example.com"), (1, "not-an-email"), (2, "also@example.com")],
        ["id", "email"],
    ).write.mode("overwrite").parquet(str(tmp_path / "dq-input"))
    spark_writer.stop()

    pipeline = _dq_pipeline(tmp_path, "report", fail_on_violation=False)
    definitions = SdpPipelineCompiler(pipeline).compile()
    assert [definition.name for definition in definitions] == [
        "dq_validate_report",
        "dq_validate_report_dq_failures",
        "dq_validate_report_dq_summary",
    ]
    platform = get_platform(Platform(name="spark_declarative", config={"remote": "local"}))
    try:
        summary = run_declarative_pipeline(pipeline, platform)
        assert summary["status"] == "succeeded"
        assert summary["tasks"] == [
            {"id": "validate", "dataset": "dq_validate_report", "status": "passed_with_violations"}
        ]
        assert summary["dq_violations"]["validate"] == [
            {"rule": "unique-id", "columns": "id", "count": 2},
            {"rule": "email-format", "columns": "email", "count": 1},
        ]
        spark = platform.get_spark_session("sdp-dq-readback")
        assert spark.read.table("dq_validate_report_dq_failures").count() == 3
        assert spark.read.table("dq_validate_report_dq_summary").count() == 2
    finally:
        platform.shutdown()

    gated_pipeline = _dq_pipeline(tmp_path, "gate", fail_on_violation=True)
    gated_platform = get_platform(Platform(name="spark_declarative", config={"remote": "local"}))
    try:
        gated_summary = run_declarative_pipeline(gated_pipeline, gated_platform)
        assert gated_summary["status"] == "failed"
        assert gated_summary["graph_status"] == "succeeded"
        spark = gated_platform.get_spark_session("sdp-dq-gated-readback")
        assert spark.read.table("dq_validate_gate_dq_summary").count() == 2
    finally:
        gated_platform.shutdown()
