import importlib.util

import pytest

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

    platform = get_platform(Platform(name="spark_declarative", config={"remote": "local"}))
    try:
        summary = run_declarative_pipeline(pipeline, platform)
        assert summary["graph_status"] == "succeeded"
        assert summary["publish_status"] == "succeeded"
    finally:
        platform.shutdown()
