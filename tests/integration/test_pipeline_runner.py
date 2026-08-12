import json
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform
from ufw_mock.runtime.pipeline_runner import PipelineRunner
from ufw_mock.runtime.platform import LocalPySparkPlatform, get_platform
from ufw_mock.runtime.task_executor import TaskExecutor
from ufw_mock.runtime.edge_node_registry import EdgeNodeRegistry


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("integration-tests").master("local[*]").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def tmp_pipeline(tmp_path):
    source_path = tmp_path / "source"
    raw_path = tmp_path / "raw"
    curated_path = tmp_path / "curated"
    outbound_path = tmp_path / "outbound"

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            {"customer_id": "1", "email": "a@example.com", "country": "au", "income": 50000},
            {"customer_id": "2", "email": "b@example.com", "country": "nz", "income": 120000},
            {"customer_id": "3", "email": None, "country": "us", "income": 80000},
        ]
    )
    df.write.mode("overwrite").parquet(str(source_path))

    pipeline = Pipeline(
        name="test-kyc",
        edge_nodes=[],
        tasks=[
            {
                "id": "ingest",
                "type": "INGEST",
                "source": {"edge_node": "inbound", "path": str(source_path), "format": "parquet"},
                "target": {"edge_node": "raw", "path": str(raw_path), "format": "parquet", "mode": "overwrite"},
                "transformations": [],
            },
            {
                "id": "transform",
                "type": "TRANSFORM",
                "source": {"edge_node": "raw", "path": str(raw_path), "format": "parquet"},
                "target": {"edge_node": "curated", "path": str(curated_path), "format": "parquet", "mode": "overwrite"},
                "transformations": [
                    {"name": "drop-null-emails", "type": "filter", "params": {"condition": "email IS NOT NULL"}},
                    {"name": "uppercase-country", "type": "uppercase", "input_cols": ["country"], "output_col": "country_standardised"},
                ],
            },
            {
                "id": "validate",
                "type": "VALIDATE",
                "source": {"edge_node": "curated", "path": str(curated_path), "format": "parquet"},
                "target": {"edge_node": "curated", "path": "", "format": "parquet"},
                "transformations": [
                    {"name": "unique-id", "type": "unique", "input_cols": ["customer_id"]},
                    {"name": "not-null-email", "type": "not_null", "input_cols": ["email"]},
                ],
            },
            {
                "id": "publish",
                "type": "PUBLISH",
                "source": {"edge_node": "curated", "path": str(curated_path), "format": "parquet"},
                "target": {"edge_node": "outbound", "path": str(outbound_path), "format": "parquet", "mode": "overwrite"},
                "transformations": [
                    {"name": "select", "type": "select", "input_cols": ["customer_id", "email", "country_standardised"]},
                ],
            },
        ],
    )
    return pipeline


def test_pipeline_runner(tmp_pipeline):
    platform = LocalPySparkPlatform()
    with PipelineRunner(tmp_pipeline, platform) as runner:
        summary = runner.run()
    assert summary["status"] == "success"
    assert len(summary["tasks"]) == 4


def test_validation_failure(spark, tmp_path):
    source_path = tmp_path / "source"
    raw_path = tmp_path / "raw"
    df = spark.createDataFrame(
        [
            {"customer_id": "1", "email": "a@example.com"},
            {"customer_id": "1", "email": "b@example.com"},
        ]
    )
    df.write.mode("overwrite").parquet(str(source_path))

    pipeline = Pipeline(
        name="test-validate-fail",
        edge_nodes=[],
        tasks=[
            {
                "id": "validate",
                "type": "VALIDATE",
                "source": {"edge_node": "raw", "path": str(source_path), "format": "parquet"},
                "target": {"edge_node": "raw", "path": "", "format": "parquet"},
                "transformations": [
                    {"name": "unique", "type": "unique", "input_cols": ["customer_id"]},
                ],
            },
        ],
    )

    platform = LocalPySparkPlatform()
    with pytest.raises(Exception) as exc_info:
        with PipelineRunner(pipeline, platform) as runner:
            runner.run()
    assert "not unique" in str(exc_info.value).lower() or "validation failed" in str(exc_info.value).lower()
