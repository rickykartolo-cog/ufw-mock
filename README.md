# ufw-mock

A mock / reference implementation of UFW's Spark abstraction layer.

## Overview

`ufw-mock` is a lightweight, opinionated PySpark framework that mirrors the core
constructs of Westpac's UFW (Unified Framework for Workloads):

- **Pipeline** — a declarative, JSON-driven workflow of tasks.
- **Task** — a unit of work: `INGEST`, `TRANSFORM`, `VALIDATE`, or `PUBLISH`.
- **Edge Node** — a pluggable source/target connector abstraction.
- **Format** — pluggable storage adapters for Parquet, Iceberg, and Delta.
- **Platform** — pluggable runtime for local PySpark, Databricks, and Spark Declarative
  Pipelines (SDP).

## Requirements

- Python 3.10+
- Java 8, 11, or 17 (required by PySpark)
- `uv` or `pip` for dependency management

## Quick start

```bash
# Install in editable mode with dev dependencies
uv pip install -e ".[dev]"

# Validate an example pipeline
ufw-run --config examples/kyc_pipeline.json --validate-only

# Run tests
pytest -q

# Compile a pipeline into SDP dataset definitions
ufw-run --config examples/kyc_pipeline_sdp.json
```

## Platforms

| `platform.name` | Adapter | Behaviour |
| --- | --- | --- |
| `local_pyspark` | `LocalPySparkPlatform` | Creates a local `SparkSession` and runs tasks sequentially. |
| `databricks` | `DatabricksPlatform` | Stub/contract: reuses the Databricks-managed `SparkSession`. |
| `sdp` | `SparkDeclarativePipelinesPlatform` | Compiles the pipeline into Spark Declarative Pipelines dataset definitions. |

## Spark Declarative Pipelines (SDP)

The real SDP decorator API (`pipelines`, historically `dlt`) only exists inside a
Databricks/SDP pipeline, so the `sdp` platform **compiles and emits** dataset definitions
instead of requiring a live SDP engine:

- `PipelineRunner` takes an alternate branch and calls `compile_pipeline()`
  (`src/ufw_mock/runtime/sdp_compiler.py`) rather than the sequential `TaskExecutor` loop.
- Datasets are wired into a DAG by data dependency: each task's `target` (edge node + path) is
  matched against downstream tasks' `source`, so task list order does not matter.
- `INGEST` maps to an Auto Loader-style streaming table, `TRANSFORM`/`PUBLISH` to materialized
  views or tables, and `VALIDATE` transformations to SDP expectations (`@sdp.expect_all`,
  `@sdp.expect_all_or_drop`, `@sdp.expect_all_or_fail`) attached to the dataset they validate.
- Generated dataset bodies reuse the existing `TransformRegistry`, since transforms are already
  `DataFrame -> DataFrame`.
- The output is a dependency-ordered `SdpPipelineGraph` plus a generated Python module
  (`graph.to_python_module()`). Both the module and the platform adapter guard the
  `pipelines`/`dlt` import and fail with a clear error when the SDP engine is unavailable, so
  compilation works locally while registration only runs inside a Databricks/SDP pipeline.

SDP-specific options live under `platform.sdp` in the pipeline JSON:

```json
{
  "platform": {
    "name": "sdp",
    "sdp": {
      "catalog": "main",
      "schema": "kyc",
      "dataset_names": {"ingest-customer-raw": "customers_bronze"},
      "ingest_materialization": "streaming_table",
      "derived_materialization": "materialized_view"
    }
  }
}
```

Per-task overrides can be supplied via `task.properties.sdp`
(`materialization`, `catalog`, `schema`).

## Project layout

```
src/ufw_mock/      # Framework code
examples/          # Example pipeline JSON configs (incl. an SDP example)
tests/             # Unit and integration tests
docs/              # PRD and reference architecture
```

## License

MIT
