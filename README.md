# ufw-mock

A mock / reference implementation of UFW's Spark abstraction layer.

## Overview

`ufw-mock` is a lightweight, opinionated PySpark framework that mirrors the core
constructs of Westpac's UFW (Unified Framework for Workloads):

- **Pipeline** — a declarative, JSON-driven workflow of tasks.
- **Task** — a unit of work: `INGEST`, `TRANSFORM`, `VALIDATE`, or `PUBLISH`.
- **Edge Node** — a pluggable source/target connector abstraction.
- **Format** — pluggable storage adapters for Parquet, Iceberg, and Delta.
- **Platform** — pluggable runtime for local PySpark and Databricks.

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
```

## Declarative mode (Spark Declarative Pipelines)

The same pipeline JSON can run through Spark Declarative Pipelines in batch
mode:

```bash
uv pip install -e ".[declarative]"
ufw-run --config examples/kyc_pipeline_runnable.json --mode declarative
```

Declarative mode uses Spark Connect. The optional `declarative` dependency
installs `pyspark[pipelines]>=4.1`; it is not installed by the default
dependency set.

Add a top-level `declarative` block to a pipeline when using this mode:

```json
{
  "declarative": {
    "storage": "file:///tmp/ufw-mock/pipeline-storage",
    "catalog": "spark_catalog",
    "database": "default",
    "configuration": {
      "spark.sql.shuffle.partitions": "2"
    },
    "publish_legacy_paths": true,
    "dq": {
      "mode": "in_graph",
      "fail_on_violation": false
    }
  }
}
```

`storage` is the SDP pipeline storage location. `catalog` and `database`
provide the graph defaults, `configuration` contains Spark SQL settings,
`publish_legacy_paths` controls export to existing target paths, and `dq`
accepts the current DQ settings. `spark.sql.warehouse.dir` is rejected in
`configuration` because it is a static Spark setting.

Use `--dry-run` to validate the graph without materializing datasets. It
prints the mapping from each non-empty `target.path` to its generated dataset
name. Use `--full-refresh-all` to request a full refresh of all declarative
datasets:

```bash
ufw-run --config examples/kyc_pipeline_runnable.json \
  --mode declarative --dry-run
ufw-run --config examples/kyc_pipeline_runnable.json \
  --mode declarative --full-refresh-all
```

Dataset names default to a SQL-safe slug of the task ID and can be overridden
with `task.properties.dataset_name`. Internal task dependencies read the
upstream named dataset. After the graph completes, the compatibility shim
reads each produced dataset and writes it to the configured `target.path`
through the existing format adapter, so declarative execution currently has
the cost of writing the dataset and then exporting it to the legacy path.

The current declarative backend supports batch materialized views only.
Streaming datasets, `MERGE`, `ignore`, and `error_if_exists` write modes are
unsupported. A `VALIDATE` task remains in the graph as a pass-through dataset
and, when it has rules, also produces `<dataset>_dq_failures` and
`<dataset>_dq_summary` datasets. The failures dataset contains offending rows
with rule metadata; the summary contains one count per rule.

Declarative DQ is non-blocking by default. The run completes and the task is
reported as `passed_with_violations`. Set
`declarative.dq.fail_on_violation` to `true` to make the overall declarative
command fail after the graph completes; the DQ tables and other graph outputs
remain available. The same configuration can therefore fail fast in
imperative mode while succeeding with reported violations in declarative mode.

## Project layout

```
src/ufw_mock/      # Framework code
examples/          # Example pipeline JSON configs
tests/             # Unit and integration tests
docs/              # PRD and reference architecture
```

## License

MIT
