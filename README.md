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

## Dependency-resolved execution

Tasks may declare upstream dependencies, and the runner topologically sorts them
before execution (SDP-style declarative graph rather than authoring order):

```json
{
  "id": "publish-customer-outbound",
  "type": "PUBLISH",
  "depends_on": ["validate-customer-quality"],
  "output_dataset": "outbound.customers"
}
```

A task also implicitly depends on the task whose `output_dataset` matches its
`source.path` (or `source.properties.dataset`). Cycles, unknown dependency ids,
and duplicate task ids raise errors before anything runs. Pipelines that declare
no dependencies execute in authoring order exactly as before — see
`examples/kyc_pipeline_dag.json` for a declared-dependency pipeline.

## Project layout

```
src/ufw_mock/      # Framework code
examples/          # Example pipeline JSON configs
tests/             # Unit and integration tests
docs/              # PRD and reference architecture
```

## License

MIT
