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

## Project layout

```
src/ufw_mock/      # Framework code
examples/          # Example pipeline JSON configs
tests/             # Unit and integration tests
docs/              # PRD and reference architecture
```

## License

MIT
