# UFW Mock — Reference Architecture

## 1. Context

Westpac's **Unified Framework for Workloads (UFW)** is a proprietary PySpark
framework built over 8–10 years on Confluent/Cloudera. This document describes
how the `ufw-mock` reference implementation maps UFW constructs to a modern
Databricks/Iceberg ecosystem while remaining data-platform-agnostic.

## 2. Target State Architecture

```
                    Source systems
                           │
                           ▼
            ┌──────────────────────────────┐
            │  Inbound Edge Node           │  Databricks: Unity Catalog Volumes
            │  (contract / landing zone)   │             + Autoloader / DLT
            └──────────────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │  UFW Runtime                 │  Databricks: Jobs / Workflows
            │  - JSON metadata             │
            │  - PySpark execution         │
            │  - Pluggable transforms      │
            └──────────────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │  Storage abstraction         │  Parquet (current)
            │  - Parquet                   │  Iceberg (strategic)
            │  - Iceberg                   │  Delta (Databricks native)
            │  - Delta                     │
            └──────────────────────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │  Outbound Edge Node          │  Databricks: Delta Sharing
            │  (curated / publish)         │             + Unity Catalog external locations
            └──────────────────────────────┘
                           │
                           ▼
                    Downstream consumers
```

## 3. Construct Mapping

| UFW Construct | Confluent/Cloudera Implementation | Databricks Target | ufw-mock Component |
|---------------|-----------------------------------|-------------------|--------------------|
| **Pipeline** | PySpark job orchestrated by UFW | Databricks Job / Workflow | `Pipeline` + `PipelineRunner` |
| **Task** | Ingest, transform, validate, publish stages | Same task types in Databricks tasks | `Task` + `TaskExecutor` |
| **Edge Node** | HDFS/S3 landing zone + Kafka topics | Unity Catalog volumes + Kafka/Delta Live Tables | `EdgeNode` + `Edge` adapters |
| **Source / Target** | HDFS paths, Kafka topics, JDBC tables | UC tables, volumes, Kafka, JDBC | `Source` / `Target` |
| **Format** | Parquet | Parquet / Iceberg / Delta | `Format` adapters |
| **Transform** | JSON metadata + custom PySpark | Same JSON metadata; runtime on Databricks Spark | `TransformRegistry` |
| **Validate** | Custom validators | Great Expectations / Delta constraints / custom | `ValidationRegistry` |
| **Platform** | Cloudera Spark cluster | Databricks runtime | `PlatformAdapter` |

## 4. Storage Format Migration Strategy

### Current state: Parquet

- UFW writes intermediate and outbound tables as Parquet files.
- `ParquetFormat` adapter in `ufw-mock` preserves this behaviour.

### Strategic target: Iceberg

- Iceberg provides time travel, hidden partitioning, and schema evolution.
- `IcebergFormat` adapter is the target contract. It requires:
  - Iceberg Spark runtime jar on the classpath.
  - An Iceberg catalog configured on the Spark session, e.g.:
    ```python
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "...",
    ```
- Pipelines migrate by changing the target `format` from `parquet` to `iceberg`.

### Intermediary / Databricks native: Delta

- Databricks' native table format is Delta.
- `DeltaFormat` adapter allows migration to Databricks without adopting Iceberg
  immediately.
- Delta supports MERGE, time travel, and Unity Catalog integration.

## 5. Platform Runtime Strategy

### Local PySpark (`LocalPySparkPlatform`)

- Used for development, CI, and unit/integration tests.
- Creates a `SparkSession` locally with `master("local[*]")`.
- Does not include Iceberg or Delta jars by default.

### Databricks (`DatabricksPlatform`)

- Production target.
- Reuses the existing `SparkSession` inside a Databricks notebook or job.
- Integrates with Unity Catalog, DLT, and Databricks Jobs.
- In the mock, this is a contract/stub; a real deployment would wire in
  Databricks-specific utilities.

### Spark Declarative Pipelines (`SparkDeclarativePlatform`)

Declarative mode compiles the existing `Pipeline` model into named Spark
Declarative Pipelines datasets and graph dependencies. It uses Spark Connect
and is enabled with the optional dependency:

```bash
uv pip install -e ".[declarative]"
```

The top-level `declarative` configuration block contains:

- `storage`: required SDP pipeline storage location.
- `catalog`: optional default catalog for the graph.
- `database`: optional default database for the graph.
- `configuration`: Spark SQL settings passed when creating the graph.
- `publish_legacy_paths`: whether completed datasets are exported to their
  existing target paths; defaults to `true`.
- `dq`: currently accepts `mode: "in_graph"` and `fail_on_violation`; DQ
  datasets are not part of the current backend.

`spark.sql.warehouse.dir` is rejected in `configuration` because Spark treats
it as a static setting. Run with:

```bash
ufw-run --config examples/kyc_pipeline_runnable.json --mode declarative
```

The declarative CLI also supports `--dry-run` and `--full-refresh-all`.
`--dry-run` prints the path-to-dataset mapping without publishing legacy
paths. Dataset names default to SQL-safe slugs of task IDs and can be
overridden with `task.properties.dataset_name`. A source path matching an
earlier task's target path reads that task's named dataset rather than
re-reading the file. After a successful graph run, the compatibility shim
reads each named dataset and writes it through the existing format adapter to
the original `target.path`; this preserves legacy consumers but incurs a
second write.

The current backend supports batch materialized views only. Streaming datasets,
`MERGE`, `ignore`, and `error_if_exists` are unsupported. `VALIDATE` tasks
compile as pass-through datasets until in-graph DQ support is added.

## 6. Edge Node Strategy

Edge nodes isolate source systems from the core platform. The mock supports
pluggable protocols:

| Protocol | Use case | Notes |
|----------|----------|-------|
| `file` | Batch files on HDFS/S3/ADLS | Default; `FileEdge` resolves base paths. |
| `kafka` | Streaming ingestion | `KafkaEdge` is a contract stub. |
| `rest` | API-driven sources | `RestEdge` is a contract stub. |
| `jdbc` | Relational databases | `JdbcEdge` is a contract stub. |

In Databricks, inbound edges map to:
- **Files**: Unity Catalog volumes + Auto Loader.
- **Kafka**: Databricks streaming Kafka connector.
- **JDBC**: Databricks JDBC ingestion.

Outbound edges map to:
- **Files**: UC external locations / Delta Sharing.
- **JDBC**: Databricks JDBC export.

## 7. Transformation Strategy

The framework uses a **pluggable transform registry**:

- **Built-ins**: `select`, `filter`, `drop`, `rename`, `cast`, `uppercase`,
  `lowercase`, `alias`, `with_column`.
- **Custom transforms**: registered under a `custom.*` namespace. This lets the
  central platform expose approved business logic while keeping pipeline JSON
  declarative.

In Databricks, the same registry pattern applies. Custom transforms can wrap
Python UDFs, Pandas UDFs, or Databricks Mosaic/ML functions.

## 8. Validation Strategy

The `ValidationRegistry` supports:

- `not_null`
- `unique`
- `regex`
- `range`

On Databricks, these can be replaced or augmented with:
- Delta Lake constraints (`CHECK` constraints).
- Great Expectations.
- Databricks Lakehouse Monitoring.

## 9. Governance Model

- **Central data platform** owns all pipeline definitions (JSON configs),
  edge node contracts, and approved transform/validator libraries.
- **Business units** submit requirements but do not author pipeline configs.
- This is enforced by:
  - Strict JSON Schema validation before execution.
  - Central control of the transform/validator registry.
  - Versioned, owned edge node definitions.

## 10. Migration Roadmap

1. **Phase 1 — Abstraction layer** (this mock):
   - Define `Pipeline`, `Task`, `EdgeNode`, `Format`, `Platform` contracts.
   - Run locally on Parquet to prove the model.

2. **Phase 2 — Databricks runtime**:
   - Implement `DatabricksPlatform` with UC and cluster integration.
   - Run the same pipeline JSON on Databricks.

3. **Phase 3 — Storage migration**:
   - Add Iceberg Spark runtime and catalog configuration.
   - Change target `format` from `parquet` to `iceberg` incrementally.

4. **Phase 4 — Edge modernization**:
   - Replace file-based inbound edges with Autoloader/Kafka connectors.
   - Replace file-based outbound edges with Delta Sharing / UC external tables.

## 11. Decision Records

### ADR-001: Pluggable runtime over single platform

- **Context**: UFW must move from Cloudera to Databricks, but future platforms
  are possible.
- **Decision**: Abstract `PlatformAdapter` so the same pipeline runs on local
  PySpark and Databricks.
- **Consequences**: Additional abstraction complexity; future platform swaps
  require only a new adapter.

### ADR-002: Pluggable storage format

- **Context**: Strategic target is Iceberg, but current state is Parquet and
  Databricks prefers Delta.
- **Decision**: Support Parquet, Iceberg, and Delta via `Format` adapters.
- **Consequences**: Migration is a config change, not a code rewrite.

### ADR-003: JSON metadata as the primary contract

- **Context**: UFW already uses JSON metadata for transformations.
- **Decision**: Keep JSON as the single source of truth; PySpark code is the
  runtime executor.
- **Consequences**: Pipelines are versionable, reviewable, and platform-agnostic.

### ADR-004: Central platform ownership

- **Context**: Enterprise data platform wants governance and consistency.
- **Decision**: Business units do not author pipeline configs; the central
  platform owns them.
- **Consequences**: Slower BU iteration but higher consistency and compliance.
