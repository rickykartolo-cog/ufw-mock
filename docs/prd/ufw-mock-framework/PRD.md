# UFW Spark Abstraction Framework — Product Requirements Document

## 1. Problem Statement

Westpac's **Unified Framework for Workloads (UFW)** is a proprietary, opinionated PySpark framework that evolved over 8–10 years in a Confluent/Cloudera environment. It is now being reverse-engineered and migrated toward the Databricks ecosystem, with a strategic goal of moving storage from Parquet to Iceberg.

The problem is threefold:

1. **Knowledge risk**: UFW is undocumented and tightly coupled to its current runtime. There is no clear, implementation-ready spec for its constructs, runtime behavior, or migration path.
2. **Platform lock-in**: The current implementation is bound to Confluent/Cloudera and Parquet. The bank wants a data-platform-agnostic core that can run on Databricks (and potentially other Spark platforms) without rewriting every pipeline.
3. **Storage transition uncertainty**: Westpac wants to move from Parquet to Iceberg but does not yet know how to execute that transition without breaking existing pipelines.

This PRD specifies a **mock / reference implementation** of a PySpark-based abstraction layer that captures UFW's core constructs, supports pluggable runtimes and storage formats, and can serve as the blueprint for a Databricks/Iceberg migration.

## 2. Proposed Solution

Build a **Python/PySpark framework mock** named `ufw-mock` with the following artifacts:

1. **PRD** (this document) — fully specified architecture, domain model, JSON schema, decisions, and user journeys.
2. **Reference architecture** — diagrams and decision records describing how the mock maps to Databricks/Iceberg.
3. **Working prototype** — runnable Python package with domain classes, JSON config validation, pluggable runtime, pluggable storage adapters, and tests.

### 2.1 High-Level Architecture

```
Source systems
    │ publish / push / drop
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Inbound Edge Node (contract layer)                         │
│  - Protocol: file, kafka, rest, jdbc                        │
│  - Schema contract enforced on read                         │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  UFW Runtime / Orchestrator                                 │
│  - Parse Pipeline JSON                                      │
│  - Validate against JSON Schema                             │
│  - Execute tasks sequentially                               │
│  - Resolve pluggable transforms                             │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Storage abstraction layer                                  │
│  - Parquet adapter (current)                                │
│  - Iceberg adapter (strategic target)                       │
│  - Delta adapter (Databricks native)                        │
└─────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Outbound Edge Node (contract layer)                        │
│  - Curated / publish-ready datasets                         │
│  - Downstream consumption                                   │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Core Domain Model

```python
class Pipeline:
    name: str
    version: str
    owner: str
    schedule: str | None
    properties: dict
    tasks: List[Task]

class Task:
    id: str
    type: TaskType  # INGEST | TRANSFORM | VALIDATE | PUBLISH
    source: Source
    target: Target
    transformations: List[Transformation]
    properties: dict

class Source:
    edge_node: str
    path: str
    format: str  # parquet | iceberg | delta | json | csv
    schema: SchemaRef | None

class Target:
    edge_node: str
    path: str
    format: str
    mode: str  # overwrite | append | merge

class EdgeNode:
    name: str
    direction: Direction  # INBOUND | OUTBOUND
    protocol: str  # file | kafka | rest | jdbc
    owner: str
    properties: dict

class Transformation:
    name: str
    type: str  # built-in or custom plugin key
    params: dict
    input_cols: List[str] | None
    output_col: str | None

class Platform:
    name: str  # local_pyspark | databricks
    config: dict
    # resolves SparkSession / catalog / storage handlers
```

### 2.3 Prototype File Structure

```
ufw_mock/
├── pyproject.toml
├── README.md
├── src/
│   └── ufw_mock/
│       ├── __init__.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── pipeline.py
│       │   ├── task.py
│       │   ├── edge_node.py
│       │   ├── source_target.py
│       │   ├── transformation.py
│       │   └── platform.py
│       ├── runtime/
│       │   ├── __init__.py
│       │   ├── pipeline_runner.py
│       │   ├── task_executor.py
│       │   └── transform_registry.py
│       ├── formats/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── parquet.py
│       │   ├── iceberg.py
│       │   └── delta.py
│       ├── edges/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── file_edge.py
│       │   └── kafka_edge.py  # stub
│       ├── validation/
│       │   ├── __init__.py
│       │   └── schema_loader.py
│       └── config/
│           └── pipeline_schema.json
├── examples/
│   └── kyc_pipeline.json
└── tests/
    ├── unit/
    │   ├── test_models.py
    │   ├── test_validation.py
    │   └── test_transform_registry.py
    └── integration/
        └── test_pipeline_runner.py
```

### 2.4 JSON Config Example (KYC Pipeline)

```json
{
  "name": "kyc-customer-onboarding",
  "version": "1.0.0",
  "owner": "central-data-platform",
  "schedule": "0 6 * * *",
  "properties": {
    "environment": "dev",
    "line_of_business": "retail"
  },
  "tasks": [
    {
      "id": "ingest-customer-raw",
      "type": "INGEST",
      "source": {
        "edge_node": "inbound-crm",
        "path": "/data/inbound/crm/customers",
        "format": "parquet"
      },
      "target": {
        "edge_node": "core-raw-zone",
        "path": "/data/core/raw/customers",
        "format": "parquet",
        "mode": "overwrite"
      },
      "transformations": []
    },
    {
      "id": "transform-customer-clean",
      "type": "TRANSFORM",
      "source": {
        "edge_node": "core-raw-zone",
        "path": "/data/core/raw/customers",
        "format": "parquet"
      },
      "target": {
        "edge_node": "core-curated-zone",
        "path": "/data/core/curated/customers",
        "format": "iceberg",
        "mode": "overwrite"
      },
      "transformations": [
        {
          "name": "drop-null-emails",
          "type": "filter",
          "params": {"condition": "email IS NOT NULL"}
        },
        {
          "name": "standardise-country",
          "type": "uppercase",
          "input_cols": ["country"],
          "output_col": "country_standardised"
        },
        {
          "name": "segment-customer",
          "type": "custom.segment_by_risk",
          "input_cols": ["income", "country_standardised"],
          "output_col": "risk_segment"
        }
      ]
    },
    {
      "id": "validate-customer-quality",
      "type": "VALIDATE",
      "source": {
        "edge_node": "core-curated-zone",
        "path": "/data/core/curated/customers",
        "format": "iceberg"
      },
      "target": {
        "edge_node": "core-curated-zone",
        "path": "/data/core/curated/customers",
        "format": "iceberg",
        "mode": "overwrite"
      },
      "transformations": [
        {
          "name": "check-unique-customer-id",
          "type": "unique",
          "input_cols": ["customer_id"]
        },
        {
          "name": "check-email-format",
          "type": "regex",
          "input_cols": ["email"],
          "params": {"pattern": "^[^@]+@[^@]+$"}
        }
      ]
    },
    {
      "id": "publish-customer-outbound",
      "type": "PUBLISH",
      "source": {
        "edge_node": "core-curated-zone",
        "path": "/data/core/curated/customers",
        "format": "iceberg"
      },
      "target": {
        "edge_node": "outbound-marketing",
        "path": "/data/outbound/marketing/customers",
        "format": "delta",
        "mode": "overwrite"
      },
      "transformations": [
        {
          "name": "select-publishable-cols",
          "type": "select",
          "input_cols": ["customer_id", "email", "country_standardised", "risk_segment"]
        }
      ]
    }
  ]
}
```

### 2.5 Runtime Flow

1. Load `Pipeline` from JSON.
2. Validate JSON structure against `pipeline_schema.json`.
3. Instantiate `Platform` (default: `local_pyspark`).
4. For each `Task` in order:
   - Resolve the `Source` via its `EdgeNode` and `Format` adapter.
   - Read data into a Spark DataFrame.
   - Apply each `Transformation` using the transform registry.
   - If task type is `VALIDATE`, run validations and either fail or pass.
   - Write DataFrame to `Target` via its `EdgeNode` and `Format` adapter.
5. Capture execution summary and any validation failures.

### 2.6 Pluggable Components

| Component | Responsibility | Default Implementations |
|-----------|----------------|-------------------------|
| `Platform` | Provides `SparkSession` and runtime context | `LocalPySparkPlatform`, `DatabricksPlatform` (stub) |
| `Format` | Read/write data in a given format | `ParquetFormat`, `IcebergFormat`, `DeltaFormat`, `JsonFormat` |
| `Edge` | Resolve source/target location | `FileEdge`, `KafkaEdge` (stub) |
| `Transform` | Apply a transformation to a DataFrame | `select`, `filter`, `cast`, `uppercase`, `rename`, `custom.*` |
| `Validator` | Check data quality rules | `unique`, `not_null`, `regex`, `range` |

## 3. User Journeys

### 3.1 Happy Path: Central Platform Engineer Authors a KYC Pipeline

1. Engineer writes a JSON pipeline config describing ingestion, transformation, validation, and publish tasks.
2. Engineer runs the UFW CLI: `ufw-run --config examples/kyc_pipeline.json --platform local_pyspark`.
3. Framework validates the JSON against the schema.
4. Framework initializes a local Spark session.
5. Each task executes sequentially:
   - Ingest reads from inbound CRM Parquet and writes to raw zone.
   - Transform cleans and enriches data, writing to Iceberg.
   - Validate checks uniqueness and email format.
   - Publish writes final columns to Delta format for outbound marketing.
6. Runner reports success and task-level metrics.

### 3.2 Edge Case: Invalid JSON Config

1. Engineer submits a pipeline JSON with a missing required field (e.g. `target.format`).
2. Framework validates before execution.
3. Validation fails with a clear, structured error: `tasks[1].target.format: required property missing`.
4. Runner exits without starting Spark.

### 3.3 Edge Case: Custom Business Transform

1. A business unit requires a KYC risk-segmentation transform that is not in the built-in library.
2. The central platform registers a Python function under the key `custom.segment_by_risk` in the transform registry.
3. The pipeline JSON references `custom.segment_by_risk`.
4. At runtime, the registry resolves and executes the custom function against the DataFrame.

### 3.4 Edge Case: Platform-Specific Deployment

1. The same `kyc_pipeline.json` is promoted from local development to Databricks.
2. Engineer changes only the `--platform databricks` argument (and platform config such as cluster, catalog, schema).
3. The pipeline definition remains unchanged; the `DatabricksPlatform` adapter provides the Spark session and catalog integration.

### 3.5 Edge Case: Storage Format Migration

1. A pipeline currently writes targets in Parquet.
2. Engineer changes the target `format` from `parquet` to `iceberg` in the JSON config.
3. The `IcebergFormat` adapter handles table creation, partitioning, and write semantics.
4. No PySpark task code changes.

## 4. Constraints

1. **Python / PySpark only** — The prototype is written in Python 3.10+ with PySpark 3.5. No Scala or Java code.
2. **No sample data required** — The prototype does not need realistic data fixtures; integration tests can use in-memory DataFrames.
3. **Central platform ownership** — Business units do not author pipeline configs; the central data platform owns all pipeline definitions.
4. **Strict JSON Schema validation** — Every pipeline config must validate structurally before execution.
5. **Sequential task execution** — Tasks run in the order declared in the JSON array. No DAG or external scheduler integration in the mock.
6. **Local-first, Databricks-ready** — The default runtime is local PySpark; Databricks is a pluggable adapter.
7. **Storage format neutrality** — The framework must support Parquet, Iceberg, and Delta via adapters.
8. **UFW terminology preserved** — Use UFW terms (Pipeline, Task, Edge Node) for stakeholder alignment.

## 5. Decisions Log

| Decision | Alternatives Considered | Reasoning |
|----------|--------------------------|-----------|
| Deliverables: PRD + working prototype + reference architecture | PRD only, prototype only, architecture only | The framework is complex and politically charged; all three artifacts are needed for stakeholder alignment and future implementation. |
| Primary goal: mock a PySpark framework, not a full migration | Document as-is, design target replacement only, map old→new | The user explicitly wants a runnable abstraction layer on top of PySpark; Databricks/Iceberg are strategic context. |
| Runtime: pluggable platform (local + Databricks) | Local only, Databricks native only, Databricks plain Spark | Demonstrates data-platform agnosticism and separates pipeline definition from execution. |
| Storage: Parquet + Iceberg + Delta adapters | Parquet only, Iceberg only, Parquet+Iceberg | Most platform-agnostic; covers current state, strategic target, and Databricks native. |
| Edge node: pluggable connector abstraction | Physical landing zone only, Kafka topics only, API contract only | Matches UFW's Confluent/Cloudera heritage and diverse source types. |
| Task types: Ingest + Transform + Validate + Publish | Fewer or more task types | Realistic enterprise lifecycle without over-expanding scope. |
| Transform vocabulary: pluggable transforms | Simple column ops, SQL-like ops, SQL expression engine | Balances declarative JSON metadata with business-logic flexibility. |
| Ownership: central platform controls all | BU-owned pipelines, templates with BU extension, decentralized | Simplifies governance; BUs submit requirements, central platform authors configs. |
| Sample domain: Customer / KYC data | Banking transactions, risk reporting, generic retail | Domain-relevant and demonstrates validation, schema evolution, and segmentation. |
| Orchestration: sequential task list | Dependency DAG, named stages, DAG+external scheduler | Simplest to author and reason about for a centrally controlled platform. |
| Config validation: strict JSON Schema | Light validation, runtime-only, validation plugin | Catches errors early and demonstrates a governed platform. |
| Sample data: none required | Synthetic fixtures, hard-coded files, in-memory DataFrames | Focus is on the framework skeleton, not realistic data loading. |
| Tests: unit + integration | Unit only, integration only, none | Framework code needs comprehensive coverage; integration tests prove runtime correctness. |
| Terminology: reuse UFW terms | Generic terms, UFW terms with glossary, generic with aliases | The mock reverse-engineers UFW; using its vocabulary aids communication. |

## 6. Out of Scope

The following are explicitly not covered by this PRD:

1. **Production orchestration / scheduling** — No Airflow, Databricks Jobs, or cron integration in the mock.
2. **Security / IAM / entitlement** — No row-level security, column masking, or access control.
3. **Secret management** — No integration with secret stores; config may reference connection strings as plain text in the mock.
4. **Data lineage / observability platform** — No OpenLineage, Unity Catalog lineage, or external monitoring.
5. **Real-time / streaming semantics** — Kafka edge is a stub; the mock is batch-oriented.
6. **Auto-scaling / cluster management** — Platform adapters do not manage cluster lifecycle.
7. **Migration of existing UFW pipelines** — The PRD does not map every legacy UFW construct 1:1; it provides the target abstraction and a sample.
8. **Performance benchmarking** — No SLAs, throughput targets, or optimization guidelines.

## 7. Prototype Implementation Plan

The working prototype will be built as a Python package in `/Users/ricky.k/workspaces/cognition/ufw-mock/` with the following milestones:

1. **Project scaffolding** — `pyproject.toml`, package structure, pytest configuration.
2. **Domain models** — Pydantic or dataclasses for `Pipeline`, `Task`, `Source`, `Target`, `EdgeNode`, `Transformation`, `Platform`.
3. **JSON Schema** — `pipeline_schema.json` and validation function.
4. **Platform adapter** — `LocalPySparkPlatform` with `SparkSession` management; `DatabricksPlatform` as a stub/contract.
5. **Format adapters** — `ParquetFormat`, `IcebergFormat`, `DeltaFormat`, with read/write signatures.
6. **Edge adapters** — `FileEdge` concrete; `KafkaEdge`/`RestEdge` as stubs.
7. **Transform registry** — Built-in transforms + custom function registration.
8. **Task executor** — Sequential runner for `INGEST`, `TRANSFORM`, `VALIDATE`, `PUBLISH`.
9. **CLI entry point** — `ufw-run --config <path> --platform <name>`.
10. **Tests** — Unit tests for models, validation, registry; integration test for local pipeline runner.
11. **Example** — `examples/kyc_pipeline.json`.
12. **Reference architecture doc** — Migration decision records and Databricks/Iceberg mapping diagrams.

## 8. Reference Architecture Notes

### 8.1 Confluent/Cloudera → Databricks Mapping

| UFW Construct | Confluent/Cloudera Implementation | Databricks Target |
|---------------|-----------------------------------|-------------------|
| Inbound Edge  | HDFS / S3 landing + Kafka topics  | Unity Catalog volumes + Delta Live Tables or Autoloader |
| Pipeline      | PySpark job orchestrated by UFW   | Databricks Jobs / Workflows or plain Spark jobs |
| Transform     | JSON metadata + PySpark runtime   | Same JSON metadata; runtime uses Databricks Spark + Unity Catalog |
| Validate      | Custom validators                 | Great Expectations / Delta constraints / custom validators |
| Outbound Edge | HDFS / S3 publish zone            | Unity Catalog external locations / Delta Sharing |
| Storage       | Parquet                           | Iceberg (strategic) or Delta (intermediate) |

### 8.2 Iceberg Migration Considerations

- Use a `Format` abstraction so pipelines do not reference Parquet-specific paths.
- Implement `IcebergFormat` with catalog-aware reads/writes.
- Support `mode: merge` for Iceberg MERGE INTO semantics in `PUBLISH` tasks.
- Keep `ParquetFormat` as a fallback during transition.
- Store table metadata (partitioning, schema evolution) in the pipeline JSON `properties`.
