# UFW Spark Abstraction Framework — Spec Interview DRAFT

## Restated Understanding

The user wants to **mock / specify an abstraction layer on top of Spark** that mirrors UFW (Westpac's proprietary, opinionated Spark framework). UFW evolved over 8–10 years in a Confluent/Cloudera environment and is analogous to Databricks' SDP. It is now being reverse-engineered / migrated toward the Databricks ecosystem, with a strategic desire to move storage from Parquet to Iceberg.

Current architecture sketch:

```
Source systems
    ↓
Inbound Edge node (abstraction layer / landing zone)
    ↓
UFW-orchestrated jobs (PySpark + JSON metadata-driven transformations)
    ↓
Intermediate tables (Parquet today → Iceberg target)
    ↓
Outbound Edge node (downstream consumers)
```

Key constructs identified so far:
- **Pipeline** (Westpac-specific concept, *not* Databricks Pipelines): contains tasks such as ingestion, transformation, etc.
- **Task**: a unit of work inside a pipeline (e.g. ingest, transform, validate, publish).
- **PySpark job**: the runtime artifact.
- **JSON metadata config**: declarative description of transformation stages.
- **Edge node**: inbound abstraction layer that isolates source systems from the core platform; also outbound layer for downstream consumers.
- **Core platform**: central data platform that owns UFW; aims to be data-platform-agnostic.

## Proposed Approach (best guess)

Produce an **implementation-ready PRD + a lightweight mock/reference skeleton** for the abstraction layer, covering:
1. Domain model / core abstractions (Pipeline, Task, Source, Target, Edge, Format, Platform).
2. JSON schema for metadata-driven transformation definitions.
3. A thin PySpark runtime that interprets the JSON and executes against Parquet (v1) and, via a pluggable adapter, Iceberg (v2).
4. Edge-node contracts (inbound/outbound).
5. Migration path from Confluent/Cloudera → Databricks.

## Initial Best-Guess Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Source systems                          │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  Inbound Edge Node  (raw / landing / contract layer)        │
│  - Schema validation                                        │
│  - Source decoupling                                        │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│              UFW Orchestrator / Runtime                     │
│  - Pipeline definition (JSON metadata)                      │
│  - Task scheduler / DAG                                     │
│  - PySpark execution engine                                 │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  Storage abstraction layer                                  │
│  - Parquet adapter (current)                                │
│  - Iceberg adapter (future / strategic)                     │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│  Outbound Edge Node  (curated / publish / contract layer)   │
│  - Downstream consumption                                   │
│  - Data sharing / API / export                              │
└─────────────────────────────────────────────────────────────┘
```

## Core Constructs (proposed)

| Construct | Description | Example Attributes |
|-----------|-------------|--------------------|
| `Pipeline` | A named workflow of tasks with a runtime target | name, version, owner, schedule, tasks[], properties |
| `Task` | A single stage in a pipeline | id, type, source, target, transformations[], dependencies |
| `Source` / `Target` | Edge or storage references | edge_node, path, format, catalog, schema |
| `EdgeNode` | Contract boundary between systems | direction (inbound/outbound), owner, protocol, schema_registry |
| `Format` | Storage format abstraction | type (parquet/iceberg/delta), options, partitioning |
| `Platform` | Execution environment abstraction | type (cloudera/databricks/local), cluster, configs |
| `Transformation` | Declarative transform definition | name, type, params, input_cols, output_col |

## Open Questions / Assumptions

1. **Deliverable scope**: Does "mock a framework" mean (a) a written PRD/spec, (b) a runnable code skeleton, or (c) both?
   - **Decision**: PRD/spec, working prototype, and reference architecture doc. All three are required.
   - **Rationale**: The framework is complex and politically charged; having a spec, a runnable proof-of-concept, and migration architecture will give stakeholders the fullest picture.
2. **Migration vs. documentation**: Is the goal to document UFW as-is, design the target Databricks/Iceberg replacement, or map old → new?
   - **Decision**: The primary goal is to **mock a framework on top of PySpark**. Databricks and Iceberg are strategic context, but the immediate deliverable is a PySpark-based abstraction layer / prototype. PRD and architecture doc frame this prototype.
3. **Depth of code**: If code is expected, how much should be implemented (domain classes only, JSON schema, a working PySpark runtime, tests)?
4. **Storage format support**: Should the mock assume Parquet only, target Iceberg, or support multiple formats via an adapter?
   - **Decision**: Support **Parquet, Iceberg, and Delta** via a pluggable `Format` adapter.
   - **Rationale**: This is the most platform-agnostic option and accommodates UFW's current Parquet state, the strategic Iceberg target, and Databricks' native Delta.
5. **Databricks specifics**: Should the mock target Databricks Unity Catalog, DLT, Jobs, or plain Spark on Databricks?
   - **Decision**: Pluggable runtime. The same pipeline definition should run on local PySpark and on Databricks. This demonstrates data-platform agnosticism and matches the stated goal of a core platform abstraction.
   - **Rationale**: It forces the framework to separate pipeline definitions from execution concerns.
6. **Edge node semantics**: Is the Edge node a physical landing zone, an API contract, a Kafka/Confluent topic, or all of the above?
   - **Decision**: Pluggable edge abstraction. Edge nodes are source/target connectors with contracts; they can be files, Kafka topics, REST APIs, or databases.
   - **Rationale**: Matches the platform-agnostic goal and UFW's Confluent/Cloudera environment where sources vary.
7. **Pipeline task types**: Which tasks should the mock pipeline support (ingest, transform, validate, publish, monitor)?
   - **Decision**: Support **Ingest + Transform + Validate + Publish**.
   - **Rationale**: This is a realistic enterprise pipeline lifecycle without over-expanding scope. Validation addresses data-quality concerns.
8. **User pain points**: "90% of users detest UFW" — which specific frictions should the mock/redesign avoid (verbosity, debugging, deployment, testing, metadata drift)?
   - **Decision**: User indicated this comment is not applicable and should be ignored for the mock.
9. **Transform vocabulary**: How rich should the metadata-driven transformations be?
   - **Decision**: Pluggable transforms. Core built-ins plus a registry for custom Python functions.
   - **Rationale**: Balances declarative JSON metadata with the flexibility needed by diverse BUs and complex business logic.
10. **BU autonomy vs. central control**: How much should each business unit be able to customize pipelines vs. the central platform enforcing guardrails?
    - **Decision**: Central platform controls all. Business units submit requirements; the central platform authors and owns all pipelines and configs.
    - **Rationale**: Simplifies governance and reflects a tightly controlled enterprise data platform.
11. **Sample domain**: What example data domain should the mock pipeline use to demonstrate the framework?
    - **Decision**: Customer / KYC data. Profiles, addresses, identity data with validation and segmentation.
    - **Rationale**: Demonstrates schema evolution, data-quality checks, and is domain-relevant.
12. **Orchestration model**: How should tasks within a pipeline be ordered and executed?
    - **Decision**: Sequential list. Tasks run in the order they appear in the JSON array.
    - **Rationale**: Simplest to author and reason about; fits a centrally controlled platform where order is explicit.
13. **Config validation**: Should the mock validate pipeline JSON against a schema before execution?
    - **Decision**: Yes, strict JSON Schema validation before execution.
    - **Rationale**: Catches config errors early and demonstrates a controlled, governed platform.
14. **Sample data**: How should the mock obtain input data for the example KYC pipeline?
    - **Decision**: No sample data is needed now; the focus is on mocking the framework itself.
    - **Rationale**: The prototype can be a structural/skeletal proof-of-concept without realistic data loading.
15. **Tests**: Should the mock include automated tests?
    - **Decision**: Yes, both unit and integration tests.
    - **Rationale**: Framework code benefits from comprehensive coverage; integration tests prove the pipeline runtime works.
16. **Terminology**: Should the mock reuse UFW-specific terms (Pipeline, Edge node) or use generic names?
    - **Decision**: Reuse UFW terms (Pipeline, Edge Node, Task, Config) to keep the mapping to Westpac's world explicit.
    - **Rationale**: The mock is explicitly reverse-engineering UFW; using its vocabulary aids communication with stakeholders.

## Assumptions I Am Making (to be validated)

- The mock/spec will be written in Python / PySpark to match the existing UFW stack.
- The JSON metadata is the primary contract; Python code is the runtime executor.
- Parquet-to-Iceberg migration is a strategic constraint, not an immediate requirement.
- The Databricks ecosystem is the target runtime.
- The Edge node is an abstraction for source/target contracts, not a literal single machine.

## Next Step

Validate deliverable scope and primary goal before expanding the spec or writing code.
