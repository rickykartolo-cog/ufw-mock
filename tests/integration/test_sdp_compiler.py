import pytest

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform, SdpOptions
from ufw_mock.runtime.pipeline_runner import PipelineRunner
from ufw_mock.runtime.platform import SparkDeclarativePipelinesPlatform, get_platform
from ufw_mock.runtime.sdp_compiler import SdpCompilationError, compile_pipeline
from ufw_mock.types import PlatformType, SdpMaterialization


@pytest.fixture
def sdp_pipeline():
    """A KYC-style pipeline whose task order does not match its data dependencies."""
    return Pipeline(
        name="test-kyc-sdp",
        edge_nodes=[],
        tasks=[
            {
                "id": "publish-marketing",
                "type": "PUBLISH",
                "source": {"edge_node": "curated", "path": "core/curated/customers"},
                "target": {"edge_node": "outbound", "path": "outbound/marketing/customers"},
                "transformations": [
                    {"name": "select", "type": "select", "input_cols": ["customer_id", "email"]},
                ],
            },
            {
                "id": "validate-curated",
                "type": "VALIDATE",
                "source": {"edge_node": "curated", "path": "core/curated/customers"},
                "target": {"edge_node": "curated", "path": ""},
                "transformations": [
                    {"name": "unique-id", "type": "unique", "input_cols": ["customer_id"]},
                    {
                        "name": "not-null-email",
                        "type": "not_null",
                        "input_cols": ["email"],
                        "params": {"on_violation": "drop"},
                    },
                    {
                        "name": "income-range",
                        "type": "range",
                        "input_cols": ["income"],
                        "params": {"min": 0, "max": 1000000, "on_violation": "fail"},
                    },
                ],
            },
            {
                "id": "transform-customer-clean",
                "type": "TRANSFORM",
                "source": {"edge_node": "raw", "path": "core/raw/customers"},
                "target": {"edge_node": "curated", "path": "core/curated/customers"},
                "transformations": [
                    {
                        "name": "uppercase-country",
                        "type": "uppercase",
                        "input_cols": ["country"],
                        "output_col": "country_standardised",
                    },
                ],
            },
            {
                "id": "ingest-customer-raw",
                "type": "INGEST",
                "source": {"edge_node": "inbound", "path": "inbound/crm/customers"},
                "target": {"edge_node": "raw", "path": "core/raw/customers"},
                "transformations": [],
            },
        ],
    )


def test_get_platform_resolves_sdp():
    platform = get_platform(Platform(name=PlatformType.SPARK_DECLARATIVE_PIPELINES))
    assert isinstance(platform, SparkDeclarativePipelinesPlatform)


def test_sdp_module_import_is_guarded():
    platform = SparkDeclarativePipelinesPlatform()
    with pytest.raises(RuntimeError, match="Spark Declarative Pipelines engine is unavailable"):
        platform.get_sdp_module()


def test_dag_is_wired_by_data_dependency(sdp_pipeline):
    graph = compile_pipeline(sdp_pipeline)

    # VALIDATE tasks decorate an existing dataset rather than creating one.
    assert graph.dataset_names == [
        "ingest_customer_raw",
        "transform_customer_clean",
        "publish_marketing",
    ]
    assert graph.edges() == [
        ("ingest_customer_raw", "transform_customer_clean"),
        ("transform_customer_clean", "publish_marketing"),
    ]
    assert [dataset.name for dataset in graph.roots()] == ["ingest_customer_raw"]


def test_task_type_materialization_mapping(sdp_pipeline):
    graph = compile_pipeline(sdp_pipeline)

    assert graph.get("ingest_customer_raw").materialization == SdpMaterialization.STREAMING_TABLE
    assert graph.get("ingest_customer_raw").is_streaming
    assert (
        graph.get("transform_customer_clean").materialization
        == SdpMaterialization.MATERIALIZED_VIEW
    )
    assert graph.get("publish_marketing").materialization == SdpMaterialization.MATERIALIZED_VIEW


def test_validate_task_compiles_to_expectations(sdp_pipeline):
    graph = compile_pipeline(sdp_pipeline)
    curated = graph.get("transform_customer_clean")

    assert curated.validated_by == ["validate-curated"]
    assert [(e.name, e.decorator) for e in curated.expectations] == [
        ("unique-id", "expect_all"),
        ("not-null-email", "expect_all_or_drop"),
        ("income-range", "expect_all_or_fail"),
    ]
    assert curated.expectations[1].constraints == {"email_not_null": "email IS NOT NULL"}
    assert curated.expectations[2].constraints == {"income_in_range": "income BETWEEN 0 AND 1000000"}
    assert not graph.get("publish_marketing").expectations


def test_validate_task_without_producer_emits_view():
    pipeline = Pipeline(
        name="validate-only",
        tasks=[
            {
                "id": "validate-external",
                "type": "VALIDATE",
                "source": {"edge_node": "curated", "path": "core/curated/customers"},
                "target": {"edge_node": "curated", "path": ""},
                "transformations": [
                    {
                        "name": "email-pattern",
                        "type": "regex",
                        "input_cols": ["email"],
                        "params": {"pattern": "^.+@.+$"},
                    },
                ],
            },
        ],
    )
    graph = compile_pipeline(pipeline)
    dataset = graph.get("validate_external")

    assert dataset.materialization == SdpMaterialization.VIEW
    assert dataset.decorator == "view"
    assert dataset.expectations[0].constraints == {"email_matches_pattern": "email RLIKE '^.+@.+$'"}


def test_unknown_validator_is_rejected():
    pipeline = Pipeline(
        name="bad-validator",
        tasks=[
            {
                "id": "validate",
                "type": "VALIDATE",
                "source": {"edge_node": "curated", "path": "core/curated/customers"},
                "target": {"edge_node": "curated", "path": ""},
                "transformations": [{"name": "nope", "type": "not_a_validator"}],
            },
        ],
    )
    with pytest.raises(KeyError, match="Unknown validator"):
        compile_pipeline(pipeline)


def test_options_override_names_and_materialization(sdp_pipeline):
    options = SdpOptions(
        catalog="main",
        schema="kyc",
        dataset_names={"ingest-customer-raw": "customers_bronze"},
        derived_materialization=SdpMaterialization.TABLE,
    )
    graph = compile_pipeline(sdp_pipeline, options)
    bronze = graph.get("customers_bronze")

    assert bronze.qualified_name == "main.kyc.customers_bronze"
    assert graph.get("transform_customer_clean").upstream == ["customers_bronze"]
    assert graph.get("transform_customer_clean").materialization == SdpMaterialization.TABLE


def test_task_level_materialization_override(sdp_pipeline):
    sdp_pipeline.tasks[0].properties = {"sdp": {"materialization": "table", "schema": "outbound"}}
    graph = compile_pipeline(sdp_pipeline)
    publish = graph.get("publish_marketing")

    assert publish.materialization == SdpMaterialization.TABLE
    assert publish.qualified_name == "outbound.publish_marketing"


def test_emitted_module_structure(sdp_pipeline):
    module = compile_pipeline(sdp_pipeline).to_python_module()

    assert "import dlt as sdp" in module
    assert "Spark Declarative Pipelines engine is unavailable" in module
    assert "_TRANSFORM_REGISTRY = TransformRegistry()" in module
    # Entry point uses an Auto Loader-style streaming read; derived datasets read upstream.
    assert "spark.readStream.format('cloudFiles')" in module
    assert "df = sdp.read('ingest_customer_raw')" in module
    assert "df = sdp.read('transform_customer_clean')" in module
    assert '@sdp.expect_all_or_drop({"email_not_null": "email IS NOT NULL"})' in module
    for name in ("ingest_customer_raw", "transform_customer_clean", "publish_marketing"):
        assert f"def {name}():" in module
    assert module.index("def ingest_customer_raw():") < module.index("def publish_marketing():")

    compile(module, "<sdp-test>", "exec")


def test_emitted_module_is_importable_with_a_stub_engine(sdp_pipeline, monkeypatch):
    """The generated module registers datasets against an injected SDP module."""
    registered: dict[str, dict] = {}

    class StubSdp:
        def table(self, name, comment=None):
            def decorator(fn):
                registered[name] = {"comment": comment, "fn": fn}
                return fn

            return decorator

        view = table

        def _expect(self, constraints):
            def decorator(fn):
                registered.setdefault(fn.__name__, {}).setdefault("constraints", {}).update(
                    constraints
                )
                return fn

            return decorator

        expect_all = _expect
        expect_all_or_drop = _expect
        expect_all_or_fail = _expect

        def read(self, name):
            return f"read:{name}"

        def read_stream(self, name):
            return f"read_stream:{name}"

    graph = compile_pipeline(sdp_pipeline)
    namespace = graph.register(StubSdp(), spark=None)

    assert set(namespace["_TRANSFORMATIONS"]) == set(graph.dataset_names)
    assert set(registered) >= set(graph.dataset_names)


def test_pipeline_runner_uses_sdp_branch(sdp_pipeline):
    platform = get_platform(
        Platform(name=PlatformType.SPARK_DECLARATIVE_PIPELINES, sdp={"catalog": "main"})
    )
    with PipelineRunner(sdp_pipeline, platform) as runner:
        summary = runner.run()

    assert summary["mode"] == "sdp"
    assert summary["status"] == "compiled"
    assert summary["registered"] is False
    assert "engine_error" in summary
    assert "tasks" not in summary
    assert [dataset["name"] for dataset in summary["datasets"]] == [
        "ingest_customer_raw",
        "transform_customer_clean",
        "publish_marketing",
    ]
    assert summary["datasets"][1]["upstream"] == ["ingest_customer_raw"]
    assert len(summary["datasets"][1]["expectations"]) == 3
    assert "def ingest_customer_raw():" in summary["module"]


def test_sdp_compilation_error_on_cycle():
    pipeline = Pipeline(
        name="cyclic",
        tasks=[
            {
                "id": "a",
                "type": "TRANSFORM",
                "source": {"edge_node": "zone", "path": "b"},
                "target": {"edge_node": "zone", "path": "a"},
            },
            {
                "id": "b",
                "type": "TRANSFORM",
                "source": {"edge_node": "zone", "path": "a"},
                "target": {"edge_node": "zone", "path": "b"},
            },
        ],
    )
    with pytest.raises(SdpCompilationError, match="Cyclic data dependency"):
        compile_pipeline(pipeline)
