from pyspark.sql import SparkSession

from ufw_mock.models.transformation import Transformation
from ufw_mock.runtime.validation_registry import ValidationRegistry


def test_regex_predicate_checks_all_columns_and_preserves_null_handling() -> None:
    spark = SparkSession.builder.master("local[1]").appName("validation-predicate-test").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("bad", 1), ("ok", 2), ("ok", None)],
            ["text_value", "numeric_value"],
        )
        rule = Transformation(
            name="regex",
            type="regex",
            input_cols=["text_value", "numeric_value"],
            params={"pattern": "^ok$"},
        )

        failures = ValidationRegistry().failing_rows(df, rule)

        assert failures.count() == 2
    finally:
        spark.stop()
