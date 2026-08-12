import pytest
from pyspark.sql import SparkSession

from ufw_mock.models.transformation import Transformation
from ufw_mock.runtime.transform_registry import TransformRegistry


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test-transforms").master("local[*]").getOrCreate()


@pytest.fixture
def registry():
    return TransformRegistry()


def test_select_transform(spark, registry):
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    transform = Transformation(name="select", type="select", input_cols=["a"])
    result = registry.apply(df, transform)
    assert result.columns == ["a"]


def test_filter_transform(spark, registry):
    df = spark.createDataFrame([{"a": 1}, {"a": 2}])
    transform = Transformation(name="filter", type="filter", params={"condition": "a > 1"})
    result = registry.apply(df, transform)
    assert result.count() == 1


def test_uppercase_transform(spark, registry):
    df = spark.createDataFrame([{"country": "au"}])
    transform = Transformation(name="upper", type="uppercase", input_cols=["country"], output_col="c")
    result = registry.apply(df, transform)
    assert result.collect()[0]["c"] == "AU"


def test_custom_transform_registration(spark, registry):
    def double_a(df, transformation):
        from pyspark.sql.functions import col

        return df.withColumn("doubled", col("a") * 2)

    registry.register("custom.double_a", double_a)
    df = spark.createDataFrame([{"a": 3}])
    transform = Transformation(name="double", type="custom.double_a")
    result = registry.apply(df, transform)
    assert result.collect()[0]["doubled"] == 6


def test_unknown_transform(registry):
    with pytest.raises(KeyError):
        registry.get("does_not_exist")
