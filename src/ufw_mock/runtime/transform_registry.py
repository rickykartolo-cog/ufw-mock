from typing import Callable

from pyspark.sql import DataFrame

from ufw_mock.models.transformation import Transformation

TransformFn = Callable[[DataFrame, Transformation], DataFrame]


class TransformRegistry:
    """Registry for built-in and custom transforms."""

    def __init__(self) -> None:
        self._registry: dict[str, TransformFn] = {}
        self._register_builtins()

    def register(self, key: str, fn: TransformFn) -> None:
        if not callable(fn):
            raise ValueError(f"Transform '{key}' must be callable.")
        self._registry[key] = fn

    def get(self, key: str) -> TransformFn:
        if key not in self._registry:
            raise KeyError(f"Unknown transform '{key}'. Registered transforms: {sorted(self._registry)}")
        return self._registry[key]

    def apply(self, df: DataFrame, transformation: Transformation) -> DataFrame:
        fn = self.get(transformation.type)
        return fn(df, transformation)

    def _register_builtins(self) -> None:
        self.register("select", _select)
        self.register("filter", _filter)
        self.register("drop", _drop)
        self.register("rename", _rename)
        self.register("cast", _cast)
        self.register("uppercase", _uppercase)
        self.register("lowercase", _lowercase)
        self.register("alias", _alias)
        self.register("with_column", _with_column)


def _select(df: DataFrame, transformation: Transformation) -> DataFrame:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'select' transform requires input_cols.")
    return df.select(*cols)


def _drop(df: DataFrame, transformation: Transformation) -> DataFrame:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'drop' transform requires input_cols.")
    return df.drop(*cols)


def _filter(df: DataFrame, transformation: Transformation) -> DataFrame:
    condition = transformation.params.get("condition")
    if not condition:
        raise ValueError("'filter' transform requires params.condition.")
    return df.filter(condition)


def _rename(df: DataFrame, transformation: Transformation) -> DataFrame:
    input_cols = transformation.input_cols or []
    output_col = transformation.output_col
    if len(input_cols) != 1 or not output_col:
        raise ValueError("'rename' transform requires exactly one input_col and output_col.")
    return df.withColumnRenamed(input_cols[0], output_col)


def _cast(df: DataFrame, transformation: Transformation) -> DataFrame:
    input_cols = transformation.input_cols or []
    if len(input_cols) != 1:
        raise ValueError("'cast' transform requires exactly one input_col.")
    target_type = transformation.params.get("type")
    if not target_type:
        raise ValueError("'cast' transform requires params.type.")
    output_col = transformation.output_col or input_cols[0]
    return df.withColumn(output_col, df[input_cols[0]].cast(target_type))


def _uppercase(df: DataFrame, transformation: Transformation) -> DataFrame:
    from pyspark.sql.functions import upper

    input_cols = transformation.input_cols or []
    if len(input_cols) != 1:
        raise ValueError("'uppercase' transform requires exactly one input_col.")
    output_col = transformation.output_col or input_cols[0]
    return df.withColumn(output_col, upper(df[input_cols[0]]))


def _lowercase(df: DataFrame, transformation: Transformation) -> DataFrame:
    from pyspark.sql.functions import lower

    input_cols = transformation.input_cols or []
    if len(input_cols) != 1:
        raise ValueError("'lowercase' transform requires exactly one input_col.")
    output_col = transformation.output_col or input_cols[0]
    return df.withColumn(output_col, lower(df[input_cols[0]]))


def _alias(df: DataFrame, transformation: Transformation) -> DataFrame:
    output_col = transformation.output_col
    expression = transformation.params.get("expression")
    if not output_col or expression is None:
        raise ValueError("'alias' transform requires output_col and params.expression.")
    from pyspark.sql.functions import expr

    return df.withColumn(output_col, expr(expression))


def _with_column(df: DataFrame, transformation: Transformation) -> DataFrame:
    output_col = transformation.output_col
    expression = transformation.params.get("expression")
    if not output_col or expression is None:
        raise ValueError("'with_column' transform requires output_col and params.expression.")
    from pyspark.sql.functions import expr

    return df.withColumn(output_col, expr(expression))
