from functools import reduce
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ufw_mock.models.transformation import Transformation

ValidationFn = Callable[[DataFrame, Transformation], None]
ValidationPredicate = Callable[[DataFrame, Transformation], Column]
ValidationRowsFn = Callable[[DataFrame, Transformation], DataFrame]


class ValidationError(Exception):
    """Raised when a data validation rule fails."""


class ValidationRegistry:
    """Registry for built-in and custom data-quality validators."""

    def __init__(self) -> None:
        self._registry: dict[str, ValidationFn] = {}
        self._predicates: dict[str, ValidationPredicate] = {}
        self._failing_rows: dict[str, ValidationRowsFn] = {}
        self._register_builtins()

    def register(
        self,
        key: str,
        fn: ValidationFn,
        predicate: ValidationPredicate | None = None,
        failing_rows: ValidationRowsFn | None = None,
    ) -> None:
        if not callable(fn):
            raise ValueError(f"Validator '{key}' must be callable.")
        if predicate is not None and failing_rows is not None:
            raise ValueError(f"Validator '{key}' cannot register both predicate kinds.")
        self._registry[key] = fn
        if predicate is not None:
            if not callable(predicate):
                raise ValueError(f"Predicate for validator '{key}' must be callable.")
            self._predicates[key] = predicate
        else:
            self._predicates.pop(key, None)
        if failing_rows is not None:
            if not callable(failing_rows):
                raise ValueError(f"Failing-rows function for validator '{key}' must be callable.")
            self._failing_rows[key] = failing_rows
        else:
            self._failing_rows.pop(key, None)

    def get(self, key: str) -> ValidationFn:
        if key not in self._registry:
            raise KeyError(f"Unknown validator '{key}'. Registered validators: {sorted(self._registry)}")
        return self._registry[key]

    def apply(self, df: DataFrame, transformation: Transformation) -> None:
        fn = self.get(transformation.type)
        fn(df, transformation)

    def get_predicate(self, key: str) -> ValidationPredicate:
        self.get(key)
        if key not in self._predicates:
            raise NotImplementedError(
                f"Validator '{key}' does not expose a declarative row-level predicate."
            )
        return self._predicates[key]

    def get_failing_rows(self, key: str) -> ValidationRowsFn:
        self.get(key)
        if key in self._failing_rows:
            return self._failing_rows[key]
        predicate = self.get_predicate(key)

        def filter_rows(df: DataFrame, transformation: Transformation) -> DataFrame:
            return (
                df.withColumn("_dq_violation", predicate(df, transformation))
                .filter(F.col("_dq_violation"))
            )

        return filter_rows

    def failing_rows(self, df: DataFrame, transformation: Transformation) -> DataFrame:
        return self.get_failing_rows(transformation.type)(df, transformation)

    def _register_builtins(self) -> None:
        self.register("not_null", _not_null, _not_null_predicate)
        self.register("unique", _unique, failing_rows=_unique_failing_rows)
        self.register("regex", _regex, _regex_predicate)
        self.register("range", _range, _range_predicate)


def _not_null(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'not_null' validator requires input_cols.")
    for col in cols:
        null_count = df.filter(_not_null_predicate(df, transformation_for_column(transformation, col))).count()
        if null_count > 0:
            raise ValidationError(f"Column '{col}' contains {null_count} null values.")


def _unique(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'unique' validator requires input_cols.")
    total = df.count()
    distinct = df.select(*cols).distinct().count()
    if distinct != total:
        raise ValidationError(f"Columns {cols} are not unique ({total} rows, {distinct} distinct).")


def _regex(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    pattern = transformation.params.get("pattern")
    if not cols or not pattern:
        raise ValueError("'regex' validator requires input_cols and params.pattern.")
    for col in cols:
        invalid = df.filter(_regex_predicate(df, transformation_for_column(transformation, col))).count()
        if invalid:
            raise ValidationError(f"Column '{col}' has {invalid} values not matching pattern '{pattern}'.")


def _range(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    min_val = transformation.params.get("min")
    max_val = transformation.params.get("max")
    if not cols or min_val is None or max_val is None:
        raise ValueError("'range' validator requires input_cols, params.min, and params.max.")
    for col in cols:
        out_of_range = df.filter(_range_predicate(df, transformation_for_column(transformation, col))).count()
        if out_of_range > 0:
            raise ValidationError(
                f"Column '{col}' has {out_of_range} values outside range [{min_val}, {max_val}]."
            )


def transformation_for_column(transformation: Transformation, column: str) -> Transformation:
    return transformation.model_copy(update={"input_cols": [column]})


def _not_null_predicate(df: DataFrame, transformation: Transformation) -> Column:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'not_null' validator requires input_cols.")
    return reduce(lambda left, right: left | right, (df[col].isNull() for col in cols))


def _regex_predicate(df: DataFrame, transformation: Transformation) -> Column:
    cols = transformation.input_cols or []
    pattern = transformation.params.get("pattern")
    if not cols or not pattern:
        raise ValueError("'regex' validator requires input_cols and params.pattern.")
    return reduce(lambda left, right: left | right, (~df[col].rlike(pattern) for col in cols))


def _range_predicate(df: DataFrame, transformation: Transformation) -> Column:
    cols = transformation.input_cols or []
    min_val = transformation.params.get("min")
    max_val = transformation.params.get("max")
    if not cols or min_val is None or max_val is None:
        raise ValueError("'range' validator requires input_cols, params.min, and params.max.")
    return reduce(
        lambda left, right: left | right,
        ((df[col] < min_val) | (df[col] > max_val) for col in cols),
    )


def _unique_failing_rows(df: DataFrame, transformation: Transformation) -> DataFrame:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'unique' validator requires input_cols.")
    duplicate_keys = (
        df.groupBy(*cols)
        .count()
        .filter(F.col("count") > 1)
        .select(*cols)
    )
    return df.join(duplicate_keys, cols, "inner").withColumn(
        "_dq_violation",
        F.lit(True),
    )
