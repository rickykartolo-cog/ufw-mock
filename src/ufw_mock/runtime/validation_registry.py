from typing import Callable

from pyspark.sql import DataFrame

from ufw_mock.models.transformation import Transformation

ValidationFn = Callable[[DataFrame, Transformation], None]


class ValidationError(Exception):
    """Raised when a data validation rule fails."""


class ValidationRegistry:
    """Registry for built-in and custom data-quality validators."""

    def __init__(self) -> None:
        self._registry: dict[str, ValidationFn] = {}
        self._register_builtins()

    def register(self, key: str, fn: ValidationFn) -> None:
        if not callable(fn):
            raise ValueError(f"Validator '{key}' must be callable.")
        self._registry[key] = fn

    def get(self, key: str) -> ValidationFn:
        if key not in self._registry:
            raise KeyError(f"Unknown validator '{key}'. Registered validators: {sorted(self._registry)}")
        return self._registry[key]

    def apply(self, df: DataFrame, transformation: Transformation) -> None:
        fn = self.get(transformation.type)
        fn(df, transformation)

    def _register_builtins(self) -> None:
        self.register("not_null", _not_null)
        self.register("unique", _unique)
        self.register("regex", _regex)
        self.register("range", _range)


def _not_null(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    if not cols:
        raise ValueError("'not_null' validator requires input_cols.")
    for col in cols:
        null_count = df.filter(df[col].isNull()).count()
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
    import re

    cols = transformation.input_cols or []
    pattern = transformation.params.get("pattern")
    if not cols or not pattern:
        raise ValueError("'regex' validator requires input_cols and params.pattern.")
    compiled = re.compile(pattern)
    for col in cols:
        values = [row[col] for row in df.select(col).collect() if row[col] is not None]
        invalid = [v for v in values if not compiled.search(str(v))]
        if invalid:
            raise ValidationError(f"Column '{col}' has {len(invalid)} values not matching pattern '{pattern}'.")


def _range(df: DataFrame, transformation: Transformation) -> None:
    cols = transformation.input_cols or []
    min_val = transformation.params.get("min")
    max_val = transformation.params.get("max")
    if not cols or min_val is None or max_val is None:
        raise ValueError("'range' validator requires input_cols, params.min, and params.max.")
    for col in cols:
        out_of_range = df.filter((df[col] < min_val) | (df[col] > max_val)).count()
        if out_of_range > 0:
            raise ValidationError(
                f"Column '{col}' has {out_of_range} values outside range [{min_val}, {max_val}]."
            )
