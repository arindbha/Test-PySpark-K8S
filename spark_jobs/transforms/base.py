"""Transform abstractions — ABC and built-in transform implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Transform(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame: ...


# ---------------------------------------------------------------------------
# Built-in transforms
# ---------------------------------------------------------------------------


class SQLTransform(Transform):
    """Execute a SQL query using a temporary view named ``__table__``."""

    def apply(self, df: DataFrame) -> DataFrame:
        query = self.config.get("query", "")
        if not query:
            return df
        view_name = self.config.get("view_name", "__table__")
        df.createOrReplaceTempView(view_name)
        return df.sparkSession.sql(query)


class ColumnRenameTransform(Transform):
    """Rename columns based on a mapping dict in config."""

    def apply(self, df: DataFrame) -> DataFrame:
        mapping = self.config.get("mapping", {})
        for old_name, new_name in mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df


class FilterTransform(Transform):
    """Apply a SQL filter condition."""

    def apply(self, df: DataFrame) -> DataFrame:
        condition = self.config.get("condition", "")
        if not condition:
            return df
        return df.filter(condition)


class TypeCastTransform(Transform):
    """Cast columns to specified types."""

    def apply(self, df: DataFrame) -> DataFrame:
        casts = self.config.get("casts", {})
        for col_name, target_type in casts.items():
            df = df.withColumn(col_name, df[col_name].cast(target_type))
        return df


class DropColumnsTransform(Transform):
    """Drop specified columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        columns = self.config.get("columns", [])
        if columns:
            df = df.drop(*columns)
        return df


class AddColumnTransform(Transform):
    """Add a new column using a SQL expression."""

    def apply(self, df: DataFrame) -> DataFrame:
        name = self.config.get("name", "")
        expression = self.config.get("expression", "")
        if name and expression:
            df = df.withColumn(name, F.expr(expression))
        return df


# ---------------------------------------------------------------------------
# Transform pipeline & factory
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, type[Transform]] = {
    "sql": SQLTransform,
    "column_rename": ColumnRenameTransform,
    "filter": FilterTransform,
    "type_cast": TypeCastTransform,
    "drop_columns": DropColumnsTransform,
    "add_column": AddColumnTransform,
}


def get_transform(transform_type: str, config: dict) -> Transform:
    cls = _REGISTRY.get(transform_type)
    if cls is None:
        raise ValueError(f"Unknown transform type: {transform_type}")
    return cls(config)


class TransformPipeline:
    """Chains multiple transforms in order."""

    @staticmethod
    def apply(df: DataFrame, steps: list[dict]) -> DataFrame:
        for step in steps:
            transform_type = step.get("type", "")
            config = step.get("config", {})
            transform = get_transform(transform_type, config)
            df = transform.apply(df)
        return df
