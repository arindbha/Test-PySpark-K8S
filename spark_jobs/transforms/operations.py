"""Transform operations applied to Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame


def apply_filter(df: DataFrame, params: dict[str, str]) -> DataFrame:
    condition = params.get("condition", "")
    if not condition:
        return df
    return df.filter(condition)


def apply_select(df: DataFrame, params: dict[str, str]) -> DataFrame:
    columns = params.get("columns", "")
    if not columns:
        return df
    return df.select(*[c.strip() for c in columns.split(",")])


def apply_rename(df: DataFrame, params: dict[str, str]) -> DataFrame:
    for old_name, new_name in params.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def apply_sql(df: DataFrame, params: dict[str, str]) -> DataFrame:
    query = params.get("query", "")
    if not query:
        return df
    view_name = params.get("view_name", "source_data")
    df.createOrReplaceTempView(view_name)
    return df.sparkSession.sql(query)


OPERATIONS: dict[str, callable] = {
    "filter": apply_filter,
    "select": apply_select,
    "rename": apply_rename,
    "sql": apply_sql,
}


def apply_transforms(df: DataFrame, steps: list[dict]) -> DataFrame:
    for step in steps:
        operation = step.get("operation", "")
        params = step.get("params", {})
        fn = OPERATIONS.get(operation)
        if fn is None:
            raise ValueError(f"Unknown transform operation: {operation}")
        df = fn(df, params)
    return df
