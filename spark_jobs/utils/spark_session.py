"""Spark session builder utility."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_or_create_spark(app_name: str = "spark-k8s-ingestion", extra_config: dict[str, str] | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in (extra_config or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
