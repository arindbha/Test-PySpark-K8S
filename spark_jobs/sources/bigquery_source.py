"""BigQuery source."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import BaseSource


class BigQuerySource(BaseSource):
    def __init__(self, table: str, options: dict[str, str] | None = None) -> None:
        self.table = table
        self.options = options or {}

    def read(self, spark: SparkSession) -> DataFrame:
        return (
            spark.read.format("bigquery")
            .option("table", self.table)
            .options(**self.options)
            .load()
        )
