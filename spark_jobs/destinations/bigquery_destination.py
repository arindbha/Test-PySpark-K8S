"""BigQuery destination."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_jobs.destinations.base import BaseDestination


class BigQueryDestination(BaseDestination):
    def __init__(self, table: str, mode: str = "overwrite", options: dict[str, str] | None = None) -> None:
        self.table = table
        self.mode = mode
        self.options = options or {}

    def write(self, df: DataFrame) -> None:
        (
            df.write.format("bigquery")
            .option("table", self.table)
            .mode(self.mode)
            .options(**self.options)
            .save()
        )
