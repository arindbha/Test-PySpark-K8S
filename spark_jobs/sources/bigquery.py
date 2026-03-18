"""BigQuery source."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class BigQuerySource(Source):
    def validate_config(self) -> None:
        if "table" not in self.config:
            raise ValueError("BigQuerySource requires 'table' in config")

    def read(self, spark: SparkSession) -> DataFrame:
        reader = spark.read.format("bigquery").option("table", self.config["table"])
        for k, v in self.config.get("options", {}).items():
            reader = reader.option(k, v)
        return reader.load()
