"""Google Cloud Storage source."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import BaseSource


class GCSSource(BaseSource):
    def __init__(self, uri: str, fmt: str = "parquet", options: dict[str, str] | None = None) -> None:
        self.uri = uri
        self.fmt = fmt
        self.options = options or {}

    def read(self, spark: SparkSession) -> DataFrame:
        reader = spark.read.format(self.fmt).options(**self.options)
        return reader.load(self.uri)
