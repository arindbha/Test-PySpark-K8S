"""Filesystem source — reads from local, GCS, S3, or HDFS paths."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class FilesystemSource(Source):
    def validate_config(self) -> None:
        if "path" not in self.config:
            raise ValueError("FilesystemSource requires 'path' in config")

    def read(self, spark: SparkSession) -> DataFrame:
        fmt = self.config.get("format", "parquet")
        path = self.config["path"]
        options = self.config.get("options", {})
        return spark.read.format(fmt).options(**options).load(path)
