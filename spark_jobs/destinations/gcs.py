"""GCS / filesystem destination."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_jobs.destinations.base import Destination


class GcsDestination(Destination):
    def validate_config(self) -> None:
        if "path" not in self.config:
            raise ValueError("GcsDestination requires 'path' in config")

    def write(self, df: DataFrame) -> None:
        fmt = self.config.get("format", "parquet")
        mode = self.config.get("mode", "overwrite")
        path = self.config["path"]
        partition_by = self.config.get("partition_by", [])
        options = self.config.get("options", {})

        writer = df.write.format(fmt).mode(mode).options(**options)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
