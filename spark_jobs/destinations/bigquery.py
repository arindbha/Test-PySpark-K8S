"""BigQuery destination."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_jobs.destinations.base import Destination


class BigQueryDestination(Destination):
    def validate_config(self) -> None:
        if "table" not in self.config:
            raise ValueError("BigQueryDestination requires 'table' in config")

    def write(self, df: DataFrame) -> None:
        mode = self.config.get("mode", "overwrite")
        writer = (
            df.write.format("bigquery")
            .option("table", self.config["table"])
            .mode(mode)
        )
        if "temp_gcs_bucket" in self.config:
            writer = writer.option("temporaryGcsBucket", self.config["temp_gcs_bucket"])
        for k, v in self.config.get("options", {}).items():
            writer = writer.option(k, v)
        writer.save()
