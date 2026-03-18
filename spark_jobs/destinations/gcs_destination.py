"""Google Cloud Storage destination."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_jobs.destinations.base import BaseDestination


class GCSDestination(BaseDestination):
    def __init__(self, uri: str, fmt: str = "parquet", mode: str = "overwrite", options: dict[str, str] | None = None) -> None:
        self.uri = uri
        self.fmt = fmt
        self.mode = mode
        self.options = options or {}

    def write(self, df: DataFrame) -> None:
        df.write.format(self.fmt).mode(self.mode).options(**self.options).save(self.uri)
