"""Abstract base class for data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class Source(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.validate_config()

    def validate_config(self) -> None:
        """Override to add config validation."""

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame: ...


def get_source(source_type: str, config: dict) -> Source:
    """Factory that returns a Source instance for the given type."""
    from spark_jobs.sources.api import ApiSource
    from spark_jobs.sources.bigquery import BigQuerySource
    from spark_jobs.sources.custom import CustomSource
    from spark_jobs.sources.filesystem import FilesystemSource
    from spark_jobs.sources.jdbc import JdbcSource
    from spark_jobs.sources.sftp import SftpSource

    registry: dict[str, type[Source]] = {
        "filesystem": FilesystemSource,
        "jdbc": JdbcSource,
        "api": ApiSource,
        "sftp": SftpSource,
        "custom": CustomSource,
        "bigquery": BigQuerySource,
    }
    cls = registry.get(source_type)
    if cls is None:
        raise ValueError(f"Unknown source type: {source_type}")
    return cls(config)
