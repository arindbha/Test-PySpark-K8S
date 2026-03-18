"""Abstract base class for data destinations."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Destination(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.validate_config()

    def validate_config(self) -> None:
        """Override to add config validation."""

    @abstractmethod
    def write(self, df: DataFrame) -> None: ...


def get_destination(dest_type: str, config: dict) -> Destination:
    """Factory that returns a Destination instance for the given type."""
    from spark_jobs.destinations.bigquery import BigQueryDestination
    from spark_jobs.destinations.gcs import GcsDestination
    from spark_jobs.destinations.jdbc import JdbcDestination

    registry: dict[str, type[Destination]] = {
        "gcs": GcsDestination,
        "bigquery": BigQueryDestination,
        "jdbc": JdbcDestination,
    }
    cls = registry.get(dest_type)
    if cls is None:
        raise ValueError(f"Unknown destination type: {dest_type}")
    return cls(config)
