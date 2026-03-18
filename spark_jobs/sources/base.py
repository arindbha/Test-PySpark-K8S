"""Base source interface for Spark ingestion jobs."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class BaseSource(ABC):
    """Abstract base class for data sources."""

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """Read data from the source and return a Spark DataFrame."""
