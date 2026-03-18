"""Base destination interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseDestination(ABC):
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write a DataFrame to the destination."""
