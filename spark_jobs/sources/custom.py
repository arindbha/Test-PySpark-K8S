"""Custom source — dynamically imports a user-provided Source class."""

from __future__ import annotations

import importlib

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class CustomSource(Source):
    def validate_config(self) -> None:
        if "class_path" not in self.config:
            raise ValueError("CustomSource requires 'class_path' in config (e.g. 'my_module:MySource')")

    def read(self, spark: SparkSession) -> DataFrame:
        class_path = self.config["class_path"]
        module_path, class_name = class_path.rsplit(":", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        inner_config = {k: v for k, v in self.config.items() if k != "class_path"}
        instance = cls(inner_config)
        return instance.read(spark)
