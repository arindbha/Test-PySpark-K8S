"""JDBC source — reads from relational databases via Spark JDBC connector."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class JdbcSource(Source):
    def validate_config(self) -> None:
        for key in ("url", "table"):
            if key not in self.config:
                raise ValueError(f"JdbcSource requires '{key}' in config")

    def read(self, spark: SparkSession) -> DataFrame:
        reader = spark.read.format("jdbc")
        reader = reader.option("url", self.config["url"])
        reader = reader.option("dbtable", self.config["table"])
        if "driver" in self.config:
            reader = reader.option("driver", self.config["driver"])
        if "user" in self.config:
            reader = reader.option("user", self.config["user"])
        if "password" in self.config:
            reader = reader.option("password", self.config["password"])
        for k, v in self.config.get("options", {}).items():
            reader = reader.option(k, v)
        return reader.load()
