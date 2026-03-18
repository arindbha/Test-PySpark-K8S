"""JDBC destination — writes to relational databases."""

from __future__ import annotations

from pyspark.sql import DataFrame

from spark_jobs.destinations.base import Destination


class JdbcDestination(Destination):
    def validate_config(self) -> None:
        for key in ("url", "table"):
            if key not in self.config:
                raise ValueError(f"JdbcDestination requires '{key}' in config")

    def write(self, df: DataFrame) -> None:
        mode = self.config.get("mode", "overwrite")
        writer = (
            df.write.format("jdbc")
            .option("url", self.config["url"])
            .option("dbtable", self.config["table"])
            .mode(mode)
        )
        if "driver" in self.config:
            writer = writer.option("driver", self.config["driver"])
        if "user" in self.config:
            writer = writer.option("user", self.config["user"])
        if "password" in self.config:
            writer = writer.option("password", self.config["password"])
        for k, v in self.config.get("options", {}).items():
            writer = writer.option(k, v)
        writer.save()
