"""Main Spark ingestion job entrypoint."""

from __future__ import annotations

import json
import sys

from spark_jobs.destinations.bigquery_destination import BigQueryDestination
from spark_jobs.destinations.gcs_destination import GCSDestination
from spark_jobs.sources.bigquery_source import BigQuerySource
from spark_jobs.sources.gcs_source import GCSSource
from spark_jobs.transforms.operations import apply_transforms
from spark_jobs.utils.spark_session import get_or_create_spark


def build_source(config: dict):
    source_type = config["source_type"]
    if source_type == "gcs":
        return GCSSource(uri=config["uri"], fmt=config.get("format", "parquet"), options=config.get("options", {}))
    if source_type == "bigquery":
        return BigQuerySource(table=config["uri"], options=config.get("options", {}))
    raise ValueError(f"Unsupported source type: {source_type}")


def build_destination(config: dict):
    dest_type = config["destination_type"]
    if dest_type == "gcs":
        return GCSDestination(
            uri=config["uri"],
            fmt=config.get("format", "parquet"),
            mode=config.get("mode", "overwrite"),
            options=config.get("options", {}),
        )
    if dest_type == "bigquery":
        return BigQueryDestination(
            table=config["uri"],
            mode=config.get("mode", "overwrite"),
            options=config.get("options", {}),
        )
    raise ValueError(f"Unsupported destination type: {dest_type}")


def run(job_config: dict) -> None:
    spark = get_or_create_spark(
        app_name=job_config.get("job_name", "spark-k8s-ingestion"),
        extra_config=job_config.get("spark_config", {}),
    )
    try:
        source = build_source(job_config["source"])
        destination = build_destination(job_config["destination"])

        df = source.read(spark)
        df = apply_transforms(df, job_config.get("transforms", []))
        destination.write(df)
    finally:
        spark.stop()


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/work-dir/job_config.json"
    with open(config_path) as f:
        config = json.load(f)
    run(config)
