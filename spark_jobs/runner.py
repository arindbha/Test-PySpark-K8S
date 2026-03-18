"""Spark driver entrypoint — reads config from JOB_CONFIG_B64 env var.

Executes all datasets in the pipeline in parallel using a ThreadPoolExecutor.
Each dataset: source.read() → TransformPipeline.apply() → destination.write().
Reports per-dataset results via metadata_reporter.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("spark_jobs.runner")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _process_dataset(spark, dataset_cfg: dict) -> dict[str, Any]:
    """Process a single dataset: read → transform → write. Returns status dict."""
    from spark_jobs.destinations.base import get_destination
    from spark_jobs.sources.base import get_source
    from spark_jobs.transforms.base import TransformPipeline

    source_cfg = dataset_cfg["source"]
    dest_cfg = dataset_cfg["destination"]
    transforms = dataset_cfg.get("transforms", [])
    name = source_cfg.get("name", "unnamed")

    status: dict[str, Any] = {
        "name": name,
        "status": "running",
        "records_read": 0,
        "records_written": 0,
        "error_message": "",
        "start_time": _now_iso(),
        "end_time": None,
    }

    try:
        source = get_source(source_cfg["type"], source_cfg.get("config", {}))
        df = source.read(spark)
        status["records_read"] = df.count()

        df = TransformPipeline.apply(df, transforms)

        destination = get_destination(dest_cfg["type"], dest_cfg.get("config", {}))
        destination.write(df)
        status["records_written"] = status["records_read"]  # best effort
        status["status"] = "completed"
    except Exception as exc:
        status["status"] = "failed"
        status["error_message"] = traceback.format_exc()
        logger.error("Dataset %s failed: %s", name, exc)
    finally:
        status["end_time"] = _now_iso()

    return status


def run(job_config: dict) -> int:
    """Run the ingestion pipeline. Returns 0 on full success, 1 on any failure."""
    from spark_jobs.utils.spark_session import get_or_create_spark

    job_id = os.environ.get("JOB_ID", "unknown")
    spark = get_or_create_spark(
        app_name=job_config.get("pipeline_name", "spark-k8s-ingestion"),
        extra_config=job_config.get("spark_config", {}).get("extra_conf", {}),
    )

    datasets = job_config.get("datasets", [])
    checkpoint = job_config.get("checkpoint_data", {})
    restart_from = checkpoint.get("restart_from_stage")

    results: list[dict[str, Any]] = []
    try:
        with ThreadPoolExecutor(max_workers=max(len(datasets), 1)) as executor:
            futures = {}
            for ds in datasets:
                ds_name = ds.get("source", {}).get("name", "")
                if restart_from and ds_name != restart_from:
                    # Skip datasets before the restart stage
                    results.append({"name": ds_name, "status": "skipped"})
                    continue
                # Once we hit restart_from, clear it so subsequent datasets run
                restart_from = None
                future = executor.submit(_process_dataset, spark, ds)
                futures[future] = ds_name

            for future in as_completed(futures):
                results.append(future.result())
    finally:
        # Report results
        try:
            from spark_jobs.utils.metadata_reporter import report_results
            report_results(job_id, results)
        except Exception:
            logger.exception("Failed to report metadata")
        spark.stop()

    all_ok = all(r.get("status") in ("completed", "skipped") for r in results)
    return 0 if all_ok else 1


if __name__ == "__main__":
    config_b64 = os.environ.get("JOB_CONFIG_B64", "")
    if config_b64:
        config = json.loads(base64.b64decode(config_b64))
    elif len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            config = json.load(f)
    else:
        logger.error("No config: set JOB_CONFIG_B64 or pass config file path")
        sys.exit(2)

    exit_code = run(config)
    sys.exit(exit_code)
