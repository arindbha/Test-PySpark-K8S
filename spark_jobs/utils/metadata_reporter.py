"""Reports pipeline run results back to the metadata store.

Supports two modes:
- HTTP callback: POSTs results to a FastAPI endpoint (filesystem metadata mode)
- BigQuery direct write: inserts results into BQ (production mode)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def report_results(job_id: str, dataset_results: list[dict[str, Any]]) -> None:
    """Report per-dataset results to the metadata store."""
    callback_url = os.environ.get("METADATA_CALLBACK_URL")
    bq_project = os.environ.get("BQ_PROJECT_ID")

    overall_status = "completed" if all(
        r.get("status") in ("completed", "skipped") for r in dataset_results
    ) else "failed"

    payload = {
        "run_id": job_id,
        "status": overall_status,
        "datasets_status": dataset_results,
    }

    if callback_url:
        _report_http(callback_url, job_id, payload)
    elif bq_project:
        _report_bigquery(bq_project, job_id, payload)
    else:
        logger.info("No reporter configured. Results: %s", json.dumps(payload, default=str))


def _report_http(callback_url: str, job_id: str, payload: dict) -> None:
    import requests

    url = f"{callback_url.rstrip('/')}/v1/pipelines/{job_id}/callback"
    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        logger.info("Reported results via HTTP for job %s", job_id)
    except Exception:
        logger.exception("Failed to report via HTTP for job %s", job_id)


def _report_bigquery(project: str, job_id: str, payload: dict) -> None:
    try:
        from google.cloud import bigquery

        dataset = os.environ.get("BQ_DATASET", "ingestion_metadata")
        table_ref = f"{project}.{dataset}.pipeline_runs"
        client = bigquery.Client(project=project)

        row = {
            "run_id": job_id,
            "status": payload["status"],
            "datasets_status": json.dumps(payload["datasets_status"], default=str),
        }
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error("BQ insert errors for job %s: %s", job_id, errors)
        else:
            logger.info("Reported results via BigQuery for job %s", job_id)
    except Exception:
        logger.exception("Failed to report via BigQuery for job %s", job_id)
