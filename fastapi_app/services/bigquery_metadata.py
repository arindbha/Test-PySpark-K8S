"""BigQuery-backed metadata store implementation."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from fastapi_app.services.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


class BigQueryMetadataStore(MetadataStore):
    """Persist pipeline run / batch metadata in BigQuery tables."""

    def __init__(self, project_id: str, dataset: str, credentials_path: str = "") -> None:
        self._project = project_id
        self._dataset = dataset
        self._credentials_path = credentials_path
        self._client: bigquery.Client | None = None
        self._runs_table = f"{project_id}.{dataset}.pipeline_runs"
        self._batches_table = f"{project_id}.{dataset}.pipeline_batches"

    async def initialize(self) -> None:
        kwargs: dict[str, Any] = {"project": self._project}
        if self._credentials_path:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(self._credentials_path)
            kwargs["credentials"] = creds
        self._client = bigquery.Client(**kwargs)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        assert self._client is not None
        runs_schema = [
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pipeline_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("config", "STRING"),  # JSON string
            bigquery.SchemaField("spark_app_id", "STRING"),
            bigquery.SchemaField("k8s_driver_pod", "STRING"),
            bigquery.SchemaField("k8s_namespace", "STRING"),
            bigquery.SchemaField("datasets_status", "STRING"),  # JSON string
            bigquery.SchemaField("start_time", "TIMESTAMP"),
            bigquery.SchemaField("end_time", "TIMESTAMP"),
            bigquery.SchemaField("error_details", "STRING"),
            bigquery.SchemaField("parent_run_id", "STRING"),
            bigquery.SchemaField("execution_mode", "STRING"),
            bigquery.SchemaField("batch_id", "STRING"),
            bigquery.SchemaField("priority", "INTEGER"),
            bigquery.SchemaField("checkpoint_data", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        batches_schema = [
            bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("run_ids", "STRING"),  # JSON list
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        for table_ref, schema in [
            (self._runs_table, runs_schema),
            (self._batches_table, batches_schema),
        ]:
            table = bigquery.Table(table_ref, schema=schema)
            self._client.create_table(table, exists_ok=True)
            logger.info("Ensured table %s", table_ref)

    # -- runs ---------------------------------------------------------------

    async def insert_run(self, run_data: dict[str, Any]) -> None:
        row = _serialise_row(run_data)
        row.setdefault("updated_at", _now_iso())
        assert self._client is not None
        errors = self._client.insert_rows_json(self._runs_table, [row])
        if errors:
            raise RuntimeError(f"BQ insert_run errors: {errors}")

    async def update_run(self, run_id: str, updates: dict[str, Any]) -> None:
        assert self._client is not None
        set_clauses = []
        params = []
        for key, value in updates.items():
            set_clauses.append(f"{key} = @{key}")
            params.append(bigquery.ScalarQueryParameter(key, "STRING", str(value)))
        set_clauses.append("updated_at = @updated_at")
        params.append(bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", _now_iso()))
        params.append(bigquery.ScalarQueryParameter("run_id", "STRING", run_id))
        query = f"UPDATE `{self._runs_table}` SET {', '.join(set_clauses)} WHERE run_id = @run_id"
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self._client.query(query, job_config=job_config).result()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        assert self._client is not None
        query = f"SELECT * FROM `{self._runs_table}` WHERE run_id = @run_id LIMIT 1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        )
        rows = list(self._client.query(query, job_config=job_config).result())
        if not rows:
            return None
        return dict(rows[0])

    async def list_runs(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        assert self._client is not None
        filters = filters or {}
        where_parts: list[str] = []
        params: list[bigquery.ScalarQueryParameter] = []

        if "status" in filters:
            where_parts.append("status = @status")
            params.append(bigquery.ScalarQueryParameter("status", "STRING", filters["status"]))
        if "pipeline_name" in filters:
            where_parts.append("pipeline_name = @pipeline_name")
            params.append(bigquery.ScalarQueryParameter("pipeline_name", "STRING", filters["pipeline_name"]))

        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        limit = int(filters.get("limit", 100))
        offset = int(filters.get("offset", 0))
        query = f"SELECT * FROM `{self._runs_table}` {where_clause} ORDER BY updated_at DESC LIMIT {limit} OFFSET {offset}"
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        return [dict(row) for row in self._client.query(query, job_config=job_config).result()]

    # -- batches ------------------------------------------------------------

    async def insert_batch(self, batch_data: dict[str, Any]) -> None:
        assert self._client is not None
        row = _serialise_row(batch_data)
        errors = self._client.insert_rows_json(self._batches_table, [row])
        if errors:
            raise RuntimeError(f"BQ insert_batch errors: {errors}")

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        assert self._client is not None
        query = f"SELECT * FROM `{self._batches_table}` WHERE batch_id = @batch_id LIMIT 1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
        )
        rows = list(self._client.query(query, job_config=job_config).result())
        if not rows:
            return None
        return dict(rows[0])


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_metadata_store(settings) -> MetadataStore:
    """Return the appropriate MetadataStore based on application settings."""
    if settings.metadata_store_type == "bigquery":
        return BigQueryMetadataStore(
            project_id=settings.bq_project_id,
            dataset=settings.bq_dataset,
            credentials_path=settings.gcp_credentials_path,
        )
    from fastapi_app.services.metadata_store import FileSystemMetadataStore
    return FileSystemMetadataStore(data_dir=settings.metadata_dir)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialise_row(data: dict) -> dict:
    """Ensure complex fields are JSON-encoded strings for BQ streaming insert."""
    row = dict(data)
    for key in ("config", "datasets_status", "checkpoint_data", "run_ids"):
        if key in row and not isinstance(row[key], str):
            row[key] = json.dumps(row[key], default=str)
    return row
