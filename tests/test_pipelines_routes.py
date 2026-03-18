"""Tests for /v1/pipelines/* endpoints."""

import pytest
from httpx import AsyncClient


async def test_run_pipeline(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run", json={
        "pipeline_name": "test_pipeline",
    })
    assert resp.status_code == 202
    data = resp.json()
    assert "run_id" in data
    assert data["status"] == "queued"
    assert data["message"] == "Job queued successfully"


async def test_run_pipeline_not_found(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run", json={
        "pipeline_name": "nonexistent",
    })
    assert resp.status_code == 404


async def test_run_pipeline_with_overrides(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run", json={
        "pipeline_name": "test_pipeline",
        "override_params": {"spark_config": {"driver_memory": "4g"}},
        "priority": 5,
    })
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"


async def test_run_pipeline_with_execution_mode_override(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run", json={
        "pipeline_name": "test_pipeline",
        "execution_mode_override": "k8s",
    })
    assert resp.status_code == 202


async def test_run_batch(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run-batch", json={
        "pipelines": [
            {"name": "test_pipeline"},
            {"name": "test_pipeline", "overrides": {"spark_config": {"executor_instances": 5}}},
        ],
    })
    assert resp.status_code == 202
    data = resp.json()
    assert "batch_id" in data
    assert len(data["run_ids"]) == 2


async def test_run_batch_pipeline_not_found(client: AsyncClient):
    resp = await client.post("/v1/pipelines/run-batch", json={
        "pipelines": [{"name": "nonexistent"}],
    })
    assert resp.status_code == 404


async def test_get_run_status(client: AsyncClient, metadata_store):
    # Insert a run directly
    await metadata_store.insert_run({
        "run_id": "test123",
        "pipeline_name": "test_pipeline",
        "status": "running",
        "execution_mode": "local",
        "datasets_status": [{"name": "ds1", "status": "running"}],
    })
    resp = await client.get("/v1/pipelines/test123/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["run_id"] == "test123"
    assert data["status"] == "running"
    assert len(data["datasets"]) == 1


async def test_get_run_status_not_found(client: AsyncClient):
    resp = await client.get("/v1/pipelines/nonexistent/status")
    assert resp.status_code == 404


async def test_restart_pipeline(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({
        "run_id": "failed_run",
        "pipeline_name": "test_pipeline",
        "status": "failed",
        "config": {
            "pipeline_name": "test_pipeline",
            "datasets": [{"source": {"name": "s1", "type": "filesystem", "config": {}}, "destination": {"type": "gcs", "config": {}}}],
            "execution_mode": "local",
        },
        "execution_mode": "local",
        "datasets_status": [{"name": "s1", "status": "failed"}],
    })
    resp = await client.post("/v1/pipelines/failed_run/restart", json={})
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"
    assert data["message"] == "Restart queued successfully"


async def test_restart_not_found(client: AsyncClient):
    resp = await client.post("/v1/pipelines/nonexistent/restart", json={})
    assert resp.status_code == 404
