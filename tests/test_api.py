"""Tests for the FastAPI ingestion API."""

import pytest
from httpx import ASGITransport, AsyncClient

from fastapi_app import create_app
from fastapi_app.services.job_manager import job_manager


@pytest.fixture
def app():
    application = create_app()
    job_manager._jobs.clear()
    return application


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


SAMPLE_PAYLOAD = {
    "job_name": "test-ingestion",
    "source": {
        "source_type": "gcs",
        "uri": "gs://test-bucket/input",
        "format": "parquet",
    },
    "destination": {
        "destination_type": "bigquery",
        "uri": "project.dataset.table",
        "mode": "overwrite",
    },
    "transforms": [
        {"operation": "filter", "params": {"condition": "age > 18"}},
    ],
}


async def test_health(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


async def test_submit_job(client: AsyncClient):
    resp = await client.post("/api/v1/ingestion/", json=SAMPLE_PAYLOAD)
    assert resp.status_code == 201
    data = resp.json()
    assert data["job_name"] == "test-ingestion"
    assert data["status"] == "pending"
    assert "job_id" in data


async def test_get_job_status(client: AsyncClient):
    resp = await client.post("/api/v1/ingestion/", json=SAMPLE_PAYLOAD)
    job_id = resp.json()["job_id"]

    resp = await client.get(f"/api/v1/ingestion/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id


async def test_get_job_not_found(client: AsyncClient):
    resp = await client.get("/api/v1/ingestion/nonexistent")
    assert resp.status_code == 404


async def test_list_jobs(client: AsyncClient):
    await client.post("/api/v1/ingestion/", json=SAMPLE_PAYLOAD)
    await client.post("/api/v1/ingestion/", json={**SAMPLE_PAYLOAD, "job_name": "second-job"})

    resp = await client.get("/api/v1/ingestion/")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


async def test_submit_validation_error(client: AsyncClient):
    resp = await client.post("/api/v1/ingestion/", json={"job_name": ""})
    assert resp.status_code == 422
