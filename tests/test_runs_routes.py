"""Tests for /v1/runs/* endpoints."""

import pytest
from httpx import AsyncClient


async def test_list_runs_empty(client: AsyncClient):
    resp = await client.get("/v1/runs/")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_runs_with_data(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({
        "run_id": "run1",
        "pipeline_name": "pipe_a",
        "status": "completed",
        "execution_mode": "local",
    })
    await metadata_store.insert_run({
        "run_id": "run2",
        "pipeline_name": "pipe_b",
        "status": "failed",
        "execution_mode": "local",
    })
    resp = await client.get("/v1/runs/")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


async def test_list_runs_filter_by_status(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({"run_id": "r1", "pipeline_name": "p", "status": "completed", "execution_mode": "local"})
    await metadata_store.insert_run({"run_id": "r2", "pipeline_name": "p", "status": "failed", "execution_mode": "local"})

    resp = await client.get("/v1/runs/", params={"status": "completed"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["status"] == "completed"


async def test_list_runs_filter_by_pipeline_name(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({"run_id": "r1", "pipeline_name": "alpha", "status": "completed", "execution_mode": "local"})
    await metadata_store.insert_run({"run_id": "r2", "pipeline_name": "beta", "status": "completed", "execution_mode": "local"})

    resp = await client.get("/v1/runs/", params={"pipeline_name": "alpha"})
    data = resp.json()
    assert len(data) == 1
    assert data[0]["pipeline_name"] == "alpha"


async def test_list_runs_pagination(client: AsyncClient, metadata_store):
    for i in range(5):
        await metadata_store.insert_run({"run_id": f"r{i}", "pipeline_name": "p", "status": "completed", "execution_mode": "local"})

    resp = await client.get("/v1/runs/", params={"limit": 2, "offset": 0})
    assert len(resp.json()) == 2

    resp = await client.get("/v1/runs/", params={"limit": 2, "offset": 3})
    assert len(resp.json()) == 2


async def test_cancel_run(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({
        "run_id": "cancel_me",
        "pipeline_name": "p",
        "status": "running",
        "execution_mode": "local",
        "spark_app_id": "",
    })
    resp = await client.delete("/v1/runs/cancel_me/cancel")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "cancelled"


async def test_cancel_run_not_found(client: AsyncClient):
    resp = await client.delete("/v1/runs/nonexistent/cancel")
    assert resp.status_code == 404


async def test_cancel_already_completed(client: AsyncClient, metadata_store):
    await metadata_store.insert_run({
        "run_id": "done_run",
        "pipeline_name": "p",
        "status": "completed",
        "execution_mode": "local",
    })
    resp = await client.delete("/v1/runs/done_run/cancel")
    assert resp.status_code == 409
