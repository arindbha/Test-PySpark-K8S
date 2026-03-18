"""Tests for FileSystemMetadataStore."""

import pytest

from fastapi_app.services.metadata_store import FileSystemMetadataStore


@pytest.fixture
async def store(tmp_path):
    s = FileSystemMetadataStore(str(tmp_path / "meta"))
    await s.initialize()
    return s


async def test_insert_and_get_run(store):
    await store.insert_run({"run_id": "r1", "pipeline_name": "p1", "status": "pending"})
    run = await store.get_run("r1")
    assert run is not None
    assert run["run_id"] == "r1"
    assert run["pipeline_name"] == "p1"


async def test_get_run_not_found(store):
    assert await store.get_run("nope") is None


async def test_update_run(store):
    await store.insert_run({"run_id": "r1", "status": "pending"})
    await store.update_run("r1", {"status": "running", "spark_app_id": "app-123"})
    run = await store.get_run("r1")
    assert run["status"] == "running"
    assert run["spark_app_id"] == "app-123"
    assert "updated_at" in run


async def test_update_nonexistent_run(store):
    # Should silently no-op
    await store.update_run("nope", {"status": "running"})


async def test_list_runs_empty(store):
    runs = await store.list_runs()
    assert runs == []


async def test_list_runs_with_filters(store):
    await store.insert_run({"run_id": "r1", "pipeline_name": "alpha", "status": "completed"})
    await store.insert_run({"run_id": "r2", "pipeline_name": "beta", "status": "failed"})
    await store.insert_run({"run_id": "r3", "pipeline_name": "alpha", "status": "running"})

    # Filter by status
    runs = await store.list_runs({"status": "completed"})
    assert len(runs) == 1
    assert runs[0]["run_id"] == "r1"

    # Filter by pipeline_name
    runs = await store.list_runs({"pipeline_name": "alpha"})
    assert len(runs) == 2

    # Pagination
    runs = await store.list_runs({"limit": 1, "offset": 0})
    assert len(runs) == 1


async def test_insert_and_get_batch(store):
    await store.insert_batch({"batch_id": "b1", "run_ids": ["r1", "r2"]})
    batch = await store.get_batch("b1")
    assert batch is not None
    assert batch["batch_id"] == "b1"
    assert batch["run_ids"] == ["r1", "r2"]


async def test_get_batch_not_found(store):
    assert await store.get_batch("nope") is None
