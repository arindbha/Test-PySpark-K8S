"""Shared test fixtures."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml
from httpx import ASGITransport, AsyncClient

from fastapi_app.config import Settings
from fastapi_app.services.metadata_store import FileSystemMetadataStore
from fastapi_app.services.pipeline_loader import PipelineLoader


@pytest.fixture
def tmp_data_dir(tmp_path):
    return str(tmp_path / "data")


@pytest.fixture
def tmp_pipelines_dir(tmp_path):
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    # Create a test pipeline YAML
    config = {
        "pipeline_name": "test_pipeline",
        "execution_mode": "local",
        "datasets": [
            {
                "source": {
                    "name": "test_source",
                    "type": "filesystem",
                    "config": {"path": "/tmp/input.csv", "format": "csv"},
                },
                "destination": {
                    "type": "gcs",
                    "config": {"path": "gs://bucket/output", "format": "parquet", "mode": "overwrite"},
                },
                "transforms": [
                    {"type": "filter", "config": {"condition": "id > 0"}},
                ],
            }
        ],
        "spark_config": {
            "driver_memory": "1g",
            "executor_memory": "2g",
            "executor_instances": 2,
        },
    }
    with open(pipelines_dir / "test_pipeline.yaml", "w") as f:
        yaml.dump(config, f)
    return str(pipelines_dir)


@pytest.fixture
async def metadata_store(tmp_data_dir):
    store = FileSystemMetadataStore(tmp_data_dir)
    await store.initialize()
    return store


@pytest.fixture
def settings(tmp_data_dir, tmp_pipelines_dir):
    return Settings(
        metadata_store_type="filesystem",
        metadata_dir=tmp_data_dir,
        pipelines_dir=tmp_pipelines_dir,
        default_execution_mode="local",
        max_concurrent_jobs=2,
    )


@pytest.fixture
def pipeline_loader(tmp_pipelines_dir):
    return PipelineLoader(tmp_pipelines_dir)


@pytest.fixture
def mock_job_queue():
    queue = AsyncMock()
    queue.queue_depth = 0
    queue.active_job_count = 0
    return queue


@pytest.fixture
async def app(metadata_store, pipeline_loader, mock_job_queue, settings):
    from fastapi import FastAPI
    from fastapi_app.routes import health, pipelines, runs

    application = FastAPI()
    application.include_router(health.router)
    application.include_router(pipelines.router)
    application.include_router(runs.router)

    application.state.metadata_store = metadata_store
    application.state.pipeline_loader = pipeline_loader
    application.state.job_queue = mock_job_queue
    application.state.settings = settings

    return application


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
