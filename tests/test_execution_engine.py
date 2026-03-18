"""Tests for execution engine — verify spark-submit command construction."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastapi_app.config import Settings
from fastapi_app.models.enums import ExecutionMode
from fastapi_app.services.execution_engine import (
    K8SExecutionEngine,
    LocalExecutionEngine,
    get_execution_engine,
)


@pytest.fixture
def test_settings():
    return Settings(
        spark_home="/opt/spark",
        k8s_master_url="https://k8s.example.com",
        spark_namespace="spark-ns",
        spark_image="my-spark:latest",
        spark_service_account="spark-sa",
        default_deploy_mode="cluster",
    )


def test_get_execution_engine_k8s(test_settings):
    engine = get_execution_engine(ExecutionMode.K8S, test_settings)
    assert isinstance(engine, K8SExecutionEngine)


def test_get_execution_engine_local(test_settings):
    engine = get_execution_engine(ExecutionMode.LOCAL, test_settings)
    assert isinstance(engine, LocalExecutionEngine)


@pytest.mark.asyncio
async def test_k8s_submit_builds_correct_command(test_settings):
    engine = K8SExecutionEngine(test_settings)
    config = {
        "pipeline_name": "test",
        "spark_config": {
            "driver_memory": "2g",
            "executor_memory": "4g",
            "executor_instances": 3,
            "extra_conf": {},
        },
    }

    mock_proc = AsyncMock()

    # Create a proper async iterator for stdout
    async def _empty_aiter():
        return
        yield  # noqa: make it an async generator

    mock_proc.stdout = _empty_aiter()

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        result = await engine.submit("run123", config)

        # Verify spark-submit was called
        mock_exec.assert_called_once()
        args = mock_exec.call_args[0]
        assert args[0] == "/opt/spark/bin/spark-submit"
        assert "--master" in args
        assert "k8s://https://k8s.example.com" in args
        assert "--deploy-mode" in args
        assert "--driver-memory" in args
        assert "2g" in args
        assert "--executor-memory" in args
        assert "4g" in args


@pytest.mark.asyncio
async def test_local_submit_builds_correct_command(test_settings):
    engine = LocalExecutionEngine(test_settings)
    config = {"pipeline_name": "test", "spark_config": {"driver_memory": "1g", "extra_conf": {}}}

    mock_proc = AsyncMock()
    mock_proc.returncode = None

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        result = await engine.submit("run456", config)

        mock_exec.assert_called_once()
        args = mock_exec.call_args[0]
        assert args[0] == "/opt/spark/bin/spark-submit"
        assert "--master" in args
        assert "local[*]" in args
        assert result == "local-run456"


@pytest.mark.asyncio
async def test_local_cancel(test_settings):
    engine = LocalExecutionEngine(test_settings)
    mock_proc = AsyncMock()
    mock_proc.returncode = None
    engine._processes["run789"] = mock_proc

    await engine.cancel("run789", "local-run789")
    mock_proc.terminate.assert_called_once()


@pytest.mark.asyncio
async def test_local_get_status(test_settings):
    engine = LocalExecutionEngine(test_settings)

    # Unknown process
    assert await engine.get_status("local-unknown") == "Unknown"

    # Running process
    mock_proc = AsyncMock()
    mock_proc.returncode = None
    engine._processes["running"] = mock_proc
    assert await engine.get_status("local-running") == "Running"

    # Completed process
    mock_proc2 = AsyncMock()
    mock_proc2.returncode = 0
    engine._processes["done"] = mock_proc2
    assert await engine.get_status("local-done") == "Completed"
