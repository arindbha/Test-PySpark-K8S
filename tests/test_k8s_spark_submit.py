"""Tests for K8s Spark submission service."""

from unittest.mock import AsyncMock, patch

import pytest

from fastapi_app.services.k8s_spark_submit import _build_config_map, _build_spark_pod_spec


def test_build_config_map():
    cm = _build_config_map("abc123", {"job_name": "test", "source": {}})
    assert cm["metadata"]["name"] == "spark-ingestion-abc123-config"
    assert "job_config.json" in cm["data"]
    assert '"job_name"' in cm["data"]["job_config.json"]


def test_build_spark_pod_spec():
    pod = _build_spark_pod_spec("abc123", {"job_name": "test"})
    assert pod["metadata"]["name"] == "spark-ingestion-abc123"
    assert pod["metadata"]["labels"]["job-id"] == "abc123"
    container = pod["spec"]["containers"][0]
    assert container["name"] == "spark-driver"


@pytest.mark.asyncio
async def test_submit_spark_job_calls_k8s_api():
    with (
        patch("fastapi_app.services.k8s_spark_submit.settings") as mock_settings,
        patch("kubernetes_asyncio.client.CoreV1Api") as MockCoreV1Api,
        patch("kubernetes_asyncio.client.ApiClient") as MockApiClient,
        patch("kubernetes_asyncio.config.load_incluster_config"),
    ):
        mock_settings.k8s_in_cluster = True
        mock_settings.k8s_namespace = "test-ns"
        mock_settings.spark_service_account = "spark"
        mock_settings.spark_image = "apache/spark:4.0.0"

        mock_api_instance = AsyncMock()
        MockCoreV1Api.return_value = mock_api_instance

        mock_client_instance = AsyncMock()
        MockApiClient.return_value.__aenter__ = AsyncMock(return_value=mock_client_instance)
        MockApiClient.return_value.__aexit__ = AsyncMock(return_value=False)

        from fastapi_app.services.k8s_spark_submit import submit_spark_job
        await submit_spark_job("job123", {"job_name": "test-job"})

        mock_api_instance.create_namespaced_config_map.assert_called_once()
        mock_api_instance.create_namespaced_pod.assert_called_once()
