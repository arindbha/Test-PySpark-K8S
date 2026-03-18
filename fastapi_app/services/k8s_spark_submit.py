"""Service for submitting Spark jobs to Kubernetes via the K8s API."""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi_app.config import settings

logger = logging.getLogger(__name__)


def _build_spark_pod_spec(job_id: str, job_config: dict[str, Any]) -> dict[str, Any]:
    """Build a Kubernetes Pod spec for a Spark driver that runs the ingestion job."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"spark-ingestion-{job_id}",
            "namespace": settings.k8s_namespace,
            "labels": {
                "app": "spark-ingestion",
                "spark-role": "driver",
                "job-id": job_id,
            },
        },
        "spec": {
            "serviceAccountName": settings.spark_service_account,
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "spark-driver",
                    "image": settings.spark_image,
                    "args": [
                        "driver",
                        "--master", f"k8s://https://kubernetes.default.svc",
                        "--deploy-mode", "client",
                        "--conf", f"spark.kubernetes.namespace={settings.k8s_namespace}",
                        "--conf", f"spark.kubernetes.container.image={settings.spark_image}",
                        "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName="
                        f"{settings.spark_service_account}",
                        "local:///opt/spark/work-dir/spark_jobs/ingestion_job.py",
                        "/tmp/job_config.json",
                    ],
                    "env": [
                        {"name": "SPARK_LOCAL_DIRS", "value": "/tmp/spark"},
                    ],
                    "volumeMounts": [
                        {"name": "job-config", "mountPath": "/tmp/job_config.json", "subPath": "job_config.json"},
                    ],
                    "resources": {
                        "requests": {"cpu": "500m", "memory": "1Gi"},
                        "limits": {"cpu": "1", "memory": "2Gi"},
                    },
                }
            ],
            "volumes": [
                {
                    "name": "job-config",
                    "configMap": {"name": f"spark-ingestion-{job_id}-config"},
                },
            ],
        },
    }


def _build_config_map(job_id: str, job_config: dict[str, Any]) -> dict[str, Any]:
    """Build a ConfigMap containing the job configuration JSON."""
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"spark-ingestion-{job_id}-config",
            "namespace": settings.k8s_namespace,
            "labels": {
                "app": "spark-ingestion",
                "job-id": job_id,
            },
        },
        "data": {
            "job_config.json": json.dumps(job_config),
        },
    }


async def submit_spark_job(job_id: str, job_config: dict[str, Any]) -> None:
    """Submit a Spark ingestion job to Kubernetes.

    Creates a ConfigMap with the job config and a driver Pod.
    Uses kubernetes-asyncio for non-blocking API calls.
    """
    try:
        if settings.k8s_in_cluster:
            from kubernetes_asyncio.config import load_incluster_config
            load_incluster_config()
        else:
            from kubernetes_asyncio.config import load_kube_config
            await load_kube_config()

        from kubernetes_asyncio.client import ApiClient, CoreV1Api

        async with ApiClient() as api_client:
            v1 = CoreV1Api(api_client)

            config_map = _build_config_map(job_id, job_config)
            await v1.create_namespaced_config_map(
                namespace=settings.k8s_namespace,
                body=config_map,
            )
            logger.info("Created ConfigMap for job %s", job_id)

            pod_spec = _build_spark_pod_spec(job_id, job_config)
            await v1.create_namespaced_pod(
                namespace=settings.k8s_namespace,
                body=pod_spec,
            )
            logger.info("Created Spark driver Pod for job %s", job_id)

    except Exception:
        logger.exception("Failed to submit Spark job %s to Kubernetes", job_id)
        raise


async def get_spark_job_pod_status(job_id: str) -> str | None:
    """Query the Spark driver pod status."""
    try:
        if settings.k8s_in_cluster:
            from kubernetes_asyncio.config import load_incluster_config
            load_incluster_config()
        else:
            from kubernetes_asyncio.config import load_kube_config
            await load_kube_config()

        from kubernetes_asyncio.client import ApiClient, CoreV1Api

        async with ApiClient() as api_client:
            v1 = CoreV1Api(api_client)
            pod = await v1.read_namespaced_pod(
                name=f"spark-ingestion-{job_id}",
                namespace=settings.k8s_namespace,
            )
            return pod.status.phase
    except Exception:
        logger.exception("Failed to get pod status for job %s", job_id)
        return None
