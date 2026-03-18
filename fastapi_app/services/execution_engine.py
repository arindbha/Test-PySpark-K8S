"""Execution engine abstraction for submitting Spark jobs."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any

from fastapi_app.config import Settings
from fastapi_app.models.enums import ExecutionMode

logger = logging.getLogger(__name__)


class ExecutionEngine(ABC):
    """Abstract base for Spark job execution backends."""

    @abstractmethod
    async def submit(self, run_id: str, config: dict[str, Any]) -> str:
        """Submit a Spark job. Returns a spark_app_id / reference."""

    @abstractmethod
    async def cancel(self, run_id: str, app_id: str) -> None:
        """Cancel a running Spark job."""

    @abstractmethod
    async def get_status(self, app_id: str) -> str:
        """Get the status of a submitted Spark job."""


# ---------------------------------------------------------------------------
# Kubernetes execution via spark-submit
# ---------------------------------------------------------------------------


class K8SExecutionEngine(ExecutionEngine):
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def submit(self, run_id: str, config: dict[str, Any]) -> str:
        config_b64 = base64.b64encode(json.dumps(config, default=str).encode()).decode()
        spark_submit = os.path.join(self._settings.spark_home, "bin", "spark-submit")

        cmd = [
            spark_submit,
            "--master", f"k8s://{self._settings.k8s_master_url}",
            "--deploy-mode", self._settings.default_deploy_mode,
            "--name", f"ingestion-{run_id}",
            "--conf", f"spark.kubernetes.namespace={self._settings.spark_namespace}",
            "--conf", f"spark.kubernetes.container.image={self._settings.spark_image}",
            "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={self._settings.spark_service_account}",
            "--conf", f"spark.kubernetes.driverEnv.JOB_CONFIG_B64={config_b64}",
            "--conf", f"spark.kubernetes.driverEnv.JOB_ID={run_id}",
        ]

        # Apply extra Spark conf from pipeline
        spark_config = config.get("spark_config", {})
        if spark_config.get("driver_memory"):
            cmd.extend(["--driver-memory", spark_config["driver_memory"]])
        if spark_config.get("executor_memory"):
            cmd.extend(["--executor-memory", spark_config["executor_memory"]])
        if spark_config.get("executor_instances"):
            cmd.extend(["--num-executors", str(spark_config["executor_instances"])])
        for k, v in spark_config.get("extra_conf", {}).items():
            cmd.extend(["--conf", f"{k}={v}"])

        cmd.append("/opt/spark/jobs/runner.py")

        logger.info("Submitting K8S spark-submit for run %s", run_id)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Read first few lines to find the driver pod name
        driver_pod = ""
        assert proc.stdout is not None
        async for raw_line in proc.stdout:
            line = raw_line.decode().strip()
            if "pod name:" in line.lower():
                driver_pod = line.split(":")[-1].strip()
                break

        return driver_pod or f"ingestion-{run_id}"

    async def cancel(self, run_id: str, app_id: str) -> None:
        try:
            from kubernetes_asyncio.config import load_incluster_config
            from kubernetes_asyncio.client import ApiClient, CoreV1Api

            load_incluster_config()
            async with ApiClient() as api_client:
                v1 = CoreV1Api(api_client)
                await v1.delete_namespaced_pod(
                    name=app_id,
                    namespace=self._settings.spark_namespace,
                )
            logger.info("Deleted driver pod %s for run %s", app_id, run_id)
        except Exception:
            logger.exception("Failed to cancel K8S job %s", run_id)
            raise

    async def get_status(self, app_id: str) -> str:
        try:
            from kubernetes_asyncio.config import load_incluster_config
            from kubernetes_asyncio.client import ApiClient, CoreV1Api

            load_incluster_config()
            async with ApiClient() as api_client:
                v1 = CoreV1Api(api_client)
                pod = await v1.read_namespaced_pod(
                    name=app_id,
                    namespace=self._settings.spark_namespace,
                )
                return pod.status.phase or "Unknown"
        except Exception:
            logger.exception("Failed to get status for %s", app_id)
            return "Unknown"


# ---------------------------------------------------------------------------
# Google Dataproc execution
# ---------------------------------------------------------------------------


class DataprocExecutionEngine(ExecutionEngine):
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def submit(self, run_id: str, config: dict[str, Any]) -> str:
        from google.cloud import dataproc_v1, storage

        # Upload config to GCS
        config_json = json.dumps(config, default=str)
        gcs_path = f"gs://{self._settings.spark_file_upload_path}/configs/{run_id}.json"
        bucket_name = self._settings.spark_file_upload_path.split("/")[0] if "/" in self._settings.spark_file_upload_path else self._settings.spark_file_upload_path
        blob_name = f"configs/{run_id}.json"

        storage_client = storage.Client(project=self._settings.dataproc_project)
        bucket = storage_client.bucket(bucket_name)
        bucket.blob(blob_name).upload_from_string(config_json)

        # Submit PySpark job
        job_client = dataproc_v1.JobControllerAsyncClient()
        job = {
            "placement": {"cluster_name": self._settings.dataproc_cluster},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{self._settings.spark_file_upload_path}/runner.py",
                "args": [gcs_path],
            },
            "reference": {"job_id": f"ingestion-{run_id}"},
        }
        operation = await job_client.submit_job_as_operation(
            project_id=self._settings.dataproc_project,
            region=self._settings.dataproc_region,
            job=job,
        )
        logger.info("Submitted Dataproc job for run %s", run_id)
        return f"ingestion-{run_id}"

    async def cancel(self, run_id: str, app_id: str) -> None:
        from google.cloud import dataproc_v1

        job_client = dataproc_v1.JobControllerAsyncClient()
        await job_client.cancel_job(
            project_id=self._settings.dataproc_project,
            region=self._settings.dataproc_region,
            job_id=app_id,
        )
        logger.info("Cancelled Dataproc job %s", app_id)

    async def get_status(self, app_id: str) -> str:
        from google.cloud import dataproc_v1

        job_client = dataproc_v1.JobControllerAsyncClient()
        job = await job_client.get_job(
            project_id=self._settings.dataproc_project,
            region=self._settings.dataproc_region,
            job_id=app_id,
        )
        return job.status.state.name


# ---------------------------------------------------------------------------
# Local execution (dev / testing)
# ---------------------------------------------------------------------------


class LocalExecutionEngine(ExecutionEngine):
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    async def submit(self, run_id: str, config: dict[str, Any]) -> str:
        config_b64 = base64.b64encode(json.dumps(config, default=str).encode()).decode()
        spark_submit = os.path.join(self._settings.spark_home, "bin", "spark-submit")

        cmd = [
            spark_submit,
            "--master", "local[*]",
            "--name", f"ingestion-{run_id}",
        ]

        spark_config = config.get("spark_config", {})
        if spark_config.get("driver_memory"):
            cmd.extend(["--driver-memory", spark_config["driver_memory"]])
        for k, v in spark_config.get("extra_conf", {}).items():
            cmd.extend(["--conf", f"{k}={v}"])

        cmd.append("spark_jobs/runner.py")

        env = {**os.environ, "JOB_CONFIG_B64": config_b64, "JOB_ID": run_id}
        logger.info("Submitting local spark-submit for run %s", run_id)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._processes[run_id] = proc
        return f"local-{run_id}"

    async def cancel(self, run_id: str, app_id: str) -> None:
        proc = self._processes.pop(run_id, None)
        if proc and proc.returncode is None:
            proc.terminate()
            logger.info("Terminated local process for run %s", run_id)

    async def get_status(self, app_id: str) -> str:
        run_id = app_id.removeprefix("local-")
        proc = self._processes.get(run_id)
        if proc is None:
            return "Unknown"
        if proc.returncode is None:
            return "Running"
        return "Completed" if proc.returncode == 0 else "Failed"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_execution_engine(mode: ExecutionMode, settings: Settings) -> ExecutionEngine:
    """Return an ExecutionEngine for the given mode."""
    if mode == ExecutionMode.K8S:
        return K8SExecutionEngine(settings)
    if mode == ExecutionMode.DATAPROC:
        return DataprocExecutionEngine(settings)
    return LocalExecutionEngine(settings)
