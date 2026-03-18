"""Background monitor that watches Spark driver pod status on Kubernetes."""

from __future__ import annotations

import asyncio
import logging

from fastapi_app.config import Settings
from fastapi_app.models.enums import RunStatus
from fastapi_app.services.metadata_store import MetadataStore

logger = logging.getLogger(__name__)

_POD_PHASE_MAP = {
    "Succeeded": RunStatus.COMPLETED,
    "Failed": RunStatus.FAILED,
}


class K8sMonitor:
    """Periodically polls K8S for running Spark driver pod statuses."""

    def __init__(self, metadata_store: MetadataStore, settings: Settings, poll_interval: int = 30) -> None:
        self._store = metadata_store
        self._settings = settings
        self._poll_interval = poll_interval
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("K8s monitor started (interval=%ds)", self._poll_interval)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("K8s monitor stopped")

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._check_running_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in K8s monitor poll")
            await asyncio.sleep(self._poll_interval)

    async def _check_running_jobs(self) -> None:
        runs = await self._store.list_runs({"status": RunStatus.RUNNING.value})
        if not runs:
            return

        try:
            from kubernetes_asyncio.config import load_incluster_config
            from kubernetes_asyncio.client import ApiClient, CoreV1Api

            load_incluster_config()
        except Exception:
            logger.debug("Not in K8S cluster, skipping monitor poll")
            return

        async with ApiClient() as api_client:
            v1 = CoreV1Api(api_client)
            for run in runs:
                pod_name = run.get("k8s_driver_pod") or run.get("spark_app_id", "")
                if not pod_name or pod_name.startswith("local-"):
                    continue
                try:
                    pod = await v1.read_namespaced_pod(
                        name=pod_name,
                        namespace=self._settings.spark_namespace,
                    )
                    phase = pod.status.phase
                    new_status = _POD_PHASE_MAP.get(phase)
                    if new_status:
                        await self._store.update_run(run["run_id"], {"status": new_status.value})
                        logger.info("Run %s pod %s → %s", run["run_id"], pod_name, new_status.value)
                except Exception:
                    logger.debug("Could not read pod %s", pod_name)
