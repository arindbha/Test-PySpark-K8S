"""In-memory asyncio job queue with priority and concurrency control."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from fastapi_app.config import Settings
from fastapi_app.models.enums import ExecutionMode, RunStatus
from fastapi_app.services.execution_engine import get_execution_engine
from fastapi_app.services.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


@dataclass(order=True)
class _QueueItem:
    priority: int
    run_id: str = field(compare=False)
    config: dict[str, Any] = field(compare=False)


class JobQueue:
    """Async priority queue that dispatches pipeline runs to execution engines."""

    def __init__(self, metadata_store: MetadataStore, settings: Settings) -> None:
        self._store = metadata_store
        self._settings = settings
        self._queue: asyncio.PriorityQueue[_QueueItem] = asyncio.PriorityQueue()
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_jobs)
        self._workers: list[asyncio.Task] = []
        self._active_jobs: dict[str, asyncio.Task] = {}

    # -- public API ---------------------------------------------------------

    async def start(self, num_workers: int = 3) -> None:
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i))
            self._workers.append(task)
        logger.info("Started %d queue workers", num_workers)

    async def stop(self) -> None:
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("Stopped queue workers")

    async def enqueue(self, run_id: str, pipeline_config: dict[str, Any], priority: int = 0) -> None:
        await self._queue.put(_QueueItem(priority=priority, run_id=run_id, config=pipeline_config))
        await self._store.update_run(run_id, {"status": RunStatus.QUEUED.value})
        logger.info("Enqueued run %s (priority=%d)", run_id, priority)

    # -- properties ---------------------------------------------------------

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def active_job_count(self) -> int:
        return len(self._active_jobs)

    # -- internal -----------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        while True:
            item = await self._queue.get()
            async with self._semaphore:
                task = asyncio.create_task(self._execute(item))
                self._active_jobs[item.run_id] = task
                try:
                    await task
                finally:
                    self._active_jobs.pop(item.run_id, None)
                    self._queue.task_done()

    async def _execute(self, item: _QueueItem) -> None:
        run_id = item.run_id
        config = item.config
        execution_mode = ExecutionMode(config.get("execution_mode", self._settings.default_execution_mode))
        engine = get_execution_engine(execution_mode, self._settings)

        try:
            await self._store.update_run(run_id, {"status": RunStatus.SUBMITTED.value})
            app_id = await engine.submit(run_id, config)
            await self._store.update_run(run_id, {
                "status": RunStatus.RUNNING.value,
                "spark_app_id": app_id,
            })
            logger.info("Run %s submitted (app_id=%s)", run_id, app_id)
        except Exception as exc:
            logger.exception("Run %s failed during submission", run_id)
            await self._store.update_run(run_id, {
                "status": RunStatus.FAILED.value,
                "error_details": str(exc),
            })
