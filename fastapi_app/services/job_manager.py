"""In-memory job manager for tracking Spark ingestion jobs."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi_app.models.ingestion import (
    IngestionRequest,
    IngestionResponse,
    JobStatus,
    JobStatusEnum,
)


class JobManager:
    """Tracks submitted ingestion jobs. Uses an in-memory store (swap for a real DB in production)."""

    def __init__(self) -> None:
        self._jobs: dict[str, JobStatus] = {}

    def submit(self, request: IngestionRequest) -> IngestionResponse:
        job_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc)
        status = JobStatus(
            job_id=job_id,
            job_name=request.job_name,
            status=JobStatusEnum.PENDING,
            created_at=now,
            updated_at=now,
        )
        self._jobs[job_id] = status
        return IngestionResponse(
            job_id=job_id,
            job_name=request.job_name,
            status=JobStatusEnum.PENDING,
        )

    def get_status(self, job_id: str) -> JobStatus | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobStatus]:
        return list(self._jobs.values())

    def update_status(self, job_id: str, status: JobStatusEnum, message: str = "") -> JobStatus | None:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        job.status = status
        job.message = message
        job.updated_at = datetime.now(timezone.utc)
        return job


job_manager = JobManager()
