"""Ingestion API routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from fastapi_app.models.ingestion import IngestionRequest, IngestionResponse, JobStatus, JobStatusEnum
from fastapi_app.services.job_manager import job_manager
from fastapi_app.services.k8s_spark_submit import submit_spark_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingestion", tags=["ingestion"])


@router.post("/", response_model=IngestionResponse, status_code=201)
async def submit_job(request: IngestionRequest) -> IngestionResponse:
    """Submit a new Spark ingestion job."""
    response = job_manager.submit(request)

    try:
        job_config = request.model_dump(mode="json")
        await submit_spark_job(response.job_id, job_config)
        job_manager.update_status(response.job_id, JobStatusEnum.RUNNING, "Spark driver pod created")
        response.status = JobStatusEnum.RUNNING
    except Exception:
        logger.exception("K8s submission failed for job %s; job stays in PENDING state", response.job_id)

    return response


@router.get("/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str) -> JobStatus:
    """Get the status of a submitted job."""
    status = job_manager.get_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return status


@router.get("/", response_model=list[JobStatus])
async def list_jobs() -> list[JobStatus]:
    """List all submitted jobs."""
    return job_manager.list_jobs()
