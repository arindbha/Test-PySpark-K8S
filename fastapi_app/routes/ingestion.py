"""Ingestion API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from fastapi_app.models.ingestion import IngestionRequest, IngestionResponse, JobStatus
from fastapi_app.services.job_manager import job_manager

router = APIRouter(prefix="/api/v1/ingestion", tags=["ingestion"])


@router.post("/", response_model=IngestionResponse, status_code=201)
async def submit_job(request: IngestionRequest) -> IngestionResponse:
    """Submit a new Spark ingestion job."""
    return job_manager.submit(request)


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
