"""Health and readiness endpoints — /v1/health."""

from fastapi import APIRouter, Request

from fastapi_app.models.pipeline import HealthResponse

router = APIRouter(prefix="/v1", tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    queue = getattr(request.app.state, "job_queue", None)
    return HealthResponse(
        status="healthy",
        queue_depth=queue.queue_depth if queue else 0,
        active_jobs=queue.active_job_count if queue else 0,
    )
