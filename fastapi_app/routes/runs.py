"""Run management endpoints — /v1/runs/*."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from fastapi_app.models.enums import ExecutionMode, RunStatus
from fastapi_app.models.pipeline import RunStatusResponse
from fastapi_app.services.execution_engine import get_execution_engine

router = APIRouter(prefix="/v1/runs", tags=["runs"])


@router.get("/", response_model=list[RunStatusResponse])
async def list_runs(
    request: Request,
    status: str | None = Query(None),
    pipeline_name: str | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    since: str | None = Query(None),
    until: str | None = Query(None),
) -> list[RunStatusResponse]:
    """List runs with optional filters and pagination."""
    metadata = request.app.state.metadata_store
    filters: dict = {"limit": limit, "offset": offset}
    if status:
        filters["status"] = status
    if pipeline_name:
        filters["pipeline_name"] = pipeline_name
    if since:
        filters["since"] = since
    if until:
        filters["until"] = until

    runs = await metadata.list_runs(filters)
    return [
        RunStatusResponse(
            run_id=r["run_id"],
            pipeline_name=r.get("pipeline_name", ""),
            status=r.get("status", "pending"),
            datasets=r.get("datasets_status", []),
            start_time=r.get("start_time"),
            end_time=r.get("end_time"),
            error_details=r.get("error_details"),
            spark_app_id=r.get("spark_app_id"),
            k8s_driver_pod=r.get("k8s_driver_pod"),
            parent_run_id=r.get("parent_run_id"),
            execution_mode=r.get("execution_mode"),
        )
        for r in runs
    ]


@router.delete("/{run_id}/cancel", status_code=200)
async def cancel_run(run_id: str, request: Request) -> dict:
    """Cancel a running pipeline."""
    metadata = request.app.state.metadata_store
    settings = request.app.state.settings

    run = await metadata.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    current_status = run.get("status")
    if current_status in (RunStatus.COMPLETED.value, RunStatus.CANCELLED.value, RunStatus.FAILED.value):
        raise HTTPException(status_code=409, detail=f"Run {run_id} is already {current_status}")

    app_id = run.get("spark_app_id", "")
    execution_mode = ExecutionMode(run.get("execution_mode", settings.default_execution_mode))
    engine = get_execution_engine(execution_mode, settings)

    if app_id:
        await engine.cancel(run_id, app_id)

    await metadata.update_run(run_id, {"status": RunStatus.CANCELLED.value})
    return {"run_id": run_id, "status": RunStatus.CANCELLED.value, "message": "Run cancelled"}
