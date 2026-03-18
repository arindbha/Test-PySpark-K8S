"""Pipeline lifecycle endpoints — /v1/pipelines/*."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request

from fastapi_app.models.enums import RunStatus
from fastapi_app.models.pipeline import (
    BatchRunRequest,
    BatchRunResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    RestartRequest,
    RunStatusResponse,
)

router = APIRouter(prefix="/v1/pipelines", tags=["pipelines"])


@router.post("/run", response_model=PipelineRunResponse, status_code=202)
async def run_pipeline(body: PipelineRunRequest, request: Request) -> PipelineRunResponse:
    """Trigger a single pipeline run (async)."""
    loader = request.app.state.pipeline_loader
    metadata = request.app.state.metadata_store
    queue = request.app.state.job_queue

    # Load & merge config
    try:
        config = loader.load_pipeline(body.pipeline_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Pipeline '{body.pipeline_name}' not found")

    if body.override_params:
        config = loader.merge_overrides(config, body.override_params)

    if body.execution_mode_override:
        config = config.model_copy(update={"execution_mode": body.execution_mode_override})

    run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()
    run_data = {
        "run_id": run_id,
        "pipeline_name": body.pipeline_name,
        "status": RunStatus.PENDING.value,
        "config": config.model_dump(mode="json"),
        "execution_mode": config.execution_mode.value,
        "priority": body.priority,
        "start_time": now,
        "datasets_status": [{"name": ds.source.name, "status": "pending"} for ds in config.datasets],
    }
    await metadata.insert_run(run_data)
    await queue.enqueue(run_id, config.model_dump(mode="json"), priority=body.priority)

    return PipelineRunResponse(run_id=run_id, status=RunStatus.QUEUED, message="Job queued successfully")


@router.post("/run-batch", response_model=BatchRunResponse, status_code=202)
async def run_batch(body: BatchRunRequest, request: Request) -> BatchRunResponse:
    """Trigger multiple pipelines in parallel."""
    loader = request.app.state.pipeline_loader
    metadata = request.app.state.metadata_store
    queue = request.app.state.job_queue

    batch_id = uuid.uuid4().hex[:12]
    run_ids: list[str] = []

    for item in body.pipelines:
        try:
            config = loader.load_pipeline(item.name)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Pipeline '{item.name}' not found")

        if item.overrides:
            config = loader.merge_overrides(config, item.overrides)

        run_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        run_data = {
            "run_id": run_id,
            "pipeline_name": item.name,
            "status": RunStatus.PENDING.value,
            "config": config.model_dump(mode="json"),
            "execution_mode": config.execution_mode.value,
            "batch_id": batch_id,
            "start_time": now,
            "datasets_status": [{"name": ds.source.name, "status": "pending"} for ds in config.datasets],
        }
        await metadata.insert_run(run_data)
        await queue.enqueue(run_id, config.model_dump(mode="json"))
        run_ids.append(run_id)

    await metadata.insert_batch({"batch_id": batch_id, "run_ids": run_ids, "created_at": datetime.now(timezone.utc).isoformat()})
    return BatchRunResponse(batch_id=batch_id, run_ids=run_ids)


@router.get("/{run_id}/status", response_model=RunStatusResponse)
async def get_run_status(run_id: str, request: Request) -> RunStatusResponse:
    """Get detailed status of a pipeline run."""
    metadata = request.app.state.metadata_store
    run = await metadata.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return RunStatusResponse(**{
        "run_id": run["run_id"],
        "pipeline_name": run.get("pipeline_name", ""),
        "status": run.get("status", "pending"),
        "datasets": run.get("datasets_status", []),
        "start_time": run.get("start_time"),
        "end_time": run.get("end_time"),
        "error_details": run.get("error_details"),
        "spark_app_id": run.get("spark_app_id"),
        "k8s_driver_pod": run.get("k8s_driver_pod"),
        "parent_run_id": run.get("parent_run_id"),
        "execution_mode": run.get("execution_mode"),
    })


@router.post("/{run_id}/restart", response_model=PipelineRunResponse, status_code=202)
async def restart_pipeline(run_id: str, body: RestartRequest, request: Request) -> PipelineRunResponse:
    """Restart a failed run from last checkpoint."""
    metadata = request.app.state.metadata_store
    queue = request.app.state.job_queue

    original = await metadata.get_run(run_id)
    if original is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    config = original.get("config", {})
    checkpoint = original.get("checkpoint_data", {})
    if body.restart_from_stage:
        checkpoint["restart_from_stage"] = body.restart_from_stage

    new_run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat()
    run_data = {
        "run_id": new_run_id,
        "pipeline_name": original.get("pipeline_name", ""),
        "status": RunStatus.PENDING.value,
        "config": config,
        "execution_mode": original.get("execution_mode", "local"),
        "parent_run_id": run_id,
        "checkpoint_data": checkpoint,
        "start_time": now,
        "datasets_status": original.get("datasets_status", []),
    }
    await metadata.insert_run(run_data)
    await queue.enqueue(new_run_id, config)

    return PipelineRunResponse(run_id=new_run_id, status=RunStatus.QUEUED, message="Restart queued successfully")
