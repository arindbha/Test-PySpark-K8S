"""Pydantic models for pipeline configuration and API request/response."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from fastapi_app.models.enums import (
    DeployMode,
    DestinationType,
    ExecutionMode,
    RunStatus,
    SourceType,
    TransformType,
)


# ---------------------------------------------------------------------------
# Pipeline configuration models
# ---------------------------------------------------------------------------


class SourceConfig(BaseModel):
    name: str = Field(..., description="Unique name for tracking this source")
    type: SourceType
    config: dict = Field(default_factory=dict)


class DestinationConfig(BaseModel):
    type: DestinationType
    config: dict = Field(default_factory=dict)


class TransformStep(BaseModel):
    type: TransformType
    config: dict = Field(default_factory=dict)


class DatasetConfig(BaseModel):
    source: SourceConfig
    destination: DestinationConfig
    transforms: list[TransformStep] = Field(default_factory=list)


class SparkConfig(BaseModel):
    driver_memory: str = "1g"
    executor_memory: str = "2g"
    executor_instances: int = 2
    deploy_mode: DeployMode = DeployMode.CLUSTER
    extra_conf: dict[str, str] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    pipeline_name: str = Field(..., min_length=1, max_length=128)
    datasets: list[DatasetConfig] = Field(..., min_length=1)
    spark_config: SparkConfig = Field(default_factory=SparkConfig)
    execution_mode: ExecutionMode = ExecutionMode.LOCAL


# ---------------------------------------------------------------------------
# API request models
# ---------------------------------------------------------------------------


class PipelineRunRequest(BaseModel):
    pipeline_name: str = Field(..., min_length=1)
    override_params: dict | None = None
    priority: int = Field(default=0, ge=0)
    execution_mode_override: ExecutionMode | None = None


class BatchPipelineItem(BaseModel):
    name: str
    overrides: dict | None = None


class BatchRunRequest(BaseModel):
    pipelines: list[BatchPipelineItem] = Field(..., min_length=1)
    max_parallel: int | None = None


class RestartRequest(BaseModel):
    restart_from_stage: str | None = None


# ---------------------------------------------------------------------------
# API response models
# ---------------------------------------------------------------------------


class PipelineRunResponse(BaseModel):
    run_id: str
    status: RunStatus
    message: str = "Job queued successfully"


class BatchRunResponse(BaseModel):
    batch_id: str
    run_ids: list[str]


class DatasetStatus(BaseModel):
    name: str
    status: str = "pending"
    records_read: int = 0
    records_written: int = 0
    error_message: str = ""
    start_time: datetime | None = None
    end_time: datetime | None = None


class RunStatusResponse(BaseModel):
    run_id: str
    pipeline_name: str
    status: RunStatus
    datasets: list[DatasetStatus] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    error_details: str | None = None
    spark_app_id: str | None = None
    k8s_driver_pod: str | None = None
    parent_run_id: str | None = None
    execution_mode: ExecutionMode | None = None


class HealthResponse(BaseModel):
    status: str = "healthy"
    queue_depth: int = 0
    active_jobs: int = 0
