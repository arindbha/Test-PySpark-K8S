"""Pydantic models for the ingestion API."""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field


class SourceType(str, enum.Enum):
    GCS = "gcs"
    BIGQUERY = "bigquery"


class DestinationType(str, enum.Enum):
    GCS = "gcs"
    BIGQUERY = "bigquery"


class SourceConfig(BaseModel):
    source_type: SourceType
    uri: str = Field(..., description="Source URI, e.g. gs://bucket/path or project.dataset.table")
    format: str = Field(default="parquet", description="Data format (parquet, csv, json)")
    options: dict[str, str] = Field(default_factory=dict)


class DestinationConfig(BaseModel):
    destination_type: DestinationType
    uri: str = Field(..., description="Destination URI")
    format: str = Field(default="parquet")
    mode: str = Field(default="overwrite", description="Write mode: overwrite, append, ignore, error")
    options: dict[str, str] = Field(default_factory=dict)


class TransformStep(BaseModel):
    operation: str = Field(..., description="Transform operation: filter, select, rename, sql")
    params: dict[str, str] = Field(default_factory=dict)


class IngestionRequest(BaseModel):
    job_name: str = Field(..., min_length=1, max_length=128)
    source: SourceConfig
    destination: DestinationConfig
    transforms: list[TransformStep] = Field(default_factory=list)
    spark_config: dict[str, str] = Field(default_factory=dict, description="Extra Spark configuration")
    namespace: str = Field(default="default", description="Kubernetes namespace for the Spark job")


class JobStatusEnum(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStatus(BaseModel):
    job_id: str
    job_name: str
    status: JobStatusEnum
    created_at: datetime
    updated_at: datetime
    message: str = ""


class IngestionResponse(BaseModel):
    job_id: str
    job_name: str
    status: JobStatusEnum
    message: str = "Job submitted successfully"


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str = "0.1.0"
