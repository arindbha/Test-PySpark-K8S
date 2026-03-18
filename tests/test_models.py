"""Tests for Pydantic models."""

import pytest
from pydantic import ValidationError

from fastapi_app.models.ingestion import (
    DestinationConfig,
    DestinationType,
    IngestionRequest,
    SourceConfig,
    SourceType,
    TransformStep,
)


def test_source_config():
    cfg = SourceConfig(source_type=SourceType.GCS, uri="gs://bucket/path")
    assert cfg.format == "parquet"
    assert cfg.options == {}


def test_destination_config():
    cfg = DestinationConfig(destination_type=DestinationType.BIGQUERY, uri="project.dataset.table")
    assert cfg.mode == "overwrite"


def test_transform_step():
    step = TransformStep(operation="filter", params={"condition": "col > 0"})
    assert step.operation == "filter"


def test_ingestion_request_minimal():
    req = IngestionRequest(
        job_name="test",
        source={"source_type": "gcs", "uri": "gs://b/p"},
        destination={"destination_type": "gcs", "uri": "gs://b/out"},
    )
    assert req.namespace == "default"
    assert req.transforms == []


def test_ingestion_request_empty_name_fails():
    with pytest.raises(ValidationError):
        IngestionRequest(
            job_name="",
            source={"source_type": "gcs", "uri": "gs://b/p"},
            destination={"destination_type": "gcs", "uri": "gs://b/out"},
        )
