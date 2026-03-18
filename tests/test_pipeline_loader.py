"""Tests for PipelineLoader."""

import pytest
import yaml

from fastapi_app.models.pipeline import PipelineConfig
from fastapi_app.services.pipeline_loader import PipelineLoader


def test_load_pipeline(tmp_pipelines_dir):
    loader = PipelineLoader(tmp_pipelines_dir)
    config = loader.load_pipeline("test_pipeline")
    assert isinstance(config, PipelineConfig)
    assert config.pipeline_name == "test_pipeline"
    assert len(config.datasets) == 1
    assert config.datasets[0].source.name == "test_source"


def test_load_pipeline_not_found(tmp_pipelines_dir):
    loader = PipelineLoader(tmp_pipelines_dir)
    with pytest.raises(FileNotFoundError):
        loader.load_pipeline("nonexistent")


def test_merge_overrides(tmp_pipelines_dir):
    loader = PipelineLoader(tmp_pipelines_dir)
    base = loader.load_pipeline("test_pipeline")
    merged = loader.merge_overrides(base, {"spark_config": {"driver_memory": "8g"}})
    assert merged.spark_config.driver_memory == "8g"
    # Other fields preserved
    assert merged.pipeline_name == "test_pipeline"
    assert len(merged.datasets) == 1


def test_list_pipelines(tmp_pipelines_dir):
    loader = PipelineLoader(tmp_pipelines_dir)
    names = loader.list_pipelines()
    assert "test_pipeline" in names


def test_list_pipelines_empty_dir(tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    loader = PipelineLoader(str(empty_dir))
    assert loader.list_pipelines() == []


def test_list_pipelines_nonexistent_dir(tmp_path):
    loader = PipelineLoader(str(tmp_path / "nope"))
    assert loader.list_pipelines() == []


def test_load_invalid_yaml(tmp_path):
    pipelines_dir = tmp_path / "pipes"
    pipelines_dir.mkdir()
    (pipelines_dir / "bad.yaml").write_text("pipeline_name: bad\n")  # missing required 'datasets'
    loader = PipelineLoader(str(pipelines_dir))
    with pytest.raises(Exception):
        loader.load_pipeline("bad")
