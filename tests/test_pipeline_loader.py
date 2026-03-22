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


# ---------------------------------------------------------------------------
# Connection resolution tests
# ---------------------------------------------------------------------------

def _write_pipeline(tmp_path, name, config):
    """Helper to write a pipeline YAML and return a loader."""
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir(exist_ok=True)
    with open(pipelines_dir / f"{name}.yaml", "w") as f:
        yaml.dump(config, f)
    return PipelineLoader(str(pipelines_dir))


def test_load_pipeline_with_connections(tmp_path):
    loader = _write_pipeline(tmp_path, "conn_test", {
        "pipeline_name": "conn_test",
        "connections": {
            "my_db": {
                "type": "jdbc",
                "config": {
                    "url": "jdbc:postgresql://host:5432/db",
                    "driver": "org.postgresql.Driver",
                    "user": "admin",
                },
            },
        },
        "datasets": [
            {
                "source": {
                    "name": "tbl_a",
                    "connection": "my_db",
                    "config": {"table": "table_a"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out/a"}},
            },
            {
                "source": {
                    "name": "tbl_b",
                    "connection": "my_db",
                    "config": {"table": "table_b"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out/b"}},
            },
        ],
    })
    config = loader.load_pipeline("conn_test")
    # type inherited from connection
    assert config.datasets[0].source.type.value == "jdbc"
    assert config.datasets[1].source.type.value == "jdbc"
    # connection config merged into source config
    assert config.datasets[0].source.config["url"] == "jdbc:postgresql://host:5432/db"
    assert config.datasets[0].source.config["table"] == "table_a"
    assert config.datasets[1].source.config["table"] == "table_b"
    # user from connection present in both
    assert config.datasets[0].source.config["user"] == "admin"
    assert config.datasets[1].source.config["user"] == "admin"


def test_connection_dataset_config_overrides(tmp_path):
    """Dataset-level config keys take precedence over connection-level keys."""
    loader = _write_pipeline(tmp_path, "override_test", {
        "pipeline_name": "override_test",
        "connections": {
            "db": {
                "type": "jdbc",
                "config": {"url": "jdbc:postgresql://host:5432/db", "user": "default_user"},
            },
        },
        "datasets": [
            {
                "source": {
                    "name": "ds1",
                    "connection": "db",
                    "config": {"table": "t1", "user": "override_user"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out"}},
            },
        ],
    })
    config = loader.load_pipeline("override_test")
    assert config.datasets[0].source.config["user"] == "override_user"
    assert config.datasets[0].source.config["url"] == "jdbc:postgresql://host:5432/db"


def test_connection_ref_not_found(tmp_path):
    loader = _write_pipeline(tmp_path, "bad_ref", {
        "pipeline_name": "bad_ref",
        "datasets": [
            {
                "source": {
                    "name": "ds1",
                    "connection": "nonexistent",
                    "config": {"table": "t1"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out"}},
            },
        ],
    })
    with pytest.raises(ValueError, match="undefined connection 'nonexistent'"):
        loader.load_pipeline("bad_ref")


def test_merge_overrides_with_connections(tmp_path):
    """Runtime overrides on connection config propagate to all datasets."""
    loader = _write_pipeline(tmp_path, "merge_conn", {
        "pipeline_name": "merge_conn",
        "connections": {
            "db": {
                "type": "jdbc",
                "config": {"url": "jdbc:postgresql://dev:5432/db", "user": "dev"},
            },
        },
        "datasets": [
            {
                "source": {
                    "name": "ds1",
                    "connection": "db",
                    "config": {"table": "t1"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out"}},
            },
        ],
    })
    base = loader.load_pipeline("merge_conn")
    merged = loader.merge_overrides(base, {
        "connections": {"db": {"config": {"url": "jdbc:postgresql://prod:5432/db", "user": "prod"}}},
    })
    assert merged.datasets[0].source.config["url"] == "jdbc:postgresql://prod:5432/db"
    assert merged.datasets[0].source.config["user"] == "prod"
    assert merged.datasets[0].source.config["table"] == "t1"


def test_explicit_type_not_overridden_by_connection(tmp_path):
    """If source sets type explicitly, it is kept even with a connection ref."""
    loader = _write_pipeline(tmp_path, "explicit_type", {
        "pipeline_name": "explicit_type",
        "connections": {
            "db": {
                "type": "jdbc",
                "config": {"url": "jdbc:postgresql://host:5432/db"},
            },
        },
        "datasets": [
            {
                "source": {
                    "name": "ds1",
                    "type": "jdbc",
                    "connection": "db",
                    "config": {"table": "t1"},
                },
                "destination": {"type": "gcs", "config": {"path": "/out"}},
            },
        ],
    })
    config = loader.load_pipeline("explicit_type")
    assert config.datasets[0].source.type.value == "jdbc"
