"""Load and validate pipeline configurations from YAML files."""

from __future__ import annotations

import copy
from pathlib import Path

import yaml

from fastapi_app.models.pipeline import PipelineConfig


class PipelineLoader:
    def __init__(self, pipelines_dir: str) -> None:
        self._dir = Path(pipelines_dir)

    def load_pipeline(self, pipeline_name: str) -> PipelineConfig:
        """Load a pipeline config from ``{pipelines_dir}/{pipeline_name}.yaml``."""
        path = self._dir / f"{pipeline_name}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Pipeline config not found: {path}")
        with open(path) as fh:
            raw = yaml.safe_load(fh)
        resolved = copy.deepcopy(raw)
        _resolve_connections(resolved)
        config = PipelineConfig.model_validate(resolved)
        # Keep the *unresolved* raw dict so merge_overrides can re-resolve
        # after applying overrides on the original structure.
        config._raw = raw  # type: ignore[attr-defined]
        return config

    def merge_overrides(self, base: PipelineConfig, overrides: dict) -> PipelineConfig:
        """Deep-merge *overrides* on top of *base* and re-validate."""
        # Merge onto the original unresolved YAML when available so that
        # connection overrides propagate correctly to all referencing datasets.
        base_raw = getattr(base, "_raw", None) or base.model_dump()
        merged = _deep_merge(base_raw, overrides)
        _resolve_connections(merged)
        config = PipelineConfig.model_validate(merged)
        config._raw = merged  # type: ignore[attr-defined]
        return config

    def list_pipelines(self) -> list[str]:
        """Return available pipeline names (YAML stems) in the config directory."""
        if not self._dir.exists():
            return []
        return sorted(p.stem for p in self._dir.glob("*.yaml"))


def _resolve_connections(raw: dict) -> None:
    """Resolve ``connection`` references in dataset sources, mutating *raw* in place.

    For each dataset source that declares a ``connection`` name:
    * Look up the connection in the top-level ``connections`` map.
    * Inherit ``type`` from the connection when the source omits it.
    * Merge connection ``config`` as the base, with dataset-level ``config``
      overriding on conflict (dataset wins).
    """
    connections = raw.get("connections") or {}
    for ds in raw.get("datasets") or []:
        source = ds.get("source") or {}
        conn_name = source.get("connection")
        if not conn_name:
            continue
        if conn_name not in connections:
            raise ValueError(
                f"Source '{source.get('name', '?')}' references undefined "
                f"connection '{conn_name}'"
            )
        conn = connections[conn_name]
        # Inherit type from connection when source doesn't set it explicitly.
        if not source.get("type"):
            source["type"] = conn["type"] if isinstance(conn, dict) else conn.type
        # Merge config: connection config as base, dataset config wins.
        conn_config = conn["config"] if isinstance(conn, dict) else conn.config
        merged_config = {**conn_config, **source.get("config", {})}
        source["config"] = merged_config


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
