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
        return PipelineConfig.model_validate(raw)

    def merge_overrides(self, base: PipelineConfig, overrides: dict) -> PipelineConfig:
        """Deep-merge *overrides* on top of *base* and re-validate."""
        merged = _deep_merge(base.model_dump(), overrides)
        return PipelineConfig.model_validate(merged)

    def list_pipelines(self) -> list[str]:
        """Return available pipeline names (YAML stems) in the config directory."""
        if not self._dir.exists():
            return []
        return sorted(p.stem for p in self._dir.glob("*.yaml"))


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
