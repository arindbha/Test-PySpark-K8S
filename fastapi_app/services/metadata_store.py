"""Abstract metadata store and filesystem JSON implementation."""

from __future__ import annotations

import json
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class MetadataStore(ABC):
    """Abstract base for pipeline run / batch metadata persistence."""

    @abstractmethod
    async def initialize(self) -> None: ...

    @abstractmethod
    async def insert_run(self, run_data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def update_run(self, run_id: str, updates: dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_run(self, run_id: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def list_runs(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def insert_batch(self, batch_data: dict[str, Any]) -> None: ...

    @abstractmethod
    async def get_batch(self, batch_id: str) -> dict[str, Any] | None: ...


class FileSystemMetadataStore(MetadataStore):
    """Stores run/batch metadata as JSON files on disk."""

    def __init__(self, data_dir: str) -> None:
        self._data_dir = Path(data_dir)
        self._runs_dir = self._data_dir / "runs"
        self._batches_dir = self._data_dir / "batches"
        self._lock = threading.Lock()

    async def initialize(self) -> None:
        self._runs_dir.mkdir(parents=True, exist_ok=True)
        self._batches_dir.mkdir(parents=True, exist_ok=True)

    # -- runs ---------------------------------------------------------------

    async def insert_run(self, run_data: dict[str, Any]) -> None:
        run_id = run_data["run_id"]
        run_data.setdefault("updated_at", _now_iso())
        self._write_json(self._runs_dir / f"{run_id}.json", run_data)

    async def update_run(self, run_id: str, updates: dict[str, Any]) -> None:
        path = self._runs_dir / f"{run_id}.json"
        with self._lock:
            data = self._read_json(path)
            if data is None:
                return
            data.update(updates)
            data["updated_at"] = _now_iso()
            self._write_json_unlocked(path, data)

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._read_json(self._runs_dir / f"{run_id}.json")

    async def list_runs(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        filters = filters or {}
        runs: list[dict[str, Any]] = []
        if not self._runs_dir.exists():
            return runs
        for path in sorted(self._runs_dir.glob("*.json"), reverse=True):
            data = self._read_json(path)
            if data is None:
                continue
            if not _matches_filters(data, filters):
                continue
            runs.append(data)

        # Pagination
        offset = int(filters.get("offset", 0))
        limit = int(filters.get("limit", 100))
        return runs[offset : offset + limit]

    # -- batches ------------------------------------------------------------

    async def insert_batch(self, batch_data: dict[str, Any]) -> None:
        batch_id = batch_data["batch_id"]
        self._write_json(self._batches_dir / f"{batch_id}.json", batch_data)

    async def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self._read_json(self._batches_dir / f"{batch_id}.json")

    # -- helpers ------------------------------------------------------------

    def _read_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with open(path) as fh:
            return json.load(fh)

    def _write_json(self, path: Path, data: dict[str, Any]) -> None:
        with self._lock:
            self._write_json_unlocked(path, data)

    def _write_json_unlocked(self, path: Path, data: dict[str, Any]) -> None:
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2, default=str)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _matches_filters(data: dict, filters: dict) -> bool:
    """Return True if *data* matches all non-pagination filter fields."""
    if "status" in filters and data.get("status") != filters["status"]:
        return False
    if "pipeline_name" in filters and data.get("pipeline_name") != filters["pipeline_name"]:
        return False
    if "since" in filters:
        created = data.get("start_time") or data.get("updated_at", "")
        if created < filters["since"]:
            return False
    if "until" in filters:
        created = data.get("start_time") or data.get("updated_at", "")
        if created > filters["until"]:
            return False
    return True
