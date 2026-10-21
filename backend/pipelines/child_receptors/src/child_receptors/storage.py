"""Thin local/R2 file wrapper around the shared StorageAccess filesystem."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from child_receptors.config import BRONZE_PREFIX, SILVER_PREFIX


class PipelineStorage:
    """Read and write byte-preserving objects using the common R2 filesystem."""

    def __init__(self, *, storage_access: Any = None, bucket: str | None = None) -> None:
        self.bucket = bucket or os.getenv("R2_BUCKET") or "landbruget-data"
        if storage_access is None:
            from common.storage import StorageAccess

            storage_access = StorageAccess()
        self.access = storage_access
        self.fs = storage_access.fs

    def _path(self, relative_path: str) -> str:
        return f"{self.bucket}/{relative_path.lstrip('/')}"

    def upload_bytes(self, relative_path: str, data: bytes) -> None:
        with self.fs.open(self._path(relative_path), "wb") as destination:
            destination.write(data)

    def download_bytes(self, relative_path: str) -> bytes:
        with self.fs.open(self._path(relative_path), "rb") as source:
            return source.read()

    def file_exists(self, relative_path: str) -> bool:
        return bool(self.fs.exists(self._path(relative_path)))

    def list_directories(self, relative_path: str) -> list[str]:
        path = self._path(relative_path)
        try:
            entries = self.fs.ls(path, detail=False)
        except FileNotFoundError:
            return []
        return sorted(Path(str(entry).rstrip("/")).name for entry in entries if self.fs.isdir(entry))

    def enforce_retention(self, *, keep: int = 3) -> list[str]:
        return self.access.enforce_retention(self._path(SILVER_PREFIX), keep=keep)


def source_run_prefix(source: str, timestamp: str) -> str:
    return f"{BRONZE_PREFIX}/{source}/{timestamp}"


def silver_run_prefix(timestamp: str) -> str:
    return f"{SILVER_PREFIX}/{timestamp}"


def encode_manifest(manifest: dict[str, Any]) -> bytes:
    return json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
