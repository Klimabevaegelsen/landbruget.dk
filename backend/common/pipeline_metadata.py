import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from .data_source_registry import DataSourceInfo, get_source_info


class ProcessingMetadata(BaseModel):
    """Automatically captured processing metadata"""

    # Processing Context (AUTOMATIC)
    processing_timestamp: datetime = Field(default_factory=datetime.now)
    git_commit_hash: str | None = Field(None, description="Git commit when processed")
    git_branch: str | None = Field(None)
    git_tag: str | None = Field(None)

    # Processing Stats (AUTOMATIC)
    record_count: int | None = Field(None)
    processing_duration_seconds: float | None = Field(None)
    file_size_bytes: int | None = Field(None)

    # Environment (AUTOMATIC)
    environment: str = Field(..., description="local/dev/prod")
    runner: str = Field(..., description="github-actions/local/vm")


class DatasetMetadata(BaseModel):
    """Complete metadata combining manual config + automatic processing info"""

    # From registry (MANUAL CONFIGURATION)
    source_info: DataSourceInfo

    # Processing info (AUTOMATIC)
    processing: ProcessingMetadata

    # For combined datasets (MANUAL SPECIFICATION)
    source_datasets: list[str] | None = Field(
        None, description="If this combines other datasets"
    )

    def to_frontend_display(self) -> dict[str, Any]:
        """Format for frontend consumption"""
        return {
            "source": self.source_info.source_authority,
            "date_of_acquisition": self.source_info.data_acquisition_date.isoformat()
            if self.source_info.data_acquisition_date
            else None,
            "date_of_processing": self.processing.processing_timestamp.isoformat(),
            "code_version": self.processing.git_commit_hash,
            "documentation": self.source_info.documentation_url,
            "display_name": self.source_info.display_name,
            "display_description": self.source_info.display_description,
            "update_frequency": self.source_info.update_frequency,
            "record_count": self.processing.record_count,
            "source_datasets": self.source_datasets,  # For combined datasets
            "data_acquisition_method": self.source_info.data_acquisition_method,
            "processing_duration": self.processing.processing_duration_seconds,
            "environment": self.processing.environment,
            "file_size_bytes": self.processing.file_size_bytes,
        }


class MetadataManager:
    """Manages metadata creation and storage"""

    def __init__(self):
        self.git_context = self._get_git_context()
        self.environment = self._detect_environment()

    def _get_git_context(self) -> dict[str, str | None]:
        """Get current git context (AUTOMATIC)"""
        try:
            commit_hash = (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode()
                .strip()
            )

            try:
                branch = (
                    subprocess.check_output(
                        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                        stderr=subprocess.DEVNULL,
                    )
                    .decode()
                    .strip()
                )
            except Exception:
                branch = None

            try:
                tag = (
                    subprocess.check_output(
                        ["git", "describe", "--exact-match", "--tags", "HEAD"],
                        stderr=subprocess.DEVNULL,
                    )
                    .decode()
                    .strip()
                )
            except Exception:
                tag = None

            return {"commit_hash": commit_hash, "branch": branch, "tag": tag}
        except Exception:
            return {"commit_hash": None, "branch": None, "tag": None}

    def _detect_environment(self) -> str:
        """Detect execution environment (AUTOMATIC)"""
        if os.getenv("GITHUB_ACTIONS"):
            return "prod"
        if os.getenv("ENVIRONMENT") == "dev":
            return "dev"
        return "local"

    def create_metadata(
        self,
        source_key: str,
        record_count: int | None = None,
        processing_duration: float | None = None,
        file_size_bytes: int | None = None,
        source_datasets: list[str] | None = None,
    ) -> DatasetMetadata:
        """Create complete metadata for a dataset"""

        # Get source info from registry (VALIDATES THAT YOU'VE CONFIGURED IT)
        source_info = get_source_info(source_key)

        # Create automatic processing metadata
        processing = ProcessingMetadata(
            git_commit_hash=self.git_context["commit_hash"],
            git_branch=self.git_context["branch"],
            git_tag=self.git_context["tag"],
            record_count=record_count,
            processing_duration_seconds=processing_duration,
            file_size_bytes=file_size_bytes,
            environment=self.environment,
            runner="github-actions" if os.getenv("GITHUB_ACTIONS") else "local",
        )

        return DatasetMetadata(
            source_info=source_info,
            processing=processing,
            source_datasets=source_datasets,
        )

    def save_metadata(self, metadata: DatasetMetadata, data_file_path: Path) -> Path:
        """Save metadata alongside data file"""
        # Create metadata filename
        metadata_path = data_file_path.parent / f"{data_file_path.stem}_metadata.json"

        # Save metadata
        with metadata_path.open("w", encoding="utf-8") as f:
            json.dump(metadata.model_dump(mode="json"), f, indent=2, default=str)

        return metadata_path

    def load_metadata(self, metadata_path: Path) -> DatasetMetadata:
        """Load metadata from file"""
        with metadata_path.open(encoding="utf-8") as f:
            data = json.load(f)
        return DatasetMetadata.model_validate(data)
