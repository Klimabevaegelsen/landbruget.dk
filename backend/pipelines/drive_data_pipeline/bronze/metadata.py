"""Metadata management for Bronze layer."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from ..utils.helpers import calculate_file_checksum, generate_timestamp
from ..utils.logging import get_logger

# Get logger
logger = get_logger()


class FileMetadata(BaseModel):
    """Metadata for a file in the Bronze layer."""

    timestamp: str = Field(..., description="ISO format timestamp of download")
    source_url: str = Field(..., description="URL of the Google Drive file")
    file_id: str = Field(..., description="Google Drive file ID")
    original_filename: str = Field(..., description="Name of the file in Google Drive")
    original_subfolder: str = Field(..., description="Name of the parent folder in Google Drive")
    checksum: str = Field(..., description="Hash of the file contents for deduplication")
    mime_type: str = Field(..., description="MIME type of the file")
    file_size: int = Field(..., description="Size of the file in bytes")
    record_count: Optional[int] = Field(None, description="Number of records (where applicable)")
    modified_time: str = Field(..., description="Last modified time in ISO format")
    drive_path: str = Field(..., description="Full path within Google Drive")


class MetadataManager:
    """Manager for file metadata."""

    def __init__(self, base_path: Path):
        """Initialize the metadata manager.

        Args:
            base_path: Base path for storing metadata
        """
        self.base_path = base_path
        logger.info(f"Initialized metadata manager with base path: {base_path}")

    def generate_metadata(
        self,
        file_path: Path,
        file_id: str,
        original_filename: str,
        original_subfolder: str,
        mime_type: str,
        file_size: int,
        modified_time: datetime.datetime,
        drive_path: str,
        record_count: Optional[int] = None,
    ) -> FileMetadata:
        """Generate metadata for a file.

        Args:
            file_path: Path to the file to generate metadata for
            file_id: Google Drive file ID
            original_filename: Name of the file in Google Drive
            original_subfolder: Name of the parent folder in Google Drive
            mime_type: MIME type of the file
            file_size: Size of the file in bytes
            modified_time: Last modified time
            drive_path: Full path within Google Drive
            record_count: Number of records (where applicable)

        Returns:
            FileMetadata object
        """
        logger.debug(f"Generating metadata for file: {file_path}")

        # Calculate checksum
        checksum = calculate_file_checksum(file_path)

        # Generate metadata
        metadata = FileMetadata(
            timestamp=datetime.datetime.now().isoformat(),
            source_url=f"https://drive.google.com/file/d/{file_id}/view",
            file_id=file_id,
            original_filename=original_filename,
            original_subfolder=original_subfolder,
            checksum=checksum,
            mime_type=mime_type,
            file_size=file_size,
            record_count=record_count,
            modified_time=modified_time.isoformat(),
            drive_path=drive_path,
        )

        logger.debug(f"Generated metadata for file {file_path}")
        return metadata

    def save_metadata(self, metadata: FileMetadata, target_path: Path) -> Path:
        """Save metadata to a JSON file.

        Args:
            metadata: FileMetadata object
            target_path: Path to the file the metadata is for

        Returns:
            Path to the saved metadata file
        """
        # Create metadata file path with same name as the file but with .json extension
        metadata_path = target_path.with_suffix(".metadata.json")
        
        # Save metadata as JSON
        metadata_dict = metadata.model_dump()
        with open(metadata_path, "w", encoding="utf-8") as f:
            import json
            json.dump(metadata_dict, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved metadata to {metadata_path}")
        return metadata_path 