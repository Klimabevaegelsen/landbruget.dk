"""Metadata management for Bronze layer."""

import datetime
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, validator

from ..utils.error_handling import StorageError
from ..utils.helpers import calculate_content_checksum, calculate_file_checksum
from ..utils.logging import get_logger

# Get logger
logger = get_logger()


class FileMetadata(BaseModel):
    """Metadata for a file in the Bronze layer."""

    # Basic metadata
    timestamp: str = Field(..., description="ISO format timestamp of download")
    source_url: str = Field(..., description="URL of the Google Drive file")
    file_id: str = Field(..., description="Google Drive file ID")
    original_filename: str = Field(..., description="Name of the file in Google Drive")
    original_subfolder: str = Field(..., description="Name of the parent folder in Google Drive")

    # File integrity
    checksum: str = Field(..., description="Hash of the file contents for deduplication")
    checksum_algorithm: str = Field("sha256", description="Algorithm used for checksum")

    # File details
    mime_type: str = Field(..., description="MIME type of the file")
    file_size: int = Field(..., description="Size of the file in bytes")
    file_extension: str = Field(..., description="File extension")

    # Content details
    record_count: int | None = Field(None, description="Number of records (where applicable)")
    content_type: str | None = Field(None, description="Type of content (e.g., PDF, Excel)")

    # Source details
    modified_time: str = Field(..., description="Last modified time in ISO format")
    drive_path: str = Field(..., description="Full path within Google Drive")

    # Processing details
    processing_status: str = Field("downloaded", description="Status of processing")
    processing_notes: str | None = Field(None, description="Notes about processing")

    # Validation
    is_valid: bool = Field(True, description="Whether the file is valid")
    validation_errors: list[str] = Field(default_factory=list, description="Validation errors")

    @validator("file_extension")
    def validate_file_extension(cls, v: str) -> str:
        """Ensure file extension starts with a dot."""
        if v and not v.startswith("."):
            v = f".{v}"
        return v.lower()


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
        file_content: bytes | None = None,
        record_count: int | None = None,
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
            file_content: File content as bytes (preferred for checksum calculation)
            record_count: Number of records (where applicable)

        Returns:
            FileMetadata object
        """
        logger.debug(f"Generating metadata for file: {file_path}")

        # Calculate checksum - prefer content-based calculation for reliability
        try:
            if file_content is not None:
                checksum = calculate_content_checksum(file_content)
                logger.debug(f"Used content-based checksum calculation for {file_path}")
            else:
                checksum = calculate_file_checksum(file_path)
                logger.debug(f"Used file-based checksum calculation for {file_path}")
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            # Use a fallback checksum based on file properties if file access fails
            checksum = calculate_content_checksum(
                f"{file_id}_{original_filename}_{file_size}_{modified_time.isoformat()}".encode()
            )
            logger.debug(f"Used fallback property-based checksum for {file_path}")

        # Get file extension
        file_extension = Path(original_filename).suffix.lower()

        # Determine content type based on mime_type
        content_type = None
        if "pdf" in mime_type:
            content_type = "PDF"
        elif "spreadsheet" in mime_type or "excel" in mime_type:
            content_type = "Excel"

        # Generate metadata
        metadata = FileMetadata(
            timestamp=datetime.datetime.now().isoformat(),
            source_url=f"https://drive.google.com/file/d/{file_id}/view",
            file_id=file_id,
            original_filename=original_filename,
            original_subfolder=original_subfolder,
            checksum=checksum,
            checksum_algorithm="sha256",
            mime_type=mime_type,
            file_size=file_size,
            file_extension=file_extension,
            record_count=record_count,
            content_type=content_type,
            modified_time=modified_time.isoformat(),
            drive_path=drive_path,
            processing_status="downloaded",
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
            json.dump(metadata_dict, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved metadata to {metadata_path}")
        return metadata_path

    def read_metadata(self, metadata_path: Path) -> FileMetadata:
        """Read metadata from a JSON file.

        Args:
            metadata_path: Path to the metadata file

        Returns:
            FileMetadata object

        Raises:
            StorageError: If the metadata file could not be read
        """
        try:
            with open(metadata_path, encoding="utf-8") as f:
                metadata_dict = json.load(f)

            metadata = FileMetadata.model_validate(metadata_dict)
            logger.debug(f"Read metadata from {metadata_path}")
            return metadata

        except Exception as e:
            error_msg = f"Failed to read metadata from {metadata_path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def validate_checksum(self, file_path: Path, metadata: FileMetadata) -> bool:
        """Validate the checksum of a file against its metadata.

        Args:
            file_path: Path to the file
            metadata: FileMetadata object

        Returns:
            True if the checksum is valid, False otherwise
        """
        try:
            # Calculate the current checksum
            current_checksum = calculate_file_checksum(
                file_path, algorithm=metadata.checksum_algorithm
            )

            # Compare with the stored checksum
            is_valid = current_checksum == metadata.checksum

            if is_valid:
                logger.debug(f"Checksum validation passed for {file_path}")
            else:
                logger.warning(
                    f"Checksum validation failed for {file_path}. "
                    f"Expected: {metadata.checksum}, Got: {current_checksum}"
                )

            return is_valid

        except Exception as e:
            logger.error(f"Failed to validate checksum for {file_path}: {str(e)}")
            return False

    def find_duplicates(self, run_dir: Path, checksum: str) -> list[Path]:
        """Find duplicate files based on checksum.

        Args:
            run_dir: Directory to search for duplicates
            checksum: Checksum to search for

        Returns:
            List of paths to duplicate files
        """
        duplicates = []

        # Walk through the run directory
        for metadata_path in run_dir.glob("**/*.metadata.json"):
            try:
                metadata = self.read_metadata(metadata_path)
                if metadata.checksum == checksum:
                    # Get the corresponding file path
                    file_path = metadata_path.with_suffix("").with_suffix(metadata.file_extension)
                    duplicates.append(file_path)
            except Exception as e:
                logger.warning(f"Error reading metadata {metadata_path}: {str(e)}")

        if duplicates:
            logger.info(f"Found {len(duplicates)} duplicate(s) with checksum {checksum}")

        return duplicates

    def update_metadata(self, metadata_path: Path, updates: dict[str, Any]) -> FileMetadata:
        """Update metadata for a file.

        Args:
            metadata_path: Path to the metadata file
            updates: Dictionary of updates to apply

        Returns:
            Updated FileMetadata object

        Raises:
            StorageError: If the metadata could not be updated
        """
        try:
            # Read existing metadata
            metadata = self.read_metadata(metadata_path)

            # Apply updates
            metadata_dict = metadata.model_dump()
            metadata_dict.update(updates)

            # Create updated metadata
            updated_metadata = FileMetadata.model_validate(metadata_dict)

            # Save updated metadata
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(updated_metadata.model_dump(), f, ensure_ascii=False, indent=2)

            logger.info(f"Updated metadata at {metadata_path}")
            return updated_metadata

        except Exception as e:
            error_msg = f"Failed to update metadata at {metadata_path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e
