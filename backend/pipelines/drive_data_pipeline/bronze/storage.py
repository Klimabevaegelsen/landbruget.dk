"""Bronze layer storage management."""

import os
from pathlib import Path
from typing import Any

from ..utils.helpers import generate_timestamp
from ..utils.logging import get_logger
from ..utils.storage import DriveStorageManager, StorageError

# Get logger
logger = get_logger()


class BronzeStorageManager:
    """Storage manager for the Bronze layer."""

    def __init__(
        self,
        storage_manager: DriveStorageManager,
        base_path: Path,
    ):
        """Initialize the Bronze storage manager.

        Args:
            storage_manager: Storage manager for file operations
            base_path: Base path for Bronze layer storage (ignored for GCS, used for local)
        """
        self.storage_manager = storage_manager
        self.base_path = base_path
        # Use standard pipeline folder structure
        self.pipeline_name = "drive_data"
        logger.info(f"Initialized Bronze storage manager for pipeline: {self.pipeline_name}")

    def create_run_directory(self, timestamp: str | None = None) -> Path:
        """Create a timestamped run directory following the required path structure.

        Args:
            timestamp: Optional timestamp string (if not provided, one will be generated)

        Returns:
            Path to the created run directory
        """
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = generate_timestamp()

        # Use required structure: bronze/static_data/drive/{timestamp}
        # For local storage, this will be relative to base_path
        # For GCS, this will be the full path in the bucket
        if hasattr(self.storage_manager.storage, "bucket"):
            # GCS storage - use required structure
            run_dir = Path(f"bronze/static_data/drive/{timestamp}")
        else:
            # Local storage - use base_path with required structure
            run_dir = self.base_path / "static_data" / "drive" / timestamp

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(run_dir)

        logger.info(f"Created run directory: {run_dir}")
        return run_dir

    def create_folder_structure(self, run_dir: Path, folder_path: str) -> Path:
        """Create a folder structure mirroring the source with subfolder organization.

        Args:
            run_dir: Base run directory (already includes bronze/static_data/drive/{timestamp})
            folder_path: Path of the folder in the source (e.g., Google Drive)

        Returns:
            Path to the created folder
        """
        # Normalize folder path (remove leading/trailing slashes)
        folder_path = folder_path.strip("/")

        # For the required structure, we need to organize by subfolder name
        # The run_dir is already bronze/static_data/drive/{timestamp}
        # We need to add the subfolder name as the next level
        if folder_path:
            # Split the path and sanitize each component
            path_parts = folder_path.split("/")
            sanitized_parts = []

            for part in path_parts:
                # Sanitize each folder name for storage
                sanitized_part = part.replace(" ", "_").replace(".", "_").replace(":", "_")
                # Remove any other problematic characters
                sanitized_part = "".join(c for c in sanitized_part if c.isalnum() or c in "_-")
                # Convert to lowercase
                sanitized_part = sanitized_part.lower()
                sanitized_parts.append(sanitized_part)

            # For the required structure, the first part becomes the subfolder name
            # Additional parts preserve the hierarchy within that subfolder
            target_path = run_dir
            for part in sanitized_parts:
                target_path = target_path / part
        else:
            target_path = run_dir

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(target_path)

        logger.debug(f"Created folder structure: {target_path} (from: {folder_path})")
        return target_path

    def save_file(self, content: bytes, run_dir: Path, source_path: str, filename: str) -> Path:
        """Save a file to the Bronze layer.

        Args:
            content: File content as bytes
            run_dir: Base run directory
            source_path: Path of the file in the source (e.g., Google Drive)
            filename: Name of the file

        Returns:
            Path to the saved file

        Raises:
            StorageError: If the file could not be saved
        """
        try:
            # Create folder structure
            # If source_path is a folder name (not a full path), use it directly
            if source_path and "/" not in source_path:
                folder_path = source_path
            else:
                folder_path = os.path.dirname(source_path) if source_path else ""
            target_dir = self.create_folder_structure(run_dir, folder_path)

            # Create file path
            file_path = target_dir / filename

            # Save the file
            self.storage_manager.save_file(content, file_path)

            # Verify file was saved - add better error handling for GCS
            try:
                file_exists = self.storage_manager.file_exists(file_path)
                if not file_exists:
                    logger.error(
                        f"IMMEDIATE VERIFICATION FAILED: File does not exist in storage backend at {file_path}"
                    )
                    # For GCS, try a brief retry since there might be eventual consistency issues
                    if hasattr(self.storage_manager.storage, "bucket"):
                        import time

                        time.sleep(0.5)  # Brief wait for GCS consistency
                        file_exists = self.storage_manager.file_exists(file_path)
                        if file_exists:
                            logger.info(f"File verified after retry: {file_path}")
                        else:
                            raise StorageError(
                                f"File save verification failed even after retry: {file_path}"
                            )
            except Exception as verify_error:
                logger.error(f"File verification failed: {verify_error}")
                raise StorageError(f"Could not verify file save: {verify_error}")

            logger.info(f"Saved file to {file_path}")
            return file_path

        except Exception as e:
            error_msg = f"Failed to save file {filename}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def save_metadata(self, metadata: dict[str, Any], file_path: Path) -> Path:
        """Save metadata for a file.

        Args:
            metadata: File metadata
            file_path: Path to the file the metadata is for

        Returns:
            Path to the saved metadata file

        Raises:
            StorageError: If the metadata could not be saved
        """
        try:
            # Create metadata file path
            metadata_path = file_path.with_suffix(".metadata.json")

            # Save the metadata
            self.storage_manager.save_json(metadata, metadata_path)

            logger.info(f"Saved metadata to {metadata_path}")
            return metadata_path

        except Exception as e:
            error_msg = f"Failed to save metadata for {file_path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def file_exists(self, run_dir: Path, source_path: str, filename: str) -> bool:
        """Check if a file exists in the Bronze layer.

        Args:
            run_dir: Base run directory
            source_path: Path of the file in the source (e.g., Google Drive)
            filename: Name of the file

        Returns:
            True if the file exists, False otherwise
        """
        # Create folder structure path using the same logic as create_folder_structure
        # If source_path is a folder name (not a full path), use it directly
        if source_path and "/" not in source_path:
            folder_path = source_path
        else:
            folder_path = os.path.dirname(source_path) if source_path else ""

        target_dir = run_dir
        if folder_path:
            # Split the path and sanitize each component (same as create_folder_structure)
            path_parts = folder_path.split("/")
            for part in path_parts:
                sanitized_part = part.replace(" ", "_").replace(".", "_").replace(":", "_")
                sanitized_part = "".join(c for c in sanitized_part if c.isalnum() or c in "_-")
                # Convert to lowercase
                sanitized_part = sanitized_part.lower()
                target_dir = target_dir / sanitized_part

        # Check if the file exists
        file_path = target_dir / filename
        return self.storage_manager.file_exists(file_path)

    def list_files_in_run(self, run_dir: Path, pattern: str | None = None) -> dict[str, Path]:
        """List all files in a run directory.

        Args:
            run_dir: Run directory to list files from
            pattern: Optional glob pattern to filter files

        Returns:
            Dictionary mapping file names to file paths
        """
        files = {}

        try:
            # List files in the run directory
            for file_path in self.storage_manager.list_files(run_dir, pattern):
                files[file_path.name] = file_path

            # For local storage, also check subdirectories
            if not hasattr(self.storage_manager.storage, "bucket"):
                for root, _, _ in os.walk(run_dir):
                    root_path = Path(root)
                    if root_path != run_dir:
                        for file_path in self.storage_manager.list_files(root_path, pattern):
                            # Use relative path as key
                            rel_path = file_path.relative_to(run_dir)
                            files[str(rel_path)] = file_path

            logger.debug(f"Listed {len(files)} files in run directory: {run_dir}")
        except Exception as e:
            logger.warning(f"Could not list files in run directory {run_dir}: {e}")

        return files
