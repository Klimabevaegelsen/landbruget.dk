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
            base_path: Base path for Bronze layer storage
        """
        self.storage_manager = storage_manager
        self.base_path = base_path
        logger.info(f"Initialized Bronze storage manager with base path: {base_path}")

    def create_run_directory(self, timestamp: str | None = None) -> Path:
        """Create a timestamped run directory.

        Args:
            timestamp: Optional timestamp string (if not provided, one will be generated)

        Returns:
            Path to the created run directory
        """
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = generate_timestamp()

        # Create run directory path
        run_dir = self.base_path / timestamp

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(run_dir)

        logger.info(f"Created run directory: {run_dir}")
        return run_dir

    def create_folder_structure(self, run_dir: Path, folder_path: str) -> Path:
        """Create a folder structure mirroring the source.

        Args:
            run_dir: Base run directory
            folder_path: Path of the folder in the source (e.g., Google Drive)

        Returns:
            Path to the created folder
        """
        # Normalize folder path (remove leading/trailing slashes)
        folder_path = folder_path.strip("/")

        # Create folder path
        target_path = run_dir
        if folder_path:
            for folder in folder_path.split("/"):
                target_path = target_path / folder

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(target_path)

        logger.debug(f"Created folder structure: {target_path}")
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
            folder_path = os.path.dirname(source_path) if source_path else ""
            target_dir = self.create_folder_structure(run_dir, folder_path)
            logger.info(f"Created target directory: {target_dir}")

            # Create file path
            file_path = target_dir / filename
            logger.info(f"Target file path: {file_path}")
            logger.info(f"Target file path absolute: {file_path.absolute()}")

            # Save the file
            logger.info(f"Calling storage_manager.save_file with path: {file_path}")
            self.storage_manager.save_file(content, file_path)

            # Immediate verification after save - FIX: Use storage manager instead of local path check
            logger.info("Verifying file exists via storage manager...")
            file_exists = self.storage_manager.file_exists(file_path)
            logger.info(f"Storage manager file_exists result: {file_exists}")
            if file_exists:
                logger.info("File saved successfully to storage backend")
            else:
                logger.error(
                    f"IMMEDIATE VERIFICATION FAILED: File does not exist in storage backend at {file_path}"
                )

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
        # Create folder structure
        folder_path = os.path.dirname(source_path) if source_path else ""
        target_dir = run_dir
        if folder_path:
            for folder in folder_path.split("/"):
                target_dir = target_dir / folder

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

        # List files in the run directory
        for file_path in self.storage_manager.list_files(run_dir, pattern):
            files[file_path.name] = file_path

        # List files in subdirectories
        for root, _, _ in os.walk(run_dir):
            root_path = Path(root)
            if root_path != run_dir:
                for file_path in self.storage_manager.list_files(root_path, pattern):
                    # Use relative path as key
                    rel_path = file_path.relative_to(run_dir)
                    files[str(rel_path)] = file_path

        logger.debug(f"Listed {len(files)} files in run directory: {run_dir}")
        return files
