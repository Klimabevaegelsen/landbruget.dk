"""Storage utilities for Google Drive Data Pipeline."""

import sys
from pathlib import Path
from typing import Any, BinaryIO

# Add the backend directory to the path to import common modules
backend_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(backend_dir))

from common.storage_interface import GCSStorage, LocalStorage, StorageInterface

from .error_handling import StorageError
from .logging import get_logger

# Get logger
logger = get_logger()


class DriveStorageManager:
    """Storage manager wrapper that adapts the common storage interface for drive pipeline needs."""

    def __init__(self, storage_interface: StorageInterface):
        """Initialize with a storage interface.

        Args:
            storage_interface: The storage interface to use (LocalStorage or GCSStorage)
        """
        self.storage = storage_interface

    def save_file(self, data: bytes | BinaryIO, path: str | Path) -> None:
        """Save file data to the given path.

        Args:
            data: File data as bytes or file-like object
            path: Path to save the file

        Raises:
            StorageError: If the file could not be saved
        """
        try:
            # Convert file-like object to bytes if needed
            if hasattr(data, "read"):
                data.seek(0)
                file_bytes = data.read()
            else:
                file_bytes = data

            logger.debug(f"DriveStorageManager.save_file called with path: {path}")
            logger.debug(f"Storage type: {type(self.storage)}")

            # For local storage, we need to handle the file writing ourselves
            if isinstance(self.storage, LocalStorage):
                full_path = Path(self.storage.base_dir) / path
                logger.debug(f"Local storage base_dir: {self.storage.base_dir}")
                logger.debug(f"Full path constructed: {full_path}")
                logger.debug(f"Full path absolute: {full_path.absolute()}")

                # Create parent directories
                full_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created parent directories for: {full_path.parent}")

                # Write the file
                logger.debug(f"Writing {len(file_bytes)} bytes to: {full_path}")
                with open(full_path, "wb") as f:
                    f.write(file_bytes)

                # Immediate verification
                if full_path.exists():
                    written_size = full_path.stat().st_size
                    logger.debug(f"File written successfully: {written_size} bytes")
                else:
                    logger.error(
                        f"CRITICAL: File write failed - file does not exist at: {full_path}"
                    )
            else:
                # For GCS, we can use the blob upload
                blob = self.storage.bucket.blob(str(path))
                blob.upload_from_string(file_bytes)

            logger.debug(f"Saved file to {path}")
        except Exception as e:
            error_msg = f"Failed to save file to {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def read_file(self, path: str | Path) -> bytes:
        """Read file data from the given path.

        Args:
            path: Path to read the file from

        Returns:
            File data as bytes

        Raises:
            StorageError: If the file could not be read
        """
        try:
            if isinstance(self.storage, LocalStorage):
                full_path = Path(self.storage.base_dir) / path
                with open(full_path, "rb") as f:
                    data = f.read()
            else:
                blob = self.storage.bucket.blob(str(path))
                data = blob.download_as_bytes()

            logger.debug(f"Read file from {path}")
            return data
        except Exception as e:
            error_msg = f"Failed to read file from {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def save_json(self, data: dict[str, Any], path: str | Path) -> None:
        """Save JSON data to the given path.

        Args:
            data: JSON-serializable data
            path: Path to save the JSON file

        Raises:
            StorageError: If the JSON could not be saved
        """
        try:
            self.storage.save_json(data, str(path))
            logger.debug(f"Saved JSON to {path}")
        except Exception as e:
            error_msg = f"Failed to save JSON to {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def read_json(self, path: str | Path) -> dict[str, Any]:
        """Read JSON data from the given path.

        Args:
            path: Path to read the JSON file from

        Returns:
            Parsed JSON data

        Raises:
            StorageError: If the JSON could not be read
        """
        try:
            data = self.storage.read_json(str(path))
            logger.debug(f"Read JSON from {path}")
            return data
        except Exception as e:
            error_msg = f"Failed to read JSON from {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def list_files(self, path: str | Path, pattern: str | None = None) -> list[Path]:
        """List files in the given path.

        Args:
            path: Path to list files from
            pattern: Optional glob pattern to filter files

        Returns:
            List of file paths

        Raises:
            StorageError: If the directory could not be read
        """
        try:
            if isinstance(self.storage, LocalStorage):
                full_path = Path(self.storage.base_dir) / path
                if pattern:
                    files = list(full_path.glob(pattern))
                else:
                    files = [f for f in full_path.iterdir() if f.is_file()]
                # Return relative paths
                return [f.relative_to(Path(self.storage.base_dir)) for f in files]
            else:
                # For GCS, list blobs with prefix
                prefix = str(path).rstrip("/") + "/" if path else ""
                blobs = self.storage.bucket.list_blobs(prefix=prefix)
                files = []
                for blob in blobs:
                    if not blob.name.endswith("/"):  # Skip directories
                        if pattern:
                            # Simple pattern matching for now
                            if pattern.replace("*", "") in blob.name:
                                files.append(Path(blob.name))
                        else:
                            files.append(Path(blob.name))
                return files
        except Exception as e:
            error_msg = f"Failed to list files in {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def ensure_directory_exists(self, path: str | Path) -> None:
        """Ensure that the directory exists, creating it if necessary.

        Args:
            path: Directory path

        Raises:
            StorageError: If the directory could not be created
        """
        try:
            if isinstance(self.storage, LocalStorage):
                full_path = Path(self.storage.base_dir) / path
                full_path.mkdir(parents=True, exist_ok=True)
            # For GCS, directories don't need to be created explicitly
            logger.debug(f"Ensured directory exists: {path}")
        except Exception as e:
            error_msg = f"Failed to create directory {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def file_exists(self, path: str | Path) -> bool:
        """Check if a file exists at the given path.

        Args:
            path: Path to check

        Returns:
            True if the file exists, False otherwise
        """
        try:
            if isinstance(self.storage, LocalStorage):
                full_path = Path(self.storage.base_dir) / path
                return full_path.exists()
            else:
                blob = self.storage.bucket.blob(str(path))
                return blob.exists()
        except Exception:
            return False


def get_storage_manager(
    storage_type: str, bucket_name: str | None = None, base_dir: str = ""
) -> DriveStorageManager:
    """Get a storage manager based on the storage type.

    Args:
        storage_type: Type of storage ("local" or "gcs")
        bucket_name: GCS bucket name (required for GCS storage)
        base_dir: Base directory for local storage

    Returns:
        Storage manager instance

    Raises:
        ValueError: If the storage type is invalid or required parameters are missing
    """
    if storage_type.lower() == "local":
        storage_interface = LocalStorage(base_dir)
        return DriveStorageManager(storage_interface)
    elif storage_type.lower() == "gcs":
        if not bucket_name:
            raise ValueError("bucket_name is required for GCS storage")
        storage_interface = GCSStorage(bucket_name)
        return DriveStorageManager(storage_interface)
    else:
        raise ValueError(f"Invalid storage type: {storage_type}. Must be 'local' or 'gcs'")
