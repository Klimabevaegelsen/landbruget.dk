"""Storage utilities for Drive Data Pipeline."""

from pathlib import Path
from typing import Any, BinaryIO

from .error_handling import StorageError
from .logging import get_logger

# Get logger
logger = get_logger()


# Try to import optimized cloud storage access with fallback
def _get_storage_access() -> type | None:
    """
    Get StorageAccess with robust import handling for different environments.

    Returns StorageAccess class if available, otherwise None.
    """
    try:
        from common.storage import StorageAccess

        logger.info("✅ Successfully imported StorageAccess")
        return StorageAccess
    except ImportError as e:
        logger.warning(f"⚠️ Could not import StorageAccess from common.storage: {e}")
        logger.warning(
            "⚠️ Falling back to basic storage - ensure common.storage is installed "
            "for optimal performance"
        )
        return None


# Get StorageAccess class or None if not available
StorageAccess = _get_storage_access()


class DriveStorageManager:
    """Storage manager wrapper that uses optimized cloud storage access for drive pipeline needs."""

    def __init__(
        self, storage_type: str, bucket_name: str | None = None, base_dir: str = ""
    ) -> None:
        """Initialize with storage configuration.

        Args:
            storage_type: Type of storage ('r2' or 'local')
            bucket_name: R2 bucket name (required for R2 storage)
            base_dir: Base directory for local storage
        """
        self.storage_type = storage_type
        self.bucket_name = bucket_name
        self.base_dir = Path(base_dir) if base_dir else Path()

        if storage_type.lower() == "r2":
            if not bucket_name:
                raise ValueError("bucket_name is required for R2 storage")

            # Try to use optimized R2 access via StorageAccess (s3fs)
            if StorageAccess:
                try:
                    self.storage = StorageAccess()
                    self.use_optimized = True
                    logger.info(
                        f"✅ DriveStorageManager: Initialized with optimized R2 access "
                        f"for bucket: {bucket_name}"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to initialize optimized R2 access: {e}")
                    self.storage = None
                    self.use_optimized = False
                    self._init_fallback_r2(bucket_name)
            else:
                self.storage = None
                self.use_optimized = False
                self._init_fallback_r2(bucket_name)
        else:
            # Local storage
            self.storage = None
            self.use_optimized = False
            self.base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"✅ DriveStorageManager: Initialized with local storage at: {self.base_dir}"
            )

    def _init_fallback_r2(self, bucket_name: str) -> None:
        """Initialize fallback storage using s3fs via common.storage.filesystem."""
        try:
            from common.storage.filesystem import get_r2_filesystem

            self.fs = get_r2_filesystem()
            logger.info(
                f"✅ DriveStorageManager: Initialized with s3fs fallback for bucket: {bucket_name}"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize storage: {e}") from e

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

            if self.storage_type.lower() == "r2":
                # For cloud storage, we need to construct the path correctly
                # Remove any base_dir prefix if it exists in the path
                relative_path = str(path)
                if str(self.base_dir) != "." and relative_path.startswith(str(self.base_dir)):
                    # Remove base_dir prefix to get relative path
                    relative_path = relative_path[len(str(self.base_dir)) :].lstrip("/\\")

                # Ensure we have the correct layer prefix based on the path
                if not relative_path.startswith(("bronze/", "silver/")):
                    # Determine the correct prefix based on the path content
                    if "bronze" in relative_path or "/bronze/" in str(path):
                        relative_path = f"bronze/{relative_path}"
                    else:
                        # Default to silver for drive data pipeline output
                        relative_path = f"silver/{relative_path}"

                if self.use_optimized and self.storage:
                    # Use optimized R2 access via s3fs
                    storage_path = f"{self.bucket_name}/{relative_path}"

                    # Use streaming upload for better performance
                    with self.storage.fs.open(storage_path, "wb") as cloud_file:
                        cloud_file.write(file_bytes)

                    logger.info(
                        f"✅ Saved file to storage (optimized): {storage_path} ({len(file_bytes)} bytes)"
                    )
                else:
                    # Use s3fs fallback storage
                    r2_path = f"{self.bucket_name}/{relative_path}"
                    with self.fs.open(r2_path, "wb") as f:
                        f.write(file_bytes)
                    logger.info(
                        f"✅ Saved file to storage (fallback): {r2_path} ({len(file_bytes)} bytes)"
                    )
            else:
                # Local storage
                full_path = self.base_dir / path
                full_path.parent.mkdir(parents=True, exist_ok=True)

                with open(full_path, "wb") as f:
                    f.write(file_bytes)

                if not full_path.exists():
                    raise StorageError(f"File write failed - file does not exist at: {full_path}")

                logger.debug(f"Saved file locally: {full_path}")

        except Exception as e:
            error_msg = f"Failed to save file to {path}: {e!s}"
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
            if self.storage_type.lower() == "r2":
                if self.use_optimized and self.storage:
                    storage_path = f"{self.bucket_name}/{path}"

                    with self.storage.fs.open(storage_path, "rb") as cloud_file:
                        data = cloud_file.read()

                    logger.debug(f"Read file from storage (optimized): {storage_path}")
                else:
                    # Use s3fs fallback storage
                    r2_path = f"{self.bucket_name}/{path}"
                    with self.fs.open(r2_path, "rb") as f:
                        data = f.read()
                    logger.debug(f"Read file from storage (fallback): {r2_path}")
            else:
                full_path = self.base_dir / path
                with open(full_path, "rb") as f:
                    data = f.read()

                logger.debug(f"Read file from local: {full_path}")

            return data
        except Exception as e:
            error_msg = f"Failed to read file from {path}: {e!s}"
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
            if self.storage_type.lower() == "r2":
                if self.use_optimized and self.storage:
                    storage_path = f"{self.bucket_name}/{path}"
                    self.storage.upload_json(data, storage_path)
                    logger.debug(f"Saved JSON to storage (optimized): {storage_path}")
                else:
                    # Use s3fs fallback storage
                    import json

                    json_str = json.dumps(data, indent=2, ensure_ascii=False)
                    r2_path = f"{self.bucket_name}/{path}"
                    with self.fs.open(r2_path, "w") as f:
                        f.write(json_str)
                    logger.debug(f"Saved JSON to storage (fallback): {r2_path}")
            else:
                full_path = self.base_dir / path
                full_path.parent.mkdir(parents=True, exist_ok=True)

                import json

                with open(full_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                logger.debug(f"Saved JSON locally: {full_path}")
        except Exception as e:
            error_msg = f"Failed to save JSON to {path}: {e!s}"
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
            if self.storage_type.lower() == "r2":
                if self.use_optimized and self.storage:
                    storage_path = f"{self.bucket_name}/{path}"
                    data = self.storage.download_json(storage_path)
                    logger.debug(f"Read JSON from storage (optimized): {storage_path}")
                else:
                    # Use s3fs fallback storage
                    import json

                    r2_path = f"{self.bucket_name}/{path}"
                    with self.fs.open(r2_path, "r") as f:
                        data = json.loads(f.read())
                    logger.debug(f"Read JSON from storage (fallback): {r2_path}")
            else:
                full_path = self.base_dir / path
                import json

                with open(full_path, encoding="utf-8") as f:
                    data = json.load(f)

                logger.debug(f"Read JSON from local: {full_path}")

            return data
        except Exception as e:
            error_msg = f"Failed to read JSON from {path}: {e!s}"
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
            if self.storage_type.lower() == "r2":
                if self.use_optimized and self.storage:
                    # Use optimized R2 file listing via s3fs
                    prefix = (
                        f"{self.bucket_name}/{path}".rstrip("/") + "/"
                        if path
                        else f"{self.bucket_name}/"
                    )
                    files = self.storage.list_files(prefix + "*")

                    # Convert to relative paths and filter
                    result = []
                    for file_path in files:
                        # Remove the bucket/ prefix
                        relative_path = file_path.replace(f"{self.bucket_name}/", "", 1)
                        if not relative_path.endswith("/"):  # Skip directories
                            if pattern:
                                if pattern.replace("*", "") in relative_path:
                                    result.append(Path(relative_path))
                            else:
                                result.append(Path(relative_path))

                    return result
                # Use s3fs fallback storage
                prefix = f"{self.bucket_name}/{path}".rstrip("/") + "/"
                try:
                    all_files = self.fs.ls(prefix, detail=False)
                except FileNotFoundError:
                    all_files = []
                files = []
                for file_path in all_files:
                    # Remove bucket prefix to get relative path
                    relative_path = file_path.replace(f"{self.bucket_name}/", "", 1)
                    if not relative_path.endswith("/"):  # Skip directories
                        if pattern:
                            if pattern.replace("*", "") in relative_path:
                                files.append(Path(relative_path))
                        else:
                            files.append(Path(relative_path))
                return files
            full_path = self.base_dir / path
            if pattern:
                files = list(full_path.glob(pattern))
            else:
                files = [f for f in full_path.iterdir() if f.is_file()]

            # Return relative paths
            return [f.relative_to(self.base_dir) for f in files]
        except Exception as e:
            error_msg = f"Failed to list files in {path}: {e!s}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def ensure_directory_exists(self, path: str | Path) -> None:
        """Ensure directory exists (no-op for cloud storage, creates directory for local).

        Args:
            path: Directory path

        Raises:
            StorageError: If the directory could not be created
        """
        try:
            if self.storage_type.lower() == "r2":
                # R2 doesn't require directory creation
                logger.debug(f"Directory ensured for R2: {path}")
            else:
                full_path = self.base_dir / path
                full_path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Directory created locally: {full_path}")
        except Exception as e:
            error_msg = f"Failed to ensure directory exists: {path}: {e!s}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def file_exists(self, path: str | Path) -> bool:
        """Check if a file exists.

        Args:
            path: File path

        Returns:
            True if file exists, False otherwise
        """
        try:
            if self.storage_type.lower() == "r2":
                if self.use_optimized and self.storage:
                    storage_path = f"{self.bucket_name}/{path}"
                    return self.storage.file_exists(storage_path)
                # Use s3fs fallback storage
                r2_path = f"{self.bucket_name}/{path}"
                return self.fs.exists(r2_path)
            full_path = self.base_dir / path
            return full_path.exists() and full_path.is_file()
        except Exception as e:
            logger.warning(f"Error checking file existence for {path}: {e}")
            return False


def get_storage_manager(
    storage_type: str, bucket_name: str | None = None, base_dir: str = ""
) -> DriveStorageManager:
    """Get a storage manager instance.

    Args:
        storage_type: Type of storage ('r2' or 'local')
        bucket_name: R2 bucket name (required for R2)
        base_dir: Base directory for local storage

    Returns:
        Configured DriveStorageManager instance

    Raises:
        ValueError: If invalid configuration is provided
    """
    return DriveStorageManager(storage_type, bucket_name, base_dir)
