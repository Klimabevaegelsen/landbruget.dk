"""Storage utilities for Google Drive Data Pipeline."""

import abc
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Union, BinaryIO

from .error_handling import StorageError
from .logging import get_logger

# Get logger
logger = get_logger()


class StorageManager(abc.ABC):
    """Abstract base class for storage managers."""

    @abc.abstractmethod
    def save_file(self, data: Union[bytes, BinaryIO], path: Union[str, Path]) -> None:
        """Save file data to the given path.

        Args:
            data: File data as bytes or file-like object
            path: Path to save the file

        Raises:
            StorageError: If the file could not be saved
        """
        pass

    @abc.abstractmethod
    def read_file(self, path: Union[str, Path]) -> bytes:
        """Read file data from the given path.

        Args:
            path: Path to read the file from

        Returns:
            File data as bytes

        Raises:
            StorageError: If the file could not be read
        """
        pass

    @abc.abstractmethod
    def save_json(self, data: Dict[str, Any], path: Union[str, Path]) -> None:
        """Save JSON data to the given path.

        Args:
            data: JSON-serializable data
            path: Path to save the JSON file

        Raises:
            StorageError: If the JSON could not be saved
        """
        pass

    @abc.abstractmethod
    def read_json(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Read JSON data from the given path.

        Args:
            path: Path to read the JSON file from

        Returns:
            Parsed JSON data

        Raises:
            StorageError: If the JSON could not be read
        """
        pass

    @abc.abstractmethod
    def list_files(self, path: Union[str, Path], pattern: Optional[str] = None) -> List[Path]:
        """List files in the given path.

        Args:
            path: Path to list files from
            pattern: Optional glob pattern to filter files

        Returns:
            List of file paths

        Raises:
            StorageError: If the directory could not be read
        """
        pass

    @abc.abstractmethod
    def ensure_directory_exists(self, path: Union[str, Path]) -> None:
        """Ensure that the directory exists, creating it if necessary.

        Args:
            path: Directory path

        Raises:
            StorageError: If the directory could not be created
        """
        pass

    @abc.abstractmethod
    def file_exists(self, path: Union[str, Path]) -> bool:
        """Check if a file exists at the given path.

        Args:
            path: Path to check

        Returns:
            True if the file exists, False otherwise
        """
        pass


class LocalStorageManager(StorageManager):
    """Storage manager for local file system."""

    def save_file(self, data: Union[bytes, BinaryIO], path: Union[str, Path]) -> None:
        """Save file data to local file system.

        Args:
            data: File data as bytes or file-like object
            path: Path to save the file

        Raises:
            StorageError: If the file could not be saved
        """
        path = Path(path)
        try:
            # Create parent directory if it doesn't exist
            self.ensure_directory_exists(path.parent)

            # Write the file
            mode = "wb" if isinstance(data, bytes) else "wb+"
            with open(path, mode) as f:
                if isinstance(data, bytes):
                    f.write(data)
                else:
                    # Reset the file position to the beginning
                    data.seek(0)
                    f.write(data.read())
            
            logger.debug(f"Saved file to {path}")
        except Exception as e:
            error_msg = f"Failed to save file to {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def read_file(self, path: Union[str, Path]) -> bytes:
        """Read file data from local file system.

        Args:
            path: Path to read the file from

        Returns:
            File data as bytes

        Raises:
            StorageError: If the file could not be read
        """
        path = Path(path)
        try:
            with open(path, "rb") as f:
                data = f.read()
            
            logger.debug(f"Read file from {path}")
            return data
        except Exception as e:
            error_msg = f"Failed to read file from {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def save_json(self, data: Dict[str, Any], path: Union[str, Path]) -> None:
        """Save JSON data to local file system.

        Args:
            data: JSON-serializable data
            path: Path to save the JSON file

        Raises:
            StorageError: If the JSON could not be saved
        """
        path = Path(path)
        try:
            # Create parent directory if it doesn't exist
            self.ensure_directory_exists(path.parent)

            # Write the JSON file with indentation for readability
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Saved JSON to {path}")
        except Exception as e:
            error_msg = f"Failed to save JSON to {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def read_json(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Read JSON data from local file system.

        Args:
            path: Path to read the JSON file from

        Returns:
            Parsed JSON data

        Raises:
            StorageError: If the JSON could not be read
        """
        path = Path(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            logger.debug(f"Read JSON from {path}")
            return data
        except Exception as e:
            error_msg = f"Failed to read JSON from {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def list_files(self, path: Union[str, Path], pattern: Optional[str] = None) -> List[Path]:
        """List files in the given path on local file system.

        Args:
            path: Path to list files from
            pattern: Optional glob pattern to filter files

        Returns:
            List of file paths

        Raises:
            StorageError: If the directory could not be read
        """
        path = Path(path)
        try:
            if not path.exists():
                return []
            
            if pattern:
                files = list(path.glob(pattern))
            else:
                files = list(path.iterdir())
            
            # Filter out directories
            files = [f for f in files if f.is_file()]
            
            logger.debug(f"Listed {len(files)} files from {path}")
            return files
        except Exception as e:
            error_msg = f"Failed to list files from {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def ensure_directory_exists(self, path: Union[str, Path]) -> None:
        """Ensure that the directory exists, creating it if necessary.

        Args:
            path: Directory path

        Raises:
            StorageError: If the directory could not be created
        """
        path = Path(path)
        try:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created directory {path}")
        except Exception as e:
            error_msg = f"Failed to create directory {path}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def file_exists(self, path: Union[str, Path]) -> bool:
        """Check if a file exists at the given path.

        Args:
            path: Path to check

        Returns:
            True if the file exists, False otherwise
        """
        path = Path(path)
        return path.exists() and path.is_file()


class GCSStorageManager(StorageManager):
    """Storage manager for Google Cloud Storage.
    
    This is a placeholder implementation. In a real project, you would use the
    google-cloud-storage library to implement this class.
    """

    def __init__(self, bucket_name: str):
        """Initialize GCS storage manager.
        
        Args:
            bucket_name: GCS bucket name
        """
        self.bucket_name = bucket_name
        logger.info(f"GCS storage manager initialized with bucket {bucket_name}")
        # TODO: Initialize GCS client

    def save_file(self, data: Union[bytes, BinaryIO], path: Union[str, Path]) -> None:
        """Save file data to Google Cloud Storage.

        Args:
            data: File data as bytes or file-like object
            path: Path within the bucket to save the file

        Raises:
            StorageError: If the file could not be saved
        """
        # TODO: Implement GCS file saving
        path_str = str(path)
        logger.debug(f"Would save file to gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")

    def read_file(self, path: Union[str, Path]) -> bytes:
        """Read file data from Google Cloud Storage.

        Args:
            path: Path within the bucket to read the file from

        Returns:
            File data as bytes

        Raises:
            StorageError: If the file could not be read
        """
        # TODO: Implement GCS file reading
        path_str = str(path)
        logger.debug(f"Would read file from gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")

    def save_json(self, data: Dict[str, Any], path: Union[str, Path]) -> None:
        """Save JSON data to Google Cloud Storage.

        Args:
            data: JSON-serializable data
            path: Path within the bucket to save the JSON file

        Raises:
            StorageError: If the JSON could not be saved
        """
        # TODO: Implement GCS JSON saving
        path_str = str(path)
        logger.debug(f"Would save JSON to gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")

    def read_json(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Read JSON data from Google Cloud Storage.

        Args:
            path: Path within the bucket to read the JSON file from

        Returns:
            Parsed JSON data

        Raises:
            StorageError: If the JSON could not be read
        """
        # TODO: Implement GCS JSON reading
        path_str = str(path)
        logger.debug(f"Would read JSON from gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")

    def list_files(self, path: Union[str, Path], pattern: Optional[str] = None) -> List[Path]:
        """List files in the given path on Google Cloud Storage.

        Args:
            path: Path within the bucket to list files from
            pattern: Optional glob pattern to filter files

        Returns:
            List of file paths

        Raises:
            StorageError: If the files could not be listed
        """
        # TODO: Implement GCS file listing
        path_str = str(path)
        logger.debug(f"Would list files from gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")

    def ensure_directory_exists(self, path: Union[str, Path]) -> None:
        """Ensure that the directory prefix exists.
        
        Note: GCS doesn't have actual directories, only objects with / in their names.
        This method is a no-op for GCS.

        Args:
            path: Directory path
        """
        # GCS doesn't have directories, so this is a no-op
        pass

    def file_exists(self, path: Union[str, Path]) -> bool:
        """Check if a file exists at the given path in Google Cloud Storage.

        Args:
            path: Path within the bucket to check

        Returns:
            True if the file exists, False otherwise
        """
        # TODO: Implement GCS file existence check
        path_str = str(path)
        logger.debug(f"Would check if file exists at gs://{self.bucket_name}/{path_str}")
        raise NotImplementedError("GCS storage is not yet implemented")


def get_storage_manager(storage_type: str, bucket_name: Optional[str] = None) -> StorageManager:
    """Get a storage manager instance based on the specified type.

    Args:
        storage_type: Storage type ("local" or "gcs")
        bucket_name: GCS bucket name (required for "gcs" storage type)

    Returns:
        StorageManager instance

    Raises:
        ValueError: If the storage type is invalid or if bucket_name is not
            provided for "gcs" storage type
    """
    if storage_type.lower() == "local":
        return LocalStorageManager()
    elif storage_type.lower() == "gcs":
        if not bucket_name:
            raise ValueError("Bucket name must be provided for GCS storage")
        return GCSStorageManager(bucket_name)
    else:
        raise ValueError(f"Invalid storage type: {storage_type}") 