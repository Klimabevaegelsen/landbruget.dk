"""Helper functions for Google Drive Data Pipeline."""

import datetime
import hashlib
import mimetypes
from pathlib import Path


def generate_timestamp() -> str:
    """Generate a timestamp string in the format YYYYMMDD_HHMMSS.

    Returns:
        Timestamp string
    """
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def calculate_file_checksum(file_path: str | Path, algorithm: str = "sha256") -> str:
    """Calculate a checksum for a file.

    Args:
        file_path: Path to the file
        algorithm: Hash algorithm to use (default: sha256)

    Returns:
        Checksum string

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the algorithm is not supported
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if algorithm.lower() == "sha256":
        hash_func = hashlib.sha256()
    elif algorithm.lower() == "md5":
        hash_func = hashlib.md5()
    elif algorithm.lower() == "sha1":
        hash_func = hashlib.sha1()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with open(file_path, "rb") as f:
        # Read in chunks to handle large files
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def calculate_content_checksum(content: bytes, algorithm: str = "sha256") -> str:
    """Calculate a checksum for file content.

    Args:
        content: File content as bytes
        algorithm: Hash algorithm to use (default: sha256)

    Returns:
        Checksum string

    Raises:
        ValueError: If the algorithm is not supported
        TypeError: If content is not bytes
    """
    if not isinstance(content, bytes):
        raise TypeError(f"Content must be bytes, got {type(content)}")

    if algorithm.lower() == "sha256":
        hash_func = hashlib.sha256()
    elif algorithm.lower() == "md5":
        hash_func = hashlib.md5()
    elif algorithm.lower() == "sha1":
        hash_func = hashlib.sha1()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    # For content that's already in memory, we can hash it directly
    # But still process in chunks for consistency and memory efficiency with large files
    chunk_size = 4096
    for i in range(0, len(content), chunk_size):
        chunk = content[i : i + chunk_size]
        hash_func.update(chunk)

    return hash_func.hexdigest()


def get_mime_type(file_path: str | Path) -> str:
    """Get the MIME type of a file based on its extension.

    Args:
        file_path: Path to the file

    Returns:
        MIME type string
    """
    mime_type, _ = mimetypes.guess_type(str(file_path))
    return mime_type or "application/octet-stream"


def is_supported_file_type(file_path: str | Path, supported_types: set[str]) -> bool:
    """Check if a file type is supported.

    Args:
        file_path: Path to the file
        supported_types: Set of supported file extensions (without dots)

    Returns:
        True if the file type is supported, False otherwise
    """
    file_path = Path(file_path)
    extension = file_path.suffix.lower().lstrip(".")
    return extension in supported_types
