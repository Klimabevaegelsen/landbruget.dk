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


def get_mime_type(file_path: str | Path) -> str:
    """Get the MIME type of a file.

    Args:
        file_path: Path to the file

    Returns:
        MIME type string
    """
    file_path = Path(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)
    
    # If mimetypes.guess_type() couldn't determine the type, use a default
    if mime_type is None:
        # Use extension to make a best guess
        extension = file_path.suffix.lower()
        if extension == ".pdf":
            mime_type = "application/pdf"
        elif extension in (".xlsx", ".xls"):
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            mime_type = "application/octet-stream"
    
    return mime_type


def is_supported_file_type(file_path: str | Path, supported_types: set[str] | None = None) -> bool:
    """Check if a file is of a supported type.

    Args:
        file_path: Path to the file
        supported_types: Set of supported file extensions (without the dot)
                        Default is PDF and Excel files

    Returns:
        True if the file is of a supported type, False otherwise
    """
    if supported_types is None:
        # Default supported types: PDF and Excel files
        supported_types = {"pdf", "xlsx", "xls"}
    
    file_path = Path(file_path)
    extension = file_path.suffix.lower().lstrip(".")
    
    return extension in supported_types 