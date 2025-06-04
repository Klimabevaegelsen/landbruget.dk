"""Authentication with Google Drive API."""

from pathlib import Path
from typing import Any

from google.auth import default
from google.oauth2 import service_account
from googleapiclient.discovery import build

from ...utils.logging import get_logger

# Get logger
logger = get_logger()


def get_drive_service(credentials_path: Path | None = None, use_public_access: bool = False) -> Any:
    """Get an authenticated Google Drive service.

    Args:
        credentials_path: Path to the service account credentials JSON file.
                         Can be None if using Application Default Credentials or public access.
        use_public_access: If True, creates a service for accessing public files
                          without authentication. Default is False.

    Returns:
        Authenticated Google Drive service

    Raises:
        Exception: If authentication fails and no public access is requested
    """
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]

    if use_public_access:
        try:
            # For public access, try without credentials first
            service = build("drive", "v3")
            logger.info("Google Drive service created for public access")
            return service
        except Exception as e:
            logger.warning(f"Failed to create public service: {e}")
            # Fall back to default credentials if available
            pass

    try:
        if credentials_path and credentials_path.exists():
            # Use service account credentials if provided
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path), scopes=scopes
            )
            logger.info(f"Using service account credentials from {credentials_path}")
        else:
            # Try Application Default Credentials
            credentials, project = default(scopes=scopes)
            logger.info("Using Application Default Credentials")

        service = build("drive", "v3", credentials=credentials)
        logger.info("Google Drive service created successfully")
        return service

    except Exception as e:
        if use_public_access:
            logger.warning(f"Authentication failed, attempting public access: {e}")
            # Try building service without credentials for public access
            service = build("drive", "v3")
            return service
        else:
            logger.error(f"Failed to authenticate with Google Drive API: {e}")
            raise
