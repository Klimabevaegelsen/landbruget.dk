"""Authentication with Google Drive API."""

from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from ...utils.logging import get_logger

# Get logger
logger = get_logger()


def get_drive_service(credentials_path: Path) -> Any:
    """Get an authenticated Google Drive service.

    Args:
        credentials_path: Path to the service account credentials JSON file

    Returns:
        Authenticated Google Drive service

    Raises:
        FileNotFoundError: If the credentials file does not exist
        ValueError: If the credentials are invalid
    """
    if not credentials_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    try:
        # Authenticate with service account
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )

        # Build the Drive API client
        drive_service = build('drive', 'v3', credentials=credentials)
        
        logger.info("Successfully authenticated with Google Drive API")
        return drive_service
    
    except Exception as e:
        error_msg = f"Failed to authenticate with Google Drive API: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e 