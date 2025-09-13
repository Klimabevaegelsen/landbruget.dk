"""Google Drive file fetcher for Bronze layer."""

import io
import ssl
from typing import Any

import requests
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseDownload

from ...utils.error_handling import (
    FileDownloadError,
    GoogleDriveAPIError,
    retry_with_exponential_backoff,
)
from ...utils.logging import get_logger, set_context
from .models import DriveFile, DriveFolder

# Get logger
logger = get_logger()

# Google Drive folder MIME type
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

# Supported file types
SUPPORTED_MIME_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
}


class GoogleDriveFetcher:
    """Fetcher for Google Drive files."""

    def __init__(self, drive_service: Resource, use_public_access: bool = False) -> None:
        """Initialize the fetcher.

        Args:
            drive_service: Authenticated Google Drive service
            use_public_access: Whether to use public access mode
        """
        self.drive_service = drive_service
        self.use_public_access = use_public_access
        logger.info("Initialized Google Drive fetcher")

    def list_folder_contents(
        self, folder_id: str, recursive: bool = True, parent_path: str = ""
    ) -> DriveFolder:
        """List the contents of a Google Drive folder.

        Args:
            folder_id: ID of the folder to list
            recursive: Whether to recursively list subfolders
            parent_path: Path of the parent folder (for nested folders)

        Returns:
            DriveFolder instance with files and subfolders

        Raises:
            GoogleDriveAPIError: If the folder could not be accessed
        """
        set_context(folder_id=folder_id)
        logger.info(f"Listing contents of folder: {folder_id}")

        try:
            # Try to get folder metadata
            folder_metadata = None
            if not self.use_public_access:
                # For authenticated access, get folder metadata
                folder_metadata = self._get_file_metadata(folder_id)
                if folder_metadata.get("mimeType") != FOLDER_MIME_TYPE:
                    raise GoogleDriveAPIError(f"ID {folder_id} is not a folder")
            else:
                # For public access, try to get metadata but don't fail if it doesn't work
                try:
                    folder_metadata = self._get_file_metadata(folder_id)
                    if folder_metadata.get("mimeType") != FOLDER_MIME_TYPE:
                        logger.warning(
                            f"ID {folder_id} might not be a folder, but continuing anyway"
                        )
                except Exception as e:
                    logger.warning(f"Could not get folder metadata for {folder_id}: {str(e)}")
                    # Create a minimal folder metadata for public access
                    folder_metadata = {
                        "id": folder_id,
                        "name": f"Public Folder {folder_id}",
                        "mimeType": FOLDER_MIME_TYPE,
                    }

            # Create folder model
            folder = DriveFolder.from_api_response(folder_metadata, parent_path)

            # List files and subfolders in the folder
            query = f"'{folder_id}' in parents and trashed = false"
            fields = "files(id, name, mimeType, parents, modifiedTime, size, webViewLink)"

            items = []
            page_token = None

            while True:
                # Get a page of results
                response = (
                    self.drive_service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields=f"nextPageToken, {fields}",
                        pageToken=page_token,
                    )
                    .execute()
                )

                items.extend(response.get("files", []))
                page_token = response.get("nextPageToken")

                if not page_token:
                    break

            # Process items
            for item in items:
                if item["mimeType"] == FOLDER_MIME_TYPE:
                    if recursive:
                        # Recursively list subfolder contents
                        subfolder = self.list_folder_contents(
                            item["id"], recursive=True, parent_path=folder.path
                        )
                        folder.subfolders.append(subfolder)
                    else:
                        # Just create the subfolder without recursing into it
                        subfolder = DriveFolder.from_api_response(item, folder.path)
                        folder.subfolders.append(subfolder)
                elif self._is_supported_file(item["mimeType"]):
                    # Add file to the folder
                    file = DriveFile.from_api_response(item, folder.path)
                    folder.files.append(file)

            logger.info(
                f"Found {len(folder.files)} files and {len(folder.subfolders)} subfolders "
                f"in folder {folder_id}"
            )
            return folder

        except Exception as e:
            error_msg = f"Failed to list contents of folder {folder_id}: {str(e)}"
            logger.error(error_msg)
            raise GoogleDriveAPIError(error_msg) from e

    @retry_with_exponential_backoff(
        max_attempts=5,
        retry_exceptions=GoogleDriveAPIError,
        min_wait_seconds=1,
        max_wait_seconds=60,
    )
    def _get_file_metadata(self, file_id: str) -> dict[str, Any]:
        """Get metadata for a file or folder.

        Args:
            file_id: ID of the file or folder

        Returns:
            File metadata as a dictionary

        Raises:
            GoogleDriveAPIError: If the file metadata could not be retrieved
        """
        try:
            fields = "id, name, mimeType, parents, modifiedTime, size, webViewLink"
            return self.drive_service.files().get(fileId=file_id, fields=fields).execute()
        except Exception as e:
            error_msg = f"Failed to get metadata for file {file_id}: {str(e)}"
            logger.error(error_msg)
            raise GoogleDriveAPIError(error_msg) from e

    def _is_supported_file(self, mime_type: str) -> bool:
        """Check if a file type is supported.

        Args:
            mime_type: MIME type of the file

        Returns:
            True if the file type is supported, False otherwise
        """
        return mime_type in SUPPORTED_MIME_TYPES

    @retry_with_exponential_backoff(
        max_attempts=5,
        retry_exceptions=(FileDownloadError, ssl.SSLError, TimeoutError, ConnectionError),
        min_wait_seconds=2,
        max_wait_seconds=120,
    )
    def download_file(self, file_id: str) -> tuple[bytes, dict[str, Any]]:
        """Download a file from Google Drive.

        Args:
            file_id: ID of the file to download

        Returns:
            Tuple of (file_content, metadata)

        Raises:
            FileDownloadError: If the file could not be downloaded
        """
        set_context(file_id=file_id)
        logger.info(f"Downloading file: {file_id}")

        try:
            # Get file metadata
            metadata = self._get_file_metadata(file_id)
            file_size = int(metadata.get("size", 0))

            # Decide download strategy based on file size
            # Use single request for files < 50MB, chunked for larger files
            if file_size > 0 and file_size < 50 * 1024 * 1024:  # 50MB threshold
                logger.info(f"Using single-request download for {file_size} byte file")
                content = self._download_single_request(file_id)
            else:
                logger.info(f"Using chunked download for {file_size} byte file")
                content = self._download_chunked(file_id)

            logger.info(f"Downloaded file {file_id} ({len(content)} bytes)")
            return content, metadata

        except ssl.SSLError as e:
            error_msg = f"SSL error downloading file {file_id}: {str(e)}"
            logger.error(error_msg)
            raise FileDownloadError(error_msg) from e
        except (TimeoutError, ConnectionError) as e:
            error_msg = f"Connection error downloading file {file_id}: {str(e)}"
            logger.error(error_msg)
            raise FileDownloadError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to download file {file_id}: {str(e)}"
            logger.error(error_msg)
            raise FileDownloadError(error_msg) from e

    def _download_single_request(self, file_id: str) -> bytes:
        """Download a file in a single HTTP request using the fastest method.

        Uses files.get with alt=media parameter for maximum speed.
        This is the fastest way according to Google Drive API docs.
        """
        import time

        start_time = time.time()

        # METHOD 1: Try the fastest approach using direct HTTP with alt=media
        try:
            # Get the access token from the service
            credentials = self.drive_service._http.credentials
            credentials.refresh(self.drive_service._http.request)
            access_token = credentials.token

            # Direct HTTP request with alt=media (fastest method)
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept-Encoding": "gzip",  # Enable compression for speed
                "User-Agent": "gzip",  # Required for compression
            }

            logger.debug(f"Using direct HTTP download with alt=media for file {file_id}")
            response = requests.get(url, headers=headers, stream=False, timeout=300)
            response.raise_for_status()
            content = response.content

            download_time = time.time() - start_time
            speed_mbps = (len(content) / (1024 * 1024)) / download_time if download_time > 0 else 0
            logger.info(
                f"Direct HTTP download: {len(content)} bytes in {download_time:.2f}s "
                f"({speed_mbps:.2f} MB/s)"
            )

            return content

        except Exception as e:
            logger.warning(f"Direct HTTP download failed, falling back to API client: {e}")

            # METHOD 2: Fallback to the original API client method
            request = self.drive_service.files().get_media(fileId=file_id)
            content = request.execute()

            download_time = time.time() - start_time
            speed_mbps = (len(content) / (1024 * 1024)) / download_time if download_time > 0 else 0
            logger.info(
                f"API client download: {len(content)} bytes in {download_time:.2f}s "
                f"({speed_mbps:.2f} MB/s)"
            )

            return content

    def _download_chunked(self, file_id: str) -> bytes:
        """Download a file using optimized chunked approach for large files."""
        import time

        start_time = time.time()

        # Try direct HTTP streaming first (fastest for large files too)
        try:
            # Get the access token from the service
            credentials = self.drive_service._http.credentials
            credentials.refresh(self.drive_service._http.request)
            access_token = credentials.token

            # Direct HTTP request with alt=media and streaming
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept-Encoding": "gzip",  # Enable compression
                "User-Agent": "gzip",
            }

            logger.debug(f"Using direct HTTP streaming download for large file {file_id}")
            response = requests.get(url, headers=headers, stream=True, timeout=300)
            response.raise_for_status()

            # Stream the content in chunks to avoid memory issues
            content_chunks = []
            total_size = 0
            chunk_count = 0

            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB chunks
                if chunk:  # Filter out keep-alive chunks
                    content_chunks.append(chunk)
                    total_size += len(chunk)
                    chunk_count += 1

                    if chunk_count % 5 == 0:  # Log every 5 chunks (40MB)
                        logger.debug(
                            f"Downloaded {total_size / (1024*1024):.1f} MB in {chunk_count} chunks"
                        )

            content = b"".join(content_chunks)

            download_time = time.time() - start_time
            speed_mbps = (len(content) / (1024 * 1024)) / download_time if download_time > 0 else 0
            logger.info(
                f"Direct HTTP streaming: {len(content)} bytes in {chunk_count} chunks, "
                f"{download_time:.2f}s ({speed_mbps:.2f} MB/s)"
            )

            return content

        except Exception as e:
            logger.warning(
                f"Direct HTTP streaming failed, falling back to MediaIoBaseDownload: {e}"
            )

            # Fallback to original chunked method
            request = self.drive_service.files().get_media(fileId=file_id)
            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request, chunksize=8 * 1024 * 1024)

            done = False
            progress = 0
            chunk_count = 0
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    chunk_count += 1

                    if status:
                        new_progress = int(status.progress() * 100)
                        if new_progress - progress >= 10:
                            progress = new_progress
                            logger.debug(f"Download progress: {progress}% (chunk {chunk_count})")

                except Exception as chunk_error:
                    logger.warning(
                        f"Error downloading chunk {chunk_count} for file {file_id}: "
                        f"{str(chunk_error)}"
                    )
                    raise

            file_content.seek(0)
            content = file_content.read()

            download_time = time.time() - start_time
            speed_mbps = (len(content) / (1024 * 1024)) / download_time if download_time > 0 else 0
            logger.info(
                f"MediaIoBaseDownload: {len(content)} bytes in {chunk_count} chunks, "
                f"{download_time:.2f}s ({speed_mbps:.2f} MB/s)"
            )

            return content
