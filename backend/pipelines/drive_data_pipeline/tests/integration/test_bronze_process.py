"""Integration test for Bronze layer processing."""

import json
import os
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("google.auth", reason="google-auth not installed")
from drive_data_pipeline.bronze import BronzeProcessor
from drive_data_pipeline.bronze.drive import DriveFile, DriveFolder
from drive_data_pipeline.config import Settings
from drive_data_pipeline.utils.storage import DriveStorageManager


@pytest.fixture
def mock_drive_folder() -> DriveFolder:
    """Mock DriveFolder tree returned from Google Drive API."""
    nested_folder = DriveFolder(
        id="file3",
        name="nested_folder",
        parent_ids=["folder1"],
        path="root/folder1/nested_folder",
        files=[
            DriveFile(
                id="file4",
                name="test_nested_file.xlsx",
                mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                parent_ids=["file3"],
                modified_time="2023-01-01T00:00:00Z",
                size=512,
                path="root/folder1/nested_folder/test_nested_file.xlsx",
            )
        ],
    )

    folder1 = DriveFolder(
        id="folder1",
        name="folder1",
        parent_ids=["root"],
        path="root/folder1",
        files=[
            DriveFile(
                id="file1",
                name="test_file.xlsx",
                mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                parent_ids=["folder1"],
                modified_time="2023-01-01T00:00:00Z",
                size=1024,
                path="root/folder1/test_file.xlsx",
            )
        ],
        subfolders=[nested_folder],
    )

    folder2 = DriveFolder(
        id="folder2",
        name="folder2",
        parent_ids=["root"],
        path="root/folder2",
        files=[
            DriveFile(
                id="file2",
                name="test_file.pdf",
                mime_type="application/pdf",
                parent_ids=["folder2"],
                modified_time="2023-01-01T00:00:00Z",
                size=2048,
                path="root/folder2/test_file.pdf",
            )
        ],
    )

    return DriveFolder(
        id="mock_folder_id",
        name="root",
        path="root",
        files=[],
        subfolders=[folder1, folder2],
    )


@pytest.fixture
def mock_drive_fetcher(mock_drive_folder: DriveFolder) -> Generator[MagicMock, None, None]:
    """Mock the GoogleDriveFetcher class."""
    with patch("drive_data_pipeline.bronze.drive.GoogleDriveFetcher") as mock_fetcher:
        fetcher_instance = MagicMock()
        mock_fetcher.return_value = fetcher_instance

        # Mock list_folder_contents to return the DriveFolder tree
        fetcher_instance.list_folder_contents.return_value = mock_drive_folder

        # Mock download_file to return in-memory content and metadata
        def mock_download(file_id: str) -> tuple[bytes, dict[str, Any]]:
            if file_id in {"file1", "file4"}:
                return (b"mock excel content", {"file_id": file_id})
            if file_id == "file2":
                return (b"mock pdf content", {"file_id": file_id})
            raise ValueError(f"Unknown file ID: {file_id}")

        fetcher_instance.download_file.side_effect = mock_download

        yield fetcher_instance


@pytest.fixture
def test_settings() -> Generator[Settings, None, None]:
    """Create test settings with temporary directories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        bronze_path = temp_path / "bronze"
        silver_path = temp_path / "silver"
        bronze_path.mkdir()
        silver_path.mkdir()

        settings = Settings(
            google_drive_folder_id="mock_folder_id",
            google_application_credentials="mock_credentials.json",
            storage_type="local",
            r2_bucket=None,
            bronze_path=str(bronze_path),
            silver_path=str(silver_path),
            max_workers=1,
            log_level="INFO",
        )

        yield settings


@pytest.mark.integration
def test_bronze_processor(test_settings: Settings, mock_drive_fetcher: MagicMock) -> None:
    """Test bronze processor end-to-end functionality."""
    # Create storage manager
    storage_manager = DriveStorageManager("local")

    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )

    # Process the mock drive folder
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id", specific_subfolders=None, supported_file_types=None
    )

    # Check that bronze output was created
    bronze_root = Path(test_settings.bronze_path)

    # Check that files were downloaded
    files = list(bronze_root.rglob("*.xlsx")) + list(bronze_root.rglob("*.pdf"))
    assert len(files) == 3, "Not all files were downloaded"

    # Check that per-file metadata files were created
    metadata_files = list(bronze_root.rglob("*.metadata.json"))
    assert len(metadata_files) == 3, "Not all file metadata was created"

    with open(metadata_files[0]) as f:
        metadata = json.load(f)

    assert "file_id" in metadata, "File ID missing from metadata"
    assert "original_filename" in metadata, "Original filename missing from metadata"


@pytest.mark.integration
def test_bronze_specific_subfolders(test_settings: Settings, mock_drive_fetcher: MagicMock) -> None:
    """Test bronze processor with specific subfolders filter."""
    # Create storage manager
    storage_manager = DriveStorageManager("local")

    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )

    # Process only folder1
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id", specific_subfolders=["folder1"], supported_file_types=None
    )

    # Check that bronze output was created
    bronze_root = Path(test_settings.bronze_path)

    # Check that only folder1 files were downloaded
    folder1_files = list(bronze_root.rglob("folder1/**/*"))
    folder2_files = list(bronze_root.rglob("folder2/**/*"))

    assert len(folder1_files) > 0, "folder1 files not downloaded"
    assert len(folder2_files) == 0, "folder2 files were downloaded despite filter"


@pytest.mark.integration
def test_bronze_specific_file_types(test_settings: Settings, mock_drive_fetcher: MagicMock) -> None:
    """Test bronze processor with specific file types filter."""
    # Create storage manager
    storage_manager = DriveStorageManager("local")

    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )

    # Process only Excel files
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id", specific_subfolders=None, supported_file_types=["xlsx"]
    )

    # Check that bronze output was created
    bronze_root = Path(test_settings.bronze_path)

    # Check that only Excel files were downloaded
    excel_files = list(bronze_root.rglob("*.xlsx"))
    pdf_files = list(bronze_root.rglob("*.pdf"))

    assert len(excel_files) > 0, "Excel files not downloaded"
    assert len(pdf_files) == 0, "PDF files were downloaded despite filter"


@pytest.mark.integration
def test_bronze_error_handling(test_settings: Settings, mock_drive_fetcher: MagicMock) -> None:
    """Test bronze processor error handling capabilities."""
    # Create storage manager
    storage_manager = DriveStorageManager("local")

    # Make one file download fail
    original_download = mock_drive_fetcher.download_file.side_effect

    def download_with_error(file_id: str) -> tuple[bytes, dict[str, Any]]:
        if file_id == "file1":
            raise Exception("Simulated download error")
        return original_download(file_id)

    mock_drive_fetcher.download_file.side_effect = download_with_error

    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )

    # Process the mock drive folder
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id", specific_subfolders=None, supported_file_types=None
    )

    # Check that bronze output was created despite errors
    bronze_root = Path(test_settings.bronze_path)

    # Check that some files were downloaded (the ones that didn't error)
    files = list(bronze_root.rglob("*.xlsx")) + list(bronze_root.rglob("*.pdf"))
    assert len(files) > 0, "No files were downloaded"

    # The failed file shouldn't exist
    failed_file = list(bronze_root.rglob("test_file.xlsx"))
    assert len(failed_file) == 0 or (
        len(failed_file) == 1 and os.path.getsize(failed_file[0]) == 0
    ), "Failed file exists and has content"
