"""Integration test for Bronze layer processing."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from drive_data_pipeline.bronze import BronzeProcessor
from drive_data_pipeline.config import Settings
from drive_data_pipeline.utils.storage import LocalStorageManager


@pytest.fixture
def mock_drive_files():
    """Mock files returned from Google Drive API."""
    # Sample file structure for testing
    return [
        {
            "id": "file1",
            "name": "test_file.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "parents": ["folder1"],
            "modifiedTime": "2023-01-01T00:00:00Z",
            "size": "1024"
        },
        {
            "id": "file2",
            "name": "test_file.pdf",
            "mimeType": "application/pdf",
            "parents": ["folder2"],
            "modifiedTime": "2023-01-01T00:00:00Z",
            "size": "2048"
        },
        {
            "id": "file3",
            "name": "nested_folder",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["folder1"],
        },
        {
            "id": "file4",
            "name": "test_nested_file.xlsx",
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "parents": ["file3"],
            "modifiedTime": "2023-01-01T00:00:00Z",
            "size": "512"
        }
    ]


@pytest.fixture
def mock_drive_fetcher(mock_drive_files):
    """Mock the GoogleDriveFetcher class."""
    with patch("drive_data_pipeline.bronze.drive.GoogleDriveFetcher") as MockFetcher:
        fetcher_instance = MagicMock()
        MockFetcher.return_value = fetcher_instance
        
        # Mock list_folder_contents to return sample files
        fetcher_instance.list_folder_contents.return_value = mock_drive_files
        
        # Mock get_file_metadata to return sample metadata
        def mock_get_metadata(file_id):
            for file in mock_drive_files:
                if file["id"] == file_id:
                    return file
            return None
        
        fetcher_instance.get_file_metadata.side_effect = mock_get_metadata
        
        # Mock download_file to create a simple file
        def mock_download(file_id, destination_path):
            # Create a simple test file at the destination
            with open(destination_path, "w") as f:
                if destination_path.endswith(".xlsx"):
                    f.write("mock excel content")
                elif destination_path.endswith(".pdf"):
                    f.write("mock pdf content")
            return True
            
        fetcher_instance.download_file.side_effect = mock_download
        
        yield fetcher_instance


@pytest.fixture
def test_settings():
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
            gcs_bucket=None,
            bronze_path=str(bronze_path),
            silver_path=str(silver_path),
            max_workers=1,
            log_level="INFO"
        )
        
        yield settings


@pytest.mark.integration
def test_bronze_processor(test_settings, mock_drive_fetcher):
    """Test bronze processor end-to-end functionality."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )
    
    # Process the mock drive folder
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id",
        specific_subfolders=None,
        supported_file_types=None
    )
    
    # Check that bronze output was created
    bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
    assert len(bronze_dirs) == 1, "Bronze output directory not found"
    
    bronze_run_dir = bronze_dirs[0]
    
    # Check that files were downloaded
    files = list(bronze_run_dir.glob("**/*.xlsx")) + list(bronze_run_dir.glob("**/*.pdf"))
    assert len(files) == 3, "Not all files were downloaded"
    
    # Check that metadata.json was created
    metadata_file = bronze_run_dir / "metadata.json"
    assert metadata_file.exists(), "Metadata file not created"
    
    # Check metadata content
    with open(metadata_file) as f:
        metadata = json.load(f)
    
    assert "timestamp" in metadata, "Timestamp missing from metadata"
    assert "files" in metadata, "Files list missing from metadata"
    assert len(metadata["files"]) == 3, "Not all files recorded in metadata"


@pytest.mark.integration
def test_bronze_specific_subfolders(test_settings, mock_drive_fetcher):
    """Test bronze processor with specific subfolders filter."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Configure mock_drive_fetcher to simulate folder structure
    def mock_list_contents(folder_id, recursive=False):
        if folder_id == "mock_folder_id":
            # Return only top-level folders if not recursive
            if not recursive:
                return [
                    {"id": "folder1", "name": "folder1", "mimeType": "application/vnd.google-apps.folder"},
                    {"id": "folder2", "name": "folder2", "mimeType": "application/vnd.google-apps.folder"}
                ]
            # Return all files if recursive
            return mock_drive_fetcher.list_folder_contents.return_value
        elif folder_id == "folder1":
            return [
                {
                    "id": "file1",
                    "name": "test_file.xlsx",
                    "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "parents": ["folder1"],
                    "modifiedTime": "2023-01-01T00:00:00Z",
                    "size": "1024"
                },
                {
                    "id": "file3",
                    "name": "nested_folder",
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": ["folder1"],
                }
            ]
        elif folder_id == "folder2":
            return [
                {
                    "id": "file2",
                    "name": "test_file.pdf",
                    "mimeType": "application/pdf",
                    "parents": ["folder2"],
                    "modifiedTime": "2023-01-01T00:00:00Z",
                    "size": "2048"
                }
            ]
        return []
    
    mock_drive_fetcher.list_folder_contents.side_effect = mock_list_contents
    
    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )
    
    # Process only folder1
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id",
        specific_subfolders=["folder1"],
        supported_file_types=None
    )
    
    # Check that bronze output was created
    bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
    assert len(bronze_dirs) == 1, "Bronze output directory not found"
    
    bronze_run_dir = bronze_dirs[0]
    
    # Check that only folder1 files were downloaded
    folder1_files = list(bronze_run_dir.glob("folder1/**/*"))
    folder2_files = list(bronze_run_dir.glob("folder2/**/*"))
    
    assert len(folder1_files) > 0, "folder1 files not downloaded"
    assert len(folder2_files) == 0, "folder2 files were downloaded despite filter"


@pytest.mark.integration
def test_bronze_specific_file_types(test_settings, mock_drive_fetcher):
    """Test bronze processor with specific file types filter."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )
    
    # Process only Excel files
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id",
        specific_subfolders=None,
        supported_file_types=["xlsx"]
    )
    
    # Check that bronze output was created
    bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
    assert len(bronze_dirs) == 1, "Bronze output directory not found"
    
    bronze_run_dir = bronze_dirs[0]
    
    # Check that only Excel files were downloaded
    excel_files = list(bronze_run_dir.glob("**/*.xlsx"))
    pdf_files = list(bronze_run_dir.glob("**/*.pdf"))
    
    assert len(excel_files) > 0, "Excel files not downloaded"
    assert len(pdf_files) == 0, "PDF files were downloaded despite filter"


@pytest.mark.integration
def test_bronze_error_handling(test_settings, mock_drive_fetcher):
    """Test bronze processor error handling capabilities."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Make one file download fail
    original_download = mock_drive_fetcher.download_file.side_effect
    
    def download_with_error(file_id, destination_path):
        if file_id == "file1":
            raise Exception("Simulated download error")
        return original_download(file_id, destination_path)
        
    mock_drive_fetcher.download_file.side_effect = download_with_error
    
    # Initialize BronzeProcessor
    bronze_processor = BronzeProcessor(
        settings=test_settings,
        drive_fetcher=mock_drive_fetcher,
        storage_manager=storage_manager,
    )
    
    # Process the mock drive folder
    bronze_processor.process_drive_folder(
        folder_id="mock_folder_id",
        specific_subfolders=None,
        supported_file_types=None
    )
    
    # Check that bronze output was created despite errors
    bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
    assert len(bronze_dirs) == 1, "Bronze output directory not found"
    
    bronze_run_dir = bronze_dirs[0]
    
    # Check that some files were downloaded (the ones that didn't error)
    files = list(bronze_run_dir.glob("**/*.xlsx")) + list(bronze_run_dir.glob("**/*.pdf"))
    assert len(files) > 0, "No files were downloaded"
    
    # The failed file shouldn't exist
    failed_file = list(bronze_run_dir.glob("**/test_file.xlsx"))
    assert len(failed_file) == 0 or (len(failed_file) == 1 and os.path.getsize(failed_file[0]) == 0), "Failed file exists and has content" 