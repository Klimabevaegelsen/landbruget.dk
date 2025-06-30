"""Integration test for end-to-end pipeline execution."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from drive_data_pipeline.config import Settings
from drive_data_pipeline.main import main


@pytest.fixture
def mock_drive_service():
    """Mock the Google Drive service."""
    with patch("drive_data_pipeline.bronze.drive.auth.get_drive_service") as mock_service:
        # Configure mock to return a fake service
        mock_service.return_value = MagicMock()
        yield mock_service.return_value


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
        }
    ]


@pytest.fixture
def mock_fetcher(mock_drive_service, mock_drive_files):
    """Mock the GoogleDriveFetcher class."""
    with patch("drive_data_pipeline.bronze.drive.GoogleDriveFetcher") as MockFetcher:
        fetcher_instance = MagicMock()
        MockFetcher.return_value = fetcher_instance
        
        # Mock list_folder_contents to return sample files
        fetcher_instance.list_folder_contents.return_value = mock_drive_files
        
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
        
        with patch("drive_data_pipeline.config.get_settings", return_value=settings):
            yield settings


@pytest.mark.integration
def test_end_to_end_pipeline(test_settings, mock_fetcher, monkeypatch):
    """Test the full pipeline execution from Bronze to Silver."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")
    
    # Mock Excel and PDF transformers
    with patch("drive_data_pipeline.silver.transformers.excel_transformer.ExcelTransformer") as MockExcelTransformer, \
         patch("drive_data_pipeline.silver.transformers.pdf_transformer.PDFTransformer") as MockPDFTransformer:
        # Configure transformers to create mock output files
        excel_transformer = MagicMock()
        pdf_transformer = MagicMock()
        MockExcelTransformer.return_value = excel_transformer
        MockPDFTransformer.return_value = pdf_transformer
        
        def mock_transform(file_path, output_dir):
            # Create a mock transformed file
            output_file = Path(output_dir) / f"{Path(file_path).stem}.parquet"
            with open(output_file, "w") as f:
                f.write("mock transformed content")
            return output_file
            
        excel_transformer.transform.side_effect = mock_transform
        pdf_transformer.transform.side_effect = mock_transform
        
        # Use patch to intercept args parsing to simulate command line
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            mock_args.return_value = MagicMock(
                subfolders=None,
                file_types=None,
                start_date=None,
                end_date=None,
                bronze_only=False,
                silver_only=False,
                log_level="INFO",
                verbose=False,
                quiet=False,
                config_file=None,
            )
            
            # Run the pipeline
            exit_code = main()
            
            # Verify the pipeline ran successfully
            assert exit_code == 0
            
            # Check that the drive fetcher was called with the correct folder ID
            mock_fetcher.list_folder_contents.assert_called_with(
                folder_id="mock_folder_id", recursive=True
            )
            
            # Verify at least one file was downloaded
            assert mock_fetcher.download_file.call_count > 0
            
            # Check for bronze and silver output
            bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
            assert len(bronze_dirs) > 0, "No bronze output directories found"
            
            silver_dirs = list(Path(test_settings.silver_path).glob("*"))
            assert len(silver_dirs) > 0, "No silver output directories found"


@pytest.mark.integration
def test_bronze_only_mode(test_settings, mock_fetcher, monkeypatch):
    """Test the pipeline in bronze-only mode."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")
    
    # Use patch to intercept args parsing to simulate command line
    with patch("argparse.ArgumentParser.parse_args") as mock_args:
        mock_args.return_value = MagicMock(
            subfolders=None,
            file_types=None,
            start_date=None,
            end_date=None,
            bronze_only=True,
            silver_only=False,
            log_level="INFO",
            verbose=False,
            quiet=False,
            config_file=None,
        )
        
        # Run the pipeline
        exit_code = main()
        
        # Verify the pipeline ran successfully
        assert exit_code == 0
        
        # Check for bronze output but no silver output
        bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
        assert len(bronze_dirs) > 0, "No bronze output directories found"
        
        silver_dirs = list(Path(test_settings.silver_path).glob("*"))
        assert len(silver_dirs) == 0, "Silver output found when using bronze-only mode"


@pytest.mark.integration
def test_silver_only_mode(test_settings, mock_fetcher, monkeypatch):
    """Test the pipeline in silver-only mode."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")
    
    # Create a mock bronze run directory with test files
    bronze_run_dir = Path(test_settings.bronze_path) / "20230101_000000"
    bronze_run_dir.mkdir()
    test_file = bronze_run_dir / "test_file.xlsx"
    with open(test_file, "w") as f:
        f.write("mock excel content")
    
    # Create metadata file
    metadata_file = bronze_run_dir / "metadata.json"
    with open(metadata_file, "w") as f:
        f.write('{"files": [{"file_id": "file1", "name": "test_file.xlsx", "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}]}')
    
    # Mock Excel transformer
    with patch("drive_data_pipeline.silver.transformers.excel_transformer.ExcelTransformer") as MockExcelTransformer:
        # Configure transformer to create mock output files
        excel_transformer = MagicMock()
        MockExcelTransformer.return_value = excel_transformer
        
        def mock_transform(file_path, output_dir):
            # Create a mock transformed file
            output_file = Path(output_dir) / f"{Path(file_path).stem}.parquet"
            with open(output_file, "w") as f:
                f.write("mock transformed content")
            return output_file
            
        excel_transformer.transform.side_effect = mock_transform
        
        # Use patch to intercept args parsing to simulate command line
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            mock_args.return_value = MagicMock(
                subfolders=None,
                file_types=None,
                start_date=None,
                end_date=None,
                bronze_only=False,
                silver_only=True,
                log_level="INFO",
                verbose=False,
                quiet=False,
                config_file=None,
            )
            
            # Run the pipeline
            exit_code = main()
            
            # Verify the pipeline ran successfully
            assert exit_code == 0
            
            # Check that no new bronze directories were created
            bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
            assert len(bronze_dirs) == 1, "Unexpected bronze directories found"
            
            # Check for silver output
            silver_dirs = list(Path(test_settings.silver_path).glob("*"))
            assert len(silver_dirs) > 0, "No silver output directories found"


@pytest.mark.integration
def test_error_recovery(test_settings, mock_fetcher, monkeypatch):
    """Test the pipeline's error recovery capabilities."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")
    
    # Make the second file download fail
    original_download = mock_fetcher.download_file.side_effect
    mock_file_ids = ["file1", "file2"]
    
    def download_with_error(file_id, destination_path):
        if file_id == mock_file_ids[1]:
            raise Exception("Simulated download error")
        return original_download(file_id, destination_path)
        
    mock_fetcher.download_file.side_effect = download_with_error
    
    # Use patch to intercept args parsing to simulate command line
    with patch("argparse.ArgumentParser.parse_args") as mock_args:
        mock_args.return_value = MagicMock(
            subfolders=None,
            file_types=None,
            start_date=None,
            end_date=None,
            bronze_only=True,  # Bronze only to simplify test
            silver_only=False,
            log_level="INFO",
            verbose=False,
            quiet=False,
            config_file=None,
        )
        
        # Run the pipeline
        exit_code = main()
        
        # Verify the pipeline ran successfully despite errors
        assert exit_code == 0
        
        # Check for bronze output
        bronze_dirs = list(Path(test_settings.bronze_path).glob("*"))
        assert len(bronze_dirs) > 0, "No bronze output directories found"
        
        # Verify one file was downloaded successfully
        assert mock_fetcher.download_file.call_count == 2  # Both attempts should be made 