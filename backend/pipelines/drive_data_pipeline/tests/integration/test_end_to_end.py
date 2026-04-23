"""Integration test for end-to-end pipeline execution."""

import tempfile
from argparse import Namespace
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("google.auth", reason="google-auth not installed")
from drive_data_pipeline.bronze.drive import DriveFile, DriveFolder
from drive_data_pipeline.bronze.metadata import MetadataManager
from drive_data_pipeline.config import Settings
from drive_data_pipeline.main import main
from drive_data_pipeline.silver.processor import SilverProcessor
from drive_data_pipeline.silver.transformers.base import TransformResult
from drive_data_pipeline.utils.storage import DriveStorageManager


class FakeTransformer:
    """Transformer stub aligned with the current silver transformer contract."""

    def __init__(self, extension: str, rows: list[dict[str, object]]) -> None:
        self.extension = extension
        self.rows = rows
        self.transform = MagicMock(side_effect=self._transform)
        self.transform_from_content = MagicMock(side_effect=self._transform_from_content)

    def can_handle(self, file_path: Path, metadata: dict[str, object]) -> bool:
        return Path(file_path).suffix.lower() == self.extension

    def _transform(self, file_path: Path, metadata: object, output_dir: Path) -> TransformResult:
        output_path = Path(output_dir) / f"{Path(file_path).stem}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"mock parquet content")
        return TransformResult(success=True, output_path=output_path)

    def _transform_from_content(
        self, file_content: bytes, filename: str, metadata_dict: dict[str, object]
    ) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


@pytest.fixture
def mock_drive_service() -> Generator[MagicMock, None, None]:
    """Mock the Google Drive service."""
    with patch("drive_data_pipeline.main.get_drive_service") as mock_service:
        # Configure mock to return a fake service
        mock_service.return_value = MagicMock()
        yield mock_service.return_value


@pytest.fixture
def mock_drive_folder() -> DriveFolder:
    """Mock DriveFolder tree returned from Google Drive API."""
    return DriveFolder(
        id="mock_folder_id",
        name="root",
        path="root",
        files=[],
        subfolders=[
            DriveFolder(
                id="folder1",
                name="folder1",
                path="root/folder1",
                parent_ids=["mock_folder_id"],
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
            ),
            DriveFolder(
                id="folder2",
                name="folder2",
                path="root/folder2",
                parent_ids=["mock_folder_id"],
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
            ),
        ],
    )


@pytest.fixture
def mock_fetcher(
    mock_drive_service: MagicMock, mock_drive_folder: DriveFolder
) -> Generator[MagicMock, None, None]:
    """Mock the GoogleDriveFetcher class."""
    with patch("drive_data_pipeline.main.GoogleDriveFetcher") as mock_fetcher_patch:
        fetcher_instance = MagicMock()
        mock_fetcher_patch.return_value = fetcher_instance

        fetcher_instance.list_folder_contents.return_value = mock_drive_folder

        def mock_download(file_id: str) -> tuple[bytes, dict[str, Any]]:
            if file_id == "file1":
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
            base_path=str(temp_path),
            bronze_path=str(bronze_path),
            silver_path=str(silver_path),
            max_workers=1,
            log_level="INFO",
        )

        with patch("drive_data_pipeline.main.get_settings", return_value=settings):
            yield settings


@pytest.mark.integration
def test_end_to_end_pipeline(
    test_settings: Settings, mock_fetcher: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the full pipeline execution from Bronze to Silver."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")

    # Mock Excel and PDF transformers
    with (
        patch(
            "drive_data_pipeline.silver.transformers.excel_transformer.ExcelTransformer"
        ) as mock_excel_transformer,
        patch(
            "drive_data_pipeline.silver.transformers.advanced_pdf_transformer.AdvancedPDFTransformer"
        ) as mock_pdf_transformer,
        patch.object(SilverProcessor, "_apply_schema_to_file", return_value=None),
        patch.object(SilverProcessor, "_handle_pii_in_file", return_value=None),
    ):
        excel_transformer = FakeTransformer(".xlsx", [{"kind": "excel"}])
        pdf_transformer = FakeTransformer(".pdf", [{"kind": "pdf"}])
        mock_excel_transformer.return_value = excel_transformer
        mock_pdf_transformer.return_value = pdf_transformer

        # Use patch to intercept args parsing to simulate command line
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            mock_args.return_value = Namespace(
                subfolders=None,
                file_types=None,
                start_date=None,
                end_date=None,
                bronze_only=False,
                silver_only=False,
                both=False,
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
        bronze_dirs = list(Path(test_settings.bronze_path).rglob("*.xlsx")) + list(
            Path(test_settings.bronze_path).rglob("*.pdf")
        )
        assert len(bronze_dirs) > 0, "No bronze output directories found"

        silver_dirs = list(Path(test_settings.silver_path).rglob("*.parquet"))
        assert len(silver_dirs) > 0, "No silver output directories found"


@pytest.mark.integration
def test_bronze_only_mode(
    test_settings: Settings, mock_fetcher: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the pipeline in bronze-only mode."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")

    # Use patch to intercept args parsing to simulate command line
    with patch("argparse.ArgumentParser.parse_args") as mock_args:
        mock_args.return_value = Namespace(
            subfolders=None,
            file_types=None,
            start_date=None,
            end_date=None,
            bronze_only=True,
            silver_only=False,
            both=False,
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
        bronze_dirs = list(Path(test_settings.bronze_path).rglob("*.xlsx")) + list(
            Path(test_settings.bronze_path).rglob("*.pdf")
        )
        assert len(bronze_dirs) > 0, "No bronze output directories found"

        silver_dirs = list(Path(test_settings.silver_path).rglob("*.parquet"))
        assert len(silver_dirs) == 0, "Silver output found when using bronze-only mode"


@pytest.mark.integration
def test_silver_only_mode(
    test_settings: Settings, mock_fetcher: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the pipeline in silver-only mode."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")

    # Create a mock bronze run directory with test files
    bronze_run_dir = Path(test_settings.bronze_path) / "root" / "20230101_000000"
    bronze_run_dir.mkdir(parents=True)
    test_file = bronze_run_dir / "test_file.xlsx"
    with open(test_file, "w") as f:
        f.write("mock excel content")

    storage_manager = DriveStorageManager("local", base_dir=str(test_settings.base_path))
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)
    metadata = metadata_manager.generate_metadata(
        file_path=test_file,
        file_content=b"mock excel content",
        file_id="file1",
        original_filename="test_file.xlsx",
        original_subfolder="root",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        file_size=len(b"mock excel content"),
        modified_time=datetime.fromisoformat("2023-01-01T00:00:00"),
        drive_path="root/test_file.xlsx",
    )
    metadata_manager.save_metadata(metadata, test_file)

    # Mock Excel transformer
    with (
        patch(
            "drive_data_pipeline.silver.transformers.excel_transformer.ExcelTransformer"
        ) as mock_excel_transformer,
        patch.object(SilverProcessor, "_apply_schema_to_file", return_value=None),
        patch.object(SilverProcessor, "_handle_pii_in_file", return_value=None),
    ):
        excel_transformer = FakeTransformer(".xlsx", [{"kind": "excel"}])
        mock_excel_transformer.return_value = excel_transformer

        # Use patch to intercept args parsing to simulate command line
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            mock_args.return_value = Namespace(
                subfolders=None,
                file_types=None,
                start_date=None,
                end_date=None,
                bronze_only=False,
                silver_only=True,
                both=False,
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
            bronze_dataset_dirs = [
                path for path in Path(test_settings.bronze_path).iterdir() if path.is_dir()
            ]
            assert len(bronze_dataset_dirs) == 1, "Unexpected bronze dataset directories found"

            # Check for silver output
            silver_dirs = list(Path(test_settings.silver_path).rglob("*.parquet"))
            assert len(silver_dirs) > 0, "No silver output directories found"


@pytest.mark.integration
def test_error_recovery(
    test_settings: Settings, mock_fetcher: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the pipeline's error recovery capabilities."""
    # Set necessary environment variables
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "mock_folder_id")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "mock_credentials.json")

    # Make the second file download fail
    original_download = mock_fetcher.download_file.side_effect
    mock_file_ids = ["file1", "file2"]

    def download_with_error(file_id: str) -> tuple[bytes, dict[str, Any]]:
        if file_id == mock_file_ids[1]:
            raise Exception("Simulated download error")
        return original_download(file_id)

    mock_fetcher.download_file.side_effect = download_with_error

    # Use patch to intercept args parsing to simulate command line
    with patch("argparse.ArgumentParser.parse_args") as mock_args:
        mock_args.return_value = Namespace(
            subfolders=None,
            file_types=None,
            start_date=None,
            end_date=None,
            bronze_only=True,  # Bronze only to simplify test
            silver_only=False,
            both=False,
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
        bronze_dirs = list(Path(test_settings.bronze_path).rglob("*.xlsx")) + list(
            Path(test_settings.bronze_path).rglob("*.pdf")
        )
        assert len(bronze_dirs) > 0, "No bronze output directories found"

        # Verify one file was downloaded successfully
        assert mock_fetcher.download_file.call_count == 2  # Both attempts should be made
