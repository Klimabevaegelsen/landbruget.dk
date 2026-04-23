"""Integration test for Silver layer processing."""

import tempfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

pytest.importorskip("google.auth", reason="google-auth not installed")
from drive_data_pipeline.bronze.metadata import MetadataManager
from drive_data_pipeline.config import Settings
from drive_data_pipeline.silver import SilverProcessor
from drive_data_pipeline.silver.transformers.base import TransformResult
from drive_data_pipeline.utils.storage import DriveStorageManager


class FakeTransformer:
    """Minimal transformer stub matching the current SilverProcessor contract."""

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

        yield settings


@pytest.fixture
def mock_bronze_data(test_settings: Settings) -> Generator[DriveStorageManager, None, None]:
    """Create mock bronze data for testing."""
    # Create a mock bronze run directory following bronze/<dataset>/<timestamp>.
    bronze_run_dir = Path(test_settings.bronze_path) / "test_dataset" / "20230101_000000"
    bronze_run_dir.mkdir(parents=True)

    # Create mock files
    excel_file = bronze_run_dir / "test_excel.xlsx"
    pdf_file = bronze_run_dir / "test_pdf.pdf"

    with open(excel_file, "w") as f:
        f.write("mock excel content")

    with open(pdf_file, "w") as f:
        f.write("mock pdf content")

    storage_manager = DriveStorageManager("local")
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)

    excel_metadata = metadata_manager.generate_metadata(
        file_path=excel_file,
        file_content=b"mock excel content",
        file_id="excel1",
        original_filename="test_excel.xlsx",
        original_subfolder="subfolder1",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        file_size=len(b"mock excel content"),
        modified_time=datetime.fromisoformat("2023-01-01T00:00:00"),
        drive_path="root/subfolder1/test_excel.xlsx",
    )
    metadata_manager.save_metadata(excel_metadata, excel_file)

    pdf_metadata = metadata_manager.generate_metadata(
        file_path=pdf_file,
        file_content=b"mock pdf content",
        file_id="pdf1",
        original_filename="test_pdf.pdf",
        original_subfolder="subfolder2",
        mime_type="application/pdf",
        file_size=len(b"mock pdf content"),
        modified_time=datetime.fromisoformat("2023-01-01T00:00:00"),
        drive_path="root/subfolder2/test_pdf.pdf",
    )
    metadata_manager.save_metadata(pdf_metadata, pdf_file)

    yield bronze_run_dir


@pytest.mark.integration
def test_silver_processor(test_settings: Settings, mock_bronze_data: DriveStorageManager) -> None:
    """Test silver processor end-to-end functionality."""
    # Create storage manager
    storage_manager = DriveStorageManager("local", base_dir=str(test_settings.base_path))
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)

    # Mock the transformers
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
        excel_transformer = FakeTransformer(".xlsx", [{"column": "excel"}])
        pdf_transformer = FakeTransformer(".pdf", [{"column": "pdf"}])
        mock_excel_transformer.return_value = excel_transformer
        mock_pdf_transformer.return_value = pdf_transformer

        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
            metadata_manager=metadata_manager,
        )

        # Process the mock bronze data
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data, specific_subfolders=None, supported_file_types=None
        )

        # Check that silver output was created
        silver_dirs = list(Path(test_settings.silver_path).glob("*"))
        assert len(silver_dirs) == 1, "Silver output directory not found"

        # Check that transformers were called for each file type
        assert excel_transformer.transform.call_count == 1
        assert pdf_transformer.transform.call_count == 1

        # Check that output files were created
        silver_run_dir = silver_dirs[0]
        excel_output = list(silver_run_dir.glob("**/test_excel.parquet"))
        pdf_output = list(silver_run_dir.glob("**/test_pdf.parquet"))

        assert len(excel_output) == 1, "Excel output file not found"
        assert len(pdf_output) == 1, "PDF output file not found"


@pytest.mark.integration
def test_silver_specific_subfolders(
    test_settings: Settings, mock_bronze_data: DriveStorageManager
) -> None:
    """Test silver processor with specific subfolders filter."""
    # Create storage manager
    storage_manager = DriveStorageManager("local", base_dir=str(test_settings.base_path))
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)

    # Mock the transformers
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
        excel_transformer = FakeTransformer(".xlsx", [{"column": "excel"}])
        pdf_transformer = FakeTransformer(".pdf", [{"column": "pdf"}])
        mock_excel_transformer.return_value = excel_transformer
        mock_pdf_transformer.return_value = pdf_transformer

        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
            metadata_manager=metadata_manager,
        )

        # Process only subfolder1
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data,
            specific_subfolders=["subfolder1"],
            supported_file_types=None,
        )

        # Check that excel transformer was called but not pdf transformer
        assert excel_transformer.transform.call_count == 1
        assert pdf_transformer.transform.call_count == 0


@pytest.mark.integration
def test_silver_specific_file_types(
    test_settings: Settings, mock_bronze_data: DriveStorageManager
) -> None:
    """Test silver processor with specific file types filter."""
    # Create storage manager
    storage_manager = DriveStorageManager("local", base_dir=str(test_settings.base_path))
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)

    # Mock the transformers
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
        excel_transformer = FakeTransformer(".xlsx", [{"column": "excel"}])
        pdf_transformer = FakeTransformer(".pdf", [{"column": "pdf"}])
        mock_excel_transformer.return_value = excel_transformer
        mock_pdf_transformer.return_value = pdf_transformer

        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
            metadata_manager=metadata_manager,
        )

        # Process only PDF files
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data, specific_subfolders=None, supported_file_types=["pdf"]
        )

        # Check that pdf transformer was called but not excel transformer
        assert excel_transformer.transform.call_count == 0
        assert pdf_transformer.transform.call_count == 1


@pytest.mark.integration
def test_silver_error_handling(
    test_settings: Settings, mock_bronze_data: DriveStorageManager
) -> None:
    """Test silver processor error handling capabilities."""
    # Create storage manager
    storage_manager = DriveStorageManager("local", base_dir=str(test_settings.base_path))
    metadata_manager = MetadataManager(Path(test_settings.bronze_path), storage_manager)

    # Mock the transformers
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
        # Configure transformers - make excel transformer fail
        excel_transformer = FakeTransformer(".xlsx", [{"column": "excel"}])
        pdf_transformer = FakeTransformer(".pdf", [{"column": "pdf"}])
        mock_excel_transformer.return_value = excel_transformer
        mock_pdf_transformer.return_value = pdf_transformer
        excel_transformer.transform.side_effect = Exception("Mock transformation error")

        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
            metadata_manager=metadata_manager,
        )

        # Process the mock bronze data - should not fail due to error handling
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data, specific_subfolders=None, supported_file_types=None
        )

        # Check that silver output was created
        silver_dirs = list(Path(test_settings.silver_path).glob("*"))
        assert len(silver_dirs) == 1, "Silver output directory not found"

        # Check that both transformers were called
        assert excel_transformer.transform.call_count == 1
        assert pdf_transformer.transform.call_count == 1

        # Only PDF output should exist due to Excel error
        silver_run_dir = silver_dirs[0]
        excel_output = list(silver_run_dir.glob("**/test_excel.parquet"))
        pdf_output = list(silver_run_dir.glob("**/test_pdf.parquet"))

        assert len(excel_output) == 0, "Excel output file found despite error"
        assert len(pdf_output) == 1, "PDF output file not found"
