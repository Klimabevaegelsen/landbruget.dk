"""Integration test for Silver layer processing."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from drive_data_pipeline.config import Settings
from drive_data_pipeline.silver import SilverProcessor
from drive_data_pipeline.utils.storage import LocalStorageManager


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


@pytest.fixture
def mock_bronze_data(test_settings):
    """Create mock bronze data for testing."""
    # Create a mock bronze run directory
    bronze_run_dir = Path(test_settings.bronze_path) / "20230101_000000"
    bronze_run_dir.mkdir()
    
    # Create subdirectories mimicking Google Drive structure
    subfolder1 = bronze_run_dir / "subfolder1"
    subfolder2 = bronze_run_dir / "subfolder2"
    subfolder1.mkdir()
    subfolder2.mkdir()
    
    # Create mock files
    excel_file = subfolder1 / "test_excel.xlsx"
    pdf_file = subfolder2 / "test_pdf.pdf"
    
    with open(excel_file, "w") as f:
        f.write("mock excel content")
    
    with open(pdf_file, "w") as f:
        f.write("mock pdf content")
    
    # Create metadata file
    metadata = {
        "timestamp": "2023-01-01T00:00:00Z",
        "files": [
            {
                "file_id": "excel1",
                "name": "test_excel.xlsx",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "path": str(subfolder1 / "test_excel.xlsx"),
                "checksum": "abc123",
                "size_bytes": 1024
            },
            {
                "file_id": "pdf1",
                "name": "test_pdf.pdf",
                "mime_type": "application/pdf",
                "path": str(subfolder2 / "test_pdf.pdf"),
                "checksum": "def456",
                "size_bytes": 2048
            }
        ]
    }
    
    metadata_file = bronze_run_dir / "metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f)
    
    yield bronze_run_dir


@pytest.mark.integration
def test_silver_processor(test_settings, mock_bronze_data):
    """Test silver processor end-to-end functionality."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Mock the transformers
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
            return str(output_file)
            
        excel_transformer.transform.side_effect = mock_transform
        pdf_transformer.transform.side_effect = mock_transform
        
        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
        )
        
        # Process the mock bronze data
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data,
            specific_subfolders=None,
            supported_file_types=None
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
def test_silver_specific_subfolders(test_settings, mock_bronze_data):
    """Test silver processor with specific subfolders filter."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Mock the transformers
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
            return str(output_file)
            
        excel_transformer.transform.side_effect = mock_transform
        pdf_transformer.transform.side_effect = mock_transform
        
        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
        )
        
        # Process only subfolder1
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data,
            specific_subfolders=["subfolder1"],
            supported_file_types=None
        )
        
        # Check that excel transformer was called but not pdf transformer
        assert excel_transformer.transform.call_count == 1
        assert pdf_transformer.transform.call_count == 0


@pytest.mark.integration
def test_silver_specific_file_types(test_settings, mock_bronze_data):
    """Test silver processor with specific file types filter."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Mock the transformers
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
            return str(output_file)
            
        excel_transformer.transform.side_effect = mock_transform
        pdf_transformer.transform.side_effect = mock_transform
        
        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
        )
        
        # Process only PDF files
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data,
            specific_subfolders=None,
            supported_file_types=["pdf"]
        )
        
        # Check that pdf transformer was called but not excel transformer
        assert excel_transformer.transform.call_count == 0
        assert pdf_transformer.transform.call_count == 1


@pytest.mark.integration
def test_silver_error_handling(test_settings, mock_bronze_data):
    """Test silver processor error handling capabilities."""
    # Create storage manager
    storage_manager = LocalStorageManager()
    
    # Mock the transformers
    with patch("drive_data_pipeline.silver.transformers.excel_transformer.ExcelTransformer") as MockExcelTransformer, \
         patch("drive_data_pipeline.silver.transformers.pdf_transformer.PDFTransformer") as MockPDFTransformer:
        
        # Configure transformers - make excel transformer fail
        excel_transformer = MagicMock()
        pdf_transformer = MagicMock()
        MockExcelTransformer.return_value = excel_transformer
        MockPDFTransformer.return_value = pdf_transformer
        
        def mock_pdf_transform(file_path, output_dir):
            # Create a mock transformed file
            output_file = Path(output_dir) / f"{Path(file_path).stem}.parquet"
            with open(output_file, "w") as f:
                f.write("mock transformed content")
            return str(output_file)
            
        excel_transformer.transform.side_effect = Exception("Mock transformation error")
        pdf_transformer.transform.side_effect = mock_pdf_transform
        
        # Initialize SilverProcessor
        silver_processor = SilverProcessor(
            settings=test_settings,
            storage_manager=storage_manager,
        )
        
        # Process the mock bronze data - should not fail due to error handling
        silver_processor.process_bronze_files(
            bronze_run_path=mock_bronze_data,
            specific_subfolders=None,
            supported_file_types=None
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