#!/usr/bin/env python3
"""
Test the drive pipeline with a local PDF file.
"""

import shutil
import sys
import tempfile
from pathlib import Path

# Add paths for imports
parent_dir = Path(__file__).parent
sys.path.insert(0, str(parent_dir))

from bronze.metadata import MetadataManager  # noqa: E402
from config.settings import get_settings  # noqa: E402
from silver.processor import SilverProcessor  # noqa: E402
from utils.logging import get_logger, setup_logging  # noqa: E402
from utils.storage import get_storage_manager  # noqa: E402


def test_drive_pipeline_with_pdf(pdf_path: Path) -> None:
    """Test the drive pipeline with a local PDF file."""

    # Setup logging
    setup_logging(log_level="INFO")
    logger = get_logger()

    logger.info(f"Testing drive pipeline with PDF: {pdf_path}")

    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return

    # Create temporary directories for bronze and silver
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        bronze_dir = temp_path / "bronze"
        silver_dir = temp_path / "silver"

        bronze_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)

        # Override settings to use temp directories
        settings = get_settings()
        settings.bronze_path = bronze_dir
        settings.silver_path = silver_dir
        settings.storage_type = "local"

        # Initialize storage manager and metadata manager
        storage_manager = get_storage_manager(settings)
        metadata_manager = MetadataManager(bronze_dir, storage_manager)

        logger.info("=== BRONZE LAYER PROCESSING ===")

        # Create a simple bronze processor test
        # We'll manually create the file structure that the bronze processor expects
        timestamp = "20250802_test"
        bronze_run_dir = bronze_dir / timestamp
        bronze_run_dir.mkdir(parents=True)

        # Copy the PDF to bronze directory
        bronze_pdf_path = bronze_run_dir / pdf_path.name
        shutil.copy2(pdf_path, bronze_pdf_path)

        # Create minimal metadata
        metadata_dict = {
            "file_id": "test_visa_pdf",
            "original_filename": pdf_path.name,
            "local_filename": bronze_pdf_path.name,
            "content_type": "PDF",
            "mime_type": "application/pdf",
            "size": bronze_pdf_path.stat().st_size,
            "checksum": "test_checksum",
            "created_time": "2025-08-02T00:00:00Z",
            "modified_time": "2025-08-02T00:00:00Z",
        }

        # Save metadata
        metadata_path = bronze_run_dir / f"{pdf_path.stem}_metadata.json"
        metadata_manager.write_metadata(metadata_dict, metadata_path)

        logger.info(f"✅ Bronze processing complete: {bronze_pdf_path}")

        # === SILVER LAYER PROCESSING ===
        logger.info("=== SILVER LAYER PROCESSING ===")

        # Initialize silver processor
        silver_processor = SilverProcessor(
            settings=settings, storage_manager=storage_manager, metadata_manager=metadata_manager
        )

        # Process the bronze data to silver
        silver_run_path = silver_dir / timestamp
        silver_run_path.mkdir(parents=True, exist_ok=True)

        try:
            # Process the file from bronze to silver
            from bronze.metadata import FileMetadata

            FileMetadata(**metadata_dict)

            success = silver_processor._process_file(
                file_path=bronze_pdf_path,
                metadata_path=metadata_path,
                silver_run_path=silver_run_path,
                apply_schemas=True,
                handle_pii=False,  # Skip PII for testing
            )

            if success:
                logger.info("✅ Silver processing successful!")

                # List the output files
                silver_files = list(silver_run_path.rglob("*.parquet"))
                logger.info(f"📁 Silver output files ({len(silver_files)}):")
                for f in silver_files:
                    logger.info(f"  - {f.name} ({f.stat().st_size} bytes)")

                # Try to read one of the files to see the data
                if silver_files:
                    try:
                        import pandas as pd

                        df = pd.read_parquet(silver_files[0])
                        logger.info(f"📊 Sample data from {silver_files[0].name}:")
                        logger.info(f"Shape: {df.shape}")
                        logger.info(f"Columns: {list(df.columns)}")
                        print(df.head().to_string())
                    except Exception as e:
                        logger.warning(f"Could not read parquet file: {e}")

            else:
                logger.error("❌ Silver processing failed")

        except Exception as e:
            logger.error(f"❌ Silver processing error: {e}", exc_info=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_drive_pipeline.py path/to/visa.pdf")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    test_drive_pipeline_with_pdf(pdf_path)
