#!/usr/bin/env python3
"""
Test the drive pipeline with work permits PDF.

This script tests the complete bronze → silver flow with the work permits PDF
to ensure the specialized WorkPermitsTransformer is working correctly.
"""

import shutil
import sys
from pathlib import Path

# Add the parent directory to sys.path for imports
parent_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(parent_dir))

from backend.pipelines.drive_data_pipeline.bronze.metadata import MetadataManager
from backend.pipelines.drive_data_pipeline.config.settings import get_settings
from backend.pipelines.drive_data_pipeline.silver.processor import SilverProcessor
from backend.pipelines.drive_data_pipeline.utils.logging import get_logger, setup_logging
from backend.pipelines.drive_data_pipeline.utils.storage import get_storage_manager


def test_work_permits_pipeline(pdf_path: Path) -> bool | None:
    """Test the drive pipeline with work permits PDF."""

    # Setup logging
    setup_logging(log_level="INFO")
    logger = get_logger()

    logger.info(f"Testing drive pipeline with work permits PDF: {pdf_path}")

    if not pdf_path.exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return False

    try:
        # Get settings with local storage for testing
        settings = get_settings()
        settings.storage_type = "local"
        settings.bronze_path = Path("data_local/bronze/drive_data")
        settings.silver_path = Path("data_local/silver/drive_data")

        # Create temporary directories
        settings.bronze_path.mkdir(parents=True, exist_ok=True)
        settings.silver_path.mkdir(parents=True, exist_ok=True)

        # Get storage manager
        storage_manager = get_storage_manager(
            storage_type=settings.storage_type,
            bucket_name=None,  # Not needed for local storage
            base_dir=Path("data_local"),
        )

        # Mock proper file metadata for testing (matching FileMetadata structure)
        from datetime import datetime

        now_iso = datetime.now().isoformat()

        test_metadata = {
            # Basic metadata
            "timestamp": now_iso,
            "source_url": "https://drive.google.com/file/d/test_work_permits/view",
            "file_id": "test_work_permits",
            "original_filename": pdf_path.name,
            "original_subfolder": "work_permits_test",
            # File integrity
            "checksum": "5299199867beb729afc17085ede99206f990c285e5716165f8db8ebec6313c50",
            "checksum_algorithm": "sha256",
            # File details
            "mime_type": "application/pdf",
            "file_size": pdf_path.stat().st_size,
            "file_extension": ".pdf",
            # Content details
            "record_count": None,
            "content_type": "PDF",
            # Source details
            "modified_time": now_iso,
            "drive_path": f"/work_permits_test/{pdf_path.name}",
            # Processing details
            "processing_status": "downloaded",
            "processing_notes": None,
            # Validation
            "is_valid": True,
        }

        # Create a bronze run directory and copy the PDF there
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bronze_run_path = settings.bronze_path / timestamp
        bronze_run_path.mkdir(parents=True, exist_ok=True)

        # Copy PDF to bronze location
        bronze_pdf_path = bronze_run_path / pdf_path.name
        shutil.copy2(pdf_path, bronze_pdf_path)

        # Create metadata file - should be named {filename_without_extension}.metadata.json
        pdf_stem = pdf_path.stem  # filename without extension
        metadata_path = bronze_run_path / f"{pdf_stem}.metadata.json"
        import json

        with open(metadata_path, "w") as f:
            json.dump(test_metadata, f, indent=2)

        logger.info(f"Bronze data prepared in: {bronze_run_path}")

        # Initialize metadata manager
        metadata_manager = MetadataManager(settings.bronze_path, storage_manager)

        # Initialize silver processor
        silver_processor = SilverProcessor(
            settings=settings, storage_manager=storage_manager, metadata_manager=metadata_manager
        )

        # Debug: Check what files are in bronze directory
        logger.info("Debug: Files in bronze directory:")
        for file in bronze_run_path.iterdir():
            logger.info(f"  {file.name} ({'file' if file.is_file() else 'dir'})")

        # Process the bronze data through silver
        # Convert absolute path to relative path from storage manager base
        relative_bronze_path = bronze_run_path.relative_to(Path("data_local"))
        logger.info(f"Processing through silver layer with relative path: {relative_bronze_path}")
        processed_count = silver_processor.process_bronze_files(
            bronze_run_path=relative_bronze_path,
            supported_file_types={"pdf"},  # Remove the dot - the code strips it automatically
        )

        if processed_count > 0:
            logger.info(f"✅ Successfully processed {processed_count} files through silver layer")

            # Check what was created in silver
            silver_run_path = silver_processor.silver_run_path
            if silver_run_path:
                logger.info(f"Silver output in: {silver_run_path}")

                # List output files
                for output_file in silver_run_path.rglob("*"):
                    if output_file.is_file():
                        logger.info(f"  Created: {output_file}")

            return True
        else:
            logger.error("❌ No files were processed successfully")
            return False

    except Exception as e:
        logger.error(f"❌ Pipeline test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main() -> None:
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python test_work_permits_pipeline.py <path_to_work_permits_pdf>")
        sys.exit(1)

    pdf_path = Path(sys.argv[1])
    success = test_work_permits_pipeline(pdf_path)

    if success:
        print("\\n🎉 Work permits pipeline test completed successfully!")
    else:
        print("\\n❌ Work permits pipeline test failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
