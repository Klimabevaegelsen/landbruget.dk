"""Module for loading CHR_dyr data (Animal Movements) - Bronze Layer.

This module provides the main interface for loading animal movement data from CHR_dyr.
The functionality has been separated into focused modules for better maintainability.
"""

import logging
import os
from datetime import date, timedelta
from typing import Any, Optional

from dotenv import load_dotenv

from .animal_movements import load_animal_movements, load_cattle_movement_summaries
from .auth import create_chr_dyr_client, get_fvm_credentials

# Import unified pipeline GCS access
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_chr_dyr")


def load_animal_movements_task(
    chr_dyr_client,
    username: str,
    herd_number: int,
    start_date: Optional[date],
    end_date: Optional[date],
) -> Optional[Any]:
    """
    Wrapper function for parallel processing of animal movement loading.

    Args:
        chr_dyr_client: The CHR_dyr SOAP client
        username: Username for authentication
        herd_number: The herd number to fetch data for
        start_date: Optional start date for filtering
        end_date: Optional end date for filtering

    Returns:
        Raw response object or None if failed
    """
    return load_animal_movements(chr_dyr_client, username, herd_number, start_date, end_date)


# Consolidated processing for unified pipeline
_duckdb_conn = None
_gcs_access = None


def initialize_consolidated_processing():
    """Initialize DuckDB connection and GCS access for consolidated processing."""
    global _duckdb_conn, _gcs_access

    if not GCS_AVAILABLE:
        logger.error("GCSDataAccess not available - cannot use consolidated processing")
        return False

    try:
        _gcs_access = GCSDataAccess()
        _duckdb_conn = _gcs_access.duckdb_conn

        # Create consolidated table for all movement data
        _duckdb_conn.execute("""
            CREATE TABLE IF NOT EXISTS consolidated_movements (
                reporting_herd_number INTEGER,
                movement_date DATE,
                counterparty_herd INTEGER,
                movement_type VARCHAR,
                animal_count INTEGER,
                movement_reasons TEXT,  -- JSON array of reasons
                data_source VARCHAR DEFAULT 'chr_dyr'
            )
        """)

        logger.info("✅ Initialized consolidated DuckDB processing")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize consolidated processing: {e}")
        return False


def add_to_consolidated_table(movement_data):
    """Add movement data directly to consolidated DuckDB table."""
    global _duckdb_conn
    import json

    if not _duckdb_conn or not movement_data:
        return

    try:
        # Extract movements from the data
        movements = movement_data.get("movements", [])
        reporting_herd = movement_data.get("reporting_herd_number")

        if not movements:
            return

        # Insert each movement directly into DuckDB table
        for movement in movements:
            _duckdb_conn.execute(
                """
                INSERT INTO consolidated_movements 
                (reporting_herd_number, movement_date, counterparty_herd, movement_type, 
                 animal_count, movement_reasons)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    reporting_herd,
                    movement.get("movement_date"),
                    movement.get("counterparty_herd"),
                    movement.get("movement_type"),
                    movement.get("animal_count", 0),
                    json.dumps(
                        [str(r) for r in movement.get("movement_reasons", []) if r is not None], ensure_ascii=False
                    ),
                ],
            )

        logger.debug(f"Added {len(movements)} movements from herd {reporting_herd} to consolidated table")

    except Exception as e:
        logger.error(f"Failed to add data to consolidated table: {e}")


def finalize_consolidated_processing():
    """Save consolidated data to GCS and cleanup resources."""
    global _duckdb_conn, _gcs_access

    if not _duckdb_conn or not _gcs_access:
        return False

    try:
        # Get record count
        count_result = _duckdb_conn.execute("SELECT COUNT(*) FROM consolidated_movements").fetchone()
        record_count = count_result[0] if count_result else 0

        if record_count == 0:
            logger.warning("No movement data to save - consolidated table is empty")
            return True

        logger.info(f"Saving {record_count:,} consolidated movement records to GCS")

        # Save consolidated data directly to GCS using the unified pipeline pattern
        from .export import EXPORT_TIMESTAMP

        bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        gcs_path = f"gs://{bucket_name}/bronze/chr/{EXPORT_TIMESTAMP}/chr_dyr_movement_summaries.parquet"

        # Use the unified GCS access pattern - export directly to parquet
        _gcs_access.export_table_to_gcs_direct("consolidated_movements", gcs_path)

        logger.info(f"✅ Saved consolidated movement data to {gcs_path}")

        # Clean up table
        _duckdb_conn.execute("DROP TABLE IF EXISTS consolidated_movements")

        return True

    except Exception as e:
        logger.error(f"Failed to finalize consolidated processing: {e}")
        return False


# Main entry points for backward compatibility
def main():
    """Main entry point for the CHR_dyr data loading process."""
    try:
        # Initialize SOAP client
        chr_dyr_client = create_chr_dyr_client()
        username, _ = get_fvm_credentials()

        # Initialize consolidated processing
        if not initialize_consolidated_processing():
            logger.error("Failed to initialize consolidated processing")
            return False

        logger.info("CHR_dyr data loading process started")

        # Example usage - this would normally be called with specific herd numbers
        # and date ranges from the main pipeline
        test_herd = 12345  # Example herd number
        result = load_cattle_movement_summaries(
            chr_dyr_client, username, test_herd, start_date=date.today() - timedelta(days=30), end_date=date.today()
        )

        if result:
            logger.info(f"Successfully processed herd {test_herd}: {result}")
        else:
            logger.warning(f"Failed to process herd {test_herd}")

        # Finalize consolidated processing
        finalize_consolidated_processing()

        logger.info("CHR_dyr data loading process completed")
        return True

    except Exception as e:
        logger.error(f"Error in CHR_dyr data loading process: {e}")
        return False


if __name__ == "__main__":
    main()
