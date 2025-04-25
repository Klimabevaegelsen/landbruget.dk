"""
DMI Climate Data Pipeline
Fetches and processes climate data from the Danish Meteorological Institute (DMI) API.
Transforms raw grid data into a structured format for analysis and storage.
"""

import argparse
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path
import asyncio
import os

from bronze.extract import DMIConfig, DMIApiClient
from silver.transform import DataTransformer
from silver.load import DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='DMI Climate Data Pipeline')
    parser.add_argument('--parameter', type=str, required=True, help='Parameter ID to fetch')
    parser.add_argument('--days', type=int, default=30, help='Number of days of data to fetch')
    parser.add_argument('--log-level', type=str, default='WARNING', choices=['WARNING', 'INFO', 'DEBUG', 'ERROR'],
                      help='Logging level')
    args = parser.parse_args()

    # Set logging level
    logger.setLevel(getattr(logging, args.log_level))

    try:
        # Initialize ETL pipeline components
        config = DMIConfig()
        extractor = DMIApiClient(config)
        transformer = DataTransformer()
        loader = DataLoader()

        # Calculate time range for data extraction
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=args.days)

        # Create timestamped output directories for data storage
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bronze_dir = Path(f"data/bronze/{timestamp}")
        silver_dir = Path(f"data/silver/{timestamp}")
        bronze_dir.mkdir(parents=True, exist_ok=True)
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Extract climate data from DMI API
        logger.info(f"Fetching {args.parameter} data from {start_time} to {end_time}")
        gdf = await extractor.fetch_grid_data(args.parameter, start_time, end_time)

        if not gdf.empty:
            # Save raw data in bronze layer
            loader.save_data(gdf, bronze_dir / "raw", f"{args.parameter}_raw")
            logger.info(f"Saved raw data to bronze layer: {bronze_dir}")

            # Transform raw grid data into structured format using DuckDB
            processed_result = transformer.transform_data(gdf)

            # Save processed data in silver layer
            loader.save_data(processed_result, silver_dir / "processed", f"{args.parameter}_processed")
            logger.info(f"Saved processed data to silver layer: {silver_dir}")

            # Get count of processed records
            count = processed_result.execute("SELECT COUNT(*) as count").fetchone()[0]
            logger.info(f"Successfully processed {count} records")
        else:
            logger.warning("No data returned from DMI API")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())