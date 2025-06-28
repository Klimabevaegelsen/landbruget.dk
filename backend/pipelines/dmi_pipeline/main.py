"""
DMI Climate Data Pipeline
Fetches and processes climate data from the Danish Meteorological Institute (DMI) API.
Transforms raw grid data into a structured format for analysis and storage.
"""

import argparse
import asyncio
import logging
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from bronze.extract import DMIApiClient, DMIConfig
from silver.load import DataLoader
from silver.transform import DataTransformer
from tqdm.contrib.logging import logging_redirect_tqdm

# Configure logging
logger = logging.getLogger(__name__)

# Constants for resource management
DEFAULT_DAYS = 30
DEFAULT_MAX_CONCURRENT_FETCHES = 5


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Remove all existing handlers to start fresh
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Set up root logger at WARNING by default with a format that works well with tqdm
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Configure our pipeline loggers
    pipeline_logger = logging.getLogger("dmi_pipeline")
    pipeline_logger.setLevel(numeric_level)

    # Configure bronze module loggers
    bronze_logger = logging.getLogger("dmi_pipeline.bronze")
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)

    # Set third-party loggers to WARNING or higher
    for logger_name in ["aiohttp", "urllib3", "google", "requests"]:
        third_party_logger = logging.getLogger(logger_name)
        third_party_logger.setLevel(logging.WARNING)
        if numeric_level > logging.DEBUG:
            third_party_logger.propagate = False


def get_default_dates() -> tuple[date, date]:
    """Get default start and end dates (last 30 days)."""
    today = date.today()
    end_date = today
    start_date = today - timedelta(days=DEFAULT_DAYS)
    return start_date, end_date


def parse_args():
    """Parse command line arguments."""
    start_date_def, end_date_def = get_default_dates()

    parser = argparse.ArgumentParser(description="Run the DMI Climate Data Pipeline.")
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=start_date_def,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=end_date_def,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="WARNING", help="Logging level"
    )
    parser.add_argument("--progress", action="store_true", help="Show progress information")
    parser.add_argument("--environment", choices=["prod", "test"], default="prod", help="Environment to use")
    parser.add_argument("--test", action="store_true", help="Run in test mode with limited data")
    parser.add_argument(
        "--max-concurrent-fetches",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_FETCHES,
        help="Maximum number of concurrent API calls",
    )

    args = parser.parse_args()

    # Validate date range
    if args.start_date > args.end_date:
        parser.error("Start date must be before end date")

    return args


async def process_parameter(
    extractor: DMIApiClient,
    transformer: DataTransformer,
    loader: DataLoader,
    parameter_id: str,
    start_time: datetime,
    end_time: datetime,
    bronze_dir: Path,
    silver_dir: Path,
) -> int:
    """Process a single parameter and return the count of processed records"""
    logger.info(f"Fetching {parameter_id} data from {start_time} to {end_time}")

    # Fetch and save raw data in bronze layer
    data = await extractor.fetch_grid_data(parameter_id, start_time, end_time, bronze_dir / "raw")

    if data and data["features"]:
        # Transform raw grid data into structured format using DuckDB
        processed_result = transformer.transform_data(data)

        if processed_result:
            # Save processed data in silver layer
            if loader.save_data(processed_result, silver_dir / "processed", f"{parameter_id}_processed"):
                # Get count of processed records
                count = processed_result.execute("SELECT COUNT(*) as count").fetchone()[0]
                logger.info(f"Successfully processed {count} records for {parameter_id}")
                return count
            else:
                logger.error(f"Failed to save processed data for {parameter_id}")
                return 0
        else:
            logger.error(f"Failed to transform data for {parameter_id}")
            return 0
    else:
        logger.warning(f"No data returned from DMI API for {parameter_id}")
        return 0


async def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args.log_level)

    logger.warning("Starting DMI Climate Data pipeline")
    if args.progress:
        logger.warning(f"Processing date range: {args.start_date} to {args.end_date}")
        logger.warning(f"Using {args.max_concurrent_fetches} concurrent fetches")

    try:
        # Initialize ETL pipeline components
        config = DMIConfig()
        extractor = DMIApiClient(config)
        transformer = DataTransformer()
        loader = DataLoader()

        # Convert dates to datetime with UTC timezone
        start_time = datetime.combine(args.start_date, datetime.min.time(), tzinfo=UTC)
        end_time = datetime.combine(args.end_date, datetime.max.time(), tzinfo=UTC)

        # Create timestamped output directories for data storage using pipeline start time
        pipeline_start_time = datetime.now()
        timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")

        # Use environment variable for base data path, with fallback to relative path for local development
        base_data_path = os.getenv("DATA_PATH", "./data")
        bronze_dir = Path(base_data_path) / "bronze" / "dmi" / timestamp
        silver_dir = Path(base_data_path) / "silver" / "dmi" / timestamp
        bronze_dir.mkdir(parents=True, exist_ok=True)
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Process both parameters
        parameters = {"pot_evaporation_makkink": "Potential Evaporation", "acc_precip": "Precipitation"}

        total_records = 0
        with logging_redirect_tqdm():
            for param_id, param_name in parameters.items():
                try:
                    count = await process_parameter(
                        extractor, transformer, loader, param_id, start_time, end_time, bronze_dir, silver_dir
                    )
                    total_records += count
                except Exception as e:
                    logger.error(f"Failed to process {param_name}: {e}")
                    continue

        logger.warning("Pipeline completed successfully")
        if args.progress:
            logger.warning(f"Total records processed: {total_records}")
            logger.warning(f"Data exported to: {bronze_dir} and {silver_dir}")

        # Generate schema documentation for processed data
        if total_records > 0:
            try:
                logger.info("Generating schema documentation for DMI climate data")

                # Import schema documentation (with path adjustment)
                import sys
                from pathlib import Path

                # Find the project root (directory containing 'backend' folder)
                current_file = Path(__file__).resolve()
                project_root = None

                # Go up the directory tree to find the project root
                for parent in current_file.parents:
                    if (parent / "backend").is_dir():
                        project_root = parent
                        break

                if project_root and str(project_root) not in sys.path:
                    sys.path.insert(0, str(project_root))

                # Use the pipeline start time we already have
                # Create DuckDB connection and load processed parquet files
                import duckdb

                try:
                    from backend.common.schema_documentation import SchemaDocumentationManager
                except ImportError as e:
                    import warnings

                    warnings.warn(f"Schema documentation not available: {e}")
                    SchemaDocumentationManager = None

                conn = duckdb.connect()

                # Find all processed parquet files in silver directory
                processed_files = list((silver_dir / "processed").glob("*.parquet"))
                table_names = []

                for file_path in processed_files:
                    table_name = file_path.stem  # filename without extension
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
                    table_names.append(table_name)

                if table_names and SchemaDocumentationManager is not None:
                    # Initialize schema documentation manager
                    schema_manager = SchemaDocumentationManager(
                        connection=conn,
                        pipeline_name="dmi_pipeline",
                        pipeline_start_time=pipeline_start_time,
                        logger=logger,
                    )

                    # Generate documentation for all processed tables
                    schema_files = schema_manager.generate_all_documentation(table_names, stage="silver")
                    logger.info("Generated schema documentation for DMI climate data")

                    # Commit to GitHub
                    schema_manager.commit_to_github()
                    logger.info("DMI schema documentation committed to GitHub")
                elif SchemaDocumentationManager is None:
                    logger.warning("Schema documentation disabled due to import error")

            except Exception as e:
                logger.error(f"Failed to generate DMI schema documentation: {e}", exc_info=True)
                # Don't fail the pipeline if schema documentation fails

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
