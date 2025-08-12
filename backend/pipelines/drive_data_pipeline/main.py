"""Main entry point for Google Drive Data Pipeline."""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add the parent directory to sys.path to enable imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Use absolute imports
from drive_data_pipeline.bronze import BronzeProcessor
from drive_data_pipeline.bronze.drive import GoogleDriveFetcher, get_drive_service
from drive_data_pipeline.bronze.metadata import MetadataManager
from drive_data_pipeline.config import get_settings, parse_args
from drive_data_pipeline.silver import SilverProcessor
from drive_data_pipeline.utils.logging import get_logger, setup_logging
from drive_data_pipeline.utils.storage import get_storage_manager

# Try to import CVR collection utilities
try:
    from unified_pipeline.util.cvr_collection import (
        extract_cvr_numbers_from_table,
        save_pipeline_cvr_numbers,
    )
    from unified_pipeline.util.gcs_access import GCSDataAccess

    CVR_COLLECTION_AVAILABLE = True
    print("✅ CVR collection utilities imported successfully")
except ImportError as e:
    print(f"⚠️ CVR collection utilities not available: {e}")
    CVR_COLLECTION_AVAILABLE = False
    extract_cvr_numbers_from_table = None
    save_pipeline_cvr_numbers = None
    GCSDataAccess = None

# Load .env file directly - check current directory first, then parent
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if not os.path.exists(env_path):
    # Try parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
load_dotenv(env_path)


def _save_discovered_cvr_numbers(silver_path: Path, pipeline_start_time: datetime, logger) -> None:
    """
    Extract and save CVR numbers discovered in the drive data pipeline silver data.

    Args:
        silver_path: Path to silver data directory
        pipeline_start_time: Pipeline start time for timestamp generation
        logger: Logger instance
    """
    try:
        logger.info("📊 Starting CVR collection for drive data pipeline")

        # Strategy 1: First try to find local parquet files from current pipeline run
        # Ensure silver directory exists before searching for files
        if silver_path.exists():
            local_parquet_files = list(silver_path.rglob("*.parquet"))
        else:
            logger.info(f"⚠️ Silver directory does not exist: {silver_path}")
            local_parquet_files = []

        files_to_process = []
        source_description = ""

        if local_parquet_files:
            logger.info(f"✅ Found {len(local_parquet_files)} local parquet files from current run")
            files_to_process = [(str(f), "local") for f in local_parquet_files]
            source_description = "local files"
        else:
            # Strategy 2: Fallback to downloading recent files from GCS
            logger.info("🔍 No local parquet files found, searching GCS for recent files")

            try:
                gcs_access = GCSDataAccess()
                bucket = "landbrugsdata-raw-data"

                # Find parquet files in GCS silver directory with pattern matching
                silver_pattern = f"gs://{bucket}/silver/*/*/*.parquet"
                parquet_files = gcs_access.list_files(silver_pattern)

                # Filter to only recent files (within reasonable timeframe of pipeline run)
                pipeline_date = pipeline_start_time.strftime("%Y%m%d")
                recent_files = [f for f in parquet_files if pipeline_date in f]

                if not recent_files:
                    # Fallback to most recent files if no files from today
                    logger.warning(
                        f"⚠️ No files found for date {pipeline_date}, using most recent files"
                    )
                    recent_files = sorted(parquet_files, reverse=True)[:20]  # Most recent 20 files

                files_to_process = [
                    (f, "gcs") for f in recent_files[:20]
                ]  # Limit to avoid excessive processing
                source_description = f"GCS files (filtered for {pipeline_date})"

            except Exception as e:
                logger.error(f"❌ Could not access GCS files: {e}")
                return

        if not files_to_process:
            logger.warning("⚠️ No parquet files found in local directory or GCS")
            return

        logger.info(f"📂 Processing {len(files_to_process)} files from {source_description}")

        import duckdb

        conn = duckdb.connect()
        all_cvr_numbers = []

        # Initialize GCS access only if needed
        gcs_access = None
        if any(source == "gcs" for _, source in files_to_process):
            gcs_access = GCSDataAccess()

        # Process each parquet file
        for i, (file_path, source) in enumerate(files_to_process):
            try:
                table_name = f"drive_table_{i + 1}"
                file_display_name = (
                    Path(file_path).name if source == "local" else file_path.split("/")[-1]
                )

                if source == "gcs":
                    # 🚀 ENHANCED: Use native HMAC acceleration for faster Drive data loading
                    try:
                        gcs_access.query_parquet_native(file_path, "SELECT *", table_name)
                    except Exception as e:
                        print(f"Native loading failed, using fallback: {e}")
                        # Fallback to existing temp file method
                        with gcs_access._temp_download(file_path) as temp_file:
                            conn.execute(
                                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{temp_file}')"
                            )
                else:
                    # Local file - use directly
                    conn.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
                    )

                # Extract CVR numbers from this table
                cvr_numbers = extract_cvr_numbers_from_table(
                    table_name=table_name,
                    connection=conn,
                    cvr_column="cvr_number",  # Standardized column name from Excel transformer
                )

                if cvr_numbers:
                    all_cvr_numbers.extend(cvr_numbers)
                    logger.info(
                        f"   • {file_display_name}: {len(cvr_numbers)} CVR numbers ({source})"
                    )
                else:
                    logger.debug(f"   • {file_display_name}: No CVR numbers found ({source})")

            except Exception as e:
                logger.warning(f"   • {file_display_name}: Error extracting CVR numbers - {e}")

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(list(set(all_cvr_numbers)))

        if unique_cvr_numbers:
            # Initialize GCS access for saving if not already done
            if gcs_access is None:
                gcs_access = GCSDataAccess()

            timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")

            gcs_path = save_pipeline_cvr_numbers(
                pipeline_name="drive_data_pipeline",
                cvr_numbers=unique_cvr_numbers,
                gcs_access=gcs_access,
                bucket="landbrugsdata-raw-data",
                timestamp=timestamp,
            )

            logger.info(f"✅ Saved {len(unique_cvr_numbers)} unique CVR numbers to {gcs_path}")
        else:
            logger.warning("⚠️ No CVR numbers found in drive data pipeline")

    except Exception as e:
        logger.error(f"❌ Failed to save CVR numbers for drive data pipeline: {e}")


class ProgressTracker:
    """Tracks progress of pipeline operations and provides reporting capabilities."""

    def __init__(self, quiet: bool = False, verbose: bool = False) -> None:
        """Initialize the progress tracker.

        Args:
            quiet: Whether to suppress progress output
            verbose: Whether to show detailed progress information
        """
        self.quiet = quiet
        self.verbose = verbose
        self.start_time = time.time()
        self.bronze_stats = {
            "total_files": 0,
            "downloaded_files": 0,
            "download_errors": 0,
            "total_bytes": 0,
        }
        self.silver_stats = {
            "total_files": 0,
            "processed_files": 0,
            "processing_errors": 0,
        }
        self.logger = get_logger()

    def start_bronze_operation(self, total_files: int) -> None:
        """Start tracking a bronze layer operation.

        Args:
            total_files: Total number of files to process
        """
        self.bronze_stats["total_files"] = total_files
        if not self.quiet:
            print(f"Starting Bronze layer processing: {total_files} files identified")
        self.logger.info(f"Bronze layer processing started: {total_files} files")

    def update_bronze_progress(self, file_count: int, success: bool, file_size: int = 0) -> None:
        """Update bronze layer progress.

        Args:
            file_count: Number of new files processed
            success: Whether the operation was successful
            file_size: Size of processed file in bytes
        """
        if success:
            self.bronze_stats["downloaded_files"] += file_count
            self.bronze_stats["total_bytes"] += file_size
        else:
            self.bronze_stats["download_errors"] += file_count

        if self.verbose and not self.quiet:
            # Prevent division by zero
            if self.bronze_stats["total_files"] > 0:
                pct = (
                    self.bronze_stats["downloaded_files"] / self.bronze_stats["total_files"]
                ) * 100
                print(
                    f"Bronze progress: {self.bronze_stats['downloaded_files']}/{self.bronze_stats['total_files']} files ({pct:.1f}%)"
                )
            else:
                print(
                    f"Bronze progress: {self.bronze_stats['downloaded_files']} files processed (total unknown)"
                )

    def start_silver_operation(self, total_files: int) -> None:
        """Start tracking a silver layer operation.

        Args:
            total_files: Total number of files to process
        """
        self.silver_stats["total_files"] = total_files
        if not self.quiet:
            print(f"Starting Silver layer processing: {total_files} files to transform")
        self.logger.info(f"Silver layer processing started: {total_files} files")

    def update_silver_progress(self, file_count: int, success: bool) -> None:
        """Update silver layer progress.

        Args:
            file_count: Number of new files processed
            success: Whether the operation was successful
        """
        if success:
            self.silver_stats["processed_files"] += file_count
        else:
            self.silver_stats["processing_errors"] += file_count

        if self.verbose and not self.quiet:
            # Prevent division by zero
            if self.silver_stats["total_files"] > 0:
                pct = (
                    self.silver_stats["processed_files"] / self.silver_stats["total_files"]
                ) * 100
                print(
                    f"Silver progress: {self.silver_stats['processed_files']}/{self.silver_stats['total_files']} files ({pct:.1f}%)"
                )
            else:
                print(
                    f"Silver progress: {self.silver_stats['processed_files']} files processed (total unknown)"
                )

    def print_summary(self) -> None:
        """Print a summary of the pipeline run."""
        if self.quiet:
            return

        elapsed_time = time.time() - self.start_time
        print("\n" + "=" * 50)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 50)

        if self.bronze_stats["total_files"] > 0:
            print("\nBronze Layer:")
            print(f"  Files identified: {self.bronze_stats['total_files']}")
            print(f"  Files downloaded: {self.bronze_stats['downloaded_files']}")
            if self.bronze_stats["download_errors"] > 0:
                print(f"  Download errors: {self.bronze_stats['download_errors']}")
            print(f"  Total data size: {self.bronze_stats['total_bytes'] / (1024 * 1024):.2f} MB")

        if self.silver_stats["total_files"] > 0:
            print("\nSilver Layer:")
            print(
                f"  Files processed: {self.silver_stats['processed_files']}/{self.silver_stats['total_files']}"
            )
            if self.silver_stats["processing_errors"] > 0:
                print(f"  Processing errors: {self.silver_stats['processing_errors']}")

        print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

        # Log summary to logger as well
        self.logger.info(f"Pipeline execution completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"Bronze stats: {self.bronze_stats}")
        self.logger.info(f"Silver stats: {self.silver_stats}")


def main() -> int:
    """Main entry point for the pipeline.

    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    # Parse command-line arguments
    args = parse_args()

    # Initialize logging
    log_level = args.log_level or os.environ.get("LOG_LEVEL", "INFO")
    setup_logging(log_level=log_level)
    logger = get_logger()

    # Initialize progress tracker
    progress = ProgressTracker(quiet=args.quiet, verbose=args.verbose)

    try:
        # Track pipeline start time for consistent timestamping
        pipeline_start_time = datetime.now()

        logger.info("Starting Google Drive Data Pipeline")

        if args.verbose and not args.quiet:
            # Print startup information in verbose mode
            print("Google Drive Data Pipeline")
            print(f"Run time: {pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Log level: {log_level}")

        # Load settings
        settings = get_settings()

        # Check if required settings are available
        if not settings.google_drive_folder_id:
            error_msg = "Missing GOOGLE_DRIVE_FOLDER_ID in environment variables"
            logger.error(error_msg)
            if not args.quiet:
                print(f"Error: {error_msg}")
            return 1

        # Note: Credentials check is now handled in the auth module
        # It will try service account credentials, then Application Default Credentials,
        # then public access if enabled

        # Initialize storage manager
        storage_manager = get_storage_manager(
            storage_type=settings.storage_type.value,
            bucket_name=settings.gcs_bucket,
            base_dir=str(settings.base_path),
        )

        # Initialize metadata manager
        metadata_manager = MetadataManager(settings.bronze_path, storage_manager)

        # Initialize Google Drive service
        drive_service = get_drive_service(
            credentials_path=settings.google_application_credentials,
            use_public_access=settings.use_public_access,
        )

        # Initialize Google Drive fetcher
        drive_fetcher = GoogleDriveFetcher(
            drive_service, use_public_access=settings.use_public_access
        )

        # Process file types argument
        file_types = None
        if args.file_types:
            file_types = set(args.file_types.lower().split(","))
            logger.info(f"Processing file types: {file_types}")

        # Process subfolders argument
        subfolders = None
        if args.subfolders:
            subfolders = args.subfolders.split(",")
            logger.info(f"Processing subfolders: {subfolders}")

        # Initialize Bronze processor with progress tracking
        bronze_processor = BronzeProcessor(
            settings=settings,
            drive_fetcher=drive_fetcher,
            storage_manager=storage_manager,
            progress_callback=progress.update_bronze_progress,
            pipeline_start_time=pipeline_start_time,
        )

        # Process the Google Drive folder (Bronze layer)
        bronze_run_path = None
        bronze_data = None

        if not args.silver_only:
            # List files once and reuse the result
            if not args.quiet:
                print("Listing files in Google Drive folder...")

            drive_folder = drive_fetcher.list_folder_contents(
                folder_id=settings.google_drive_folder_id, recursive=True
            )

            # Extract all files recursively from the folder structure for progress tracking
            all_files = []

            def collect_files(folder) -> None:
                all_files.extend(folder.files)
                for subfolder in folder.subfolders:
                    collect_files(subfolder)

            collect_files(drive_folder)

            # Initialize progress tracking
            progress.start_bronze_operation(len(all_files))

            # Determine if we need in-memory data for Silver processing
            return_data = args.both

            # Process bronze layer - OPTIMIZED: Pass the already-fetched drive_folder
            result = bronze_processor.process_drive_folder(
                drive_folder=drive_folder,  # Use the already-fetched folder
                specific_subfolders=subfolders,
                supported_file_types=file_types,
                return_data=return_data,
            )

            if return_data:
                # Extract data and run path from result
                bronze_data = result
                bronze_run_path = bronze_processor.run_path
            else:
                # Traditional mode - just get the run path
                bronze_run_path = bronze_processor.run_path

        # Process Silver layer if requested
        silver_processed_count = 0
        if not args.bronze_only:
            logger.info("Starting Silver layer processing")

            # Initialize Silver processor with progress tracking
            silver_processor = SilverProcessor(
                settings=settings,
                storage_manager=storage_manager,
                metadata_manager=metadata_manager,
                progress_callback=progress.update_silver_progress,
            )

            # If no bronze_run_path (silver_only mode), find the latest bronze run
            if args.silver_only and not bronze_run_path:
                # Find the latest bronze run directory
                bronze_runs = sorted(
                    Path(settings.bronze_path).glob("*"),
                    key=lambda p: p.stat().st_mtime if p.is_dir() else 0,
                    reverse=True,
                )

                if not bronze_runs:
                    error_msg = "No Bronze runs found for Silver processing"
                    logger.error(error_msg)
                    if not args.quiet:
                        print(f"Error: {error_msg}")
                    return 1

                bronze_run_path = bronze_runs[0]
                logger.info(f"Using latest Bronze run for Silver processing: {bronze_run_path}")
                if args.verbose and not args.quiet:
                    print(f"Using Bronze data from: {bronze_run_path}")

            # Process Bronze files to Silver
            if bronze_data is not None:
                # In-memory processing mode
                logger.info("Processing Bronze data from memory - skipping disk I/O")

                # Count files from in-memory data
                file_data = bronze_data.get("data", {})
                files_to_process_count = len(file_data)

                # Apply filtering if specified
                if file_types or subfolders:
                    filtered_count = 0
                    for _file_key, file_info in file_data.items():
                        # Filter by file type if specified
                        if file_types:
                            file_extension = Path(file_info["original_filename"]).suffix.lstrip(".")
                            if file_extension not in file_types:
                                continue

                        # Filter by subfolder if specified
                        if subfolders and file_info.get("folder_name") not in subfolders:
                            continue

                        filtered_count += 1
                    files_to_process_count = filtered_count

                logger.info(
                    f"Found {files_to_process_count} files to process in Silver layer from memory"
                )

                # Initialize progress tracking
                progress.start_silver_operation(files_to_process_count)

                # Process from memory
                silver_processed_count = silver_processor.process_from_memory(
                    bronze_data=bronze_data,
                    specific_subfolders=subfolders,
                    supported_file_types=file_types,
                )

            elif bronze_run_path:
                # Traditional file-based processing mode
                # Count files to process for progress tracking using storage manager
                # This ensures compatibility with both local and GCS storage
                try:
                    # Use storage manager to list metadata files (which correspond to processable files)
                    metadata_files = storage_manager.list_files(
                        bronze_run_path, pattern="*.metadata.json"
                    )

                    # For GCS storage, also check recursively
                    if storage_manager.storage_type.lower() == "gcs":
                        # GCS storage - list with recursive prefix
                        prefix = str(bronze_run_path).rstrip("/") + "/"
                        if hasattr(storage_manager, "gcs_bucket") and storage_manager.gcs_bucket:
                            blobs = storage_manager.gcs_bucket.list_blobs(prefix=prefix)
                            additional_files = [
                                Path(blob.name)
                                for blob in blobs
                                if blob.name.endswith(".metadata.json")
                            ]
                            metadata_files.extend(additional_files)

                    # Filter by file types if specified
                    files_to_process_count = len(metadata_files)
                    if file_types:
                        # We need to check each metadata file to see if it matches the file types
                        filtered_count = 0
                        for metadata_path in metadata_files:
                            try:
                                metadata = metadata_manager.read_metadata(metadata_path)
                                if metadata.file_extension.lstrip(".") in file_types:
                                    filtered_count += 1
                            except Exception:
                                # Skip files we can't read metadata for
                                continue
                        files_to_process_count = filtered_count

                    logger.info(f"Found {files_to_process_count} files to process in Silver layer")

                except Exception as e:
                    logger.warning(f"Could not count files for progress tracking: {e}")
                    files_to_process_count = 1  # Fallback to avoid division by zero

                # Initialize progress tracking
                progress.start_silver_operation(files_to_process_count)

                logger.info(f"Processing Bronze files to Silver layer from: {bronze_run_path}")
                silver_processed_count = silver_processor.process_bronze_files(
                    bronze_run_path=bronze_run_path,
                    specific_subfolders=subfolders,
                    supported_file_types=file_types,
                )
            else:
                error_msg = "No Bronze run path or data available for Silver processing"
                logger.error(error_msg)
                if not args.quiet:
                    print(f"Error: {error_msg}")
                return 1  # Return failure immediately if no data to process

        # Print summary at the end
        progress.print_summary()

        # Determine success based on actual processed files
        bronze_processed = progress.bronze_stats["downloaded_files"]
        silver_processed = progress.silver_stats["processed_files"]

        # Check if we actually processed any files
        if not args.silver_only and bronze_processed == 0:
            logger.error("Pipeline failed: No files were downloaded in Bronze layer")
            if not args.quiet:
                print("Error: No files were downloaded")
            return 1

        if not args.bronze_only and silver_processed == 0:
            logger.error("Pipeline failed: No files were processed in Silver layer")
            if not args.quiet:
                print("Error: No files were processed in Silver layer")
            return 1

        # Generate schema documentation for processed data
        if not args.bronze_only:
            try:
                logger.info("Generating schema documentation for drive data pipeline")

                # Import schema documentation (with path adjustment)
                from pathlib import Path as PathLib

                # Find the project root (directory containing 'backend' folder)
                current_file = PathLib(__file__).resolve()
                project_root = None

                # Go up the directory tree to find the project root
                for parent in current_file.parents:
                    if (parent / "backend").is_dir():
                        project_root = parent
                        break

                if project_root and str(project_root) not in sys.path:
                    sys.path.insert(0, str(project_root))

                # Use the pipeline start time we already have
                # Create DuckDB connection and look for silver parquet files
                import duckdb

                try:
                    from backend.common.schema_documentation import SchemaDocumentationManager
                except ImportError as e:
                    import warnings

                    warnings.warn(f"Schema documentation not available: {e}", stacklevel=2)
                    SchemaDocumentationManager = None

                conn = duckdb.connect()

                # Find silver parquet files in the silver output directory
                silver_path = Path(settings.silver_path)
                if silver_path.exists():
                    parquet_files = list(silver_path.rglob("*.parquet"))
                    table_names = []

                    for i, file_path in enumerate(
                        parquet_files[:10]
                    ):  # Limit to first 10 files to avoid overwhelming
                        try:
                            table_name = f"drive_data_{i + 1}"
                            conn.execute(
                                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
                            )
                            table_names.append(table_name)
                        except Exception as e:
                            logger.warning(
                                f"Could not load {file_path} for schema documentation: {e}"
                            )

                    if table_names and SchemaDocumentationManager is not None:
                        # Initialize schema documentation manager
                        schema_manager = SchemaDocumentationManager(
                            connection=conn,
                            pipeline_name="drive_data_pipeline",
                            pipeline_start_time=pipeline_start_time,
                            logger=logger,
                        )

                        # Generate documentation for drive data tables
                        schema_files = schema_manager.generate_all_documentation(
                            table_names, stage="silver"
                        )
                        logger.info("Generated schema documentation for drive data tables")

                        # Commit to GitHub
                        schema_manager.commit_to_github()
                        logger.info("Drive data schema documentation committed to GitHub")
                    elif SchemaDocumentationManager is None:
                        logger.warning("Schema documentation disabled due to import error")
                    else:
                        logger.info("No processable parquet files found for schema documentation")
                else:
                    logger.info("Silver output directory not found, skipping schema documentation")

            except Exception as e:
                logger.error(
                    f"Failed to generate drive data schema documentation: {e}", exc_info=True
                )
                # Don't fail the pipeline if schema documentation fails

        # Save CVR numbers after Silver processing is complete
        if not args.bronze_only and CVR_COLLECTION_AVAILABLE:
            # Use the actual silver run path from the processor if available
            actual_silver_path = Path(settings.silver_path)
            if (
                "silver_processor" in locals()
                and hasattr(silver_processor, "silver_run_path")
                and silver_processor.silver_run_path
            ):
                actual_silver_path = storage_manager.base_dir / silver_processor.silver_run_path
                logger.info(
                    f"Using actual silver run path for CVR extraction: {actual_silver_path}"
                )
            else:
                logger.info(f"Using base silver path for CVR extraction: {actual_silver_path}")

            _save_discovered_cvr_numbers(
                silver_path=actual_silver_path,
                pipeline_start_time=pipeline_start_time,
                logger=logger,
            )

        logger.info("Google Drive Data Pipeline completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        if not args.quiet:
            print(f"Error: Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
