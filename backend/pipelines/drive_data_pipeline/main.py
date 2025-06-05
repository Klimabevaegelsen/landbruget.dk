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

# Load .env file directly
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path)

# Use absolute imports
from drive_data_pipeline.bronze import BronzeProcessor
from drive_data_pipeline.bronze.drive import GoogleDriveFetcher, get_drive_service
from drive_data_pipeline.bronze.metadata import MetadataManager
from drive_data_pipeline.config import get_settings, parse_args
from drive_data_pipeline.silver import SilverProcessor
from drive_data_pipeline.utils.logging import get_logger, setup_logging
from drive_data_pipeline.utils.storage import get_storage_manager


class ProgressTracker:
    """Tracks progress of pipeline operations and provides reporting capabilities."""

    def __init__(self, quiet: bool = False, verbose: bool = False):
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

    def start_bronze_operation(self, total_files: int):
        """Start tracking a bronze layer operation.

        Args:
            total_files: Total number of files to process
        """
        self.bronze_stats["total_files"] = total_files
        if not self.quiet:
            print(f"Starting Bronze layer processing: {total_files} files identified")
        self.logger.info(f"Bronze layer processing started: {total_files} files")

    def update_bronze_progress(self, file_count: int, success: bool, file_size: int = 0):
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
            pct = (self.bronze_stats["downloaded_files"] / self.bronze_stats["total_files"]) * 100
            print(
                f"Bronze progress: {self.bronze_stats['downloaded_files']}/{self.bronze_stats['total_files']} files ({pct:.1f}%)"
            )

    def start_silver_operation(self, total_files: int):
        """Start tracking a silver layer operation.

        Args:
            total_files: Total number of files to process
        """
        self.silver_stats["total_files"] = total_files
        if not self.quiet:
            print(f"Starting Silver layer processing: {total_files} files to transform")
        self.logger.info(f"Silver layer processing started: {total_files} files")

    def update_silver_progress(self, file_count: int, success: bool):
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
            pct = (self.silver_stats["processed_files"] / self.silver_stats["total_files"]) * 100
            print(
                f"Silver progress: {self.silver_stats['processed_files']}/{self.silver_stats['total_files']} files ({pct:.1f}%)"
            )

    def print_summary(self):
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
        logger.info("Starting Google Drive Data Pipeline")

        if args.verbose and not args.quiet:
            # Print startup information in verbose mode
            print("Google Drive Data Pipeline")
            print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        )

        # Initialize metadata manager
        metadata_manager = MetadataManager(settings.bronze_path)

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
        )

        # Process the Google Drive folder (Bronze layer)
        bronze_run_path = None
        if not args.silver_only:
            # List files once and reuse the result
            if not args.quiet:
                print("Listing files in Google Drive folder...")

            drive_folder = drive_fetcher.list_folder_contents(
                folder_id=settings.google_drive_folder_id, recursive=True
            )

            # Extract all files recursively from the folder structure for progress tracking
            all_files = []

            def collect_files(folder):
                all_files.extend(folder.files)
                for subfolder in folder.subfolders:
                    collect_files(subfolder)

            collect_files(drive_folder)

            # Initialize progress tracking
            progress.start_bronze_operation(len(all_files))

            # Process bronze layer - OPTIMIZED: Pass the already-fetched drive_folder
            bronze_processor.process_drive_folder(
                drive_folder=drive_folder,  # Use the already-fetched folder
                specific_subfolders=subfolders,
                supported_file_types=file_types,
            )
            bronze_run_path = bronze_processor.run_path

        # Process Silver layer if not bronze_only
        if not args.bronze_only:
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
            if bronze_run_path:
                # Count files to process for progress tracking
                files_to_process = list(Path(bronze_run_path).glob("**/*.*"))
                if file_types:
                    files_to_process = [
                        f
                        for f in files_to_process
                        if f.suffix.lower().replace(".", "") in file_types
                    ]

                # Initialize progress tracking
                progress.start_silver_operation(len(files_to_process))

                logger.info(f"Processing Bronze files to Silver layer from: {bronze_run_path}")
                silver_processor.process_bronze_files(
                    bronze_run_path=bronze_run_path,
                    specific_subfolders=subfolders,
                    supported_file_types=file_types,
                )
            else:
                error_msg = "No Bronze run path available for Silver processing"
                logger.error(error_msg)
                if not args.quiet:
                    print(f"Error: {error_msg}")

        # Print summary at the end
        progress.print_summary()

        logger.info("Google Drive Data Pipeline completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        if not args.quiet:
            print(f"Error: Pipeline execution failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
