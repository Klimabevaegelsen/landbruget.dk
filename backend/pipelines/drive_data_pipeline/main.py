"""Main entry point for Google Drive Data Pipeline."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add the parent directory to sys.path to enable imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Load .env file directly
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

# Use absolute imports
from drive_data_pipeline.bronze import BronzeProcessor
from drive_data_pipeline.bronze.drive import GoogleDriveFetcher, get_drive_service
from drive_data_pipeline.bronze.metadata import MetadataManager
from drive_data_pipeline.config import get_settings, parse_args
from drive_data_pipeline.silver import SilverProcessor
from drive_data_pipeline.utils.logging import get_logger, setup_logging
from drive_data_pipeline.utils.storage import get_storage_manager


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
    
    try:
        logger.info("Starting Google Drive Data Pipeline")
        
        # Debug: Print environment variables
        folder_id = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
        credentials = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        logger.info(f"GOOGLE_DRIVE_FOLDER_ID: {folder_id}")
        logger.info(f"GOOGLE_APPLICATION_CREDENTIALS: {credentials}")
        logger.info(f"Current directory: {os.getcwd()}")
        
        env_exists = os.path.exists(os.path.join(os.getcwd(), '.env'))
        logger.info(f"Env file exists: {env_exists}")
        
        # Load settings
        settings = get_settings()
        
        # Check if required settings are available
        if not settings.google_drive_folder_id:
            logger.error("Missing GOOGLE_DRIVE_FOLDER_ID in environment variables")
            return 1
            
        if not settings.google_application_credentials:
            logger.error("Missing GOOGLE_APPLICATION_CREDENTIALS in environment variables")
            return 1
        
        # Initialize storage manager
        storage_manager = get_storage_manager(
            storage_type=settings.storage_type.value,
            bucket_name=settings.gcs_bucket,
        )
        
        # Initialize metadata manager
        metadata_manager = MetadataManager(settings.bronze_path)
        
        # Initialize Google Drive service
        drive_service = get_drive_service(settings.google_application_credentials)
        
        # Initialize Google Drive fetcher
        drive_fetcher = GoogleDriveFetcher(drive_service)
        
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
        
        # Initialize Bronze processor
        bronze_processor = BronzeProcessor(
            settings=settings,
            drive_fetcher=drive_fetcher,
            storage_manager=storage_manager,
        )
        
        # Process the Google Drive folder (Bronze layer)
        bronze_run_path = None
        if not args.silver_only:
            bronze_processor.process_drive_folder(
                folder_id=settings.google_drive_folder_id,
                specific_subfolders=subfolders,
                supported_file_types=file_types,
            )
            bronze_run_path = bronze_processor.run_path
        
        # Process Silver layer if not bronze_only
        if not args.bronze_only:
            # Initialize Silver processor
            silver_processor = SilverProcessor(
                settings=settings,
                storage_manager=storage_manager,
                metadata_manager=metadata_manager,
            )
            
            # If no bronze_run_path (silver_only mode), find the latest bronze run
            if args.silver_only and not bronze_run_path:
                # Find the latest bronze run directory
                bronze_runs = sorted(
                    Path(settings.bronze_path).glob("*"),
                    key=lambda p: p.stat().st_mtime if p.is_dir() else 0,
                    reverse=True
                )
                
                if not bronze_runs:
                    logger.error("No Bronze runs found for Silver processing")
                    return 1
                
                bronze_run_path = bronze_runs[0]
                logger.info(
                    f"Using latest Bronze run for Silver processing: {bronze_run_path}"
                )
            
            # Process Bronze files to Silver
            if bronze_run_path:
                logger.info(
                    f"Processing Bronze files to Silver layer from: {bronze_run_path}"
                )
                silver_processor.process_bronze_files(
                    bronze_run_path=bronze_run_path,
                    specific_subfolders=subfolders,
                    supported_file_types=file_types,
                )
            else:
                logger.error("No Bronze run path available for Silver processing")
        
        logger.info("Google Drive Data Pipeline completed successfully")
        return 0
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 