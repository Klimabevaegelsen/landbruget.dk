"""Bronze layer processor for Google Drive data pipeline."""

import datetime
from pathlib import Path
from typing import List, Optional, Set, Union

from ..config.settings import Settings
from ..utils.helpers import generate_timestamp
from ..utils.logging import get_logger, set_context
from ..utils.storage import StorageManager
from .drive import GoogleDriveFetcher, DriveFile, DriveFolder
from .metadata import MetadataManager, FileMetadata
from .storage import BronzeStorageManager

# Get logger
logger = get_logger()


class BronzeProcessor:
    """Processor for the Bronze layer."""

    def __init__(
        self,
        settings: Settings,
        drive_fetcher: GoogleDriveFetcher,
        storage_manager: StorageManager,
    ):
        """Initialize the Bronze processor.

        Args:
            settings: Application settings
            drive_fetcher: Fetcher for Google Drive files
            storage_manager: Storage manager for file operations
        """
        self.settings = settings
        self.drive_fetcher = drive_fetcher
        
        # Initialize Bronze-specific storage manager
        self.bronze_storage = BronzeStorageManager(
            storage_manager=storage_manager,
            base_path=settings.bronze_path,
        )
        
        # Initialize metadata manager
        self.metadata_manager = MetadataManager(settings.bronze_path)
        
        # Generate a timestamp for this run and create run directory
        self.run_timestamp = generate_timestamp()
        self.run_path = self.bronze_storage.create_run_directory(self.run_timestamp)
        
        logger.info(f"Initialized Bronze processor with run timestamp: {self.run_timestamp}")

    def process_drive_folder(
        self,
        folder_id: str,
        specific_subfolders: Optional[List[str]] = None,
        supported_file_types: Optional[Set[str]] = None,
    ) -> int:
        """Process files from a Google Drive folder.

        Args:
            folder_id: ID of the Google Drive folder to process
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)

        Returns:
            Number of files processed

        Raises:
            Exception: If the processing fails
        """
        try:
            set_context(folder_id=folder_id, run_timestamp=self.run_timestamp)
            logger.info(f"Processing Google Drive folder: {folder_id}")
            
            # List folder contents
            root_folder = self.drive_fetcher.list_folder_contents(folder_id)
            
            # Process the folder
            processed_count = self._process_folder(
                root_folder, specific_subfolders, supported_file_types
            )
            
            logger.info(f"Successfully processed {processed_count} files from folder {folder_id}")
            return processed_count
        
        except Exception as e:
            logger.error(f"Failed to process folder {folder_id}: {str(e)}")
            raise

    def _process_folder(
        self,
        folder: DriveFolder,
        specific_subfolders: Optional[List[str]] = None,
        supported_file_types: Optional[Set[str]] = None,
    ) -> int:
        """Process a folder and its contents.

        Args:
            folder: DriveFolder to process
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)

        Returns:
            Number of files processed
        """
        processed_count = 0
        
        # Process files in the folder
        for file in folder.files:
            # Check if the file type is supported
            if supported_file_types:
                extension = Path(file.name).suffix.lower().lstrip(".")
                if extension not in supported_file_types:
                    logger.debug(f"Skipping unsupported file type: {file.name}")
                    continue
            
            # Download and save the file
            if self._process_file(file, folder.path, folder.name):
                processed_count += 1
        
        # Process subfolders
        if specific_subfolders:
            # Process only specific subfolders
            for subfolder in folder.subfolders:
                if subfolder.name in specific_subfolders:
                    processed_count += self._process_folder(
                        subfolder, None, supported_file_types
                    )
        else:
            # Process all subfolders
            for subfolder in folder.subfolders:
                processed_count += self._process_folder(
                    subfolder, None, supported_file_types
                )
        
        return processed_count

    def _process_file(
        self, file: DriveFile, folder_path: str, folder_name: str
    ) -> bool:
        """Process a file.

        Args:
            file: DriveFile to process
            folder_path: Path of the file in the source (e.g., Google Drive)
            folder_name: Name of the folder containing the file

        Returns:
            True if the file was processed successfully, False otherwise
        """
        try:
            set_context(file_id=file.id, file_name=file.name)
            logger.info(f"Processing file: {file.name} (ID: {file.id})")
            
            # Check if file already exists in this run
            if self.bronze_storage.file_exists(self.run_path, folder_path, file.name):
                logger.info(f"File {file.name} already exists in this run, skipping")
                return True
            
            # Download the file
            file_content, metadata = self.drive_fetcher.download_file(file.id)
            
            # Save the file
            target_path = self.bronze_storage.save_file(
                content=file_content,
                run_dir=self.run_path,
                source_path=folder_path,
                filename=file.name,
            )
            
            # Generate and save metadata
            file_metadata = self.metadata_manager.generate_metadata(
                file_path=target_path,
                file_id=file.id,
                original_filename=file.name,
                original_subfolder=folder_name,
                mime_type=file.mime_type,
                file_size=len(file_content),
                modified_time=file.modified_time,
                drive_path=file.path,
            )
            
            self.bronze_storage.save_metadata(
                metadata=file_metadata.model_dump(),
                file_path=target_path,
            )
            
            logger.info(f"Successfully processed file: {file.name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to process file {file.name} (ID: {file.id}): {str(e)}")
            return False 