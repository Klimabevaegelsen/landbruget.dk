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
        self.storage_manager = storage_manager
        self.metadata_manager = MetadataManager(settings.bronze_path)
        
        # Generate a timestamp for this run
        self.run_timestamp = generate_timestamp()
        self.run_path = settings.get_bronze_path_for_run(self.run_timestamp)
        
        # Ensure the run directory exists
        self.storage_manager.ensure_directory_exists(self.run_path)
        
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
        current_path: Optional[Path] = None,
    ) -> int:
        """Process a folder and its contents.

        Args:
            folder: DriveFolder to process
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)
            current_path: Current path within the run directory (optional)

        Returns:
            Number of files processed
        """
        # Determine the current path
        if current_path is None:
            current_path = self.run_path
        else:
            current_path = current_path / folder.name
        
        # Create the folder
        self.storage_manager.ensure_directory_exists(current_path)
        
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
            if self._process_file(file, current_path, folder.name):
                processed_count += 1
        
        # Process subfolders
        if specific_subfolders:
            # Process only specific subfolders
            for subfolder in folder.subfolders:
                if subfolder.name in specific_subfolders:
                    processed_count += self._process_folder(
                        subfolder, None, supported_file_types, current_path
                    )
        else:
            # Process all subfolders
            for subfolder in folder.subfolders:
                processed_count += self._process_folder(
                    subfolder, None, supported_file_types, current_path
                )
        
        return processed_count

    def _process_file(
        self, file: DriveFile, folder_path: Path, folder_name: str
    ) -> bool:
        """Process a file.

        Args:
            file: DriveFile to process
            folder_path: Path to the folder where the file should be saved
            folder_name: Name of the folder containing the file

        Returns:
            True if the file was processed successfully, False otherwise
        """
        try:
            set_context(file_id=file.id, file_name=file.name)
            logger.info(f"Processing file: {file.name} (ID: {file.id})")
            
            # Determine the target path
            target_path = folder_path / file.name
            
            # Download the file
            file_content, metadata = self.drive_fetcher.download_file(file.id)
            
            # Save the file
            self.storage_manager.save_file(file_content, target_path)
            
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
            
            self.metadata_manager.save_metadata(file_metadata, target_path)
            
            logger.info(f"Successfully processed file: {file.name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to process file {file.name} (ID: {file.id}): {str(e)}")
            return False 