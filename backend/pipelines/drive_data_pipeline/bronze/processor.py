"""Bronze layer processor for Google Drive data pipeline."""

from collections.abc import Callable
from pathlib import Path

from ..config.settings import Settings
from ..utils.helpers import generate_timestamp
from ..utils.logging import get_logger, set_context
from ..utils.storage import DriveStorageManager
from .drive import DriveFile, DriveFolder, GoogleDriveFetcher
from .metadata import MetadataManager
from .storage import BronzeStorageManager

# Get logger
logger = get_logger()


class BronzeProcessor:
    """Processor for the Bronze layer."""

    def __init__(
        self,
        settings: Settings,
        drive_fetcher: GoogleDriveFetcher,
        storage_manager: DriveStorageManager,
        progress_callback: Callable[[int, bool, int], None] | None = None,
    ):
        """Initialize the Bronze processor.

        Args:
            settings: Application settings
            drive_fetcher: Fetcher for Google Drive files
            storage_manager: Storage manager for file operations
            progress_callback: Optional callback function for progress tracking
        """
        self.settings = settings
        self.drive_fetcher = drive_fetcher
        self.progress_callback = progress_callback

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
        folder_id: str = None,
        drive_folder: DriveFolder = None,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
    ) -> int:
        """Process files from a Google Drive folder.

        Args:
            folder_id: ID of the Google Drive folder to process (deprecated, use drive_folder)
            drive_folder: Already fetched DriveFolder object (preferred)
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)

        Returns:
            Number of files processed

        Raises:
            Exception: If the processing fails
        """
        try:
            # Support both old and new calling patterns for backward compatibility
            if drive_folder is None and folder_id is not None:
                logger.warning(
                    "Using deprecated folder_id parameter. Consider passing drive_folder instead."
                )
                set_context(folder_id=folder_id, run_timestamp=self.run_timestamp)
                logger.info(f"Processing Google Drive folder: {folder_id}")
                # Get folder contents (this is the old inefficient way)
                drive_folder = self.drive_fetcher.list_folder_contents(
                    folder_id=folder_id, recursive=True
                )
            elif drive_folder is not None:
                set_context(folder_id=drive_folder.id, run_timestamp=self.run_timestamp)
                logger.info(f"Processing Google Drive folder: {drive_folder.id}")
            else:
                raise ValueError("Either folder_id or drive_folder must be provided")

            # Process the folder
            processed_count = self._process_folder(
                drive_folder, specific_subfolders, supported_file_types
            )

            logger.info(
                f"Successfully processed {processed_count} files from folder {drive_folder.id}"
            )
            return processed_count

        except Exception as e:
            folder_ref = drive_folder.id if drive_folder else folder_id
            logger.error(f"Failed to process folder {folder_ref}: {str(e)}")
            raise

    def _process_folder(
        self,
        folder: DriveFolder,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
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
            success = self._process_file(file, folder.path, folder.name)
            processed_count += 1 if success else 0

            # Update progress
            if self.progress_callback:
                file_size = int(file.size) if hasattr(file, "size") and file.size else 0
                self.progress_callback(1, success, file_size)

        # Process subfolders
        if specific_subfolders:
            # Process only specific subfolders
            for subfolder in folder.subfolders:
                if subfolder.name in specific_subfolders:
                    processed_count += self._process_folder(subfolder, None, supported_file_types)
        else:
            # Process all subfolders
            for subfolder in folder.subfolders:
                processed_count += self._process_folder(subfolder, None, supported_file_types)

        return processed_count

    def _process_file(self, file: DriveFile, folder_path: str, folder_name: str) -> bool:
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
            logger.info(f"Downloaded {len(file_content)} bytes for file {file.name}")

            # Save the file
            target_path = self.bronze_storage.save_file(
                content=file_content,
                run_dir=self.run_path,
                source_path=folder_path,
                filename=file.name,
            )
            logger.info(f"Saved file to path: {target_path}")
            logger.info(f"Target path type: {type(target_path)}")
            logger.info(f"Target path absolute: {target_path.absolute()}")
            logger.info(f"Working directory: {Path.cwd()}")

            # Verify file was saved correctly - EXTENSIVE DEBUGGING
            logger.info(f"Checking if file exists at: {target_path}")
            exists_check = target_path.exists()
            logger.info(f"File exists result: {exists_check}")

            if not exists_check:
                # Additional debugging to understand path issues
                logger.error(f"File was not saved correctly: {target_path} does not exist")
                logger.info(f"Parent directory exists: {target_path.parent.exists()}")
                logger.info(
                    f"Parent directory contents: {list(target_path.parent.iterdir()) if target_path.parent.exists() else 'Directory does not exist'}"
                )
                logger.info(f"Current working directory: {Path.cwd()}")
                logger.info(f"Target path parts: {target_path.parts}")
                logger.info(
                    f"Target path relative to cwd: {target_path.relative_to(Path.cwd()) if target_path.is_absolute() else 'Not absolute'}"
                )
                return False

            saved_size = target_path.stat().st_size
            if saved_size != len(file_content):
                logger.warning(
                    f"File size mismatch for {file.name}: expected {len(file_content)}, got {saved_size}"
                )
            else:
                logger.info(f"File size verified: {saved_size} bytes")

            # Generate and save metadata - FIXED: Pass content for checksum calculation
            file_metadata = self.metadata_manager.generate_metadata(
                file_path=target_path,
                file_content=file_content,  # Add content for more reliable checksum
                file_id=file.id,
                original_filename=file.name,
                original_subfolder=folder_name,
                mime_type=file.mime_type,
                file_size=len(file_content),
                modified_time=file.modified_time,
                drive_path=file.path,
            )
            logger.debug(f"Generated metadata with checksum: {file_metadata.checksum[:8]}...")

            self.bronze_storage.save_metadata(
                metadata=file_metadata.model_dump(),
                file_path=target_path,
            )

            logger.info(f"Successfully processed file: {file.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to process file {file.name} (ID: {file.id}): {str(e)}")
            return False
