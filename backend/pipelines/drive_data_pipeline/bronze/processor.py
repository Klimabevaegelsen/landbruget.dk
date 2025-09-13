"""Bronze layer processor for Google Drive data pipeline."""

import datetime
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

from ..config.settings import Settings
from ..utils.logging import get_logger, set_context
from ..utils.storage import DriveStorageManager
from .drive import DriveFile, DriveFolder, GoogleDriveFetcher
from .metadata import MetadataManager
from .storage import BronzeStorageManager

# Import the new data tracing system
try:
    from backend.common.pipeline_metadata import MetadataManager as PipelineMetadataManager

    PIPELINE_METADATA_AVAILABLE = True
except ImportError:
    print("⚠️  Pipeline metadata system not available - continuing without data tracing")
    PipelineMetadataManager = None
    PIPELINE_METADATA_AVAILABLE = False

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
        pipeline_start_time: datetime.datetime | None = None,
    ) -> None:
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

        # Initialize metadata manager (file-level)
        self.metadata_manager = MetadataManager(settings.bronze_path, storage_manager)

        # Initialize pipeline metadata manager (pipeline-level data tracing)
        if PIPELINE_METADATA_AVAILABLE:
            self.pipeline_metadata_manager = PipelineMetadataManager()
            logger.info("✅ Pipeline metadata system initialized for data tracing")
        else:
            self.pipeline_metadata_manager = None
            logger.warning("⚠️  Pipeline metadata system not available")

        # Use provided pipeline start time or generate one
        if pipeline_start_time is not None:
            self.pipeline_start_time = pipeline_start_time
        else:
            self.pipeline_start_time = datetime.datetime.now()

        # Generate a timestamp for this run and create run directory
        self.run_timestamp = self.pipeline_start_time.strftime("%Y%m%d_%H%M%S")
        self.run_path = self.bronze_storage.create_run_directory(self.run_timestamp)

        # Thread-safe progress tracking
        self.progress_lock = Lock()

        logger.info(f"Initialized Bronze processor with run timestamp: {self.run_timestamp}")
        logger.info(f"Max workers for parallel processing: {settings.max_workers}")

    def process_drive_folder(
        self,
        folder_id: str = None,
        drive_folder: DriveFolder = None,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
        return_data: bool = False,
    ) -> int | dict:
        """Process files from a Google Drive folder.

        Args:
            folder_id: ID of the Google Drive folder to process (deprecated, use drive_folder)
            drive_folder: Already fetched DriveFolder object (preferred)
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)
            return_data: If True, return processed data in memory for Silver layer

        Returns:
            Number of files processed, or dict with data if return_data=True

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

            # Initialize data collection for in-memory processing
            in_memory_data = {} if return_data else None

            # Track processing start time for pipeline metadata
            processing_start_time = time.time()

            # Process the folder
            processed_count = self._process_folder(
                drive_folder, specific_subfolders, supported_file_types, in_memory_data
            )

            # Calculate processing duration
            processing_duration = time.time() - processing_start_time

            logger.info(
                f"Successfully processed {processed_count} files from folder "
                f"{drive_folder.id} in {processing_duration:.1f}s"
            )

            # Create and save pipeline metadata for data tracing
            if self.pipeline_metadata_manager:
                try:
                    # Calculate total file size processed
                    total_file_size = 0
                    if in_memory_data:
                        total_file_size = sum(
                            item.get("file_size", 0) for item in in_memory_data.values()
                        )

                    # Create pipeline metadata
                    pipeline_metadata = self.pipeline_metadata_manager.create_metadata(
                        source_key="drive_pesticide_reports",  # From DATA_SOURCE_REGISTRY
                        record_count=processed_count,
                        processing_duration=processing_duration,
                        file_size_bytes=total_file_size,
                    )

                    # Save pipeline metadata alongside the run
                    pipeline_metadata_path = self.pipeline_metadata_manager.save_metadata(
                        pipeline_metadata, self.run_path / "pipeline_metadata.json"
                    )

                    logger.info(f"✅ Pipeline metadata saved to {pipeline_metadata_path}")

                except Exception as e:
                    logger.error(f"❌ Failed to create pipeline metadata: {e}")

            if return_data:
                return {
                    "data": in_memory_data,
                    "metadata": {
                        "run_timestamp": self.run_timestamp,
                        "run_path": str(self.run_path),
                        "processed_count": processed_count,
                        "folder_id": drive_folder.id,
                    },
                }
            else:
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
        in_memory_data: dict | None = None,
    ) -> int:
        """Process a folder and its contents.

        Args:
            folder: DriveFolder to process
            specific_subfolders: List of specific subfolder names to process (optional)
            supported_file_types: Set of supported file extensions (optional)
            in_memory_data: Dict to collect file data for in-memory processing (optional)

        Returns:
            Number of files processed
        """
        processed_count = 0

        # Filter files by supported types first
        files_to_process = []
        for file in folder.files:
            if supported_file_types:
                extension = Path(file.name).suffix.lower().lstrip(".")
                if extension not in supported_file_types:
                    logger.debug(f"Skipping unsupported file type: {file.name}")
                    continue
            files_to_process.append(file)

        # TEMPORARILY DISABLE parallel processing due to Google Drive API SSL issues
        # Process files sequentially to ensure stability
        logger.info(
            f"Processing {len(files_to_process)} files sequentially "
            f"(parallel disabled for API stability)"
        )

        for file in files_to_process:
            success = self._process_file(file, folder.path, folder.name, in_memory_data)
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
                    processed_count += self._process_folder(
                        subfolder, None, supported_file_types, in_memory_data
                    )
        else:
            # Process all subfolders
            for subfolder in folder.subfolders:
                processed_count += self._process_folder(
                    subfolder, None, supported_file_types, in_memory_data
                )

        return processed_count

    def _process_files_parallel(
        self,
        files: list[DriveFile],
        folder_path: str,
        folder_name: str,
        in_memory_data: dict | None = None,
    ) -> int:
        """Process multiple files in parallel.

        Args:
            files: List of DriveFile objects to process
            folder_path: Path of the folder containing the files
            folder_name: Name of the folder containing the files
            in_memory_data: Dict to collect file data for in-memory processing (optional)

        Returns:
            Number of files processed successfully
        """
        processed_count = 0

        def process_single_file(file: DriveFile) -> tuple[bool, int]:
            """Process a single file and return success status and file size."""
            import random
            import time

            try:
                # Add small random delay to stagger API calls and reduce SSL connection pressure
                delay = random.uniform(0.5, 2.0)  # 0.5-2 second delay
                time.sleep(delay)

                success = self._process_file(file, folder_path, folder_name, in_memory_data)
                file_size = int(file.size) if hasattr(file, "size") and file.size else 0
                return success, file_size
            except Exception as e:
                logger.error(f"Error processing file {file.name}: {e}")
                return False, 0

        # Process files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.settings.max_workers) as executor:
            # Submit all file processing tasks
            future_to_file = {executor.submit(process_single_file, file): file for file in files}

            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    success, file_size = future.result()
                    if success:
                        processed_count += 1

                    # Thread-safe progress update
                    if self.progress_callback:
                        with self.progress_lock:
                            self.progress_callback(1, success, file_size)

                except Exception as e:
                    logger.error(f"Error in parallel processing of {file.name}: {e}")
                    # Still update progress for failed files
                    if self.progress_callback:
                        with self.progress_lock:
                            self.progress_callback(1, False, 0)

        logger.info(
            f"Parallel processing completed: {processed_count}/{len(files)} files successful"
        )
        return processed_count

    def _process_file(
        self,
        file: DriveFile,
        folder_path: str,
        folder_name: str,
        in_memory_data: dict | None = None,
    ) -> bool:
        """Process a file.

        Args:
            file: DriveFile to process
            folder_path: Path of the file in the source (e.g., Google Drive)
            folder_name: Name of the folder containing the file
            in_memory_data: Dict to collect file data for in-memory processing (optional)

        Returns:
            True if the file was processed successfully, False otherwise
        """
        try:
            set_context(file_id=file.id, file_name=file.name)
            logger.info(f"Processing file: {file.name} (ID: {file.id})")

            # Use the file's full path if available, otherwise fall back to folder path
            # The file.path includes the full Google Drive hierarchy
            file_source_path = file.path if hasattr(file, "path") and file.path else folder_path
            # Extract the directory part of the file path (remove the filename)
            if file_source_path and "/" in file_source_path:
                source_folder_path = "/".join(file_source_path.split("/")[:-1])
            else:
                source_folder_path = folder_path

            # Check if file already exists in this run
            if self.bronze_storage.file_exists(self.run_path, source_folder_path, file.name):
                logger.info(f"File {file.name} already exists in this run, skipping")
                return True

            # Download the file
            file_content, metadata = self.drive_fetcher.download_file(file.id)
            logger.info(f"Downloaded {len(file_content)} bytes for file {file.name}")

            # Save the file using the full Google Drive path hierarchy
            target_path = self.bronze_storage.save_file(
                content=file_content,
                run_dir=self.run_path,
                source_path=source_folder_path,
                filename=file.name,
            )

            # Verify file was saved correctly - FIX: Use storage manager for verification
            exists_check = self.bronze_storage.storage_manager.file_exists(target_path)

            if not exists_check:
                logger.error(
                    f"File was not saved correctly: {target_path} does not exist in storage backend"
                )
                return False

            # For local storage, we can also verify file size
            if self.bronze_storage.storage_manager.storage_type.lower() == "local":
                # Local storage - can check size
                try:
                    full_path = self.bronze_storage.storage_manager.base_dir / target_path
                    if full_path.exists():
                        saved_size = full_path.stat().st_size
                        if saved_size != len(file_content):
                            logger.warning(
                                f"File size mismatch for {file.name}: "
                                f"expected {len(file_content)}, got {saved_size}"
                            )
                except Exception as e:
                    logger.warning(f"Could not verify file size locally: {e}")

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

            # Collect data for in-memory processing if requested
            if in_memory_data is not None:
                file_key = f"{source_folder_path}/{file.name}".replace("//", "/")
                in_memory_data[file_key] = {
                    "content": file_content,
                    "metadata": file_metadata.model_dump(),
                    "file_path": str(target_path),
                    "original_filename": file.name,
                    "folder_name": folder_name,
                    "mime_type": file.mime_type,
                    "file_size": len(file_content),
                }

            logger.info(f"Successfully processed file: {file.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to process file {file.name} (ID: {file.id}): {str(e)}")
            return False
