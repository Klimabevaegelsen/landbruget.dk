"""Silver layer processor for Google Drive data pipeline."""

from collections.abc import Callable
from pathlib import Path

from ..bronze.metadata import FileMetadata, MetadataManager
from ..config.settings import Settings
from ..utils.logging import get_logger, set_context
from ..utils.storage import DriveStorageManager
from .models.schema_adapter import SchemaAdapter
from .parquet_manager import ParquetManager
from .schema_manager import SchemaManager
from .storage import SilverStorageManager
from .validators.pii_validator import PIIAction, PIIValidator

# Get logger
logger = get_logger()


class SilverProcessor:
    """Processor for the Silver layer."""

    def __init__(
        self,
        settings: Settings,
        storage_manager: DriveStorageManager,
        metadata_manager: MetadataManager,
        schema_dir: Path | None = None,
        progress_callback: Callable[[int, bool], None] | None = None,
    ):
        """Initialize the Silver processor.

        Args:
            settings: Application settings
            storage_manager: Storage manager for file operations
            metadata_manager: Metadata manager from Bronze layer
            schema_dir: Directory containing schema definitions (optional)
            progress_callback: Optional callback function for progress tracking
        """
        self.settings = settings
        self.progress_callback = progress_callback

        # Initialize Silver-specific storage manager
        self.silver_storage = SilverStorageManager(
            storage_manager=storage_manager,
            base_path=settings.silver_path,
        )

        self.metadata_manager = metadata_manager

        # Initialize specialized managers
        self.parquet_manager = ParquetManager(
            compression="snappy",
            partition_by=["source_subfolder"],
        )

        # Initialize schema manager if schema_dir is provided
        self.schema_manager = SchemaManager(schema_dir=schema_dir)

        # Initialize schema adapter
        self.schema_adapter = SchemaAdapter()

        # Initialize PII validator
        self.pii_validator = PIIValidator(
            action=PIIAction.MASK,
            threshold=0.3,
        )

        # Import transformers here to avoid circular imports
        from .transformers.advanced_pdf_transformer import AdvancedPDFTransformer
        from .transformers.excel_transformer import ExcelTransformer

        # Initialize transformers map
        self.transformers = {
            "Excel": ExcelTransformer(),
            "PDF": AdvancedPDFTransformer(
                use_ocr=self.settings.enable_ocr if hasattr(self.settings, "enable_ocr") else False,
                ocr_language="dan+eng",
            ),
        }

        logger.info("Initialized Silver processor")

    def process_bronze_files(
        self,
        bronze_run_path: Path,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
        apply_schemas: bool = True,
        handle_pii: bool = True,
    ) -> int:
        """Process files from the Bronze layer.

        Args:
            bronze_run_path: Path to the Bronze layer run directory
            specific_subfolders: List of specific subfolder names to process
            supported_file_types: Set of supported file extensions
            apply_schemas: Whether to apply schemas to the data
            handle_pii: Whether to detect and handle PII

        Returns:
            Number of files processed

        Raises:
            Exception: If the processing fails
        """
        try:
            logger.info(f"Processing Bronze files from: {bronze_run_path}")
            set_context(bronze_run_path=str(bronze_run_path))

            # Create a new run directory in the Silver layer
            silver_run_path = self.silver_storage.create_run_directory()
            processed_count = 0

            # List all files in the Bronze run directory
            bronze_files = self._list_bronze_files(
                bronze_run_path, specific_subfolders, supported_file_types
            )

            # Process each file
            for file_path, metadata_path in bronze_files:
                success = self._process_file(
                    file_path,
                    metadata_path,
                    silver_run_path,
                    apply_schemas,
                    handle_pii,
                )

                # Update progress tracking if callback is provided
                if self.progress_callback:
                    self.progress_callback(1, success)

                if success:
                    processed_count += 1

            logger.info(f"Successfully processed {processed_count} files to Silver layer")
            return processed_count

        except Exception as e:
            logger.error(f"Failed to process Bronze files: {str(e)}")
            raise

    def _list_bronze_files(
        self,
        bronze_run_path: Path,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
    ) -> list[tuple[Path, Path]]:
        """List files in the Bronze layer to be processed.

        Args:
            bronze_run_path: Path to the Bronze layer run directory
            specific_subfolders: List of specific subfolder names to process
            supported_file_types: Set of supported file extensions

        Returns:
            List of tuples containing (file_path, metadata_path)
        """
        bronze_files = []

        # Walk through the Bronze run directory
        for metadata_path in bronze_run_path.glob("**/*.metadata.json"):
            try:
                # Read metadata
                metadata = self.metadata_manager.read_metadata(metadata_path)

                # Filter by file type if specified
                if (
                    supported_file_types
                    and metadata.file_extension.lstrip(".") not in supported_file_types
                ):
                    logger.debug(f"Skipping unsupported file type: {metadata.original_filename}")
                    continue

                # Filter by subfolder if specified
                if specific_subfolders and metadata.original_subfolder not in specific_subfolders:
                    logger.debug(
                        f"Skipping file from unspecified subfolder: {metadata.original_subfolder}"
                    )
                    continue

                # Get the corresponding file path
                file_path = metadata_path.with_suffix("").with_suffix(metadata.file_extension)

                # Validate the file exists
                if file_path.exists():
                    bronze_files.append((file_path, metadata_path))
                else:
                    logger.warning(f"File does not exist: {file_path}")

            except Exception as e:
                logger.warning(f"Error processing metadata {metadata_path}: {str(e)}")

        logger.info(f"Found {len(bronze_files)} Bronze files to process")
        return bronze_files

    def _process_file(
        self,
        file_path: Path,
        metadata_path: Path,
        silver_run_path: Path,
        apply_schemas: bool = True,
        handle_pii: bool = True,
    ) -> bool:
        """Process a single file from Bronze to Silver.

        Args:
            file_path: Path to the file in Bronze layer
            metadata_path: Path to the metadata file
            silver_run_path: Path to the Silver layer run directory
            apply_schemas: Whether to apply schemas to the data
            handle_pii: Whether to detect and handle PII

        Returns:
            True if the file was processed successfully, False otherwise
        """
        try:
            # Read metadata
            metadata = self.metadata_manager.read_metadata(metadata_path)
            set_context(
                file_id=metadata.file_id,
                file_name=metadata.original_filename,
            )

            logger.info(f"Processing file to Silver: {metadata.original_filename}")

            # Validate file exists and metadata is consistent
            if not self.metadata_manager.validate_checksum(file_path, metadata):
                logger.error(f"Checksum validation failed for {file_path}")
                return False

            # Select transformer based on content type
            if not metadata.content_type or metadata.content_type not in self.transformers:
                logger.warning(f"Unsupported content type: {metadata.content_type}")
                return False

            transformer = self.transformers[metadata.content_type]

            # Transform the file
            result = transformer.transform(
                file_path=file_path,
                metadata=metadata,
                output_dir=silver_run_path,
            )

            if not result.success:
                logger.error(f"Failed to transform {file_path}: {result.error}")
                return False

            # Apply schema if requested and output path is available
            if apply_schemas and result.output_path:
                self._apply_schema_to_file(result.output_path, metadata, silver_run_path)

            # Handle PII if requested
            if handle_pii and result.output_path:
                self._handle_pii_in_file(result.output_path, silver_run_path)

            logger.info(f"Successfully processed file to Silver: {metadata.original_filename}")
            return True

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            return False

    def _apply_schema_to_file(
        self, output_path: Path, metadata: FileMetadata, silver_run_path: Path
    ) -> Path | None:
        """Apply schema to a processed file.

        Args:
            output_path: Path to the processed file
            metadata: File metadata
            silver_run_path: Silver layer run directory

        Returns:
            Path to the schema-applied file or None if failed
        """
        try:
            # Try to find a schema for this subfolder
            table_schema = self.schema_manager.get_schema_by_subfolder(metadata.original_subfolder)

            if not table_schema:
                logger.info(
                    f"No schema found for {metadata.original_subfolder}, skipping schema application"
                )
                return None

            # Read the parquet file
            import pandas as pd

            df = pd.read_parquet(output_path)

            # Apply the schema
            df_with_schema = self.schema_adapter.apply_schema(
                df=df,
                table_schema=table_schema,
                infer_types=True,
            )

            # Save with schema to a new file
            schema_output_path = output_path.with_name(
                f"{output_path.stem}_schema{output_path.suffix}"
            )

            # Get schema as dict for metadata
            schema_dict = {
                "name": table_schema.name,
                "columns": {col.name: str(col.data_type) for col in table_schema.columns},
            }

            # Save with schema metadata
            self.parquet_manager.save_dataframe_to_parquet(
                df=df_with_schema,
                output_path=schema_output_path,
                schema_metadata=schema_dict,
            )

            logger.info(f"Applied schema to file: {schema_output_path}")
            return schema_output_path

        except Exception as e:
            logger.warning(f"Failed to apply schema to {output_path}: {str(e)}")
            return None

    def _handle_pii_in_file(self, output_path: Path, silver_run_path: Path) -> Path | None:
        """Detect and handle PII in a processed file.

        Args:
            output_path: Path to the processed file
            silver_run_path: Silver layer run directory

        Returns:
            Path to the PII-handled file or None if failed
        """
        try:
            # Read the parquet file
            import pandas as pd

            df = pd.read_parquet(output_path)

            # Validate for PII
            validation_result = self.pii_validator.validate(df)

            # If PII is found, handle it
            if not validation_result.is_valid:
                # Handle PII according to validator's action
                df_handled = self.pii_validator.handle_pii(df, validation_result)

                # Save to new file
                pii_output_path = output_path.with_name(
                    f"{output_path.stem}_pii_handled{output_path.suffix}"
                )

                # Save handled file
                self.parquet_manager.save_dataframe_to_parquet(
                    df=df_handled,
                    output_path=pii_output_path,
                )

                logger.info(f"Handled PII in file: {pii_output_path}")
                return pii_output_path

            # If no PII found or just reporting, return None
            return None

        except Exception as e:
            logger.warning(f"Failed to handle PII in {output_path}: {str(e)}")
            return None
