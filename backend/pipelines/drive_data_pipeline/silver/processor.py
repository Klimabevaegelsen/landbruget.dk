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
from .validators.pii_validator import PIIAction, PIIType, PIIValidator

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
        self.storage_manager = storage_manager
        self.progress_callback = progress_callback

        # Initialize Silver-specific storage manager
        self.silver_storage = SilverStorageManager(
            storage_manager=storage_manager,
            base_path=settings.silver_path,
        )

        self.metadata_manager = metadata_manager

        # Initialize specialized managers
        self.parquet_manager = ParquetManager(
            storage_manager=storage_manager,
            compression="snappy",
            partition_by=["source_subfolder"],
        )

        # Initialize schema manager if schema_dir is provided
        self.schema_manager = SchemaManager(schema_dir=schema_dir)
        
        # Store the silver run path for later access
        self.silver_run_path = None

        # Initialize schema adapter
        self.schema_adapter = SchemaAdapter()

        # Initialize PII validator (excluding CVR from masking)
        self.pii_validator = PIIValidator(
            pii_types={
                PIIType.EMAIL,
                PIIType.PHONE,
                PIIType.CPR,
                # PIIType.CVR,  # Excluded - CVRs should not be masked
                PIIType.CREDIT_CARD,
                PIIType.IP_ADDRESS,
            },
            action=PIIAction.MASK,
            threshold=0.3,
        )

        # Import transformers here to avoid circular imports
        from .transformers.advanced_pdf_transformer import AdvancedPDFTransformer
        from .transformers.excel_transformer import ExcelTransformer
        from .transformers.work_permits_transformer import WorkPermitsTransformer

        # Initialize transformers map
        self.transformers = {
            "Excel": ExcelTransformer(),
            "PDF": AdvancedPDFTransformer(
                use_ocr=self.settings.enable_ocr if hasattr(self.settings, "enable_ocr") else False,
                ocr_language="dan+eng",
            ),
            "WorkPermits": WorkPermitsTransformer(),
        }

        logger.info("Initialized Silver processor")

    def process_from_memory(
        self,
        bronze_data: dict,
        specific_subfolders: list[str] | None = None,
        supported_file_types: set[str] | None = None,
        apply_schemas: bool = True,
        handle_pii: bool = True,
    ) -> int:
        """Process files from in-memory Bronze data.

        Args:
            bronze_data: Bronze data dict with file data and metadata
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
            logger.info("Processing Bronze data from memory - skipping disk I/O")

            # Extract data from bronze_data structure
            file_data = bronze_data.get("data", {})
            bronze_metadata = bronze_data.get("metadata", {})

            logger.info(f"Found {len(file_data)} files in Bronze data")

            # Create a new run directory in the Silver layer
            silver_run_path = self.silver_storage.create_run_directory()
            self.silver_run_path = silver_run_path  # Store for later access
            processed_count = 0

            # Process each file from memory
            for file_key, file_info in file_data.items():
                # Apply filters
                file_metadata_dict = file_info.get("metadata", {})

                # Filter by file type if specified
                if supported_file_types:
                    file_extension = Path(file_info["original_filename"]).suffix.lstrip(".")
                    if file_extension not in supported_file_types:
                        logger.debug(
                            f"Skipping unsupported file type: {file_info['original_filename']}"
                        )
                        continue

                # Filter by subfolder if specified
                if specific_subfolders and file_info.get("folder_name") not in specific_subfolders:
                    logger.debug(
                        f"Skipping file from unspecified subfolder: {file_info.get('folder_name')}"
                    )
                    continue

                # Process the file from memory
                success = self._process_file_from_memory(
                    file_info,
                    silver_run_path,
                    apply_schemas,
                    handle_pii,
                )

                # Update progress tracking if callback is provided
                if self.progress_callback:
                    self.progress_callback(1, success)

                if success:
                    processed_count += 1

            logger.info(
                f"Successfully processed {processed_count} files from memory to Silver layer"
            )
            return processed_count

        except Exception as e:
            logger.error(f"Failed to process Bronze data from memory: {str(e)}")
            raise

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
            self.silver_run_path = silver_run_path  # Store for later access
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

        # FIXED: Use storage manager to list files instead of Path.glob() for GCS compatibility
        try:
            # List all files in the bronze run directory using storage manager
            all_files = self.storage_manager.list_files(bronze_run_path, pattern="*.metadata.json")

            # Also check subdirectories for metadata files
            # For GCS, we need to list files recursively
            if self.storage_manager.storage_type.lower() == "gcs":
                # GCS storage - list with recursive prefix
                prefix = str(bronze_run_path).rstrip("/") + "/"
                if hasattr(self.storage_manager, "gcs_bucket") and self.storage_manager.gcs_bucket:
                    blobs = self.storage_manager.gcs_bucket.list_blobs(prefix=prefix)
                    for blob in blobs:
                        if blob.name.endswith(".metadata.json"):
                            all_files.append(Path(blob.name))
            else:
                # Local storage - use recursive glob through storage manager
                import os

                for root, dirs, files in os.walk(self.storage_manager.base_dir / bronze_run_path):
                    for file in files:
                        if file.endswith(".metadata.json"):
                            file_path = Path(root) / file
                            # Convert to relative path from storage base
                            relative_path = file_path.relative_to(
                                Path(self.storage_manager.base_dir)
                            )
                            all_files.append(relative_path)

        except Exception as e:
            logger.error(f"Failed to list files in Bronze directory {bronze_run_path}: {str(e)}")
            return bronze_files

        # Process each metadata file
        for metadata_path in all_files:
            if not str(metadata_path).endswith(".metadata.json"):
                continue

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

                # Validate the file exists using storage manager
                if self.storage_manager.file_exists(file_path):
                    bronze_files.append((file_path, metadata_path))
                else:
                    logger.warning(f"File does not exist: {file_path}")

            except Exception as e:
                logger.warning(f"Error processing metadata {metadata_path}: {str(e)}")

        logger.info(f"Found {len(bronze_files)} Bronze files to process")
        return bronze_files

    def _process_file_from_memory(
        self,
        file_info: dict,
        silver_run_path: Path,
        apply_schemas: bool = True,
        handle_pii: bool = True,
    ) -> bool:
        """Process a single file from in-memory data.

        Args:
            file_info: Dict containing file content and metadata
            silver_run_path: Path to the Silver layer run directory
            apply_schemas: Whether to apply schemas to the data
            handle_pii: Whether to detect and handle PII

        Returns:
            True if the file was processed successfully, False otherwise
        """
        try:
            # Extract file information
            file_content = file_info["content"]
            metadata_dict = file_info["metadata"]
            original_filename = file_info["original_filename"]
            mime_type = file_info.get("mime_type", "")

            # Convert metadata dict to FileMetadata object
            from ..bronze.metadata import FileMetadata

            metadata = FileMetadata(**metadata_dict)

            set_context(
                file_id=metadata.file_id,
                file_name=original_filename,
            )

            logger.info(f"Processing file from memory to Silver: {original_filename}")

            # Use content type from metadata (same approach as _process_file method)
            content_type = metadata.content_type

            if not content_type or content_type not in self.transformers:
                logger.warning(f"Unsupported content type: {content_type} for {original_filename}")
                return False

            transformer = self.transformers[content_type]

            # Transform the file content directly from memory
            try:
                transformed_data = transformer.transform_from_content(
                    file_content, original_filename, metadata_dict
                )
            except Exception as e:
                logger.error(f"Failed to transform {original_filename}: {str(e)}")
                return False

            # Check if we have valid transformed data
            if transformed_data is None:
                logger.warning(f"No data extracted from {original_filename}")
                return False

            # Handle different return types from transformers
            if isinstance(transformed_data, dict):
                # Multiple DataFrames (e.g., multi-sheet Excel)
                if not transformed_data:
                    logger.warning(f"No data extracted from {original_filename}")
                    return False

                # Save each DataFrame
                saved_files = []
                for sheet_name, df in transformed_data.items():
                    if df is not None and not df.empty:
                        # Create output directory for the subfolder
                        output_dir = self.silver_storage.create_output_directory(
                            silver_run_path, metadata.original_subfolder
                        )
                        # Use original filename with sheet name for multi-sheet files
                        output_filename = f"{Path(original_filename).stem}_{sheet_name}.parquet"
                        output_path = output_dir / output_filename

                        try:
                            self.parquet_manager.save_dataframe_to_parquet(df, output_path)
                            saved_files.append(output_path)
                            logger.info(f"Saved transformed data to: {output_path}")
                        except Exception as e:
                            logger.error(
                                f"Failed to save transformed data for {original_filename} sheet {sheet_name}: {str(e)}"
                            )
                            return False

                if not saved_files:
                    logger.warning(f"No valid data saved from {original_filename}")
                    return False

                # Use the first saved file for schema/PII processing
                output_path = saved_files[0]

            elif isinstance(transformed_data, list):
                # List of DataFrames
                if not transformed_data:
                    logger.warning(f"No data extracted from {original_filename}")
                    return False

                # Save each DataFrame
                saved_files = []
                for i, df in enumerate(transformed_data):
                    if df is not None and not df.empty:
                        # Create output directory for the subfolder
                        output_dir = self.silver_storage.create_output_directory(
                            silver_run_path, metadata.original_subfolder
                        )
                        # Use original filename with part number for multi-part files
                        output_filename = f"{Path(original_filename).stem}_{i}.parquet"
                        output_path = output_dir / output_filename

                        try:
                            self.parquet_manager.save_dataframe_to_parquet(df, output_path)
                            saved_files.append(output_path)
                            logger.info(f"Saved transformed data to: {output_path}")
                        except Exception as e:
                            logger.error(
                                f"Failed to save transformed data for {original_filename} part {i}: {str(e)}"
                            )
                            return False

                if not saved_files:
                    logger.warning(f"No valid data saved from {original_filename}")
                    return False

                # Use the first saved file for schema/PII processing
                output_path = saved_files[0]

            else:
                # Single result - could be DataFrame, table name, or other data

                # Check if it's a DuckDB table name (string)
                if isinstance(transformed_data, str):
                    # It's a DuckDB table name - use it directly
                    table_name = transformed_data

                    # Create output directory for the subfolder
                    output_dir = self.silver_storage.create_output_directory(
                        silver_run_path, metadata.original_subfolder
                    )
                    # Use original filename for single files
                    output_filename = f"{Path(original_filename).stem}.parquet"
                    output_path = output_dir / output_filename

                    # Save the DuckDB table directly
                    try:
                        # Use the transformer's connection to save the table
                        transformer.save_table_to_parquet(table_name, output_path)
                        logger.info(f"Saved transformed data to: {output_path}")
                    except Exception as e:
                        logger.error(
                            f"Failed to save transformed data for {original_filename}: {str(e)}"
                        )
                        return False

                elif transformed_data is None or (
                    hasattr(transformed_data, "empty") and transformed_data.empty
                ):
                    logger.warning(f"No valid data extracted from {original_filename}")
                    return False
                else:
                    # It's a DataFrame or other data
                    # Create output directory for the subfolder
                    output_dir = self.silver_storage.create_output_directory(
                        silver_run_path, metadata.original_subfolder
                    )
                    # Use original filename for single files
                    output_filename = f"{Path(original_filename).stem}.parquet"
                    output_path = output_dir / output_filename

                    # Save the transformed data
                    try:
                        self.parquet_manager.save_dataframe_to_parquet(
                            transformed_data, output_path
                        )
                        logger.info(f"Saved transformed data to: {output_path}")
                    except Exception as e:
                        logger.error(
                            f"Failed to save transformed data for {original_filename}: {str(e)}"
                        )
                        return False

            # Apply schema if requested
            if apply_schemas:
                schema_output_path = self._apply_schema_to_file(
                    output_path, metadata, silver_run_path
                )
                if schema_output_path:
                    output_path = schema_output_path

            # Handle PII if requested
            if handle_pii:
                pii_output_path = self._handle_pii_in_file(output_path, silver_run_path)
                if pii_output_path:
                    output_path = pii_output_path

            logger.info(f"Successfully processed file from memory: {original_filename}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to process file from memory {file_info.get('original_filename', 'unknown')}: {str(e)}"
            )
            return False

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

            # Select transformer based on content type and file specifics
            transformer = None
            
            # First, check if specialized transformers can handle this file
            for transformer_name, potential_transformer in self.transformers.items():
                if hasattr(potential_transformer, 'can_handle'):
                    # Convert metadata to dict for transformer
                    metadata_dict = metadata.dict() if hasattr(metadata, 'dict') else metadata.__dict__
                    if potential_transformer.can_handle(file_path, metadata_dict):
                        transformer = potential_transformer
                        logger.info(f"Using specialized transformer: {transformer_name}")
                        break
            
            # If no specialized transformer found, use content type mapping
            if not transformer:
                if not metadata.content_type or metadata.content_type not in self.transformers:
                    logger.warning(f"Unsupported content type: {metadata.content_type}")
                    return False
                transformer = self.transformers[metadata.content_type]
                logger.info(f"Using content type transformer: {metadata.content_type}")
            
            if not transformer:
                logger.error("No suitable transformer found")
                return False

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

            # ✅ MIGRATION: Read parquet file using DuckDB instead of pandas
            import duckdb

            # Use DuckDB to read parquet file
            temp_conn = duckdb.connect()
            df = temp_conn.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            temp_conn.close()

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
            # ✅ MIGRATION: Read parquet file using DuckDB instead of pandas
            import duckdb

            # Use DuckDB to read parquet file
            temp_conn = duckdb.connect()
            df = temp_conn.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            temp_conn.close()

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
