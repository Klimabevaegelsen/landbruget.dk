"""Silver storage manager for handling data storage operations."""

import re
from pathlib import Path
from typing import Any

# Handle imports for both standalone and package usage
try:
    from ..utils.error_handling import StorageError
    from ..utils.helpers import generate_timestamp
    from ..utils.logging import get_logger
except ImportError:
    # Fallback for standalone usage
    import logging
    import time

    def get_logger() -> logging.Logger:
        return logging.getLogger(__name__)

    def generate_timestamp() -> int:
        return int(time.time())

    class StorageError(Exception):
        pass


from .duckdb_base import DuckDBProcessor

# Get logger
logger = get_logger()


class SilverStorageManager(DuckDBProcessor):
    """Manager for Silver layer storage operations using DuckDB."""

    def __init__(
        self,
        storage_manager: Any,  # DriveStorageManager
        base_path: Path,
    ) -> None:
        """Initialize the Silver storage manager.

        Args:
            storage_manager: Storage manager instance
            base_path: Base path for storage
        """
        super().__init__()
        self.storage_manager = storage_manager
        self.base_path = base_path
        self._current_timestamp = None
        logger.info(f"Initialized SilverStorageManager with base path: {base_path}")

    def create_run_directory(self, timestamp: str | None = None) -> Path:
        """Create a run directory for the current processing run.

        Args:
            timestamp: Optional timestamp string

        Returns:
            Path to the created run directory (just stores timestamp for later use)
        """
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = generate_timestamp()

        # Store timestamp for use in create_output_directory
        # The actual directory creation happens in create_output_directory based on subfolder
        if self.storage_manager.storage_type.lower() == "gcs":
            # GCS storage - use empty path as base since base_path already includes
            # the silver structure
            # This prevents the nested silver/silver/... issue
            run_dir = Path("")
        else:
            # Local storage - use base_path/silver
            run_dir = self.base_path / "silver"

        # Store the timestamp for later use
        self._current_timestamp = timestamp

        logger.info(f"Created run directory base: {run_dir} with timestamp: {timestamp}")
        return run_dir

    def create_output_directory(
        self, run_dir: Path, source_subfolder: str | None = None, content_type: str | None = None
    ) -> Path:
        """Create output directory for a specific file or content type following the new structure.

        Args:
            run_dir: Run directory path (silver/)
            source_subfolder: Optional subfolder name (will be reorganized to
                silver/{subfolder_name}/{timestamp})
            content_type: Optional content type descriptor

        Returns:
            Path to the created output directory
        """
        # Get the timestamp from the stored value and ensure it's a string
        timestamp = getattr(self, "_current_timestamp", None)
        if timestamp is None:
            timestamp = generate_timestamp()

        # Ensure timestamp is a string for path concatenation
        timestamp_str = str(timestamp)

        # New structure: silver/{subfolder_name}/{timestamp}/{content_type}
        if source_subfolder:
            # Sanitize subfolder name
            sanitized_subfolder = re.sub(r'[<>:"/\\|?*]', "_", source_subfolder)
            sanitized_subfolder = sanitized_subfolder.strip(". ").lower()

            # Create path: silver/{subfolder_name}/{timestamp}
            output_dir = run_dir / sanitized_subfolder / timestamp_str

            # Add content type if provided
            if content_type:
                sanitized_content_type = re.sub(r'[<>:"/\\|?*]', "_", content_type)
                sanitized_content_type = sanitized_content_type.strip(". ")
                output_dir = output_dir / sanitized_content_type
        else:
            # No subfolder provided, use 'root' as default
            output_dir = run_dir / "root" / timestamp_str
            if content_type:
                sanitized_content_type = re.sub(r'[<>:"/\\|?*]', "_", content_type)
                sanitized_content_type = sanitized_content_type.strip(". ")
                output_dir = output_dir / sanitized_content_type

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(output_dir)

        logger.info(f"Created Silver output directory: {output_dir}")
        return output_dir

    def save_parquet(self, table_name_or_data: Any, output_dir: Path, filename: str) -> Path:
        """Save DuckDB table or data to Parquet format.

        Args:
            table_name_or_data: DuckDB table name (str) or data to save
            output_dir: Directory to save the file
            filename: Base filename without extension

        Returns:
            Path to the saved file

        Raises:
            StorageError: If the file could not be saved
        """
        try:
            # Create file path with parquet extension
            file_path = output_dir / f"{filename}.parquet"

            # Convert filename to snake_case for Parquet files
            file_path = self._convert_to_snake_case(file_path)

            # Ensure the output directory exists
            self.storage_manager.ensure_directory_exists(output_dir)

            # Handle different input types
            if isinstance(table_name_or_data, str):
                # It's a table name - export directly from DuckDB
                table_name = table_name_or_data
                self.export_to_parquet(table_name, file_path)
            else:
                # It's data - register it first, then export
                temp_table = f"temp_save_{filename}"
                self.register_table(table_name_or_data, temp_table)

                # Clean problematic columns that might cause PyArrow issues
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {temp_table}_clean AS
                    SELECT * FROM {temp_table}
                """)

                # Convert problematic columns to strings
                columns_info = self.conn.execute(f"DESCRIBE {temp_table}_clean").fetchall()
                for col_name, col_type, *_ in columns_info:
                    # Skip numeric and datetime columns
                    if any(
                        t in col_type.upper()
                        for t in ["INTEGER", "DOUBLE", "FLOAT", "DATE", "TIMESTAMP"]
                    ):
                        continue

                    # Convert other columns to string to avoid PyArrow issues
                    self.conn.execute(f"""
                        UPDATE {temp_table}_clean
                        SET {col_name} = CAST({col_name} AS VARCHAR)
                    """)

                self.export_to_parquet(f"{temp_table}_clean", file_path)

                # Clean up temporary tables
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}_clean")

            logger.info(f"Saved Parquet file to {file_path}")
            return file_path

        except Exception as e:
            # Fallback to CSV if Parquet fails
            logger.warning(f"Failed to save as Parquet: {str(e)}, falling back to CSV")
            try:
                csv_path = output_dir / f"{filename}.csv"
                csv_path = self._convert_to_snake_case(csv_path)

                if isinstance(table_name_or_data, str):
                    table_name = table_name_or_data
                else:
                    temp_table = f"temp_save_{filename}"
                    self.register_table(table_name_or_data, temp_table)
                    table_name = temp_table

                self.conn.execute(f"""
                    COPY {table_name} TO '{csv_path}'
                    (FORMAT CSV, HEADER true)
                """)

                logger.info(f"Saved CSV file to {csv_path}")
                return csv_path

            except Exception as csv_error:
                error_msg = f"Failed to save file {filename}: Parquet failed ({str(e)}), CSV failed ({str(csv_error)})"
                logger.error(error_msg)
                raise StorageError(error_msg) from csv_error

    def save_geoparquet(self, table_name_or_data: Any, output_dir: Path, filename: str) -> Path:
        """Save DuckDB spatial table or data to GeoParquet format.

        Args:
            table_name_or_data: DuckDB table name (str) or spatial data to save
            output_dir: Directory to save the file
            filename: Base filename without extension

        Returns:
            Path to the saved file

        Raises:
            StorageError: If the file could not be saved
        """
        try:
            # Create file path with geoparquet extension
            file_path = output_dir / f"{filename}.geoparquet"

            # Convert filename to snake_case for GeoParquet files
            file_path = self._convert_to_snake_case(file_path)

            # Ensure the output directory exists
            self.storage_manager.ensure_directory_exists(output_dir)

            # Handle different input types
            if isinstance(table_name_or_data, str):
                # It's a table name - export directly from DuckDB
                table_name = table_name_or_data
                self.export_to_geoparquet(table_name, file_path)
            else:
                # It's data - register it first, then export
                temp_table = f"temp_geosave_{filename}"
                self.register_table(table_name_or_data, temp_table)
                self.export_to_geoparquet(temp_table, file_path)

                # Clean up temporary table
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

            logger.info(f"Saved GeoParquet file to {file_path}")
            return file_path

        except Exception as e:
            error_msg = f"Failed to save GeoParquet file {filename}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def _convert_to_snake_case(self, path: Path) -> Path:
        """Convert a filename in a path to snake_case.

        Args:
            path: Path with filename

        Returns:
            Path with snake_case filename
        """
        # Get directory and filename
        directory = path.parent
        filename = path.name

        # Split filename and extension
        name_parts = filename.split(".")
        name = name_parts[0]
        extension = ".".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Convert to snake_case
        # Replace spaces and non-alphanumeric characters with underscores
        snake_name = "".join(c if c.isalnum() else "_" for c in name)

        # Replace consecutive underscores with single underscore
        while "__" in snake_name:
            snake_name = snake_name.replace("__", "_")

        # Remove leading/trailing underscores
        snake_name = snake_name.strip("_")

        # Reconstruct the path
        if extension:
            new_path = directory / f"{snake_name}.{extension}"
        else:
            new_path = directory / snake_name

        return new_path
