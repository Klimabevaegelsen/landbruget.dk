"""Silver layer storage management."""

from pathlib import Path
from typing import Any

import pandas as pd

from ..utils.helpers import generate_timestamp
from ..utils.logging import get_logger
from ..utils.storage import DriveStorageManager, StorageError

# Get logger
logger = get_logger()


class SilverStorageManager:
    """Storage manager for the Silver layer."""

    def __init__(
        self,
        storage_manager: DriveStorageManager,
        base_path: Path,
    ):
        """Initialize the Silver storage manager.

        Args:
            storage_manager: Storage manager for file operations
            base_path: Base path for Silver layer storage (ignored for GCS, used for local)
        """
        self.storage_manager = storage_manager
        self.base_path = base_path
        # Use standard pipeline folder structure
        self.pipeline_name = "drive_data"
        logger.info(f"Initialized Silver storage manager for pipeline: {self.pipeline_name}")

    def create_run_directory(self, timestamp: str | None = None) -> Path:
        """Create a timestamped run directory following the required path structure.

        Args:
            timestamp: Optional timestamp string (if not provided, one will be generated)

        Returns:
            Path to the created run directory
        """
        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = generate_timestamp()

        # Use standard structure: silver/{pipeline_name}/{timestamp}
        # Note: pipeline_name will be added in create_output_directory
        # For local storage, this will be relative to base_path
        # For GCS, this will be the full path in the bucket
        if hasattr(self.storage_manager.storage, "bucket"):
            # GCS storage - use silver as base
            run_dir = Path("silver")
        else:
            # Local storage - use base_path/silver
            run_dir = self.base_path / "silver"

        # Store timestamp for later use
        self.current_timestamp = timestamp

        logger.info(f"Created Silver run directory base: {run_dir} (timestamp: {timestamp})")
        return run_dir

    def create_output_directory(
        self, run_dir: Path, source_subfolder: str | None = None, content_type: str | None = None
    ) -> Path:
        """Create output directory following the standard path: silver/{pipeline_name}/{timestamp}.

        Args:
            run_dir: Run directory path (silver)
            source_subfolder: Subfolder name (will be used in path structure)
            content_type: Optional content type descriptor

        Returns:
            Path to the created output directory
        """
        # Use the standard structure: silver/{pipeline_name}/{timestamp}
        # For drive data pipeline, we use the pipeline name as the main folder
        output_dir = run_dir / self.pipeline_name / self.current_timestamp

        # Add source subfolder if provided
        if source_subfolder:
            # Sanitize subfolder name
            sanitized_subfolder = (
                source_subfolder.replace(" ", "_").replace(".", "_").replace(":", "_")
            )
            sanitized_subfolder = "".join(
                c for c in sanitized_subfolder if c.isalnum() or c in "_-"
            )
            if sanitized_subfolder:
                output_dir = output_dir / sanitized_subfolder

        # Add content type subfolder if provided
        if content_type:
            output_dir = output_dir / content_type.lower()

        # Ensure the directory exists
        self.storage_manager.ensure_directory_exists(output_dir)

        logger.debug(f"Created output directory: {output_dir}")
        return output_dir

    def save_parquet(self, df: Any, output_dir: Path, filename: str) -> Path:
        """Save dataframe to Parquet format.

        Args:
            df: DuckDB/Ibis dataframe to save
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

            # Convert ALL problematic columns to string to avoid PyArrow conversion errors
            for col in df.columns:
                # Skip numeric and datetime columns
                if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_datetime64_any_dtype(
                    df[col]
                ):
                    continue

                # Convert all other columns to string
                df[col] = df[col].astype(str)

            try:
                # Try to save as Parquet
                df.to_parquet(
                    file_path,
                    index=False,
                    engine="pyarrow",
                    compression="snappy",
                    allow_truncated_timestamps=True,
                )
            except Exception as parquet_error:
                # If Parquet fails, save as CSV as a fallback
                logger.warning(
                    f"Failed to save as Parquet: {str(parquet_error)}, falling back to CSV"
                )
                csv_path = output_dir / f"{filename}.csv"
                csv_path = self._convert_to_snake_case(csv_path)
                df.to_csv(csv_path, index=False, encoding="utf-8")
                logger.info(f"Saved CSV file to {csv_path}")
                return csv_path

            logger.info(f"Saved Parquet file to {file_path}")
            return file_path

        except Exception as e:
            error_msg = f"Failed to save Parquet file {filename}: {str(e)}"
            logger.error(error_msg)
            raise StorageError(error_msg) from e

    def save_geoparquet(self, df: Any, output_dir: Path, filename: str) -> Path:
        """Save dataframe to GeoParquet format.

        Args:
            df: DuckDB/Ibis/GeoPandas dataframe to save
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

            # Save using GeoPandas
            df.to_parquet(file_path)

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
