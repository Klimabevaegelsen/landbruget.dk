"""Parquet and GeoParquet output management for Silver layer."""

import json
import os
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from ..utils.logging import get_logger
from ..utils.storage import DriveStorageManager

# Get logger
logger = get_logger()


class ParquetManager:
    """Manager for Parquet and GeoParquet output."""

    def __init__(
        self,
        storage_manager: DriveStorageManager,
        compression: str = "snappy",
        partition_by: list[str] | None = None,
    ):
        """Initialize the Parquet manager.

        Args:
            storage_manager: Storage manager for file operations
            compression: Compression algorithm to use
            partition_by: List of columns to partition by
        """
        self.storage_manager = storage_manager
        self.compression = compression
        self.partition_by = partition_by
        logger.info(f"Initialized ParquetManager with compression={compression}")

    def save_dataframe_to_parquet(
        self,
        df: pd.DataFrame,
        output_path: Path,
        schema_metadata: dict | None = None,
        row_group_size: int = 100000,
    ) -> Path:
        """Save a DataFrame to Parquet format.

        Args:
            df: DataFrame to save
            output_path: Path to save to
            schema_metadata: Optional schema metadata to include
            row_group_size: Number of rows per group

        Returns:
            Path to the saved file
        """
        try:
            logger.info(f"Saving DataFrame to Parquet: {output_path}")

            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)

            # Add schema metadata
            if schema_metadata:
                # Convert metadata to JSON string
                schema_json = json.dumps(schema_metadata)

                # Get existing schema metadata
                existing_metadata = table.schema.metadata

                # Update metadata with schema info
                new_metadata = {b"schema": schema_json.encode("utf-8")}

                # Preserve existing metadata
                if existing_metadata:
                    for key, value in existing_metadata.items():
                        if key != b"schema":
                            new_metadata[key] = value

                # Set new metadata
                table = table.replace_schema_metadata(new_metadata)

            # Check if we're using GCS storage
            if hasattr(self.storage_manager.storage, "bucket"):
                # GCS storage - write to temp file first, then upload
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Write with partitioning if specified
                    if self.partition_by and all(col in df.columns for col in self.partition_by):
                        # For partitioned datasets, we'll create a temp directory
                        with tempfile.TemporaryDirectory() as temp_dir:
                            temp_root = Path(temp_dir)

                            # Write partitioned dataset
                            pq.write_to_dataset(
                                table,
                                root_path=str(temp_root),
                                partition_cols=self.partition_by,
                                basename_template=f"{output_path.stem}_{{i}}.parquet",
                                filesystem=None,  # Use local filesystem for temp
                                use_dictionary=True,
                                compression=self.compression,
                                write_statistics=True,
                                writer_version="2.0",
                                data_page_size=1024 * 1024,  # 1MB page size
                                row_group_size=row_group_size,
                            )

                            # Upload all files in the partitioned dataset
                            for root, dirs, files in os.walk(temp_root):
                                for file in files:
                                    if file.endswith(".parquet"):
                                        local_file_path = Path(root) / file
                                        # Create relative path structure
                                        rel_path = local_file_path.relative_to(temp_root)
                                        gcs_path = output_path.parent / rel_path

                                        with open(local_file_path, "rb") as f:
                                            self.storage_manager.save_file(f.read(), gcs_path)

                            logger.info(
                                f"Saved partitioned Parquet to GCS: {output_path.parent} "
                                f"(partitioned by {', '.join(self.partition_by)})"
                            )
                            return output_path.parent
                    else:
                        # Standard write to a single file
                        pq.write_table(
                            table,
                            temp_path,
                            compression=self.compression,
                            write_statistics=True,
                            row_group_size=row_group_size,
                        )

                        # Upload to GCS
                        with open(temp_path, "rb") as f:
                            self.storage_manager.save_file(f.read(), output_path)

                        logger.info(f"Saved Parquet file to GCS: {output_path}")
                        return output_path

                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            else:
                # Local storage - use original logic
                # Create parent directory if it doesn't exist
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Write with partitioning if specified
                if self.partition_by and all(col in df.columns for col in self.partition_by):
                    # Create partition directory
                    root_path = output_path.parent

                    # Write partitioned dataset
                    pq.write_to_dataset(
                        table,
                        root_path=str(root_path),
                        partition_cols=self.partition_by,
                        basename_template=f"{output_path.stem}_{{i}}.parquet",
                        filesystem=None,  # Use local filesystem
                        use_dictionary=True,
                        compression=self.compression,
                        write_statistics=True,
                        writer_version="2.0",
                        data_page_size=1024 * 1024,  # 1MB page size
                        row_group_size=row_group_size,
                    )

                    logger.info(
                        f"Saved partitioned Parquet to {root_path} "
                        f"(partitioned by {', '.join(self.partition_by)})"
                    )
                    return root_path
                else:
                    # Standard write to a single file
                    pq.write_table(
                        table,
                        output_path,
                        compression=self.compression,
                        write_statistics=True,
                        row_group_size=row_group_size,
                    )

                    logger.info(f"Saved Parquet file to {output_path}")
                    return output_path

        except Exception as e:
            error_msg = f"Failed to save DataFrame to Parquet: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def save_geodataframe_to_geoparquet(
        self,
        gdf: gpd.GeoDataFrame | pd.DataFrame,
        output_path: Path,
        geometry_column: str = "geometry",
        schema_metadata: dict | None = None,
    ) -> Path:
        """Save a GeoDataFrame to GeoParquet format.

        Args:
            gdf: GeoDataFrame to save
            output_path: Path to save to
            geometry_column: Name of the geometry column
            schema_metadata: Optional schema metadata to include

        Returns:
            Path to the saved file
        """
        try:
            logger.info(f"Saving GeoDataFrame to GeoParquet: {output_path}")

            # Ensure we have a GeoDataFrame
            if not isinstance(gdf, gpd.GeoDataFrame):
                # Try to convert DataFrame to GeoDataFrame
                if geometry_column in gdf.columns:
                    # Check if geometry column contains WKT strings or Shapely geometries
                    if isinstance(gdf[geometry_column].iloc[0], str):
                        # Convert WKT strings to geometries
                        from shapely import wkt

                        gdf[geometry_column] = gdf[geometry_column].apply(wkt.loads)

                    # Convert to GeoDataFrame
                    gdf = gpd.GeoDataFrame(gdf, geometry=geometry_column, crs="EPSG:4326")
                else:
                    error_msg = f"Column '{geometry_column}' not found in DataFrame"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

            # Ensure CRS is set
            if gdf.crs is None:
                logger.warning("CRS not set in GeoDataFrame, assuming EPSG:4326")
                gdf.crs = "EPSG:4326"

            # Check if we're using GCS storage
            if hasattr(self.storage_manager.storage, "bucket"):
                # GCS storage - write to temp file first, then upload
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Add schema metadata if provided
                    if schema_metadata:
                        # Convert metadata to dictionary format used by geopandas
                        metadata = {"schema": schema_metadata}

                        # Save with metadata
                        gdf.to_parquet(
                            temp_path,
                            compression=self.compression,
                            metadata=metadata,
                        )
                    else:
                        # Standard save
                        gdf.to_parquet(
                            temp_path,
                            compression=self.compression,
                        )

                    # Upload to GCS
                    with open(temp_path, "rb") as f:
                        self.storage_manager.save_file(f.read(), output_path)

                    logger.info(f"Saved GeoParquet file to GCS: {output_path}")
                    return output_path

                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            else:
                # Local storage - use original logic
                # Create parent directory if it doesn't exist
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Add schema metadata if provided
                if schema_metadata:
                    # Convert metadata to dictionary format used by geopandas
                    metadata = {"schema": schema_metadata}

                    # Save with metadata
                    gdf.to_parquet(
                        output_path,
                        compression=self.compression,
                        metadata=metadata,
                    )
                else:
                    # Standard save
                    gdf.to_parquet(
                        output_path,
                        compression=self.compression,
                    )

                logger.info(f"Saved GeoParquet file to {output_path}")
                return output_path

        except Exception as e:
            error_msg = f"Failed to save GeoDataFrame to GeoParquet: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def dataframe_to_geodataframe(
        self,
        df: pd.DataFrame,
        latitude_col: str,
        longitude_col: str,
        target_crs: str = "EPSG:4326",
    ) -> gpd.GeoDataFrame:
        """Convert a DataFrame with lat/lon columns to a GeoDataFrame.

        Args:
            df: DataFrame to convert
            latitude_col: Name of latitude column
            longitude_col: Name of longitude column
            target_crs: Target coordinate reference system

        Returns:
            GeoDataFrame with geometry column
        """
        try:
            logger.info(f"Converting DataFrame with {latitude_col}/{longitude_col} to GeoDataFrame")

            # Create copy to avoid modifying original
            df_copy = df.copy()

            # Create geometry column
            geometry = [
                Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
                for lon, lat in zip(df_copy[longitude_col], df_copy[latitude_col], strict=False)
            ]

            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(df_copy, geometry=geometry, crs=target_crs)

            logger.info(f"Converted DataFrame to GeoDataFrame with CRS {target_crs}")
            return gdf

        except Exception as e:
            error_msg = f"Failed to convert DataFrame to GeoDataFrame: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e
