"""Parquet and GeoParquet output management for Silver layer - DuckDB optimized."""

import json
import os
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from shapely.geometry import Point

# Handle imports for both standalone and package usage
try:
    from ..utils.logging import get_logger
    from ..utils.storage import DriveStorageManager
except ImportError:
    # Fallback for standalone usage
    import logging

    get_logger = lambda: logging.getLogger(__name__)
    DriveStorageManager = None
from .duckdb_base import DuckDBProcessor

# Get logger
logger = get_logger()


class ParquetManager(DuckDBProcessor):
    """Manager for Parquet and GeoParquet output using DuckDB."""

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
        super().__init__(dataset_name="parquet_manager")
        self.storage_manager = storage_manager
        self.compression = compression
        self.partition_by = partition_by
        logger.info(f"Initialized DuckDB-based ParquetManager with compression={compression}")

    def save_table_to_parquet(
        self,
        table_name: str,
        output_path: Path,
        schema_metadata: dict | None = None,
        row_group_size: int = 100000,
    ) -> Path:
        """Save a DuckDB table to Parquet format.

        Args:
            table_name: Name of DuckDB table to save
            output_path: Path to save to
            schema_metadata: Optional schema metadata to include
            row_group_size: Number of rows per group

        Returns:
            Path to the saved file
        """
        try:
            logger.info(f"Saving DuckDB table '{table_name}' to Parquet: {output_path}")

            # Check if we're using GCS storage
            if hasattr(self.storage_manager.storage, "bucket"):
                # GCS storage - write to temp file first, then upload
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Export from DuckDB table to parquet
                    self.conn.execute(f"""
                        COPY {table_name} TO '{temp_path}' 
                        (FORMAT PARQUET, COMPRESSION {self.compression}, ROW_GROUP_SIZE {row_group_size})
                    """)

                    # Add schema metadata if provided
                    if schema_metadata:
                        # Read the parquet file and add metadata
                        table = pq.read_table(temp_path)
                        schema_json = json.dumps(schema_metadata)

                        # Update metadata
                        new_metadata = {b"schema": schema_json.encode("utf-8")}
                        if table.schema.metadata:
                            for key, value in table.schema.metadata.items():
                                if key != b"schema":
                                    new_metadata[key] = value

                        # Write with updated metadata
                        table = table.replace_schema_metadata(new_metadata)
                        pq.write_table(table, temp_path, compression=self.compression)

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
                # Local storage - direct export
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Export directly from DuckDB
                self.conn.execute(f"""
                    COPY {table_name} TO '{output_path}' 
                    (FORMAT PARQUET, COMPRESSION {self.compression}, ROW_GROUP_SIZE {row_group_size})
                """)

                # Add schema metadata if provided (requires reading and rewriting)
                if schema_metadata:
                    table = pq.read_table(output_path)
                    schema_json = json.dumps(schema_metadata)

                    new_metadata = {b"schema": schema_json.encode("utf-8")}
                    if table.schema.metadata:
                        for key, value in table.schema.metadata.items():
                            if key != b"schema":
                                new_metadata[key] = value

                    table = table.replace_schema_metadata(new_metadata)
                    pq.write_table(table, output_path, compression=self.compression)

                logger.info(f"Saved Parquet file to {output_path}")
                return output_path

        except Exception as e:
            error_msg = f"Failed to save DuckDB table to Parquet: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    # Legacy method for backward compatibility during migration
    def save_dataframe_to_parquet(
        self,
        df,  # Could be pandas DataFrame or table name
        output_path: Path,
        schema_metadata: dict | None = None,
        row_group_size: int = 100000,
    ) -> Path:
        """Save a DataFrame to Parquet format (compatibility method).

        Args:
            df: DataFrame or table name to save
            output_path: Path to save to
            schema_metadata: Optional schema metadata to include
            row_group_size: Number of rows per group

        Returns:
            Path to the saved file
        """
        if isinstance(df, str):
            # It's already a table name
            return self.save_table_to_parquet(df, output_path, schema_metadata, row_group_size)
        else:
            # It's a pandas DataFrame - register it first
            import pandas as pd

            if isinstance(df, pd.DataFrame):
                table_name = f"temp_df_{int(time.time())}"
                self.register_dataframe(df, table_name)
                return self.save_table_to_parquet(
                    table_name, output_path, schema_metadata, row_group_size
                )
            else:
                raise ValueError(f"Unsupported data type: {type(df)}")

    def create_spatial_table_from_coords(
        self,
        table_name: str,
        latitude_col: str,
        longitude_col: str,
        target_crs: str = "EPSG:4326",
    ) -> str:
        """Create spatial table from coordinate columns using DuckDB-spatial.

        Args:
            table_name: Name of source table
            latitude_col: Name of latitude column
            longitude_col: Name of longitude column
            target_crs: Target coordinate reference system

        Returns:
            Name of spatial table
        """
        try:
            logger.info(f"Creating spatial table from coordinates in '{table_name}'")

            spatial_table = f"{table_name}_spatial"

            # Create spatial table using DuckDB-spatial
            self.conn.execute(f"""
                CREATE TABLE {spatial_table} AS
                SELECT 
                    * EXCLUDE ({latitude_col}, {longitude_col}),
                    ST_Point({longitude_col}, {latitude_col}) as geometry
                FROM {table_name}
                WHERE {latitude_col} IS NOT NULL 
                  AND {longitude_col} IS NOT NULL
                  AND {latitude_col} BETWEEN -90 AND 90
                  AND {longitude_col} BETWEEN -180 AND 180
            """)

            count = self.conn.execute(f"SELECT COUNT(*) FROM {spatial_table}").fetchone()[0]
            logger.info(f"Created spatial table '{spatial_table}' with {count:,} features")
            return spatial_table

        except Exception as e:
            error_msg = f"Failed to create spatial table: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def save_spatial_table_to_geoparquet(
        self,
        table_name: str,
        output_path: Path,
        geometry_column: str = "geometry",
        schema_metadata: dict | None = None,
    ) -> Path:
        """Save a spatial DuckDB table to GeoParquet format.

        Args:
            table_name: Name of spatial table to save
            output_path: Path to save to
            geometry_column: Name of the geometry column
            schema_metadata: Optional schema metadata to include

        Returns:
            Path to the saved file
        """
        try:
            logger.info(f"Saving spatial table '{table_name}' to GeoParquet: {output_path}")

            # Check if we're using GCS storage
            if hasattr(self.storage_manager.storage, "bucket"):
                # GCS storage - need to convert through geopandas for GeoParquet
                with tempfile.NamedTemporaryFile(suffix=".geoparquet", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Export geometry as WKT for geopandas conversion
                    df = self.conn.execute(f"""
                        SELECT 
                            * EXCLUDE ({geometry_column}),
                            ST_AsText({geometry_column}) as {geometry_column}
                        FROM {table_name}
                    """).df()

                    # Convert to GeoDataFrame
                    import geopandas as gpd
                    from shapely import wkt

                    df[geometry_column] = df[geometry_column].apply(wkt.loads)
                    gdf = gpd.GeoDataFrame(df, geometry=geometry_column, crs="EPSG:4326")

                    # Save to GeoParquet
                    if schema_metadata:
                        metadata = {"schema": schema_metadata}
                        gdf.to_parquet(temp_path, compression=self.compression, metadata=metadata)
                    else:
                        gdf.to_parquet(temp_path, compression=self.compression)

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
                # Local storage - convert through geopandas
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Export geometry as WKT for geopandas conversion
                df = self.conn.execute(f"""
                    SELECT 
                        * EXCLUDE ({geometry_column}),
                        ST_AsText({geometry_column}) as {geometry_column}
                    FROM {table_name}
                """).df()

                # Convert to GeoDataFrame
                import geopandas as gpd
                from shapely import wkt

                df[geometry_column] = df[geometry_column].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df, geometry=geometry_column, crs="EPSG:4326")

                # Save to GeoParquet
                if schema_metadata:
                    metadata = {"schema": schema_metadata}
                    gdf.to_parquet(output_path, compression=self.compression, metadata=metadata)
                else:
                    gdf.to_parquet(output_path, compression=self.compression)

                logger.info(f"Saved GeoParquet file to {output_path}")
                return output_path

        except Exception as e:
            error_msg = f"Failed to save spatial table to GeoParquet: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    # Legacy methods for backward compatibility
    def save_geodataframe_to_geoparquet(
        self,
        gdf,  # Could be GeoDataFrame or table name
        output_path: Path,
        geometry_column: str = "geometry",
        schema_metadata: dict | None = None,
    ) -> Path:
        """Save a GeoDataFrame to GeoParquet format (compatibility method)."""
        if isinstance(gdf, str):
            # It's already a table name
            return self.save_spatial_table_to_geoparquet(
                gdf, output_path, geometry_column, schema_metadata
            )
        else:
            # It's a GeoDataFrame - register it first as spatial table
            import time

            import geopandas as gpd

            if isinstance(gdf, gpd.GeoDataFrame):
                table_name = f"temp_gdf_{int(time.time())}"

                # Convert geometry to WKT for DuckDB
                df_wkt = gdf.copy()
                df_wkt[f"{geometry_column}_wkt"] = gdf[geometry_column].to_wkt()
                df_wkt = df_wkt.drop(geometry_column, axis=1)

                # Register as regular table first
                self.register_dataframe(df_wkt, f"{table_name}_temp")

                # Create spatial table
                self.conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT 
                        * EXCLUDE ({geometry_column}_wkt),
                        ST_GeomFromText({geometry_column}_wkt) as {geometry_column}
                    FROM {table_name}_temp
                """)

                # Clean up temp table
                self.drop_table(f"{table_name}_temp")

                return self.save_spatial_table_to_geoparquet(
                    table_name, output_path, geometry_column, schema_metadata
                )
            else:
                raise ValueError(f"Unsupported data type: {type(gdf)}")

    def dataframe_to_geodataframe(
        self,
        df,  # Could be DataFrame or table name
        latitude_col: str,
        longitude_col: str,
        target_crs: str = "EPSG:4326",
    ):
        """Convert a DataFrame with lat/lon columns to a GeoDataFrame (compatibility method)."""
        if isinstance(df, str):
            # It's a table name - create spatial table and export as GeoDataFrame
            spatial_table = self.create_spatial_table_from_coords(
                df, latitude_col, longitude_col, target_crs
            )

            # Export to GeoDataFrame for compatibility
            result_df = self.conn.execute(f"""
                SELECT 
                    * EXCLUDE (geometry),
                    ST_AsText(geometry) as geometry
                FROM {spatial_table}
            """).df()

            import geopandas as gpd
            from shapely import wkt

            result_df["geometry"] = result_df["geometry"].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(result_df, geometry="geometry", crs=target_crs)

            return gdf
        else:
            # It's a pandas DataFrame - use original logic
            import geopandas as gpd
            import pandas as pd

            if isinstance(df, pd.DataFrame):
                # Create geometry column
                geometry = [
                    Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
                    for lon, lat in zip(df[longitude_col], df[latitude_col], strict=False)
                ]

                # Create GeoDataFrame
                gdf = gpd.GeoDataFrame(
                    df.drop([longitude_col, latitude_col], axis=1),
                    geometry=geometry,
                    crs=target_crs,
                )

                return gdf
            else:
                raise ValueError(f"Unsupported data type: {type(df)}")
