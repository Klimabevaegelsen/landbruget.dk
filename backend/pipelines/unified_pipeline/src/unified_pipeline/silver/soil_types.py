"""
Silver layer data processing for Soil Types data.

This module handles the transformation and cleaning of soil types data from the bronze layer.
It processes the raw WFS data into a standardized format suitable for analysis and
further downstream processing.

The module contains:
- SoilTypesSilverConfig: Configuration class for the silver layer processing
- SoilTypesSilver: Implementation class for processing and transforming data

The processing includes data validation, geometry cleaning, attribute standardization,
and quality assurance checks.
"""

import os
from typing import Any, Optional

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb

load_dotenv()


class SoilTypesSilverConfig(BaseJobConfig):
    """
    Configuration for the Soil Types Silver source.

    This class defines all configuration parameters needed for processing soil types
    data in the silver layer. It includes dataset configuration, storage settings,
    and processing parameters.

    Attributes:
        name (str): Human-readable name of the data source
        dataset (str): Name of the dataset
        type (str): Type of the data source (wfs)
        description (str): Brief description of the data
        frequency (str): How often the data is updated
        bucket (str): GCS bucket name for processed data storage
        save_local (bool): Whether to save data locally instead of uploading to GCS
    """

    name: str = "Danish Soil Types"
    dataset: str = "soil_types"
    type: str = "wfs"
    description: str = "Processed soil types data from Danish Environmental Portal"
    frequency: str = "monthly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
    save_local: bool = os.getenv("SAVE_LOCAL", "False").lower() == "true"

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class SoilTypesSilver(BaseSource[SoilTypesSilverConfig], SilverJobInterface):
    """
    Silver layer processing for soil types data.

    This class is responsible for processing and transforming soil types data from
    the bronze layer into a clean, standardized format. It handles data validation,
    geometry cleaning, attribute standardization, and quality assurance.

    Processing flow:
    1. Read data from the bronze layer
    2. Validate and clean geometries using DuckDB-spatial
    3. Standardize attribute names and values
    4. Perform quality checks
    5. Save processed data to the silver layer
    """

    def __init__(self, config: SoilTypesSilverConfig) -> None:
        """
        Initialize the SoilTypesSilver source.

        Args:
            config (SoilTypesSilverConfig): Configuration for the silver layer processing"""
        super().__init__(config)

    async def _validate_and_transform_with_duckdb(self, wfs_url: str) -> str:
        """
        Fetch WFS data and validate geometries using HTTP request + DuckDB-spatial.

        Args:
            wfs_url (str): The WFS URL to fetch data from

        Returns:
            str: Name of the DuckDB table containing validated data

        Raises:
            Exception: If WFS fetch or validation fails
        """
        try:
            self.log.info("Fetching soil types data from WFS using HTTP request")

            # Fetch data using HTTP request (like other pipelines do)

            import aiohttp

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
                async with session.get(wfs_url) as response:
                    if response.status != 200:
                        response_text = await response.text()
                        raise Exception(
                            f"WFS request failed with status {response.status}: "
                            f"{response_text[:500]}"
                        )

                    # Get the response as JSON
                    wfs_data = await response.json()

            self.log.info("Successfully fetched WFS data via HTTP request")

            # Process the GeoJSON data with DuckDB
            table_name = "soil_types_raw"

            # Convert GeoJSON features to records for DuckDB
            features = wfs_data.get("features", [])
            if not features:
                raise Exception("No features found in WFS response")

                # Use DuckDB's JSON functions to process GeoJSON directly
            import json

            # Save the GeoJSON to a temporary file that DuckDB can read
            json.dumps(wfs_data)

            # Stream process the features in batches to avoid memory issues
            features = wfs_data.get("features", [])
            if not features:
                raise Exception("No features found in WFS response")

            # Create the table first
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} (
                    properties JSON,
                    geometry GEOMETRY
                )
            """)

            # Process features in batches to avoid memory issues
            batch_size = 1000
            total_processed = 0

            for i in range(0, len(features), batch_size):
                batch = features[i : i + batch_size]

                # Convert batch to list of tuples for DuckDB
                batch_data = []
                for feature in batch:
                    if feature.get("geometry"):
                        properties_json = json.dumps(feature.get("properties", {}))
                        geometry_json = json.dumps(feature["geometry"])
                        batch_data.append((properties_json, geometry_json))

                if batch_data:
                    # Insert batch using DuckDB's VALUES clause
                    placeholders = ",".join(["(?, ST_GeomFromGeoJSON(?))"] * len(batch_data))
                    params = []
                    for props, geom in batch_data:
                        params.extend([props, geom])

                    self.conn.execute(
                        f"""
                        INSERT INTO {table_name} (properties, geometry)
                        VALUES {placeholders}
                    """,
                        params,
                    )

                    total_processed += len(batch_data)
                    self.log.info(f"Processed {total_processed:,} features so far...")

            self.log.info(f"Finished streaming processing of {total_processed:,} features")

            # Flatten the properties JSON into individual columns
            # First, get the column names from the properties
            sample_properties = self.conn.execute(f"""
                SELECT properties 
                FROM {table_name} 
                WHERE properties IS NOT NULL 
                LIMIT 1
            """).fetchone()

            if sample_properties and sample_properties[0]:
                properties_dict = json.loads(sample_properties[0])
                property_columns = list(properties_dict.keys())

                # Create column extractions
                column_extractions = []
                for col in property_columns:
                    # Handle different data types
                    column_extractions.append(f"json_extract(properties, '$.{col}') as \"{col}\"")

                # Recreate table with flattened properties
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name}_final AS
                    SELECT 
                        {", ".join(column_extractions)},
                        geometry,
                        current_timestamp as processed_at,
                        'validated' as data_quality
                    FROM {table_name}
                """)

                # Clean up temporary table and rename final table
                self.conn.execute(f"DROP TABLE {table_name}")
                self.conn.execute(f"ALTER TABLE {table_name}_final RENAME TO {table_name}")
            else:
                # If no properties, just add metadata columns
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name}_final AS
                    SELECT 
                        geometry,
                        current_timestamp as processed_at,
                        'validated' as data_quality
                    FROM {table_name}
                """)

                # Clean up temporary table and rename final table
                self.conn.execute(f"DROP TABLE {table_name}")
                self.conn.execute(f"ALTER TABLE {table_name}_final RENAME TO {table_name}")

            # Check if we got any data
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if count == 0:
                raise Exception("No data loaded into DuckDB table")

            self.log.info(f"Loaded {count:,} features into DuckDB table")

            # ✅ MIGRATION: Standardize column names using DuckDB
            processed_table = "soil_types_processed"

            # Get column names to check what we have
            columns = [row[0] for row in self.conn.execute(f"DESCRIBE {table_name}").fetchall()]
            self.log.info(f"Available columns: {columns}")

            # Build column mapping for standardization (removed useless theme_name column)
            select_parts = []
            for col in columns:
                if col.upper() == "JORDHT":
                    select_parts.append(f"TRY_CAST({col} AS DOUBLE) as soil_height")
                elif col.upper() == "JORD_TEKST":
                    select_parts.append(f"TRIM({col}) as soil_description")
                elif col.upper() == "TEMANAVN":
                    # Skip theme_name - always NULL, not useful
                    continue
                elif col.upper() == "KODE":
                    select_parts.append(f"TRY_CAST({col} AS DOUBLE) as soil_code")
                elif col.lower() in ["geom", "geometry", "wkb_geometry"]:
                    select_parts.append(f"{col} as geometry")
                else:
                    # Keep other columns as-is
                    select_parts.append(f"{col}")

            # Create processed table with standardized columns
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {processed_table} AS
                SELECT 
                    {", ".join(select_parts)},
                    current_timestamp as processed_at,
                    'validated' as data_quality
                FROM {table_name}
            """)

            # ✅ MIGRATION: Use DuckDB-spatial geometry validation
            # ✅ COORDINATE FIX: ST_FlipCoordinates is applied in the validator
            # to fix swapped lat/lon coordinates
            validate_and_transform_geometries_duckdb(
                self.conn, processed_table, self.config.dataset
            )

            # Clean up temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            final_count = self.conn.execute(f"SELECT COUNT(*) FROM {processed_table}").fetchone()[0]
            self.log.info(f"Validation completed. Processed {final_count:,} features")

            return processed_table

        except Exception as e:
            self.log.error(f"Error in DuckDB-spatial validation and transformation: {str(e)}")
            raise

    def _geometry_to_wkt(self, geometry_dict: dict) -> str:
        """Convert GeoJSON geometry to WKT format for DuckDB-spatial."""
        geom_type = geometry_dict.get("type")
        coordinates = geometry_dict.get("coordinates", [])

        def coord_to_string(coord):
            """Convert coordinate array to string, handling 2D and 3D coordinates."""
            if len(coord) >= 2:
                # Only use X and Y coordinates, ignore Z if present
                return f"{coord[0]} {coord[1]}"
            return None

        if geom_type == "Point":
            if len(coordinates) >= 2:
                coord_str = coord_to_string(coordinates)
                if coord_str:
                    return f"POINT({coord_str})"
        elif geom_type == "Polygon":
            if coordinates and len(coordinates) > 0:
                exterior = coordinates[0]
                points = []
                for pt in exterior:
                    coord_str = coord_to_string(pt)
                    if coord_str:
                        points.append(coord_str)
                if points:
                    return f"POLYGON(({', '.join(points)}))"
        elif geom_type == "MultiPolygon":
            if coordinates:
                polygons = []
                for polygon in coordinates:
                    if polygon and len(polygon) > 0:
                        exterior = polygon[0]
                        points = []
                        for pt in exterior:
                            coord_str = coord_to_string(pt)
                            if coord_str:
                                points.append(coord_str)
                        if points:
                            polygons.append(f"(({', '.join(points)}))")
                if polygons:
                    return f"MULTIPOLYGON({', '.join(polygons)})"

        return None

    def _perform_quality_checks(self, table_name: str) -> None:
        """
        Perform quality checks on the processed soil types data using DuckDB.

        This method validates data completeness, geometry validity, and attribute
        consistency to ensure the processed data meets quality standards.

        Args:
            table_name (str): Name of the DuckDB table to check

        Raises:
            Exception: If critical quality issues are found
        """
        try:
            self.log.info("Performing quality checks on processed soil types data")

            # Check total feature count
            total_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"Total features after processing: {total_count:,}")

            if total_count == 0:
                raise Exception("No features remain after processing - data quality check failed")

            # Check geometry validity
            invalid_geom_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE geometry IS NULL OR NOT ST_IsValid(geometry)
            """).fetchone()[0]

            if invalid_geom_count > 0:
                self.log.warning(f"Found {invalid_geom_count} features with invalid geometries")
                if invalid_geom_count / total_count > 0.1:  # More than 10% invalid
                    raise Exception("Too many invalid geometries - data quality check failed")

            # Check for required attributes
            null_soil_code = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE soil_code IS NULL
            """).fetchone()[0]

            if null_soil_code > 0:
                self.log.info(f"Features with missing soil_code: {null_soil_code}")

            # Check coordinate bounds (should be in Denmark after transformation to WGS84)
            bounds = self.conn.execute(f"""
                SELECT 
                    MIN(ST_XMin(geometry)) as min_x,
                    MAX(ST_XMax(geometry)) as max_x,
                    MIN(ST_YMin(geometry)) as min_y,
                    MAX(ST_YMax(geometry)) as max_y
                FROM {table_name}
                WHERE geometry IS NOT NULL
            """).fetchone()

            if bounds:
                min_x, max_x, min_y, max_y = bounds
                self.log.info(
                    f"Coordinate bounds: X({min_x:.3f}, {max_x:.3f}), Y({min_y:.3f}, {max_y:.3f})"
                )

                # Check if coordinates are reasonable for Denmark in WGS84
                if not (7.0 <= min_x <= 16.0 and 54.0 <= min_y <= 58.0):
                    self.log.warning(
                        "Coordinates appear to be outside Denmark bounds "
                        "(coordinates should be fixed with ST_FlipCoordinates)"
                    )

            # Check attribute distributions
            soil_type_count = self.conn.execute(f"""
                SELECT COUNT(DISTINCT soil_code) FROM {table_name} WHERE soil_code IS NOT NULL
            """).fetchone()[0]

            self.log.info(f"Unique soil types: {soil_type_count}")

            self.log.info("Quality checks completed successfully")

        except Exception as e:
            self.log.error(f"Quality check failed: {str(e)}")
            raise

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the soil types data processing pipeline using DuckDB-spatial.

        This method orchestrates the entire process of reading bronze data,
        processing it with DuckDB-spatial, and saving the results.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        should contain WFS metadata including the URL.

        Returns:
            None

        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            self.log.info("Starting soil types silver layer processing with DuckDB-spatial")

            # Read data with support for in-memory passing
            if bronze_data is not None:
                self.log.info("Using bronze data from memory (in-memory data passing)")
                if isinstance(bronze_data, dict) and "wfs_url" in bronze_data:
                    wfs_url = bronze_data["wfs_url"]
                    self.log.info(f"Using WFS URL from bronze data: {wfs_url}")
                else:
                    self.log.error(
                        f"Expected dict with wfs_url from bronze stage, got {type(bronze_data)}"
                    )
                    return
            else:
                # Fallback to reading from storage
                self.log.info("Reading bronze data from storage (fallback)")
                bronze_metadata = self._read_bronze_data_from_storage(
                    self.config.dataset, self.config.bucket
                )
                if bronze_metadata is None:
                    self.log.error("Failed to read bronze metadata from storage")
                    return

                # Extract WFS URL from stored metadata
                if isinstance(bronze_metadata, dict) and "wfs_url" in bronze_metadata:
                    wfs_url = bronze_metadata["wfs_url"]
                else:
                    self.log.error("Bronze metadata does not contain wfs_url")
                    return

            # ✅ MIGRATION: Process data using DuckDB-spatial
            processed_table = await self._validate_and_transform_with_duckdb(wfs_url)

            # Perform quality checks
            self._perform_quality_checks(processed_table)

            # Save the processed data using new unified method
            self._save_data(
                data=processed_table,
                dataset=self.config.dataset,
                bucket=self.config.bucket,
                stage="silver",
                conn=self.conn,
            )

            self.log.info("Soil types silver layer processing completed successfully")

        except Exception as e:
            self.log.error(f"Failed to process soil types silver layer: {e}")
            raise
