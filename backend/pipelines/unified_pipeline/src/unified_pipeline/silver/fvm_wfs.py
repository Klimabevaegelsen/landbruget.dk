"""
Silver layer processing for FVM WFS Agricultural data.

This module transforms raw WFS data (from the bronze layer) into cleaner,
more structured Geos for analytical purposes. It handles the extraction
of GeoJSON features from WFS responses, converts them to Geos,
and applies transformations such as column renaming and geometry validation.

The module processes three types of FVM data:
- Markblokke (field blocks): Primary field boundary data 2005-2026
- Marker (field markers): Field usage/application data 2008-2025
- Smaabiotoper (small biotopes): Special biotope layers 2023-2025

The module consists of two main components:
- FVMWFSSilverConfig: Configuration for Silver processing
- FVMWFSSilver: Implementation of Silver processing logic
"""

import asyncio
import json
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.timing import AsyncTimer


class FVMWFSSilverConfig(BaseJobConfig):
    """
    Configuration for FVM WFS Silver data processing.

    This configuration defines parameters for transforming FVM WFS data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and column mappings.

    Attributes:
        name (str): Human-readable name of the data processing
        type (str): Type of the data processing
        dataset (str): Primary dataset name for silver data collection
        dataset_markblokke (str): Name of the markblokke dataset
        dataset_marker (str): Name of the marker dataset
        dataset_smaabiotoper (str): Name of the smaabiotoper dataset
        bucket (str): GCS bucket name for storing processed data
        storage_batch_size (int): Batch size for storage operations
        markblokke_years (List[int]): Years to process for Markblokke (2005-2026)
        marker_years (List[int]): Years to process for Marker (2008-2025)
        smaabiotoper_years (List[int]): Years to process for Smaabiotoper (2023-2025)
        column_mapping (Dict): Dictionary mapping raw field names to standardized names
    """

    name: str = "Danish FVM WFS Agricultural Data - Silver"
    type: str = "transformation"
    dataset: str = "fvm_wfs"  # Primary dataset name for app.py silver data collection

    # Bronze dataset names (for reading from bronze storage)
    bronze_dataset_markblokke: str = "fvm_markblokke"
    bronze_dataset_marker: str = "fvm_marker"

    # Silver dataset names (for saving to silver storage and test expectations)
    dataset_markblokke: str = "fvm_markblokke"
    dataset_marker: str = "fvm_marker"
    dataset_smaabiotoper: str = "fvm_smaabiotoper"

    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 5000

    # Year ranges based on FVM WFS capabilities
    markblokke_years: List[int] = list(range(2005, 2027))  # 2005-2026 (22 years)
    marker_years: List[int] = list(range(2008, 2026))  # 2008-2025 (18 years)
    smaabiotoper_years: List[int] = [2023, 2024, 2025]  # Special biotope layers

    # Column mapping for standardization
    # Markblokke fields
    markblokke_column_mapping: Dict[str, str] = {
        "MB_NR": "block_id",
        "BLOKAREAL": "block_area_ha",
        "MARKBLOKTY": "block_type",
        "STATUSOPL": "status_info",
        "NOTAT": "notes",
        "BRUGER_ID": "user_id",
        "OPRINDATO": "creation_date",
        "CVR": "cvr_number",
        "JOURNALNR": "journal_number",
    }

    # Marker fields - Dynamic mapping based on field harmonization analysis

    marker_column_mapping: Dict[str, str] = {
        # Core fields (present in all years)
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        # Company/applicant fields (evolved over time - all harmonized to cvr_number)
        "Ansoeger": "cvr_number",  # 2008-2011 (legacy applicant ID)
        "KUNDE_LB": "cvr_number",  # 2012-2013 (legacy customer ID)
        "CVR": "cvr_number",  # 2016-2025 (official CVR number)
        # Crop information (2010+)
        "Afgkode": "crop_code",
        "Afgroede": "crop_name",
        "Hovedafg": "main_crop_code",  # Only 2016
        # Area fields
        "Ansoegt": "applied_area_ha",  # 2010-2014
        "GBanmeldt": "reported_area_ha",  # 2017-2025
        # Administrative fields
        "Journalnr": "journal_number",  # 2014+
        "Markblok": "block_id",  # 2016+
        # Agricultural practice
        "GB": "organic_farming",  # 2010+
    }

    # Smaabiotoper fields (similar to Marker but with biotope-specific fields)
    smaabiotoper_column_mapping: Dict[str, str] = {
        "Marknr": "field_id",
        "IMK_areal": "area_ha",
        "Journalnr": "journal_number",
        "CVR": "cvr_number",
        "Afgkode": "biotope_code",
        "Afgroede": "biotope_type",
        "GB": "organic_farming",
        "GBanmeldt": "reported_area_ha",
        "Markblok": "block_id",
        "MarkblokNr": "block_number",
        "BRUGER_ID": "user_id",
        "OPRINDATO": "creation_date",
        "NOTAT": "notes",
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FVMWFSSilver(BaseSource[FVMWFSSilverConfig], SilverJobInterface):
    """
    Silver layer processor for FVM WFS agricultural data.

    This class transforms raw FVM WFS data from the bronze layer into
    structured Geos. It handles extracting GeoJSON features from WFS responses,
    validates geometries, standardizes column names, and saves the processed data.

    The processing includes:
    1. Reading raw WFS data from GCS
    2. Extracting GeoJSON features from each payload and converting to Geos
    3. Validating and transforming geometries
    4. Standardizing column names using the mapping from config
    5. Saving processed data to GCS for each year
    """

    def __init__(self, config: FVMWFSSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the FVMWFSSilver processor.

        Args:
            config: Configuration for the silver processing job
            gcs_util: Utility for GCS operations
        """
        super().__init__(config, gcs_util)

    async def extract_geojson_from_wfs_payload(
        self, payload_json: str, column_mapping: Dict[str, str]
    ):
        """
        Extract GeoJSON features from a raw WFS payload and convert to  using DuckDB.

        This method parses a JSON string payload containing features from the FVM WFS response,
        converts them to spatial table using DuckDB-spatial operations with standardized
        column names.

        Args:
            payload_json: JSON string containing features from FVM WFS response
            column_mapping: Dictionary mapping original column names to standardized names

        Returns:
            A  containing the extracted features with standardized column names,
            or an empty  if extraction fails or no features are found

        Note:
            The source data uses EPSG:25832 coordinate system (UTM Zone 32N)
        """
        try:
            payload = json.loads(payload_json)
            features = payload.get("features", [])

            if not features:
                self.log.warning("No features found in payload")
                return self.conn.execute("SELECT NULL as geometry_wkt LIMIT 0")

            # Convert features to data for DuckDB registration
            feature_data = []
            for feature in features:
                properties = feature.get("properties", {})
                geometry = feature.get("geometry")

                if geometry:
                    # Convert geometry to WKT for DuckDB
                    feature_data.append(
                        {**properties, "geometry_wkt": self._geometry_to_wkt(geometry)}
                    )

            if not feature_data:
                return self.conn.execute("SELECT NULL as geometry_wkt LIMIT 0")

            # Create table directly from the list of dictionaries using DuckDB's native capabilities
            # Get column names from the first feature
            columns = list(feature_data[0].keys())

            # Create the table schema
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_features (
                    {", ".join([f"{col} VARCHAR" for col in columns])}
                )
            """)

            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(feature_data), batch_size):
                batch = feature_data[i : i + batch_size]

                # Use parameterized queries instead of string concatenation
                for feature in batch:
                    values = [feature.get(col) for col in columns]
                    placeholders = ", ".join(["?" for _ in columns])

                    self.conn.execute(
                        f"""
                        INSERT INTO raw_features ({", ".join(columns)})
                        VALUES ({placeholders})
                    """,
                        values,
                    )

            # Apply column mapping in SQL
            column_mappings = []
            for old_col, new_col in column_mapping.items():
                column_mappings.append(f'"{old_col}" as "{new_col}"')

            # Create spatial table with transformed geometry as WKT string
            # Return DuckDB relation instead of pandas  to avoid Shapely conversion
            result_query = f"""
                SELECT 
                    {", ".join(column_mappings) if column_mappings else "*"},
                    ST_AsText(ST_Transform(ST_GeomFromText(geometry_wkt), 'EPSG:25832', 'EPSG:4326')) as geometry_wkt
                FROM raw_features
                WHERE geometry_wkt IS NOT NULL
            """

            return self.conn.execute(result_query)

        except json.JSONDecodeError as e:
            self.log.error(f"Error parsing JSON payload: {e}")
            return self.conn.execute("SELECT NULL as geometry_wkt LIMIT 0")
        except Exception as e:
            self.log.error(f"Error processing payload: {e}")
            return self.conn.execute("SELECT NULL as geometry_wkt LIMIT 0")

    def _geometry_to_wkt(self, geometry_dict: dict) -> str:
        """Convert GeoJSON geometry to WKT format for DuckDB-spatial."""
        geom_type = geometry_dict.get("type")
        coordinates = geometry_dict.get("coordinates", [])

        if geom_type == "Point":
            if len(coordinates) >= 2:
                return f"POINT({coordinates[0]} {coordinates[1]})"
        elif geom_type == "Polygon":
            if coordinates and len(coordinates) > 0:
                exterior = coordinates[0]
                points = " ".join([f"{pt[0]} {pt[1]}" for pt in exterior])
                return f"POLYGON(({points}))"
        elif geom_type == "MultiPolygon":
            if coordinates:
                polygons = []
                for polygon in coordinates:
                    if polygon and len(polygon) > 0:
                        exterior = polygon[0]
                        points = " ".join([f"{pt[0]} {pt[1]}" for pt in exterior])
                        polygons.append(f"(({points}))")
                if polygons:
                    return f"MULTIPOLYGON({', '.join(polygons)})"

        return None

    def _geojson_to_wkt(self, geojson_dict: dict) -> str:
        """Convert GeoJSON geometry to WKT format for DuckDB-spatial."""
        return self._geometry_to_wkt(geojson_dict)

    async def _add_block_ids_via_spatial_join(self, marker_data, year: int):
        """
        Add block IDs to Marker data via spatial join with Markblokke data using DuckDB-spatial.

        This method is used for years 2008-2015 where Marker data doesn't include
        the Markblok field, but we can spatially join with the corresponding
        Markblokke layer to get block IDs using DuckDB-spatial's SPATIAL_JOIN operator
        for optimal performance.

        Args:
            marker_data: Either a table name (string) or Marker Geo without block IDs
            year: Year of the data

        Returns:
            Table name (string) with block_id field added via DuckDB spatial join
        """
        # ✅ MIGRATION: Handle both table names and s
        if isinstance(marker_data, str):
            # marker_data is a table name
            marker_table = marker_data
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {marker_table}").fetchone()[0]
            if row_count == 0:
                return marker_data
        else:
            # marker_data is a /Geo
            if marker_data.empty:
                return marker_data
            self.conn.register("temp_marker_input", marker_data)
            marker_table = "temp_marker_input"
            row_count = len(marker_data)

        try:
            # Read corresponding Markblokke data for spatial join
            markblokke_dataset = f"fvm_markblokke_{year}"
            markblokke_df = self._read_data_from_storage(
                markblokke_dataset, self.config.bucket, stage="silver"
            )

            if markblokke_df is None or markblokke_df.empty:
                self.log.warning(f"No Markblokke data found for {year}, cannot add block IDs")
                # Add empty block_id column using DuckDB
                result_table = f"marker_with_null_block_{year}"
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {result_table} AS SELECT *, NULL as block_id FROM {marker_table}"
                )
                return result_table

            self.log.info(f"Markblokke data columns for {year}: {list(markblokke_df.columns)}")
            self.log.info(f"Markblokke data shape: {markblokke_df.shape}")

            # Find the actual block ID column name (could be MARKBLOKNR, block_id, MB_NR, etc.)
            block_id_column = None
            for col in markblokke_df.columns:
                if col.lower() in ["block_id", "markbloknr", "mb_nr", "markblok_nr"]:
                    block_id_column = col
                    break

            if block_id_column is None:
                self.log.error(
                    f"No block ID column found in Markblokke data for {year}. Available columns: {list(markblokke_df.columns)}"
                )
                # Add empty block_id column using DuckDB
                result_table = f"marker_with_null_block_{year}"
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {result_table} AS SELECT *, NULL as block_id FROM {marker_table}"
                )
                return result_table

            self.log.info(f"Using {block_id_column} as block ID column")

            # Register markblokke data with DuckDB
            self.conn.register("blocks", markblokke_df)

            # Get geometry column names
            marker_columns = self.conn.execute(f"DESCRIBE {marker_table}").fetchall()
            marker_col_names = [col[0] for col in marker_columns]

            geom_col = "geometry_wkt" if "geometry_wkt" in marker_col_names else "geometry"
            markblokke_geom_col = (
                "geometry_wkt" if "geometry_wkt" in markblokke_df.columns else "geometry"
            )

            # Check if geometries need conversion from Shapely to WKT
            sample_geom_query = (
                f"SELECT {geom_col} FROM {marker_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
            )
            sample_result = self.conn.execute(sample_geom_query).fetchone()

            if sample_result and sample_result[0] is not None:
                sample_geom = sample_result[0]
                if hasattr(sample_geom, "wkt"):
                    self.log.info("Converting Shapely geometries to WKT for DuckDB compatibility")
                    # Convert Shapely to WKT in the table
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE {marker_table}_wkt AS
                        SELECT *, 
                            CASE WHEN {geom_col} IS NOT NULL THEN ST_AsText({geom_col}) ELSE NULL END as {geom_col}_wkt
                        FROM {marker_table}
                    """)
                    marker_table = f"{marker_table}_wkt"
                    geom_col = f"{geom_col}_wkt"

                    # Also convert markblokke geometries if needed
                    markblokke_df_wkt = markblokke_df.copy()
                    markblokke_df_wkt[f"{markblokke_geom_col}_wkt"] = markblokke_df[
                        markblokke_geom_col
                    ].apply(lambda x: x.wkt if hasattr(x, "wkt") and x is not None else x)
                    self.conn.register("blocks", markblokke_df_wkt)
                    markblokke_geom_col = f"{markblokke_geom_col}_wkt"

            # Perform spatial join using DuckDB-spatial with WKT geometries
            spatial_join_query = f"""
                SELECT 
                    m.*,
                    b.{block_id_column} as block_id
                FROM {marker_table} m
                LEFT JOIN blocks b ON ST_Intersects(
                    ST_GeomFromText(m.{geom_col}), 
                    ST_GeomFromText(b.{markblokke_geom_col})
                )
                WHERE m.{geom_col} IS NOT NULL AND b.{markblokke_geom_col} IS NOT NULL
            """

            self.log.info(
                f"Executing DuckDB-spatial join for {row_count} markers with {len(markblokke_df)} blocks in {year}"
            )

            # Execute the spatial join
            result_table = f"spatial_join_result_{year}"
            self.conn.execute(f"CREATE OR REPLACE TABLE {result_table} AS {spatial_join_query}")
            result_count = self.conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()[0]

            # Add markers that didn't have geometry (NULL geometries)
            if result_count < row_count:
                self.log.info(f"Adding {row_count - result_count} markers without valid geometry")

                # Get markers with NULL geometry and add to final result
                final_table = f"final_result_{year}"
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {final_table} AS
                    SELECT * FROM {result_table}
                    UNION ALL
                    SELECT *, NULL as block_id
                    FROM {marker_table}
                    WHERE {geom_col} IS NULL
                """)

                result_count = self.conn.execute(f"SELECT COUNT(*) FROM {final_table}").fetchone()[
                    0
                ]
                result_table = final_table

            self.log.info(
                f"Added block IDs via DuckDB spatial join for {result_count} markers in {year}"
            )

            # Log statistics about the join success
            markers_with_blocks = self.conn.execute(
                f"SELECT COUNT(*) FROM {result_table} WHERE block_id IS NOT NULL"
            ).fetchone()[0]
            if row_count > 0:
                self.log.info(
                    f"Spatial join success rate: {markers_with_blocks}/{row_count} ({markers_with_blocks / row_count * 100:.1f}%)"
                )

            return result_table

        except Exception as e:
            self.log.error(f"Error adding block IDs via spatial join for {year}: {e}")
            import traceback

            self.log.error(f"Traceback: {traceback.format_exc()}")
            # Add empty block_id column as fallback using DuckDB
            try:
                fallback_table = f"marker_fallback_result_{year}"
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {fallback_table} AS SELECT *, NULL as block_id FROM {marker_table}"
                )
                return fallback_table
            except:
                pass
            return marker_data if isinstance(marker_data, str) else "temp_marker_input"

    async def _process_data(self, raw_df, layer_type: str, year: int):
        """
        Process raw data into a clean Geo.

        This method takes raw data from the bronze layer, extracts GeoJSON features from each
        payload in parallel, and combines them into a single Geo. It also handles
        column name cleaning and geometry validation.

        Args:
            raw_df:  containing raw payloads from the bronze layer
            layer_type: Type of layer being processed (Markblokke, Marker, Smaabiotoper)
            year: Year of the data being processed

        Returns:
            A Geo containing all processed features with validated geometries,
            or an empty Geo if processing fails
        """
        async with AsyncTimer(f"Processing {layer_type} data for {year}"):
            # Handle case where raw_df might be a table name
            if isinstance(raw_df, str):
                # raw_df is a table name, extract payloads using DuckDB
                payloads_result = self.conn.execute(f"SELECT payload FROM {raw_df}").fetchall()
                payloads = [row[0] for row in payloads_result]
            else:
                # raw_df is a
                payloads = raw_df["payload"].tolist()

            # Get appropriate column mapping based on layer type
            if layer_type == "Markblokke":
                column_mapping = self.config.markblokke_column_mapping
            elif layer_type == "Smaabiotoper":
                column_mapping = self.config.smaabiotoper_column_mapping
            else:  # Marker
                column_mapping = self.config.marker_column_mapping

            # Extract GeoJSON features from each payload using DuckDB relations
            tasks = [
                self.extract_geojson_from_wfs_payload(payload, column_mapping)
                for payload in payloads
            ]
            geo_relations_list = await asyncio.gather(*tasks)

            # Filter out empty relations and register them for UNION
            valid_relations = []
            for i, relation in enumerate(geo_relations_list):
                # Check if relation has data by registering and counting rows
                table_name = f"temp_relation_{i}"
                self.conn.register(table_name, relation)
                row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if row_count > 0:
                    valid_relations.append(table_name)

            if not valid_relations:
                self.log.warning(f"No valid data extracted for {layer_type} {year}")
                return self.conn.execute("SELECT NULL as geometry_wkt LIMIT 0")

            # ✅ MIGRATION: Use DuckDB UNION operations instead of pandas concat
            if len(valid_relations) == 1:
                combined_query = f"SELECT * FROM {valid_relations[0]}"
            else:
                # Create UNION query for all tables
                union_query = " UNION ALL ".join(
                    [f"SELECT * FROM {table}" for table in valid_relations]
                )
                combined_query = f"""
                    SELECT * FROM ({union_query})
                    ORDER BY COALESCE(processed_at, current_timestamp)
                """

            # Execute the combined query to get the final relation
            combined_relation = self.conn.execute(combined_query)

            # Register the combined relation and get column info
            self.conn.register("combined_temp", combined_relation)

            # Get column names using DuckDB DESCRIBE
            columns_info = self.conn.execute("DESCRIBE combined_temp").fetchall()
            columns = [col[0] for col in columns_info]  # Extract column names
            cleaned_columns = [
                col.replace(".", "_").replace("()", "_").replace("(", "_").replace(")", "_")
                for col in columns
            ]

            # Create column mapping for renaming
            column_renames = ", ".join(
                [f'"{old}" as "{new}"' for old, new in zip(columns, cleaned_columns)]
            )

            # Add metadata using DuckDB and clean column names in one query
            current_timestamp = self.conn.execute("SELECT current_timestamp").fetchone()[0]

            final_query = f"""
                SELECT {column_renames},
                    {year} as year,
                    '{layer_type}' as layer_type,
                    '{current_timestamp}' as processed_at
                FROM combined_temp
            """

            # ✅ MIGRATION: Create table instead of  conversion
            self.conn.execute(f"CREATE OR REPLACE TABLE final_processed AS {final_query}")

            # Check if we have data
            row_count = self.conn.execute("SELECT COUNT(*) FROM final_processed").fetchone()[0]

            # For Marker data: Add block IDs via spatial join if not present
            if layer_type == "Marker" and row_count > 0:
                # Check if block_id field exists and has data
                columns_info = self.conn.execute("DESCRIBE final_processed").fetchall()
                column_names = [col[0] for col in columns_info]

                has_block_id = "block_id" in column_names
                if has_block_id:
                    block_id_count = self.conn.execute(
                        "SELECT COUNT(*) FROM final_processed WHERE block_id IS NOT NULL"
                    ).fetchone()[0]
                    has_block_data = block_id_count > 0
                else:
                    has_block_data = False

                if not has_block_id or not has_block_data:
                    self.log.info(
                        f"Block ID not available in Marker data for {year}, attempting spatial join with Markblokke"
                    )
                    # ❌ ELIMINATED: No more wasteful  conversion
                    # temp_df = self.conn.execute("SELECT * FROM final_processed")
                    # ✅ MIGRATION: Pass table name instead of
                    result_table = await self._add_block_ids_via_spatial_join(
                        "final_processed", year
                    )

                    # If result is a table name, use it; otherwise register the result
                    if isinstance(result_table, str):
                        self.conn.execute(
                            f"CREATE OR REPLACE TABLE final_processed AS SELECT * FROM {result_table}"
                        )
                    else:
                        self.conn.register("final_processed", result_table)

            return "final_processed"

    async def _process_layer_type(
        self,
        layer_type: str,
        years: List[int],
        bronze_dataset_name: str,
        silver_dataset_name: str,
        bronze_data: Optional[Any] = None,
    ) -> None:
        """
        Process all years for a specific layer type.

        Args:
            layer_type: Type of layer to process (Markblokke, Marker, Smaabiotoper)
            years: List of years to process
            bronze_dataset_name: Base dataset name for reading from bronze storage
            silver_dataset_name: Base dataset name for saving to silver storage
            bronze_data: Optional in-memory data from bronze stage
        """
        self.log.info(f"Processing {layer_type} silver data for {len(years)} years")

        for year in years:
            try:
                bronze_dataset_with_year = f"{bronze_dataset_name}_{year}"
                silver_dataset_with_year = f"{silver_dataset_name}_{year}"
                self.log.info(f"Processing {layer_type} for year {year}")

                # Read data with support for in-memory passing
                if bronze_data is not None:
                    self.log.info("Using bronze data from memory (in-memory data passing)")
                    # Bronze data structure: {layer_type: {year: raw_data}}
                    layer_data = bronze_data.get(layer_type.lower(), {})
                    if year in layer_data:
                        raw_data = layer_data[year]
                        # ✅ MIGRATION: Convert to  if it's not already using DuckDB
                        if not hasattr(raw_data, "iterrows"):  # Check if it's -like
                            # Use DuckDB to create  - avoid dict registration
                            self.conn.execute(
                                "CREATE OR REPLACE TABLE temp_raw_data (payload VARCHAR)"
                            )
                            self.conn.execute(
                                "INSERT INTO temp_raw_data VALUES (?)", [str(raw_data)]
                            )
                            # ✅ MIGRATION: Keep as table instead of converting to
                            raw_data = "temp_raw_data"
                    else:
                        self.log.warning(f"No in-memory data found for {layer_type} {year}")
                        continue
                else:
                    # Fallback to reading from storage
                    self.log.info("Reading bronze data from storage (fallback)")
                    raw_data = self._read_bronze_data(bronze_dataset_with_year, self.config.bucket)
                    if raw_data is None:
                        self.log.warning(
                            f"No raw data found for {bronze_dataset_with_year}, skipping"
                        )
                        continue

                self.log.info(f"Read raw data successfully for {bronze_dataset_with_year}")

                # Process the data
                geo_relation = await self._process_data(raw_data, layer_type, year)

                if geo_relation is None:
                    self.log.warning(f"No processed data for {silver_dataset_with_year}, skipping")
                    continue

                # Check if relation has data
                if isinstance(geo_relation, str):
                    # geo_relation is a table name
                    row_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {geo_relation}"
                    ).fetchone()[0]
                    table_name = f"silver_{layer_type.lower()}_{year}"
                    self.conn.execute(
                        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {geo_relation}"
                    )
                else:
                    # geo_relation is a relation object, register it
                    temp_table_name = f"temp_check_{layer_type.lower()}_{year}"
                    self.conn.register(temp_table_name, geo_relation)
                    row_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {temp_table_name}"
                    ).fetchone()[0]

                    # Register as final table for export
                    table_name = f"silver_{layer_type.lower()}_{year}"
                    self.conn.register(table_name, geo_relation)

                if row_count == 0:
                    self.log.warning(f"No processed data for {silver_dataset_with_year}, skipping")
                    continue

                self.log.info(f"Processed {row_count:,} features for {silver_dataset_with_year}")

                # Export directly to GCS using DuckDB COPY command
                gcs_path = f"gs://{self.config.bucket}/silver/{silver_dataset_with_year}/{silver_dataset_with_year}.parquet"

                # Use temporary file approach since DuckDB can't write directly to GCS
                import tempfile

                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                    temp_path = tmp.name

                try:
                    # Export to temp file using DuckDB
                    self.conn.execute(
                        f"COPY {table_name} TO '{temp_path}' (FORMAT PARQUET, COMPRESSION zstd)"
                    )

                    # Upload to GCS using gcsfs
                    from unified_pipeline.util.gcs_access import get_gcs_filesystem

                    fs = get_gcs_filesystem()
                    gcs_path_no_gs = gcs_path.replace("gs://", "")

                    with open(temp_path, "rb") as src:
                        with fs.open(gcs_path_no_gs, "wb") as dst:
                            import shutil

                            shutil.copyfileobj(src, dst)

                finally:
                    # Cleanup temp file
                    import os

                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                self.log.info(f"Saved processed data successfully for {silver_dataset_with_year}")

            except Exception as e:
                self.log.error(f"Error processing {layer_type} for year {year}: {e}")
                continue

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """
        Execute the silver processing job for all FVM WFS data.

        This method orchestrates the processing of raw multi-year data from the bronze
        layer into structured Geos. It processes Markblokke, Marker, and
        Smaabiotoper data for all available years and saves the results to Google Cloud Storage.

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        The processing workflow for each layer type and year:
        1. Read raw data from GCS or use in-memory data
        2. Process raw WFS data into Geos with standardized column names
        3. Add year and layer type information to the processed data
        4. Validate geometries and apply any needed transformations
        5. Save processed data back to GCS with year information

        Returns:
            Optional[Dict[str, Any]]: Summary information about processed datasets
                                    for potential gold layer usage, or None if processing fails
        """
        self.log.info("Running FVM WFS silver job for all available data")
        async with AsyncTimer("FVM WFS Silver Job"):
            # Process Markblokke data (field blocks) 2005-2026
            await self._process_layer_type(
                "Markblokke",
                self.config.markblokke_years,
                self.config.bronze_dataset_markblokke,
                self.config.dataset_markblokke,
                bronze_data,
            )

            # Process Marker data (field markers) 2008-2025
            await self._process_layer_type(
                "Marker",
                self.config.marker_years,
                self.config.bronze_dataset_marker,
                self.config.dataset_marker,
                bronze_data,
            )

            # Process Smaabiotoper data (small biotopes) 2023-2025
            await self._process_layer_type(
                "Smaabiotoper",
                self.config.smaabiotoper_years,
                f"{self.config.bronze_dataset_marker}_smaabiotoper",
                self.config.dataset_smaabiotoper,
                bronze_data,
            )

            self.log.info("FVM WFS silver job completed for all available data")

            # Return summary information for potential gold layer usage
            return {
                "dataset": self.config.dataset,
                "markblokke_years": self.config.markblokke_years,
                "marker_years": self.config.marker_years,
                "smaabiotoper_years": self.config.smaabiotoper_years,
                # ✅ MIGRATION: Use DuckDB current_timestamp instead of pandas
                "processed_at": self.conn.execute("SELECT current_timestamp")
                .fetchone()[0]
                .isoformat(),
                "status": "completed",
            }
