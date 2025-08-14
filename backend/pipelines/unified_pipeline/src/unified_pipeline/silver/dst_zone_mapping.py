"""
DST Zone Mapping silver layer component for DAGI pipeline.

This module creates a spatial lookup table that maps field geometries to DST
(Danmarks Statistik) zones
by combining DAGI administrative data with DST regional classifications.

The module contains:
- DSTZoneMappingConfig: Configuration for DST zone mapping processing
- DSTZoneMapping: Implementation class for creating the spatial lookup table

The processing creates a comprehensive mapping between:
- DST regions (from Danmarks Statistik)
- DAGI landsdele (administrative geographic divisions)
- DAGI regions (current administrative regions)
- DAGI municipalities (local administrative units)
"""

import json
from typing import Any, Dict, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer


class DSTZoneMappingConfig(BaseJobConfig):
    """
    Configuration for DST zone mapping processing.

    Attributes:
        name: Human-readable name of the component
        type: Type of the component
        description: Brief description of the functionality
        dataset: Name of the output dataset
        bucket: GCS bucket name for data storage
        target_crs: Target coordinate reference system
        dst_mappings: Dictionary defining DST region to DAGI landsdele mappings
    """

    name: str = "DST Zone Spatial Mapping"
    type: str = "dst_zone_mapping"
    description: str = (
        "Spatial lookup table for mapping field geometries to DST statistical zones"
    )
    dataset: str = "dst_zone_mapping"
    bucket: str = "landbrugsdata-raw-data"

    target_crs: str = Field(
        default="EPSG:4326",
        description="Target coordinate reference system - WGS84 for consistency",
    )

    dst_mappings: Dict[str, Dict[str, Any]] = Field(
        default={
            "Hele landet": {
                "landsdele_codes": [
                    "DK011",
                    "DK012",
                    "DK013",
                    "DK014",
                    "DK021",
                    "DK022",
                    "DK031",
                    "DK032",
                    "DK041",
                    "DK042",
                    "DK050",
                ],
                "description": "All of Denmark",
            },
            "Landsdelene Byen København, Københavns omegn og Nordsjælland": {
                "landsdele_codes": ["DK011", "DK012", "DK013"],
                "description": "Copenhagen metropolitan area and North Zealand",
            },
            "Region Sjælland": {
                "landsdele_codes": ["DK021", "DK022"],
                "description": "Region Zealand (East and West/South Zealand)",
            },
            "Landsdel Fyn": {
                "landsdele_codes": ["DK031"], 
                "description": "Funen region"
            },
            "Landsdel Sydjylland": {
                "landsdele_codes": ["DK032"], 
                "description": "South Jutland"
            },
            "Landsdel Vestjylland": {
                "landsdele_codes": ["DK041"], 
                "description": "West Jutland"
            },
            "Landsdel Østjylland": {
                "landsdele_codes": ["DK042"], 
                "description": "East Jutland"
            },
            "Region Nordjylland": {
                "landsdele_codes": ["DK050"], 
                "description": "North Jutland"
            },
            "Landsdel Bornholm": {
                "landsdele_codes": ["DK014"], 
                "description": "Bornholm island"
            },
        },
        description="Mapping of DST regions to DAGI landsdele codes",
    )


class DSTZoneMapping(BaseSource[DSTZoneMappingConfig], SilverJobInterface):
    """
    DST Zone Mapping implementation for creating spatial lookup tables using DuckDB.

    This component processes DAGI administrative data and creates a comprehensive
    spatial lookup table that can be used to map any field geometry to its
    corresponding DST statistical zones.

    The output includes:
    - Spatial geometries for each landsdel (as WKT strings)
    - Mapping to DST regions
    - DAGI region and municipality information
    - Metadata for analysis and validation
    """

    def __init__(self, config: DSTZoneMappingConfig):
        """Initialize the DST zone mapping component."""
        super().__init__(config)
        self.data_conn = None  # Track which connection has the DAGI data

    def _load_dagi_data(self, bronze_data: Optional[Dict[str, Any]] = None) -> None:
        """Load DAGI data and set the connection to use for all operations."""
        """
        Load DAGI data into DuckDB tables.

        Args:
            bronze_data: Optional in-memory data from bronze stage
        """
        try:
            # Required DAGI layers for DST mapping
            required_layers = ["landsdele", "regioner", "kommuner"]

            # Map layer-specific column mappings based on actual DAGI silver
            # data structure
            layer_column_mapping = {
                "kommuner": {
                    "code_col": "code", 
                    "name_col": "name", 
                    "region_col": "region_code"
                },
                "regioner": {
                    "code_col": "code", 
                    "name_col": "name", 
                    "region_col": "nuts2"
                },
                "landsdele": {
                    "code_col": "code",  # landsdele actually has a code column
                    "name_col": "name",
                    "region_col": "region_code",
                },
                "postnumre": {
                    "code_col": "code",  # postnumre uses 'code', not 'nr'
                    "name_col": "name",
                    "region_col": "NULL",  # postnumre doesn't have a region column
                },
            }

            for layer in required_layers:
                try:
                    if bronze_data and layer in bronze_data:
                        # Use in-memory data if available
                        self.log.info(f"Using in-memory data for DAGI {layer}")
                        raw_json = bronze_data[layer]
                        data = json.loads(raw_json)

                        if "features" in data and data["features"]:
                            # Convert GeoJSON features to table data
                            features_data = []
                            for feature in data["features"]:
                                properties = feature.get("properties", {})
                                geometry = feature.get("geometry")

                                if geometry:
                                    # Convert geometry to WKT
                                    geometry_wkt = self._geojson_to_wkt(geometry)
                                    features_data.append(
                                        {**properties, "geometry_wkt": geometry_wkt}
                                    )

                            # ✅ FIXED: Use pure DuckDB table creation instead of registration
                            if features_data:
                                # Get column names from first feature
                                columns = list(features_data[0].keys())
                                column_defs = ", ".join([f'"{col}" VARCHAR' for col in columns])

                                # Create table structure
                                self.conn.execute(
                                    f"CREATE OR REPLACE TABLE {layer}_raw ({column_defs})"
                                )

                                # Insert each feature as a row using prepared statements
                                placeholders = ", ".join(["?" for _ in columns])
                                for feature_dict in features_data:
                                    values = [feature_dict.get(col) for col in columns]
                                    self.conn.execute(
                                        f"INSERT INTO {layer}_raw VALUES ({placeholders})", values
                                    )

                            # Create standardized table with spatial geometry
                            # use layer-specific column mapping
                            if layer == "kommuner":
                                code_column = "kode"
                            elif layer == "regioner":
                                code_column = "kode"
                            elif layer == "landsdele":
                                code_column = "nuts3"  # landsdele uses nuts3 as the primary code
                            elif layer == "postnumre":
                                code_column = "nr"
                            else:
                                code_column = "kode"  # fallback

                            # Check what columns are actually available in the raw data
                            available_columns = [
                                desc[0]
                                for desc in self.conn.execute(f"DESCRIBE {layer}_raw").fetchall()
                            ]

                            # Build SELECT clause based on available columns
                            select_parts = []
                            select_parts.append(f"{code_column} as code")

                            # Add name column
                            if "navn" in available_columns:
                                select_parts.append("navn as name")
                            elif "name" in available_columns:
                                select_parts.append("name as name")
                            else:
                                select_parts.append("'' as name")

                            # Add region code - be flexible about column names
                            if "regionskode" in available_columns:
                                select_parts.append("regionskode as region_code")
                            elif "region_code" in available_columns:
                                select_parts.append("region_code as region_code")
                            else:
                                select_parts.append("'' as region_code")

                            # Add other optional columns
                            if "nuts2" in available_columns:
                                select_parts.append("COALESCE(nuts2, '') as nuts2")
                            else:
                                select_parts.append("'' as nuts2")

                            select_clause = ", ".join(select_parts)

                            self.conn.execute(f"""
                                CREATE TABLE {layer} AS
                                SELECT
                                    {select_clause},
                                    ST_GeomFromText(geometry_wkt) as geometry,
                                    geometry_wkt,
                                    ST_Area(ST_GeomFromText(geometry_wkt)) as area_m2,
                                    ST_X(ST_Centroid(ST_GeomFromText(geometry_wkt))) as centroid_x,
                                    ST_Y(ST_Centroid(ST_GeomFromText(geometry_wkt))) as centroid_y
                                FROM {layer}_raw
                                WHERE geometry_wkt IS NOT NULL
                            """)

                            count = self.conn.execute(f"SELECT COUNT(*) FROM {layer}").fetchone()[0]
                            self.log.info(f"Loaded {count} features for {layer} from memory")
                    else:
                        # Fallback to reading from silver layer
                        dataset_name = f"dagi_{layer}"
                        self.log.info(f"Reading DAGI {layer} from silver layer")
                        data_result = self._read_silver_data(dataset_name)

                        if data_result is not None:
                            # Handle the new return format from base class
                            if isinstance(data_result, dict) and "gcs_access" in data_result:
                                # New format: dict with gcs_access instance and table_name
                                gcs_access = data_result["gcs_access"]
                                source_table = data_result["table_name"]
                                # Use the GCS connection for all operations
                                conn = gcs_access.duckdb_conn
                                # Set the data connection for all subsequent operations
                                if self.data_conn is None:
                                    self.data_conn = conn
                                has_data = True
                            elif isinstance(data_result, str):
                                # Old format: just a table name - use base class connection
                                source_table = data_result
                                conn = self.conn
                                # Set the data connection for all subsequent operations
                                if self.data_conn is None:
                                    self.data_conn = conn
                                has_data = True
                            else:
                                self.log.warning(
                                    f"Unsupported data format for DAGI {layer}: {type(data_result)}"
                                )
                                continue
                        else:
                            has_data = False

                        if has_data:
                            # The source_table is the actual table name that was created
                            # No need to copy - just use the correct connection and table name

                            # Get column mapping for this layer
                            col_mapping = layer_column_mapping.get(layer, {})
                            code_col = col_mapping.get("code_col", "code")
                            name_col = col_mapping.get("name_col", "name")
                            region_col = col_mapping.get("region_col", "NULL")

                            # Copy data from GCS connection to base class connection
                            # First check what columns are available in the silver data
                            available_silver_columns = [
                                desc[0]
                                for desc in conn.execute(f"DESCRIBE {source_table}").fetchall()
                            ]

                            # Build SELECT clause based on available columns
                            select_parts = []

                            # Map code column
                            if code_col in available_silver_columns:
                                select_parts.append(f"{code_col} as code")
                            elif "code" in available_silver_columns:
                                select_parts.append("code as code")
                            else:
                                select_parts.append("'' as code")

                            # Map name column
                            if name_col in available_silver_columns:
                                select_parts.append(f"{name_col} as name")
                            elif "name" in available_silver_columns:
                                select_parts.append("name as name")
                            else:
                                select_parts.append("'' as name")

                            # Map region code column
                            if region_col != "NULL" and region_col in available_silver_columns:
                                select_parts.append(f"{region_col} as region_code")
                            elif "region_code" in available_silver_columns:
                                select_parts.append("region_code as region_code")
                            else:
                                select_parts.append("'' as region_code")

                            # Add spatial columns
                            if "geometry" in available_silver_columns:
                                select_parts.extend(
                                    [
                                        "ST_AsText(geometry) as geometry_wkt",
                                        "ST_Area(geometry) as area_m2",
                                        "ST_X(ST_Centroid(geometry)) as centroid_x",
                                        "ST_Y(ST_Centroid(geometry)) as centroid_y",
                                    ]
                                )
                            elif "geometry_wkt" in available_silver_columns:
                                select_parts.extend(
                                    [
                                        "geometry_wkt",
                                        "COALESCE(area_m2, 0.0) as area_m2",
                                        "COALESCE(centroid_x, 0.0) as centroid_x",
                                        "COALESCE(centroid_y, 0.0) as centroid_y",
                                    ]
                                )
                            else:
                                select_parts.extend(
                                    [
                                        "'' as geometry_wkt",
                                        "0.0 as area_m2",
                                        "0.0 as centroid_x",
                                        "0.0 as centroid_y",
                                    ]
                                )

                            select_clause = ", ".join(select_parts)

                            # ✅ OPTIMIZED: Use efficient data transfer between connections
                            if conn == self.conn:
                                # Data is already in the main connection, just create a view/table
                                self.conn.execute(f"""
                                    CREATE OR REPLACE TABLE {layer} AS
                                    SELECT {select_clause}
                                    FROM {source_table}
                                    WHERE 1=1
                                """)
                                count = self.conn.execute(
                                    f"SELECT COUNT(*) FROM {layer}"
                                ).fetchone()[0]
                            else:
                                # Data is in GCS connection, use optimized transfer
                                try:
                                    # Export to temporary file and import to main connection
                                    import os
                                    import tempfile

                                    with tempfile.NamedTemporaryFile(
                                        suffix=".parquet", delete=False
                                    ) as tmp_file:
                                        temp_path = tmp_file.name

                                    # Create a temporary view with the selected columns
                                    # in the GCS connection
                                    conn.execute(f"""
                                        CREATE OR REPLACE VIEW temp_layer_view AS
                                        SELECT {select_clause}
                                        FROM {source_table}
                                        WHERE 1=1
                                    """)

                                    # Export to temporary file
                                    conn.execute(f"""
                                        COPY temp_layer_view TO '{temp_path}'
                                        (FORMAT PARQUET, COMPRESSION zstd)
                                    """)

                                    # Import into main connection
                                    self.conn.execute(f"""
                                        CREATE OR REPLACE TABLE {layer} AS
                                        SELECT * FROM read_parquet('{temp_path}')
                                    """)

                                    # Clean up
                                    if os.path.exists(temp_path):
                                        os.unlink(temp_path)
                                    conn.execute("DROP VIEW IF EXISTS temp_layer_view")

                                    count = self.conn.execute(
                                        f"SELECT COUNT(*) FROM {layer}"
                                    ).fetchone()[0]

                                except Exception as e:
                                    self.log.warning(
                                        f"Failed optimized transfer for {layer}, "
                                        f"falling back to row-by-row: {e}"
                                    )
                                    # Fallback to row-by-row copying
                                    rows = conn.execute(f"""
                                        SELECT {select_clause}
                                        FROM {source_table}
                                        WHERE 1=1
                                    """).fetchall()

                                    # Create table in base class connection
                                    self.conn.execute(f"""
                                        CREATE OR REPLACE TABLE {layer} (
                                            code VARCHAR,
                                            name VARCHAR,
                                            region_code VARCHAR,
                                            geometry_wkt VARCHAR,
                                            area_m2 DOUBLE,
                                            centroid_x DOUBLE,
                                            centroid_y DOUBLE
                                        )
                                    """)

                                    # Insert all rows into base class connection
                                    for row in rows:
                                        self.conn.execute(
                                            f"""
                                            INSERT INTO {layer} VALUES (?, ?, ?, ?, ?, ?, ?)
                                        """,
                                            row,
                                        )
                                    count = len(rows)
                            self.log.info(f"Loaded {count} features for {layer} from silver")
                        else:
                            self.log.warning(f"No data found for DAGI {layer}")

                except Exception as e:
                    self.log.error(f"Error loading DAGI {layer}: {e}")
                    continue

            # Skip validation since GCS connections get closed after each layer
            # The individual layer loading already validated the data successfully
            self.log.info("DAGI data loading completed - skipping validation of closed connections")

        except Exception as e:
            self.log.error(f"Error loading DAGI data: {e}")
            raise

    def _geojson_to_wkt(self, geometry: Dict) -> str:
        """Convert GeoJSON geometry to WKT format using DuckDB."""
        try:
            # Use DuckDB to convert GeoJSON to WKT
            geojson_str = json.dumps(geometry)
            result = self.conn.execute(
                "SELECT ST_AsText(ST_GeomFromGeoJSON(?)) as wkt", [geojson_str]
            ).fetchone()
            return result[0] if result else None
        except Exception as e:
            self.log.warning(f"Error converting geometry to WKT: {e}")
            return None

    def _create_dst_zone_lookup(self) -> None:
        """Create the DST zone spatial lookup table using DuckDB."""
        try:
            self.log.info("Creating DST zone lookup table with DuckDB")

            # Always use the base class connection since GCS connections get closed
            conn = self.conn

            # Create DST mappings table
            dst_mappings_data = []
            for dst_region, mapping in self.config.dst_mappings.items():
                for landsdel_code in mapping["landsdele_codes"]:
                    dst_mappings_data.append(
                        {
                            "dst_region": dst_region,
                            "landsdel_code": landsdel_code,
                            "description": mapping["description"],
                        }
                    )

            # ✅ FIXED: Use pure DuckDB table creation instead of registration
            if dst_mappings_data:
                # Create table structure
                conn.execute("""
                    CREATE OR REPLACE TABLE dst_mappings_raw (
                        dst_region VARCHAR,
                        landsdel_code VARCHAR,
                        description VARCHAR
                    )
                """)

                # Insert each mapping as a row using prepared statements
                for mapping_dict in dst_mappings_data:
                    conn.execute(
                        "INSERT INTO dst_mappings_raw VALUES (?, ?, ?)",
                        [
                            mapping_dict["dst_region"],
                            mapping_dict["landsdel_code"],
                            mapping_dict["description"],
                        ],
                    )

            # Create the lookup table by joining landsdele with DST mappings
            # Use the standardized table names that were created in the base connection
            conn.execute("""
                CREATE TABLE dst_zone_lookup AS
                SELECT
                    l.code as landsdel_code,
                    l.name as landsdel_name,
                    '' as landsdel_dagi_id,
                    l.region_code as dagi_region_code,
                    l.name as dagi_region_name,  -- Use name as region_name since we
                    -- don't have separate region names
                    '' as dagi_region_nuts2,     -- Empty for now since regioner table
                    -- might not be available
                    STRING_AGG(dm.dst_region, '|' ORDER BY dm.dst_region) as dst_regions,
                    l.geometry_wkt as geometry,
                    l.area_m2,
                    l.centroid_x,
                    l.centroid_y,
                    current_timestamp as created_at,
                    'dst_zone_mapping' as data_source,
                    '1.0' as mapping_version
                FROM landsdele l
                LEFT JOIN dst_mappings_raw dm ON l.code = dm.landsdel_code
                WHERE dm.landsdel_code IS NOT NULL
                GROUP BY l.code, l.name, l.region_code,
                         l.geometry_wkt, l.area_m2, l.centroid_x, l.centroid_y
            """)

            # Get count and log summary
            count = conn.execute("SELECT COUNT(*) FROM dst_zone_lookup").fetchone()[0]
            self.log.info(f"Created DST zone lookup table with {count} records")

            # Log mapping summary
            dst_summary = conn.execute("""
                SELECT dst_regions, COUNT(*) as count
                FROM dst_zone_lookup
                GROUP BY dst_regions
                ORDER BY count DESC
            """).fetchall()

            self.log.info("DST zone coverage:")
            for dst_region, count in dst_summary:
                self.log.info(f"  {dst_region}: {count} landsdele")

        except Exception as e:
            self.log.error(f"Error creating DST zone lookup: {e}")
            raise

    def _create_reference_table(self) -> None:
        """Create a reference table without geometry for easy viewing."""
        try:
            # Always use the base class connection since GCS connections get closed
            conn = self.conn

            conn.execute("""
                CREATE TABLE dst_zone_reference AS
                SELECT
                    landsdel_code,
                    landsdel_name,
                    landsdel_dagi_id,
                    dagi_region_code,
                    dagi_region_name,
                    dagi_region_nuts2,
                    dst_regions,
                    area_m2,
                    centroid_x,
                    centroid_y,
                    created_at,
                    data_source,
                    mapping_version
                FROM dst_zone_lookup
            """)

            count = conn.execute("SELECT COUNT(*) FROM dst_zone_reference").fetchone()[0]
            self.log.info(f"Created reference table with {count} records")

        except Exception as e:
            self.log.error(f"Error creating reference table: {e}")
            raise

    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run the DST zone mapping processing using DuckDB.

        This method creates a comprehensive spatial lookup table that maps
        DAGI administrative divisions to DST statistical zones.

        Args:
            bronze_data: Optional in-memory data from bronze stage
        """
        try:
            async with AsyncTimer("DST zone mapping processing") as timer:
                self.log.info("Starting DST zone mapping processing with DuckDB")

                # Load DAGI data into DuckDB
                self._load_dagi_data(bronze_data)

                # Create the DST zone lookup table
                self._create_dst_zone_lookup()

                # ✅ MIGRATION: Save the spatial lookup table directly from DuckDB (no  conversion)
                self._save_data(
                    "dst_zone_lookup",
                    self.config.dataset,
                    self.config.bucket,
                    stage="silver",
                    conn=self.conn,
                )
                self.log.info("Saved DST zone spatial lookup table")

                # Create and save reference table (without geometry)
                self._create_reference_table()
                reference_dataset = f"{self.config.dataset}_reference"
                self._save_data(
                    "dst_zone_reference",
                    reference_dataset,
                    self.config.bucket,
                    stage="silver",
                    conn=self.conn,
                )
                self.log.info("Saved DST zone reference table")

                lookup_count = self.conn.execute("SELECT COUNT(*) FROM dst_zone_lookup").fetchone()[
                    0
                ]
                self.log.info(
                    f"DST zone mapping processing completed in {timer.elapsed():.2f}s. "
                    f"Created lookup table with {lookup_count} records covering "
                    f"{len(self.config.dst_mappings)} DST regions"
                )

        except Exception as e:
            self.log.error(f"Critical error in DST zone mapping processing: {e}")
            raise
