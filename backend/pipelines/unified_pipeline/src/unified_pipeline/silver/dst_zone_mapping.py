"""
DST Zone Mapping silver layer component for DAGI pipeline.

This module creates a spatial lookup table that maps field geometries to DST (Danmarks Statistik) zones
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

import duckdb
from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
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
    description: str = "Spatial lookup table for mapping field geometries to DST statistical zones"
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
            "Landsdel Fyn": {"landsdele_codes": ["DK031"], "description": "Funen region"},
            "Landsdel Sydjylland": {"landsdele_codes": ["DK032"], "description": "South Jutland"},
            "Landsdel Vestjylland": {"landsdele_codes": ["DK041"], "description": "West Jutland"},
            "Landsdel Østjylland": {"landsdele_codes": ["DK042"], "description": "East Jutland"},
            "Region Nordjylland": {"landsdele_codes": ["DK050"], "description": "North Jutland"},
            "Landsdel Bornholm": {"landsdele_codes": ["DK014"], "description": "Bornholm island"},
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

    def __init__(self, config: DSTZoneMappingConfig, gcs_util: GCSUtil):
        """Initialize the DST zone mapping component."""
        super().__init__(config, gcs_util)
        self.conn = duckdb.connect()
        self._configure_duckdb()
        self.data_conn = None  # Track which connection has the DAGI data

    def _configure_duckdb(self):
        """Configure DuckDB with spatial extensions."""
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

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

            # Map layer-specific column mappings based on actual DAGI silver data structure
            layer_column_mapping = {
                "kommuner": {"code_col": "code", "name_col": "name", "region_col": "region_code"},
                "regioner": {"code_col": "code", "name_col": "name", "region_col": "nuts2"},
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

                            # Create standardized table with spatial geometry - use layer-specific column mapping
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

                            self.conn.execute(f"""
                                CREATE TABLE {layer} AS
                                SELECT 
                                    {code_column} as code,
                                    navn as name,
                                    regionskode as region_code,
                                    regionsnavn as region_name,
                                    COALESCE(nuts2, '') as nuts2,
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

                            # Create standardized table using the connection where the data exists
                            conn.execute(f"""
                                CREATE TABLE {layer} AS
                                SELECT 
                                    {code_col} as code,
                                    {name_col} as name,
                                    {region_col} as region_code,
                                    geometry,
                                    ST_AsText(geometry) as geometry_wkt,
                                    ST_Area(geometry) as area_m2,
                                    ST_X(ST_Centroid(geometry)) as centroid_x,
                                    ST_Y(ST_Centroid(geometry)) as centroid_y
                                FROM {source_table}
                                WHERE geometry IS NOT NULL
                            """)

                            count = conn.execute(f"SELECT COUNT(*) FROM {layer}").fetchone()[0]
                            self.log.info(f"Loaded {count} features for {layer} from silver")
                        else:
                            self.log.warning(f"No data found for DAGI {layer}")

                except Exception as e:
                    self.log.error(f"Error loading DAGI {layer}: {e}")
                    continue

            # Validate that we have all required data using the connection that has the data
            for layer in required_layers:
                try:
                    if self.data_conn is not None:
                        count = self.data_conn.execute(f"SELECT COUNT(*) FROM {layer}").fetchone()[
                            0
                        ]
                    else:
                        count = self.conn.execute(f"SELECT COUNT(*) FROM {layer}").fetchone()[0]
                    if count == 0:
                        raise ValueError(f"No data loaded for {layer}")
                except:
                    raise ValueError(f"Missing required DAGI layer: {layer}")

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

            # Use the connection that has the DAGI data
            conn = self.data_conn if self.data_conn is not None else self.conn

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
            self.conn.execute("""
                CREATE TABLE dst_zone_lookup AS
                SELECT 
                    l.code as landsdel_code,
                    l.name as landsdel_name,
                    '' as landsdel_dagi_id,
                    l.region_code as dagi_region_code,
                    l.region_name as dagi_region_name,
                    COALESCE(r.nuts2, '') as dagi_region_nuts2,
                    STRING_AGG(dm.dst_region, '|' ORDER BY dm.dst_region) as dst_regions,
                    l.geometry_wkt as geometry,
                    l.area_m2,
                    l.centroid_x,
                    l.centroid_y,
                    current_timestamp as created_at,
                    'dst_zone_mapping' as data_source,
                    '1.0' as mapping_version
                FROM landsdele l
                LEFT JOIN regioner r ON l.region_code = r.code
                LEFT JOIN dst_mappings_raw dm ON l.code = dm.landsdel_code
                WHERE dm.landsdel_code IS NOT NULL
                GROUP BY l.code, l.name, l.region_code, l.region_name, r.nuts2, 
                         l.geometry_wkt, l.area_m2, l.centroid_x, l.centroid_y
            """)

            # Get count and log summary
            count = self.conn.execute("SELECT COUNT(*) FROM dst_zone_lookup").fetchone()[0]
            self.log.info(f"Created DST zone lookup table with {count} records")

            # Log mapping summary
            dst_summary = self.conn.execute("""
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
            self.conn.execute("""
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

            count = self.conn.execute("SELECT COUNT(*) FROM dst_zone_reference").fetchone()[0]
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
