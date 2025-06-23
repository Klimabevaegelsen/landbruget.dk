#!/usr/bin/env python3
"""
Agricultural Field Overview Pipeline

This pipeline creates comprehensive field overviews for each year including:
- geometry
- field id + block id (unique identifier)
- user (CVR)
- area
- crop
- production (using DST mappings)

Optimized for DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for maximum performance.
"""

import argparse
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd

warnings.filterwarnings("ignore")

# Add the parent directory to the path to import common modules
sys.path.append(str(Path(__file__).parent.parent.parent))
from common.storage_interface import GCSStorage

# Import the DST mapping table from the project root
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from dst_field_crop_mapping_table import get_dst_category


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class FieldProductionEstimator:
    """Class to handle field production estimation using DST data - Optimized for DuckDB Spatial v1.2.2."""

    def __init__(self, gcs_storage: Optional[GCSStorage] = None):
        """Initialize the estimator with DST data and optimized spatial setup."""
        self.gcs_storage = gcs_storage
        self.dst_cache_dir = Path("data_cache/dst_pipeline")
        self.dst_data = self._load_dst_data()
        self.dst_zone_mapping = self._load_dst_zone_mapping()

        # Initialize optimized DuckDB connection for spatial operations
        self.spatial_conn = None
        self._setup_optimized_spatial_connection()

    def _load_dst_data(self):
        """Load and process DST data from all tables."""
        dst_data = {}

        # Load each DST table with its specific measurement units
        dst_tables = {
            "HST77": ["Gennemsnitsudbytte, hkg pr. hektar"],
            "GARTN1": ["Produktion, tons", "Dyrket areal, hektar", "Høstet areal, hektar"],
            "FRO": ["Gennemsnitsudbytte, hkg pr. hektar"],
            "HALM1": ["Mængde (mio. kilo)", "Areal (1000 hektar)"],
        }

        for table_name, measurement_units in dst_tables.items():
            try:
                # Try local cache first
                table_path = self.dst_cache_dir / f"{table_name.lower()}_processed.parquet"
                df = None

                if table_path.exists():
                    df = pd.read_parquet(table_path)
                    logging.info(f"Loaded {table_name} from local cache: {len(df)} records")
                else:
                    # Try to load from GCS
                    try:
                        import subprocess

                        # Get the most recent DST data folder
                        cmd = "gsutil ls gs://landbrugsdata-raw-data/silver/dst/ | sort | tail -1"
                        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

                        if result.returncode == 0 and result.stdout.strip():
                            latest_folder = result.stdout.strip().rstrip("/")
                            gcs_path = f"{latest_folder}/{table_name.lower()}_processed.parquet"

                            logging.info(f"Loading {table_name} from GCS: {gcs_path}")
                            df = pd.read_parquet(gcs_path)
                            logging.info(f"✅ Loaded {table_name} from GCS: {len(df)} records")
                        else:
                            logging.warning(f"Could not find {table_name} data in GCS")

                    except Exception as gcs_e:
                        logging.warning(f"Failed to load {table_name} from GCS: {gcs_e}")

                if df is not None:
                    # Filter for the relevant measurement units
                    if "measurement_unit" in df.columns:
                        df_filtered = df[df["measurement_unit"].isin(measurement_units)]
                    else:
                        df_filtered = df

                    dst_data[table_name] = df_filtered
                    logging.info(f"Processed {table_name}: {len(df_filtered)} records after filtering")
                else:
                    logging.warning(f"{table_name} file not found locally or in GCS")
                    dst_data[table_name] = pd.DataFrame()

            except Exception as e:
                logging.error(f"Error loading {table_name}: {e}")
                dst_data[table_name] = pd.DataFrame()

        return dst_data

    def _load_dst_zone_mapping(self) -> Optional[gpd.GeoDataFrame]:
        """Load DST zone mapping for regional yield data."""
        # Try local cache first
        local_path = "data_cache/dst_zone_mapping/data.parquet"

        try:
            if Path(local_path).exists():
                zone_mapping = gpd.read_parquet(local_path)
                logging.info(f"Loaded DST zone mapping from local cache: {len(zone_mapping)} zones")
                return zone_mapping
        except Exception as e:
            logging.warning(f"Failed to load local DST zone mapping: {e}")

        # Try to load from GCS
        try:
            import subprocess

            # Get the most recent DST zone mapping folder
            cmd = "gsutil ls gs://landbrugsdata-raw-data/silver/dst_zone_mapping/ | sort | tail -1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0 and result.stdout.strip():
                latest_folder = result.stdout.strip().rstrip("/")
                gcs_path = f"{latest_folder}/data.parquet"

                logging.info(f"Loading DST zone mapping from GCS: {gcs_path}")
                zone_mapping = gpd.read_parquet(gcs_path)
                logging.info(f"✅ Loaded DST zone mapping from GCS: {len(zone_mapping)} zones")
                return zone_mapping
            else:
                logging.warning("DST zone mapping not found in GCS, using national averages only")

        except Exception as e:
            logging.warning(f"Failed to load DST zone mapping from GCS: {e}")

        logging.warning("DST zone mapping not found locally or in GCS, using national averages only")
        return None

    def _setup_optimized_spatial_connection(self):
        """Setup optimized DuckDB connection with spatial extension and indexed zone data."""
        if self.dst_zone_mapping is None:
            return

        try:
            import duckdb

            # Create optimized DuckDB connection
            self.spatial_conn = duckdb.connect()

            # Configure DuckDB for optimal spatial performance
            self.spatial_conn.execute("SET memory_limit = '8GB'")
            self.spatial_conn.execute("SET threads = 4")
            self.spatial_conn.execute("SET enable_progress_bar = true")
            self.spatial_conn.execute("SET preserve_insertion_order = false")

            # Install and load spatial extension
            self.spatial_conn.execute("INSTALL spatial")
            self.spatial_conn.execute("LOAD spatial")

            # Verify DuckDB Spatial version to ensure SPATIAL_JOIN operator support
            try:
                version_result = self.spatial_conn.execute(
                    "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
                ).fetchone()
                if version_result:
                    logging.info(f"DuckDB Spatial version: {version_result[1]}")
                    # SPATIAL_JOIN operator was introduced in v1.2.2+ according to PR #545
                    if version_result[1] >= "1.2.2":
                        logging.info("✅ SPATIAL_JOIN operator should be available")
                    else:
                        logging.warning(f"⚠️  SPATIAL_JOIN operator may not be available in version {version_result[1]}")
                else:
                    logging.warning("Could not detect spatial extension version")
            except Exception as e:
                logging.warning(f"Could not verify spatial extension version: {e}")

            # Create optimized permanent table for DST zones with native geometry column
            logging.info("Creating optimized DST zones table with spatial index...")

            self.spatial_conn.execute("""
                CREATE TABLE dst_zones_optimized (
                    zone_id INTEGER,
                    zone_name VARCHAR,
                    geometry GEOMETRY,
                    bbox_minx DOUBLE,
                    bbox_miny DOUBLE,
                    bbox_maxx DOUBLE,
                    bbox_maxy DOUBLE
                )
            """)

            # Prepare zone data with precomputed bounding boxes
            zone_data = []
            for idx, zone in self.dst_zone_mapping.iterrows():
                # Extract zone name from dst_regions or landsdel_name
                if "dst_regions" in zone.index and zone["dst_regions"]:
                    dst_regions = zone["dst_regions"]
                    zones = [z.strip() for z in dst_regions.split("|")]
                    zone_name = next((z for z in zones if z != "Hele landet"), "Hele landet")
                elif "landsdel_name" in zone.index:
                    zone_name = zone["landsdel_name"]
                else:
                    zone_name = "Unknown"

                # Get bounding box for spatial index optimization
                bounds = zone.geometry.bounds

                zone_data.append(
                    {
                        "zone_id": idx,
                        "zone_name": zone_name,
                        "geometry": zone.geometry.wkb,
                        "bbox_minx": bounds[0],
                        "bbox_miny": bounds[1],
                        "bbox_maxx": bounds[2],
                        "bbox_maxy": bounds[3],
                    }
                )

            # Insert zone data using batch operations
            insert_query = """
                INSERT INTO dst_zones_optimized (zone_id, zone_name, geometry, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy)
                VALUES (?, ?, ST_GeomFromWKB(?), ?, ?, ?, ?)
            """

            batch_data = [
                (
                    z["zone_id"],
                    z["zone_name"],
                    z["geometry"],
                    z["bbox_minx"],
                    z["bbox_miny"],
                    z["bbox_maxx"],
                    z["bbox_maxy"],
                )
                for z in zone_data
            ]

            self.spatial_conn.executemany(insert_query, batch_data)

            # Analyze table for query optimization
            self.spatial_conn.execute("ANALYZE dst_zones_optimized")

            logging.info(f"Setup optimized DuckDB spatial connection with {len(zone_data)} zones")
            logging.info("✅ Ready for high-performance SPATIAL_JOIN operations")

        except Exception as e:
            logging.error(f"Failed to setup optimized spatial connection: {e}")
            self.spatial_conn = None

    def get_optimized_batch_spatial_yields(self, fields_batch: List[Dict], year: int) -> Dict[str, Dict]:
        """Get yield estimates using optimized DuckDB SPATIAL_JOIN operator."""
        if self.spatial_conn is None or not fields_batch:
            return {}

        try:
            logging.info(f"Processing batch of {len(fields_batch)} fields with optimized SPATIAL_JOIN...")

            # Create temporary table for this batch using native geometry column
            self.spatial_conn.execute("DROP TABLE IF EXISTS temp_fields_batch")
            self.spatial_conn.execute("""
                CREATE TABLE temp_fields_batch (
                    field_id VARCHAR,
                    crop_type VARCHAR,
                    area_ha DOUBLE,
                    geometry GEOMETRY
                )
            """)

            # Prepare batch data with WKB for efficient geometry storage
            batch_data = []
            for field in fields_batch:
                if hasattr(field["geometry"], "wkb"):
                    geom_wkb = field["geometry"].wkb
                else:
                    continue

                batch_data.append((field["field_id"], field["crop_type"], field["area_ha"], geom_wkb))

            # Batch insert using WKB for optimal performance
            insert_query = """
                INSERT INTO temp_fields_batch (field_id, crop_type, area_ha, geometry)
                VALUES (?, ?, ?, ST_GeomFromWKB(?))
            """
            self.spatial_conn.executemany(insert_query, batch_data)

            # CRITICAL: Use ONLY ST_Intersects condition to trigger SPATIAL_JOIN operator
            # According to PR #545 (https://github.com/duckdb/duckdb-spatial/pull/545):
            # "The spatial join operator only supports a single join condition for now"
            # This means we can ONLY use ST_Intersects() - no additional WHERE clauses or complex conditions
            optimized_spatial_join_query = """
            SELECT 
                f.field_id,
                f.crop_type,
                f.area_ha,
                z.zone_name
            FROM temp_fields_batch f
            INNER JOIN dst_zones_optimized z 
                ON ST_Intersects(f.geometry, z.geometry)
            """

            # Check if SPATIAL_JOIN operator is being used
            spatial_join_detected = False
            try:
                explain_result = self.spatial_conn.execute(f"EXPLAIN {optimized_spatial_join_query}").fetchall()
                plan_text = "\n".join([str(row[1]) for row in explain_result])

                if "SPATIAL_JOIN" in plan_text:
                    logging.info("✅ SPATIAL_JOIN operator detected in query plan!")
                    spatial_join_detected = True
                else:
                    logging.warning("⚠️  SPATIAL_JOIN operator not detected - using fallback join")
                    logging.warning("Query plan preview:")
                    for i, row in enumerate(explain_result[:5]):  # Show first 5 lines
                        logging.warning(f"  {row[1]}")
                    if len(explain_result) > 5:
                        logging.warning("  ...")

            except Exception as e:
                logging.warning(f"Could not analyze query plan: {e}")

            # Execute the spatial join
            intersection_results = self.spatial_conn.execute(optimized_spatial_join_query).fetchall()

            # Log performance metrics
            if spatial_join_detected:
                logging.info(f"✅ SPATIAL_JOIN processed {len(intersection_results)} intersections")
            else:
                logging.warning(f"⚠️  Fallback join processed {len(intersection_results)} intersections")
            logging.info(f"Found {len(intersection_results)} spatial intersections")

            # Smart yield estimation: simple for single zones, area-weighted for multiple zones
            field_intersections = {}
            for field_id, crop_type, area_ha, zone_name in intersection_results:
                if field_id not in field_intersections:
                    field_intersections[field_id] = {"crop_type": crop_type, "area_ha": area_ha, "zones": []}
                field_intersections[field_id]["zones"].append(zone_name)

            single_zone_fields = []
            multi_zone_fields = []

            for field_id, field_data in field_intersections.items():
                if len(field_data["zones"]) == 1:
                    single_zone_fields.append((field_id, field_data))
                else:
                    multi_zone_fields.append((field_id, field_data))

            logging.info(f"  Single zone fields: {len(single_zone_fields)} (fast processing)")
            logging.info(f"  Multi zone fields: {len(multi_zone_fields)} (area-weighted processing)")

            field_yields = {}

            # Process single-zone fields (fast)
            for field_id, field_data in single_zone_fields:
                crop_type = field_data["crop_type"]
                zone_name = field_data["zones"][0]

                dst_info = get_dst_category(crop_type)
                if not dst_info:
                    continue

                dst_table = dst_info["dst_table"]
                dst_category = dst_info["dst_category"]

                if dst_table not in self.dst_data:
                    continue

                dst_df = self.dst_data[dst_table]
                zone_yield = self._get_zone_yield(dst_df, dst_table, dst_category, year, zone_name)

                if zone_yield is not None:
                    field_yields[field_id] = {
                        "yield_value": zone_yield["yield_value"],
                        "yield_unit": "hkg/ha",
                        "source_table": dst_table,
                        "source_unit": f"Single zone: {zone_name}",
                        "conversion_applied": zone_yield.get("conversion_applied"),
                        "estimation_method": "dst_mapping_single_zone",
                    }

            # Process multi-zone fields (area-weighted)
            if multi_zone_fields:
                logging.info(f"  Calculating area-weighted yields for {len(multi_zone_fields)} multi-zone fields...")

                for field_id, field_data in multi_zone_fields:
                    crop_type = field_data["crop_type"]
                    zones = field_data["zones"]

                    dst_info = get_dst_category(crop_type)
                    if not dst_info:
                        continue

                    dst_table = dst_info["dst_table"]
                    dst_category = dst_info["dst_category"]

                    if dst_table not in self.dst_data:
                        continue

                    dst_df = self.dst_data[dst_table]

                    try:
                        area_weighted_result = self._calculate_area_weighted_yield(
                            field_id, zones, dst_df, dst_table, dst_category, year
                        )

                        if area_weighted_result:
                            field_yields[field_id] = area_weighted_result

                    except Exception as e:
                        logging.warning(f"Area weighting failed for {field_id}: {e}")
                        # Fallback to first zone
                        zone_yield = self._get_zone_yield(dst_df, dst_table, dst_category, year, zones[0])
                        if zone_yield is not None:
                            field_yields[field_id] = {
                                "yield_value": zone_yield["yield_value"],
                                "yield_unit": "hkg/ha",
                                "source_table": dst_table,
                                "source_unit": f"Fallback to first zone: {zones[0]}",
                                "conversion_applied": zone_yield.get("conversion_applied"),
                                "estimation_method": "dst_mapping_fallback",
                            }

            # Clean up temporary table
            self.spatial_conn.execute("DROP TABLE temp_fields_batch")

            logging.info(f"✅ Processed {len(field_yields)} fields with yield estimates using optimized SPATIAL_JOIN")
            return field_yields

        except Exception as e:
            logging.error(f"Optimized batch spatial join failed: {e}")
            import traceback

            traceback.print_exc()
            return {}

    def _calculate_area_weighted_yield(
        self, field_id: str, zones: List[str], dst_df: pd.DataFrame, dst_table: str, dst_category: str, year: int
    ) -> Optional[Dict[str, any]]:
        """Calculate area-weighted yield for a field intersecting multiple zones."""
        try:
            # CRITICAL: For SPATIAL_JOIN operator compliance, we need to avoid multiple conditions
            # We'll create a temporary table with just this field and use pure spatial join
            self.spatial_conn.execute("DROP TABLE IF EXISTS temp_single_field")
            self.spatial_conn.execute(
                """
                CREATE TABLE temp_single_field AS 
                SELECT field_id, geometry 
                FROM temp_fields_batch 
                WHERE field_id = ?
            """,
                [field_id],
            )

            # Pure spatial join query - ONLY ST_Intersects condition to trigger SPATIAL_JOIN
            area_query = """
            SELECT 
                z.zone_name,
                ST_Area_Spheroid(ST_Intersection(f.geometry, z.geometry)) as intersection_area_m2
            FROM temp_single_field f
            JOIN dst_zones_optimized z ON ST_Intersects(f.geometry, z.geometry)
            """

            area_results = self.spatial_conn.execute(area_query).fetchall()

            # Filter results to only the zones we care about (post-processing, not in SQL)
            filtered_results = [
                (zone_name, intersection_area) for zone_name, intersection_area in area_results if zone_name in zones
            ]

            if not filtered_results:
                # Clean up and return None
                self.spatial_conn.execute("DROP TABLE temp_single_field")
                return None

            # Calculate area-weighted yield
            total_weighted_yield = 0
            total_intersection_area = 0
            yield_sources = []

            for zone_name, intersection_area in filtered_results:
                if intersection_area > 0:
                    zone_yield = self._get_zone_yield(dst_df, dst_table, dst_category, year, zone_name)

                    if zone_yield is not None:
                        weighted_yield = zone_yield["yield_value"] * intersection_area
                        total_weighted_yield += weighted_yield
                        total_intersection_area += intersection_area
                        yield_sources.append(f"{zone_name}({intersection_area:.0f}m²)")

            # Clean up temporary table
            self.spatial_conn.execute("DROP TABLE temp_single_field")

            if total_intersection_area > 0:
                weighted_average_yield = total_weighted_yield / total_intersection_area

                return {
                    "yield_value": weighted_average_yield,
                    "yield_unit": "hkg/ha",
                    "source_table": dst_table,
                    "source_unit": f"Area-weighted from: {', '.join(yield_sources)}",
                    "conversion_applied": "Area-weighted spatial average",
                    "estimation_method": "dst_mapping_area_weighted",
                }

            return None

        except Exception as e:
            logging.error(f"Error in area weighting for {field_id}: {e}")
            # Clean up on error
            try:
                self.spatial_conn.execute("DROP TABLE IF EXISTS temp_single_field")
            except:
                pass
            return None

    def _get_zone_yield(
        self, dst_df: pd.DataFrame, dst_table: str, dst_category: str, year: int, zone_name: str
    ) -> Optional[Dict[str, any]]:
        """Get yield data for a specific zone and crop."""
        # Handle different data formats by table
        if dst_table == "HST77":
            return self._get_hst77_yield(dst_df, dst_category, year, zone_name)
        elif dst_table == "FRO":
            return self._get_fro_yield(dst_df, dst_category, year, zone_name)
        elif dst_table == "GARTN1":
            return self._get_gartn1_yield(dst_df, dst_category, year, zone_name)
        elif dst_table == "HALM1":
            return self._get_halm1_yield(dst_df, dst_category, year, zone_name)

        return None

    def _get_hst77_yield(
        self, dst_df: pd.DataFrame, dst_category: str, year: int, region: str
    ) -> Optional[Dict[str, any]]:
        """Get HST77 yield data in hkg/ha."""
        mask = (
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"] == region)
            & (dst_df["measurement_unit"] == "Gennemsnitsudbytte, hkg pr. hektar")
        )
        matches = dst_df[mask]

        if matches.empty and region != "Hele landet":
            mask = (
                (dst_df["crop_type"] == dst_category)
                & (dst_df["year"] == year)
                & (dst_df["region"] == "Hele landet")
                & (dst_df["measurement_unit"] == "Gennemsnitsudbytte, hkg pr. hektar")
            )
            matches = dst_df[mask]

        if not matches.empty:
            return {
                "yield_value": matches["value"].iloc[0],
                "yield_unit": "hkg/ha",
                "source_table": "HST77",
                "source_unit": "Gennemsnitsudbytte, hkg pr. hektar",
                "conversion_applied": None,
            }
        return None

    def _get_fro_yield(
        self, dst_df: pd.DataFrame, dst_category: str, year: int, region: str
    ) -> Optional[Dict[str, any]]:
        """Get FRO yield data in hkg/ha."""
        mask = (
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"] == region)
            & (dst_df["measurement_unit"] == "Gennemsnitsudbytte, hkg pr. hektar")
        )
        matches = dst_df[mask]

        if matches.empty and region != "Hele landet":
            mask = (
                (dst_df["crop_type"] == dst_category)
                & (dst_df["year"] == year)
                & (dst_df["region"] == "Hele landet")
                & (dst_df["measurement_unit"] == "Gennemsnitsudbytte, hkg pr. hektar")
            )
            matches = dst_df[mask]

        if not matches.empty:
            return {
                "yield_value": matches["value"].iloc[0],
                "yield_unit": "hkg/ha",
                "source_table": "FRO",
                "source_unit": "Gennemsnitsudbytte, hkg pr. hektar",
                "conversion_applied": None,
            }
        return None

    def _get_gartn1_yield(
        self, dst_df: pd.DataFrame, dst_category: str, year: int, region: str
    ) -> Optional[Dict[str, any]]:
        """Calculate yield from GARTN1 production and area data."""
        production_mask = (
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"] == region)
            & (dst_df["measurement_unit"] == "Produktion, tons")
        )
        production_data = dst_df[production_mask]

        for area_unit in ["Høstet areal, hektar", "Dyrket areal, hektar"]:
            area_mask = (
                (dst_df["crop_type"] == dst_category)
                & (dst_df["year"] == year)
                & (dst_df["region"] == region)
                & (dst_df["measurement_unit"] == area_unit)
            )
            area_data = dst_df[area_mask]

            if not production_data.empty and not area_data.empty:
                production_tons = production_data["value"].iloc[0]
                area_ha = area_data["value"].iloc[0]

                if area_ha > 0:
                    yield_tons_ha = production_tons / area_ha
                    yield_hkg_ha = yield_tons_ha * 10

                    return {
                        "yield_value": yield_hkg_ha,
                        "yield_unit": "hkg/ha",
                        "source_table": "GARTN1",
                        "source_unit": f"Calculated from {area_unit} and Produktion, tons",
                        "conversion_applied": "1 ton = 10 hkg",
                    }

        if region != "Hele landet":
            return self._get_gartn1_yield(dst_df, dst_category, year, "Hele landet")

        return None

    def _get_halm1_yield(
        self, dst_df: pd.DataFrame, dst_category: str, year: int, region: str
    ) -> Optional[Dict[str, any]]:
        """Calculate yield from HALM1 quantity and area data."""
        quantity_mask = (
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"] == region)
            & (dst_df["measurement_unit"] == "Mængde (mio. kilo)")
        )
        quantity_data = dst_df[quantity_mask]

        area_mask = (
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"] == region)
            & (dst_df["measurement_unit"] == "Areal (1000 hektar)")
        )
        area_data = dst_df[area_mask]

        if not quantity_data.empty and not area_data.empty:
            quantity_mio_kilo = quantity_data["value"].iloc[0]
            area_1000_ha = area_data["value"].iloc[0]

            if area_1000_ha > 0:
                yield_hkg_ha = (quantity_mio_kilo * 10000) / (area_1000_ha * 1000)

                return {
                    "yield_value": yield_hkg_ha,
                    "yield_unit": "hkg/ha",
                    "source_table": "HALM1",
                    "source_unit": "Calculated from Mængde (mio. kilo) and Areal (1000 hektar)",
                    "conversion_applied": "1 mio. kilo = 10,000 hkg, 1000 hektar = 1000 ha",
                }

        if region != "Hele landet":
            return self._get_halm1_yield(dst_df, dst_category, year, "Hele landet")

        return None

    def calculate_production(self, area_ha: float, yield_hkg_ha: float) -> float:
        """Calculate total production."""
        return area_ha * yield_hkg_ha


def load_agricultural_fields(year: int, gcs_storage: Optional[GCSStorage] = None) -> gpd.GeoDataFrame:
    """Load agricultural fields data for a specific year."""
    if gcs_storage:
        # In production, load from GCS silver layer with correct bucket and path structure
        try:
            import subprocess

            # Find the most recent timestamped folder for this year
            cmd = f"gsutil ls gs://landbrugsdata-raw-data/silver/agricultural_fields_{year}/ | sort | tail -1"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode != 0 or not result.stdout.strip():
                raise FileNotFoundError(f"No agricultural fields data found for {year} in GCS")

            latest_folder = result.stdout.strip().rstrip("/")
            gcs_path = f"{latest_folder}/data.parquet"

            logging.info(f"Found latest data folder: {latest_folder}")
            logging.info(f"Loading from GCS: {gcs_path}")

            # Load directly from GCS using geopandas
            gdf = gpd.read_parquet(gcs_path)
            logging.info(f"✅ Loaded {len(gdf):,} fields for {year} from GCS")

            return gdf

        except Exception as e:
            logging.error(f"Failed to load from GCS: {e}")
            raise
    else:
        # Local development - try local cache first
        file_path = f"data_cache/agricultural_fields/agricultural_fields_{year}_data.parquet"

        if Path(file_path).exists():
            gdf = gpd.read_parquet(file_path)
            logging.info(f"Loaded {len(gdf):,} fields for {year} from local cache")
            return gdf
        else:
            # Try to load directly from GCS even in local mode
            try:
                import subprocess

                cmd = f"gsutil ls gs://landbrugsdata-raw-data/silver/agricultural_fields_{year}/ | sort | tail -1"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

                if result.returncode == 0 and result.stdout.strip():
                    latest_folder = result.stdout.strip().rstrip("/")
                    gcs_path = f"{latest_folder}/data.parquet"

                    logging.info(f"Local cache not found, loading from GCS: {gcs_path}")
                    gdf = gpd.read_parquet(gcs_path)
                    logging.info(f"✅ Loaded {len(gdf):,} fields for {year} from GCS")
                    return gdf

            except Exception as gcs_e:
                logging.warning(f"Failed to load from GCS: {gcs_e}")

            raise FileNotFoundError(
                f"Agricultural fields data for {year} not found in local cache ({file_path}) or GCS"
            )


def create_field_production_optimized(
    year: int, estimator: FieldProductionEstimator, limit: Optional[int] = None
) -> pd.DataFrame:
    """Create comprehensive field production estimates using optimized batch spatial processing."""
    # Load agricultural fields
    gdf = load_agricultural_fields(year, estimator.gcs_storage)

    # Apply limit for testing if specified
    if limit is not None and limit > 0:
        gdf = gdf.head(limit)
        logging.info(f"Limited to {limit} fields for testing")

    logging.info(f"Creating optimized field production estimates for {year}...")

    # Create the overview dataframe
    overview_data = []

    # Use larger batch sizes for optimal SPATIAL_JOIN performance
    batch_size = 5000
    total_batches = (len(gdf) + batch_size - 1) // batch_size

    logging.info(f"Processing {len(gdf):,} fields in {total_batches} batches of {batch_size}")

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(gdf))
        batch = gdf.iloc[start_idx:end_idx]

        logging.info(f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} fields)")

        # Prepare batch data for optimized spatial processing
        batch_fields = []
        for _, field in batch.iterrows():
            unique_id = f"{field['field_id']}_{field['block_id']}"
            batch_fields.append(
                {
                    "field_id": unique_id,
                    "crop_type": field["crop_type"],
                    "area_ha": field["area_ha"],
                    "geometry": field["geometry"],
                    "original_field": field,
                }
            )

        # Get spatial yield estimates for entire batch using optimized SPATIAL_JOIN
        batch_yields = estimator.get_optimized_batch_spatial_yields(batch_fields, year)

        # Process each field in the batch
        for field_data in batch_fields:
            field = field_data["original_field"]
            unique_id = field_data["field_id"]

            # Get DST mapping info
            dst_info = get_dst_category(field["crop_type"])

            # Get yield estimate from batch results
            yield_estimate = batch_yields.get(unique_id)

            # Calculate production estimate if yield available
            production_estimate = None
            production_unit = None
            if yield_estimate is not None and pd.notna(field["area_ha"]):
                production_estimate = estimator.calculate_production(
                    area_ha=field["area_ha"], yield_hkg_ha=yield_estimate["yield_value"]
                )
                production_unit = "hkg"

            # Add field to overview
            field_overview = {
                # Basic field information
                "field_id": field["field_id"],
                "block_id": field["block_id"],
                "cvr_number": field["cvr_number"],
                "area_ha": field["area_ha"],
                "crop_type": field["crop_type"],
                "organic_farming": field["organic_farming"],
                # Spatial information
                "dst_zone": "Hele landet",  # Default
                "geometry_wkt": field["geometry"].wkt if hasattr(field["geometry"], "wkt") else str(field["geometry"]),
                # Production estimates
                "yield_estimate_hkg_ha": yield_estimate["yield_value"] if yield_estimate else None,
                "yield_source_table": yield_estimate["source_table"] if yield_estimate else None,
                "yield_source_unit": yield_estimate["source_unit"] if yield_estimate else None,
                "yield_conversion_applied": yield_estimate["conversion_applied"] if yield_estimate else None,
                "production_estimate_hkg": production_estimate,
                "production_unit": production_unit,
                # DST mapping info
                "has_dst_mapping": dst_info is not None,
                "dst_table": dst_info["dst_table"] if dst_info else None,
                "dst_category": dst_info["dst_category"] if dst_info else None,
                # Additional metadata
                "year": year,
                "created_at": pd.Timestamp.now(),
                "data_source": f"agricultural_fields_{year}_data.parquet",
                "estimation_method": yield_estimate.get("estimation_method", "unknown") if yield_estimate else None,
            }

            overview_data.append(field_overview)

    # Create DataFrame
    overview_df = pd.DataFrame(overview_data)

    # Add summary statistics
    total_fields = len(overview_df)
    fields_with_production = len(overview_df[overview_df["production_estimate_hkg"].notna()])
    total_area = overview_df["area_ha"].sum()
    total_production = overview_df["production_estimate_hkg"].sum()

    logging.info(f"Optimized Production Summary for {year}:")
    logging.info(f"  Total fields: {total_fields:,}")
    logging.info(
        f"  Fields with production estimates: {fields_with_production:,} ({fields_with_production / total_fields * 100:.1f}%)"
    )
    logging.info(f"  Total area: {total_area:,.1f} ha")
    logging.info(
        f"  Total estimated production: {total_production:,.0f} hkg"
        if pd.notna(total_production)
        else "  Total estimated production: N/A"
    )

    return overview_df


def save_production_data(
    overview_df: pd.DataFrame, year: int, output_dir: Path, gcs_storage: Optional[GCSStorage] = None
):
    """Save the field production data to file and optionally upload to GCS."""
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save locally first
    local_output_file = output_dir / f"field_production_{year}.parquet"
    overview_df.to_parquet(local_output_file, index=False)

    logging.info(f"Saved field production data to: {local_output_file}")
    logging.info(f"File size: {local_output_file.stat().st_size / 1024 / 1024:.1f} MB")

    # Upload to GCS if available
    if gcs_storage:
        try:
            # Create timestamped path for silver layer
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Upload to the correct bucket using gsutil
            import subprocess

            temp_file = f"/tmp/field_production_{year}_{timestamp}.parquet"
            overview_df.to_parquet(temp_file, index=False)

            gcs_path = (
                f"gs://landbrugsdata-raw-data/silver/field_production/{timestamp}/field_production_{year}.parquet"
            )

            cmd = f"gsutil cp {temp_file} {gcs_path}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logging.info(f"✅ Uploaded field production data to GCS: {gcs_path}")
            else:
                logging.error(f"Failed to upload to GCS: {result.stderr}")

            # Clean up temp file
            Path(temp_file).unlink(missing_ok=True)
        except Exception as e:
            logging.error(f"Failed to upload to GCS: {e}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate agricultural field production estimates - Optimized for DuckDB Spatial v1.2.2"
    )

    # Pipeline-specific arguments
    parser.add_argument("--year", type=int, help="Specific year to process (2020-2025)")
    parser.add_argument("--all-years", action="store_true", help="Process all available years")
    parser.add_argument("--output-dir", type=str, default="data/silver", help="Output directory for results")
    parser.add_argument("--bucket", type=str, help="GCS bucket name for production use")
    parser.add_argument("--limit", type=int, help="Limit number of fields to process (for testing)")
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level"
    )

    return parser.parse_args()


def main():
    """Main function to run the optimized field production pipeline."""
    args = parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)

    if not args.year and not args.all_years:
        logging.error("Must specify either --year or --all-years")
        sys.exit(1)

    # Initialize GCS storage if bucket is specified
    gcs_storage = None
    if args.bucket:
        gcs_storage = GCSStorage(bucket_name=args.bucket)

    # Initialize the optimized production estimator
    logging.info("Initializing optimized production estimator with DuckDB Spatial v1.2.2...")
    try:
        estimator = FieldProductionEstimator(gcs_storage=gcs_storage)
    except Exception as e:
        logging.error(f"Failed to initialize production estimator: {e}")
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output_dir)

    # Determine years to process
    if args.all_years:
        years = [2020, 2021, 2022, 2023, 2024, 2025]
    else:
        years = [args.year]

    # Track processing results
    successful_years = []
    failed_years = []
    skipped_years = []

    # Process each year
    for year in years:
        try:
            logging.info(f"{'=' * 60}")
            logging.info(f"Processing year {year} with optimized SPATIAL_JOIN")
            logging.info(f"{'=' * 60}")

            overview_df = create_field_production_optimized(year, estimator, args.limit)
            save_production_data(overview_df, year, output_dir, gcs_storage)
            successful_years.append(year)

        except FileNotFoundError as e:
            logging.warning(f"Skipping {year}: {e}")
            skipped_years.append(year)
        except ImportError as e:
            # Critical dependency errors should fail the pipeline
            logging.error(f"Critical dependency error processing {year}: {e}")
            if "pyarrow" in str(e) or "parquet" in str(e):
                logging.error("Missing pyarrow dependency - this is a critical error")
                sys.exit(1)
            failed_years.append(year)
        except Exception as e:
            logging.error(f"Error processing {year}: {e}")
            import traceback

            traceback.print_exc()
            failed_years.append(year)

    # Final status report
    logging.info(f"{'=' * 60}")
    logging.info("Field Production Pipeline Results:")
    logging.info(f"  Successfully processed: {successful_years}")
    logging.info(f"  Skipped (data not found): {skipped_years}")
    logging.info(f"  Failed: {failed_years}")

    if failed_years:
        logging.error(f"Pipeline failed for {len(failed_years)} year(s): {failed_years}")
        sys.exit(1)
    elif not successful_years:
        logging.error("No years were successfully processed")
        sys.exit(1)
    else:
        logging.info("Optimized field production pipeline complete!")
        logging.info("Leveraged DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for maximum performance")
        logging.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
