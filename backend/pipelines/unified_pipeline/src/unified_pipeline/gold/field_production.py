"""
Field Production Gold Layer

This module implements the gold layer processor for field production estimates.
It combines agricultural fields data with DST (Danish Statistics) yield data to create
comprehensive production estimates for analytics and downstream consumption.

Migrated from the standalone field_production_pipeline to the unified pipeline architecture.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import geopandas as gpd
import pandas as pd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

# Import the DST mapping table from the DST pipeline
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "dst_pipeline"))
from dst_field_crop_mapping_table import get_dst_category


class FieldProductionGoldConfig(BaseJobConfig):
    """Configuration for Field Production gold layer."""

    name: str = "Field Production Gold"
    dataset: str = "field_production"
    type: str = "gold"
    description: str = "Comprehensive field production estimates using DST yield data"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    # Input silver datasets
    agricultural_fields_dataset: str = "agricultural_fields"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"

    # Processing configuration
    batch_size: int = 5000  # Optimized for SPATIAL_JOIN performance
    max_year_lag: int = 3  # Maximum years between field and DST data

    # DST data sources (local cache paths)
    dst_cache_dir: str = "data_cache/dst_pipeline"
    dst_tables: List[str] = ["HST77", "GARTN1", "FRO", "HALM1"]

    # Quality thresholds
    min_yield_coverage: float = 0.3  # Minimum acceptable yield coverage rate

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class FieldProductionGold(BaseSource[FieldProductionGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field production estimates.

    Combines agricultural fields and DST yield data to create
    comprehensive production estimates for analytics and downstream consumption.
    """

    def __init__(self, config: FieldProductionGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()

        # Initialize DuckDB connection for spatial operations
        self.conn = duckdb.connect()
        self._configure_duckdb()

        # Initialize DST data and yield estimator
        self.dst_data = self._load_dst_data()
        self.dst_zone_mapping = None
        self.spatial_conn = None

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        self.conn.execute("SET memory_limit = '8GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("SET enable_progress_bar = true")
        self.conn.execute("SET preserve_insertion_order = false")
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

        # Verify SPATIAL_JOIN operator availability
        try:
            version_result = self.conn.execute(
                "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name = 'spatial'"
            ).fetchone()
            if version_result:
                self.log.info(f"DuckDB Spatial version: {version_result[1]}")
                if version_result[1] >= "1.2.2":
                    self.log.info("✅ SPATIAL_JOIN operator available")
                else:
                    self.log.warning(
                        f"⚠️  SPATIAL_JOIN operator may not be available in version {version_result[1]}"
                    )
        except Exception as e:
            self.log.warning(f"Could not verify spatial extension version: {e}")

    def _load_dst_data(self) -> Dict[str, pd.DataFrame]:
        """Load DST data from local cache."""
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
                table_path = (
                    Path(self.config.dst_cache_dir) / f"{table_name.lower()}_processed.parquet"
                )
                if table_path.exists():
                    df = pd.read_parquet(table_path)

                    # Filter for relevant measurement units
                    if "measurement_unit" in df.columns:
                        df_filtered = df[df["measurement_unit"].isin(measurement_units)]
                    else:
                        df_filtered = df

                    dst_data[table_name] = df_filtered
                    self.log.info(f"Loaded {table_name}: {len(df_filtered)} records")
                else:
                    self.log.warning(f"DST table {table_name} not found in cache")
                    dst_data[table_name] = pd.DataFrame()
            except Exception as e:
                self.log.error(f"Error loading {table_name}: {e}")
                dst_data[table_name] = pd.DataFrame()

        return dst_data

    def _load_silver_data(
        self, dataset: str, silver_data: Optional[Dict[str, Any]]
    ) -> Optional[Any]:
        """Load silver data with fallback to storage."""

        if silver_data and dataset in silver_data:
            self.log.info(f"Using in-memory silver data for {dataset}")
            return silver_data[dataset]

        # Fallback to storage
        self.log.info(f"Reading {dataset} from GCS storage")
        return self._read_data_from_storage(dataset, self.config.bucket, stage="silver")

    def _setup_optimized_spatial_connection(self, dst_zone_mapping: gpd.GeoDataFrame):
        """Setup optimized DuckDB with spatial zones for SPATIAL_JOIN."""

        if dst_zone_mapping is None or len(dst_zone_mapping) == 0:
            self.log.warning("No DST zone mapping available, using national averages only")
            return

        try:
            # Create optimized dst_zones table
            self.conn.execute("DROP TABLE IF EXISTS dst_zones_optimized")
            self.conn.execute("""
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

            # Insert zone data with WKB geometries and bounding boxes
            for idx, zone in dst_zone_mapping.iterrows():
                bounds = zone.geometry.bounds
                self.conn.execute(
                    """
                    INSERT INTO dst_zones_optimized VALUES (?, ?, ST_GeomFromWKB(?), ?, ?, ?, ?)
                """,
                    [
                        idx,
                        zone.get("NAVN", f"Zone_{idx}"),
                        zone.geometry.wkb,
                        bounds[0],
                        bounds[1],
                        bounds[2],
                        bounds[3],
                    ],
                )

            # Create spatial index for optimal performance
            self.conn.execute(
                "CREATE INDEX idx_dst_zones_geom ON dst_zones_optimized USING RTREE (geometry)"
            )

            zone_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones_optimized").fetchone()[0]
            self.log.info(
                f"✅ Created optimized DST zones table with {zone_count} zones and spatial index"
            )

        except Exception as e:
            self.log.error(f"Failed to setup optimized spatial connection: {e}")
            self.dst_zone_mapping = None

    def _process_batch_spatial_yields(self, fields_batch: List[Dict], year: int) -> Dict[str, Dict]:
        """Process field yields using optimized SPATIAL_JOIN operator."""

        if not fields_batch:
            return {}

        try:
            # Create temporary fields table
            self.conn.execute("DROP TABLE IF EXISTS temp_fields_batch")
            self.conn.execute("""
                CREATE TABLE temp_fields_batch (
                    field_id VARCHAR,
                    crop_type VARCHAR,
                    area_ha DOUBLE,
                    geometry GEOMETRY
                )
            """)

            # Insert batch fields with WKB geometries
            for field_data in fields_batch:
                self.conn.execute(
                    """
                    INSERT INTO temp_fields_batch VALUES (?, ?, ?, ST_GeomFromWKB(?))
                """,
                    [
                        field_data["field_id"],
                        field_data["crop_type"],
                        field_data["area_ha"],
                        field_data["geometry"].wkb,
                    ],
                )

            # Execute optimized spatial join to find field-zone intersections
            spatial_join_query = """
            SELECT f.field_id, f.crop_type, f.area_ha, z.zone_name,
                   ST_Area(ST_Intersection(f.geometry, z.geometry)) as intersection_area
            FROM temp_fields_batch f
            INNER JOIN dst_zones_optimized z 
                ON ST_Intersects(f.geometry, z.geometry)
            """

            intersections = self.conn.execute(spatial_join_query).fetchdf()

            # Check if SPATIAL_JOIN operator was used
            query_plan = self.conn.execute("EXPLAIN " + spatial_join_query).fetchdf()
            if any("SPATIAL_JOIN" in str(row) for row in query_plan.values.flatten()):
                self.log.info("✅ SPATIAL_JOIN operator detected in query plan!")

            # Process intersections to calculate yields
            field_yields = {}

            for field_id in intersections["field_id"].unique():
                field_intersections = intersections[intersections["field_id"] == field_id]
                crop_type = field_intersections["crop_type"].iloc[0]
                total_area = field_intersections["area_ha"].iloc[0]

                # Get DST category for this crop
                dst_info = get_dst_category(crop_type)
                if not dst_info["has_dst_mapping"]:
                    continue

                if len(field_intersections) == 1:
                    # Single zone - fast processing
                    zone_name = field_intersections["zone_name"].iloc[0]
                    yield_estimate = self._get_zone_yield(
                        dst_info["dst_table"], dst_info["dst_category"], year, zone_name
                    )
                else:
                    # Multi-zone - area-weighted calculation
                    yield_estimate = self._calculate_area_weighted_yield(
                        field_id, field_intersections, dst_info, year
                    )

                if yield_estimate:
                    field_yields[field_id] = yield_estimate

            # Clean up temporary table
            self.conn.execute("DROP TABLE IF EXISTS temp_fields_batch")

            return field_yields

        except Exception as e:
            self.log.error(f"Error in spatial yield processing: {e}")
            # Clean up on error
            self.conn.execute("DROP TABLE IF EXISTS temp_fields_batch")
            return {}

    def _get_zone_yield(
        self, dst_table: str, dst_category: str, year: int, zone_name: str
    ) -> Optional[Dict[str, any]]:
        """Get yield estimate for a specific zone and crop category."""

        dst_df = self.dst_data.get(dst_table)
        if dst_df is None or len(dst_df) == 0:
            return None

        # Try zone-specific data first
        zone_data = dst_df[
            (dst_df["dst_category"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["region"].str.contains(zone_name, na=False))
        ]

        if len(zone_data) == 0:
            # Fallback to national average
            zone_data = dst_df[
                (dst_df["dst_category"] == dst_category)
                & (dst_df["year"] == year)
                & (dst_df["region"].str.contains("Hele landet", na=False))
            ]

        if len(zone_data) > 0:
            yield_value = zone_data["value"].iloc[0]
            return {
                "yield_value": yield_value,
                "source_table": dst_table,
                "source_unit": zone_data["measurement_unit"].iloc[0],
                "conversion_applied": False,
                "zone_name": zone_name,
                "dst_category": dst_category,
                "estimation_method": "zone_specific"
                if zone_name in zone_data["region"].iloc[0]
                else "national_average",
            }

        return None

    def _calculate_area_weighted_yield(
        self, field_id: str, intersections: pd.DataFrame, dst_info: Dict, year: int
    ) -> Optional[Dict[str, any]]:
        """Calculate area-weighted yield for multi-zone fields."""

        total_weighted_yield = 0
        total_intersection_area = 0
        source_zones = []

        for _, intersection in intersections.iterrows():
            zone_name = intersection["zone_name"]
            intersection_area = intersection["intersection_area"]

            zone_yield = self._get_zone_yield(
                dst_info["dst_table"], dst_info["dst_category"], year, zone_name
            )

            if zone_yield:
                total_weighted_yield += zone_yield["yield_value"] * intersection_area
                total_intersection_area += intersection_area
                source_zones.append(zone_name)

        if total_intersection_area > 0:
            weighted_yield = total_weighted_yield / total_intersection_area
            return {
                "yield_value": weighted_yield,
                "source_table": dst_info["dst_table"],
                "source_unit": "hkg/ha",
                "conversion_applied": False,
                "zone_name": ", ".join(source_zones),
                "dst_category": dst_info["dst_category"],
                "estimation_method": "area_weighted_multi_zone",
            }

        return None

    def _create_production_estimates(
        self, fields_gdf: gpd.GeoDataFrame, yields: Dict[str, Dict]
    ) -> pd.DataFrame:
        """Create normalized field production estimates."""

        production_data = []

        for _, field in fields_gdf.iterrows():
            field_id = field["field_id"]
            block_id = field["block_id"]
            unique_id = f"{field_id}_{block_id}"

            # Get yield estimate for this field
            yield_info = yields.get(unique_id)

            # Get DST mapping info
            dst_info = get_dst_category(field["crop_type"])

            # Create production estimate record
            production_estimate = {
                # JOIN KEYS
                "field_id": field_id,
                "block_id": block_id,
                "cvr_number": field.get("cvr_number"),
                "year": field.get("year", 2024),  # Default to 2024 if not specified
                # FIELD DATA
                "area_ha": field["area_ha"],
                "crop_type": field["crop_type"],
                "organic_farming": field.get("organic_farming", False),
                # YIELD DATA
                "yield_estimate_hkg_ha": yield_info["yield_value"] if yield_info else None,
                "yield_source_table": yield_info["source_table"] if yield_info else None,
                "yield_source_unit": yield_info["source_unit"] if yield_info else None,
                "yield_conversion_applied": yield_info["conversion_applied"]
                if yield_info
                else None,
                "production_estimate_hkg": (field["area_ha"] * yield_info["yield_value"])
                if yield_info
                else None,
                "production_unit": "hkg" if yield_info else None,
                # DST MAPPING INFO
                "has_dst_mapping": dst_info["has_dst_mapping"],
                "dst_table": dst_info["dst_table"],
                "dst_category": dst_info.get("dst_category"),
                "dst_zone": yield_info.get("zone_name") if yield_info else "Hele landet",
                # SPATIAL INFO
                "geometry_wkt": field["geometry"].wkt
                if hasattr(field["geometry"], "wkt")
                else str(field["geometry"]),
                # METADATA
                "estimation_method": yield_info["estimation_method"]
                if yield_info
                else "no_yield_data",
                "created_at": pd.Timestamp.now(),
            }
            production_data.append(production_estimate)

        return pd.DataFrame(production_data)

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run field production estimation gold processing."""

        self.log.info("Starting field production gold layer processing")

        # Load required silver datasets
        agricultural_fields = self._load_silver_data(
            self.config.agricultural_fields_dataset, silver_data
        )
        dst_zone_mapping = self._load_silver_data(self.config.dst_zone_mapping_dataset, silver_data)

        if agricultural_fields is None:
            self.log.error("No agricultural fields data available")
            return

        self.log.info(f"Loaded {len(agricultural_fields)} agricultural fields")

        # Setup spatial processing if zone mapping available
        if dst_zone_mapping is not None:
            self.dst_zone_mapping = dst_zone_mapping
            self._setup_optimized_spatial_connection(dst_zone_mapping)
            self.log.info(f"Loaded DST zone mapping with {len(dst_zone_mapping)} zones")
        else:
            self.log.warning("No DST zone mapping available, using national averages only")

        # Process field production estimates in batches
        all_yields = {}
        batch_size = self.config.batch_size
        total_batches = (len(agricultural_fields) + batch_size - 1) // batch_size

        self.log.info(
            f"Processing {len(agricultural_fields):,} fields in {total_batches} batches of {batch_size}"
        )

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(agricultural_fields))
            batch = agricultural_fields.iloc[start_idx:end_idx]

            self.log.info(f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} fields)")

            # Prepare batch data for spatial processing
            batch_fields = []
            for _, field in batch.iterrows():
                unique_id = f"{field['field_id']}_{field['block_id']}"
                batch_fields.append(
                    {
                        "field_id": unique_id,
                        "crop_type": field["crop_type"],
                        "area_ha": field["area_ha"],
                        "geometry": field["geometry"],
                    }
                )

            # Get spatial yield estimates for batch
            if self.dst_zone_mapping is not None:
                batch_yields = self._process_batch_spatial_yields(
                    batch_fields, 2024
                )  # Default to 2024
                all_yields.update(batch_yields)

        # Create production estimates
        production_estimates = self._create_production_estimates(agricultural_fields, all_yields)

        # Log summary statistics
        total_fields = len(production_estimates)
        fields_with_estimates = len(
            production_estimates[production_estimates["yield_estimate_hkg_ha"].notna()]
        )
        coverage_rate = fields_with_estimates / total_fields if total_fields > 0 else 0

        self.log.info("Production estimates summary:")
        self.log.info(f"  Total fields: {total_fields:,}")
        self.log.info(f"  Fields with estimates: {fields_with_estimates:,} ({coverage_rate:.1%})")

        if coverage_rate < self.config.min_yield_coverage:
            self.log.warning(
                f"Yield coverage {coverage_rate:.1%} below minimum threshold {self.config.min_yield_coverage:.1%}"
            )

        # Save to gold layer
        self._save_data(production_estimates, self.config.dataset, self.config.bucket, stage="gold")

        self.log.info("Field production gold layer processing completed")
