"""
Field Production Gold Layer

This module implements the gold layer processor for field production estimates.
It combines agricultural fields data with DST (Danish Statistics) yield data to create
comprehensive production estimates for analytics and downstream consumption.

Migrated from the standalone field_production_pipeline to the unified pipeline architecture.
"""

import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
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
    agricultural_fields_dataset: str = "fvm_marker"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"
    dst_yield_datasets: List[str] = [
        "hst77_processed",
        "gartn1_processed",
        "fro_processed",
        "halm1_processed",
    ]

    # Processing configuration
    batch_size: int = 5000  # Optimized for SPATIAL_JOIN performance
    max_year_lag: int = 3  # Maximum years between field and DST data

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

        # Initialize spatial processing
        self.dst_zone_mapping = None
        self.spatial_conn = None

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        self.conn.execute("SET memory_limit = '12GB'")  # Use 75% of available 16GB RAM
        self.conn.execute("SET threads = 4")  # Use all available CPU cores
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

    def _get_available_fvm_marker_years(self) -> List[int]:
        """Get all available fvm_marker years from GCS storage."""
        try:
            # List all files in silver layer to extract directory names
            files = self.gcs_util.list_files(bucket_name=self.config.bucket, prefix="silver/")
            years = set()

            for file_blob in files:
                # Extract years from blob names like "silver/fvm_marker_2021/timestamp/data.parquet"
                match = re.search(r"silver/fvm_marker_(\d{4})/", file_blob.name)
                if match:
                    year = int(match.group(1))
                    years.add(year)

            return sorted(list(years))
        except Exception as e:
            self.log.error(f"Error discovering fvm_marker years: {e}")
            return []

    def _load_agricultural_fields_for_years(
        self, years: List[int], silver_data: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """Load agricultural fields data for all available years."""
        all_fields = []

        for year in years:
            dataset_name = f"fvm_marker_{year}"
            try:
                # Try to load from silver_data first
                if silver_data and dataset_name in silver_data:
                    year_data = silver_data[dataset_name]
                    self.log.info(f"Using in-memory data for {dataset_name}")
                else:
                    # Load from GCS
                    year_data = self._read_data_from_storage(
                        dataset_name, self.config.bucket, stage="silver"
                    )
                    self.log.info(f"Loaded {dataset_name} from GCS")

                if year_data is not None and len(year_data) > 0:
                    # Add year column if not present
                    if "year" not in year_data.columns:
                        year_data["year"] = year
                    all_fields.append(year_data)
                    self.log.info(f"Added {len(year_data)} fields for year {year}")
                else:
                    self.log.warning(f"No data found for {dataset_name}")

            except Exception as e:
                self.log.error(f"Error loading {dataset_name}: {e}")
                continue

        if not all_fields:
            return None

        # ✅ MIGRATION: Use DuckDB UNION operations instead of pandas concat
        if len(all_fields) == 1:
            combined_fields = all_fields[0]
        else:
            # Register all dataframes and combine with UNION
            for i, df in enumerate(all_fields):
                self.conn.register(f"fields_year_{i}", df)

            # Create UNION query for all years
            union_parts = [f"SELECT * FROM fields_year_{i}" for i in range(len(all_fields))]
            union_query = " UNION ALL ".join(union_parts)

            combined_fields = self.conn.execute(union_query).df()

        self.log.info(f"Combined {len(combined_fields)} fields across {len(all_fields)} years")

        return combined_fields

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

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run field production estimation gold processing."""

        self.log.info("Starting field production gold layer processing")

        # Get all available years
        available_years = self._get_available_fvm_marker_years()
        if not available_years:
            self.log.error("No fvm_marker years found")
            return

        self.log.info(
            f"Found fvm_marker data for years: {available_years} ({len(available_years)} years)"
        )

        # Load DST zone mapping (this is small and can stay in memory)
        dst_zone_mapping = self._load_silver_data(self.config.dst_zone_mapping_dataset, silver_data)
        if dst_zone_mapping is None:
            self.log.error("DST zone mapping is required for production estimation")
            return

        self.log.info(f"Loaded DST zone mapping with {len(dst_zone_mapping)} zones")

        # Setup spatial processing with DST zones (once)
        self._setup_spatial_processing_with_dst_zones(dst_zone_mapping)

        # Load DST yield data (this is relatively small and can stay in memory)
        dst_data = self._load_dst_yield_data(silver_data)
        if not dst_data:
            self.log.warning("No DST yield data available - production estimates will be limited")
        else:
            total_yield_records = sum(len(df) for df in dst_data.values())
            self.log.info(
                f"Loaded {total_yield_records} yield records from {len(dst_data)} DST datasets"
            )

        # Process each year individually to avoid memory issues
        all_production_estimates = []
        batch_size = self.config.batch_size
        total_fields_processed = 0

        for year in available_years:
            self.log.info(f"Processing year {year}...")

            # Load agricultural fields for this year only
            year_fields = self._load_agricultural_fields_for_years([year], silver_data)
            if year_fields is None or year_fields.empty:
                self.log.warning(f"No agricultural fields data found for year {year}")
                continue

            self.log.info(f"  Loaded {len(year_fields)} fields for year {year}")
            total_fields_processed += len(year_fields)

            # Process year fields in batches
            total_batches = (len(year_fields) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(year_fields))
                batch = year_fields.iloc[start_idx:end_idx]

                self.log.info(
                    f"    Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} fields)"
                )

                # Process batch with DST yields
                batch_estimates = self._process_field_batch_with_dst_yields(batch, dst_data, year)
                if not batch_estimates.empty:
                    all_production_estimates.append(batch_estimates)

                # Progress update
                progress = (batch_idx + 1) / total_batches * 100
                self.log.info(f"      ⏱️ Batch completed | Progress: {progress:.1f}%")

            # Clear year data from memory after processing
            del year_fields
            self.log.info(f"  ✅ Completed year {year}")

        # Combine all results
        if not all_production_estimates:
            self.log.error("No production estimates were generated")
            return

        self.log.info(
            f"🔗 Combining {len(all_production_estimates)} batches from {len(available_years)} years..."
        )

        # ✅ MIGRATION: Use DuckDB UNION operations instead of pandas concat
        if len(all_production_estimates) == 1:
            final_estimates = all_production_estimates[0]
        else:
            # Register all dataframes and combine with UNION
            for i, df in enumerate(all_production_estimates):
                self.conn.register(f"production_batch_{i}", df)

            # Create UNION query for all batches
            union_parts = [
                f"SELECT * FROM production_batch_{i}" for i in range(len(all_production_estimates))
            ]
            union_query = " UNION ALL ".join(union_parts)

            final_estimates = self.conn.execute(union_query).df()

        # Log summary statistics
        total_fields = len(final_estimates)
        years_covered = sorted(final_estimates["year"].unique())
        crops_covered = len(final_estimates["crop_type"].unique())
        fields_with_yields = len(final_estimates[final_estimates["yield_estimate_hkg_ha"].notna()])
        fields_with_production = len(
            final_estimates[final_estimates["production_estimate_hkg"].notna()]
        )
        yield_coverage = fields_with_yields / total_fields if total_fields > 0 else 0
        production_coverage = fields_with_production / total_fields if total_fields > 0 else 0

        self.log.info("Field production summary:")
        self.log.info(f"  Total fields processed: {total_fields_processed:,}")
        self.log.info(f"  Total production estimates: {total_fields:,}")
        self.log.info(
            f"  Years covered: {len(years_covered)} years ({min(years_covered)}-{max(years_covered)})"
        )
        self.log.info(f"  Unique crop types: {crops_covered}")
        self.log.info(
            f"  Fields with yield estimates: {fields_with_yields:,} ({yield_coverage:.1%})"
        )
        self.log.info(
            f"  Fields with production estimates: {fields_with_production:,} ({production_coverage:.1%})"
        )

        # Summary by year
        for year in years_covered:
            year_data = final_estimates[final_estimates["year"] == year]
            year_count = len(year_data)
            year_with_production = len(year_data[year_data["production_estimate_hkg"].notna()])
            year_coverage = year_with_production / year_count if year_count > 0 else 0
            self.log.info(
                f"    Year {year}: {year_count:,} fields, {year_with_production:,} with production ({year_coverage:.1%})"
            )

        # Check quality thresholds
        if yield_coverage < self.config.min_yield_coverage:
            self.log.warning(
                f"Yield coverage {yield_coverage:.1%} below minimum threshold {self.config.min_yield_coverage:.1%}"
            )

        # Save to gold layer
        self._save_data(final_estimates, self.config.dataset, self.config.bucket, stage="gold")

        self.log.info(
            f"Field production gold layer processing completed - processed {len(available_years)} years"
        )

    def _setup_spatial_processing_with_dst_zones(self, dst_zone_mapping: pd.DataFrame) -> None:
        """Setup spatial processing with DST zones in DuckDB."""
        try:
            self.log.info("Setting up spatial processing with DST zones")

            # Register DST zone mapping with DuckDB
            self.conn.register("dst_zones_df", dst_zone_mapping)

            # Create optimized DST zones table
            self.conn.execute("DROP TABLE IF EXISTS dst_zones")
            self.conn.execute("""
                CREATE TABLE dst_zones AS
                SELECT 
                    landsdel_code,
                    landsdel_name,
                    dst_regions,
                    ST_GeomFromText(geometry) as geometry
                FROM dst_zones_df
                WHERE geometry IS NOT NULL
            """)

            # Create spatial index
            self.conn.execute("CREATE INDEX idx_dst_zones_geom ON dst_zones USING RTREE (geometry)")

            zone_count = self.conn.execute("SELECT COUNT(*) FROM dst_zones").fetchone()[0]
            self.log.info(f"✅ Created DST zones table with {zone_count} zones and spatial index")

        except Exception as e:
            self.log.error(f"Failed to setup spatial processing with DST zones: {e}")
            raise

    def _load_dst_yield_data(
        self, silver_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, pd.DataFrame]:
        """Load DST yield data from all available sources."""
        dst_data = {}

        for dataset in self.config.dst_yield_datasets:
            try:
                data = self._load_silver_data(dataset, silver_data)
                if data is not None and len(data) > 0:
                    dst_data[dataset] = data
                    self.log.info(f"Loaded {len(data)} records from {dataset}")
                else:
                    self.log.warning(f"No data found for {dataset}")
            except Exception as e:
                self.log.error(f"Error loading {dataset}: {e}")
                continue

        return dst_data

    def _process_field_batch_with_dst_yields(
        self, fields_batch: pd.DataFrame, dst_data: Dict[str, pd.DataFrame], year: int
    ) -> pd.DataFrame:
        """Process field batch with spatial joins and yield calculations."""
        try:
            # Register fields batch with DuckDB
            self.conn.register("fields_batch_df", fields_batch)

            # Create fields table with spatial geometries
            self.conn.execute("""
                CREATE OR REPLACE TABLE current_fields AS
                SELECT 
                    field_id,
                    block_id,
                    cvr_number,
                    area_ha,
                    crop_type,
                    organic_farming,
                    year,
                    ST_GeomFromText(geometry) as geometry
                FROM fields_batch_df
                WHERE geometry IS NOT NULL
            """)

            # Spatial join with DST zones
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_dst_zones AS
                SELECT 
                    f.field_id,
                    f.block_id,
                    f.cvr_number,
                    f.area_ha,
                    f.crop_type,
                    f.organic_farming,
                    f.year,
                    f.geometry,
                    z.landsdel_code,
                    z.landsdel_name,
                    z.dst_regions
                FROM current_fields f
                LEFT JOIN dst_zones z ON ST_Within(f.geometry, z.geometry)
            """)

            # Get fields with DST zone information
            fields_with_zones = self.conn.execute("""
                SELECT * FROM fields_with_dst_zones
            """).df()

            # Calculate production estimates for each field
            production_estimates = []

            for _, field in fields_with_zones.iterrows():
                field_id = field["field_id"]
                block_id = field["block_id"]
                crop_type = field["crop_type"]
                area_ha = field["area_ha"]
                dst_regions = field.get("dst_regions", "")
                landsdel_name = field.get("landsdel_name", "")

                # Get DST mapping info
                dst_info = get_dst_category(crop_type)

                # Find yield data for this crop and region
                yield_estimate = self._get_yield_estimate(
                    dst_info, crop_type, dst_regions, landsdel_name, year, dst_data
                )

                # Calculate production
                production_hkg = None
                if yield_estimate and yield_estimate["yield_value"]:
                    production_hkg = area_ha * yield_estimate["yield_value"]

                # Create production record
                production_estimate = {
                    # JOIN KEYS
                    "field_id": field_id,
                    "block_id": block_id,
                    "cvr_number": field.get("cvr_number"),
                    "year": field.get("year", year),
                    # FIELD DATA
                    "area_ha": area_ha,
                    "crop_type": crop_type,
                    "organic_farming": field.get("organic_farming", False),
                    # DST ZONE INFO
                    "landsdel_code": field.get("landsdel_code"),
                    "landsdel_name": landsdel_name,
                    "dst_regions": dst_regions,
                    # DST MAPPING INFO
                    "has_dst_mapping": dst_info["has_dst_mapping"] if dst_info else False,
                    "dst_table": dst_info["dst_table"] if dst_info else None,
                    "dst_category": dst_info.get("dst_category") if dst_info else None,
                    # YIELD DATA
                    "yield_estimate_hkg_ha": yield_estimate["yield_value"]
                    if yield_estimate
                    else None,
                    "yield_source_table": yield_estimate["source_table"]
                    if yield_estimate
                    else None,
                    "yield_source_region": yield_estimate["source_region"]
                    if yield_estimate
                    else None,
                    "yield_estimation_method": yield_estimate["estimation_method"]
                    if yield_estimate
                    else "no_yield_data",
                    # PRODUCTION ESTIMATE
                    "production_estimate_hkg": production_hkg,
                    "production_unit": "hkg" if production_hkg else None,
                    # SPATIAL INFO
                    "geometry_wkt": field["geometry"].wkt
                    if hasattr(field["geometry"], "wkt")
                    else str(field["geometry"]),
                    # METADATA
                    "created_at": pd.Timestamp.now(),
                }
                production_estimates.append(production_estimate)

            return pd.DataFrame(production_estimates)

        except Exception as e:
            self.log.error(f"Error processing field batch with DST yields: {e}")
            return pd.DataFrame()

    def _get_yield_estimate(
        self,
        dst_info: Dict,
        crop_type: str,
        dst_regions: str,
        landsdel_name: str,
        year: int,
        dst_data: Dict[str, pd.DataFrame],
    ) -> Optional[Dict[str, Any]]:
        """Get yield estimate for a field based on DST data."""
        if not dst_info or not dst_info.get("has_dst_mapping"):
            return None

        dst_table = dst_info["dst_table"]
        dst_category = dst_info["dst_category"]

        # Map DST table to dataset name
        table_mapping = {
            "HST77": "hst77_processed",
            "GARTN1": "gartn1_processed",
            "FRO": "fro_processed",
            "HALM1": "halm1_processed",
        }

        dataset_name = table_mapping.get(dst_table)
        if not dataset_name or dataset_name not in dst_data:
            return None

        dst_df = dst_data[dataset_name]

        # Filter for the specific crop and year
        crop_data = dst_df[
            (dst_df["crop_type"] == dst_category)
            & (dst_df["year"] == year)
            & (dst_df["measurement_unit"].str.contains("yield|hektoliter", case=False, na=False))
        ]

        if crop_data.empty:
            return None

        # Try to find data for the specific DST region
        best_match = None
        estimation_method = "no_match"

        if dst_regions:
            # Split pipe-separated DST regions
            regions = [r.strip() for r in dst_regions.split("|")]

            for region in regions:
                region_data = crop_data[
                    crop_data["region"].str.contains(region, case=False, na=False)
                ]
                if not region_data.empty:
                    best_match = region_data.iloc[0]
                    estimation_method = f"dst_region_{region}"
                    break

        # Fallback to national average ("Hele landet")
        if best_match is None:
            national_data = crop_data[
                crop_data["region"].str.contains("Hele landet", case=False, na=False)
            ]
            if not national_data.empty:
                best_match = national_data.iloc[0]
                estimation_method = "national_average"

        # Last fallback - any available data
        if best_match is None and not crop_data.empty:
            best_match = crop_data.iloc[0]
            estimation_method = "fallback_any_region"

        if best_match is not None:
            return {
                "yield_value": best_match["value"],
                "source_table": dst_table,
                "source_region": best_match["region"],
                "estimation_method": estimation_method,
            }

        return None
