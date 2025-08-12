#!/usr/bin/env python3
"""
Pesticide Proximity Analysis Gold Layer - Separate Pipeline

This pipeline performs spatial proximity analysis on disaggregated pesticide data.
It runs as a separate process after pesticide disaggregation to avoid DuckDB spatial crashes.

Key Features:
- Residential building proximity analysis (100m radius)
- Educational facility proximity analysis (100m radius)
- Water feature proximity analysis (100m radius)
- Formatted string outputs with addresses and distances
- Matrix job approach for parallel processing by year
"""

import os
import time
from typing import Any, Dict, Optional

from loguru import logger
from pydantic import ConfigDict, Field
from tqdm import tqdm

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface


class PesticideProximityGoldConfig(BaseJobConfig):
    """Configuration for pesticide proximity analysis gold processor."""

    name: str = "Pesticide Proximity Analysis Gold"
    dataset: str = "pesticide_proximity"
    type: str = "gold"
    description: str = "Spatial proximity analysis for disaggregated pesticide applications"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Distance thresholds for proximity analysis
    building_proximity_distance_m: float = Field(
        default=100.0,
        description="Distance threshold for building proximity analysis (meters)",
    )
    water_proximity_distance_m: float = Field(
        default=100.0,
        description="Maximum distance for water proximity analysis (meters)",
    )

    # Input datasets
    pesticide_disaggregation_dataset: str = "pesticide_disaggregation"
    agricultural_fields_dataset: str = "fvm_marker"
    buildings_dataset: str = "bbr_buildings"
    water_typology_dataset: str = "water_typology"

    # Year filtering for matrix jobs (process single year instead of all years)
    pesticide_year: Optional[int] = Field(
        default=None,
        description="Specific pesticide year to process (if None, processes all available years)",
    )

    # Performance settings
    batch_size: int = Field(
        default=5000,
        description="Number of fields to process per batch for memory management",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    def apply_cli_filters(self, cli_config) -> None:
        """
        Apply CLI filtering for matrix job processing.

        This method sets the pesticide_year from CLI parameters to enable
        processing of specific years for parallel matrix jobs.

        Args:
            cli_config: CLI configuration containing pesticide_year filter
        """
        if cli_config.pesticide_year:
            object.__setattr__(self, "pesticide_year", cli_config.pesticide_year)


class PesticideProximityGold(BaseSource[PesticideProximityGoldConfig], GoldJobInterface):
    """
    Pesticide Proximity Analysis Gold Layer Processor

    This processor performs spatial proximity analysis on disaggregated pesticide data,
    identifying nearby buildings and water features for environmental impact assessment.
    """

    def __init__(self, config: PesticideProximityGoldConfig):
        super().__init__(config)
        self.log = logger

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        print("🚨 PESTICIDE PROXIMITY RUN METHOD: Starting execution")
        print(f"🚨 CONFIG: pesticide_year = {self.config.pesticide_year}")
        """Main execution method for pesticide proximity analysis."""

        # Initialize DuckDB spatial extension
        self.log.info("🚀 Starting pesticide proximity analysis pipeline...")
        await self._setup_duckdb()

        # Load input datasets
        datasets = await self._load_datasets()

        # Get available years from disaggregated pesticide data
        if self.config.pesticide_year:
            # Matrix job mode: process only specified year
            years = [self.config.pesticide_year]
            self.log.info(f"🎯 Matrix job mode: Processing only year {self.config.pesticide_year}")
            print(f"🎯 PROXIMITY MATRIX JOB: Processing only year {self.config.pesticide_year}")
        else:
            # Regular mode: process all available years
            years = await self._get_available_years(datasets["disaggregation"])
            self.log.info(f"📅 Found {len(years)} years to process: {years}")

        # Process each year
        for year in years:
            self.log.info(f"🔄 Processing proximity analysis for year {year}...")

            try:
                # Load year-specific disaggregated data
                await self._load_year_data(year, datasets)

                # Perform proximity analysis
                results_count = await self._perform_proximity_analysis(year)

                # Save results
                await self._save_year_results(year, results_count)

                self.log.info(
                    f"✅ Year {year} completed: {results_count:,} records with proximity data"
                )

            except Exception as e:
                self.log.error(f"❌ Year {year} failed: {e}")
                continue

        self.log.info("🏁 Pesticide proximity analysis completed successfully!")

    async def _setup_duckdb(self) -> None:
        """Initialize DuckDB with spatial extensions."""
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        self.log.info("✅ DuckDB spatial extension loaded")

    async def _load_datasets(self) -> Dict[str, list]:
        """Load required datasets for proximity analysis."""
        datasets = {}

        self.log.info("📥 Loading input datasets...")

        # Load disaggregated pesticide data - get available years
        datasets["disaggregation"] = self._get_pesticide_disaggregation_files()
        if not datasets["disaggregation"]:
            raise ValueError("No pesticide disaggregation data found")

        # Agricultural fields will be loaded year-specifically in _load_year_data
        datasets["fields"] = None  # Will be loaded per year

        # Load buildings data
        try:
            self._read_silver_data(self.config.buildings_dataset)
            datasets["buildings"] = f"data_{self.config.buildings_dataset}_silver"

            # Verify buildings data loaded
            building_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {datasets['buildings']}"
            ).fetchone()[0]
            self.log.info(f"✅ Buildings dataset loaded: {building_count:,} records")
        except Exception as e:
            self.log.error(f"❌ Failed to load buildings dataset: {e}")
            self.log.error("Buildings data is required for proximity analysis!")
            datasets["buildings"] = None

        # Load water typology data
        try:
            self._read_silver_data(self.config.water_typology_dataset)
            datasets["water"] = f"data_{self.config.water_typology_dataset}_silver"

            # Verify water data loaded
            water_count = self.conn.execute(f"SELECT COUNT(*) FROM {datasets['water']}").fetchone()[
                0
            ]
            self.log.info(f"✅ Water typology dataset loaded: {water_count:,} records")
        except Exception as e:
            self.log.error(f"❌ Failed to load water typology dataset: {e}")
            self.log.error("Water data is required for proximity analysis!")
            datasets["water"] = None

        # Validate that critical datasets are available
        missing_datasets = []
        if not datasets["buildings"]:
            missing_datasets.append("buildings")
        if not datasets["water"]:
            missing_datasets.append("water_typology")

        if missing_datasets:
            raise ValueError(
                f"Critical datasets missing for proximity analysis: {', '.join(missing_datasets)}"
            )

        self.log.info("✅ All input datasets loaded and validated")
        return datasets

    def _get_pesticide_disaggregation_files(self) -> Dict[int, str]:
        """Get mapping of years to pesticide disaggregation file paths."""
        pattern = f"gs://{self.config.bucket}/gold/pesticide_disaggregation_*/*/*.parquet"
        files = self.gcs_access.list_files(pattern)

        year_files = {}
        for file_path in files:
            # Extract year from path: .../pesticide_disaggregation_YYYY_YYYY+1/...
            parts = file_path.split("/")
            for part in parts:
                if part.startswith("pesticide_disaggregation_"):
                    try:
                        # Format: pesticide_disaggregation_2023_2024
                        year_part = part.replace("pesticide_disaggregation_", "")
                        year = int(year_part.split("_")[0])
                        year_files[year] = file_path
                        break
                    except (ValueError, IndexError):
                        continue

        self.log.info(
            f"📅 Found pesticide disaggregation data for years: {sorted(year_files.keys())}"
        )
        return year_files

    async def _get_available_years(self, disaggregation_files: Dict[int, str]) -> list:
        """Get list of available years from disaggregated pesticide files."""
        return sorted(disaggregation_files.keys())

    async def _load_year_data(self, year: int, datasets: Dict[str, Any]) -> None:
        """Load data for a specific year."""

        # Load disaggregated data for this year from the file path
        if year not in datasets["disaggregation"]:
            raise ValueError(f"No disaggregation data found for year {year}")

        file_path = datasets["disaggregation"][year]
        self.log.info(f"📂 PROXIMITY INPUT: Loading disaggregation data for year {year}")
        self.log.info(f"📁 Disaggregation Data Path: {file_path}")

        try:
            # Load the parquet file into a table using proper GCS access
            table_name = "current_disaggregation"
            self.gcs_access.create_table_from_gcs(table_name, file_path)

            # Check the loaded data
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            record_count = count_result[0] if count_result else 0
            self.log.info(f"✅ Loaded {record_count:,} disaggregated records for year {year}")

            # Load year-specific FVM marker data
            # For pesticide year YYYY, we need field data from year YYYY+1
            field_year = year + 1
            field_dataset = f"fvm_marker_{field_year}"

            self.log.info(f"🗺️ Loading agricultural fields data for field year {field_year}")
            try:
                self._read_silver_data(field_dataset)
                field_table = f"data_{field_dataset}_silver"

                # Check field data
                field_count_result = self.conn.execute(
                    f"SELECT COUNT(*) FROM {field_table}"
                ).fetchone()
                field_record_count = field_count_result[0] if field_count_result else 0
                self.log.info(
                    f"✅ Loaded {field_record_count:,} field records for field year {field_year}"
                )

            except Exception as e:
                self.log.error(f"Failed to load FVM marker data for field year {field_year}: {e}")
                raise

        except Exception as e:
            self.log.error(f"Failed to load data for year {year}: {e}")
            raise

    async def _perform_proximity_analysis(self, year: int) -> int:
        """Perform spatial proximity analysis for the current year."""

        self.log.info(f"🌍 Performing spatial proximity analysis for year {year}...")

        # Get the field table name for this year
        field_year = year + 1
        field_table = f"data_fvm_marker_{field_year}_silver"

        # Get unique fields from current disaggregation data
        unique_fields = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) 
            FROM current_disaggregation 
            WHERE field_uuid IS NOT NULL
        """).fetchone()[0]

        self.log.info(f"📊 Processing {unique_fields:,} unique fields with pesticide applications")

        if unique_fields == 0:
            self.log.warning("No fields with valid field_uuid found")
            return 0

        # Create proximity analysis results table by breaking into steps to avoid DuckDB CTE alias issues

        # Step 1: Create fields with geometry
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE fields_with_geometry AS
            SELECT DISTINCT
                cd.field_uuid,
                ST_Transform(f.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm
            FROM current_disaggregation cd
            JOIN {field_table} f ON cd.field_uuid = f.field_uuid
            WHERE f.geometry IS NOT NULL
        """)

        # Step 2: Create residential proximity using optimized SPATIAL_JOIN pattern
        self.log.info("🏠 Processing residential building proximity...")
        total_fields = self.conn.execute("SELECT COUNT(*) FROM fields_with_geometry").fetchone()[0]
        self.log.info(
            f"📊 Processing {total_fields:,} fields in batches of {self.config.batch_size}"
        )

        # Initialize results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE residential_proximity (
                field_uuid VARCHAR,
                residential_buildings_formatted VARCHAR
            )
        """)

        # Process in chunks for memory safety
        processed = 0
        total_chunks = (total_fields + self.config.batch_size - 1) // self.config.batch_size

        # Create progress bar for residential processing
        residential_pbar = tqdm(
            total=total_fields,
            desc="🏠 Residential proximity",
            unit="fields",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} fields [{elapsed}<{remaining}, {rate_fmt}]",
        )

        for chunk_num, offset in enumerate(range(0, total_fields, self.config.batch_size), 1):
            chunk_start = time.time()
            self.log.info(
                f"🔄 Processing residential batch {chunk_num}/{total_chunks} (fields {offset:,}-{min(offset + self.config.batch_size, total_fields):,})"
            )

            # Step 1: Create current batch of fields with consistent ordering
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE current_field_batch AS
                SELECT * FROM fields_with_geometry 
                ORDER BY field_uuid
                LIMIT {self.config.batch_size} OFFSET {offset}
            """)

            # Step 2: Pre-filter buildings to residential only
            self.conn.execute("""
                CREATE OR REPLACE TABLE residential_buildings AS
                SELECT 
                    address,
                    ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832') as building_geom_utm
                FROM data_bbr_buildings_silver 
                WHERE category_group = 'residential' 
                  AND address IS NOT NULL
                  AND geometry IS NOT NULL
            """)

            # Step 3: Simple spatial join with distance calculation
            self.conn.execute(f"""
                INSERT INTO residential_proximity
                SELECT 
                    f.field_uuid,
                    CASE 
                        WHEN COUNT(b.address) > 0 THEN
                            string_agg(
                                b.address || ':' || 
                                ROUND(ST_Distance(f.field_geom_utm, b.building_geom_utm), 1) || 'm',
                                chr(10)
                                ORDER BY ST_Distance(f.field_geom_utm, b.building_geom_utm)
                            )
                        ELSE ''
                    END as residential_buildings_formatted
                FROM current_field_batch f
                LEFT JOIN residential_buildings b ON ST_Intersects(
                    ST_Buffer(f.field_geom_utm, {self.config.building_proximity_distance_m}),
                    b.building_geom_utm
                )
                GROUP BY f.field_uuid
            """)

            chunk_time = time.time() - chunk_start
            batch_processed = min(self.config.batch_size, total_fields - offset)
            processed += batch_processed

            # Update progress bar
            residential_pbar.update(batch_processed)
            residential_pbar.set_postfix(
                {
                    "batch_time": f"{chunk_time:.1f}s",
                    "rate": f"{batch_processed / chunk_time:.0f} fields/s",
                }
            )

            self.log.info(
                f"✅ Residential batch {chunk_num}/{total_chunks} completed in {chunk_time:.2f}s - Rate: {batch_processed / chunk_time:.0f} fields/s"
            )

        residential_pbar.close()

        # Step 4: Create educational proximity using step-by-step approach
        self.log.info("🏫 Processing educational facility proximity...")

        # Initialize results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE educational_proximity (
                field_uuid VARCHAR,
                educational_facilities_formatted VARCHAR
            )
        """)

        # Process in chunks for memory safety
        processed = 0

        # Create progress bar for educational processing
        educational_pbar = tqdm(
            total=total_fields,
            desc="🏫 Educational proximity",
            unit="fields",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} fields [{elapsed}<{remaining}, {rate_fmt}]",
        )

        for chunk_num, offset in enumerate(range(0, total_fields, self.config.batch_size), 1):
            chunk_start = time.time()
            self.log.info(
                f"🔄 Processing educational batch {chunk_num}/{total_chunks} (fields {offset:,}-{min(offset + self.config.batch_size, total_fields):,})"
            )

            # Step 1: Create current batch of fields with consistent ordering
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE current_field_batch AS
                SELECT * FROM fields_with_geometry 
                ORDER BY field_uuid
                LIMIT {self.config.batch_size} OFFSET {offset}
            """)

            # Step 2: Pre-filter buildings to educational only
            self.conn.execute("""
                CREATE OR REPLACE TABLE educational_buildings AS
                SELECT 
                    address,
                    ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832') as building_geom_utm
                FROM data_bbr_buildings_silver 
                WHERE category_group = 'publicServices' 
                  AND address IS NOT NULL
                  AND geometry IS NOT NULL
            """)

            # Step 3: Simple spatial join with distance calculation
            self.conn.execute(f"""
                INSERT INTO educational_proximity
                SELECT 
                    f.field_uuid,
                    CASE 
                        WHEN COUNT(b.address) > 0 THEN
                            string_agg(
                                b.address || ':' || 
                                ROUND(ST_Distance(f.field_geom_utm, b.building_geom_utm), 1) || 'm',
                                chr(10)
                                ORDER BY ST_Distance(f.field_geom_utm, b.building_geom_utm)
                            )
                        ELSE ''
                    END as educational_facilities_formatted
                FROM current_field_batch f
                LEFT JOIN educational_buildings b ON ST_Intersects(
                    ST_Buffer(f.field_geom_utm, {self.config.building_proximity_distance_m}),
                    b.building_geom_utm
                )
                GROUP BY f.field_uuid
            """)

            chunk_time = time.time() - chunk_start
            batch_processed = min(self.config.batch_size, total_fields - offset)
            processed += batch_processed

            # Update progress bar
            educational_pbar.update(batch_processed)
            educational_pbar.set_postfix(
                {
                    "batch_time": f"{chunk_time:.1f}s",
                    "rate": f"{batch_processed / chunk_time:.0f} fields/s",
                }
            )

            self.log.info(
                f"✅ Educational batch {chunk_num}/{total_chunks} completed in {chunk_time:.2f}s - Rate: {batch_processed / chunk_time:.0f} fields/s"
            )

        educational_pbar.close()

        # Step 5: Create water proximity using step-by-step approach
        self.log.info("💧 Processing water feature proximity...")

        # Initialize results table
        self.conn.execute("""
            CREATE OR REPLACE TABLE water_proximity (
                field_uuid VARCHAR,
                water_distance_formatted VARCHAR
            )
        """)

        # Process in chunks for memory safety
        processed = 0

        # Create progress bar for water processing
        water_pbar = tqdm(
            total=total_fields,
            desc="💧 Water proximity",
            unit="fields",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} fields [{elapsed}<{remaining}, {rate_fmt}]",
        )

        for chunk_num, offset in enumerate(range(0, total_fields, self.config.batch_size), 1):
            chunk_start = time.time()
            self.log.info(
                f"🔄 Processing water batch {chunk_num}/{total_chunks} (fields {offset:,}-{min(offset + self.config.batch_size, total_fields):,})"
            )

            # Step 1: Create current batch of fields with consistent ordering
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE current_field_batch AS
                SELECT * FROM fields_with_geometry 
                ORDER BY field_uuid
                LIMIT {self.config.batch_size} OFFSET {offset}
            """)

            # Step 2: Pre-filter water features
            self.conn.execute("""
                CREATE OR REPLACE TABLE water_features AS
                SELECT 
                    ST_Transform(geometry_spatial, 'EPSG:4326', 'EPSG:25832') as water_geom_utm
                FROM data_water_typology_silver 
                WHERE geometry_spatial IS NOT NULL
            """)

            # Step 3: Simple spatial join with distance calculation
            self.conn.execute(f"""
                INSERT INTO water_proximity
                SELECT 
                    f.field_uuid,
                    CASE 
                        WHEN MIN(ST_Distance(f.field_geom_utm, w.water_geom_utm)) IS NOT NULL
                        THEN ROUND(MIN(ST_Distance(f.field_geom_utm, w.water_geom_utm)), 1) || 'm'
                        ELSE ''
                    END as water_distance_formatted
                FROM current_field_batch f
                LEFT JOIN water_features w ON ST_Intersects(
                    ST_Buffer(f.field_geom_utm, {self.config.water_proximity_distance_m}),
                    w.water_geom_utm
                )
                GROUP BY f.field_uuid
            """)

            chunk_time = time.time() - chunk_start
            batch_processed = min(self.config.batch_size, total_fields - offset)
            processed += batch_processed

            # Update progress bar
            water_pbar.update(batch_processed)
            water_pbar.set_postfix(
                {
                    "batch_time": f"{chunk_time:.1f}s",
                    "rate": f"{batch_processed / chunk_time:.0f} fields/s",
                }
            )

            self.log.info(
                f"✅ Water batch {chunk_num}/{total_chunks} completed in {chunk_time:.2f}s - Rate: {batch_processed / chunk_time:.0f} fields/s"
            )

        water_pbar.close()

        # Step 5: Create final results table with PROPER DEDUPLICATION
        # CRITICAL FIX: Join on field_uuid but ensure no duplication by using DISTINCT
        # Each DisaggregatedID should appear exactly once in the output
        self.conn.execute("""
            CREATE OR REPLACE TABLE proximity_results AS
            SELECT DISTINCT
                cd.*,
                COALESCE(rp.residential_buildings_formatted, '') as residential_buildings_formatted,
                COALESCE(ep.educational_facilities_formatted, '') as educational_facilities_formatted,
                COALESCE(wp.water_distance_formatted, '') as water_distance_formatted
            FROM current_disaggregation cd
            LEFT JOIN residential_proximity rp ON cd.field_uuid = rp.field_uuid
            LEFT JOIN educational_proximity ep ON cd.field_uuid = ep.field_uuid
            LEFT JOIN water_proximity wp ON cd.field_uuid = wp.field_uuid
        """)

        # Get result count and validate deduplication
        result_count = self.conn.execute("SELECT COUNT(*) FROM proximity_results").fetchone()[0]
        unique_disaggregated_ids = self.conn.execute(
            "SELECT COUNT(DISTINCT DisaggregatedID) FROM proximity_results"
        ).fetchone()[0]

        # CRITICAL VALIDATION: Ensure no duplication occurred
        if result_count != unique_disaggregated_ids:
            self.log.error(
                f"❌ DUPLICATION DETECTED: {result_count:,} records but only {unique_disaggregated_ids:,} unique DisaggregatedIDs"
            )
            self.log.error(f"❌ Duplication ratio: {result_count / unique_disaggregated_ids:.1f}x")
            raise ValueError(
                f"Data duplication detected in proximity results: {result_count:,} records vs {unique_disaggregated_ids:,} unique IDs"
            )

        self.log.info(
            f"✅ Proximity analysis complete: {result_count:,} records with proximity data (no duplication)"
        )
        self.log.info(
            f"✅ Data quality check passed: {result_count:,} records = {unique_disaggregated_ids:,} unique DisaggregatedIDs"
        )
        return result_count

    async def _save_year_results(self, year: int, record_count: int) -> None:
        """Save proximity analysis results for the year."""

        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = f"{self.config.dataset}_{year}_{year + 1}"
        output_path = f"gs://{self.config.bucket}/gold/{dataset_name}/{timestamp}/pesticide_proximity_{year}_{year + 1}.parquet"

        self.log.info(f"💾 Saving {record_count:,} proximity records to: {output_path}")

        # Export results to GCS
        self.gcs_access.upload_from_duckdb_table("proximity_results", output_path)

        self.log.info(f"✅ PROXIMITY OUTPUT: Year {year} results saved to: {output_path}")
        self.log.info(f"📁 Proximity GCS Path: {output_path}")
        print(f"✅ PROXIMITY OUTPUT: Year {year} results saved to: {output_path}")
        print(f"📁 Proximity GCS Path: {output_path}")

    def get_schema_info(self) -> Dict[str, Any]:
        """Return schema information for the proximity analysis output."""
        return {
            "output_columns": [
                "All columns from disaggregated pesticide data",
                "residential_buildings_formatted: VARCHAR (newline-separated addresses with distances)",
                "educational_facilities_formatted: VARCHAR (newline-separated addresses with distances)",
                "water_distance_formatted: VARCHAR (closest water distance in meters)",
            ],
            "spatial_analysis": {
                "building_radius_m": self.config.building_proximity_distance_m,
                "water_radius_m": self.config.water_proximity_distance_m,
                "coordinate_system": "EPSG:25832 (UTM32N)",
            },
        }
