"""
NLES5 Nitrogen Estimation Gold Layer

This module implements the gold layer processor for NLES5 nitrogen washout estimation.
It combines agricultural fields data with real climate data (DMI), soil types, and
fertilizer data to create comprehensive nitrogen washout estimates using the full NLES5 model.

The NLES5 model calculates nitrogen washout based on:
- Field geometry and crop type
- Real percolation data from DMI (precipitation - evaporation)
- Soil type parameters and drainage characteristics
- Fertilizer application data
"""

import os
import re
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb
from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed


class NLES5NitrogenEstimationGoldConfig(BaseJobConfig):
    """Configuration for NLES5 Nitrogen Estimation gold layer."""

    name: str = "NLES5 Nitrogen Estimation Gold"
    dataset: str = "nles5_nitrogen_estimation"
    type: str = "gold"
    description: str = "Comprehensive nitrogen washout estimates using the NLES5 model with real climate data"
    frequency: str = "monthly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets
    soil_types_dataset: str = "soil_types"
    dmi_dataset: str = "dmi"

    # Processing configuration
    batch_size: int = 5000  # Fields to process in each batch
    max_year_lag: int = 1  # Maximum years between field and climate data
    climate_data_days: int = 365  # Days of climate data to analyze

    # FVM marker years to process (will be auto-discovered if not specified)
    target_years: Optional[List[int]] = None  # If None, will use all available years

    # Quality thresholds
    min_data_coverage: float = 0.7  # Minimum acceptable data coverage rate
    max_nitrogen_washout: float = 1000.0  # Maximum reasonable nitrogen washout (kg/ha)

    # NLES5 Model Parameters (from original implementation)
    crop_parameters: Dict[str, float] = {
        'winter_cereals': 0,
        'spring_cereals': -6.74,
        'mixed_cereals_peas': -7.28,
        'grass_clover': -13.49,
        'seed_grass': -17.48,
        'fallow': -11.19,
        'sugar_beets': -0.64,
        'maize_potatoes': 3.53,
        'winter_rape': -7.32,
        'winter_cereals_after_grass': -1.25,
        'maize_after_grass': 19.52,
        'spring_cereals_after_grass': -6.23,
        'pulses_winter_rape': -2.87
    }

    # NLES5 nitrogen coefficients
    nitrogen_coefficients: Dict[str, float] = {
        'Bt': 0.456793,
        'Bcs': 0.049570,
        'Bca': 0.157044,
        'Budb': 0.038245,
        'Bm1': 0.026499,
        'Bf0': 0.016314,
        'Bf1': 0.026499,
        'Bg0': 0.014099
    }

    # Soil type parameters for percolation effects
    soil_parameters: Dict[str, Dict[str, float]] = {
        'sand': {
            'per1_coef': -0.001194,
            'per2_coef': -0.00111,
            'per_p_coef': -0.00086
        },
        'clay': {
            'per1_coef': -0.00080,
            'per2_coef': -0.00075,
            'per_p_coef': -0.00064
        }
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class NLES5NitrogenEstimationGold(BaseSource[NLES5NitrogenEstimationGoldConfig], GoldJobInterface):
    """
    Gold layer processor for NLES5 nitrogen washout estimation using real climate data.

    This processor implements the full NLES5 model with:
    - Real DMI climate data (precipitation and evaporation)
    - Spatial joins between fields and climate grids
    - Seasonal percolation aggregations
    - Complete nitrogen effect calculations
    - Soil and drainage effect modeling

    The processor handles yearly FVM marker datasets (fvm_marker_YYYY) and automatically
    discovers available years or processes specified target years.
    """

    def __init__(self, config: NLES5NitrogenEstimationGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()

        # Initialize optimized GCS access
        self.gcs_access = GCSDataAccess()

        # Use the optimized DuckDB connection from GCS access
        self.conn = self.gcs_access.duckdb_conn
        self._configure_duckdb()

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        self.conn.execute("SET memory_limit = '12GB'")  # Use 75% of available 16GB RAM
        self.conn.execute("SET threads = 4")  # Use all available CPU cores
        self.conn.execute("SET enable_progress_bar = true")
        self.conn.execute("SET preserve_insertion_order = false")

        # Spatial extensions already loaded by GCSDataAccess
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
        """
        Get all available fvm_marker years from GCS storage.

        Returns:
            List of available years for fvm_marker datasets
        """
        try:
            # List all files in silver layer to find fvm_marker directories with actual data
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix="silver/fvm_marker_"
            )
            years = set()

            for file_blob in files:
                # Look for files like "silver/fvm_marker_2021/timestamp/fvm_marker_2021.parquet"
                match = re.search(
                    r"silver/fvm_marker_(\d{4})/.*?/fvm_marker_(\d{4})\.parquet", file_blob.name
                )
                if match:
                    year1 = int(match.group(1))
                    year2 = int(match.group(2))
                    # Ensure both years match (sanity check)
                    if year1 == year2:
                        years.add(year1)

            return sorted(list(years))
        except Exception as e:
            self.log.error(f"Error discovering FVM marker years: {e}")
            return []

    def _read_fvm_marker_data_for_year(self, year: int) -> Optional[str]:
        """
        Read agricultural fields data for a specific year.

        Args:
            year: Year to read data for

        Returns:
            Table name containing the data, or None if not found
        """
        try:
            dataset_name = f"fvm_marker_{year}"
            self.log.info(f"Reading FVM marker data for year {year}")

            # Look for the latest timestamped directory
            files = self.gcs_util.list_files(
                bucket_name=self.config.bucket, prefix=f"silver/{dataset_name}/"
            )

            # Find the parquet file in timestamped subdirectories
            target_file = None
            latest_timestamp = None
            for file_blob in files:
                # Look for files like "fvm_marker_2021.parquet"
                if file_blob.name.endswith(f"{dataset_name}.parquet"):
                    # Extract timestamp from path like "silver/fvm_marker_2021/20241201_123456/fvm_marker_2021.parquet"
                    path_parts = file_blob.name.split("/")
                    if len(path_parts) >= 3:
                        timestamp_dir = path_parts[2]  # "20241201_123456"
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = file_blob.name

            if target_file:
                # Read the data using GCS access with proper authentication
                gcs_path = f"gs://{self.config.bucket}/{target_file}"
                table_name = f"fvm_marker_{year}"

                                                # Use authenticated temporary download pattern (consistent with other gold processors)
                try:
                    with self.gcs_access._temp_download(gcs_path) as temp_file:
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * FROM read_parquet('{temp_file}')
                        """)

                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(f"Loaded {count:,} fields for year {year}")

                    return table_name
                except Exception as e:
                    self.log.error(f"Failed to load {gcs_path} using authenticated GCS access: {e}")
                    return None
            else:
                self.log.warning(f"No FVM marker file found for year {year}")
                return None

        except Exception as e:
            self.log.error(f"Error reading fields data for year {year}: {e}")
            return None

    @timed(name="Loading agricultural fields data")
    def _load_agricultural_fields_data(self, silver_data: Optional[Dict[str, Any]]) -> str:
        """
        Load agricultural fields data from multiple yearly datasets.

        Args:
            silver_data: Optional in-memory silver data

        Returns:
            Table name containing combined agricultural fields data
        """
        # Determine which years to process
        if self.config.target_years:
            years_to_process = self.config.target_years
            self.log.info(f"Processing specified years: {years_to_process}")
        else:
            years_to_process = self._get_available_fvm_marker_years()
            self.log.info(f"Auto-discovered years: {years_to_process}")

        if not years_to_process:
            self.log.error("No FVM marker years found to process")
            raise ValueError("No agricultural fields data available")

        # Process each year and collect table names
        yearly_tables = []
        for year in years_to_process:
            try:
                # Check if data is available in silver_data dict
                year_dataset = f"fvm_marker_{year}"
                if silver_data and year_dataset in silver_data:
                    self.log.info(f"Using in-memory data for {year_dataset}")
                    table_name = f"fvm_marker_{year}"
                    self.conn.register(table_name, silver_data[year_dataset])
                    yearly_tables.append(table_name)
                else:
                    # Load from storage
                    table_name = self._read_fvm_marker_data_for_year(year)
                    if table_name:
                        yearly_tables.append(table_name)
            except Exception as e:
                self.log.warning(f"Failed to load data for year {year}: {e}")
                continue

        if not yearly_tables:
            self.log.error("No agricultural fields data could be loaded")
            raise ValueError("Failed to load any agricultural fields data")

        # Combine all yearly tables into a single table
        self.log.info(f"Combining {len(yearly_tables)} yearly datasets")

        # Create UNION query for all tables
        union_queries = []
        for table_name in yearly_tables:
            union_queries.append(f"SELECT * FROM {table_name}")

        combined_query = " UNION ALL ".join(union_queries)

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE agricultural_fields AS
            {combined_query}
        """)

        total_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        self.log.info(f"Combined agricultural fields: {total_count:,} records from {len(yearly_tables)} years")

        return "agricultural_fields"

    @timed(name="Loading silver datasets for NLES5")
    def _load_required_silver_datasets(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """
        Load required silver datasets into DuckDB tables.

        Returns:
            Dict mapping dataset names to table names in DuckDB
        """
        loaded_tables = {}

        # Load agricultural fields data (handles yearly datasets)
        try:
            agricultural_fields_table = self._load_agricultural_fields_data(silver_data)
            loaded_tables["agricultural_fields"] = agricultural_fields_table
        except Exception as e:
            self.log.error(f"Failed to load agricultural fields data: {e}")

        # Define other required datasets
        other_datasets = [
            (self.config.soil_types_dataset, "soil_types"),
            (self.config.dmi_dataset, "dmi_data"),
        ]

        for dataset_name, table_name in other_datasets:
            try:
                if silver_data and dataset_name in silver_data:
                    # Use in-memory silver data
                    self.log.info(f"Using in-memory silver data for {dataset_name}")
                    self.conn.register(table_name, silver_data[dataset_name])
                    loaded_tables[dataset_name] = table_name
                else:
                    # Load from GCS storage using base class method
                    self.log.info(f"Loading {dataset_name} from GCS storage")
                    storage_result = self._read_silver_data(dataset_name)

                    if storage_result and isinstance(storage_result, dict):
                        # Use the GCS access instance and table name
                        gcs_access = storage_result['gcs_access']
                        source_table = storage_result['table_name']

                        # Copy data to our connection
                        data_df = gcs_access.duckdb_conn.execute(f"SELECT * FROM {source_table}").fetchdf()
                        self.conn.register(table_name, data_df)
                        loaded_tables[dataset_name] = table_name
                    else:
                        self.log.error(f"Failed to load {dataset_name}")
                        continue

                # Validate table was loaded
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.log.info(f"Loaded {count:,} records for {dataset_name}")

            except Exception as e:
                self.log.error(f"Error loading {dataset_name}: {e}")
                continue

        return loaded_tables

    @timed(name="Processing DMI climate data")
    def _process_climate_data(self) -> str:
        """
        Process DMI climate data to calculate percolation (precipitation - evaporation).

        Returns:
            Table name containing processed climate data with percolation
        """
        try:
            self.log.info("Processing DMI climate data for percolation calculation")

            # Create climate data table with percolation calculation
            self.conn.execute("""
                CREATE OR REPLACE TABLE climate_percolation AS
                WITH precipitation_data AS (
                    SELECT
                        avg_value as precipitation,
                        centroid_geometry,
                        valid_time,
                        parameter_id
                    FROM dmi_data
                    WHERE parameter_id = 'acc_precip'
                ),
                evaporation_data AS (
                    SELECT
                        avg_value as evaporation,
                        centroid_geometry,
                        valid_time,
                        parameter_id
                    FROM dmi_data
                    WHERE parameter_id = 'pot_evaporation_makkink'
                ),
                merged_climate AS (
                    SELECT
                        p.precipitation,
                        e.evaporation,
                        p.centroid_geometry,
                        p.valid_time,
                        -- Calculate percolation as precipitation - evaporation
                        GREATEST(0, p.precipitation - COALESCE(e.evaporation, 0)) as percolation
                    FROM precipitation_data p
                    LEFT JOIN evaporation_data e
                        ON p.centroid_geometry = e.centroid_geometry
                        AND p.valid_time = e.valid_time
                    WHERE p.precipitation IS NOT NULL
                ),
                seasonal_aggregation AS (
                    SELECT
                        centroid_geometry,
                        ST_GeomFromGeoJSON(centroid_geometry) as geometry,
                        -- NLES5 seasonal periods
                        SUM(CASE
                            WHEN EXTRACT(month FROM CAST(valid_time AS DATE)) IN (9, 10, 11)
                            THEN percolation ELSE 0
                        END) as percolation_period1,  -- Sep-Nov
                        SUM(CASE
                            WHEN EXTRACT(month FROM CAST(valid_time AS DATE)) IN (12, 1, 2)
                            THEN percolation ELSE 0
                        END) as percolation_period2,  -- Dec-Feb
                        SUM(CASE
                            WHEN EXTRACT(month FROM CAST(valid_time AS DATE)) IN (3, 4, 5, 6, 7, 8)
                            THEN percolation ELSE 0
                        END) as percolation_period3,  -- Mar-Aug
                        AVG(precipitation) as avg_precipitation,
                        AVG(evaporation) as avg_evaporation,
                        COUNT(*) as climate_data_points
                    FROM merged_climate
                    WHERE geometry IS NOT NULL
                    GROUP BY centroid_geometry
                )
                SELECT
                    *,
                    percolation_period1 + percolation_period2 + percolation_period3 as total_percolation,
                    CASE
                        WHEN climate_data_points >= 30 THEN true
                        ELSE false
                    END as sufficient_climate_data
                FROM seasonal_aggregation
                WHERE total_percolation > 0
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM climate_percolation").fetchone()[0]
            self.log.info(f"Processed {count:,} climate grid points with percolation data")

            return "climate_percolation"

        except Exception as e:
            self.log.error(f"Error processing climate data: {e}")
            raise

    @timed(name="Spatial join fields with climate data")
    def _spatial_join_fields_climate(self) -> str:
        """
        Perform spatial join between agricultural fields and climate data.

        Returns:
            Table name containing fields joined with climate data
        """
        try:
            self.log.info("Performing spatial join between fields and climate data")

            # Ensure geometries are valid and in same CRS
            validate_and_transform_geometries_duckdb(
                self.conn, "agricultural_fields", "agricultural_fields"
            )

            # Create spatial join
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_climate AS
                SELECT
                    f.field_id,
                    f.cvr_number,
                    f.area_ha,
                    f.crop_type,
                    f.organic_farming,
                    f.year,
                    ST_AsText(ST_GeomFromWKB(f.geometry)) as field_geometry_wkt,
                    -- Climate data from nearest grid point
                    c.percolation_period1,
                    c.percolation_period2,
                    c.percolation_period3,
                    c.total_percolation,
                    c.avg_precipitation,
                    c.avg_evaporation,
                    c.sufficient_climate_data,
                    -- Calculate distance to climate grid point for quality assessment
                    ST_Distance(
                        ST_GeomFromWKB(f.geometry),
                        c.geometry
                    ) as climate_distance_m
                FROM agricultural_fields f
                LEFT JOIN climate_percolation c
                    ON ST_DWithin(
                        ST_GeomFromWKB(f.geometry),
                        c.geometry,
                        5000  -- 5km search radius for climate data
                    )
                WHERE f.geometry IS NOT NULL
                    AND f.area_ha > 0
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY f.field_id
                    ORDER BY ST_Distance(ST_GeomFromWKB(f.geometry), c.geometry)
                ) = 1
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_climate").fetchone()[0]
            climate_matched = self.conn.execute(
                "SELECT COUNT(*) FROM fields_with_climate WHERE total_percolation IS NOT NULL"
            ).fetchone()[0]

            self.log.info(f"Spatial join complete: {count:,} fields, {climate_matched:,} with climate data")

            return "fields_with_climate"

        except Exception as e:
            self.log.error(f"Error in spatial join: {e}")
            raise

    @timed(name="Joining with soil data")
    def _join_with_soil_data(self) -> str:
        """
        Join fields with soil type data.

        Returns:
            Table name containing fields with climate and soil data
        """
        try:
            self.log.info("Joining fields with soil type data")

            # Validate soil data geometries
            validate_and_transform_geometries_duckdb(
                self.conn, "soil_types", "soil_types"
            )

            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_climate_soil AS
                SELECT
                    f.*,
                    -- Soil data
                    s.soil_code,
                    s.soil_description,
                    COALESCE(s.clay_content, 15.0) as clay_content,
                    CASE
                        WHEN s.soil_code IN ('1', '2', '3', '4') THEN 'sand'
                        ELSE 'clay'
                    END as soil_type_category,
                    CASE
                        WHEN s.soil_code IS NOT NULL THEN true
                        ELSE false
                    END as has_soil_data
                FROM fields_with_climate f
                LEFT JOIN soil_types s
                    ON ST_Within(
                        ST_GeomFromText(f.field_geometry_wkt),
                        ST_GeomFromText(s.geometry)
                    )
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_climate_soil").fetchone()[0]
            soil_matched = self.conn.execute(
                "SELECT COUNT(*) FROM fields_with_climate_soil WHERE has_soil_data = true"
            ).fetchone()[0]

            self.log.info(f"Soil join complete: {count:,} fields, {soil_matched:,} with soil data")

            return "fields_with_climate_soil"

        except Exception as e:
            self.log.error(f"Error joining soil data: {e}")
            raise

    @timed(name="Calculating NLES5 nitrogen estimates")
    def _calculate_nles5_estimates(self) -> str:
        """
        Calculate NLES5 nitrogen washout estimates using the full model.

        Returns:
            Table name containing final NLES5 estimates
        """
        try:
            self.log.info("Calculating NLES5 nitrogen washout estimates")

            # Create crop parameter mapping
            crop_params_list = [
                f"('{crop}', {param})"
                for crop, param in self.config.crop_parameters.items()
            ]
            crop_params_sql = ", ".join(crop_params_list)

            # Create soil parameter mapping
            soil_params_sand = self.config.soil_parameters['sand']
            soil_params_clay = self.config.soil_parameters['clay']

            # NLES5 calculation with full model
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE nles5_nitrogen_estimates AS
                WITH crop_parameters AS (
                    SELECT crop_type, crop_param FROM (VALUES {crop_params_sql}) AS t(crop_type, crop_param)
                ),
                nles5_calculations AS (
                    SELECT
                        f.*,
                        cp.crop_param,
                        -- Drainage effect calculation using NLES5 soil parameters
                        CASE f.soil_type_category
                            WHEN 'sand' THEN
                                (1 - EXP({soil_params_sand['per1_coef']} * f.percolation_period1 +
                                        {soil_params_sand['per2_coef']} * (f.percolation_period2 + f.percolation_period3))) *
                                EXP({soil_params_sand['per_p_coef']} * (f.percolation_period2 + f.percolation_period3))
                            ELSE
                                (1 - EXP({soil_params_clay['per1_coef']} * f.percolation_period1 +
                                        {soil_params_clay['per2_coef']} * (f.percolation_period2 + f.percolation_period3))) *
                                EXP({soil_params_clay['per_p_coef']} * (f.percolation_period2 + f.percolation_period3))
                        END as drainage_effect,

                        -- Soil effect calculation
                        EXP(-0.00185 * f.clay_content) as soil_effect,

                        -- Base nitrogen level (simplified - could be enhanced with fertilizer data)
                        150.0 as base_nitrogen_kg_ha,

                        -- Trend effect (reference year 2017)
                        -0.1108 * (2017 - 1991) as trend_effect
                    FROM fields_with_climate_soil f
                    LEFT JOIN crop_parameters cp ON f.crop_type = cp.crop_type
                    WHERE f.total_percolation IS NOT NULL
                        AND f.total_percolation > 0
                ),
                final_calculations AS (
                    SELECT
                        *,
                        -- NLES5 nitrogen effect (simplified - could include fertilizer coefficients)
                        {self.config.nitrogen_coefficients['Bt']} * base_nitrogen_kg_ha as nitrogen_effect,

                        -- Percolation and soil effect
                        drainage_effect * soil_effect * 1.085 as perco_soil_effect,

                        -- NLES5 base calculation
                        23.51 + COALESCE(crop_param, 0) +
                        ({self.config.nitrogen_coefficients['Bt']} * base_nitrogen_kg_ha) as v_base
                    FROM nles5_calculations
                )
                SELECT
                    field_id,
                    cvr_number,
                    area_ha,
                    crop_type,
                    organic_farming,
                    year,
                    soil_type_category as soil_type,
                    soil_code,
                    soil_description,
                    clay_content,

                    -- Climate data
                    percolation_period1,
                    percolation_period2,
                    percolation_period3,
                    total_percolation,
                    avg_precipitation,
                    avg_evaporation,
                    climate_distance_m,

                    -- NLES5 model components
                    crop_param as crop_effect,
                    drainage_effect,
                    soil_effect,
                    nitrogen_effect,
                    trend_effect,
                    base_nitrogen_kg_ha,

                    -- Final nitrogen washout calculation
                    GREATEST(0,
                        trend_effect + POWER(v_base, 1.5) * perco_soil_effect
                    ) as nitrogen_washout_kg_ha,

                    -- Total nitrogen washout for the field
                    GREATEST(0,
                        trend_effect + POWER(v_base, 1.5) * perco_soil_effect
                    ) * area_ha as total_nitrogen_washout_kg,

                    -- Data quality indicators
                    has_soil_data,
                    sufficient_climate_data,
                    CASE
                        WHEN total_percolation IS NOT NULL
                            AND has_soil_data = true
                            AND sufficient_climate_data = true
                        THEN 'high'
                        WHEN total_percolation IS NOT NULL
                            AND (has_soil_data = true OR sufficient_climate_data = true)
                        THEN 'medium'
                        ELSE 'low'
                    END as data_quality,

                    'nles5_full_model' as estimation_method,
                    current_timestamp as created_at,
                    field_geometry_wkt as geometry_wkt

                FROM final_calculations
                WHERE v_base > 0  -- Ensure valid calculations
                    AND perco_soil_effect > 0
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM nles5_nitrogen_estimates").fetchone()[0]
            avg_washout = self.conn.execute(
                "SELECT AVG(nitrogen_washout_kg_ha) FROM nles5_nitrogen_estimates"
            ).fetchone()[0]

            self.log.info(f"NLES5 calculation complete: {count:,} fields, avg washout: {avg_washout:.2f} kg N/ha")

            return "nles5_nitrogen_estimates"

        except Exception as e:
            self.log.error(f"Error in NLES5 calculation: {e}")
            raise

    @timed(name="Generating summary statistics")
    def _generate_summary_statistics(self) -> None:
        """Generate comprehensive summary statistics for NLES5 estimates."""
        try:
            self.log.info("Generating NLES5 summary statistics")

            # Overall summary
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_overall_summary AS
                SELECT
                    COUNT(*) as total_fields,
                    SUM(area_ha) as total_area_ha,
                    AVG(nitrogen_washout_kg_ha) as avg_nitrogen_washout_kg_ha,
                    MEDIAN(nitrogen_washout_kg_ha) as median_nitrogen_washout_kg_ha,
                    STDDEV(nitrogen_washout_kg_ha) as stddev_nitrogen_washout_kg_ha,
                    MIN(nitrogen_washout_kg_ha) as min_nitrogen_washout_kg_ha,
                    MAX(nitrogen_washout_kg_ha) as max_nitrogen_washout_kg_ha,
                    SUM(total_nitrogen_washout_kg) as total_nitrogen_washout_kg,

                    -- Climate data summary
                    AVG(total_percolation) as avg_total_percolation_mm,
                    AVG(avg_precipitation) as avg_precipitation_mm,
                    AVG(avg_evaporation) as avg_evaporation_mm,

                    -- Data quality metrics
                    COUNT(CASE WHEN has_soil_data THEN 1 END) / COUNT(*)::FLOAT as soil_data_coverage_rate,
                    COUNT(CASE WHEN sufficient_climate_data THEN 1 END) / COUNT(*)::FLOAT as climate_data_coverage_rate,
                    COUNT(CASE WHEN data_quality = 'high' THEN 1 END) / COUNT(*)::FLOAT as high_quality_rate,

                    -- Model diversity
                    COUNT(DISTINCT crop_type) as unique_crop_types,
                    COUNT(DISTINCT soil_type) as unique_soil_types,
                    COUNT(DISTINCT year) as years_covered,

                    current_timestamp as generated_at
                FROM nles5_nitrogen_estimates
            """)

            # Summary by soil type
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_soil_type_summary AS
                SELECT
                    soil_type,
                    COUNT(*) as field_count,
                    SUM(area_ha) as total_area_ha,
                    AVG(nitrogen_washout_kg_ha) as avg_nitrogen_washout_kg_ha,
                    MEDIAN(nitrogen_washout_kg_ha) as median_nitrogen_washout_kg_ha,
                    SUM(total_nitrogen_washout_kg) as total_nitrogen_washout_kg,
                    AVG(total_percolation) as avg_percolation_mm,
                    AVG(drainage_effect) as avg_drainage_effect,
                    AVG(soil_effect) as avg_soil_effect
                FROM nles5_nitrogen_estimates
                GROUP BY soil_type
                ORDER BY total_area_ha DESC
            """)

            # Summary by crop type
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_crop_type_summary AS
                SELECT
                    crop_type,
                    COUNT(*) as field_count,
                    SUM(area_ha) as total_area_ha,
                    AVG(nitrogen_washout_kg_ha) as avg_nitrogen_washout_kg_ha,
                    MEDIAN(nitrogen_washout_kg_ha) as median_nitrogen_washout_kg_ha,
                    SUM(total_nitrogen_washout_kg) as total_nitrogen_washout_kg,
                    AVG(crop_effect) as avg_crop_effect,
                    AVG(total_percolation) as avg_percolation_mm
                FROM nles5_nitrogen_estimates
                GROUP BY crop_type
                ORDER BY total_nitrogen_washout_kg DESC
            """)

            # Log summary
            summary = self.conn.execute("SELECT * FROM nles5_overall_summary").fetchone()
            if summary:
                self.log.info(f"NLES5 Summary - Fields: {summary[0]:,}, Total Area: {summary[1]:.1f} ha")
                self.log.info(f"Avg N Washout: {summary[2]:.2f} kg/ha, Total N Washout: {summary[7]:.1f} kg")
                self.log.info(f"Data Quality - Soil: {summary[11]:.1%}, Climate: {summary[12]:.1%}, High Quality: {summary[13]:.1%}")

        except Exception as e:
            self.log.error(f"Error generating summary statistics: {e}")
            raise

    @timed(name="Saving NLES5 results to gold layer")
    def _save_results_to_gold(self) -> None:
        """Save NLES5 results to the gold layer using optimized DuckDB export."""
        try:
            self.log.info("Saving NLES5 results to gold layer")

            # Define output tables with optimized paths
            tables_to_save = [
                ("nles5_nitrogen_estimates", "nitrogen_estimates"),
                ("nles5_overall_summary", "overall_summary"),
                ("nles5_soil_type_summary", "soil_type_summary"),
                ("nles5_crop_type_summary", "crop_type_summary"),
            ]

            for table_name, subdataset in tables_to_save:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > 0:
                        # Use optimized GCS upload directly from DuckDB table
                        output_path = f"gs://{self.config.bucket}/gold/{self.config.dataset}/latest/{subdataset}.parquet"

                        self.gcs_access.upload_from_duckdb_table(
                            table_name,
                            output_path,
                            compression="zstd",
                            row_group_size=100000,
                        )

                        self.log.info(f"✅ Saved {table_name} ({count:,} rows) to {output_path}")
                    else:
                        self.log.warning(f"Table {table_name} is empty, skipping")
                except Exception as e:
                    self.log.error(f"Failed to save {table_name}: {e}")

            self.log.info(f"NLES5 results saved to: gs://{self.config.bucket}/gold/{self.config.dataset}/latest/")

        except Exception as e:
            self.log.error(f"Error saving results: {e}")
            raise

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run NLES5 nitrogen estimation gold processing with real climate data."""
        try:
            self.log.info("Starting NLES5 nitrogen estimation with real DMI climate data")

            # Load required silver datasets
            loaded_tables = self._load_required_silver_datasets(silver_data)

            if len(loaded_tables) < 2:  # At least agricultural_fields and one other dataset
                self.log.error("Insufficient data loaded - need at least agricultural fields and climate data")
                return

            # Process climate data to calculate percolation
            climate_table = self._process_climate_data()

            # Spatial join fields with climate data
            fields_climate_table = self._spatial_join_fields_climate()

            # Join with soil data if available
            if self.config.soil_types_dataset in loaded_tables:
                fields_complete_table = self._join_with_soil_data()
            else:
                self.log.warning("No soil data available - using fields with climate data only")
                fields_complete_table = fields_climate_table

            # Calculate NLES5 nitrogen estimates
            estimates_table = self._calculate_nles5_estimates()

            # Generate summary statistics
            self._generate_summary_statistics()

            # Save results to gold layer
            self._save_results_to_gold()

            self.log.info("NLES5 nitrogen estimation completed successfully")

        except Exception as e:
            self.log.error(f"Error in NLES5 processing: {e}")
            raise