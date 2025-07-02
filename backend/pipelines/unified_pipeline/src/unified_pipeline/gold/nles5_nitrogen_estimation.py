"""
NLES5 Nitrogen Estimation Gold Layer

This module implements the gold layer processor for NLES5 nitrogen washout estimation.
It combines agricultural fields data with real climate data (DMI), soil types, and
fertilizer data to create comprehensive nitrogen washout estimates using the full NLES5 model.

ENHANCED IMPLEMENTATION (Updated):
The processor now includes full fertilizer data integration following the complete NLES5 model:

The NLES5 model calculates nitrogen washout based on:
- Field geometry and crop type with accurate crop parameters
- Real percolation data from DMI (precipitation - evaporation) in 3 seasonal periods
- Soil type parameters and drainage characteristics (sand vs clay)
- Complete fertilizer application data:
  * Total nitrogen quota (tn_t_ha)
  * Mineral nitrogen spring application (mineral_n_foraar)
  * Mineral nitrogen autumn application (mineral_n_eft)
  * Mineral nitrogen applied during growing season (mineral_n_udb)
  * Organic nitrogen from livestock manure (organic_n_hus)
  * Harmoni area level calculations (niveau)
  * Nitrogen fixation from legumes (nfix_ha) - to be enhanced
- All 8 NLES5 nitrogen coefficients (Bt, Bcs, Bca, Budb, Bm1, Bf0, Bf1, Bg0)
- Trend effect calculation with reference year 2017

Final nitrogen washout formula: Y5 = trend_effect + V^1.5 * perco_soil_effect
Where V = 23.51 + crop_effect + nitrogen_effect

DATASETS INTEGRATED:
- Required: agricultural_fields (fvm_marker_YYYY), dmi_data, soil_types
- Optional: fertilizer_accounts, field_plan, catch_crops
- Graceful degradation when optional datasets are unavailable (uses defaults)

OUTPUT:
- Detailed nitrogen washout estimates per field with quality indicators
- Summary statistics by soil type, crop type, and overall
- Full audit trail of all model components and data sources
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
    fertilizer_dataset: str = "fertilizer_accounts"  # Add fertilizer data
    field_plan_dataset: str = "field_plan"  # Add field plan data
    catch_crops_dataset: str = "catch_crops"  # Add catch crop data (optional)

    # Processing configuration
    batch_size: int = 5000  # Fields to process in each batch
    max_year_lag: int = 1  # Maximum years between field and climate data
    climate_data_days: int = 365  # Days of climate data to analyze

    # FVM marker years to process (will be auto-discovered if not specified)
    target_years: Optional[List[int]] = None  # If None, will use all available years

    # Quality thresholds
    min_data_coverage: float = 0.7  # Minimum acceptable data coverage rate
    max_nitrogen_washout: float = 1000.0  # Maximum reasonable nitrogen washout (kg/ha)

    # Uncertainty estimation parameters
    uncertainty_estimation: bool = True  # Enable uncertainty calculations
    climate_distance_threshold: float = 5000.0  # meters - beyond this increases uncertainty
    data_age_threshold: int = 2  # years - older data increases uncertainty
    min_climate_observations: int = 30  # minimum for reliable climate data

    # Model coefficient uncertainties (standard errors from original NLES5 calibration)
    coefficient_uncertainties: Dict[str, float] = {
        'Bt': 0.202200,    # βNT: Total N in top 25cm soil layer (SE from DCA Rapport 163 Table 3.2)
        'Bcs': 0.007000,   # βCS: Mineral N application in spring (SE from DCA Rapport 163 Table 3.2)
        'Bca': 0.034257,   # βCA: Mineral N application in autumn (SE from DCA Rapport 163 Table 3.2)
        'Budb': 0.011056,  # βudb: Mineral N deposited by grazing animals (SE from DCA Rapport 163 Table 3.2)
        'Bm1': 0.006121,   # βm1: Effect of mineral and organic N in previous two years (SE from DCA Rapport 163 Table 3.2)
        'Bf0': 0.005530,   # βf0: Biological N fixation in current year (SE from DCA Rapport 163 Table 3.2)
        'Bf1': 0.006121,   # βf1: Biological N fixation in previous two years (SE from DCA Rapport 163 Table 3.2)
        'Bg0': 0.008799    # βg0: Organic N in animal manure in current year (SE from DCA Rapport 163 Table 3.2)
    }

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
            (self.config.fertilizer_dataset, "fertilizer_accounts"),
            (self.config.field_plan_dataset, "field_plan"),
        ]

        for dataset_name, table_name in other_datasets:
            try:
                if silver_data and dataset_name in silver_data:
                    # Use in-memory silver data
                    self.log.info(f"Using in-memory silver data for {dataset_name}")
                    self.conn.register(table_name, silver_data[dataset_name])
                    loaded_tables[dataset_name] = table_name
                else:
                    # Load from GCS storage using base class method - optional for fertilizer data
                    self.log.info(f"Attempting to load {dataset_name} from GCS storage")
                    try:
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
                            self.log.warning(f"Could not load {dataset_name} - will use defaults")
                            continue
                    except Exception as dataset_error:
                        self.log.warning(f"Failed to load optional dataset {dataset_name}: {dataset_error}")
                        continue

                # Validate table was loaded (skip for optional datasets that failed)
                if dataset_name in loaded_tables:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    self.log.info(f"Loaded {count:,} records for {dataset_name}")

            except Exception as e:
                if dataset_name in [self.config.fertilizer_dataset, self.config.field_plan_dataset]:
                    self.log.warning(f"Optional dataset {dataset_name} not available: {e}")
                    continue
                else:
                    self.log.error(f"Error loading required dataset {dataset_name}: {e}")
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

                        -- Fertilizer data (defaults if not available)
                        COALESCE(fert.total_nitrogen_kg_ha, 150.0) as tn_t_ha,
                        COALESCE(fert.mineral_n_spring_kg_ha, 0.0) as mineral_n_foraar,
                        COALESCE(fert.mineral_n_autumn_kg_ha, 0.0) as mineral_n_eft,
                        COALESCE(fert.mineral_n_applied_kg_ha, 0.0) as mineral_n_udb,
                        COALESCE(fert.organic_n_kg_ha, 0.0) as organic_n_hus,
                        COALESCE(fp.harmoni_areal, 0.0) as niveau,
                        0.0 as nfix_ha,  -- Nitrogen fixation - to be enhanced
                        0.0 as niveau_nfix,  -- Nitrogen fixation level - to be enhanced

                        -- Trend effect (reference year 2017)
                        -0.1108 * (2017 - 1991) as trend_effect
                    FROM fields_with_climate_soil f
                    LEFT JOIN crop_parameters cp ON f.crop_type = cp.crop_type
                    LEFT JOIN fertilizer_accounts fert ON f.field_id = fert.field_id AND f.year = fert.year
                    LEFT JOIN field_plan fp ON f.field_id = fp.field_id AND f.year = fp.year
                    WHERE f.total_percolation IS NOT NULL
                        AND f.total_percolation > 0
                ),
                nitrogen_calculations AS (
                    SELECT
                        *,
                        -- Full NLES5 nitrogen effect calculation using all coefficients
                        ({self.config.nitrogen_coefficients['Bt']} * tn_t_ha +
                         {self.config.nitrogen_coefficients['Bcs']} * mineral_n_foraar +
                         {self.config.nitrogen_coefficients['Bca']} * mineral_n_eft +
                         {self.config.nitrogen_coefficients['Budb']} * mineral_n_udb +
                         {self.config.nitrogen_coefficients['Bm1']} * (niveau + niveau) / 2.0 +
                         {self.config.nitrogen_coefficients['Bf0']} * nfix_ha +
                         {self.config.nitrogen_coefficients['Bf1']} * (niveau_nfix + niveau_nfix) / 2.0 +
                         {self.config.nitrogen_coefficients['Bg0']} * organic_n_hus) as nitrogen_effect,

                        -- Percolation and soil effect
                        drainage_effect * soil_effect * 1.085 as perco_soil_effect
                    FROM nles5_calculations
                ),
                final_calculations AS (
                    SELECT
                        *,
                        -- NLES5 base calculation (V)
                        23.51 + COALESCE(crop_param, 0) + nitrogen_effect as v_base
                    FROM nitrogen_calculations
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
                    v_base,

                    -- Fertilizer data components
                    tn_t_ha as total_nitrogen_kg_ha,
                    mineral_n_foraar as mineral_n_spring_kg_ha,
                    mineral_n_eft as mineral_n_autumn_kg_ha,
                    mineral_n_udb as mineral_n_applied_kg_ha,
                    organic_n_hus as organic_n_kg_ha,

                    -- Final nitrogen washout calculation (Y5 = trend + V^1.5 * perco_soil_effect)
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

    @timed(name="Validating NLES5 estimates")
    def _validate_nles5_estimates(self) -> bool:
        """
        Validate NLES5 estimates for data quality and reasonable values.

        Returns:
            True if validation passes, False otherwise
        """
        try:
            self.log.info("Validating NLES5 nitrogen estimates")

            # Check if any estimates were generated
            total_count = self.conn.execute("SELECT COUNT(*) FROM nles5_nitrogen_estimates").fetchone()[0]
            if total_count == 0:
                self.log.error("Validation failed: No NLES5 estimates generated")
                return False

            # Check for reasonable nitrogen washout values
            stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    AVG(nitrogen_washout_kg_ha) as avg_washout,
                    MIN(nitrogen_washout_kg_ha) as min_washout,
                    MAX(nitrogen_washout_kg_ha) as max_washout,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha < 0 THEN 1 END) as negative_count,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha > ? THEN 1 END) as excessive_count,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha IS NULL THEN 1 END) as null_count,
                    COUNT(CASE WHEN data_quality = 'high' THEN 1 END) as high_quality_count
                FROM nles5_nitrogen_estimates
            """, [self.config.max_nitrogen_washout]).fetchone()

            total_records, avg_washout, min_washout, max_washout, negative_count, excessive_count, null_count, high_quality_count = stats

            # Log validation statistics
            self.log.info(f"Validation Stats - Records: {total_records:,}, Avg: {avg_washout:.2f} kg N/ha")
            self.log.info(f"Range: {min_washout:.2f} to {max_washout:.2f} kg N/ha")
            self.log.info(f"High Quality: {high_quality_count:,} ({high_quality_count/total_records:.1%})")

            # Check for data quality issues
            warnings = []
            errors = []

            if negative_count > 0:
                warnings.append(f"{negative_count:,} records with negative nitrogen washout")

            if excessive_count > 0:
                warnings.append(f"{excessive_count:,} records with excessive nitrogen washout (>{self.config.max_nitrogen_washout} kg N/ha)")

            if null_count > 0:
                errors.append(f"{null_count:,} records with NULL nitrogen washout")

            if avg_washout < 0 or avg_washout > self.config.max_nitrogen_washout:
                errors.append(f"Average nitrogen washout ({avg_washout:.2f}) outside reasonable range")

            if high_quality_count / total_records < self.config.min_data_coverage:
                warnings.append(f"Low high-quality data coverage: {high_quality_count/total_records:.1%} < {self.config.min_data_coverage:.1%}")

            # Log warnings and errors
            for warning in warnings:
                self.log.warning(f"Validation warning: {warning}")

            for error in errors:
                self.log.error(f"Validation error: {error}")

            # Validation passes if no critical errors
            if errors:
                self.log.error("Validation failed due to critical errors")
                return False
            else:
                self.log.info("✅ NLES5 estimates validation passed")
                return True

        except Exception as e:
            self.log.error(f"Error during validation: {e}")
            return False

    @timed(name="Analyzing estimates distribution")
    def _analyze_estimates_distribution(self) -> None:
        """Analyze comprehensive distribution patterns for NLES5 estimates."""
        try:
            self.log.info("Analyzing NLES5 estimates distribution")

            # Overall estimates analysis
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_estimates_analysis AS
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

                    -- Fertilizer data summary
                    AVG(total_nitrogen_kg_ha) as avg_total_nitrogen_kg_ha,
                    AVG(mineral_n_spring_kg_ha) as avg_mineral_n_spring_kg_ha,
                    AVG(mineral_n_autumn_kg_ha) as avg_mineral_n_autumn_kg_ha,
                    AVG(organic_n_kg_ha) as avg_organic_n_kg_ha,

                    current_timestamp as generated_at
                FROM nles5_nitrogen_estimates
            """)

            # Estimates by soil type
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_estimates_by_soil_type AS
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

            # Estimates by crop type
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_estimates_by_crop_type AS
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

            # Uncertainty distribution analysis
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_uncertainty_analysis AS
                SELECT
                    COUNT(*) as total_fields_with_uncertainty,

                    -- Overall uncertainty distribution
                    AVG(total_uncertainty_pct) as avg_total_uncertainty_pct,
                    MEDIAN(total_uncertainty_pct) as median_total_uncertainty_pct,
                    MIN(total_uncertainty_pct) as min_total_uncertainty_pct,
                    MAX(total_uncertainty_pct) as max_total_uncertainty_pct,
                    STDDEV(total_uncertainty_pct) as stddev_total_uncertainty_pct,

                    -- Uncertainty class distribution
                    COUNT(CASE WHEN uncertainty_class = 'low' THEN 1 END) as low_uncertainty_count,
                    COUNT(CASE WHEN uncertainty_class = 'moderate' THEN 1 END) as moderate_uncertainty_count,
                    COUNT(CASE WHEN uncertainty_class = 'high' THEN 1 END) as high_uncertainty_count,
                    COUNT(CASE WHEN uncertainty_class = 'very_high' THEN 1 END) as very_high_uncertainty_count,

                    -- Uncertainty class percentages
                    COUNT(CASE WHEN uncertainty_class = 'low' THEN 1 END) / COUNT(*)::FLOAT as low_uncertainty_pct,
                    COUNT(CASE WHEN uncertainty_class = 'moderate' THEN 1 END) / COUNT(*)::FLOAT as moderate_uncertainty_pct,
                    COUNT(CASE WHEN uncertainty_class = 'high' THEN 1 END) / COUNT(*)::FLOAT as high_uncertainty_pct,
                    COUNT(CASE WHEN uncertainty_class = 'very_high' THEN 1 END) / COUNT(*)::FLOAT as very_high_uncertainty_pct,

                    -- Component uncertainty averages
                    AVG(spatial_uncertainty_climate_pct) as avg_spatial_climate_uncertainty_pct,
                    AVG(spatial_uncertainty_soil_pct) as avg_spatial_soil_uncertainty_pct,
                    AVG(temporal_uncertainty_climate_pct) as avg_temporal_climate_uncertainty_pct,
                    AVG(input_uncertainty_fertilizer_pct) as avg_fertilizer_uncertainty_pct,
                    AVG(input_uncertainty_percolation_pct) as avg_percolation_uncertainty_pct,
                    AVG(crop_parameter_uncertainty_pct) as avg_crop_uncertainty_pct,

                    -- Confidence interval coverage
                    AVG(washout_upper_95ci - washout_lower_95ci) as avg_95ci_width_kg_ha,
                    AVG(washout_upper_90ci - washout_lower_90ci) as avg_90ci_width_kg_ha,

                    current_timestamp as generated_at
                FROM nles5_uncertainty_estimates
            """)

            # Log estimates analysis
            analysis = self.conn.execute("SELECT * FROM nles5_estimates_analysis").fetchone()
            if analysis:
                self.log.info(f"NLES5 Analysis - Fields: {analysis[0]:,}, Total Area: {analysis[1]:.1f} ha")
                self.log.info(f"Avg N Washout: {analysis[2]:.2f} kg/ha, Total N Washout: {analysis[7]:.1f} kg")
                self.log.info(f"Data Quality - Soil: {analysis[11]:.1%}, Climate: {analysis[12]:.1%}, High Quality: {analysis[13]:.1%}")
                self.log.info(f"Fertilizer Data - Avg Total N: {analysis[17]:.1f} kg/ha, Spring: {analysis[18]:.1f} kg/ha")

            # Log uncertainty analysis
            uncertainty_analysis = self.conn.execute("SELECT * FROM nles5_uncertainty_analysis").fetchone()
            if uncertainty_analysis:
                self.log.info(f"Uncertainty Analysis - Avg: {uncertainty_analysis[1]:.1f}%, Range: {uncertainty_analysis[3]:.1f}%-{uncertainty_analysis[4]:.1f}%")
                self.log.info(f"Confidence Classes - Low: {uncertainty_analysis[11]:.1%}, Moderate: {uncertainty_analysis[12]:.1%}, High: {uncertainty_analysis[13]:.1%}")

        except Exception as e:
            self.log.error(f"Error generating summary statistics: {e}")
            raise

    @timed(name="Calculating uncertainty estimates")
    def _calculate_uncertainty_estimates(self) -> str:
        """
        Calculate comprehensive uncertainty estimates for NLES5 nitrogen washout predictions.

        Uncertainty sources considered:
        1. Spatial uncertainty (distance to climate/soil data)
        2. Temporal uncertainty (data age and coverage)
        3. Input data quality uncertainty
        4. Model parameter uncertainty
        5. Overall prediction uncertainty

        Returns:
            Table name containing uncertainty estimates
        """
        try:
            self.log.info("Calculating NLES5 uncertainty estimates")

            # Calculate dynamic coefficient uncertainty from actual NLES5 calibration standard errors
            coeff_uncertainties = self.config.coefficient_uncertainties
            avg_coeff_uncertainty = sum(coeff_uncertainties.values()) / len(coeff_uncertainties)

            self.log.info(f"Using official NLES5 coefficient uncertainties - average SE: {avg_coeff_uncertainty:.6f}")

            self.conn.execute(f"""
                CREATE OR REPLACE TABLE nles5_uncertainty_estimates AS
                WITH uncertainty_components AS (
                    SELECT
                        field_id,
                        nitrogen_washout_kg_ha,
                        v_base,
                        perco_soil_effect,

                        -- 1. SPATIAL UNCERTAINTY
                        -- Climate distance uncertainty (0-1 scale)
                        CASE
                            WHEN climate_distance_m <= {self.config.climate_distance_threshold/4} THEN 0.05  -- Very close: 5% uncertainty
                            WHEN climate_distance_m <= {self.config.climate_distance_threshold/2} THEN 0.10  -- Close: 10% uncertainty
                            WHEN climate_distance_m <= {self.config.climate_distance_threshold} THEN 0.20     -- Moderate: 20% uncertainty
                            ELSE 0.35  -- Far: 35% uncertainty
                        END as spatial_uncertainty_climate,

                        -- Soil data spatial uncertainty
                        CASE
                            WHEN has_soil_data = true THEN 0.10   -- 10% uncertainty with soil data
                            ELSE 0.25  -- 25% uncertainty using defaults
                        END as spatial_uncertainty_soil,

                        -- 2. TEMPORAL UNCERTAINTY
                        -- Climate data recency and coverage
                        CASE
                            WHEN sufficient_climate_data = true THEN 0.08   -- 8% uncertainty with good coverage
                            ELSE 0.20  -- 20% uncertainty with poor coverage
                        END as temporal_uncertainty_climate,

                        -- 3. INPUT DATA QUALITY UNCERTAINTY
                        -- Fertilizer data availability uncertainty
                        CASE
                            WHEN total_nitrogen_kg_ha > 0 AND mineral_n_spring_kg_ha >= 0 THEN 0.12  -- 12% with real fertilizer data
                            ELSE 0.30  -- 30% uncertainty using defaults
                        END as input_uncertainty_fertilizer,

                        -- Percolation data quality uncertainty
                        CASE
                            WHEN total_percolation > 0 AND total_percolation < 2000 THEN 0.15  -- 15% for reasonable percolation
                            ELSE 0.25  -- 25% for extreme or missing percolation
                        END as input_uncertainty_percolation,

                        -- 4. MODEL PARAMETER UNCERTAINTY
                        -- Coefficient uncertainty propagation (Monte Carlo approximation)
                        {avg_coeff_uncertainty} as coefficient_uncertainty_base,

                        -- Crop parameter uncertainty (varies by crop knowledge)
                        CASE crop_type
                            WHEN 'grass_clover' THEN 0.08      -- 8% - well studied
                            WHEN 'winter_cereals' THEN 0.10    -- 10% - well studied
                            WHEN 'spring_cereals' THEN 0.12    -- 12% - moderate knowledge
                            WHEN 'maize_potatoes' THEN 0.15    -- 15% - more variable
                            WHEN 'fallow' THEN 0.25            -- 25% - high uncertainty
                            ELSE 0.18  -- 18% - average for other crops
                        END as crop_parameter_uncertainty
                    FROM nles5_nitrogen_estimates
                ),
                combined_uncertainty AS (
                    SELECT
                        *,
                        -- 5. COMBINED UNCERTAINTY CALCULATION
                        -- Use root sum of squares for independent uncertainties
                        SQRT(
                            POW(spatial_uncertainty_climate, 2) +
                            POW(spatial_uncertainty_soil, 2) +
                            POW(temporal_uncertainty_climate, 2) +
                            POW(input_uncertainty_fertilizer, 2) +
                            POW(input_uncertainty_percolation, 2) +
                            POW(coefficient_uncertainty_base, 2) +
                            POW(crop_parameter_uncertainty, 2)
                        ) as total_relative_uncertainty,

                        -- Scale uncertainty based on model components
                        GREATEST(0.05, LEAST(0.60,
                            SQRT(
                                POW(spatial_uncertainty_climate, 2) +
                                POW(spatial_uncertainty_soil, 2) +
                                POW(temporal_uncertainty_climate, 2) +
                                POW(input_uncertainty_fertilizer, 2) +
                                POW(input_uncertainty_percolation, 2) +
                                POW(coefficient_uncertainty_base, 2) +
                                POW(crop_parameter_uncertainty, 2)
                            )
                        )) as bounded_relative_uncertainty
                    FROM uncertainty_components
                )
                SELECT
                    field_id,
                    nitrogen_washout_kg_ha,

                    -- UNCERTAINTY COMPONENTS (as percentages)
                    ROUND(spatial_uncertainty_climate * 100, 1) as spatial_uncertainty_climate_pct,
                    ROUND(spatial_uncertainty_soil * 100, 1) as spatial_uncertainty_soil_pct,
                    ROUND(temporal_uncertainty_climate * 100, 1) as temporal_uncertainty_climate_pct,
                    ROUND(input_uncertainty_fertilizer * 100, 1) as input_uncertainty_fertilizer_pct,
                    ROUND(input_uncertainty_percolation * 100, 1) as input_uncertainty_percolation_pct,
                    ROUND(coefficient_uncertainty_base * 100, 1) as coefficient_uncertainty_pct,
                    ROUND(crop_parameter_uncertainty * 100, 1) as crop_parameter_uncertainty_pct,

                    -- TOTAL UNCERTAINTY
                    ROUND(bounded_relative_uncertainty * 100, 1) as total_uncertainty_pct,
                    bounded_relative_uncertainty as total_relative_uncertainty,

                    -- CONFIDENCE INTERVALS (assuming normal distribution)
                    ROUND(nitrogen_washout_kg_ha * (1 - 1.96 * bounded_relative_uncertainty), 2) as washout_lower_95ci,
                    ROUND(nitrogen_washout_kg_ha * (1 + 1.96 * bounded_relative_uncertainty), 2) as washout_upper_95ci,
                    ROUND(nitrogen_washout_kg_ha * (1 - 1.645 * bounded_relative_uncertainty), 2) as washout_lower_90ci,
                    ROUND(nitrogen_washout_kg_ha * (1 + 1.645 * bounded_relative_uncertainty), 2) as washout_upper_90ci,

                    -- UNCERTAINTY CLASSIFICATION
                    CASE
                        WHEN bounded_relative_uncertainty <= 0.15 THEN 'low'           -- ≤15% uncertainty
                        WHEN bounded_relative_uncertainty <= 0.25 THEN 'moderate'      -- 15-25% uncertainty
                        WHEN bounded_relative_uncertainty <= 0.35 THEN 'high'          -- 25-35% uncertainty
                        ELSE 'very_high'  -- >35% uncertainty
                    END as uncertainty_class,

                    -- CONFIDENCE LEVEL (inverse of uncertainty)
                    CASE
                        WHEN bounded_relative_uncertainty <= 0.15 THEN 'high_confidence'
                        WHEN bounded_relative_uncertainty <= 0.25 THEN 'moderate_confidence'
                        WHEN bounded_relative_uncertainty <= 0.35 THEN 'low_confidence'
                        ELSE 'very_low_confidence'
                    END as confidence_level,

                    current_timestamp as calculated_at

                FROM combined_uncertainty
                ORDER BY total_relative_uncertainty ASC
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM nles5_uncertainty_estimates").fetchone()[0]
            avg_uncertainty = self.conn.execute(
                "SELECT AVG(total_uncertainty_pct) FROM nles5_uncertainty_estimates"
            ).fetchone()[0]

            self.log.info(f"Uncertainty calculation complete: {count:,} fields, avg uncertainty: {avg_uncertainty:.1f}%")

            return "nles5_uncertainty_estimates"

        except Exception as e:
            self.log.error(f"Error calculating uncertainty estimates: {e}")
            raise

    @timed(name="Analyzing uncertainty patterns")
    def _analyze_uncertainty_patterns(self) -> str:
        """
        Analyze uncertainty patterns and risk classifications for agricultural fields.

        Returns:
            Table name containing uncertainty pattern analysis and risk classifications
        """
        try:
            self.log.info("Analyzing uncertainty patterns and risk classifications")

            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_uncertainty_patterns AS
                WITH field_risk_assessment AS (
                    SELECT
                        n.field_id,
                        n.nitrogen_washout_kg_ha,
                        n.total_nitrogen_washout_kg,
                        n.area_ha,
                        n.crop_type,
                        n.soil_type,
                        u.total_uncertainty_pct,
                        u.uncertainty_class,
                        u.confidence_level,
                        u.washout_lower_95ci,
                        u.washout_upper_95ci,

                        -- Risk classification based on washout and uncertainty
                        CASE
                            WHEN n.nitrogen_washout_kg_ha >= 100 AND u.uncertainty_class IN ('low', 'moderate') THEN 'high_risk_high_confidence'
                            WHEN n.nitrogen_washout_kg_ha >= 100 AND u.uncertainty_class IN ('high', 'very_high') THEN 'high_risk_low_confidence'
                            WHEN n.nitrogen_washout_kg_ha >= 50 AND n.nitrogen_washout_kg_ha < 100 AND u.uncertainty_class IN ('low', 'moderate') THEN 'moderate_risk_high_confidence'
                            WHEN n.nitrogen_washout_kg_ha >= 50 AND n.nitrogen_washout_kg_ha < 100 AND u.uncertainty_class IN ('high', 'very_high') THEN 'moderate_risk_low_confidence'
                            WHEN n.nitrogen_washout_kg_ha < 50 AND u.uncertainty_class IN ('low', 'moderate') THEN 'low_risk_high_confidence'
                            ELSE 'low_risk_low_confidence'
                        END as risk_confidence_class,

                        -- Management priority scoring (1-10 scale)
                        CASE
                            WHEN n.nitrogen_washout_kg_ha >= 100 AND u.uncertainty_class = 'low' THEN 10         -- Immediate action needed
                            WHEN n.nitrogen_washout_kg_ha >= 100 AND u.uncertainty_class = 'moderate' THEN 9     -- High priority
                            WHEN n.nitrogen_washout_kg_ha >= 100 AND u.uncertainty_class = 'high' THEN 7         -- Verify then act
                            WHEN n.nitrogen_washout_kg_ha >= 50 AND u.uncertainty_class IN ('low', 'moderate') THEN 6  -- Monitor closely
                            WHEN n.nitrogen_washout_kg_ha >= 50 AND u.uncertainty_class = 'high' THEN 4          -- Improve data first
                            WHEN n.nitrogen_washout_kg_ha < 50 AND u.uncertainty_class = 'low' THEN 2            -- Continue current practices
                            ELSE 3  -- Default moderate priority
                        END as management_priority_score
                    FROM nles5_nitrogen_estimates n
                    JOIN nles5_uncertainty_estimates u ON n.field_id = u.field_id
                )
                SELECT
                    field_id,
                    nitrogen_washout_kg_ha,
                    total_uncertainty_pct,
                    uncertainty_class,
                    confidence_level,
                    risk_confidence_class,
                    management_priority_score,
                    washout_lower_95ci,
                    washout_upper_95ci,

                    -- RISK CLASSIFICATION ANALYSIS
                    CASE risk_confidence_class
                        WHEN 'high_risk_high_confidence' THEN
                            'CRITICAL: High nitrogen washout with reliable data quality. Strong evidence for environmental impact.'
                        WHEN 'high_risk_low_confidence' THEN
                            'UNCERTAIN_HIGH: High washout indicated but data quality compromised. Requires verification.'
                        WHEN 'moderate_risk_high_confidence' THEN
                            'MODERATE: Moderate washout risk with reliable data. Monitoring threshold exceeded.'
                        WHEN 'moderate_risk_low_confidence' THEN
                            'UNCERTAIN_MODERATE: Moderate risk but high uncertainty. Data quality limits confidence.'
                        WHEN 'low_risk_high_confidence' THEN
                            'ACCEPTABLE: Low washout risk with high confidence. Within acceptable parameters.'
                        ELSE
                            'UNCERTAIN_LOW: Low risk but uncertain data quality. Inconclusive analysis.'
                    END as risk_classification,

                    -- DATA QUALITY ASSESSMENT
                    CASE
                        WHEN total_uncertainty_pct > 30 THEN 'POOR: Significant data gaps in soil, fertilizer, and climate data'
                        WHEN total_uncertainty_pct > 20 THEN 'LIMITED: Moderate gaps in fertilizer documentation and soil verification'
                        WHEN total_uncertainty_pct > 15 THEN 'ADEQUATE: Minor data quality limitations identified'
                        ELSE 'GOOD: Data quality sufficient for reliable analysis'
                    END as data_quality_assessment,

                    -- NITROGEN EFFICIENCY ANALYSIS
                    CASE
                        WHEN nitrogen_washout_kg_ha >= 100 THEN
                            CASE crop_type
                                WHEN 'maize_potatoes' THEN 'HIGH_LOSS_INTENSIVE: Intensive crop with high nitrogen losses'
                                WHEN 'winter_cereals' THEN 'HIGH_LOSS_CEREAL: Winter cereals showing excessive nitrogen washout'
                                WHEN 'spring_cereals' THEN 'HIGH_LOSS_SPRING: Spring cereals with poor nitrogen retention'
                                ELSE 'HIGH_LOSS_GENERAL: Excessive nitrogen washout detected'
                            END
                        WHEN nitrogen_washout_kg_ha >= 50 THEN
                            'MODERATE_LOSS: Moderate nitrogen losses - timing optimization potential'
                        ELSE
                            'EFFICIENT: Nitrogen retention within acceptable parameters'
                    END as nitrogen_efficiency_pattern,

                    -- ANALYSIS CONFIDENCE
                    CASE
                        WHEN uncertainty_class = 'low' THEN 'HIGH: Analysis based on reliable data and robust model predictions'
                        WHEN uncertainty_class = 'moderate' THEN 'MODERATE: Analysis reasonable with acceptable uncertainty levels'
                        WHEN uncertainty_class = 'high' THEN 'LOW: High uncertainty limits analysis confidence'
                        ELSE 'VERY_LOW: Extreme uncertainty - analysis highly uncertain'
                    END as analysis_confidence,

                    current_timestamp as generated_at

                FROM field_risk_assessment
                ORDER BY management_priority_score DESC, total_uncertainty_pct ASC
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM nles5_uncertainty_patterns").fetchone()[0]
            high_priority = self.conn.execute(
                "SELECT COUNT(*) FROM nles5_uncertainty_patterns WHERE management_priority_score >= 8"
            ).fetchone()[0]

            self.log.info(f"Analyzed {count:,} uncertainty patterns, {high_priority:,} high-priority fields identified")

            return "nles5_uncertainty_patterns"

        except Exception as e:
            self.log.error(f"Error analyzing uncertainty patterns: {e}")
            raise

    @timed(name="Saving NLES5 results to gold layer")
    def _save_results_to_gold(self) -> None:
        """Save NLES5 results to the gold layer using optimized DuckDB export."""
        try:
            self.log.info("Saving NLES5 results to gold layer")

            # Define output tables with optimized paths
            tables_to_save = [
                ("nles5_nitrogen_estimates", "nitrogen_estimates"),
                ("nles5_estimates_analysis", "estimates_analysis"),
                ("nles5_estimates_by_soil_type", "estimates_by_soil_type"),
                ("nles5_estimates_by_crop_type", "estimates_by_crop_type"),
                ("nles5_uncertainty_estimates", "uncertainty_estimates"),
                ("nles5_uncertainty_analysis", "uncertainty_analysis"),
                ("nles5_uncertainty_patterns", "uncertainty_patterns"),
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

            # Validate results
            if estimates_table:
                result_count = self.conn.execute(f"SELECT COUNT(*) FROM {estimates_table}").fetchone()[0]
                if result_count == 0:
                    self.log.error("No NLES5 estimates generated - check input data quality")
                    return
                else:
                    self.log.info(f"Successfully generated {result_count:,} NLES5 nitrogen estimates")

                    # Validate the estimates
                    if not self._validate_nles5_estimates():
                        self.log.error("NLES5 estimates failed validation - check data quality and model parameters")
                        return

            # Analyze estimates distribution
            self._analyze_estimates_distribution()

            # Calculate uncertainty estimates
            uncertainty_table = self._calculate_uncertainty_estimates()

            # Analyze uncertainty patterns
            patterns_table = self._analyze_uncertainty_patterns()

            # Save results to gold layer
            self._save_results_to_gold()

            self.log.info("NLES5 nitrogen estimation completed successfully")

        except Exception as e:
            self.log.error(f"Error in NLES5 processing: {e}")
            raise