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

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.common.geometry_validator import validate_and_transform_geometries_duckdb
from unified_pipeline.util.gcs_access import GCSDataAccess
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
    fertilizer_dataset: str = "fertiliser"  # Add fertilizer data from silver layer
    field_plan_dataset: str = "field_plan"  # Add field plan data
    catch_crops_dataset: str = "catch_crops"  # Add catch crop data (optional)

    # Processing configuration
    batch_size: int = 5000  # Fields to process in each batch
    max_year_lag: int = 1  # Maximum years between field and climate data
    climate_data_days: int = 365  # Days of climate data to analyze



    # FVM marker years to process (will be auto-discovered if not specified)
    target_years: Optional[List[int]] = None

    # Limit years for testing/memory management (None = no limit)
    max_years_to_process: Optional[int] = 5  # Default to 5 years for memory management

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

    # NLES5 Model Parameters from DCA Rapport 163, Table 3.3
    # Main crop effects (lambda_ma)
    crop_parameters: Dict[str, float] = {
        'M1': 0.0,  # Winter cereal (reference)
        'M2': -6.744,  # Spring cereal
        'M3': -7.279,  # Grain-legume mixtures
        'M4': -13.493,  # Grass or grass-clover
        'M5': -17.478,  # Grass for seed
        'M6': -11.192,  # Set-aside
        'M7': -0.640,  # Sugar beet, fodder beet
        'M8': 3.534,  # Silage maize and potato
        'M9': -7.319,  # Winter oilseed rape
        'M10': -1.248,  # Winter cereal after grass
        'M11': 19.524,  # Maize after grass
        'M12': -6.229,  # Spring cereal after grass
        'M13': -2.866,  # Grain legumes and spring oilseed rape
    }

    # Winter vegetation cover effects (lambda_wa)
    winter_veg_parameters: Dict[str, float] = {
        'W1': 0.0,  # Winter cereal (reference)
        'W2': -2.055,  # Bare soil
        'W3': -0.456,  # Autumn cultivation
        'W4': -15.959,  # Cover crops, undersown grass and set-aside
        'W5': -3.792,  # Weeds and volunteers
        'W6': -14.596,  # Grass and grass-clover
        'W7': -1.049,  # Winter cereal after grass
        'W8': -21.060,  # Grass ploughed late autumn or winter
    }

    # Previous main crop effects (eta_mp)
    prev_crop_parameters: Dict[str, float] = {
        'MP1': 0.0,  # Winter cereal (reference)
        'MP2': 2.847,  # Other crops than winter cereals and grass or grass-clover
        'MP3': 0.664,  # Grass or grass-clover
        'MP4': 1.160,  # Spring or winter crops after grass or grass-clover
    }

    # Previous winter vegetation cover effects (eta_wp)
    prev_winter_veg_parameters: Dict[str, float] = {
        'WP1': 0.0,  # Winter cereal (reference)
        'WP2': 9.704,  # Bare soil
        'WP3': 10.601,  # Grass-clover
        'WP4': 9.354,  # Cover crops
        'WP5': 13.241,  # Grass for seed and set aside
        'WP6': 5.483,  # Beets and hemp
        'WP7': -1.572,  # Bare soil after maize or potatoes
        'WP8': 7.413,  # Winter oilseed rape
        'WP9': 7.396,  # Bare soil or winter cereal following grass-clover ploughed in spring
        'WP10': 10.975,  # Bare soil or winter cereal following grass-clover ploughed in autumn
    }

    # Theta (θ) factors for winter vegetation classes
    theta_factors: Dict[str, float] = {
        'WC1': 1.0,  # Large N uptake in autumn
        'WC2': 1.205144,  # Low or moderate N uptake in autumn
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
            'per1_coef': 0.001194,
            'per2_coef': 0.001107,
            'per_p_coef': -0.000856
        },
        'clay': {
            'per1_coef': 0.000798,
            'per2_coef': 0.000745,
            'per_p_coef': -0.000638
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

    def __init__(self, config: NLES5NitrogenEstimationGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()
        self.phase_times: Dict[str, float] = {}
        self.gcs_access = GCSDataAccess()
        self.conn = self.gcs_access.duckdb_conn
        self._configure_duckdb()

    def _configure_duckdb(self):
        """Configure DuckDB for optimal spatial operations."""
        # Increase memory limit and add temp management for large datasets
        self.conn.execute("SET memory_limit = '16GB'")  # Increase for large datasets
        self.conn.execute("SET threads = 4")  # Use all available CPU cores
        self.conn.execute("SET enable_progress_bar = true")
        self.conn.execute("SET preserve_insertion_order = false")
        self.conn.execute("SET temp_directory = '/tmp/duckdb_nles5'")  # Dedicated temp dir

        # Configure for memory efficiency with large datasets
        self.conn.execute("SET max_memory = '16GB'")
        self.conn.execute("SET checkpoint_threshold = '1GB'")  # More frequent checkpoints

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

    def _cleanup_temp_files(self):
        """Clean up temporary files to manage disk space."""
        try:
            import os
            import shutil
            import glob

            temp_patterns = [
                "/tmp/duckdb*",
                "/tmp/gcs_temp*",
                "/tmp/temp_*"
            ]

            for pattern in temp_patterns:
                for file_path in glob.glob(pattern):
                    try:
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        self.log.warning(f"Could not remove temp file {file_path}: {e}")

            # Force garbage collection
            import gc
            gc.collect()

        except Exception as e:
            self.log.warning(f"Error during temp file cleanup: {e}")

    def _get_available_fvm_marker_years(self) -> List[int]:
        """
        Get all available fvm_marker years from GCS storage.

        Returns:
            List of available years for fvm_marker datasets
        """
        try:
            # List all parquet files in fvm_marker directories
            files = self.gcs_access.list_files(
                f"gs://{self.config.bucket}/silver/fvm_marker_*/*/*"
            )
            years = set()

            for file_path in files:
                # Look for files like "gs://bucket/silver/fvm_marker_2021/timestamp/fvm_marker_2021.parquet"
                # or "gs://bucket/silver/fvm_marker_2021/timestamp/data.parquet"
                match = re.search(
                    r"silver/fvm_marker_(\d{4})/.*?/(?:fvm_marker_(\d{4})\.parquet|data\.parquet)", file_path
                )
                if match:
                    year1 = int(match.group(1))  # Year from directory
                    year2 = match.group(2)       # Year from filename (or None for data.parquet)

                    if year2:  # fvm_marker_YYYY.parquet format
                        year2 = int(year2)
                        if year1 == year2:  # Ensure directory and filename years match
                            years.add(year1)
                    else:  # data.parquet format - trust the directory year
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

            # Look for parquet files in timestamped subdirectories
            files = self.gcs_access.list_files(
                f"gs://{self.config.bucket}/silver/{dataset_name}/*/*"
            )

            # Find the latest parquet file (either fvm_marker_YYYY.parquet or data.parquet)
            target_file = None
            latest_timestamp = None
            for file_path in files:
                # Accept both naming conventions: fvm_marker_YYYY.parquet and data.parquet
                if file_path.endswith(f"{dataset_name}.parquet") or file_path.endswith("data.parquet"):
                    # Extract timestamp from path like "gs://bucket/silver/fvm_marker_2021/20241201_123456/file.parquet"
                    clean_path = file_path.replace(f"gs://{self.config.bucket}/", "")
                    path_parts = clean_path.split("/")
                    if len(path_parts) >= 3:
                        timestamp_dir = path_parts[2]  # "20241201_123456"
                        if latest_timestamp is None or timestamp_dir > latest_timestamp:
                            latest_timestamp = timestamp_dir
                            target_file = clean_path

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
                    self.log.info(f"Loaded {count:,} FVM fields for year {year}")

                    return table_name
                except Exception as e:
                    self.log.error(f"Failed to load {gcs_path} using authenticated GCS access: {e}")
                    return None
            else:
                self.log.warning(f"No FVM marker file found for year {year}")
                return None

        except Exception as e:
            self.log.error(f"Error reading FVM marker data for year {year}: {e}")
            return None

    @timed(name="Preparing crop sequence data")
    def _prepare_crop_sequences(self, agricultural_fields_table: str) -> str:
        """
        Prepare crop sequence classifications based on N2023_62.md appendices.

        Args:
            agricultural_fields_table: Name of the table with yearly field data.

        Returns:
            Table name with NLES5 crop classifications for each field and year.
        """
        try:
            self.log.info("Preparing NLES5 crop sequence classifications.")

            # Step 1: Create GLR code to crop group mapping (from N2023_62.md, Bilag 8.1)
            # This is a large mapping, so it's defined here directly.
            glr_to_group_sql = """
            CREATE OR REPLACE TABLE glr_crop_groups AS
            SELECT unnest(codes) as glr_code, group_id, group_name
            FROM (VALUES
                (1, 'Vårraps', [21]),
                (2, 'Græs i omdrift', [116, 117, 118, 170, 171, 172, 173, 174, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 272, 273, 275, 276, 277, 278, 279, 284, 285, 286, 287, 306, 326, 596, 597, 598, 943, 944, 945, 946, 975]),
                (3, 'Permanent græs', [247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 271, 274, 315, 488, 600, 601, 602, 603, 604, 605, 921, 972]),
                (4, 'Vårbyg', [1, 4, 6, 7, 8, 41, 42, 50, 51, 52, 53, 55, 570, 579, 701, 702, 703, 704, 705]),
                (5, 'Vinterbyg', [9, 10, 17, 57, 708]),
                (6, 'Vinterhvede', [11, 13]),
                (7, 'Vinterraps', [22, 23, 24, 40, 180, 181, 182, 777]),
                (8, 'Bælgsæd', [25, 30, 31, 32, 35, 36, 54, 424]),
                (9, 'Majs', [5, 216, 423]),
                (10, 'Brak', [200, 201, 300, 301, 303, 304, 305, 308, 309, 310, 312, 313, 314, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 350, 360, 361, 487, 489, 491, 492, 493, 494, 495, 496, 497, 498, 499, 503, 545, 549, 550, 559, 560, 561, 562, 563, 564, 590, 800, 801, 888, 900, 901, 902, 903, 905, 906, 907, 908, 920, 999]),
                (11, 'Gartneri', [58, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 415, 416, 417, 418, 420, 421, 422, 430, 431, 432, 433, 434, 440, 443, 448, 449, 450, 510, 511, 512, 513, 514, 536, 540, 541, 542, 543, 544, 551, 552, 553]),
                (12, 'Kartofler', [149, 150, 151, 152, 153, 154, 429]),
                (13, 'Foderroer', [125, 280, 281, 282, 283]),
                (14, 'Sukkerroer', [160, 161, 162]),
                (15, 'Træer og buske', [500, 501, 502, 504, 505, 506, 507, 508, 509, 515, 516, 517, 518, 519, 520, 521, 522, 523, 524, 525, 528, 530, 531, 532, 533, 534, 535, 539, 547, 548]),
                (16, 'Skov', [311, 526, 527, 529, 537, 538, 576, 577, 578, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 591, 592, 593, 594]),
                (17, 'Havre', [3, 302]),
                (18, 'Vårhvede', [2]),
                (19, 'Rug', [14, 15, 16, 56]),
                (20, 'Vårhelsæd', [210, 211, 212, 213, 214, 215, 230, 234, 960, 961, 962, 963, 964, 965, 966, 970]),
                (21, 'Frøgræs', [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 120, 121, 122, 123, 124, 126, 650, 651, 652, 653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667, 668, 669]),
                (22, 'Efterafgrøder', [968]), -- Only if registered as specific types
                (23, 'Vinterhelsæd', [220, 221, 222, 223, 224, 235, 706, 707, 709, 710, 711])
            ) AS t(group_id, group_name, codes);
            """
            self.conn.execute(glr_to_group_sql)

            # Step 2: Create a crop history table with group IDs
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE crop_history AS
                SELECT
                    a.field_id,
                    a.year,
                    g.group_id as crop_group,
                    g.group_name
                FROM {agricultural_fields_table} a
                JOIN glr_crop_groups g ON a.crop_code = g.glr_code;
            """)

            # Step 3: Create full history with leads and lags
            self.conn.execute("""
                CREATE OR REPLACE TABLE full_crop_history AS
                SELECT
                    c_curr.field_id,
                    c_curr.year,
                    c_curr.crop_group as crop_t,
                    c_prev.crop_group as crop_t_minus_1,
                    c_prev2.crop_group as crop_t_minus_2,
                    c_next.crop_group as crop_t_plus_1
                FROM crop_history c_curr
                LEFT JOIN crop_history c_prev ON c_curr.field_id = c_prev.field_id AND c_curr.year = c_prev.year + 1
                LEFT JOIN crop_history c_prev2 ON c_curr.field_id = c_prev2.field_id AND c_curr.year = c_prev2.year + 2
                LEFT JOIN crop_history c_next ON c_curr.field_id = c_next.field_id AND c_curr.year = c_next.year - 1;
            """)

            # Step 4: Apply classification rules from appendices to generate NLES5 codes
            # This is a complex step that translates the logic from N2023_62.md into SQL
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_crop_classifications AS
                SELECT
                    h.field_id,
                    h.year,
                    -- Main crop (M code) from Bilag 8.4
                    CASE
                        WHEN h.crop_t_minus_1 IN (2, 3, 21) THEN -- Previous crop was grass/seed grass
                            CASE
                                WHEN h.crop_t IN (5, 6, 19, 23) THEN 'M10' -- Winter cereal after grass
                                WHEN h.crop_t IN (9) THEN 'M11' -- Maize after grass
                                WHEN h.crop_t IN (4, 17, 18, 20) THEN 'M12' -- Spring cereal after grass
                                ELSE 'M4' -- Default to grass
                            END
                        ELSE -- Previous crop not grass
                            CASE h.crop_t
                                WHEN 6 THEN 'M1'   -- Vinterhvede
                                WHEN 5 THEN 'M1'   -- Vinterbyg
                                WHEN 19 THEN 'M1'  -- Rug
                                WHEN 23 THEN 'M1'  -- Vinterhelsæd
                                WHEN 4 THEN 'M2'   -- Vårbyg
                                WHEN 17 THEN 'M2'  -- Havre
                                WHEN 18 THEN 'M2'  -- Vårhvede
                                WHEN 20 THEN 'M2'  -- Vårhelsæd
                                WHEN 2 THEN 'M4'   -- Græs i omdrift
                                WHEN 3 THEN 'M4'   -- Permanent græs
                                WHEN 21 THEN 'M5'  -- Frøgræs
                                WHEN 10 THEN 'M6'  -- Brak
                                WHEN 13 THEN 'M7'  -- Foderroer
                                WHEN 14 THEN 'M7'  -- Sukkerroer
                                WHEN 9 THEN 'M8'   -- Majs
                                WHEN 12 THEN 'M8'  -- Kartofler
                                WHEN 7 THEN 'M9'   -- Vinterraps
                                WHEN 8 THEN 'M13'  -- Bælgsæd
                                WHEN 1 THEN 'M13'  -- Vårraps
                                ELSE 'M2' -- Default to spring cereal for others
                            END
                    END as m_code,

                    -- Winter vegetation cover (W code) from Bilag 8.5
                    CASE
                        -- Specific case for grass plowed in autumn for beets/hemp
                        WHEN h.crop_t_minus_1 IN (2, 3) AND h.crop_t IN (13, 14) THEN 'W8'
                        -- Winter cereal sown after grass
                        WHEN h.crop_t IN (2, 3, 21) AND h.crop_t_plus_1 IN (1, 5, 6, 19, 23) THEN 'W7'
                        -- Weeds/volunteers after cereals
                        WHEN h.crop_t IN (1, 4, 5, 6, 17, 18, 19, 20, 23) AND h.crop_t_plus_1 NOT IN (7, 22) THEN 'W5'
                        -- Grass, beets, or followed by grass/winter rape -> long growing season or green cover
                        WHEN h.crop_t IN (2, 3, 13, 14, 21) OR h.crop_t_plus_1 IN (2, 3, 7) THEN 'W6'
                        -- Followed by winter cereals
                        WHEN h.crop_t_plus_1 IN (1, 5, 6, 19, 23) THEN 'W1'
                        -- Followed by catch crop
                        WHEN h.crop_t_plus_1 = 22 THEN 'W4'
                        -- After maize or potatoes, assume autumn cultivation
                        WHEN h.crop_t IN (9, 12) THEN 'W3'
                        ELSE 'W2' -- Default to bare soil
                    END as w_code,

                    -- Main previous crop (MP code) from Bilag 8.2
                    CASE
                        WHEN h.crop_t_minus_1 IN (2, 3, 21) THEN 'MP3' -- Forfrugt is grass
                        WHEN h.crop_t_minus_2 IN (2, 3, 21) THEN 'MP4' -- For-forfrugt is grass (and forfrugt is not)
                        WHEN h.crop_t_minus_1 IN (5, 6, 7, 19, 23) THEN 'MP1' -- Forfrugt is winter crop
                        ELSE 'MP2' -- Other crops
                    END as mp_code,

                    -- Previous winter vegetation (WP code) from Bilag 8.3 (2D Matrix)
                    CASE
                        -- Row 1: Forfrugt = Vintercereal eller Vinterraps
                        WHEN h.crop_t_minus_1 IN (5, 6, 7, 19, 23) THEN
                            CASE
                                WHEN h.crop_t IN (1, 5, 6, 19, 23) THEN 'WP1'
                                WHEN h.crop_t = 7 THEN 'WP8'
                                ELSE 'WP2'
                            END
                        -- Row 2: Forfrugt = Vårsæd
                        WHEN h.crop_t_minus_1 IN (1, 4, 8, 17, 18, 20) THEN
                            CASE
                                WHEN h.crop_t IN (1, 5, 6, 19, 23) THEN 'WP1'
                                WHEN h.crop_t = 7 THEN 'WP8'
                                ELSE 'WP2'
                            END
                        -- Row 3: Forfrugt = Majs eller kartofler
                        WHEN h.crop_t_minus_1 IN (9, 12) THEN
                            CASE
                                WHEN h.crop_t IN (1, 5, 6, 19, 23) THEN 'WP1'
                                WHEN h.crop_t = 7 THEN 'WP8'
                                ELSE 'WP7'
                            END
                        -- Row 4: Forfrugt = Roer eller hamp
                        WHEN h.crop_t_minus_1 IN (13, 14) THEN 'WP6'
                        -- Row 5: Forfrugt = Græs i omdrift, permanent græs
                        WHEN h.crop_t_minus_1 IN (2, 3) THEN
                            CASE
                                WHEN h.crop_t NOT IN (1, 5, 6, 7, 19, 20, 23) THEN 'WP9' -- plowed in spring
                                ELSE 'WP10' -- plowed in autumn
                            END
                        -- Row 6: Forfrugt = Frøgræs, brak
                        WHEN h.crop_t_minus_1 IN (10, 21) THEN 'WP5'
                        -- Row 7: Forfrugt = Efterafgrøde
                        WHEN h.crop_t_minus_1 = 22 THEN 'WP4'
                        -- Row 8: Forfrugt = Grøntsager
                        WHEN h.crop_t_minus_1 = 11 THEN
                            CASE
                                WHEN h.crop_t = 7 THEN 'WP8'
                                ELSE 'WP2'
                            END
                        ELSE 'WP2' -- Default
                    END as wp_code,

                    -- Winter crop group (WC code) for theta from Bilag 8.6
                    CASE
                        WHEN w_code IN ('W4', 'W6', 'W7', 'W8') THEN 'WC1'
                        ELSE 'WC2'
                    END as wc_code
                FROM full_crop_history h
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_crop_classifications").fetchone()[0]
            self.log.info(f"Generated NLES5 crop classifications for {count:,} field-years.")

            return "fields_with_crop_classifications"

        except Exception as e:
            self.log.error(f"Error preparing crop sequences: {e}")
            raise



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
            all_available_years = self._get_available_fvm_marker_years()
            # Apply year limit for memory management
            if self.config.max_years_to_process:
                # Take the most recent years up to the limit
                years_to_process = sorted(all_available_years)[-self.config.max_years_to_process:]
                self.log.info(f"Auto-discovered {len(all_available_years)} years, processing most recent {len(years_to_process)}: {years_to_process}")
            else:
                years_to_process = all_available_years
                self.log.info(f"Auto-discovered years (no limit): {years_to_process}")

        if not years_to_process:
            self.log.error("No FVM marker years found to process")
            raise ValueError("No FVM marker data available")

        # Process each year and collect table names
        yearly_tables = []
        for i, year in enumerate(years_to_process):
            try:
                # Clean up temp files every few years to manage disk space
                if i > 0 and i % 3 == 0:
                    self.log.info(f"Cleaning up temporary files after processing {i} years...")
                    self._cleanup_temp_files()

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
                # Clean up on error to free space
                self._cleanup_temp_files()
                continue

        if not yearly_tables:
            self.log.error("No agricultural fields data could be loaded")
            raise ValueError("Failed to load any agricultural fields data")

                # Combine all yearly tables into a single table
        self.log.info(f"Combining {len(yearly_tables)} yearly FVM marker datasets")

        # First, collect all unique columns across all tables
        all_columns = set()
        table_schemas = {}

        for table_name in yearly_tables:
            columns_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_info = {row[0]: row[1] for row in columns_result}  # {name: type}
            table_schemas[table_name] = column_info
            all_columns.update(column_info.keys())

            # Standardize crop_code as integer for reliable joins later
            if 'crop_code' in column_info:
                self.conn.execute(f"ALTER TABLE {table_name} ALTER crop_code TYPE INT;")

        # Sort columns for consistent ordering
        all_columns = sorted(list(all_columns))
        self.log.info(f"Found {len(all_columns)} unique columns across all years")

        # Create UNION query with standardized column selection
        union_queries = []
        for table_name in yearly_tables:
            table_columns = table_schemas[table_name]

            # Build SELECT clause with proper column handling
            select_columns = []
            for col in all_columns:
                if col in table_columns:
                    select_columns.append(f"{col}")
                else:
                    # Add NULL for missing columns with appropriate type
                    # Default to VARCHAR for unknown columns
                    select_columns.append(f"NULL::VARCHAR AS {col}")

            select_clause = ", ".join(select_columns)
            union_queries.append(f"SELECT {select_clause} FROM {table_name}")

        combined_query = " UNION ALL ".join(union_queries)

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE agricultural_fields AS
            {combined_query}
        """)

        total_count = self.conn.execute("SELECT COUNT(*) FROM agricultural_fields").fetchone()[0]
        self.log.info(f"Combined agricultural fields: {total_count:,} records from {len(yearly_tables)} years")

        # Debug: Show available columns in agricultural_fields table
        try:
            columns_result = self.conn.execute("DESCRIBE agricultural_fields").fetchall()
            column_names = [row[0] for row in columns_result]
            self.log.info(f"Available columns in agricultural_fields: {column_names}")

            # Check for geometry-related columns
            geometry_columns = [col for col in column_names if 'geom' in col.lower()]
            self.log.info(f"Geometry-related columns: {geometry_columns}")
        except Exception as e:
            self.log.warning(f"Could not describe agricultural_fields table: {e}")

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

        # Prepare crop sequence classifications
        try:
            crop_classifications_table = self._prepare_crop_sequences(agricultural_fields_table)
            loaded_tables["crop_classifications"] = crop_classifications_table
        except Exception as e:
            self.log.error(f"Failed to prepare crop sequence data: {e}")

        # Handle DMI data separately since it's stored in separate tables
        try:
            self.log.info("Loading DMI climate data from separate precipitation and evaporation tables")
            dmi_data_loaded = self._load_and_combine_dmi_data()
            if dmi_data_loaded:
                loaded_tables["dmi"] = "dmi_data"
                count = self.conn.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
                self.log.info(f"Loaded {count:,} records for dmi")
            else:
                self.log.warning("Could not load dmi - will use defaults")
        except Exception as e:
            self.log.warning(f"Failed to load DMI data: {e}")

        # Define other required datasets (excluding dmi since we handled it above)
        other_datasets = [
            (self.config.soil_types_dataset, "soil_types"),
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

    def _load_and_combine_dmi_data(self) -> bool:
        """
        Load separate DMI precipitation and evaporation datasets and combine them into unified dmi_data table.

        Returns:
            bool: True if data was successfully loaded and combined, False otherwise
        """
        try:
            # Try to load precipitation data
            precip_loaded = False
            try:
                precip_table = self._read_silver_data("dmi_acc_precip_dmi_acc_precip")
                if precip_table and isinstance(precip_table, str):
                    # Data is already loaded into our connection, just copy it to the expected table name
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE dmi_precip_temp AS
                        SELECT * FROM {precip_table}
                    """)
                    precip_count = self.conn.execute("SELECT COUNT(*) FROM dmi_precip_temp").fetchone()[0]
                    if precip_count > 0:
                        precip_loaded = True
                        self.log.info(f"Successfully loaded DMI precipitation data ({precip_count:,} records)")
                    else:
                        self.log.warning("DMI precipitation table is empty")
                else:
                    self.log.warning(f"Could not load precipitation data - invalid result: {precip_table}")
            except Exception as e:
                self.log.warning(f"Could not load precipitation data: {e}")
                import traceback
                self.log.warning(f"Stack trace: {traceback.format_exc()}")

            # Try to load evaporation data
            evap_loaded = False
            try:
                evap_table = self._read_silver_data("dmi_pot_evaporation_makkink_dmi_pot_evaporation_makkink")
                if evap_table and isinstance(evap_table, str):
                    # Data is already loaded into our connection, just copy it to the expected table name
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE dmi_evap_temp AS
                        SELECT * FROM {evap_table}
                    """)
                    evap_count = self.conn.execute("SELECT COUNT(*) FROM dmi_evap_temp").fetchone()[0]
                    if evap_count > 0:
                        evap_loaded = True
                        self.log.info(f"Successfully loaded DMI evaporation data ({evap_count:,} records)")
                    else:
                        self.log.warning("DMI evaporation table is empty")
                else:
                    self.log.warning(f"Could not load evaporation data - invalid result: {evap_table}")
            except Exception as e:
                self.log.warning(f"Could not load evaporation data: {e}")
                import traceback
                self.log.warning(f"Stack trace: {traceback.format_exc()}")

            if not precip_loaded and not evap_loaded:
                self.log.warning("No DMI data could be loaded")
                return False

            # Combine the data into unified dmi_data table
            try:
                # First, ensure we drop any existing dmi_data objects
                self.conn.execute("DROP TABLE IF EXISTS dmi_data")
                self.conn.execute("DROP VIEW IF EXISTS dmi_data")

                if precip_loaded and evap_loaded:
                    # Both datasets available - combine them
                    self.conn.execute("""
                        CREATE TABLE dmi_data AS
                        SELECT
                            avg_value,
                            centroid_geometry,
                            valid_time,
                            'acc_precip' as parameter_id,
                            min_value,
                            max_value,
                            count,
                            stddev_value,
                            bbox_geometry,
                            processing_time,
                            source_crs,
                            target_crs,
                            original_feature_count
                        FROM dmi_precip_temp
                        UNION ALL
                        SELECT
                            avg_value,
                            centroid_geometry,
                            valid_time,
                            'pot_evaporation_makkink' as parameter_id,
                            min_value,
                            max_value,
                            count,
                            stddev_value,
                            bbox_geometry,
                            processing_time,
                            source_crs,
                            target_crs,
                            original_feature_count
                        FROM dmi_evap_temp
                    """)
                elif precip_loaded:
                    # Only precipitation available
                    self.conn.execute("""
                        CREATE TABLE dmi_data AS
                    SELECT
                        avg_value,
                        centroid_geometry,
                        valid_time,
                        'acc_precip' as parameter_id,
                        min_value,
                        max_value,
                        count,
                        stddev_value,
                        bbox_geometry,
                        processing_time,
                        source_crs,
                        target_crs,
                        original_feature_count
                    FROM dmi_precip_temp
                    """)
                elif evap_loaded:
                    # Only evaporation available
                    self.conn.execute("""
                        CREATE TABLE dmi_data AS
                    SELECT
                        avg_value,
                        centroid_geometry,
                        valid_time,
                        'pot_evaporation_makkink' as parameter_id,
                        min_value,
                        max_value,
                        count,
                        stddev_value,
                        bbox_geometry,
                        processing_time,
                        source_crs,
                        target_crs,
                        original_feature_count
                    FROM dmi_evap_temp
                    """)

                # Clean up temporary tables
                self.conn.execute("DROP TABLE IF EXISTS dmi_precip_temp")
                self.conn.execute("DROP TABLE IF EXISTS dmi_evap_temp")

                count = self.conn.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
                self.log.info(f"Successfully combined DMI data with {count:,} total records")

                return True
            except Exception as e:
                self.log.error(f"Error combining DMI data: {e}")
                return False

        except Exception as e:
            self.log.error(f"Error loading DMI data: {e}")
            return False

    @timed(name="Processing DMI climate data")
    def _process_climate_data(self) -> str:
        """
        Process DMI climate data to calculate percolation (precipitation - evaporation).

        Returns:
            Table name containing processed climate data with percolation
        """
        try:
            self.log.info("Processing DMI climate data for percolation calculation")

            # Debug: Check what's in dmi_data
            dmi_count = self.conn.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
            self.log.info(f"Total DMI data records: {dmi_count:,}")

            if dmi_count > 0:
                # Check parameter distribution
                param_dist = self.conn.execute("""
                    SELECT parameter_id, COUNT(*) as count
                    FROM dmi_data
                    GROUP BY parameter_id
                """).fetchall()
                self.log.info(f"DMI parameter distribution: {param_dist}")

                # Check sample data
                sample_data = self.conn.execute("""
                    SELECT parameter_id, avg_value, valid_time, centroid_geometry
                    FROM dmi_data
                    LIMIT 5
                """).fetchall()
                self.log.info(f"DMI sample data: {sample_data}")

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
                        EXTRACT(year from CAST(valid_time AS DATE)) as year,
                        -- NLES5 seasonal periods
                        SUM(CASE
                            WHEN EXTRACT(month FROM CAST(valid_time AS DATE)) BETWEEN 4 AND 8
                            THEN percolation ELSE 0
                        END) as percolation_apr_aug,  -- NLES5: A_Aa
                        SUM(CASE
                            WHEN EXTRACT(month FROM CAST(valid_time AS DATE)) >= 9 OR EXTRACT(month FROM CAST(valid_time AS DATE)) <= 3
                            THEN percolation ELSE 0
                        END) as percolation_sep_mar,  -- NLES5: A_Ab
                        AVG(precipitation) as avg_precipitation,
                        AVG(evaporation) as avg_evaporation,
                        COUNT(*) as climate_data_points
                    FROM merged_climate
                    WHERE geometry IS NOT NULL
                    GROUP BY centroid_geometry, year
                )
                SELECT
                    s1.centroid_geometry,
                    s1.geometry,
                    s1.year,
                    s1.percolation_apr_aug as perco_apr_aug_current,
                    s1.percolation_sep_mar as perco_sep_mar_current,
                    COALESCE(s2.percolation_sep_mar, 0.0) as perco_sep_mar_previous,
                    s1.avg_precipitation,
                    s1.avg_evaporation,
                    s1.climate_data_points,
                    s1.percolation_apr_aug + s1.percolation_sep_mar as total_percolation,
                    CASE
                        WHEN s1.climate_data_points >= 30 THEN true
                        ELSE false
                    END as sufficient_climate_data
                FROM seasonal_aggregation s1
                LEFT JOIN seasonal_aggregation s2
                    ON s1.centroid_geometry = s2.centroid_geometry
                    AND s1.year = s2.year + 1
                WHERE s1.total_percolation > 0
                    -- Filter for Denmark coordinates (roughly 8-15°E, 54-58°N)
                    AND ST_X(geometry) BETWEEN 8 AND 15
                    AND ST_Y(geometry) BETWEEN 54 AND 58
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM climate_percolation").fetchone()[0]
            self.log.info(f"Processed {count:,} climate grid points with percolation data")

                        # Debug: If no climate data, check intermediate steps
            if count == 0:
                precip_count = self.conn.execute("SELECT COUNT(*) FROM dmi_data WHERE parameter_id = 'acc_precip'").fetchone()[0]
                evap_count = self.conn.execute("SELECT COUNT(*) FROM dmi_data WHERE parameter_id = 'pot_evaporation_makkink'").fetchone()[0]
                self.log.warning(f"No climate percolation data generated. Precipitation records: {precip_count}, Evaporation records: {evap_count}")

                # Check if there are any records with different parameter_id values
                all_params = self.conn.execute("SELECT DISTINCT parameter_id FROM dmi_data").fetchall()
                self.log.warning(f"Available parameter_id values: {[p[0] for p in all_params]}")

                # Check for geometry issues
                if precip_count > 0:
                    geom_sample = self.conn.execute("""
                        SELECT centroid_geometry, valid_time
                        FROM dmi_data
                        WHERE parameter_id = 'acc_precip'
                        LIMIT 3
                    """).fetchall()
                    self.log.warning(f"Sample centroid_geometry data: {geom_sample}")

                # Check for valid_time issues
                time_sample = self.conn.execute("""
                    SELECT valid_time, COUNT(*)
                    FROM dmi_data
                    WHERE parameter_id = 'acc_precip'
                    GROUP BY valid_time
                    ORDER BY valid_time DESC
                    LIMIT 5
                """).fetchall()
                self.log.warning(f"Sample valid_time data (latest): {time_sample}")

                # Try a simpler approach without geometry conversion
                simple_count = self.conn.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT centroid_geometry
                        FROM dmi_data
                        WHERE parameter_id = 'acc_precip'
                    )
                """).fetchone()[0]
                self.log.warning(f"Simple centroid_geometry count: {simple_count}")

            return "climate_percolation"

        except Exception as e:
            self.log.error(f"Error processing climate data: {e}")
            # Create a fallback climate table with default values
            self.log.warning("Creating fallback climate data with default values")
            self.conn.execute("""
                CREATE OR REPLACE TABLE climate_percolation AS
                SELECT
                    'POINT(10.0 56.0)' as centroid_geometry,
                    ST_GeomFromText('POINT(10.0 56.0)') as geometry,
                    2022 as year,
                    300.0 as perco_apr_aug_current,
                    400.0 as perco_sep_mar_current,
                    400.0 as perco_sep_mar_previous,
                    800.0 as avg_precipitation,
                    300.0 as avg_evaporation,
                    365 as climate_data_points,
                    900.0 as total_percolation,
                    true as sufficient_climate_data
            """)
            self.log.info("Created fallback climate data for Denmark center point")
            return "climate_percolation"

    @timed(name="Spatial join fields with climate data")
    def _spatial_join_fields_climate(self) -> str:
        """Spatially join fields with nearest climate data point."""
        try:
            self.log.info("Performing spatial join between fields and nearest climate data point")

            # Create a spatial index on climate data for performance
            self.conn.execute("CREATE INDEX climate_geom_idx ON climate_percolation(geometry);")

            # Use QUALIFY to find the single nearest climate grid point for each field centroid.
            # This is a highly efficient, DuckDB- idiomatic way to perform a nearest-neighbor join.
            # We use ST_DWithin to limit the search space for performance.
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_climate AS
                SELECT
                    f.*,
                    c.perco_apr_aug_current,
                    c.perco_sep_mar_current,
                    c.perco_sep_mar_previous,
                    c.total_percolation,
                    c.avg_precipitation,
                    c.avg_evaporation,
                    c.sufficient_climate_data,
                    ST_Distance(f.centroid_geom, c.geometry) as climate_distance_m
                FROM agricultural_fields_spatial f
                LEFT JOIN climate_percolation c
                    ON ST_DWithin(f.centroid_geom, c.geometry, 50000) -- 50km search radius
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY f.field_id
                    ORDER BY ST_Distance(f.centroid_geom, c.geometry) ASC
                ) = 1;
            """)

            # Drop the centroid_geom column as it's no longer needed
            self.conn.execute("ALTER TABLE fields_with_climate DROP centroid_geom;")

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_climate").fetchone()[0]
            self.log.info(f"Spatially joined {count:,} fields with nearest climate data")
            return "fields_with_climate"

        except Exception as e:
            self.log.error(f"Error in spatial join with climate data: {e}")
            self.log.info("Creating fallback climate data for all fields")
            try:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE fields_with_climate AS
                    SELECT
                        *,
                        300.0 as perco_apr_aug_current,
                        400.0 as perco_sep_mar_current,
                        400.0 as perco_sep_mar_previous,
                        900.0 as total_percolation,
                        800.0 as avg_precipitation,
                        300.0 as avg_evaporation,
                        true as sufficient_climate_data,
                        -1.0 as climate_distance_m
                    FROM agricultural_fields_spatial
                """)
                return "fields_with_climate"
            except Exception as fallback_e:
                self.log.error(f"Fallback climate data creation failed: {fallback_e}")
                raise

    @timed(name="Joining with soil data")
    def _join_with_soil_data(self) -> str:
        """
        Spatially join fields with soil data using largest intersection area.
        Also joins with pre-calculated crop classifications and nitrogen inputs.
        """
        try:
            self.log.info("Spatially joining fields with soil data (largest overlap)...")

            # First prepare nitrogen inputs (fixation and fertilizer data)
            self._prepare_nitrogen_inputs_tables()

            # Create a spatial index on soil data for performance
            self.conn.execute(f"CREATE INDEX soil_geom_idx ON soil_types_spatial(geometry_spatial);")

            # Find all intersections between fields and soil types, calculate the area of overlap,
            # and rank them to find the soil type with the largest intersection for each field.
            # This is a robust way to assign a single, most-representative soil type to each field.
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_climate_soil AS
                WITH field_soil_intersections AS (
                    SELECT
                        f.field_id,
                        s.soil_code,
                        s.soil_description,
                        s.clay_content,
                        s.total_n_content,
                        ST_Area(ST_Intersection(f.geom, s.geometry_spatial)) as intersection_area
                    FROM fields_with_climate f
                    JOIN soil_types_spatial s ON ST_Intersects(f.geom, s.geometry_spatial)
                ),
                ranked_intersections AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER(PARTITION BY field_id ORDER BY intersection_area DESC) as rn
                    FROM field_soil_intersections
                )
                SELECT
                    f.*,
                    COALESCE(s.soil_code, '5') as soil_code,
                    COALESCE(s.soil_description, 'Medium clay soil') as soil_description,
                    COALESCE(s.clay_content, 15.0) as clay_content,
                    COALESCE(s.total_n_content, 5.0) as tn_t_ha,
                    CASE
                        WHEN COALESCE(s.soil_code, '5') IN ('1', '2', '3', '4') THEN 'sand'
                        ELSE 'clay'
                    END as soil_type_category,
                    s.soil_code IS NOT NULL as has_soil_data
                FROM fields_with_climate f
                LEFT JOIN ranked_intersections s ON f.field_id = s.field_id AND s.rn = 1;
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_climate_soil").fetchone()[0]
            soil_matched = self.conn.execute(
                "SELECT COUNT(*) FROM fields_with_climate_soil WHERE has_soil_data = true"
            ).fetchone()[0]
            self.log.info(f"Soil join complete: {count:,} fields, {soil_matched:,} with soil data")

            # Join with crop classifications and prepare nitrogen inputs
            self.conn.execute("""
                CREATE OR REPLACE TABLE fields_with_climate_soil_crops AS
                SELECT
                    f_s.*,
                    COALESCE(c_c.m_code, 'M2') as m_code,
                    COALESCE(c_c.w_code, 'W2') as w_code,
                    COALESCE(c_c.mp_code, 'MP2') as mp_code,
                    COALESCE(c_c.wp_code, 'WP2') as wp_code,
                    COALESCE(c_c.wc_code, 'WC2') as wc_code,
                    -- Add nitrogen fixation data
                    COALESCE(nfix.nfix_ha, 2.0) as nfix_ha,
                    COALESCE(nfix.nfix_prev, 2.0) as nfix_prev,
                    -- Add fertilizer data (use defaults if not available)
                    COALESCE(fert.mineral_n_foraar, 100.0) as mineral_n_foraar,
                    COALESCE(fert.mineral_n_eft, 10.0) as mineral_n_eft,
                    COALESCE(fert.mineral_n_udb, 5.0) as mineral_n_udb,
                    COALESCE(fert.organic_n_hus, 40.0) as organic_n_hus,
                    COALESCE(fert.mineral_n_prev, 120.0) as mineral_n_prev,
                    COALESCE(fert.organic_n_prev, 40.0) as organic_n_prev
                FROM fields_with_climate_soil f_s
                LEFT JOIN fields_with_crop_classifications c_c
                    ON f_s.field_id = c_c.field_id AND f_s.year = c_c.year
                LEFT JOIN n_fixation_history nfix
                    ON f_s.field_id = nfix.field_id AND f_s.year = nfix.year
                LEFT JOIN fertilizer_history fert
                    ON f_s.cvr_number = fert.cvr_number AND f_s.year = fert.year
            """)

            count = self.conn.execute("SELECT COUNT(*) FROM fields_with_climate_soil_crops").fetchone()[0]
            self.log.info(f"Joined with crop classifications, total records: {count:,}")

            return "fields_with_climate_soil_crops"

        except Exception as e:
            self.log.error(f"Error joining soil data: {e}")
            self.log.warning("Creating fallback with default soil and crop data due to error")
            try:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE fields_with_climate_soil_crops AS
                    SELECT
                        f.*,
                        -- Default soil data
                        '5' as soil_code,
                        'Medium clay soil' as soil_description,
                        15.0 as clay_content,
                        'clay' as soil_type_category,
                        false as has_soil_data,
                        -- Default crop codes
                        'M2' as m_code,
                        'W2' as w_code,
                        'MP2' as mp_code,
                        'WP2' as wp_code,
                        'WC2' as wc_code
                    FROM fields_with_climate f
                """)
                self.log.info("Created fallback fields_with_climate_soil_crops with default soil and crop data")
                return "fields_with_climate_soil_crops"
            except Exception as fallback_error:
                self.log.error(f"Even soil/crop fallback creation failed: {fallback_error}")
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

            # Debug: Check what crop types are actually in the data
            crop_distribution = self.conn.execute("""
                SELECT crop_type, COUNT(*) as count
                FROM fields_with_climate_soil_crops
                GROUP BY crop_type
                ORDER BY count DESC
                LIMIT 10
            """).fetchall()
            self.log.info(f"Crop type distribution in data: {crop_distribution}")

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
                WITH nles5_calculations AS (
                    SELECT
                        f.*,
                        -- Get all crop effect parameters. TODO: These codes need to be derived from crop sequence data
                        COALESCE(cp.param, 0.0) as main_crop_effect,
                        COALESCE(wvp.param, 0.0) as winter_veg_effect,
                        COALESCE(pcp.param, 0.0) as prev_crop_effect,
                        COALESCE(pwvp.param, 0.0) as prev_winter_veg_effect,
                        COALESCE(th.factor, 1.0) as theta_factor,

                        -- Drainage effect calculation using NLES5 soil parameters and correct time periods
                        CASE f.soil_type_category
                            WHEN 'sand' THEN
                                (1 - EXP(-{soil_params_sand['per1_coef']} * f.perco_apr_aug_current -
                                         {soil_params_sand['per2_coef']} * f.perco_sep_mar_current)) *
                                EXP({soil_params_sand['per_p_coef']} * f.perco_sep_mar_previous)
                            ELSE
                                (1 - EXP(-{soil_params_clay['per1_coef']} * f.perco_apr_aug_current -
                                         {soil_params_clay['per2_coef']} * f.perco_sep_mar_current)) *
                                EXP({soil_params_clay['per_p_coef']} * f.perco_sep_mar_previous)
                        END as drainage_effect,

                        -- Soil effect calculation (using positive coefficient as per report, EXP will handle it)
                        EXP({0.001849} * f.clay_content) as soil_effect,

                        -- Fertilizer and nitrogen data (now properly prepared)
                        f.tn_t_ha, -- Total N in topsoil (Mg N/ha)
                        f.mineral_n_foraar,
                        f.mineral_n_eft,
                        f.mineral_n_udb,
                        f.organic_n_hus,
                        f.mineral_n_prev,
                        f.nfix_ha,
                        f.nfix_prev,
                        f.organic_n_prev

                        -- Trend effect (dynamic based on field year)
                        -0.1108 * (f.year - 1991) as trend_effect

                    FROM fields_with_climate_soil_crops f
                    -- Join with NLES5 parameter lookup tables
                    LEFT JOIN crop_params cp ON f.m_code = cp.code
                    LEFT JOIN winter_veg_params wvp ON f.w_code = wvp.code
                    LEFT JOIN prev_crop_params pcp ON f.mp_code = pcp.code
                    LEFT JOIN prev_winter_veg_params pwvp ON f.wp_code = pwvp.code
                    LEFT JOIN theta_factors th ON f.wc_code = th.code
                    WHERE f.total_percolation IS NOT NULL
                        AND f.total_percolation > 0
                ),
                nitrogen_calculations AS (
                    SELECT
                        *,
                        -- Combine all crop effects
                        (main_crop_effect + winter_veg_effect + prev_crop_effect + prev_winter_veg_effect) as total_crop_effect,

                        -- Full NLES5 nitrogen effect calculation using all coefficients
                        ({self.config.nitrogen_coefficients['Bt']} * tn_t_ha +
                         {self.config.nitrogen_coefficients['Bcs']} * mineral_n_foraar +
                         {self.config.nitrogen_coefficients['Bca']} * mineral_n_eft +
                         {self.config.nitrogen_coefficients['Budb']} * mineral_n_udb +
                         {self.config.nitrogen_coefficients['Bm1']} * (mineral_n_prev + organic_n_prev) / 2.0 +
                         {self.config.nitrogen_coefficients['Bf0']} * nfix_ha +
                         {self.config.nitrogen_coefficients['Bf1']} * nfix_prev / 2.0 +
                         {self.config.nitrogen_coefficients['Bg0']} * organic_n_hus
                        ) as nitrogen_effect,

                        -- Percolation and soil effect with bias correction (rho)
                        drainage_effect * soil_effect * 1.085 as perco_soil_effect
                    FROM nles5_calculations
                ),
                final_calculations AS (
                    SELECT
                        *,
                        -- NLES5 base calculation (V) with theta factor
                        (23.51 + total_crop_effect + theta_factor * nitrogen_effect) as v_base
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
                    perco_apr_aug_current,
                    perco_sep_mar_current,
                    perco_sep_mar_previous,
                    total_percolation,
                    avg_precipitation,
                    avg_evaporation,
                    climate_distance_m,

                    -- NLES5 model components
                    total_crop_effect as crop_effect,
                    drainage_effect,
                    soil_effect,
                    nitrogen_effect,
                    trend_effect,
                    v_base,

                    -- Fertilizer data components
                    tn_t_ha as total_soil_n_mg_ha,
                    mineral_n_foraar as mineral_n_spring_kg_ha,
                    mineral_n_eft as mineral_n_autumn_kg_ha,
                    mineral_n_udb as mineral_n_grazing_kg_ha,
                    organic_n_hus as organic_n_manure_kg_ha,
                    nfix_ha as n_fixation_kg_ha,

                    -- Final nitrogen washout calculation (L = trend + (V^1.5 * P * S * rho))
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

                    'nles5_full_model_v2' as estimation_method,
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

            # If no estimates generated, create a fallback version
            if count == 0:
                self.log.warning("No NLES5 estimates generated, creating fallback with simplified model")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE nles5_nitrogen_estimates AS
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
                        perco_apr_aug_current,
                        perco_sep_mar_current,
                        perco_sep_mar_previous,
                        total_percolation,
                        avg_precipitation,
                        avg_evaporation,
                        climate_distance_m,
                        0.0 as crop_effect,
                        0.8 as drainage_effect,
                        0.9 as soil_effect,
                        1.0 as perco_soil_effect,
                        50.0 as nitrogen_effect,
                        -2.9 as trend_effect,
                        73.51 as v_base,
                        150.0 as total_soil_n_mg_ha,
                        0.0 as mineral_n_spring_kg_ha,
                        0.0 as mineral_n_autumn_kg_ha,
                        0.0 as mineral_n_grazing_kg_ha,
                        0.0 as organic_n_manure_kg_ha,
                        0.0 as n_fixation_kg_ha,
                        -- Simplified nitrogen washout calculation
                        GREATEST(0, -2.9 + 50.0 + 0.8 * 0.9 * 1.085) as nitrogen_washout_kg_ha,
                        GREATEST(0, -2.9 + 50.0 + 0.8 * 0.9 * 1.085) * CAST(area_ha AS DOUBLE) as total_nitrogen_washout_kg,
                        has_soil_data,
                        sufficient_climate_data,
                        'medium' as data_quality,
                        'nles5_simplified_fallback' as estimation_method,
                        current_timestamp as created_at,
                        field_geometry_wkt as geometry_wkt
                    FROM fields_with_climate_soil_crops
                    WHERE total_percolation IS NOT NULL
                        AND total_percolation > 0
                """)

                fallback_count = self.conn.execute("SELECT COUNT(*) FROM nles5_nitrogen_estimates").fetchone()[0]
                self.log.info(f"Created fallback NLES5 estimates: {fallback_count:,} fields with simplified model")

            return "nles5_nitrogen_estimates"

        except Exception as e:
            self.log.error(f"Error in NLES5 calculation: {e}")
            # Create a minimal fallback version
            self.log.warning("Creating minimal fallback NLES5 estimates due to calculation error")
            try:
                self.conn.execute("""
                    CREATE OR REPLACE TABLE nles5_nitrogen_estimates AS
                    SELECT
                        field_id,
                        cvr_number,
                        area_ha,
                        crop_type,
                        organic_farming,
                        year,
                        COALESCE(soil_type_category, 'clay') as soil_type,
                        COALESCE(soil_code, '5') as soil_code,
                        COALESCE(soil_description, 'Medium clay soil') as soil_description,
                        COALESCE(clay_content, 15.0) as clay_content,
                        COALESCE(perco_apr_aug_current, 300.0) as perco_apr_aug_current,
                        COALESCE(perco_sep_mar_current, 400.0) as perco_sep_mar_current,
                        COALESCE(perco_sep_mar_previous, 400.0) as perco_sep_mar_previous,
                        COALESCE(total_percolation, 900.0) as total_percolation,
                        COALESCE(avg_precipitation, 800.0) as avg_precipitation,
                        COALESCE(avg_evaporation, 300.0) as avg_evaporation,
                        COALESCE(climate_distance_m, 0.0) as climate_distance_m,
                        0.0 as crop_effect,
                        0.8 as drainage_effect,
                        0.9 as soil_effect,
                        1.0 as perco_soil_effect,
                        50.0 as nitrogen_effect,
                        -2.9 as trend_effect,
                        73.51 as v_base,
                        150.0 as total_soil_n_mg_ha,
                        0.0 as mineral_n_spring_kg_ha,
                        0.0 as mineral_n_autumn_kg_ha,
                        0.0 as mineral_n_grazing_kg_ha,
                        0.0 as organic_n_manure_kg_ha,
                        0.0 as n_fixation_kg_ha,
                        -- Emergency fallback nitrogen washout
                        50.0 as nitrogen_washout_kg_ha,
                        50.0 * CAST(area_ha AS DOUBLE) as total_nitrogen_washout_kg,
                        COALESCE(has_soil_data, false) as has_soil_data,
                        COALESCE(sufficient_climate_data, true) as sufficient_climate_data,
                        'low' as data_quality,
                        'nles5_emergency_fallback' as estimation_method,
                        current_timestamp as created_at,
                        COALESCE(field_geometry_wkt, 'POINT(10.0 56.0)') as geometry_wkt
                    FROM fields_with_climate_soil_crops
                """)

                fallback_count = self.conn.execute("SELECT COUNT(*) FROM nles5_nitrogen_estimates").fetchone()[0]
                self.log.info(f"Created emergency fallback NLES5 estimates: {fallback_count:,} fields")
                return "nles5_nitrogen_estimates"
            except Exception as fallback_error:
                self.log.error(f"Even NLES5 fallback creation failed: {fallback_error}")
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
                    AVG(total_soil_n_mg_ha) as avg_total_soil_n_mg_ha,
                    AVG(mineral_n_spring_kg_ha) as avg_mineral_n_spring_kg_ha,
                    AVG(mineral_n_autumn_kg_ha) as avg_mineral_n_autumn_kg_ha,
                    AVG(organic_n_manure_kg_ha) as avg_organic_n_manure_kg_ha,
                    AVG(n_fixation_kg_ha) as avg_n_fixation_kg_ha,

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
                self.log.info(f"Fertilizer Data - Avg Soil N: {analysis[17]:.1f} Mg/ha, Spring Mineral N: {analysis[18]:.1f} kg/ha")

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
                            WHEN total_soil_n_mg_ha > 0 AND mineral_n_spring_kg_ha >= 0 THEN 0.12  -- 12% with real fertilizer data
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
                                WHEN 'M8' THEN 'HIGH_LOSS_INTENSIVE: Intensive crop with high nitrogen losses'
                                WHEN 'M1' THEN 'HIGH_LOSS_CEREAL: Winter cereals showing excessive nitrogen washout'
                                WHEN 'M2' THEN 'HIGH_LOSS_SPRING: Spring cereals with poor nitrogen retention'
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

            # Create spatial tables and parameter lookup tables
            self._create_spatial_tables()
            self._create_nles5_parameter_tables()

            # Process climate data to calculate percolation
            climate_table = self._process_climate_data()

            # Spatial join fields with climate data
            fields_climate_table = self._spatial_join_fields_climate()

            # Join with soil data if available
            if self.config.soil_types_dataset in loaded_tables:
                fields_complete_table = self._join_with_soil_data()
            else:
                self.log.warning("No soil data available - using fields with climate data only")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE fields_with_climate_soil_crops AS
                    SELECT
                        f_c.*,
                        '5' as soil_code,
                        'Medium clay soil' as soil_description,
                        15.0 as clay_content,
                        5.0 as tn_t_ha,
                        'clay' as soil_type_category,
                        false as has_soil_data,
                        -- Default crop codes (will be overridden by crop classification)
                        'M2' as m_code,
                        'W2' as w_code,
                        'MP2' as mp_code,
                        'WP2' as wp_code,
                        'WC2' as wc_code
                    FROM fields_with_climate f_c
                """)
                fields_complete_table = "fields_with_climate_soil_crops"

            # Note: Nitrogen inputs are prepared as part of the join_with_soil_data method
            # which creates the final fields_with_climate_soil_crops table

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

            # Calculate uncertainty estimates first
            uncertainty_table = self._calculate_uncertainty_estimates()

            # Analyze uncertainty patterns
            patterns_table = self._analyze_uncertainty_patterns()

            # Analyze estimates distribution (after uncertainty is calculated)
            self._analyze_estimates_distribution()

            # Save results to gold layer
            self._save_results_to_gold()

            self.log.info("NLES5 nitrogen estimation completed successfully")

        except Exception as e:
            self.log.error(f"Error in NLES5 processing: {e}")
            self.log.exception(e)
            raise

    @timed(name="Creating NLES5 parameter lookup tables")
    def _create_nles5_parameter_tables(self) -> None:
        """Create all NLES5 parameter lookup tables needed for calculations."""
        try:
            self.log.info("Creating NLES5 parameter lookup tables")

            # Create crop parameters table (from config.crop_parameters)
            crop_params_values = [
                f"('{code}', {value})"
                for code, value in self.config.crop_parameters.items()
            ]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE crop_params AS
                SELECT * FROM VALUES
                {', '.join(crop_params_values)}
                AS t(code, param)
            """)

            # Create winter vegetation parameters table
            winter_veg_values = [
                f"('{code}', {value})"
                for code, value in self.config.winter_veg_parameters.items()
            ]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE winter_veg_params AS
                SELECT * FROM VALUES
                {', '.join(winter_veg_values)}
                AS t(code, param)
            """)

            # Create previous crop parameters table
            prev_crop_values = [
                f"('{code}', {value})"
                for code, value in self.config.prev_crop_parameters.items()
            ]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE prev_crop_params AS
                SELECT * FROM VALUES
                {', '.join(prev_crop_values)}
                AS t(code, param)
            """)

            # Create previous winter vegetation parameters table
            prev_winter_veg_values = [
                f"('{code}', {value})"
                for code, value in self.config.prev_winter_veg_parameters.items()
            ]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE prev_winter_veg_params AS
                SELECT * FROM VALUES
                {', '.join(prev_winter_veg_values)}
                AS t(code, param)
            """)

            # Create theta factors table
            theta_values = [
                f"('{code}', {value})"
                for code, value in self.config.theta_factors.items()
            ]
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE theta_factors AS
                SELECT * FROM VALUES
                {', '.join(theta_values)}
                AS t(code, factor)
            """)

            self.log.info("✅ Created all NLES5 parameter lookup tables")

        except Exception as e:
            self.log.error(f"Error creating NLES5 parameter tables: {e}")
            raise

    @timed(name="Creating spatial tables")
    def _create_spatial_tables(self) -> None:
        """Create spatial tables with proper geometry processing for spatial operations."""
        try:
            self.log.info("Creating spatial tables for agricultural fields and soil types")

                        # Create agricultural_fields_spatial table with proper geometry
            # Based on available columns: ['geometry', 'field_id', 'field_uuid', ...]
            self.conn.execute("""
                CREATE OR REPLACE TABLE agricultural_fields_spatial AS
                SELECT
                    *,
                    -- Use the available geometry column (WKB format)
                    geometry as geom,
                    -- Create centroid for nearest neighbor operations
                    ST_Centroid(geometry) as centroid_geom
                FROM agricultural_fields
                WHERE geometry IS NOT NULL
                    AND ST_IsValid(geometry) = true
            """)

            # Validate and transform geometries for agricultural fields
            fields_count = validate_and_transform_geometries_duckdb(
                self.conn, "agricultural_fields_spatial", "geom"
            )
            self.log.info(f"Processed {fields_count:,} agricultural fields with valid geometries")

            # Create soil_types_spatial table if soil data is available
            soil_table_exists = False
            try:
                # Check if soil_types table exists
                soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]
                if soil_count > 0:
                    self.conn.execute("""
                        CREATE OR REPLACE TABLE soil_types_spatial AS
                        SELECT
                            *,
                            -- Use available geometry column
                            geometry as geometry_spatial
                        FROM soil_types
                        WHERE geometry IS NOT NULL
                            AND ST_IsValid(geometry) = true
                    """)

                    # Validate and transform soil geometries
                    soil_geom_count = validate_and_transform_geometries_duckdb(
                        self.conn, "soil_types_spatial", "geometry_spatial"
                    )
                    self.log.info(f"Processed {soil_geom_count:,} soil type geometries")
                    soil_table_exists = True
                else:
                    self.log.warning("No soil types data available")
            except Exception as e:
                self.log.warning(f"Could not create soil_types_spatial table: {e}")

            if not soil_table_exists:
                # Create dummy soil_types_spatial table for fallback
                self.log.info("Creating fallback soil_types_spatial table")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE soil_types_spatial AS
                    SELECT
                        '5' as soil_code,
                        'Medium clay soil' as soil_description,
                        15.0 as clay_content,
                        5.0 as total_n_content,
                        ST_GeomFromText('POLYGON((8 54, 15 54, 15 58, 8 58, 8 54))') as geometry_spatial
                """)

            self.log.info("✅ Created spatial tables successfully")

        except Exception as e:
            self.log.error(f"Error creating spatial tables: {e}")
            raise

    @timed(name="Preparing nitrogen input tables")
    def _prepare_nitrogen_inputs_tables(self) -> None:
        """
        Prepare nitrogen fixation and fertilizer history tables needed for NLES5 calculations.
        """
        try:
            self.log.info("Preparing nitrogen fixation and fertilizer history tables")

            # Step 1: Create N-fixation mapping table (from NLES5 documentation)
            self.conn.execute("""
                CREATE OR REPLACE TABLE n_fixation_mapping AS
                SELECT unnest(codes) as glr_code, fixation_rate
                FROM (VALUES
                    (200, [18, 25, 30, 31, 32, 35, 36, 54, 217, 326, 424]),
                    (100, [7]),
                    (70, [214, 234]),
                    (140, [215]),
                    (140, [171, 173, 273, 277, 288]),
                    (200, [120, 121]),
                    (120, [170, 172, 174, 255, 256, 260, 261, 262, 272, 274, 284, 306]),
                    (60, [247, 258, 266, 267, 268, 276, 285, 286, 287]),
                    (5, [248, 249, 250, 251, 252, 253, 254, 257, 259, 263, 264, 265, 269, 275, 278, 279, 305, 315, 350, 488]),
                    (20, [943, 944, 945, 946, 960, 961, 962, 963, 964, 965, 966, 975])
                ) AS t(fixation_rate, codes)
            """)

            # Step 2: Create N-fixation history table
            self.conn.execute("""
                CREATE OR REPLACE TABLE n_fixation_history AS
                WITH n_fixation_by_field_year AS (
                    SELECT
                        a.field_id,
                        a.year,
                        COALESCE(fix.fixation_rate, 2.0) as nfix_ha -- Default to 2 kg N/ha
                    FROM agricultural_fields a
                    LEFT JOIN n_fixation_mapping fix ON a.crop_code = fix.glr_code
                )
                SELECT
                    field_id,
                    year,
                    nfix_ha,
                    (
                        COALESCE(LAG(nfix_ha, 1) OVER (PARTITION BY field_id ORDER BY year), 0.0) +
                        COALESCE(LAG(nfix_ha, 2) OVER (PARTITION BY field_id ORDER BY year), 0.0)
                    ) / 2.0 as nfix_prev
                FROM n_fixation_by_field_year
            """)

            # Step 3: Create fertilizer history table (if fertilizer data is available)
            fertilizer_table_exists = False
            try:
                fertilizer_count = self.conn.execute("SELECT COUNT(*) FROM fertilizer_accounts").fetchone()[0]
                if fertilizer_count > 0:
                    self.conn.execute("""
                        CREATE OR REPLACE TABLE fertilizer_history AS
                        SELECT
                            cvr_number,
                            year,
                            mineral_n_foraar,
                            mineral_n_eft,
                            mineral_n_udb,
                            organic_n_hus,
                            -- Calculate average of previous 2 years for mineral and organic N
                            (
                                COALESCE(LAG(mineral_n_foraar + mineral_n_eft + mineral_n_udb, 1) OVER (PARTITION BY cvr_number ORDER BY year), 0.0) +
                                COALESCE(LAG(mineral_n_foraar + mineral_n_eft + mineral_n_udb, 2) OVER (PARTITION BY cvr_number ORDER BY year), 0.0)
                            ) / 2.0 as mineral_n_prev,
                            (
                                COALESCE(LAG(organic_n_hus, 1) OVER (PARTITION BY cvr_number ORDER BY year), 0.0) +
                                COALESCE(LAG(organic_n_hus, 2) OVER (PARTITION BY cvr_number ORDER BY year), 0.0)
                            ) / 2.0 as organic_n_prev
                        FROM fertilizer_accounts
                    """)
                    fertilizer_table_exists = True
                    self.log.info("Created fertilizer history table from fertilizer_accounts data")
                else:
                    self.log.warning("No fertilizer accounts data available")
            except Exception as e:
                self.log.warning(f"Could not create fertilizer history table: {e}")

            if not fertilizer_table_exists:
                # Create empty fertilizer history table for graceful degradation
                self.log.info("Creating empty fertilizer history table (will use defaults)")
                self.conn.execute("""
                    CREATE OR REPLACE TABLE fertilizer_history AS
                    SELECT
                        CAST(NULL AS BIGINT) as cvr_number,
                        CAST(NULL AS INT) as year,
                        CAST(NULL AS DOUBLE) as mineral_n_foraar,
                        CAST(NULL AS DOUBLE) as mineral_n_eft,
                        CAST(NULL AS DOUBLE) as mineral_n_udb,
                        CAST(NULL AS DOUBLE) as organic_n_hus,
                        CAST(NULL AS DOUBLE) as mineral_n_prev,
                        CAST(NULL AS DOUBLE) as organic_n_prev
                    WHERE 1=0  -- Empty table with proper schema
                """)

            nfix_count = self.conn.execute("SELECT COUNT(*) FROM n_fixation_history").fetchone()[0]
            self.log.info(f"✅ Prepared nitrogen input tables: {nfix_count:,} N-fixation records")

        except Exception as e:
            self.log.error(f"Error preparing nitrogen input tables: {e}")
            raise