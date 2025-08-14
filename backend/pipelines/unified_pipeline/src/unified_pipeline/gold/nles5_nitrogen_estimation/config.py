"""
NLES5 Nitrogen Estimation Configuration

This module contains the configuration class for the NLES5 Nitrogen Estimation gold layer.
All NLES5 model parameters, processing settings, and environment variable overrides are
defined here to ensure consistency across all processing modules.
"""

import os
import json
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig


class NLES5NitrogenEstimationGoldConfig(BaseJobConfig):
    """Configuration for NLES5 Nitrogen Estimation gold layer."""

    name: str = "NLES5 Nitrogen Estimation Gold"
    dataset: str = "nles5_nitrogen_estimation"
    type: str = "gold"
    description: str = "Comprehensive nitrogen washout estimates using the NLES5 model with real climate data"
    frequency: str = "monthly$"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets - Updated to match actual GCS structure
    soil_types_dataset: str = "soil_types"
    dmi_precipitation_dataset: str = "dmi_acc_precip_dmi_acc_precip"  # DMI accumulated precipitation
    dmi_evaporation_dataset: str = "dmi_pot_evaporation_makkink_dmi_pot_evaporation_makkink"  # DMI potential evaporation
    fertilizer_dataset: str = "fertiliser"  # Fertilizer data from silver layer
    agricultural_fields_dataset: str = "fvm_marker"  # Use fvm_marker instead of agricultural_fields
    # Field plans and catch crops are in fertiliser directory as GKEA and Efterafgrøder files
    field_plan_dataset: str = "field_plan"  # GKEA field plan data (in fertiliser directory)
    catch_crops_dataset: str = "catch_crops"  # Efterafgrøder data (in fertiliser directory)

    # Processing configuration - CHUNKED FOR STABILITY
    batch_size: int = 50000  # Reduced batch size for memory-intensive operations
    max_year_lag: int = 2  # NLES5 requires 3-year windows (current + 2 previous years)
    climate_data_days: int = 365  # Days of climate data to analyze

    # ULTIMATE MEMORY OPTIMIZATION - TARGET-YEAR-BY-TARGET-YEAR PROCESSING
    enable_spatial_indexing: bool = True  # Enable spatial indexes for performance  
    use_chunked_processing: bool = True  # Enable chunking to handle large datasets
    use_target_year_processing: bool = True  # ULTIMATE: Process one target year at a time with 3-year windows
    max_memory_usage_gb: int = 6  # More conservative memory limit for stability
    
    # Enhanced chunked processing settings for memory management
    # Can be overridden by environment variables for fine-tuning
    tessellation_batch_size: int = int(os.getenv('TESSELLATION_BATCH_SIZE', '25000'))  # Grid cells per tessellation batch
    spatial_join_batch_size: int = int(os.getenv('SPATIAL_JOIN_BATCH_SIZE', '30000'))  # Fields per spatial join batch
    nles5_calculation_batch_size: int = int(os.getenv('NLES5_CALCULATION_BATCH_SIZE', '40000'))  # Fields per NLES5 calculation batch
    
    # Disk space and performance optimization settings - UNRESTRICTED
    max_temp_directory_size_gb: int = 500  # Large temp space allowance
    threads: int = 2  # Very conservative to reduce memory contention
    preserve_insertion_order: bool = False  # Disable to save memory/disk space



    # OPTIMIZED YEAR SELECTION: Only loads years actually needed for NLES5 calculations
    # Specify target calculation years - pipeline automatically loads required supporting years (current + 2 previous)
    # Example: target_years = [2021, 2022] → loads [2019, 2020, 2021, 2022] (4 years instead of 18 years)
    # target_years: Optional[List[int]] = None
    target_years: Optional[List[int]] = [2021, 2022, 2023]

    # MEMORY OPTIMIZATION: Limits target calculation years (auto-discovery with memory management)
    # NLES5 requires 3-year windows: current + previous + year before previous
    # Pipeline automatically calculates minimum data years needed for each target year
    # Can be overridden by setting the MAX_YEARS_TO_PROCESS environment variable.
    max_years_to_process: Optional[int] = int(os.getenv('MAX_YEARS_TO_PROCESS')) if os.getenv('MAX_YEARS_TO_PROCESS') else 5  # Limit target years to reduce dataset size

    # PIPELINE-LEVEL BATCHING: Run entire pipeline for batches of target years
    # This provides maximum memory efficiency by completely isolating each batch
    # Example: target_year_batch_size = 2 → Process years [2021,2022], then [2023,2024], etc.
    # Each batch runs the complete pipeline (phases 1-8) independently
    enable_pipeline_batching: bool = bool(os.getenv('ENABLE_PIPELINE_BATCHING', 'true').lower() == 'true')
    target_year_batch_size: int = int(os.getenv('TARGET_YEAR_BATCH_SIZE', '2'))  # Years per pipeline batch

    # Geographic bounds for testing (WGS84 coordinates: [min_lon, min_lat, max_lon, max_lat])
    # Set to None to process entire Denmark, or specify bounds for testing
    # Test area: Small area around Aarhus city (minimal disk space usage)
    # This reduces dataset size by ~98% while maintaining representative agricultural data
    # To disable geographic filtering, set test_bounds = None
    # Can be overridden by setting the TEST_BOUNDS environment variable as a JSON string, e.g., '[10.0, 55.9, 10.3, 56.2]'.
    test_bounds: Optional[List[float]] = json.loads(os.getenv('TEST_BOUNDS')) if os.getenv('TEST_BOUNDS') else None  # Process entire Denmark by default

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

    # Soil type parameters for percolation effects (CORRECTED to match SAS reference exactly)
    # From SAS lines 145-147: 
    # Sand: (1-exp(-0.001194*per1_nles5-0.00111*per2_nles5)) * exp(-0.00086*per_p_nles5)
    # Clay: (1-exp(-0.00080*per1_nles5-0.00075*per2_nles5)) * exp(-0.00064*per_p_nles5)
    soil_parameters: Dict[str, Dict[str, float]] = {
        'sand': {
            'per1_coef': -0.001194,  # SAS: -0.001194 (Sep-Nov coefficient)
            'per2_coef': -0.00111,   # SAS: -0.00111 (Dec-Feb + Mar-Aug current coefficient)
            'per_p_coef': -0.00086   # SAS: -0.00086 (Dec-Feb + Mar-Aug previous coefficient)
        },
        'clay': {
            'per1_coef': -0.00080,   # SAS: -0.00080 (Sep-Nov coefficient)
            'per2_coef': -0.00075,   # SAS: -0.00075 (Dec-Feb + Mar-Aug current coefficient)  
            'per_p_coef': -0.00064   # SAS: -0.00064 (Dec-Feb + Mar-Aug previous coefficient)
        }
    }

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
