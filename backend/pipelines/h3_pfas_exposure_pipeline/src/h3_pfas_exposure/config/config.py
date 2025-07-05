"""
Configuration classes for H3 PFAS exposure analysis.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class H3SpatialConfig:
    """Centralized configuration for H3 spatial processing."""

    # H3 Configuration
    h3_resolution: int = 10  # Resolution 10 = ~1.5 ha cells
    denmark_bounds: dict[str, float] = field(
        default_factory=lambda: {"min_lat": 54.5, "max_lat": 57.8, "min_lon": 8.0, "max_lon": 15.2}
    )

    # Chunked Processing Configuration - OPTIMIZED for GitHub Actions Free Tier (16GB RAM, 4 CPUs)
    chunk_size: int = 10000  # Increased for 16GB RAM capacity
    min_intersection_area_sqm: float = 0.0  # Include all intersections, no matter how small

    # Memory Management - Updated for 16GB GitHub Actions runners
    memory_limit: str = "14GB"  # Conservative limit leaving 2GB for system
    thread_count: int = 4  # Utilize all 4 CPUs available
    max_memory_usage_gb: float = 14.0  # Maximum memory usage threshold
    memory_warning_threshold_gb: float = 12.0  # Warning when approaching limit

    # Disk Management - 14GB SSD available on GitHub Actions
    max_disk_usage_gb: float = 12.0  # Conservative disk usage limit
    disk_warning_threshold_gb: float = 10.0  # Warning threshold

    # Time Management - 6 hour limit for public repos
    max_job_time_hours: float = 5.5  # Conservative time limit
    time_warning_threshold_hours: float = 5.0  # Warning threshold

    # GitHub Actions Optimization Settings
    github_actions_mode: bool = True  # Enable GitHub Actions specific optimizations
    enable_memory_monitoring: bool = True  # Monitor memory usage
    enable_disk_monitoring: bool = True  # Monitor disk usage
    enable_time_monitoring: bool = True  # Monitor execution time
    cleanup_frequency: int = 1000  # Clean up every N chunks processed
    aggressive_cleanup: bool = True  # Enable aggressive cleanup for memory management

    # Area Validation Thresholds
    min_h3_area_ha: float = 0.5  # Minimum H3 cell area in hectares
    max_h3_area_ha: float = 3.0  # Maximum H3 cell area in hectares
    max_intersection_ratio: float = 1.1  # Allow 10% tolerance for intersection calculations

    # GCS Configuration
    bucket: str = "landbrugsdata-raw-data"
    gcs_base_path: str = "gs://landbrugsdata-raw-data"

    # Processing Configuration
    enable_coordinate_flipping: bool = True  # Enable coordinate flipping for spatial operations
    enable_area_validation: bool = True  # Enable area validation checks
    enable_chunked_processing: bool = True  # Enable chunked processing for large datasets
    enable_progress_reporting: bool = True  # Enable progress reporting

    # DuckDB Configuration - Optimized for 16GB RAM
    duckdb_memory_limit: str = "12GB"  # DuckDB memory limit
    duckdb_threads: int = 4  # DuckDB thread count
    duckdb_temp_directory: str = "/tmp/duckdb"  # Temporary directory for DuckDB

    # 5-Stage Spatial Join Configuration
    stage_1_fast_intersection: bool = True
    stage_2_area_calculation: bool = True
    stage_3_h3_field_aggregation: bool = True
    stage_4_pesticide_join: bool = True
    stage_5_geometric_union: bool = True

    # Coordinate System Configuration
    coordinate_system: str = "EPSG:4326"

    # H3 Resolution-specific validation (official areas from h3geo.org in hectares)
    h3_resolution_areas: dict[int, dict[str, float]] = field(
        default_factory=lambda: {
            7: {
                "min_area_ha": 312.68,
                "max_area_ha": 622.74,
                "theoretical_avg_area_ha": 516.13,
            },  # 5.161293360 km² avg
            8: {
                "min_area_ha": 44.65,
                "max_area_ha": 88.96,
                "theoretical_avg_area_ha": 73.73,
            },  # 0.737327598 km² avg
            9: {
                "min_area_ha": 6.38,
                "max_area_ha": 12.71,
                "theoretical_avg_area_ha": 10.53,
            },  # 0.105332513 km² avg
            10: {
                "min_area_ha": 1.0000,
                "max_area_ha": 2.0000,  # Reasonable bounds for H3 resolution 10
                "theoretical_avg_area_ha": 1.5048,  # Official H3 specification for resolution 10
            },  # Official H3 specification: ~1.5048 hectares average
        }
    )

    # Validation Thresholds (will be set based on resolution in __post_init__)
    theoretical_avg_area_ha: float = 1.5048
    max_area_deviation_pct: float = 20.0  # Standard tolerance for H3 area calculations

    # Performance Monitoring
    enable_progress_tracking: bool = True
    log_chunk_details: bool = False  # Reduced logging for performance
    log_stage_timings: bool = True

    # GCS Configuration
    available_years: list[int] = field(
        default_factory=lambda: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    )

    def __post_init__(self):
        """Set resolution-specific validation parameters after initialization."""
        if self.h3_resolution not in self.h3_resolution_areas:
            raise ValueError(f"H3 resolution must be 7, 8, 9, or 10. Got: {self.h3_resolution}")

        # Set resolution-specific validation parameters
        area_config = self.h3_resolution_areas[self.h3_resolution]
        self.min_h3_area_ha = area_config["min_area_ha"]
        self.max_h3_area_ha = area_config["max_area_ha"]
        self.theoretical_avg_area_ha = area_config["theoretical_avg_area_ha"]


@dataclass
class ValidationResult:
    """Result of validation checks."""

    name: str
    passed: bool
    message: str
    metrics: dict[str, Any] = field(default_factory=dict)
