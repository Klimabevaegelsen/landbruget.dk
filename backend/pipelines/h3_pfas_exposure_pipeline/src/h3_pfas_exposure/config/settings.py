"""Configuration settings for H3 PFAS exposure pipeline."""

import os
from dataclasses import dataclass, field


@dataclass
class H3PFASConfig:
    """Configuration for H3 PFAS exposure pipeline."""

    # H3 Configuration
    h3_resolution: int = 10  # Resolution 7-10 supported
    denmark_bounds: dict[str, float] = field(
        default_factory=lambda: {"min_lat": 54.5, "max_lat": 57.8, "min_lon": 8.0, "max_lon": 15.2}
    )

    # Processing Configuration
    chunk_size: int = 25000  # H3 cells per chunk
    memory_limit: str = "12GB"
    thread_count: int = 4

    # GCS Configuration
    bucket: str = "landbrugsdata-raw-data"
    available_years: list[int] = field(
        default_factory=lambda: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    )

    # H3 Resolution-specific validation (official areas from h3geo.org in hectares)
    h3_resolution_areas: dict[int, dict[str, float]] = field(
        default_factory=lambda: {
            7: {
                "min_area_ha": 312.68,
                "max_area_ha": 622.74,
                "theoretical_avg_area_ha": 516.13,
            },  # 5.161293360 km²
            8: {
                "min_area_ha": 44.65,
                "max_area_ha": 88.96,
                "theoretical_avg_area_ha": 73.73,
            },  # 0.737327598 km²
            9: {
                "min_area_ha": 6.38,
                "max_area_ha": 12.71,
                "theoretical_avg_area_ha": 10.53,
            },  # 0.105332513 km²
            10: {
                "min_area_ha": 0.91,
                "max_area_ha": 1.82,
                "theoretical_avg_area_ha": 1.50,
            },  # 0.015047502 km²
        }
    )

    # Validation Configuration (will be set based on resolution)
    min_h3_area_ha: float = 0.91
    max_h3_area_ha: float = 1.82
    theoretical_avg_area_ha: float = 1.50
    max_area_deviation_pct: float = 30.0  # More lenient for different resolutions

    # Performance Configuration
    enable_progress_tracking: bool = True
    log_chunk_details: bool = True
    log_stage_timings: bool = True

    def __post_init__(self):
        """Validate and set resolution-specific parameters after initialization."""
        if self.h3_resolution not in [7, 8, 9, 10]:
            raise ValueError(f"H3 resolution must be 7, 8, 9, or 10. Got: {self.h3_resolution}")

        # Set resolution-specific validation parameters
        if self.h3_resolution in self.h3_resolution_areas:
            area_config = self.h3_resolution_areas[self.h3_resolution]
            self.min_h3_area_ha = area_config["min_area_ha"]
            self.max_h3_area_ha = area_config["max_area_ha"]
            self.theoretical_avg_area_ha = area_config["theoretical_avg_area_ha"]

    @classmethod
    def from_env(cls) -> "H3PFASConfig":
        """Create configuration from environment variables with sensible defaults."""
        h3_resolution = int(os.getenv("H3_RESOLUTION", "10"))

        # Validate resolution
        if h3_resolution not in [7, 8, 9, 10]:
            raise ValueError(f"H3_RESOLUTION must be 7, 8, 9, or 10. Got: {h3_resolution}")

        return cls(
            h3_resolution=h3_resolution,
            chunk_size=int(os.getenv("CHUNK_SIZE", "25000")),
            memory_limit="12GB",  # Always use 12GB - no env dependency
            thread_count=int(os.getenv("THREAD_COUNT", "4")),
            bucket=os.getenv("GCS_BUCKET", "landbrugsdata-raw-data"),
            enable_progress_tracking=os.getenv("ENABLE_PROGRESS_TRACKING", "true").lower()
            == "true",
            log_chunk_details=os.getenv("LOG_CHUNK_DETAILS", "true").lower() == "true",
            log_stage_timings=os.getenv("LOG_STAGE_TIMINGS", "true").lower() == "true",
        )

    def get_resolution_description(self) -> str:
        """Get a human-readable description of the current resolution."""
        descriptions = {
            7: "~516.13 ha per hexagon (regional level)",
            8: "~73.73 ha per hexagon (county level)",
            9: "~10.53 ha per hexagon (municipal level)",
            10: "~1.50 ha per hexagon (field level)",
        }
        return descriptions.get(self.h3_resolution, f"Resolution {self.h3_resolution}")
