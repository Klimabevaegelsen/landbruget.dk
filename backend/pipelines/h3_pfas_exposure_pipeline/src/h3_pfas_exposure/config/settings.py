"""Configuration settings for H3 PFAS exposure pipeline."""

import os
from dataclasses import dataclass, field


@dataclass
class H3PFASConfig:
    """Configuration for H3 PFAS exposure pipeline."""

    # H3 Configuration
    h3_resolution: int = 10  # Resolution 10 = ~1.5 ha cells
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

    # Validation Configuration
    min_h3_area_ha: float = 0.91
    max_h3_area_ha: float = 1.82
    theoretical_avg_area_ha: float = 1.5048
    max_area_deviation_pct: float = 20.0

    # Performance Configuration
    enable_progress_tracking: bool = True
    log_chunk_details: bool = True
    log_stage_timings: bool = True

    @classmethod
    def from_env(cls) -> "H3PFASConfig":
        """Create configuration from environment variables."""
        return cls(
            h3_resolution=int(os.getenv("H3_RESOLUTION", "10")),
            chunk_size=int(os.getenv("CHUNK_SIZE", "25000")),
            memory_limit=os.getenv("MEMORY_LIMIT", "12GB"),
            thread_count=int(os.getenv("THREAD_COUNT", "4")),
            bucket=os.getenv("GCS_BUCKET", "landbrugsdata-raw-data"),
            enable_progress_tracking=os.getenv("ENABLE_PROGRESS_TRACKING", "true").lower()
            == "true",
            log_chunk_details=os.getenv("LOG_CHUNK_DETAILS", "true").lower() == "true",
            log_stage_timings=os.getenv("LOG_STAGE_TIMINGS", "true").lower() == "true",
        )
