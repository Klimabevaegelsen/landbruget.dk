"""Configuration for PMTiles Generator Gold Pipeline."""

from typing import List, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig


class PMTilesGeneratorConfig(BaseJobConfig):
    """Configuration for PMTiles Generator Gold Pipeline."""

    name: str = "PMTiles Generator Gold"
    dataset: str = "pmtiles_generator"
    type: str = "gold"

    # Output configuration
    cloudflare_r2_bucket: str = Field(
        default="pesticidkortet", description="Cloudflare R2 bucket name"
    )
    r2_base_url: str = Field(
        default="https://data.pesticidkortet.dk", description="R2 base URL for public access"
    )

    # PMTiles generation settings
    tippecanoe_max_zoom: int = Field(default=14, description="Maximum zoom level for PMTiles")
    tippecanoe_min_zoom: int = Field(default=0, description="Minimum zoom level for PMTiles")
    tippecanoe_buffer: int = Field(default=64, description="Buffer size for tippecanoe")
    tippecanoe_simplification: int = Field(default=10, description="Simplification level")

    # Data inclusion settings
    include_nles5_data: bool = Field(
        default=True, description="Include NLES5 nitrogen estimation data when available"
    )
    include_production_data: bool = Field(
        default=True, description="Include field production estimates"
    )
    include_pesticide_proximity: bool = Field(
        default=True, description="Include pesticide proximity analysis"
    )
    include_environmental_analysis: bool = Field(
        default=True, description="Include environmental analysis data"
    )

    # File size targets (MB)
    max_field_analysis_size_mb: int = Field(
        default=1200, description="Maximum size for field analysis PMTiles"
    )
    max_environmental_layer_size_mb: int = Field(
        default=600, description="Maximum size for environmental layer PMTiles"
    )

    # Processing settings
    enable_parallel_processing: bool = Field(
        default=True, description="Enable parallel year processing"
    )
    max_parallel_years: int = Field(
        default=4, description="Maximum number of years to process in parallel"
    )

    # Year filtering (optional - if not provided, will auto-detect)
    target_years: Optional[List[int]] = Field(
        default=None, description="Specific years to process (auto-detect if None)"
    )
    exclude_years: List[int] = Field(
        default_factory=list, description="Years to exclude from processing"
    )

    # Data source paths in GCS
    gcs_bucket: str = Field(
        default="landbrugsdata-raw-data", description="GCS bucket for source data"
    )

    # Dataset paths
    fvm_marker_path: str = Field(
        default="silver/fvm_marker_{year}/", description="FVM marker data path template"
    )
    field_environmental_path: str = Field(
        default="gold/field_environmental_analysis_fields_{year}/",
        description="Field environmental analysis path template",
    )
    field_production_path: str = Field(
        default="gold/field_production_{year}/",
        description="Field production estimates path template",
    )
    pesticide_proximity_path: str = Field(
        default="gold/pesticide_proximity_{year}_{next_year}/",
        description="Pesticide proximity path template",
    )
    nles5_estimation_path: str = Field(
        default="gold/nles5_nitrogen_estimation/latest/",
        description="NLES5 nitrogen estimation path",
    )

    # Environmental layers (year-independent)
    bnbo_status_path: str = Field(
        default="silver/bnbo_status_dissolved/", description="BNBO status dissolved path"
    )
    wetlands_path: str = Field(
        default="silver/wetlands_dissolved/", description="Wetlands dissolved path"
    )
    water_projects_path: str = Field(
        default="silver/water_projects_dissolved/", description="Water projects dissolved path"
    )
    bbr_buildings_path: str = Field(
        default="silver/bbr_buildings/", description="BBR buildings path"
    )

    # Temporary file settings
    temp_dir: str = Field(
        default="/tmp/pmtiles_generator", description="Temporary directory for processing"
    )
    cleanup_temp_files: bool = Field(
        default=True, description="Clean up temporary files after processing"
    )

    # R2 cleanup settings
    enable_r2_cleanup: bool = Field(
        default=True, description="Enable automatic cleanup of old PMTiles in R2"
    )
    keep_versions: int = Field(
        default=3, description="Number of versions to keep for year-specific PMTiles"
    )
    cleanup_environmental_layers: bool = Field(
        default=True, description="Replace environmental layers with new versions"
    )

    class Config:
        """Pydantic configuration."""

        env_prefix = "PMTILES_"
        case_sensitive = False
