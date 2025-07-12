"""Shared configuration for Field Area Analysis pipeline stages."""

import os
from typing import Dict

from pydantic import BaseModel, ConfigDict


class FieldAreaAnalysisConfig(BaseModel):
    """Shared configuration across all pipeline stages."""

    # GCS Configuration
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Dataset names - using consistent naming from silver layer
    agricultural_fields_dataset: str = "fvm_marker_2024"
    properties_dataset: str = "property_cadastral_merged"
    soil_types_dataset: str = "soil_types"
    bnbo_status_dataset: str = "bnbo_status_dissolved"
    wetlands_dataset: str = "wetlands_dissolved"
    water_projects_dataset: str = "water_projects_dissolved"

    # Processing parameters optimized for GitHub Actions (16GB RAM, 4 CPU)
    batch_size: int = 250000  # Stage 1C: Large datasets (fields × properties)
    stage3_batch_size: int = (
        25000  # Stage 3: Complex spatial joins (fields × env features × water projects)
    )
    max_memory_gb: int = 14  # Leave 2GB for system overhead
    max_threads: int = 4
    max_temp_directory_size: str = "12GB"  # Leave space for downloads

    # DuckDB Spatial optimization settings (based on PR #545)
    enable_spatial_join_operator: bool = True
    preserve_insertion_order: bool = False  # Faster processing
    enable_object_cache: bool = True

    # GitHub Actions memory management settings
    memory_cleanup_frequency: int = 3  # Clean up every N batches for Stage 3
    stage1_memory_cleanup_frequency: int = 5  # Less frequent for Stage 1 (simpler queries)

    # Output dataset names for intermediate stages
    stage_outputs: Dict[str, str] = {
        # Stage 1 outputs
        "bnbo_water_coverage": "field_analysis_bnbo_water_coverage",
        "water_projects_bnbo_intersections": "field_analysis_water_projects_bnbo_intersections",
        "wetland_water_coverage": "field_analysis_wetland_water_coverage",
        "water_projects_wetlands_intersections": "field_analysis_water_projects_wetlands_intersections",
        "field_property_intersections": "field_analysis_property_intersections",
        # Stage 2 outputs (formerly Stage 3)
        "fields_bnbo_water": "field_analysis_fields_bnbo_water",
        "fields_wetland_water": "field_analysis_fields_wetland_water",
        # Stage 3 outputs (formerly Stage 4)
        "final_bnbo": "field_analysis_final_bnbo",
        "final_wetland": "field_analysis_final_wetland",
        # Stage 4 outputs (formerly Stage 5)
        "consolidated": "field_analysis_final",
    }

    # Parquet export settings for optimal performance
    parquet_compression: str = "zstd"  # Best balance of speed/size
    parquet_row_group_size: int = 100000

    model_config = ConfigDict(frozen=True)


# Global config instance
CONFIG = FieldAreaAnalysisConfig()
