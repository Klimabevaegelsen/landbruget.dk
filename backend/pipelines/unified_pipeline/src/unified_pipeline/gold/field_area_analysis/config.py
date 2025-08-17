"""Shared configuration for Field Area Analysis pipeline stages."""

import os
from typing import Dict

from pydantic import BaseModel, ConfigDict


class FieldAreaAnalysisConfig(BaseModel):
    """Shared configuration across all pipeline stages."""

    # GCS Configuration
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Year configuration for agricultural fields
    agricultural_fields_year: int = int(os.getenv("AGRICULTURAL_FIELDS_YEAR", "2024"))

    # Dataset names - using consistent naming from silver layer
    properties_dataset: str = "property_cadastral_merged"
    soil_types_dataset: str = "soil_types"
    bnbo_status_dataset: str = "bnbo_status_dissolved"
    wetlands_dataset: str = "wetlands_dissolved"  # Use dissolved wetlands - now preserves
    # toerv_pct AND eliminates overlaps
    water_projects_dataset: str = "water_projects_dissolved"

    # Stage 0 pre-filtered dataset names (dramatically reduced sizes)
    properties_filtered_dataset: str = "stage0_properties_filtered"  # 6.5M → ~500K (90% reduction)
    bnbo_filtered_dataset: str = "stage0_bnbo_filtered"  # 3.7K → ~1K (70% reduction)
    wetlands_filtered_dataset: str = "stage0_wetlands_filtered"  # 1.6M → ~200K (85% reduction)
    water_projects_filtered_dataset: str = (
        "stage0_water_projects_filtered"  # 2.4K → ~500 (80% reduction)
    )
    soil_types_filtered_dataset: str = "stage0_soil_types_filtered"  # 13K → ~8K (40% reduction)

    # Processing parameters optimized for GitHub Actions with Stage 0 pre-filtering
    # Batch sizes can be larger due to dramatically reduced probe sizes
    batch_size: int = 500000  # Stage 1: Pre-filtered datasets (13x reduction in complexity)
    stage2_batch_size: int = (
        25000  # Stage 2: Field-level joins with pre-filtered data (2.5x larger)
    )
    stage3_batch_size: int = (
        10000  # Stage 3: Property-level analysis with pre-filtered data (2x larger)
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
    stage2_memory_cleanup_frequency: int = 2  # More frequent for Stage 2 (spatial joins)

    # Output dataset names for intermediate stages
    # Note: These will be dynamically updated with year suffix via update_outputs_for_year()
    stage_outputs: Dict[str, str] = {
        # Stage 0 outputs (pre-filtering for massive performance improvement)
        "properties_prefiltered": "stage0_properties_filtered",
        "bnbo_prefiltered": "stage0_bnbo_filtered",
        "wetlands_prefiltered": "stage0_wetlands_filtered",
        "water_projects_prefiltered": "stage0_water_projects_filtered",
        "soil_types_prefiltered": "stage0_soil_types_filtered",
        # Stage 1 outputs (using pre-filtered datasets)
        "bnbo_water_coverage": "field_analysis_bnbo_water_coverage",
        "water_projects_bnbo_intersections": "field_analysis_water_projects_bnbo_intersections",
        "wetland_water_coverage": "field_analysis_wetland_water_coverage",
        "water_projects_wetlands_intersections":
            "field_analysis_water_projects_wetlands_intersections",
        "field_property_intersections": "field_analysis_property_intersections",
        "field_soil_intersections": "field_analysis_soil_intersections",
        # Stage 2 outputs (field-level intersection geometries only - aggregations moved to Stage 4)
        "field_bnbo_intersections": "field_analysis_field_bnbo_intersections",
        # For Stage 3 optimization
        "field_bnbo_water_intersections":
            "field_analysis_field_bnbo_water_intersections",  # For Stage 3 optimization
        "field_wetland_intersections":
            "field_analysis_field_wetland_intersections",  # For Stage 3 optimization
        "field_wetland_water_intersections":
            "field_analysis_field_wetland_water_intersections",  # For Stage 3 optimization
        # Stage 3 outputs (property-level analysis)
        "final_bnbo": "field_analysis_bnbo",
        "final_wetland": "field_analysis_wetland",
        "property_bnbo_intersections":
            "field_analysis_property_bnbo_intersections",  # For two-table architecture
        "property_bnbo_water_intersections":
            "field_analysis_property_bnbo_water_intersections",  # For two-table architecture
        "property_wetland_intersections":
            "field_analysis_property_wetland_intersections",  # For two-table architecture
        "property_wetland_water_intersections":
            "field_analysis_property_wetland_water_intersections",
        # For two-table architecture
        # Stage 4 outputs (consolidation) - Two-Table Architecture
        "consolidated": "field_analysis",  # Legacy single table (deprecated)
        "field_environmental_analysis": "field_environmental",  # Table 1: Field-level (legacy)
        "field_environmental_analysis_fields":
            "field_environmental_analysis_fields",  # Table 1: Field-level (redesigned)
        "property_environmental_analysis":
            "property_environmental",  # Table 2: Property-level (legacy)
        "field_environmental_analysis_properties":
            "field_environmental_analysis_properties",
        # Table 2: Property-level (redesigned)
    }

    # Parquet export settings for optimal performance
    parquet_compression: str = "zstd"  # Best balance of speed/size
    parquet_row_group_size: int = 100000

    model_config = ConfigDict(frozen=True)

    def get_agricultural_fields_dataset(self) -> str:
        """Get agricultural fields dataset name for the configured year."""
        return f"fvm_marker_{self.agricultural_fields_year}"

    def update_outputs_for_year(self) -> Dict[str, str]:
        """Update output dataset names to include year suffix."""
        updated_outputs = {}
        for key, dataset_name in self.stage_outputs.items():
            # Add year suffix to field analysis outputs AND stage0 outputs for isolation
            # Match "field_analysis", "field_environmental_analysis", and "stage0" patterns
            if (
                "field_analysis" in dataset_name or "field_environmental_analysis" in dataset_name
            ) or "stage0" in dataset_name:
                updated_outputs[key] = f"{dataset_name}_{self.agricultural_fields_year}"
            else:
                updated_outputs[key] = dataset_name
        return updated_outputs


# Global config instance
CONFIG = FieldAreaAnalysisConfig()
