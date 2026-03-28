"""Backward-compatible re-export. Import from common.geometry_validator directly."""

from common.geometry_validator import (
    validate_and_normalize_to_utm,
    validate_and_transform_geometries_duckdb,
    verify_spatial_join_usage,
)

__all__ = [
    "validate_and_normalize_to_utm",
    "validate_and_transform_geometries_duckdb",
    "verify_spatial_join_usage",
]
