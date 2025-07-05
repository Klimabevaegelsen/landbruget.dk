"""
Area validation utilities for H3 PFAS exposure analysis.
"""

import duckdb
from loguru import logger

from ..config import H3SpatialConfig, ValidationResult


class AreaValidator:
    """Validates area calculations and geometric operations."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, config: H3SpatialConfig):
        self.conn = conn
        self.config = config
        self.log = logger.bind(component="AreaValidator")

    def validate_h3_cell_areas(self, results_table: str) -> ValidationResult:
        """Validate H3 cell areas are within expected bounds."""

        area_stats = self.conn.execute(f"""
            SELECT
                MIN(h3_cell_area_ha) as min_area,
                MAX(h3_cell_area_ha) as max_area,
                AVG(h3_cell_area_ha) as avg_area,
                COUNT(*) as total_cells
            FROM {results_table}
        """).fetchone()

        within_bounds = (
            self.config.min_h3_area_ha <= area_stats[0]
            and area_stats[1] <= self.config.max_h3_area_ha
        )
        avg_deviation = (
            abs(area_stats[2] - self.config.theoretical_avg_area_ha)
            / self.config.theoretical_avg_area_ha
            * 100
        )

        return ValidationResult(
            name="H3 Cell Areas",
            passed=within_bounds and avg_deviation < self.config.max_area_deviation_pct,
            metrics={
                "min_area_ha": area_stats[0],
                "max_area_ha": area_stats[1],
                "avg_area_ha": area_stats[2],
                "total_cells": area_stats[3],
                "avg_deviation_pct": avg_deviation,
            },
            message=f"H3 areas: {area_stats[0]:.3f}-{area_stats[1]:.3f} ha (expected: {self.config.min_h3_area_ha}-{self.config.max_h3_area_ha} ha)",
        )

    def validate_intersection_areas(self, results_table: str) -> ValidationResult:
        """Validate intersection area calculations."""

        validation_stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_cells,
                COUNT(CASE WHEN total_intersection_area_ha > h3_cell_area_ha THEN 1 END) as impossible_intersections,
                COUNT(CASE WHEN actual_coverage_ratio < 0 OR actual_coverage_ratio > 1 THEN 1 END) as invalid_coverage,
                MAX(total_intersection_area_ha) as max_intersection,
                MAX(h3_cell_area_ha) as max_h3_area
            FROM {results_table}
        """).fetchone()

        impossible_pct = (
            (validation_stats[1] / validation_stats[0]) * 100 if validation_stats[0] > 0 else 0
        )
        invalid_coverage_pct = (
            (validation_stats[2] / validation_stats[0]) * 100 if validation_stats[0] > 0 else 0
        )

        return ValidationResult(
            name="Intersection Areas",
            passed=validation_stats[1] == 0 and validation_stats[2] == 0,
            metrics={
                "total_cells": validation_stats[0],
                "impossible_intersections": validation_stats[1],
                "invalid_coverage_ratios": validation_stats[2],
                "impossible_intersection_pct": impossible_pct,
                "invalid_coverage_pct": invalid_coverage_pct,
            },
            message=f"Impossible intersections: {impossible_pct:.2f}%, Invalid coverage: {invalid_coverage_pct:.2f}%",
        )
