"""
Field area validation utilities.

Standalone area validation that doesn't depend on the full unified_pipeline import chain.
Used for pre-merge validation tests.
"""

from dataclasses import dataclass
from typing import Optional

import duckdb


@dataclass
class AreaValidationResult:
    """Result of area validation."""

    is_valid: bool
    stage_name: str
    before_area: float
    after_area: float
    area_difference: float
    area_difference_pct: float
    tolerance_pct: float
    validation_message: str

    def __str__(self) -> str:
        return self.validation_message


class FieldAreaValidator:
    """
    Validates field area calculations between processing stages.

    Ensures that total area doesn't change unexpectedly during transformations.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        tolerance_pct: float = 1.0,
        area_column: str = "field_area_m2",
        id_column: str = "field_uuid",
    ):
        """
        Initialize the validator.

        Args:
            conn: DuckDB connection
            tolerance_pct: Maximum allowed percentage difference (default 1%)
            area_column: Name of the area column
            id_column: Name of the field identifier column
        """
        self.conn = conn
        self.tolerance_pct = tolerance_pct
        self.area_column = area_column
        self.id_column = id_column

    def validate_field_areas(
        self,
        before_table: str,
        after_table: str,
        stage_name: str,
        use_distinct: bool = True,
    ) -> AreaValidationResult:
        """
        Validate that total field area is preserved between stages.

        Args:
            before_table: Name of the table before transformation
            after_table: Name of the table after transformation
            stage_name: Description of the transformation stage
            use_distinct: Whether to use DISTINCT on field_uuid (prevents double-counting)

        Returns:
            AreaValidationResult with validation details
        """
        # Calculate total area from before table
        if use_distinct:
            before_query = f"""
                SELECT COALESCE(SUM({self.area_column}), 0) as total_area
                FROM (
                    SELECT DISTINCT {self.id_column}, {self.area_column}
                    FROM {before_table}
                    WHERE {self.area_column} IS NOT NULL
                )
            """
            after_query = f"""
                SELECT COALESCE(SUM({self.area_column}), 0) as total_area
                FROM (
                    SELECT DISTINCT {self.id_column}, {self.area_column}
                    FROM {after_table}
                    WHERE {self.area_column} IS NOT NULL
                )
            """
        else:
            before_query = f"""
                SELECT COALESCE(SUM({self.area_column}), 0) as total_area
                FROM {before_table}
                WHERE {self.area_column} IS NOT NULL
            """
            after_query = f"""
                SELECT COALESCE(SUM({self.area_column}), 0) as total_area
                FROM {after_table}
                WHERE {self.area_column} IS NOT NULL
            """

        before_area = self.conn.execute(before_query).fetchone()[0]
        after_area = self.conn.execute(after_query).fetchone()[0]

        # Calculate difference
        if before_area == 0:
            if after_area == 0:
                diff_pct = 0.0
            else:
                diff_pct = 100.0  # From 0 to non-zero is 100% change
        else:
            diff_pct = ((after_area - before_area) / before_area) * 100

        area_diff = after_area - before_area
        is_valid = abs(diff_pct) <= self.tolerance_pct

        # Generate message
        if is_valid:
            message = f"✅ {stage_name}: Area validated within {self.tolerance_pct}% tolerance (diff: {diff_pct:.4f}%)"
        else:
            message = (
                f"❌ {stage_name}: Area change {diff_pct:.2f}% exceeds {self.tolerance_pct}% tolerance. "
                f"Before: {before_area:,.0f} m², After: {after_area:,.0f} m²"
            )

        return AreaValidationResult(
            is_valid=is_valid,
            stage_name=stage_name,
            before_area=before_area,
            after_area=after_area,
            area_difference=area_diff,
            area_difference_pct=diff_pct,
            tolerance_pct=self.tolerance_pct,
            validation_message=message,
        )

    def validate_hierarchy(
        self,
        table_name: str,
        parent_column: str,
        child_column: str,
        tolerance_pct: float = 1.0,
    ) -> AreaValidationResult:
        """
        Validate that child areas don't exceed parent areas.

        Args:
            table_name: Name of the table to validate
            parent_column: Column containing parent area (e.g., 'field_area_m2')
            child_column: Column containing child area (e.g., 'wetland_area_m2')
            tolerance_pct: Tolerance for small overflows (default 1%)

        Returns:
            AreaValidationResult with validation details
        """
        query = f"""
            SELECT COUNT(*) as violations
            FROM {table_name}
            WHERE {child_column} > {parent_column} * (1 + {tolerance_pct / 100})
              AND {child_column} IS NOT NULL
              AND {parent_column} IS NOT NULL
        """

        violations = self.conn.execute(query).fetchone()[0]
        is_valid = violations == 0

        if is_valid:
            message = f"✅ Hierarchy validated: {child_column} ≤ {parent_column}"
        else:
            message = f"❌ {violations} records where {child_column} > {parent_column}"

        return AreaValidationResult(
            is_valid=is_valid,
            stage_name=f"Hierarchy: {child_column} ≤ {parent_column}",
            before_area=0,
            after_area=0,
            area_difference=0,
            area_difference_pct=0,
            tolerance_pct=tolerance_pct,
            validation_message=message,
        )
