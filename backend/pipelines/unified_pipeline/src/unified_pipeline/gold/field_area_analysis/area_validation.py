"""Area validation utilities for Field Area Analysis pipeline."""

import time
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

from unified_pipeline.util.log_util import Logger


@dataclass
class AreaValidationResult:
    """Result of area validation check."""
    is_valid: bool
    total_area_before: float
    total_area_after: float
    area_difference: float
    area_difference_pct: float
    tolerance_pct: float
    field_count_before: int
    field_count_after: int
    validation_message: str
    details: Dict[str, Any]


class FieldAreaValidator:
    """Validates that field areas are preserved across pipeline stages."""
    
    def __init__(self, 
                 conn,
                 log: Optional[Logger] = None,
                 tolerance_pct: float = 1.0):
        """
        Initialize the area validator.
        
        Args:
            conn: DuckDB connection
            log: Logger instance
            tolerance_pct: Maximum acceptable area difference percentage (default: 1.0%)
        """
        self.conn = conn
        self.log = log or Logger.get_logger()
        self.tolerance_pct = tolerance_pct
    
    def validate_field_areas(self, 
                           before_table: str,
                           after_table: str,
                           stage_name: str,
                           field_area_column: str = "field_area_m2") -> AreaValidationResult:
        """
        Validate that field areas are preserved between before and after tables.
        
        Args:
            before_table: Name of table before processing
            after_table: Name of table after processing  
            stage_name: Name of the stage being validated
            field_area_column: Column name containing field area
            
        Returns:
            AreaValidationResult with validation outcome
        """
        validation_start = time.time()
        self.log.info(f"🔍 Starting area validation for {stage_name}...")
        
        try:
            # Get total areas and field counts from before table
            before_stats = self._get_table_area_stats(before_table, field_area_column)
            
            # Get total areas and field counts from after table
            after_stats = self._get_table_area_stats(after_table, field_area_column)
            
            # Calculate differences
            area_difference = after_stats["total_area"] - before_stats["total_area"]
            area_difference_pct = (area_difference / before_stats["total_area"]) * 100 if before_stats["total_area"] > 0 else 0
            
            # Determine if validation passes
            is_valid = abs(area_difference_pct) <= self.tolerance_pct
            
            # Create validation message
            if is_valid:
                message = f"✅ Area validation PASSED for {stage_name}: {area_difference_pct:+.2f}% change (within {self.tolerance_pct}% tolerance)"
            else:
                message = f"❌ Area validation FAILED for {stage_name}: {area_difference_pct:+.2f}% change (exceeds {self.tolerance_pct}% tolerance)"
            
            # Detailed validation info
            validation_time = time.time() - validation_start
            details = {
                "stage_name": stage_name,
                "validation_time_seconds": validation_time,
                "before_stats": before_stats,
                "after_stats": after_stats,
                "tolerance_check": {
                    "absolute_difference_pct": abs(area_difference_pct),
                    "tolerance_pct": self.tolerance_pct,
                    "passes_tolerance": is_valid
                }
            }
            
            result = AreaValidationResult(
                is_valid=is_valid,
                total_area_before=before_stats["total_area"],
                total_area_after=after_stats["total_area"],
                area_difference=area_difference,
                area_difference_pct=area_difference_pct,
                tolerance_pct=self.tolerance_pct,
                field_count_before=before_stats["field_count"],
                field_count_after=after_stats["field_count"],
                validation_message=message,
                details=details
            )
            
            self.log.info(message)
            self.log.info(f"📊 Before: {before_stats['field_count']:,} fields, {before_stats['total_area']:,.0f} m²")
            self.log.info(f"📊 After:  {after_stats['field_count']:,} fields, {after_stats['total_area']:,.0f} m²")
            self.log.info(f"📊 Difference: {area_difference:+,.0f} m² ({area_difference_pct:+.3f}%)")
            self.log.info(f"⏱️ Area validation completed in {validation_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_message = f"❌ Area validation ERROR for {stage_name}: {str(e)}"
            self.log.error(error_message)
            
            return AreaValidationResult(
                is_valid=False,
                total_area_before=0,
                total_area_after=0,
                area_difference=0,
                area_difference_pct=0,
                tolerance_pct=self.tolerance_pct,
                field_count_before=0,
                field_count_after=0,
                validation_message=error_message,
                details={"error": str(e), "stage_name": stage_name}
            )
    
    def _get_table_area_stats(self, table_name: str, field_area_column: str) -> Dict[str, Any]:
        """Get area statistics from a table."""
        
        # Check if table exists
        table_exists = self.conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        
        if table_exists == 0:
            raise ValueError(f"Table {table_name} does not exist")
        
        # Check if field area column exists
        column_exists = self.conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND column_name = '{field_area_column}'
        """).fetchone()[0]
        
        if column_exists == 0:
            raise ValueError(f"Column {field_area_column} does not exist in table {table_name}")
        
        # First check if we have fragments (multiple records per field)
        fragment_check = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT field_uuid) as unique_fields
            FROM {table_name}
            WHERE {field_area_column} IS NOT NULL 
            AND {field_area_column} > 0
        """).fetchone()
        
        has_fragments = fragment_check[0] > fragment_check[1]
        
        if has_fragments:
            self.log.info(f"🔍 Fragment mode detected for {table_name}: {fragment_check[0]} records, {fragment_check[1]} unique fields")
        
        # Get statistics - handle fragments correctly to avoid double-counting
        if has_fragments:
            # FRAGMENT MODE: Use DISTINCT to avoid double-counting field areas across fragments
            stats = self.conn.execute(f"""
                SELECT 
                    COUNT(*) as field_count,
                    COALESCE(SUM({field_area_column}), 0) as total_area_with_fragments,
                    COALESCE(AVG({field_area_column}), 0) as avg_area,
                    COALESCE(MIN({field_area_column}), 0) as min_area,
                    COALESCE(MAX({field_area_column}), 0) as max_area,
                    COUNT(DISTINCT field_uuid) as unique_fields,
                    -- Correct total area using distinct fields to avoid double-counting
                    (SELECT COALESCE(SUM({field_area_column}), 0) 
                     FROM (SELECT DISTINCT field_uuid, {field_area_column} 
                           FROM {table_name} 
                           WHERE {field_area_column} IS NOT NULL AND {field_area_column} > 0)
                    ) as total_area_distinct
                FROM {table_name}
                WHERE {field_area_column} IS NOT NULL 
                AND {field_area_column} > 0
            """).fetchone()
            
            # Use the distinct area calculation for validation
            total_area = stats[6]  # total_area_distinct
            
        else:
            # AGGREGATED MODE: Direct sum is correct (no fragments)
            stats = self.conn.execute(f"""
                SELECT 
                    COUNT(*) as field_count,
                    COALESCE(SUM({field_area_column}), 0) as total_area,
                    COALESCE(AVG({field_area_column}), 0) as avg_area,
                    COALESCE(MIN({field_area_column}), 0) as min_area,
                    COALESCE(MAX({field_area_column}), 0) as max_area,
                    COUNT(DISTINCT field_uuid) as unique_fields
                FROM {table_name}
                WHERE {field_area_column} IS NOT NULL 
                AND {field_area_column} > 0
            """).fetchone()
            
            total_area = stats[1]  # total_area
        
        return {
            "field_count": stats[0],
            "total_area": total_area,  # Use the correct total_area (fragments-aware)
            "avg_area": stats[2],
            "min_area": stats[3],
            "max_area": stats[4],
            "unique_fields": stats[5] if not has_fragments else stats[5]
        }
    
    def validate_stage_with_input_reference(self,
                                          output_table: str,
                                          stage_name: str,
                                          reference_total_area: float,
                                          reference_field_count: int,
                                          field_area_column: str = "field_area_m2") -> AreaValidationResult:
        """
        Validate stage output against a reference area (useful when input table is temporary).
        
        Args:
            output_table: Name of output table to validate
            stage_name: Name of the stage being validated
            reference_total_area: Expected total area from input
            reference_field_count: Expected field count from input
            field_area_column: Column name containing field area
            
        Returns:
            AreaValidationResult with validation outcome
        """
        validation_start = time.time()
        self.log.info(f"🔍 Starting area validation for {stage_name} against reference...")
        
        try:
            # Get stats from output table
            output_stats = self._get_table_area_stats(output_table, field_area_column)
            
            # Calculate differences against reference
            area_difference = output_stats["total_area"] - reference_total_area
            area_difference_pct = (area_difference / reference_total_area) * 100 if reference_total_area > 0 else 0
            
            # Determine if validation passes
            is_valid = abs(area_difference_pct) <= self.tolerance_pct
            
            # Create validation message
            if is_valid:
                message = f"✅ Area validation PASSED for {stage_name}: {area_difference_pct:+.2f}% change (within {self.tolerance_pct}% tolerance)"
            else:
                message = f"❌ Area validation FAILED for {stage_name}: {area_difference_pct:+.2f}% change (exceeds {self.tolerance_pct}% tolerance)"
            
            validation_time = time.time() - validation_start
            details = {
                "stage_name": stage_name,
                "validation_time_seconds": validation_time,
                "reference_stats": {
                    "total_area": reference_total_area,
                    "field_count": reference_field_count
                },
                "output_stats": output_stats,
                "tolerance_check": {
                    "absolute_difference_pct": abs(area_difference_pct),
                    "tolerance_pct": self.tolerance_pct,
                    "passes_tolerance": is_valid
                }
            }
            
            result = AreaValidationResult(
                is_valid=is_valid,
                total_area_before=reference_total_area,
                total_area_after=output_stats["total_area"],
                area_difference=area_difference,
                area_difference_pct=area_difference_pct,
                tolerance_pct=self.tolerance_pct,
                field_count_before=reference_field_count,
                field_count_after=output_stats["field_count"],
                validation_message=message,
                details=details
            )
            
            self.log.info(message)
            self.log.info(f"📊 Reference: {reference_field_count:,} fields, {reference_total_area:,.0f} m²")
            self.log.info(f"📊 Output:    {output_stats['field_count']:,} fields, {output_stats['total_area']:,.0f} m²")
            self.log.info(f"📊 Difference: {area_difference:+,.0f} m² ({area_difference_pct:+.3f}%)")
            self.log.info(f"⏱️ Area validation completed in {validation_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_message = f"❌ Area validation ERROR for {stage_name}: {str(e)}"
            self.log.error(error_message)
            
            return AreaValidationResult(
                is_valid=False,
                total_area_before=reference_total_area,
                total_area_after=0,
                area_difference=0,
                area_difference_pct=0,
                tolerance_pct=self.tolerance_pct,
                field_count_before=reference_field_count,
                field_count_after=0,
                validation_message=error_message,
                details={"error": str(e), "stage_name": stage_name}
            )


    def validate_area_hierarchy(self, 
                               table_name: str,
                               stage_name: str,
                               hierarchy_validations: List[Tuple[str, str, str]]) -> AreaValidationResult:
        """
        Validate area hierarchy constraints: child areas must be ≤ parent areas.
        
        Args:
            table_name: Name of table to validate
            stage_name: Name of the stage being validated
            hierarchy_validations: List of (child_column, parent_column, description) tuples
            
        Returns:
            AreaValidationResult with validation outcome
        """
        validation_start = time.time()
        self.log.info(f"🔍 Starting area hierarchy validation for {stage_name}...")
        
        violations = []
        total_violations = 0
        
        try:
            for child_col, parent_col, description in hierarchy_validations:
                self.log.info(f"  Checking: {description}")
                
                violation_query = f"""
                    SELECT 
                        COUNT(*) as violation_count,
                        MAX({child_col} / NULLIF({parent_col}, 0)) as max_ratio,
                        AVG({child_col} / NULLIF({parent_col}, 0)) as avg_ratio
                    FROM {table_name}
                    WHERE {parent_col} > 0 
                        AND {child_col} > {parent_col} * (1 + {self.tolerance_pct}/100.0)
                """
                
                result = self.conn.execute(violation_query).fetchone()
                violation_count = result[0]
                max_ratio = result[1] or 0
                avg_ratio = result[2] or 0
                
                if violation_count > 0:
                    violation_info = {
                        "description": description,
                        "child_column": child_col,
                        "parent_column": parent_col,
                        "violation_count": violation_count,
                        "max_ratio": max_ratio,
                        "avg_ratio": avg_ratio
                    }
                    violations.append(violation_info)
                    total_violations += violation_count
                    
                    self.log.error(f"    ❌ {violation_count:,} violations found (max ratio: {max_ratio:.1f}x)")
                else:
                    self.log.info(f"    ✅ No violations found")
            
            # Overall validation result
            is_valid = total_violations == 0
            
            if is_valid:
                message = f"✅ Area hierarchy validation PASSED for {stage_name}: All hierarchy constraints satisfied"
            else:
                message = f"❌ Area hierarchy validation FAILED for {stage_name}: {total_violations:,} total violations found"
            
            validation_time = time.time() - validation_start
            details = {
                "stage_name": stage_name,
                "validation_time_seconds": validation_time,
                "hierarchy_checks": hierarchy_validations,
                "violations": violations,
                "total_violations": total_violations
            }
            
            result = AreaValidationResult(
                is_valid=is_valid,
                total_area_before=0,  # Not applicable for hierarchy validation
                total_area_after=0,   # Not applicable for hierarchy validation
                area_difference=0,    # Not applicable for hierarchy validation
                area_difference_pct=0, # Not applicable for hierarchy validation
                tolerance_pct=self.tolerance_pct,
                field_count_before=0, # Not applicable for hierarchy validation
                field_count_after=0,  # Not applicable for hierarchy validation
                validation_message=message,
                details=details
            )
            
            self.log.info(message)
            self.log.info(f"⏱️ Hierarchy validation completed in {validation_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_message = f"❌ Area hierarchy validation ERROR for {stage_name}: {str(e)}"
            self.log.error(error_message)
            
            return AreaValidationResult(
                is_valid=False,
                total_area_before=0, total_area_after=0,
                area_difference=0, area_difference_pct=0,
                tolerance_pct=self.tolerance_pct,
                field_count_before=0, field_count_after=0,
                validation_message=error_message,
                details={"error": str(e), "stage_name": stage_name}
            )

    def validate_fragment_sum_consistency(self,
                                        detail_table: str,
                                        aggregate_table: str,
                                        stage_name: str,
                                        group_columns: List[str],
                                        detail_area_column: str,
                                        aggregate_area_column: str) -> AreaValidationResult:
        """
        Validate that sum of fragment areas equals aggregated totals.
        
        Args:
            detail_table: Table with detailed fragments
            aggregate_table: Table with aggregated totals
            stage_name: Name of the stage being validated
            group_columns: Columns to group by (e.g., ['field_uuid', 'year'])
            detail_area_column: Area column in detail table
            aggregate_area_column: Area column in aggregate table
            
        Returns:
            AreaValidationResult with validation outcome
        """
        validation_start = time.time()
        self.log.info(f"🔍 Starting fragment sum consistency validation for {stage_name}...")
        
        try:
            # Properly prefix group columns with table aliases
            group_by_clause_detail = ", ".join([f"d.{col}" for col in group_columns])
            group_by_clause_plain = ", ".join(group_columns)
            join_conditions = " AND ".join([f"d.{col} = a.{col}" for col in group_columns])
            
            validation_query = f"""
                SELECT 
                    COUNT(*) as total_groups,
                    COUNT(*) FILTER (WHERE ABS(detail_sum - aggregate_total) > aggregate_total * {self.tolerance_pct}/100.0) as inconsistent_groups,
                    MAX(ABS(detail_sum - aggregate_total) / NULLIF(aggregate_total, 0)) as max_difference_ratio,
                    AVG(ABS(detail_sum - aggregate_total) / NULLIF(aggregate_total, 0)) as avg_difference_ratio
                FROM (
                    SELECT 
                        {group_by_clause_detail},
                        SUM(d.{detail_area_column}) as detail_sum,
                        FIRST(a.{aggregate_area_column}) as aggregate_total
                    FROM {detail_table} d
                    JOIN {aggregate_table} a ON {join_conditions}
                    WHERE d.{detail_area_column} > 0 AND a.{aggregate_area_column} > 0
                    GROUP BY {group_by_clause_detail}
                ) consistency_check
            """
            
            result = self.conn.execute(validation_query).fetchone()
            total_groups = result[0]
            inconsistent_groups = result[1]
            max_difference_ratio = result[2] or 0
            avg_difference_ratio = result[3] or 0
            
            is_valid = inconsistent_groups == 0
            
            if is_valid:
                message = f"✅ Fragment sum consistency validation PASSED for {stage_name}: All {total_groups:,} groups consistent"
            else:
                message = f"❌ Fragment sum consistency validation FAILED for {stage_name}: {inconsistent_groups:,}/{total_groups:,} groups inconsistent"
            
            validation_time = time.time() - validation_start
            details = {
                "stage_name": stage_name,
                "validation_time_seconds": validation_time,
                "detail_table": detail_table,
                "aggregate_table": aggregate_table,
                "group_columns": group_columns,
                "total_groups": total_groups,
                "inconsistent_groups": inconsistent_groups,
                "max_difference_ratio": max_difference_ratio,
                "avg_difference_ratio": avg_difference_ratio,
                "tolerance_pct": self.tolerance_pct
            }
            
            result = AreaValidationResult(
                is_valid=is_valid,
                total_area_before=0,  # Not directly applicable
                total_area_after=0,   # Not directly applicable
                area_difference=0,    # Not directly applicable
                area_difference_pct=avg_difference_ratio * 100,
                tolerance_pct=self.tolerance_pct,
                field_count_before=total_groups,
                field_count_after=total_groups - inconsistent_groups,
                validation_message=message,
                details=details
            )
            
            self.log.info(message)
            if inconsistent_groups > 0:
                self.log.error(f"📊 Max difference ratio: {max_difference_ratio:.3f}x")
                self.log.error(f"📊 Avg difference ratio: {avg_difference_ratio:.3f}x")
            self.log.info(f"⏱️ Fragment consistency validation completed in {validation_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_message = f"❌ Fragment sum consistency validation ERROR for {stage_name}: {str(e)}"
            self.log.error(error_message)
            
            return AreaValidationResult(
                is_valid=False,
                total_area_before=0, total_area_after=0,
                area_difference=0, area_difference_pct=0,
                tolerance_pct=self.tolerance_pct,
                field_count_before=0, field_count_after=0,
                validation_message=error_message,
                details={"error": str(e), "stage_name": stage_name}
            )

    def run_comprehensive_stage_validation(self,
                                         table_name: str,
                                         stage_name: str,
                                         validation_type: str = "wetland") -> Dict[str, AreaValidationResult]:
        """
        Run comprehensive validation suite for a stage.
        
        Args:
            table_name: Name of table to validate
            stage_name: Name of the stage being validated
            validation_type: Type of validation ("wetland" or "bnbo")
            
        Returns:
            Dictionary of validation results by validation name
        """
        self.log.info(f"🔍 COMPREHENSIVE VALIDATION SUITE for {stage_name} ({validation_type})")
        results = {}
        
        # Define hierarchy validations based on type
        if validation_type == "wetland":
            hierarchy_checks = [
                ("field_wetland_water_covered_m2", "field_wetland_total_m2", "Water covered area ≤ Total wetland area"),
                ("field_wetland_total_m2", "field_area_m2", "Total wetland area ≤ Field area")
            ]
        elif validation_type == "bnbo":
            hierarchy_checks = [
                ("field_bnbo_water_covered_m2", "field_bnbo_total_m2", "Water covered area ≤ Total BNBO area"),
                ("field_bnbo_total_m2", "field_area_m2", "Total BNBO area ≤ Field area")
            ]
        else:
            hierarchy_checks = []
        
        # Run hierarchy validation
        if hierarchy_checks:
            results["hierarchy"] = self.validate_area_hierarchy(
                table_name, stage_name, hierarchy_checks
            )
        
        return results


class ValidationException(Exception):
    """Exception raised when area validation fails."""
    
    def __init__(self, validation_result: AreaValidationResult):
        self.validation_result = validation_result
        super().__init__(validation_result.validation_message)