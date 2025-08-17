"""
Pesticide Unit Sanitization Module

WHAT THIS MODULE DOES:
======================
This module solves a critical data quality problem: pesticide applications often have 
unit mismatches between the application data (DosageUnit) and the BMD concentration 
data (enhed_er), leading to incorrect active ingredient calculations.

THE BUSINESS PROBLEM:
====================
- Pesticide applications report dosage in units like 'kg' or 'liters'
- BMD database has concentrations in units like 'g/kg' or 'g/l'
- Unit mismatches occur in ~0.1% of records (1,610 out of 1.4M)
- These mismatches cause incorrect active ingredient calculations
- Some mismatches are labeling errors, others are legitimate different formulations

THE SOLUTION APPROACH:
=====================
This module implements a statistical validation approach based on our analysis:

1. STATISTICAL VALIDATION: Use standard deviation analysis
   - Calculate dosage/ha statistics for each product by unit type
   - Flag outliers using statistical thresholds (1σ, 2σ, 3σ)

2. AUTO-CORRECTION: Fix obvious unit labeling errors
   - If mismatch dosage is within 2σ of compatible dosage, auto-correct unit
   - Preserve original data with audit trail

3. OUTLIER FLAGGING: Flag extreme outliers for manual review
   - Products with >3σ difference likely have different formulations
   - Flag for manual review rather than auto-correct

4. AUDIT TRAIL: Track all corrections and flags
   - unit_mismatch_detected: Boolean flag
   - unit_corrected: Boolean flag  
   - unit_correction_confidence: Statistical confidence level
   - original_dosage_unit: Preserve original unit
   - statistical_deviation: How many σ away from expected

KEY TECHNICAL DECISIONS:
=======================
- Uses 2σ threshold for auto-correction (95% confidence interval)
- Uses 3σ threshold for outlier flagging (99.7% confidence interval)
- Preserves all original data for auditability
- Uses DuckDB for efficient statistical calculations
- Integrates seamlessly with existing pesticide compliance pipeline

CRITICAL: This implementation uses the exact statistical thresholds from our 
analysis that identified unit errors vs. legitimate formulation differences.
"""

import logging
from typing import Dict, Tuple

import duckdb

from unified_pipeline.util.log_util import Logger

logger = logging.getLogger(__name__)


class PesticideUnitSanitizer:
    """
    Pesticide unit sanitization processor using statistical validation.
    
    This class detects and corrects unit mismatches between pesticide application 
    data and BMD concentration data using statistical analysis of dosage patterns.
    """
        
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize the unit sanitizer.
        
        Args:
            conn: DuckDB connection with pesticide and BMD data loaded
        """
        self.conn = conn
        self.logger = Logger.get_logger()
        
        # Statistical thresholds based on our analysis
        self.AUTO_CORRECTION_THRESHOLD = 2.0  # 2σ - auto-correct within 95% confidence
        self.OUTLIER_FLAG_THRESHOLD = 3.0     # 3σ - flag for manual review
        
        # Unit compatibility mappings
        self.COMPATIBLE_UNITS = {
            ('2', 'g/kg'): True,   # kg dosage with g/kg concentration
            ('4', 'g/l'): True,    # liter dosage with g/l concentration
            ('2', 'g/l'): False,   # kg dosage with g/l concentration - MISMATCH
            ('4', 'g/kg'): False,  # liter dosage with g/kg concentration - MISMATCH
        }
    
    def sanitize_pesticide_units(self, 
                                pesticide_table: str = "pesticide_applications",
                                bmd_table: str = "bmd_data") -> str:
        """
        Main method to sanitize pesticide units using statistical validation.
        
        Args:
            pesticide_table: Name of pesticide applications table
            bmd_table: Name of BMD data table
            
        Returns:
            Name of the sanitized table with unit corrections and flags
        """
        self.logger.info("🧹 Starting pesticide unit sanitization with statistical validation")
        
        # Step 1: Create joined dataset with unit compatibility analysis
        self._create_unit_analysis_dataset(pesticide_table, bmd_table)
        
        # Step 2: Calculate statistical baselines for each product
        self._calculate_statistical_baselines()
        
        # Step 3: Apply statistical validation and corrections
        sanitized_table = self._apply_statistical_corrections()
        
        # Step 4: Generate sanitization report
        self._generate_sanitization_report(sanitized_table)
        
        self.logger.info(f"✅ Unit sanitization completed. Results in table: {sanitized_table}")
        return sanitized_table
    
    def _create_unit_analysis_dataset(self, pesticide_table: str, bmd_table: str) -> None:
        """Create joined dataset with unit compatibility analysis."""
        self.logger.info("📊 Creating unit analysis dataset with BMD joins")
        
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE unit_analysis_joined AS
            SELECT 
                p.*,
                b.enhed_er as bmd_unit,
                b.koncentration_er as bmd_concentration,
                b.produktnavn as bmd_product_name,
                b.aktivstofnavn_e as active_ingredient,
                
                -- Calculate dosage per hectare for statistical analysis
                CASE 
                    WHEN p.area_ha > 0 THEN p.dosage_quantity / p.area_ha
                    ELSE NULL
                END as dosage_per_ha,
                
                -- Determine unit compatibility
                CASE 
                    WHEN (p.dosage_unit = '2' AND b.enhed_er LIKE '%g/kg%') OR 
                         (p.dosage_unit = '4' AND b.enhed_er LIKE '%g/l%') THEN 'compatible'
                    WHEN (p.dosage_unit = '2' AND b.enhed_er LIKE '%g/l%') OR 
                         (p.dosage_unit = '4' AND b.enhed_er LIKE '%g/kg%') THEN 'mismatch'
                    ELSE 'unclear'
                END as unit_compatibility,
                
                -- Flag unit mismatches for processing
                CASE 
                    WHEN (p.dosage_unit = '2' AND b.enhed_er LIKE '%g/l%') OR 
                         (p.dosage_unit = '4' AND b.enhed_er LIKE '%g/kg%') THEN true
                    ELSE false
                END as unit_mismatch_detected
                
            FROM {pesticide_table} p
            LEFT JOIN {bmd_table} b ON p.pesticide_registration_number = b.registration_number
            WHERE b.enhed_er IS NOT NULL 
              AND p.area_ha > 0
              AND p.dosage_quantity > 0
        """)
        
        total_records = self.conn.execute("SELECT COUNT(*) FROM unit_analysis_joined").fetchone()[0]
        mismatch_records = self.conn.execute(
            "SELECT COUNT(*) FROM unit_analysis_joined WHERE unit_mismatch_detected = true"
        ).fetchone()[0]
        
        self.logger.info(f"📊 Unit analysis dataset created: {total_records:,} total records")
        self.logger.info(
            f"⚠️ Unit mismatches detected: {mismatch_records:,} records "
            f"({mismatch_records/total_records*100:.3f}%)"
        )
    
    def _calculate_statistical_baselines(self) -> None:
        """Calculate statistical baselines for each product by unit type."""
        self.logger.info("📈 Calculating statistical baselines for dosage patterns")
        
        self.conn.execute("""
            CREATE OR REPLACE TABLE product_dosage_statistics AS
            SELECT 
                pesticide_registration_number,
                pesticide_name,
                unit_compatibility,
                COUNT(*) as record_count,
                AVG(dosage_per_ha) as mean_dosage_per_ha,
                STDDEV(dosage_per_ha) as stddev_dosage_per_ha,
                MEDIAN(dosage_per_ha) as median_dosage_per_ha,
                MIN(dosage_per_ha) as min_dosage_per_ha,
                MAX(dosage_per_ha) as max_dosage_per_ha
            FROM unit_analysis_joined
            WHERE dosage_per_ha IS NOT NULL
              AND unit_compatibility IN ('compatible', 'mismatch')
            GROUP BY pesticide_registration_number, pesticide_name, unit_compatibility
            HAVING COUNT(*) >= 3  -- Need minimum records for statistical analysis
        """)
        
        # Create products that have both compatible and mismatch records
        self.conn.execute("""
            CREATE OR REPLACE TABLE products_with_mixed_units AS
            SELECT 
                pesticide_registration_number,
                pesticide_name,
                MAX(CASE 
                    WHEN unit_compatibility = 'compatible' 
                    THEN mean_dosage_per_ha 
                    END) as compatible_mean,
                MAX(CASE 
                    WHEN unit_compatibility = 'compatible' 
                    THEN stddev_dosage_per_ha 
                    END) as compatible_stddev,
                MAX(CASE 
                    WHEN unit_compatibility = 'compatible' 
                    THEN record_count 
                    END) as compatible_count,
                MAX(CASE 
                    WHEN unit_compatibility = 'mismatch' 
                    THEN mean_dosage_per_ha 
                    END) as mismatch_mean,
                MAX(CASE 
                    WHEN unit_compatibility = 'mismatch' 
                    THEN stddev_dosage_per_ha 
                    END) as mismatch_stddev,
                MAX(CASE 
                    WHEN unit_compatibility = 'mismatch' 
                    THEN record_count 
                    END) as mismatch_count
            FROM product_dosage_statistics
            GROUP BY pesticide_registration_number, pesticide_name
            HAVING compatible_mean IS NOT NULL 
              AND mismatch_mean IS NOT NULL
              AND compatible_stddev > 0
        """)
        
        mixed_products = self.conn.execute(
            "SELECT COUNT(*) FROM products_with_mixed_units"
        ).fetchone()[0]
        self.logger.info(
            f"📊 Found {mixed_products} products with both compatible and "
            "mismatch units for statistical analysis"
        )
    
    def _apply_statistical_corrections(self) -> str:
        """Apply statistical corrections based on deviation analysis."""
        self.logger.info("🔧 Applying statistical corrections and flagging outliers")
        
        sanitized_table = "pesticide_applications_sanitized"
        
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {sanitized_table} AS
            SELECT 
                j.*,
                
                -- Statistical analysis results
                s.compatible_mean,
                s.compatible_stddev,
                s.mismatch_mean,
                
                -- Calculate standard deviations away from compatible mean
                CASE 
                    WHEN j.unit_mismatch_detected = true 
                         AND s.compatible_stddev > 0 
                         AND s.compatible_mean > 0 
                    THEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev
                    ELSE NULL
                END as statistical_deviation_sigma,
                
                -- Determine correction action based on statistical thresholds
                CASE 
                    WHEN j.unit_mismatch_detected = false THEN 'no_action_needed'
                    WHEN s.compatible_stddev IS NULL THEN 'insufficient_data'
                    WHEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev 
                         <= {self.AUTO_CORRECTION_THRESHOLD} 
                    THEN 'auto_correct'
                    WHEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev 
                         <= {self.OUTLIER_FLAG_THRESHOLD} 
                    THEN 'flag_for_review'
                    ELSE 'extreme_outlier'
                END as correction_action,
                
                -- Apply unit corrections
                CASE 
                    WHEN j.unit_mismatch_detected = true 
                         AND s.compatible_stddev > 0
                         AND ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev 
                             <= {self.AUTO_CORRECTION_THRESHOLD}
                    THEN CASE 
                        WHEN j.dosage_unit = '2' AND j.bmd_unit LIKE '%g/l%' 
                        THEN '4'  -- kg -> liter
                        WHEN j.dosage_unit = '4' AND j.bmd_unit LIKE '%g/kg%' 
                        THEN '2' -- liter -> kg
                        ELSE j.dosage_unit
                    END
                    ELSE j.dosage_unit
                END as corrected_dosage_unit,
                
                -- Track corrections
                CASE 
                    WHEN j.unit_mismatch_detected = true 
                         AND s.compatible_stddev > 0
                         AND ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev 
                             <= {self.AUTO_CORRECTION_THRESHOLD}
                    THEN true
                    ELSE false
                END as unit_corrected,
                
                -- Confidence level for corrections
                CASE 
                    WHEN j.unit_mismatch_detected = true AND s.compatible_stddev > 0
                    THEN CASE 
                        WHEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev <= 1.0 
                        THEN 'high'
                        WHEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev <= 2.0 
                        THEN 'medium'
                        WHEN ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev <= 3.0 
                        THEN 'low'
                        ELSE 'very_low'
                    END
                    ELSE 'not_applicable'
                END as correction_confidence,
                
                -- Preserve original unit for audit trail
                j.dosage_unit as original_dosage_unit,
                
                -- Quality flags
                CASE 
                    WHEN j.unit_mismatch_detected = true 
                         AND s.compatible_stddev > 0
                         AND ABS(j.dosage_per_ha - s.compatible_mean) / s.compatible_stddev 
                             > {self.OUTLIER_FLAG_THRESHOLD}
                    THEN true
                    ELSE false
                END as requires_manual_review,
                
                -- Sanitization metadata
                CURRENT_TIMESTAMP as sanitization_timestamp,
                'statistical_validation_v1' as sanitization_method
                
            FROM unit_analysis_joined j
            LEFT JOIN products_with_mixed_units s ON 
                j.pesticide_registration_number = s.pesticide_registration_number
        """)
        
        # Generate correction statistics
        total_records = self.conn.execute(f"SELECT COUNT(*) FROM {sanitized_table}").fetchone()[0]
        mismatches_detected = self.conn.execute(
            f"SELECT COUNT(*) FROM {sanitized_table} WHERE unit_mismatch_detected = true"
        ).fetchone()[0]
        auto_corrected = self.conn.execute(
            f"SELECT COUNT(*) FROM {sanitized_table} WHERE unit_corrected = true"
        ).fetchone()[0]
        manual_review = self.conn.execute(
            f"SELECT COUNT(*) FROM {sanitized_table} WHERE requires_manual_review = true"
        ).fetchone()[0]
        
        self.logger.info("📊 Sanitization Results:")
        self.logger.info(f"   Total records: {total_records:,}")
        self.logger.info(f"   Unit mismatches detected: {mismatches_detected:,}")
        self.logger.info(f"   Auto-corrected: {auto_corrected:,}")
        self.logger.info(f"   Flagged for manual review: {manual_review:,}")
        
        return sanitized_table
    
    def _generate_sanitization_report(self, sanitized_table: str) -> None:
        """Generate detailed sanitization report."""
        self.logger.info("📋 Generating unit sanitization report")
        
        # Get correction statistics by action type
        correction_stats = self.conn.execute(f"""
            SELECT 
                correction_action,
                COUNT(*) as record_count,
                COUNT(DISTINCT pesticide_registration_number) as unique_products
            FROM {sanitized_table}
            WHERE unit_mismatch_detected = true
            GROUP BY correction_action
            ORDER BY record_count DESC
        """).fetchdf()
        
        # Get top products that were auto-corrected
        auto_corrected_products = self.conn.execute(f"""
            SELECT 
                pesticide_name,
                pesticide_registration_number,
                COUNT(*) as corrections_applied,
                AVG(statistical_deviation_sigma) as avg_deviation_sigma,
                original_dosage_unit,
                corrected_dosage_unit
            FROM {sanitized_table}
            WHERE unit_corrected = true
            GROUP BY pesticide_name, pesticide_registration_number, 
                     original_dosage_unit, corrected_dosage_unit
            ORDER BY corrections_applied DESC
            LIMIT 10
        """).fetchdf()
        
        # Get products requiring manual review
        manual_review_products = self.conn.execute(f"""
            SELECT 
                pesticide_name,
                pesticide_registration_number,
                COUNT(*) as outlier_records,
                AVG(statistical_deviation_sigma) as avg_deviation_sigma,
                MAX(statistical_deviation_sigma) as max_deviation_sigma
            FROM {sanitized_table}
            WHERE requires_manual_review = true
            GROUP BY pesticide_name, pesticide_registration_number
            ORDER BY avg_deviation_sigma DESC
            LIMIT 10
        """).fetchdf()
        
        self.logger.info("📊 Unit Sanitization Summary:")
        self.logger.info("=" * 50)
        
        for _, row in correction_stats.iterrows():
            self.logger.info(
                f"   {row['correction_action']}: {row['record_count']:,} records "
                f"({row['unique_products']} products)"
            )
        
        if len(auto_corrected_products) > 0:
            self.logger.info("\n🔧 Top Auto-Corrected Products:")
            for _, row in auto_corrected_products.head(5).iterrows():
                self.logger.info(
                    f"   {row['pesticide_name']} (#{row['pesticide_registration_number']}): "
                    f"{row['corrections_applied']} corrections, "
                    f"{row['original_dosage_unit']} → {row['corrected_dosage_unit']}, "
                    f"avg {row['avg_deviation_sigma']:.2f}σ"
                )
        
        if len(manual_review_products) > 0:
            self.logger.info("\n⚠️ Top Products Requiring Manual Review:")
            for _, row in manual_review_products.head(5).iterrows():
                self.logger.info(
                    f"   {row['pesticide_name']} (#{row['pesticide_registration_number']}): "
                    f"{row['outlier_records']} outliers, "
                    f"avg {row['avg_deviation_sigma']:.2f}σ (max {row['max_deviation_sigma']:.2f}σ)"
                )
    
    def get_sanitization_summary(self, sanitized_table: str) -> Dict:
        """
        Get summary statistics for the sanitization process.
        
        Args:
            sanitized_table: Name of the sanitized table
            
        Returns:
            Dictionary with sanitization statistics
        """
        stats = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN unit_mismatch_detected THEN 1 END) as mismatches_detected,
                COUNT(CASE WHEN unit_corrected THEN 1 END) as auto_corrected,
                COUNT(CASE WHEN requires_manual_review THEN 1 END) as manual_review_required,
                COUNT(CASE 
                    WHEN correction_action = 'insufficient_data' 
                    THEN 1 
                    END) as insufficient_data,
                COUNT(DISTINCT CASE 
                    WHEN unit_corrected 
                    THEN pesticide_registration_number 
                    END) as products_corrected,
                COUNT(DISTINCT CASE 
                    WHEN requires_manual_review 
                    THEN pesticide_registration_number 
                    END) as products_needing_review
            FROM {sanitized_table}
        """).fetchone()
        
        return {
            "total_records": stats[0],
            "mismatches_detected": stats[1],
            "auto_corrected": stats[2],
            "manual_review_required": stats[3],
            "insufficient_data": stats[4],
            "products_corrected": stats[5],
            "products_needing_review": stats[6],
            "correction_rate": stats[2] / stats[1] * 100 if stats[1] > 0 else 0,
            "manual_review_rate": stats[3] / stats[1] * 100 if stats[1] > 0 else 0
        }


def sanitize_pesticide_units(conn: duckdb.DuckDBPyConnection,
                           pesticide_table: str = "pesticide_applications",
                           bmd_table: str = "bmd_data") -> Tuple[str, Dict]:
    """
    Convenience function to sanitize pesticide units.
    
    Args:
        conn: DuckDB connection
        pesticide_table: Name of pesticide applications table
        bmd_table: Name of BMD data table
        
    Returns:
        Tuple of (sanitized_table_name, summary_statistics)
    """
    sanitizer = PesticideUnitSanitizer(conn)
    sanitized_table = sanitizer.sanitize_pesticide_units(pesticide_table, bmd_table)
    summary = sanitizer.get_sanitization_summary(sanitized_table)
    return sanitized_table, summary
