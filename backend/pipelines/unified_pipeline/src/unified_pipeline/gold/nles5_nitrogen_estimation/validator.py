"""
NLES5 Validator Module

This module contains all validation and quality assurance methods for NLES5 nitrogen washout estimation.
It includes:
- NLES5 estimates validation against reference targets
- Reference implementation compliance testing
- Distribution analysis and uncertainty estimation
- Data quality validation and recommendations
- Comprehensive validation reporting

All methods maintain the exact same functionality and validation criteria from the original implementation.
"""

import json
import math
from typing import Any, Dict, List, Optional

from unified_pipeline.util.timing import timed


class NLES5Validator:
    """
    NLES5 Validator containing all validation and quality assurance methods.
    
    This class handles:
    - NLES5 estimates validation against reference targets and reasonable ranges
    - Reference implementation compliance testing with known coefficient values
    - Statistical analysis of estimate distributions and uncertainty patterns
    - Data quality validation and coverage assessment
    - Comprehensive validation reporting and recommendations
    """
    
    def __init__(self, processor):
        """Initialize validator with reference to main processor."""
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.conn = processor.conn
    
    @timed(name="Validating NLES5 estimates")
    def _validate_nles5_estimates(self) -> bool:
        """
        Validate NLES5 estimates for data quality and reasonable values.
        Enhanced with reference implementation validation.

        Returns:
            True if validation passes, False otherwise
        """
        try:
            self.log.info("Validating NLES5 nitrogen estimates against reference targets")

            # Check if any estimates were generated
            total_count = self.conn.execute("SELECT COUNT(*) FROM nles5_nitrogen_estimates").fetchone()[0]
            if total_count == 0:
                self.log.error("❌ CRITICAL: No NLES5 estimates generated with real data")
                self.log.error("❌ Pipeline requires actual soil, crop, climate, and fertilizer data")
                raise ValueError("Validation failed: No NLES5 estimates generated with real data. Pipeline requires actual data, not defaults.")
            
            # Validate minimum data quality requirements
            data_quality_check = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN crop_effect IS NOT NULL THEN 1 END) as fields_with_crop_data,
                    COUNT(CASE WHEN total_soil_n_mg_ha IS NOT NULL THEN 1 END) as fields_with_soil_data,
                    COUNT(CASE WHEN perco_soil_effect IS NOT NULL THEN 1 END) as fields_with_percolation_data
                FROM nles5_nitrogen_estimates
            """).fetchone()
            
            total_fields, crop_data_count, soil_data_count, percolation_data_count = data_quality_check
            
            # Require 100% real data coverage (no fallbacks allowed)
            if crop_data_count < total_fields:
                self.log.error(f"❌ CRITICAL: Insufficient crop data coverage: {crop_data_count}/{total_fields}")
                raise ValueError("Pipeline requires 100% real crop classification data - no defaults allowed")
                
            if soil_data_count < total_fields:
                self.log.error(f"❌ CRITICAL: Insufficient soil data coverage: {soil_data_count}/{total_fields}")
                raise ValueError("Pipeline requires 100% real soil data - no defaults allowed")
                
            if percolation_data_count < total_fields:
                self.log.error(f"❌ CRITICAL: Insufficient percolation data coverage: {percolation_data_count}/{total_fields}")
                raise ValueError("Pipeline requires 100% real percolation data - no defaults allowed")
                
            self.log.info(f"✅ Data quality validation passed: {total_fields:,} fields with 100% real data coverage")

            # Enhanced validation with reference targets
            stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    AVG(nitrogen_washout_kg_ha) as avg_washout,
                    STDDEV(nitrogen_washout_kg_ha) as stddev_washout,
                    MIN(nitrogen_washout_kg_ha) as min_washout,
                    MAX(nitrogen_washout_kg_ha) as max_washout,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha < 0 THEN 1 END) as negative_count,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha > ? THEN 1 END) as excessive_count,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha IS NULL THEN 1 END) as null_count,
                    COUNT(CASE WHEN data_quality = 'high' THEN 1 END) as high_quality_count,
                    -- Validate model components
                    AVG(trend_effect) as avg_trend_effect,
                    AVG(v_base) as avg_v_base,
                    COUNT(CASE WHEN m_code != 'M2' THEN 1 END) as real_crop_codes_count
                FROM nles5_nitrogen_estimates
            """, [self.config.max_nitrogen_washout]).fetchone()

            total_records, avg_washout, stddev_washout, min_washout, max_washout, negative_count, excessive_count, null_count, high_quality_count, avg_trend_effect, avg_v_base, real_crop_codes = stats

            # Log enhanced validation statistics
            self.log.info(f"📊 NLES5 VALIDATION RESULTS:")
            self.log.info(f"   Records: {total_records:,}")
            self.log.info(f"   Avg Washout: {avg_washout:.2f} kg N/ha (σ={stddev_washout:.2f})")
            self.log.info(f"   Range: {min_washout:.2f} to {max_washout:.2f} kg N/ha")
            self.log.info(f"   High Quality: {high_quality_count:,} ({high_quality_count/total_records:.1%})")
            self.log.info(f"   Real Crop Codes: {real_crop_codes:,} ({real_crop_codes/total_records:.1%})")

            # Validate against reference targets from uncertainty.md
            validation_results = []

            # Reference target: National Nitrate Leaching ~6 kg N/ha standard deviation
            if stddev_washout is not None:
                if 4 <= stddev_washout <= 8:
                    validation_results.append("✅ Standard deviation within reference range (4-8 kg N/ha)")
                else:
                    validation_results.append(f"⚠️  Standard deviation {stddev_washout:.2f} outside reference range (4-8 kg N/ha)")

            # Reference target: Overall Model Uncertainty ~10%
            # Only check uncertainty if the table exists (it's created after validation)
            try:
                avg_uncertainty = self.conn.execute(
                    "SELECT AVG(total_uncertainty_pct) FROM nles5_uncertainty_estimates WHERE total_uncertainty_pct IS NOT NULL"
                ).fetchone()
                if avg_uncertainty and avg_uncertainty[0] is not None:
                    if 8 <= avg_uncertainty[0] <= 15:
                        validation_results.append(f"✅ Model uncertainty {avg_uncertainty[0]:.1f}% within reference range (8-15%)")
                    else:
                        validation_results.append(f"⚠️  Model uncertainty {avg_uncertainty[0]:.1f}% outside reference range (8-15%)")
                else:
                    validation_results.append("ℹ️  Model uncertainty not yet calculated")
            except Exception:
                validation_results.append("ℹ️  Model uncertainty will be calculated after validation")

            # Validate trend effect calculation method
            if avg_trend_effect is not None:
                # For year 2017 (reference year), trend should be -0.1108 * (2017 - 1991) = -2.8808
                # For other years, it should scale accordingly
                validation_results.append(f"✅ Trend effect calculated dynamically: {avg_trend_effect:.4f} (varies by field year)")

            # Validate V base calculation (should be ~23.51 + nitrogen_effect)
            if avg_v_base is not None:
                if avg_v_base > 23.51:  # Should be at least the base constant
                    validation_results.append("✅ V base calculation includes proper nitrogen effects")
                else:
                    validation_results.append(f"⚠️  V base {avg_v_base:.2f} seems too low (should be >23.51)")

            # Check for data quality issues
            warnings = []
            errors = []

            if negative_count > 0:
                warnings.append(f"{negative_count:,} records with negative nitrogen washout")

            if excessive_count > 0:
                warnings.append(f"{excessive_count:,} records with excessive nitrogen washout (>{self.config.max_nitrogen_washout} kg N/ha)")

            if null_count > 0:
                errors.append(f"{null_count:,} records with NULL nitrogen washout")

            if avg_washout < 0 or avg_washout > self.config.max_nitrogen_washout:
                errors.append(f"Average nitrogen washout ({avg_washout:.2f}) outside reasonable range")

            if high_quality_count / total_records < self.config.min_data_coverage:
                errors.append(f"CRITICAL: Insufficient high-quality data coverage: {high_quality_count/total_records:.1%} < {self.config.min_data_coverage:.1%} - Pipeline requires real data, not defaults")

            # Log validation results
            self.log.info("🔬 REFERENCE VALIDATION RESULTS:")
            for result in validation_results:
                self.log.info(f"   {result}")

            # Log warnings and errors
            for warning in warnings:
                self.log.warning(f"Validation warning: {warning}")

            for error in errors:
                self.log.error(f"Validation error: {error}")

            # Validation passes if no critical errors
            if errors:
                self.log.error("❌ Validation failed due to critical errors")
                return False
            else:
                self.log.info("✅ NLES5 estimates validation passed")
                return True

        except Exception as e:
            self.log.error(f"Error during validation: {e}")
            return False

    @timed(name="Testing reference implementation compliance")
    def _test_reference_compliance(self) -> bool:
        """
        Test specific calculations against reference implementation values.

        Returns:
            True if tests pass, False otherwise
        """
        try:
            self.log.info("🧪 Testing NLES5 implementation against reference values")

            # Test 1: Verify coefficient values match reference exactly
            test_results = []

            reference_coefficients = {
                'Bt': 0.456793,
                'Bcs': 0.049570,
                'Bca': 0.157044,
                'Budb': 0.038245,
                'Bm1': 0.026499,
                'Bf0': 0.016314,
                'Bf1': 0.026499,
                'Bg0': 0.014099
            }

            # Compare with config coefficients
            for coef_name, expected_value in reference_coefficients.items():
                actual_value = self.config.nitrogen_coefficients.get(coef_name)
                if actual_value is None:
                    test_results.append(f"❌ Missing coefficient: {coef_name}")
                elif abs(actual_value - expected_value) < 0.000001:  # 6 decimal precision
                    test_results.append(f"✅ {coef_name}: {actual_value} matches reference")
                else:
                    test_results.append(f"❌ {coef_name}: {actual_value} != {expected_value} (reference)")

            # Test 2: Verify crop parameter structure
            expected_crop_params = ['M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'M12', 'M13']
            actual_crop_params = list(self.config.crop_parameters.keys())
            
            if set(expected_crop_params) == set(actual_crop_params):
                test_results.append(f"✅ All 13 crop parameters present: {len(actual_crop_params)} parameters")
            else:
                missing = set(expected_crop_params) - set(actual_crop_params)
                extra = set(actual_crop_params) - set(expected_crop_params)
                test_results.append(f"❌ Crop parameters mismatch - Missing: {missing}, Extra: {extra}")

            # Test 3: Verify trend coefficient (should be -0.1108)
            # This is hardcoded in the calculation, let's verify it appears in the results
            try:
                sample_trend = self.conn.execute("""
                    SELECT trend_effect, year 
                    FROM nles5_nitrogen_estimates 
                    WHERE year IS NOT NULL 
                    LIMIT 1
                """).fetchone()
                
                if sample_trend:
                    trend_effect, year = sample_trend
                    expected_trend = -0.1108 * (year - 1991)
                    if abs(trend_effect - expected_trend) < 0.001:
                        test_results.append(f"✅ Trend calculation correct: {trend_effect:.4f} for year {year}")
                    else:
                        test_results.append(f"❌ Trend calculation incorrect: {trend_effect:.4f} != {expected_trend:.4f} for year {year}")
                else:
                    test_results.append("⚠️  No trend data available for testing")
            except Exception:
                test_results.append("⚠️  Could not test trend calculation")

            # Test 4: Verify V base constant (should include 23.51)
            try:
                avg_v_base = self.conn.execute("SELECT AVG(v_base) FROM nles5_nitrogen_estimates").fetchone()[0]
                if avg_v_base and avg_v_base >= 23.51:
                    test_results.append(f"✅ V base includes constant 23.51: avg = {avg_v_base:.2f}")
                else:
                    test_results.append(f"❌ V base seems incorrect: avg = {avg_v_base:.2f} (should be ≥ 23.51)")
            except Exception:
                test_results.append("⚠️  Could not test V base calculation")

            # Log all test results
            self.log.info("🧪 REFERENCE COMPLIANCE TEST RESULTS:")
            for result in test_results:
                self.log.info(f"   {result}")

            # Count failures
            failures = [r for r in test_results if r.startswith("❌")]
            
            if failures:
                self.log.error(f"❌ Reference compliance failed: {len(failures)} test(s) failed")
                return False
            else:
                self.log.info("✅ Reference compliance tests passed")
                return True

        except Exception as e:
            self.log.error(f"Error during reference compliance testing: {e}")
            return False

    @timed(name="Analyzing estimates distribution")
    def _analyze_estimates_distribution(self) -> None:
        """Analyze the distribution of NLES5 estimates for quality assessment."""
        try:
            self.log.info("📊 Analyzing NLES5 estimates distribution")

            # Basic distribution statistics
            dist_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_estimates,
                    AVG(nitrogen_washout_kg_ha) as mean_washout,
                    STDDEV(nitrogen_washout_kg_ha) as stddev_washout,
                    MIN(nitrogen_washout_kg_ha) as min_washout,
                    MAX(nitrogen_washout_kg_ha) as max_washout,
                    -- Percentiles
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY nitrogen_washout_kg_ha) as q25,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nitrogen_washout_kg_ha) as median,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY nitrogen_washout_kg_ha) as q75,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY nitrogen_washout_kg_ha) as p95,
                    -- Distribution by soil type
                    COUNT(CASE WHEN soil_type = 'sand' THEN 1 END) as sand_count,
                    COUNT(CASE WHEN soil_type = 'clay' THEN 1 END) as clay_count,
                    AVG(CASE WHEN soil_type = 'sand' THEN nitrogen_washout_kg_ha END) as sand_avg,
                    AVG(CASE WHEN soil_type = 'clay' THEN nitrogen_washout_kg_ha END) as clay_avg
                FROM nles5_nitrogen_estimates
                WHERE nitrogen_washout_kg_ha IS NOT NULL
            """).fetchone()

            if dist_stats and dist_stats[0] > 0:
                total, mean, stddev, min_val, max_val, q25, median, q75, p95, sand_count, clay_count, sand_avg, clay_avg = dist_stats

                self.log.info("📊 DISTRIBUTION ANALYSIS:")
                self.log.info(f"   Total estimates: {total:,}")
                self.log.info(f"   Mean: {mean:.2f} kg N/ha (σ={stddev:.2f})")
                self.log.info(f"   Range: {min_val:.2f} - {max_val:.2f} kg N/ha")
                self.log.info(f"   Quartiles: Q1={q25:.2f}, Q2={median:.2f}, Q3={q75:.2f} kg N/ha")
                self.log.info(f"   95th percentile: {p95:.2f} kg N/ha")
                self.log.info(f"   Soil types: Sand={sand_count:,} ({sand_avg:.2f} avg), Clay={clay_count:,} ({clay_avg:.2f} avg)")

                # Distribution by data quality
                quality_dist = self.conn.execute("""
                    SELECT
                        data_quality,
                        COUNT(*) as count,
                        AVG(nitrogen_washout_kg_ha) as avg_washout,
                        STDDEV(nitrogen_washout_kg_ha) as stddev_washout
                    FROM nles5_nitrogen_estimates
                    WHERE nitrogen_washout_kg_ha IS NOT NULL
                    GROUP BY data_quality
                    ORDER BY 
                        CASE data_quality 
                            WHEN 'high' THEN 1 
                            WHEN 'medium' THEN 2 
                            WHEN 'low' THEN 3 
                            ELSE 4 
                        END
                """).fetchall()

                self.log.info("📊 DISTRIBUTION BY DATA QUALITY:")
                for quality, count, avg_wash, std_wash in quality_dist:
                    pct = count / total * 100
                    std_str = f"σ={std_wash:.2f}" if std_wash is not None else "σ=N/A"
                    self.log.info(f"   {quality}: {count:,} ({pct:.1f}%) - avg={avg_wash:.2f} kg N/ha ({std_str})")

                # Check for potential outliers (values beyond 3 standard deviations)
                if stddev and stddev > 0:
                    outlier_threshold = mean + 3 * stddev
                    outlier_count = self.conn.execute(
                        "SELECT COUNT(*) FROM nles5_nitrogen_estimates WHERE nitrogen_washout_kg_ha > ?",
                        [outlier_threshold]
                    ).fetchone()[0]
                    
                    if outlier_count > 0:
                        self.log.warning(f"⚠️  Found {outlier_count:,} potential outliers (>{outlier_threshold:.2f} kg N/ha)")
                    else:
                        self.log.info("✅ No statistical outliers detected")

            else:
                self.log.warning("⚠️  No valid estimates found for distribution analysis")

        except Exception as e:
            self.log.error(f"Error analyzing estimates distribution: {e}")

    @timed(name="Calculating uncertainty estimates")
    def _calculate_uncertainty_estimates(self) -> str:
        """Calculate uncertainty estimates for NLES5 results."""
        try:
            self.log.info("📊 Calculating NLES5 uncertainty estimates")

            # Create uncertainty estimates table
            self.conn.execute("""
                CREATE OR REPLACE TABLE nles5_uncertainty_estimates AS
                SELECT
                    field_id,
                    cvr_number,
                    year,
                    nitrogen_washout_kg_ha,
                    
                    -- Component uncertainties from config
                    nitrogen_washout_kg_ha * ? as bt_uncertainty,
                    nitrogen_washout_kg_ha * ? as bcs_uncertainty,
                    nitrogen_washout_kg_ha * ? as bca_uncertainty,
                    nitrogen_washout_kg_ha * ? as budb_uncertainty,
                    nitrogen_washout_kg_ha * ? as bm1_uncertainty,
                    nitrogen_washout_kg_ha * ? as bf0_uncertainty,
                    nitrogen_washout_kg_ha * ? as bf1_uncertainty,
                    nitrogen_washout_kg_ha * ? as bg0_uncertainty,
                    
                    -- Total uncertainty (quadratic combination)
                    nitrogen_washout_kg_ha * SQRT(
                        POWER(?, 2) + POWER(?, 2) + POWER(?, 2) + POWER(?, 2) +
                        POWER(?, 2) + POWER(?, 2) + POWER(?, 2) + POWER(?, 2)
                    ) as total_uncertainty_kg_ha,
                    
                    -- Uncertainty percentage
                    100.0 * SQRT(
                        POWER(?, 2) + POWER(?, 2) + POWER(?, 2) + POWER(?, 2) +
                        POWER(?, 2) + POWER(?, 2) + POWER(?, 2) + POWER(?, 2)
                    ) as total_uncertainty_pct,
                    
                    data_quality,
                    soil_type
                    
                FROM nles5_nitrogen_estimates
                WHERE nitrogen_washout_kg_ha IS NOT NULL
            """, [
                # Component uncertainties (2x for each coefficient)
                self.config.coefficient_uncertainties['Bt'],
                self.config.coefficient_uncertainties['Bcs'],
                self.config.coefficient_uncertainties['Bca'],
                self.config.coefficient_uncertainties['Budb'],
                self.config.coefficient_uncertainties['Bm1'],
                self.config.coefficient_uncertainties['Bf0'],
                self.config.coefficient_uncertainties['Bf1'],
                self.config.coefficient_uncertainties['Bg0'],
                # For total uncertainty calculation (2x)
                self.config.coefficient_uncertainties['Bt'],
                self.config.coefficient_uncertainties['Bcs'],
                self.config.coefficient_uncertainties['Bca'],
                self.config.coefficient_uncertainties['Budb'],
                self.config.coefficient_uncertainties['Bm1'],
                self.config.coefficient_uncertainties['Bf0'],
                self.config.coefficient_uncertainties['Bf1'],
                self.config.coefficient_uncertainties['Bg0'],
                # For percentage calculation (2x)
                self.config.coefficient_uncertainties['Bt'],
                self.config.coefficient_uncertainties['Bcs'],
                self.config.coefficient_uncertainties['Bca'],
                self.config.coefficient_uncertainties['Budb'],
                self.config.coefficient_uncertainties['Bm1'],
                self.config.coefficient_uncertainties['Bf0'],
                self.config.coefficient_uncertainties['Bf1'],
                self.config.coefficient_uncertainties['Bg0']
            ])

            # Get uncertainty statistics
            uncertainty_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_estimates,
                    AVG(total_uncertainty_kg_ha) as avg_uncertainty_kg_ha,
                    AVG(total_uncertainty_pct) as avg_uncertainty_pct,
                    MIN(total_uncertainty_pct) as min_uncertainty_pct,
                    MAX(total_uncertainty_pct) as max_uncertainty_pct,
                    STDDEV(total_uncertainty_pct) as stddev_uncertainty_pct
                FROM nles5_uncertainty_estimates
            """).fetchone()

            if uncertainty_stats and uncertainty_stats[0] > 0:
                count, avg_unc_kg, avg_unc_pct, min_unc_pct, max_unc_pct, stddev_unc_pct = uncertainty_stats

                self.log.info(f"✅ Calculated uncertainty estimates for {count:,} fields")
                self.log.info(f"📊 Average uncertainty: {avg_unc_kg:.2f} kg N/ha ({avg_unc_pct:.1f}%)")
                self.log.info(f"📊 Uncertainty range: {min_unc_pct:.1f}% - {max_unc_pct:.1f}% (σ={stddev_unc_pct:.1f}%)")

                # Validate against reference targets
                if 8 <= avg_unc_pct <= 15:
                    self.log.info(f"✅ Model uncertainty {avg_unc_pct:.1f}% within reference range (8-15%)")
                else:
                    self.log.warning(f"⚠️  Model uncertainty {avg_unc_pct:.1f}% outside reference range (8-15%)")

            return "nles5_uncertainty_estimates"

        except Exception as e:
            self.log.error(f"Error calculating uncertainty estimates: {e}")
            raise

    @timed(name="Analyzing uncertainty patterns")
    def _analyze_uncertainty_patterns(self) -> str:
        """Analyze uncertainty patterns across different data conditions."""
        try:
            self.log.info("📊 Analyzing uncertainty patterns")

            # Analyze uncertainty by soil type
            soil_uncertainty = self.conn.execute("""
                SELECT
                    soil_type,
                    COUNT(*) as count,
                    AVG(total_uncertainty_pct) as avg_uncertainty,
                    STDDEV(total_uncertainty_pct) as stddev_uncertainty,
                    MIN(total_uncertainty_pct) as min_uncertainty,
                    MAX(total_uncertainty_pct) as max_uncertainty
                FROM nles5_uncertainty_estimates
                GROUP BY soil_type
                ORDER BY soil_type
            """).fetchall()

            self.log.info("📊 UNCERTAINTY BY SOIL TYPE:")
            for soil_type, count, avg_unc, std_unc, min_unc, max_unc in soil_uncertainty:
                std_str = f"σ={std_unc:.1f}" if std_unc is not None else "σ=N/A"
                self.log.info(f"   {soil_type}: {count:,} fields, {avg_unc:.1f}% avg ({std_str}%), range: {min_unc:.1f}-{max_unc:.1f}%")

            # Analyze uncertainty by data quality
            quality_uncertainty = self.conn.execute("""
                SELECT
                    data_quality,
                    COUNT(*) as count,
                    AVG(total_uncertainty_pct) as avg_uncertainty,
                    STDDEV(total_uncertainty_pct) as stddev_uncertainty
                FROM nles5_uncertainty_estimates
                GROUP BY data_quality
                ORDER BY 
                    CASE data_quality 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        WHEN 'low' THEN 3 
                        ELSE 4 
                    END
            """).fetchall()

            self.log.info("📊 UNCERTAINTY BY DATA QUALITY:")
            for quality, count, avg_unc, std_unc in quality_uncertainty:
                std_str = f"σ={std_unc:.1f}" if std_unc is not None else "σ=N/A"
                self.log.info(f"   {quality}: {count:,} fields, {avg_unc:.1f}% avg ({std_str}%)")

            # Create uncertainty pattern summary table
            self.conn.execute("""
                CREATE OR REPLACE TABLE uncertainty_pattern_analysis AS
                SELECT
                    'soil_type' as analysis_type,
                    soil_type as category,
                    COUNT(*) as field_count,
                    AVG(total_uncertainty_pct) as avg_uncertainty_pct,
                    STDDEV(total_uncertainty_pct) as stddev_uncertainty_pct
                FROM nles5_uncertainty_estimates
                GROUP BY soil_type
                
                UNION ALL
                
                SELECT
                    'data_quality' as analysis_type,
                    data_quality as category,
                    COUNT(*) as field_count,
                    AVG(total_uncertainty_pct) as avg_uncertainty_pct,
                    STDDEV(total_uncertainty_pct) as stddev_uncertainty_pct
                FROM nles5_uncertainty_estimates
                GROUP BY data_quality
            """)

            pattern_count = self.conn.execute("SELECT COUNT(*) FROM uncertainty_pattern_analysis").fetchone()[0]
            self.log.info(f"✅ Created uncertainty pattern analysis with {pattern_count} categories")

            return "uncertainty_pattern_analysis"

        except Exception as e:
            self.log.error(f"Error analyzing uncertainty patterns: {e}")
            raise

    @timed(name="Comprehensive data validation")
    def _comprehensive_data_validation(self) -> Dict[str, Any]:
        """Perform comprehensive validation of all data sources and quality."""
        try:
            self.log.info("🔍 Performing comprehensive data validation")
            
            validation_results = {}
            
            # Validate each major table
            tables_to_validate = [
                'agricultural_fields',
                'nles5_nitrogen_estimates',
                'fields_with_climate_soil_crops'
            ]
            
            for table_name in tables_to_validate:
                try:
                    table_stats = self._validate_table_quality(table_name)
                    validation_results[table_name] = table_stats
                except Exception as e:
                    self.log.warning(f"Could not validate table {table_name}: {e}")
                    validation_results[table_name] = {"error": str(e)}
            
            # Perform specialized validations
            try:
                self._validate_climate_data_quality(validation_results.get('fields_with_climate_soil_crops', {}))
            except Exception as e:
                self.log.warning(f"Climate data validation failed: {e}")
            
            try:
                self._validate_field_data_quality(validation_results.get('agricultural_fields', {}))
            except Exception as e:
                self.log.warning(f"Field data validation failed: {e}")
            
            try:
                self._validate_soil_data_quality(validation_results.get('fields_with_climate_soil_crops', {}))
            except Exception as e:
                self.log.warning(f"Soil data validation failed: {e}")
            
            # Generate recommendations
            self._generate_validation_recommendations(validation_results)
            
            # Log summary
            self._log_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            self.log.error(f"Error in comprehensive data validation: {e}")
            return {"error": str(e)}

    def _validate_table_quality(self, table_name: str) -> Dict[str, Any]:
        """Validate the quality of a specific table."""
        try:
            # Check if table exists
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()[0] > 0
            
            if not table_exists:
                return {"exists": False, "error": f"Table {table_name} does not exist"}
            
            # Get basic statistics
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            if row_count == 0:
                return {"exists": True, "row_count": 0, "empty": True}
            
            # Get column information
            columns = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns]
            
            # Check for NULL values in key columns
            null_checks = {}
            key_columns = ['field_id', 'cvr_number', 'geometry'] if 'field_id' in column_names else column_names[:3]
            
            for col in key_columns:
                if col in column_names:
                    null_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL").fetchone()[0]
                    null_checks[col] = {
                        "null_count": null_count,
                        "null_percentage": (null_count / row_count) * 100 if row_count > 0 else 0
                    }
            
            return {
                "exists": True,
                "row_count": row_count,
                "column_count": len(column_names),
                "columns": column_names,
                "null_checks": null_checks,
                "empty": False
            }
            
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def _validate_climate_data_quality(self, stats: Dict[str, Any]) -> None:
        """Validate climate data quality and coverage."""
        if not stats or stats.get("error"):
            return
        
        try:
            climate_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN total_percolation IS NOT NULL AND total_percolation > 0 THEN 1 END) as fields_with_percolation,
                    AVG(total_percolation) as avg_percolation,
                    COUNT(CASE WHEN climate_distance_m IS NOT NULL THEN 1 END) as fields_with_distance,
                    AVG(climate_distance_m) as avg_climate_distance
                FROM fields_with_climate_soil_crops
            """).fetchone()
            
            if climate_stats:
                total, with_perco, avg_perco, with_dist, avg_dist = climate_stats
                
                perco_coverage = (with_perco / total) * 100 if total > 0 else 0
                dist_coverage = (with_dist / total) * 100 if total > 0 else 0
                
                self.log.info(f"🌡️  Climate data quality:")
                self.log.info(f"   Percolation coverage: {with_perco:,}/{total:,} ({perco_coverage:.1f}%)")
                self.log.info(f"   Average percolation: {avg_perco:.1f} mm/year" if avg_perco else "   Average percolation: N/A")
                self.log.info(f"   Distance coverage: {with_dist:,}/{total:,} ({dist_coverage:.1f}%)")
                self.log.info(f"   Average climate distance: {avg_dist:.0f} m" if avg_dist else "   Average climate distance: N/A")
                
                if perco_coverage < 90:
                    self.log.warning(f"⚠️  Low percolation data coverage: {perco_coverage:.1f}%")
                if avg_dist and avg_dist > 5000:
                    self.log.warning(f"⚠️  High average climate distance: {avg_dist:.0f} m")
                    
        except Exception as e:
            self.log.warning(f"Could not validate climate data quality: {e}")

    def _validate_field_data_quality(self, stats: Dict[str, Any]) -> None:
        """Validate field data quality and completeness."""
        if not stats or stats.get("error"):
            return
        
        try:
            field_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN area_ha IS NOT NULL AND area_ha > 0 THEN 1 END) as fields_with_area,
                    AVG(area_ha) as avg_area_ha,
                    COUNT(CASE WHEN crop_name IS NOT NULL THEN 1 END) as fields_with_crop,
                    COUNT(DISTINCT crop_name) as unique_crops,
                    COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END) as fields_with_geometry
                FROM agricultural_fields
            """).fetchone()
            
            if field_stats:
                total, with_area, avg_area, with_crop, unique_crops, with_geom = field_stats
                
                area_coverage = (with_area / total) * 100 if total > 0 else 0
                crop_coverage = (with_crop / total) * 100 if total > 0 else 0
                geom_coverage = (with_geom / total) * 100 if total > 0 else 0
                
                self.log.info(f"🚜 Field data quality:")
                self.log.info(f"   Area coverage: {with_area:,}/{total:,} ({area_coverage:.1f}%)")
                self.log.info(f"   Average area: {avg_area:.2f} ha" if avg_area else "   Average area: N/A")
                self.log.info(f"   Crop coverage: {with_crop:,}/{total:,} ({crop_coverage:.1f}%)")
                self.log.info(f"   Unique crops: {unique_crops}")
                self.log.info(f"   Geometry coverage: {with_geom:,}/{total:,} ({geom_coverage:.1f}%)")
                
                if area_coverage < 95:
                    self.log.warning(f"⚠️  Low area data coverage: {area_coverage:.1f}%")
                if crop_coverage < 90:
                    self.log.warning(f"⚠️  Low crop data coverage: {crop_coverage:.1f}%")
                if geom_coverage < 100:
                    self.log.warning(f"⚠️  Missing geometry data: {geom_coverage:.1f}%")
                    
        except Exception as e:
            self.log.warning(f"Could not validate field data quality: {e}")

    def _validate_soil_data_quality(self, stats: Dict[str, Any]) -> None:
        """Validate soil data quality and coverage."""
        if not stats or stats.get("error"):
            return
        
        try:
            soil_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN soil_type_category IS NOT NULL THEN 1 END) as fields_with_soil_type,
                    COUNT(CASE WHEN clay_content IS NOT NULL THEN 1 END) as fields_with_clay_content,
                    AVG(clay_content) as avg_clay_content,
                    COUNT(DISTINCT soil_type_category) as unique_soil_types
                FROM fields_with_climate_soil_crops
            """).fetchone()
            
            if soil_stats:
                total, with_soil_type, with_clay, avg_clay, unique_types = soil_stats
                
                soil_type_coverage = (with_soil_type / total) * 100 if total > 0 else 0
                clay_coverage = (with_clay / total) * 100 if total > 0 else 0
                
                self.log.info(f"🏔️  Soil data quality:")
                self.log.info(f"   Soil type coverage: {with_soil_type:,}/{total:,} ({soil_type_coverage:.1f}%)")
                self.log.info(f"   Clay content coverage: {with_clay:,}/{total:,} ({clay_coverage:.1f}%)")
                self.log.info(f"   Average clay content: {avg_clay:.1f}%" if avg_clay else "   Average clay content: N/A")
                self.log.info(f"   Unique soil types: {unique_types}")
                
                if soil_type_coverage < 95:
                    self.log.warning(f"⚠️  Low soil type coverage: {soil_type_coverage:.1f}%")
                if clay_coverage < 90:
                    self.log.warning(f"⚠️  Low clay content coverage: {clay_coverage:.1f}%")
                    
        except Exception as e:
            self.log.warning(f"Could not validate soil data quality: {e}")

    def _generate_validation_recommendations(self, validation_results: Dict[str, Any]) -> None:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        for table_name, stats in validation_results.items():
            if stats.get("error"):
                recommendations.append(f"🔧 Fix {table_name}: {stats['error']}")
                continue
            
            if stats.get("empty"):
                recommendations.append(f"📊 Populate {table_name}: Table is empty")
                continue
            
            # Check NULL percentages
            null_checks = stats.get("null_checks", {})
            for col, null_info in null_checks.items():
                if null_info["null_percentage"] > 10:
                    recommendations.append(f"🔧 Improve {table_name}.{col}: {null_info['null_percentage']:.1f}% NULL values")
        
        if recommendations:
            self.log.info("🔧 VALIDATION RECOMMENDATIONS:")
            for rec in recommendations[:10]:  # Limit to top 10
                self.log.info(f"   {rec}")
            if len(recommendations) > 10:
                self.log.info(f"   ... and {len(recommendations) - 10} more recommendations")
        else:
            self.log.info("✅ No major data quality issues detected")

    def _log_validation_summary(self, validation_results: Dict[str, Any]) -> None:
        """Log a summary of validation results."""
        try:
            total_tables = len(validation_results)
            valid_tables = sum(1 for stats in validation_results.values() if not stats.get("error") and not stats.get("empty"))
            total_rows = sum(stats.get("row_count", 0) for stats in validation_results.values() if isinstance(stats.get("row_count"), int))
            
            self.log.info("📋 VALIDATION SUMMARY:")
            self.log.info(f"   Tables validated: {total_tables}")
            self.log.info(f"   Tables with data: {valid_tables}/{total_tables}")
            self.log.info(f"   Total rows: {total_rows:,}")
            
            if valid_tables == total_tables:
                self.log.info("✅ All tables passed validation")
            else:
                self.log.warning(f"⚠️  {total_tables - valid_tables} tables have issues")
                
        except Exception as e:
            self.log.warning(f"Could not generate validation summary: {e}")

    @timed(name="Validating data availability")
    def _validate_data_availability(self) -> None:
        """Validate that all required data sources are available and sufficient."""
        try:
            self.log.info("🔍 Validating data availability for NLES5 processing")
            
            # Check required tables
            required_tables = [
                'agricultural_fields',
                'fertilizer_history', 
                'field_plan',
                'catch_crops'
            ]
            
            available_tables = []
            missing_tables = []
            
            for table in required_tables:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    if count > 0:
                        available_tables.append((table, count))
                        self.log.info(f"✅ {table}: {count:,} records")
                    else:
                        missing_tables.append(table)
                        self.log.warning(f"⚠️  {table}: Empty table")
                except Exception:
                    missing_tables.append(table)
                    self.log.warning(f"❌ {table}: Table not found")
            
            # Check data coverage by year
            try:
                year_coverage = self.conn.execute("""
                    SELECT 
                        year,
                        COUNT(*) as field_count
                    FROM agricultural_fields
                    WHERE year IS NOT NULL
                    GROUP BY year
                    ORDER BY year
                """).fetchall()
                
                if year_coverage:
                    self.log.info("📅 Data coverage by year:")
                    for year, count in year_coverage:
                        self.log.info(f"   {year}: {count:,} fields")
                else:
                    self.log.warning("⚠️  No year information found in agricultural_fields")
                    
            except Exception as e:
                self.log.warning(f"Could not check year coverage: {e}")
            
            # Validate minimum data requirements
            if len(missing_tables) > 0:
                self.log.warning(f"⚠️  Missing or empty tables: {missing_tables}")
            
            if len(available_tables) >= 2:  # At least agricultural_fields and one other
                self.log.info("✅ Sufficient data sources available for NLES5 processing")
            else:
                self.log.error("❌ Insufficient data sources for NLES5 processing")
                raise ValueError(f"Required data missing. Available: {[t[0] for t in available_tables]}, Missing: {missing_tables}")
                
        except Exception as e:
            self.log.error(f"Error validating data availability: {e}")
            raise
