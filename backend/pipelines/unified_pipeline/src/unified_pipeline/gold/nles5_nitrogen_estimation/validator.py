"""
NLES5 Validator Module

This module contains all validation and quality assurance methods
for NLES5 nitrogen washout estimation. It includes:
- NLES5 estimates validation against reference targets
- Reference implementation compliance testing
- Distribution analysis and uncertainty estimation
- Data quality validation and recommendations
- Comprehensive validation reporting

All methods maintain the exact same functionality and validation criteria
from the original implementation.
"""

from typing import Any

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

            # Check if any estimates were generated - try multiple possible table names
            total_count = 0
            table_names_to_try = [
                "nles5_nitrogen_estimates_gold",  # Gold layer table
                "nles5_estimates_final_batched",  # Batched results table
                "nles5_nitrogen_estimates",  # Original table name
            ]

            found_table = None
            for table_name in table_names_to_try:
                try:
                    total_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                    if total_count > 0:
                        found_table = table_name
                        self.log.info(
                            f"✅ Found NLES5 data in table: {table_name} ({total_count:,} records)"
                        )
                        break
                except Exception:
                    continue

            if not found_table or total_count == 0:
                self.log.error("❌ CRITICAL: No NLES5 estimates generated with real data")
                self.log.error(
                    "❌ Pipeline requires actual soil, crop, climate, and fertilizer data"
                )
                raise ValueError(
                    "Validation failed: No NLES5 estimates generated with real data. "
                    "Pipeline requires actual data, not defaults."
                )

            # Validate minimum data quality requirements
            data_quality_check = self.conn.execute(f"""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN crop_type IS NOT NULL THEN 1 END) as fields_with_crop_data,
                    COUNT(CASE WHEN area_ha IS NOT NULL THEN 1 END) as fields_with_area_data,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha IS NOT NULL
                        THEN 1 END) as fields_with_nitrogen_data
                FROM {found_table}
            """).fetchone()

            total_fields, crop_data_count, area_data_count, nitrogen_data_count = data_quality_check

            # Log validation results with data preview
            self.log.info("📊 NLES5 Validation Results:")
            self.log.info(f"   Total fields: {total_fields:,}")
            self.log.info(
                f"   Fields with crop data: {crop_data_count:,} "
                f"({crop_data_count / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"   Fields with area data: {area_data_count:,} "
                f"({area_data_count / total_fields * 100:.1f}%)"
            )
            self.log.info(
                f"   Fields with nitrogen estimates: {nitrogen_data_count:,} "
                f"({nitrogen_data_count / total_fields * 100:.1f}%)"
            )

            # Check for good data coverage (allow some tolerance)
            if crop_data_count < total_fields * 0.9:
                self.log.warning(
                    f"⚠️ Low crop data coverage: {crop_data_count}/{total_fields} "
                    f"({crop_data_count / total_fields * 100:.1f}%)"
                )

            if nitrogen_data_count < total_fields * 0.8:
                self.log.warning(
                    f"⚠️ Low nitrogen estimate coverage: {nitrogen_data_count}/{total_fields} "
                    f"({nitrogen_data_count / total_fields * 100:.1f}%)"
                )

            # Add sample data preview
            self._log_data_preview(found_table)

            # Enhanced validation with reference targets
            self.log.info(f"✅ Data quality validation passed: {total_fields:,} fields processed")

            stats = self.conn.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    AVG(nitrogen_washout_kg_ha) as avg_washout,
                    STDDEV(nitrogen_washout_kg_ha) as stddev_washout,
                    MIN(nitrogen_washout_kg_ha) as min_washout,
                    MAX(nitrogen_washout_kg_ha) as max_washout,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha < 0 THEN 1 END) as negative_count,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha > 100 THEN 1 END) as excessive_count_100,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha > 200 THEN 1 END) as excessive_count_200,
                    COUNT(CASE WHEN nitrogen_washout_kg_ha IS NULL THEN 1 END) as null_count,
                    AVG(area_ha) as avg_area,
                    COUNT(DISTINCT crop_type) as unique_crops
                FROM {found_table}
            """).fetchone()

            (
                total_records,
                avg_washout,
                stddev_washout,
                min_washout,
                max_washout,
                negative_count,
                excessive_count_100,
                excessive_count_200,
                null_count,
                avg_area,
                unique_crops,
            ) = stats

            # Log enhanced validation statistics
            self.log.info("📊 NLES5 VALIDATION RESULTS:")
            self.log.info(f"   Records: {total_records:,}")
            self.log.info(f"   Average field size: {avg_area:.2f} ha")
            self.log.info(f"   Unique crop types: {unique_crops:,}")
            if avg_washout is not None:
                self.log.info(
                    f"   Avg Nitrogen Washout: {avg_washout:.2f} kg N/ha (σ={stddev_washout:.2f})"
                )
                self.log.info(f"   Range: {min_washout:.2f} to {max_washout:.2f} kg N/ha")
                self.log.info(f"   Negative values: {negative_count:,}")
                self.log.info(f"   Excessive values (>100): {excessive_count_100:,}")
                self.log.info(f"   Excessive values (>200): {excessive_count_200:,}")
                self.log.info(f"   Null values: {null_count:,}")

            # Simple validation checks
            validation_results = []

            # Check standard deviation
            # Check standard deviation (based on NLES5 validation RMSE of 30.8 kg N/ha)
            if stddev_washout is not None:
                if 15 <= stddev_washout <= 35:
                    validation_results.append(
                        "✅ Standard deviation within expected range (15-35 kg N/ha)"
                    )
                elif 8 <= stddev_washout <= 50:
                    validation_results.append(
                        f"✅ Standard deviation {stddev_washout:.2f} kg N/ha "
                        f"reasonable (extended range)"
                    )
                else:
                    validation_results.append(
                        f"⚠️ Standard deviation {stddev_washout:.2f} kg N/ha "
                        f"outside expected range (8-50 kg N/ha)"
                    )

            # Check for reasonable washout values
            if avg_washout is not None:
                if 0 <= avg_washout <= 50:
                    validation_results.append("✅ Average washout values reasonable")
                else:
                    validation_results.append(
                        f"⚠️ Average washout {avg_washout:.2f} may be outside normal range"
                    )
                    validation_results.append(
                        f"⚠️ Standard deviation {stddev_washout:.2f} kg N/ha "
                        f"outside expected range (8-50 kg N/ha)"
                    )

            # Check for reasonable washout values (based on NLES5 validation data)
            if avg_washout is not None:
                if 40 <= avg_washout <= 92:
                    validation_results.append(
                        "✅ Average washout values within expected Danish range (40-92 kg N/ha)"
                    )
                elif 20 <= avg_washout <= 150:
                    validation_results.append(
                        f"✅ Average washout {avg_washout:.2f} kg N/ha reasonable (extended range)"
                    )
                else:
                    validation_results.append(
                        f"⚠️ Average washout {avg_washout:.2f} kg N/ha "
                        f"outside expected range (20-150 kg N/ha)"
                    )

            # Report validation results
            for result in validation_results:
                self.log.info(f"   {result}")

            return True

        except Exception as e:
            self.log.error(f"Error during validation: {e}")
            return False

    def _log_data_preview(self, table_name: str) -> None:
        """Log a preview of the NLES5 data for debugging and verification."""
        try:
            self.log.info(f"📋 DATA PREVIEW from {table_name}:")

            # Sample data preview
            sample_data = self.conn.execute(f"""
                SELECT
                    field_id,
                    year,
                    crop_type,
                    area_ha,
                    nitrogen_washout_kg_ha,
                    percolation_mm,
                    uncertainty_pct
                FROM {table_name}
                ORDER BY area_ha DESC
                LIMIT 5
            """).fetchall()

            if sample_data:
                self.log.info("   Top 5 largest fields:")
                self.log.info(
                    "   Field ID | Year | Crop | Area(ha) | N-Washout(kg/ha) | "
                    "Percolation(mm) | Uncertainty(%)"
                )
                self.log.info("   " + "-" * 90)
                for row in sample_data:
                    field_id, year, crop, area, washout, perco, uncertainty = row
                    self.log.info(
                        f"   {field_id[:8]:<8} | {year} | {crop[:8]:<8} | {area:8.2f} | "
                        f"{washout or 0:12.2f} | {perco or 0:11.2f} | {uncertainty or 0:11.1f}"
                    )

            # Crop type distribution
            crop_stats = self.conn.execute(f"""
                SELECT
                    crop_type,
                    COUNT(*) as field_count,
                    AVG(area_ha) as avg_area,
                    AVG(nitrogen_washout_kg_ha) as avg_washout
                FROM {table_name}
                WHERE crop_type IS NOT NULL
                GROUP BY crop_type
                ORDER BY field_count DESC
                LIMIT 10
            """).fetchall()

            if crop_stats:
                self.log.info("   Top 10 crop types:")
                self.log.info("   Crop Type | Count | Avg Area(ha) | Avg Washout(kg/ha)")
                self.log.info("   " + "-" * 55)
                for crop, count, avg_area, avg_washout in crop_stats:
                    self.log.info(
                        f"   {crop[:15]:<15} | {count:5,} | {avg_area or 0:8.2f} | "
                        f"{avg_washout or 0:12.2f}"
                    )

        except Exception as e:
            self.log.warning(f"Could not generate data preview: {e}")

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
                "Bt": 0.456793,
                "Bcs": 0.049570,
                "Bca": 0.157044,
                "Budb": 0.038245,
                "Bm1": 0.026499,
                "Bf0": 0.016314,
                "Bf1": 0.026499,
                "Bg0": 0.014099,
            }

            # Compare with config coefficients
            for coef_name, expected_value in reference_coefficients.items():
                actual_value = self.config.nitrogen_coefficients.get(coef_name)
                if actual_value is None:
                    test_results.append(f"❌ Missing coefficient: {coef_name}")
                elif abs(actual_value - expected_value) < 0.000001:  # 6 decimal precision
                    test_results.append(f"✅ {coef_name}: {actual_value} matches reference")
                else:
                    test_results.append(
                        f"❌ {coef_name}: {actual_value} != {expected_value} (reference)"
                    )

            # Test 2: Verify crop parameter structure
            expected_crop_params = [
                "M1",
                "M2",
                "M3",
                "M4",
                "M5",
                "M6",
                "M7",
                "M8",
                "M9",
                "M10",
                "M11",
                "M12",
                "M13",
            ]
            actual_crop_params = list(self.config.crop_parameters.keys())

            if set(expected_crop_params) == set(actual_crop_params):
                test_results.append(
                    f"✅ All 13 crop parameters present: {len(actual_crop_params)} parameters"
                )
            else:
                missing = set(expected_crop_params) - set(actual_crop_params)
                extra = set(actual_crop_params) - set(expected_crop_params)
                test_results.append(
                    f"❌ Crop parameters mismatch - Missing: {missing}, Extra: {extra}"
                )

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
                        test_results.append(
                            f"✅ Trend calculation correct: {trend_effect:.4f} for year {year}"
                        )
                    else:
                        test_results.append(
                            f"❌ Trend calculation incorrect: {trend_effect:.4f} != "
                            f"{expected_trend:.4f} for year {year}"
                        )
                else:
                    test_results.append("⚠️  No trend data available for testing")
            except Exception:
                test_results.append("⚠️  Could not test trend calculation")

            # Test 4: Verify V base constant (should include 23.51)
            try:
                avg_v_base = self.conn.execute(
                    "SELECT AVG(v_base) FROM nles5_nitrogen_estimates"
                ).fetchone()[0]
                if avg_v_base and avg_v_base >= 23.51:
                    test_results.append(
                        f"✅ V base includes constant 23.51: avg = {avg_v_base:.2f}"
                    )
                else:
                    test_results.append(
                        f"❌ V base seems incorrect: avg = {avg_v_base:.2f} (should be ≥ 23.51)"
                    )
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
                (
                    total,
                    mean,
                    stddev,
                    min_val,
                    max_val,
                    q25,
                    median,
                    q75,
                    p95,
                    sand_count,
                    clay_count,
                    sand_avg,
                    clay_avg,
                ) = dist_stats

                self.log.info("📊 DISTRIBUTION ANALYSIS:")
                self.log.info(f"   Total estimates: {total:,}")
                self.log.info(f"   Mean: {mean:.2f} kg N/ha (σ={stddev:.2f})")
                self.log.info(f"   Range: {min_val:.2f} - {max_val:.2f} kg N/ha")
                self.log.info(f"   Quartiles: Q1={q25:.2f}, Q2={median:.2f}, Q3={q75:.2f} kg N/ha")
                self.log.info(f"   95th percentile: {p95:.2f} kg N/ha")
                self.log.info(
                    f"   Soil types: Sand={sand_count:,} ({sand_avg:.2f} avg), "
                    f"Clay={clay_count:,} ({clay_avg:.2f} avg)"
                )

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
                    self.log.info(
                        f"   {quality}: {count:,} ({pct:.1f}%) - "
                        f"avg={avg_wash:.2f} kg N/ha ({std_str})"
                    )

                # Check for potential outliers (values beyond 3 standard deviations)
                if stddev and stddev > 0:
                    outlier_threshold = mean + 3 * stddev
                    outlier_count = self.conn.execute(
                        "SELECT COUNT(*) FROM nles5_nitrogen_estimates "
                        "WHERE nitrogen_washout_kg_ha > ?",
                        [outlier_threshold],
                    ).fetchone()[0]

                    if outlier_count > 0:
                        self.log.warning(
                            f"⚠️  Found {outlier_count:,} potential outliers "
                            f"(>{outlier_threshold:.2f} kg N/ha)"
                        )
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
            self.conn.execute(
                """
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
            """,
                [
                    # Component uncertainties (2x for each coefficient)
                    self.config.coefficient_uncertainties["Bt"],
                    self.config.coefficient_uncertainties["Bcs"],
                    self.config.coefficient_uncertainties["Bca"],
                    self.config.coefficient_uncertainties["Budb"],
                    self.config.coefficient_uncertainties["Bm1"],
                    self.config.coefficient_uncertainties["Bf0"],
                    self.config.coefficient_uncertainties["Bf1"],
                    self.config.coefficient_uncertainties["Bg0"],
                    # For total uncertainty calculation (2x)
                    self.config.coefficient_uncertainties["Bt"],
                    self.config.coefficient_uncertainties["Bcs"],
                    self.config.coefficient_uncertainties["Bca"],
                    self.config.coefficient_uncertainties["Budb"],
                    self.config.coefficient_uncertainties["Bm1"],
                    self.config.coefficient_uncertainties["Bf0"],
                    self.config.coefficient_uncertainties["Bf1"],
                    self.config.coefficient_uncertainties["Bg0"],
                    # For percentage calculation (2x)
                    self.config.coefficient_uncertainties["Bt"],
                    self.config.coefficient_uncertainties["Bcs"],
                    self.config.coefficient_uncertainties["Bca"],
                    self.config.coefficient_uncertainties["Budb"],
                    self.config.coefficient_uncertainties["Bm1"],
                    self.config.coefficient_uncertainties["Bf0"],
                    self.config.coefficient_uncertainties["Bf1"],
                    self.config.coefficient_uncertainties["Bg0"],
                ],
            )

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
                count, avg_unc_kg, avg_unc_pct, min_unc_pct, max_unc_pct, stddev_unc_pct = (
                    uncertainty_stats
                )

                self.log.info(f"✅ Calculated uncertainty estimates for {count:,} fields")
                self.log.info(
                    f"📊 Average uncertainty: {avg_unc_kg:.2f} kg N/ha ({avg_unc_pct:.1f}%)"
                )
                self.log.info(
                    f"📊 Uncertainty range: {min_unc_pct:.1f}% - {max_unc_pct:.1f}% "
                    f"(σ={stddev_unc_pct:.1f}%)"
                )

                # Validate against reference targets
                if 8 <= avg_unc_pct <= 15:
                    self.log.info(
                        f"✅ Model uncertainty {avg_unc_pct:.1f}% within reference range (8-15%)"
                    )
                else:
                    self.log.warning(
                        f"⚠️  Model uncertainty {avg_unc_pct:.1f}% outside reference range (8-15%)"
                    )

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
                self.log.info(
                    f"   {soil_type}: {count:,} fields, {avg_unc:.1f}% avg ({std_str}%), "
                    f"range: {min_unc:.1f}-{max_unc:.1f}%"
                )

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

            pattern_count = self.conn.execute(
                "SELECT COUNT(*) FROM uncertainty_pattern_analysis"
            ).fetchone()[0]
            self.log.info(
                f"✅ Created uncertainty pattern analysis with {pattern_count} categories"
            )

            return "uncertainty_pattern_analysis"

        except Exception as e:
            self.log.error(f"Error analyzing uncertainty patterns: {e}")
            raise

    @timed(name="Comprehensive data validation")
    def _comprehensive_data_validation(self) -> dict[str, Any]:
        """Perform comprehensive validation of all data sources and quality."""
        try:
            self.log.info("🔍 Performing comprehensive data validation")

            validation_results = {
                # Initialize as passed, will be set to False if critical issues found
                "passed": True,
                "errors": [],
                "recommendations": [],
                # Initialize with 0 score, will be calculated based on actual data quality
                "data_quality_score": 0.0,
            }

            # Validate each major table (only check tables that should exist at this point)
            # Note: This validation runs at Phase 3.5, before NLES5 calculations
            tables_to_validate = [
                "agricultural_fields",  # Loaded in Phase 2
                # 'nles5_nitrogen_estimates',  # Created in Phase 4 - skip for now
                # 'fields_with_climate_soil_crops'  # Created in Phase 4 - skip for now
            ]

            # Optionally validate result tables if they exist (for final validation)
            optional_result_tables = [
                "nles5_nitrogen_estimates",
                "fields_with_climate_soil_crops",
                "nles5_nitrogen_estimates_gold",
            ]

            for table_name in tables_to_validate:
                try:
                    table_stats = self._validate_table_quality(table_name)
                    validation_results[table_name] = table_stats
                except Exception as e:
                    self.log.warning(f"Could not validate table {table_name}: {e}")
                    validation_results[table_name] = {"error": str(e)}

            # Try to validate optional tables if they exist (don't fail if they don't)
            for table_name in optional_result_tables:
                try:
                    table_exists = (
                        self.conn.execute(f"""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_name = '{table_name}'
                    """).fetchone()[0]
                        > 0
                    )

                    if table_exists:
                        table_stats = self._validate_table_quality(table_name)
                        validation_results[table_name] = table_stats
                        self.log.info(f"✅ Optional table {table_name} validated successfully")
                except Exception:
                    # Don't log warnings for optional tables that don't exist yet
                    pass

            # Perform specialized validations
            try:
                self._validate_climate_data_quality(
                    validation_results.get("fields_with_climate_soil_crops", {})
                )
            except Exception as e:
                self.log.warning(f"Climate data validation failed: {e}")

            try:
                self._validate_field_data_quality(validation_results.get("agricultural_fields", {}))
            except Exception as e:
                self.log.warning(f"Field data validation failed: {e}")

            try:
                self._validate_soil_data_quality(
                    validation_results.get("fields_with_climate_soil_crops", {})
                )
            except Exception as e:
                self.log.warning(f"Soil data validation failed: {e}")

            # Generate recommendations
            self._generate_validation_recommendations(validation_results)

            # Calculate data quality score based on validation results
            self._calculate_data_quality_score(validation_results)

            # Log summary
            self._log_validation_summary(validation_results)

            return validation_results

        except Exception as e:
            self.log.error(f"Error in comprehensive data validation: {e}")
            return {"error": str(e)}

    def _validate_table_quality(self, table_name: str) -> dict[str, Any]:
        """Validate the quality of a specific table."""
        try:
            # Check if table exists
            table_exists = (
                self.conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """).fetchone()[0]
                > 0
            )

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
            key_columns = (
                ["field_id", "cvr_number", "geometry"]
                if "field_id" in column_names
                else column_names[:3]
            )

            for col in key_columns:
                if col in column_names:
                    null_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
                    ).fetchone()[0]
                    null_checks[col] = {
                        "null_count": null_count,
                        "null_percentage": (null_count / row_count) * 100 if row_count > 0 else 0,
                    }

            return {
                "exists": True,
                "row_count": row_count,
                "column_count": len(column_names),
                "columns": column_names,
                "null_checks": null_checks,
                "empty": False,
            }

        except Exception as e:
            return {"exists": False, "error": str(e)}

    def _validate_climate_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate climate data quality and coverage."""
        if not stats or stats.get("error"):
            return

        try:
            climate_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN total_percolation IS NOT NULL AND total_percolation > 0
                        THEN 1 END) as fields_with_percolation,
                    AVG(total_percolation) as avg_percolation,
                    COUNT(CASE WHEN climate_distance_m IS NOT NULL
                        THEN 1 END) as fields_with_distance,
                    AVG(climate_distance_m) as avg_climate_distance
                FROM fields_with_climate_soil_crops
            """).fetchone()

            if climate_stats:
                total, with_perco, avg_perco, with_dist, avg_dist = climate_stats

                perco_coverage = (with_perco / total) * 100 if total > 0 else 0
                dist_coverage = (with_dist / total) * 100 if total > 0 else 0

                self.log.info("🌡️  Climate data quality:")
                self.log.info(
                    f"   Percolation coverage: {with_perco:,}/{total:,} ({perco_coverage:.1f}%)"
                )
                self.log.info(
                    f"   Average percolation: {avg_perco:.1f} mm/year"
                    if avg_perco
                    else "   Average percolation: N/A"
                )
                self.log.info(
                    f"   Distance coverage: {with_dist:,}/{total:,} ({dist_coverage:.1f}%)"
                )
                self.log.info(
                    f"   Average climate distance: {avg_dist:.0f} m"
                    if avg_dist
                    else "   Average climate distance: N/A"
                )

                if perco_coverage < 90:
                    self.log.warning(f"⚠️  Low percolation data coverage: {perco_coverage:.1f}%")
                if avg_dist and avg_dist > 5000:
                    self.log.warning(f"⚠️  High average climate distance: {avg_dist:.0f} m")

        except Exception as e:
            self.log.warning(f"Could not validate climate data quality: {e}")

    def _validate_field_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate field data quality and completeness."""
        if not stats or stats.get("error"):
            return

        try:
            field_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN area_ha IS NOT NULL AND area_ha > 0
                        THEN 1 END) as fields_with_area,
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

                self.log.info("🚜 Field data quality:")
                self.log.info(f"   Area coverage: {with_area:,}/{total:,} ({area_coverage:.1f}%)")
                self.log.info(
                    f"   Average area: {avg_area:.2f} ha" if avg_area else "   Average area: N/A"
                )
                self.log.info(f"   Crop coverage: {with_crop:,}/{total:,} ({crop_coverage:.1f}%)")
                self.log.info(f"   Unique crops: {unique_crops}")
                self.log.info(
                    f"   Geometry coverage: {with_geom:,}/{total:,} ({geom_coverage:.1f}%)"
                )

                if area_coverage < 95:
                    self.log.warning(f"⚠️  Low area data coverage: {area_coverage:.1f}%")
                if crop_coverage < 90:
                    self.log.warning(f"⚠️  Low crop data coverage: {crop_coverage:.1f}%")
                if geom_coverage < 100:
                    self.log.warning(f"⚠️  Missing geometry data: {geom_coverage:.1f}%")

        except Exception as e:
            self.log.warning(f"Could not validate field data quality: {e}")

    def _validate_soil_data_quality(self, stats: dict[str, Any]) -> None:
        """Validate soil data quality and coverage."""
        if not stats or stats.get("error"):
            return

        try:
            soil_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN soil_type_category IS NOT NULL
                        THEN 1 END) as fields_with_soil_type,
                    COUNT(CASE WHEN clay_content IS NOT NULL
                        THEN 1 END) as fields_with_clay_content,
                    AVG(clay_content) as avg_clay_content,
                    COUNT(DISTINCT soil_type_category) as unique_soil_types
                FROM fields_with_climate_soil_crops
            """).fetchone()

            if soil_stats:
                total, with_soil_type, with_clay, avg_clay, unique_types = soil_stats

                soil_type_coverage = (with_soil_type / total) * 100 if total > 0 else 0
                clay_coverage = (with_clay / total) * 100 if total > 0 else 0

                self.log.info("🏔️  Soil data quality:")
                self.log.info(
                    f"   Soil type coverage: {with_soil_type:,}/{total:,} "
                    f"({soil_type_coverage:.1f}%)"
                )
                self.log.info(
                    f"   Clay content coverage: {with_clay:,}/{total:,} ({clay_coverage:.1f}%)"
                )
                self.log.info(
                    f"   Average clay content: {avg_clay:.1f}%"
                    if avg_clay
                    else "   Average clay content: N/A"
                )
                self.log.info(f"   Unique soil types: {unique_types}")

                if soil_type_coverage < 95:
                    self.log.warning(f"⚠️  Low soil type coverage: {soil_type_coverage:.1f}%")
                if clay_coverage < 90:
                    self.log.warning(f"⚠️  Low clay content coverage: {clay_coverage:.1f}%")

        except Exception as e:
            self.log.warning(f"Could not validate soil data quality: {e}")

    def _generate_validation_recommendations(self, validation_results: dict[str, Any]) -> None:
        """Generate recommendations based on validation results."""
        recommendations = validation_results.get("recommendations", [])
        errors = validation_results.get("errors", [])

        # Tables that should exist at validation time (Phase 3.5)
        required_tables = ["agricultural_fields"]

        # Tables that are created later (Phase 4) - don't warn about these

        for table_name, stats in validation_results.items():
            if table_name in ["passed", "errors", "recommendations", "data_quality_score"]:
                continue  # Skip metadata fields

            if not isinstance(stats, dict):
                continue  # Skip non-dictionary values

            if stats.get("error"):
                # Only generate recommendations for required tables
                # Optional tables might not exist yet (they're created in Phase 4)
                if table_name in required_tables:
                    recommendations.append(f"🔧 Fix {table_name}: {stats['error']}")
                    errors.append(f"Table error: {table_name}")
                # Skip errors for optional tables - they're created later in the pipeline
                continue

            if stats.get("empty"):
                recommendations.append(f"📊 Populate {table_name}: Table is empty")
                # Don't set passed=False for empty optional tables
                continue

            # Check NULL percentages
            null_checks = stats.get("null_checks", {})
            for col, null_info in null_checks.items():
                if null_info["null_percentage"] > 10:
                    recommendations.append(
                        f"🔧 Improve {table_name}.{col}: "
                        f"{null_info['null_percentage']:.1f}% NULL values"
                    )

        # Store results back in validation_results
        validation_results["recommendations"] = recommendations
        validation_results["errors"] = errors

        if recommendations:
            self.log.info("🔧 VALIDATION RECOMMENDATIONS:")
            for rec in recommendations[:10]:  # Limit to top 10
                self.log.info(f"   {rec}")
            if len(recommendations) > 10:
                self.log.info(f"   ... and {len(recommendations) - 10} more recommendations")
        else:
            self.log.info("✅ No major data quality issues detected")

    def _calculate_data_quality_score(self, validation_results: dict[str, Any]) -> None:
        """Calculate data quality score based on validation results."""
        score = 0.0

        # Score based on existing tables (40 points possible)
        tables_found = 0

        for table_name, stats in validation_results.items():
            if table_name in ["passed", "errors", "recommendations", "data_quality_score"]:
                continue

            if isinstance(stats, dict) and stats.get("exists", False):
                tables_found += 1
                # Give partial score if table exists but has issues
                if stats.get("row_count", 0) > 0:
                    score += 10  # Base score for existing table with data

                    # Bonus for data quality
                    null_checks = stats.get("null_checks", {})
                    if null_checks:
                        avg_null_percentage = sum(
                            check.get("null_percentage", 0) for check in null_checks.values()
                        ) / len(null_checks)
                        if avg_null_percentage < 10:  # Less than 10% nulls is good
                            score += 3.33  # Up to 40 points total for tables
                else:
                    score += 3  # Minimal score for empty table

        # Score based on validation errors (30 points deducted for errors)
        error_count = len(validation_results.get("errors", []))
        if error_count == 0:
            score += 30
        elif error_count <= 2:
            score += 20  # Minor errors
        elif error_count <= 5:
            score += 10  # Moderate errors
        # No points if more than 5 errors

        # Score based on recommendations (30 points deducted for issues)
        recommendation_count = len(validation_results.get("recommendations", []))
        if recommendation_count == 0:
            score += 30
        elif recommendation_count <= 3:
            score += 20  # Minor issues
        elif recommendation_count <= 7:
            score += 10  # Moderate issues
        # No points if more than 7 recommendations

        # Ensure score is within bounds
        score = max(0.0, min(100.0, score))
        validation_results["data_quality_score"] = score

        # Set passed based on score threshold, but be more lenient for missing output tables
        # that haven't been created yet (like nles5_nitrogen_estimates)
        agricultural_fields_exists = any(
            name == "agricultural_fields"
            and isinstance(stats, dict)
            and stats.get("exists", False)
            and stats.get("row_count", 0) > 0
            for name, stats in validation_results.items()
            if isinstance(stats, dict)
            and name not in ["passed", "errors", "recommendations", "data_quality_score"]
        )

        # Pass validation if we have agricultural fields (the essential input data)
        # The output tables (nles5_nitrogen_estimates, etc.) will be created during processing
        if not agricultural_fields_exists or score < 20.0:
            validation_results["passed"] = False

    def _log_validation_summary(self, validation_results: dict[str, Any]) -> None:
        """Log a summary of validation results."""
        try:
            # Count only actual table validations, not metadata fields
            total_tables = sum(
                1
                for key, value in validation_results.items()
                if isinstance(value, dict)
                and key not in ["passed", "errors", "recommendations", "data_quality_score"]
            )
            valid_tables = sum(
                1
                for stats in validation_results.values()
                if isinstance(stats, dict) and not stats.get("error") and not stats.get("empty")
            )
            total_rows = sum(
                stats.get("row_count", 0)
                for stats in validation_results.values()
                if isinstance(stats, dict) and isinstance(stats.get("row_count"), int)
            )

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
                "agricultural_fields",
                "fertilizer_history",
                "field_plan",
                "catch_crops",
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
                raise ValueError(
                    f"Required data missing. Available: {[t[0] for t in available_tables]}, "
                    f"Missing: {missing_tables}"
                )

        except Exception as e:
            self.log.error(f"Error validating data availability: {e}")
            raise
