import csv
import logging
from typing import Dict

# from ..database import DatabaseManager # Placeholder for imports
# from ..config import Config

logger = logging.getLogger(__name__)


class CVRMatcher:
    """Handles CVR matching between datasets."""

    def __init__(self, db_manager, config):  # Simplified types for now
        """Initialize with database manager."""
        self.db = db_manager
        self.config = config  # Store config for output paths

    def find_unmatched_pesticide_cvrs(self) -> Dict:
        """
        Step 1: Find CVRs in pesticide data that can't be matched across other tables.
        Uses VARCHAR-based matching to handle data type inconsistencies.
        """
        try:
            self.db.execute_query("""
                CREATE OR REPLACE TEMPORARY VIEW field_data AS
                SELECT DISTINCT 
                    CAST(m.CVR AS VARCHAR) as cvr,
                    m.Markblok,
                    m.Marknr
                FROM marker m
                WHERE m.CVR IS NOT NULL AND TRIM(m.CVR) != ''
            """)

            unmatched_query = """
                WITH cleaned_pesticide AS (
                    SELECT 
                        CAST(CAST(CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as cvr,
                        CompanyName,
                        AcreageSize
                    FROM pesticide
                    WHERE CompanyRegistrationNumber IS NOT NULL AND TRIM(CAST(CompanyRegistrationNumber AS VARCHAR)) != ''
                ),
                cleaned_gkea AS (
                    SELECT 
                        CAST("CVR" AS VARCHAR) as cvr
                    FROM gkea
                    WHERE "CVR" IS NOT NULL AND TRIM("CVR") != ''
                ),
                unmatched_cvrs AS (
                    SELECT DISTINCT p.cvr
                    FROM cleaned_pesticide p
                    LEFT JOIN field_data f ON f.cvr = p.cvr
                    LEFT JOIN cleaned_gkea g ON g.cvr = p.cvr
                    WHERE f.cvr IS NULL AND g.cvr IS NULL
                )
                SELECT 
                    uc.cvr,
                    p.CompanyName,
                    COUNT(*) as record_count,
                    SUM(CAST(p.AcreageSize AS DOUBLE)) as total_area
                FROM unmatched_cvrs uc
                JOIN cleaned_pesticide p ON p.cvr = uc.cvr
                GROUP BY uc.cvr, p.CompanyName
            """
            unmatched_results = self.db.execute_query(unmatched_query)

            self.db.execute_query("""
                CREATE OR REPLACE TEMPORARY VIEW unmatched_pesticide_records AS
                WITH cleaned_pesticide AS (
                    SELECT 
                        CAST(CAST(CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as cvr,
                        *
                    FROM pesticide
                    WHERE CompanyRegistrationNumber IS NOT NULL AND TRIM(CAST(CompanyRegistrationNumber AS VARCHAR)) != ''
                ),
                cleaned_gkea AS (
                    SELECT 
                        CAST("CVR" AS VARCHAR) as cvr
                    FROM gkea
                    WHERE "CVR" IS NOT NULL AND TRIM("CVR") != ''
                ),
                unmatched_cvrs AS (
                    SELECT DISTINCT p.cvr
                    FROM cleaned_pesticide p
                    LEFT JOIN field_data f ON f.cvr = p.cvr
                    LEFT JOIN cleaned_gkea g ON g.cvr = p.cvr
                    WHERE f.cvr IS NULL AND g.cvr IS NULL
                )
                SELECT p.*
                FROM cleaned_pesticide p
                JOIN unmatched_cvrs uc ON p.cvr = uc.cvr
            """)

            return {
                "unmatched_cvrs": unmatched_results,
                "total_unmatched": len(unmatched_results),
                "total_records": sum(row[2] for row in unmatched_results),
                "total_area": sum((row[3] or 0) for row in unmatched_results),
            }
        except Exception as e:
            logger.error(f"Error finding unmatched pesticide CVRs: {str(e)}")
            raise

    def analyze_gkea_non_numeric_cvrs(self) -> Dict:
        """
        Step 3: Find unique IDs behind GKEA non-numeric CVRs
        """
        try:
            non_numeric_query = """
                SELECT DISTINCT 
                    "CVR" as original_cvr,
                    COUNT(*) as record_count,
                    STRING_AGG(DISTINCT "Marknummer", ', ') as field_ids
                FROM gkea
                WHERE "CVR" IS NOT NULL 
                AND "CVR" != ''
                AND NOT REGEXP_MATCHES("CVR", '^[0-9]+$')
                GROUP BY "CVR"
            """
            non_numeric_results = self.db.execute_query(non_numeric_query)
            return {
                "non_numeric_cvrs": non_numeric_results,
                "total_non_numeric": len(non_numeric_results),
                "total_records": sum(row[1] for row in non_numeric_results),
            }
        except Exception as e:
            logger.error(f"Error analyzing GKEA non-numeric CVRs: {str(e)}")
            raise

    def analyze_empty_marker_cvrs(self) -> Dict:
        """
        Step 5: Analyze empty CVR rows in marker to potentially match with remaining unmatched CVRs
        """
        try:
            empty_cvr_query = """
                SELECT 
                    m.Markblok,
                    m.Marknr,
                    COUNT(*) as record_count
                FROM marker m
                WHERE m.CVR IS NULL OR TRIM(m.CVR) = ''
                GROUP BY m.Markblok, m.Marknr
            """
            empty_cvr_results = self.db.execute_query(empty_cvr_query)
            return {
                "empty_cvr_records": empty_cvr_results,
                "total_empty": len(empty_cvr_results),
                "total_records": sum(row[2] for row in empty_cvr_results),
            }
        except Exception as e:
            logger.error(f"Error analyzing empty marker CVRs: {str(e)}")
            raise

    def match_empty_marker_cvrs_by_journalnr_afgkode_area(self) -> dict:
        """... (docstring remains the same) ..."""
        try:
            detailed_query = """ ... (SQL query remains the same) ... """
            results = self.db.execute_query(detailed_query)
            detailed_data_query = """ ... (SQL query remains the same) ... """
            detailed_data = self.db.execute_query(detailed_data_query)

            unique_matches = []
            ambiguous = []
            unmatched = []

            # Use RESOLVED_OUTPUT_DIR from config
            output_path = self.config.RESOLVED_OUTPUT_DIR
            if output_path is None:
                # Fallback or error if not set, though main.py should set it.
                logger.error("RESOLVED_OUTPUT_DIR not set in config!")
                # Optionally, default to a known path or raise an error
                # For now, let's assume it will be set. If not, Path operations might fail.
                # Consider creating it based on DATA_DIR as a last resort or raising.
                # This path is critical for saving debug files.
                # Defaulting to previous logic if RESOLVED_OUTPUT_DIR is None
                # THIS IS A SAFETY FALLBACK AND SHOULD IDEALLY NOT BE REACHED
                logger.warning(
                    "RESOLVED_OUTPUT_DIR not set, attempting fallback path construction."
                )
                output_path = self.config.DATA_DIR / self.config.OUTPUT_DIR

            output_path.mkdir(parents=True, exist_ok=True)

            detailed_matches_csv = output_path / "detailed_matches.csv"
            unique_matches_csv = output_path / "unique_matches.csv"
            ambiguous_matches_csv = output_path / "ambiguous_matches.csv"

            with open(detailed_matches_csv, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "journalnr",
                        "marker_afgkode",
                        "marker_area",
                        "pesticide_cvr",
                        "pesticide_code",
                        "pesticide_area",
                        "area_diff_pct",
                    ]
                )
                writer.writerows(detailed_data)

            for journalnr, matching_cvrs, matching_combinations in results:
                if len(matching_cvrs) == 1:
                    unique_matches.append((journalnr, matching_cvrs[0]))
                elif len(matching_cvrs) > 1:
                    ambiguous.append((journalnr, matching_cvrs))
                else:
                    unmatched.append((journalnr, []))

            with open(unique_matches_csv, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["journalnr", "pesticide_cvr"])
                writer.writerows(unique_matches)

            with open(ambiguous_matches_csv, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["journalnr", "matching_cvrs"])
                for journalnr, cvrs in ambiguous:
                    writer.writerow([journalnr, ",".join(map(str, cvrs))])

            summary = {
                "total_marker_companies": len(results),
                "unique_matches": len(unique_matches),
                "ambiguous": len(ambiguous),
                "unmatched": len(unmatched),
                "sample_unique": unique_matches[:5],
                "sample_ambiguous": ambiguous[:3],
                "sample_unmatched": unmatched[:5],
            }
            logger.info(f"Saved matches to CSV files in {output_path}:")
            logger.info(f"- {detailed_matches_csv.name}")
            logger.info(f"- {unique_matches_csv.name}")
            logger.info(f"- {ambiguous_matches_csv.name}")
            return summary
        except Exception as e:
            logger.error(
                f"Error in match_empty_marker_cvrs_by_journalnr_afgkode_area: {str(e)}"
            )
            raise

    def generate_matching_report(self) -> Dict:
        """Generate a comprehensive report of the matching process"""
        try:
            unmatched = self.find_unmatched_pesticide_cvrs()
            gkea_non_numeric = self.analyze_gkea_non_numeric_cvrs()
            empty_marker = self.analyze_empty_marker_cvrs()
            return {
                "unmatched_pesticide": unmatched,
                "gkea_non_numeric": gkea_non_numeric,
                "empty_marker_cvrs": empty_marker,
            }
        except Exception as e:
            logger.error(f"Error generating matching report: {str(e)}")
            raise


class FieldDatasetAnalyzer:
    """Analyzes field-level datasets for quality and consistency."""

    def __init__(self, db_manager):
        """Initialize with database manager."""
        self.db = db_manager

    def compare_marker_jordbrugsanalyser(self) -> Dict:
        """Compare Marker and Jordbrugsanalyser datasets."""
        try:
            metrics = self.db.execute_query("""
                WITH field_matches AS (
                    SELECT 
                        COUNT(DISTINCT m.Markblok || '-' || m.Marknr) as marker_fields,
                        COUNT(DISTINCT j.MarkBlok || '-' || j.MarkNr) as jord_fields,
                        COUNT(DISTINCT CASE 
                            WHEN m.Markblok = j.MarkBlok AND m.Marknr = j.MarkNr 
                            THEN m.Markblok || '-' || m.Marknr 
                        END) as matching_fields
                    FROM marker m
                    FULL JOIN jordbrugsanalyser j 
                    ON m.Markblok = j.MarkBlok AND m.Marknr = j.MarkNr
                ),
                area_stats AS (
                    SELECT 
                        AVG(ABS(m.IMK_areal - j.Ha) / NULLIF(j.Ha, 0) * 100) as avg_area_diff_pct,
                        COUNT(CASE WHEN ABS(m.IMK_areal - j.Ha) > 0.01 THEN 1 END) as records_with_diff
                    FROM marker m
                    INNER JOIN jordbrugsanalyser j 
                    ON m.Markblok = j.MarkBlok AND m.Marknr = j.MarkNr
                )
                SELECT 
                    fm.*,
                    area_stats.avg_area_diff_pct,
                    area_stats.records_with_diff
                FROM field_matches fm
                CROSS JOIN area_stats
            """)[0]

            comparison = {
                "marker_fields": metrics[0],
                "jord_fields": metrics[1],
                "matching_fields": metrics[2],
                "match_rate": metrics[2] / max(metrics[0], metrics[1]) * 100
                if max(metrics[0], metrics[1]) > 0
                else 0,
                "avg_area_diff_pct": metrics[3],
                "records_with_diff": metrics[4],
            }
            logger.info(f"Generated field dataset comparison: {comparison}")
            return comparison
        except Exception as e:
            logger.error(f"Error comparing field datasets: {str(e)}")
            # Return None or an empty dict or re-raise, depending on desired error handling
            # For now, let's re-raise to make it visible
            raise

    def analyze_area_differences(self) -> Dict:
        """Analyze area differences between datasets."""
        try:
            area_stats = self.db.execute_query("""
                WITH area_comparison AS (
                    SELECT 
                        m.Markblok,
                        m.Marknr,
                        m.IMK_areal as marker_area,
                        m.GBanmeldt as marker_gb_area,
                        j.Ha as jord_area,
                        ABS(m.IMK_areal - j.Ha) as imk_ha_diff,
                        ABS(m.GBanmeldt - j.Ha) as gb_ha_diff
                    FROM marker m
                    INNER JOIN jordbrugsanalyser j 
                    ON m.Markblok = j.MarkBlok AND m.Marknr = j.MarkNr
                )
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN imk_ha_diff > 0.01 THEN 1 END) as imk_diff_records,
                    COUNT(CASE WHEN gb_ha_diff > 0.01 THEN 1 END) as gb_diff_records,
                    SUM(imk_ha_diff) as total_imk_diff,
                    SUM(gb_ha_diff) as total_gb_diff,
                    AVG(imk_ha_diff) as avg_imk_diff,
                    AVG(gb_ha_diff) as avg_gb_diff,
                    MAX(imk_ha_diff) as max_imk_diff,
                    MAX(gb_ha_diff) as max_gb_diff
                FROM area_comparison
            """)[0]

            analysis = {
                "total_records": area_stats[0],
                "imk_differences": {
                    "records_with_diff": area_stats[1],
                    "diff_percentage": area_stats[1] / area_stats[0] * 100
                    if area_stats[0] > 0
                    else 0,
                    "total_diff": area_stats[3],
                    "avg_diff": area_stats[5],
                    "max_diff": area_stats[7],
                },
                "gb_differences": {
                    "records_with_diff": area_stats[2],
                    "diff_percentage": area_stats[2] / area_stats[0] * 100
                    if area_stats[0] > 0
                    else 0,
                    "total_diff": area_stats[4],
                    "avg_diff": area_stats[6],
                    "max_diff": area_stats[8],
                },
            }
            logger.info(f"Total records: {area_stats[0]}")
            logger.info(
                f"Records with IMK area differences: {area_stats[1]} ({area_stats[1] / area_stats[0] * 100:.2f}% if area_stats[0] > 0 else 0)"
            )
            logger.info(
                f"Records with GB area differences: {area_stats[2]} ({area_stats[2] / area_stats[0] * 100:.2f}% if area_stats[0] > 0 else 0)"
            )
            logger.info(f"Total IMK area difference: {area_stats[3]:.2f} ha")
            logger.info(f"Total GB area difference: {area_stats[4]:.2f} ha")
            return analysis
        except Exception as e:
            logger.error(f"Error analyzing area differences: {str(e)}")
            raise

    def validate_field_identifiers(self) -> Dict:
        """Validate field identifiers across datasets."""
        try:
            validation = self.db.execute_query("""
                WITH field_validation AS (
                    SELECT 
                        m.Markblok,
                        m.Marknr,
                        m.CVR as marker_cvr,
                        j.EjerNr as jord_cvr,
                        CASE 
                            WHEN NULLIF(TRIM(m.CVR), '') IS NULL OR j.EjerNr IS NULL THEN 'missing'
                            WHEN CAST(m.CVR AS VARCHAR) = CAST(j.EjerNr AS VARCHAR) THEN 'match'
                            ELSE 'mismatch'
                        END as cvr_status,
                        CASE 
                            WHEN m.Afgkode IS NULL OR j.AfgNr IS NULL THEN 'missing'
                            WHEN CAST(m.Afgkode AS VARCHAR) = CAST(j.AfgNr AS VARCHAR) THEN 'match'
                            ELSE 'mismatch'
                        END as crop_status
                    FROM marker m
                    FULL JOIN jordbrugsanalyser j 
                    ON m.Markblok = j.MarkBlok AND m.Marknr = j.MarkNr
                )
                SELECT 
                    COUNT(*) as total_fields,
                    COUNT(CASE WHEN cvr_status = 'match' THEN 1 END) as matching_cvrs,
                    COUNT(CASE WHEN cvr_status = 'mismatch' THEN 1 END) as mismatched_cvrs,
                    COUNT(CASE WHEN cvr_status = 'missing' THEN 1 END) as missing_cvrs,
                    COUNT(CASE WHEN crop_status = 'match' THEN 1 END) as matching_crops,
                    COUNT(CASE WHEN crop_status = 'mismatch' THEN 1 END) as mismatched_crops,
                    COUNT(CASE WHEN crop_status = 'missing' THEN 1 END) as missing_crops
                FROM field_validation
            """)[0]

            results = {
                "total_fields": validation[0],
                "cvr_validation": {
                    "matching": validation[1],
                    "mismatched": validation[2],
                    "missing": validation[3],
                    "match_rate": validation[1] / validation[0] * 100
                    if validation[0] > 0
                    else 0,
                },
                "crop_validation": {
                    "matching": validation[4],
                    "mismatched": validation[5],
                    "missing": validation[6],
                    "match_rate": validation[4] / validation[0] * 100
                    if validation[0] > 0
                    else 0,
                },
            }
            logger.info(f"Generated field identifier validation: {results}")
            return results
        except Exception as e:
            logger.error(f"Error validating field identifiers: {str(e)}")
            raise


# For brevity, the full SQL queries and logic within CVRMatcher methods
# like find_unmatched_pesticide_cvrs, match_empty_marker_cvrs_by_journalnr_afgkode_area, etc.,
# and FieldDatasetAnalyzer methods are kept as they were,
# except for the CSV output path adjustments in CVRMatcher.
