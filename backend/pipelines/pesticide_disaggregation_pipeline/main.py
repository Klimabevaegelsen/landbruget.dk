import argparse
import logging
import os
from datetime import datetime

# import sys # Removed as it's no longer used
from pathlib import Path

# This assumes main.py is in pesticide_analyzer directory
PROJECT_ROOT = Path(__file__).resolve().parent
# The following lines are removed:
# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.append(str(PROJECT_ROOT))

from analysis.cvr_matching_and_quality import CVRMatcher, FieldDatasetAnalyzer
from analysis.disaggregation import PesticideDisaggregator
from config import Config
from database import DatabaseManager
from export import PesticideExporter
from loader import DatasetLoader

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Add debug logging configuration (if needed, or manage globally)


def setup_and_load_data(
    config: Config, db_manager: DatabaseManager, dataset_loader: DatasetLoader, pesticide_year: int = None
) -> int:
    """Loads datasets and initializes the pending pesticide rows table."""
    logger.info(f"Step 1: Loading datasets for pesticide year {pesticide_year or config.PESTICIDE_YEAR}...")
    dataset_loader.load_datasets(pesticide_year)

    disaggregator = PesticideDisaggregator(db_manager, config)  # Needed for create_disaggregated_table
    disaggregator.create_disaggregated_table()

    # Filter out nopesticides=1 rows since they represent areas with no pesticide applications
    db_manager.execute_query("""
        CREATE OR REPLACE TABLE pending_pesticide_rows AS 
        SELECT * FROM pesticide 
        WHERE nopesticides IS NULL OR nopesticides != 1
    """)
    initial_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]

    # Log filtering statistics
    total_pesticide_count = db_manager.execute_query("SELECT COUNT(*) FROM pesticide")[0][0]
    nopesticides_count = total_pesticide_count - initial_pending_count
    logger.info(f"Total pesticide records: {total_pesticide_count}")
    logger.info(
        f"Excluded nopesticides=1 records: {nopesticides_count} ({nopesticides_count / total_pesticide_count * 100:.2f}%)"
    )
    logger.info(f"Initial pending pesticide rows (for disaggregation): {initial_pending_count}")

    # Save debug totals early, after pending_pesticide_rows is created
    disaggregator.save_cvr_crop_totals_for_debugging(pending_rows_table="pending_pesticide_rows")
    logger.info("Initial CVR-Crop totals saved for debugging.")
    return initial_pending_count


def perform_initial_analysis(cvr_matcher: CVRMatcher, field_analyzer: FieldDatasetAnalyzer):
    """Runs initial CVR matching and field dataset quality analysis."""
    logger.info("Step 2: Performing initial CVR matching and quality analysis...")
    matching_report = cvr_matcher.generate_matching_report()
    logger.info("CVR Matching Report Summary:")
    logger.info(f"  Total unmatched pesticide CVRs: {matching_report['unmatched_pesticide']['total_unmatched']}")

    logger.info(f"  Total empty marker CVRs: {matching_report['empty_marker_cvrs']['total_empty']}")

    logger.info("Field Dataset Validation: Jordbrugsanalyser comparison skipped (dataset removed for simplification)")


def run_disaggregation_strategies(
    disaggregator: PesticideDisaggregator,
    db_manager: DatabaseManager,
    initial_pending_count: int,
):
    """Runs the different disaggregation strategies."""
    logger.info("Step 3: Running disaggregation strategies...")
    current_pending_count = initial_pending_count

    # Strategy 1: Marker CVR-Area Match
    logger.info("Running Marker CVR-Area Match strategy...")
    marker_processed_ids = disaggregator.disaggregate_by_marker_match(pending_rows_table="pending_pesticide_rows")
    if marker_processed_ids:
        ids_tuple = tuple(marker_processed_ids)
        if ids_tuple:
            if len(ids_tuple) == 1:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID = {ids_tuple[0]}"
                )
            else:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID IN {ids_tuple}"
                )
        current_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]
        logger.info(
            f"After Marker Match, {len(marker_processed_ids)} distinct pesticide rows processed. Pending: {current_pending_count}"
        )
    else:
        logger.info("No rows processed by Marker CVR-Area Match.")

    # Strategy 3: Marker Non-Organic CVR-Area Match
    logger.info("Running Marker Non-Organic CVR-Area Match strategy...")
    # Ensure the disaggregator instance has the spatial extension loaded if _get_organic_marker_field_ids is called for the first time here.
    # This depends on how DatabaseManager handles connections and if it pre-loads spatial.
    # The disaggregator._get_organic_marker_field_ids method itself does not load spatial; it assumes the db connection can handle it.
    marker_non_organic_processed_ids = disaggregator.disaggregate_by_marker_non_organic_match(
        pending_rows_table="pending_pesticide_rows"
    )
    if marker_non_organic_processed_ids:
        ids_tuple = tuple(marker_non_organic_processed_ids)
        if ids_tuple:  # Check if tuple is not empty
            if len(ids_tuple) == 1:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID = {ids_tuple[0]}"
                )
            else:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID IN {ids_tuple}"
                )
        current_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]
        logger.info(
            f"After Marker Non-Organic Match, {len(marker_non_organic_processed_ids)} distinct pesticide rows processed. Pending: {current_pending_count}"
        )
    else:
        logger.info("No rows processed by Marker Non-Organic CVR-Area Match.")

    # Strategy 3: Partial Field Coverage (Single Field)
    logger.info("Running Partial Field Coverage (Single Field) strategy...")
    partial_coverage_processed_ids = disaggregator.disaggregate_by_partial_field_coverage(
        pending_rows_table="pending_pesticide_rows"
    )
    if partial_coverage_processed_ids:
        ids_tuple = tuple(partial_coverage_processed_ids)
        if ids_tuple:
            if len(ids_tuple) == 1:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID = {ids_tuple[0]}"
                )
            else:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID IN {ids_tuple}"
                )
        current_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]
        logger.info(
            f"After Partial Field Coverage, {len(partial_coverage_processed_ids)} distinct pesticide rows processed. Pending: {current_pending_count}"
        )
    else:
        logger.info("No rows processed by Partial Field Coverage strategy.")

    # Strategy 4: Adjacent Fields Single Cluster
    logger.info("Running Adjacent Fields Single Cluster strategy...")
    adjacent_cluster_processed_ids = disaggregator.disaggregate_by_adjacent_fields_single_cluster(
        pending_rows_table="pending_pesticide_rows"
    )
    if adjacent_cluster_processed_ids:
        ids_tuple = tuple(adjacent_cluster_processed_ids)
        if ids_tuple:
            if len(ids_tuple) == 1:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID = {ids_tuple[0]}"
                )
            else:
                db_manager.execute_query(
                    f"DELETE FROM pending_pesticide_rows WHERE OriginalPesticideRowID IN {ids_tuple}"
                )
        current_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]
        logger.info(
            f"After Adjacent Fields Single Cluster, {len(adjacent_cluster_processed_ids)} distinct pesticide rows processed. Pending: {current_pending_count}"
        )
    else:
        logger.info("No rows processed by Adjacent Fields Single Cluster strategy.")

    # Add more strategies here if needed


def finalize_and_save_results(db_manager: DatabaseManager, config: Config, pipeline_start_time=None):
    """Calculates final counts and saves results using standardized export."""
    logger.info("Step 4: Finalizing and saving results...")

    # Initialize the exporter
    exporter = PesticideExporter(db_manager, config, pipeline_start_time)

    final_disaggregated_count = db_manager.execute_query("SELECT COUNT(*) FROM disaggregated_pesticide_applications")[
        0
    ][0]
    final_pending_count = db_manager.execute_query("SELECT COUNT(*) FROM pending_pesticide_rows")[0][0]

    logger.info(
        f"Disaggregation complete. Total rows in 'disaggregated_pesticide_applications': {final_disaggregated_count}"
    )
    logger.info(f"Final remaining pending pesticide rows: {final_pending_count}")

    # Calculate and log total and unallocated acreage (excluding nopesticides=1 records)
    total_original_acreage_query = """
        SELECT COALESCE(SUM(TRY_CAST("AcreageSize" AS DOUBLE)), 0) 
        FROM pesticide 
        WHERE nopesticides IS NULL OR nopesticides != 1
    """
    total_original_acreage = db_manager.execute_query(total_original_acreage_query)[0][0]
    logger.info(
        f"Total original AcreageSize (excluding nopesticides=1) from 'pesticide' table (direct sum): {total_original_acreage:.2f}"
    )

    # Calculate total original acreage based on MAX(AcreageSize) per CVR/Crop combination (excluding nopesticides=1)
    total_original_acreage_max_per_cvr_crop_query = """
    WITH MaxAreaPerCVRCrop AS (
        SELECT
            MAX(TRY_CAST(\"AcreageSize\" AS DOUBLE)) AS MaxAcreageSize
        FROM pesticide
        WHERE \"CompanyRegistrationNumber\" IS NOT NULL AND \"Code\" IS NOT NULL
            AND (nopesticides IS NULL OR nopesticides != 1)
        GROUP BY CAST(CAST(\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR), CAST(CAST(\"Code\" AS BIGINT) AS VARCHAR)
    )
    SELECT COALESCE(SUM(MaxAcreageSize), 0) FROM MaxAreaPerCVRCrop;
    """
    total_original_acreage_max_per_cvr_crop = db_manager.execute_query(total_original_acreage_max_per_cvr_crop_query)[
        0
    ][0]
    logger.info(
        f"Total original AcreageSize (excluding nopesticides=1) from 'pesticide' table (SUM of MAX(AcreageSize) per CVR/Crop): {total_original_acreage_max_per_cvr_crop:.2f}"
    )

    total_unallocated_acreage_query = (
        'SELECT COALESCE(SUM(TRY_CAST("AcreageSize" AS DOUBLE)), 0) FROM pending_pesticide_rows'
    )
    total_unallocated_acreage = db_manager.execute_query(total_unallocated_acreage_query)[0][0]
    logger.info(f"Total unallocated AcreageSize from 'pending_pesticide_rows': {total_unallocated_acreage:.2f}")

    # Calculate unallocated acreage based on MAX(AcreageSize) per CVR/Crop from pending_pesticide_rows
    unallocated_acreage_max_per_cvr_crop_pending_query = """
    WITH MaxAreaPerCVRCropPending AS (
        SELECT
            MAX(TRY_CAST(\"AcreageSize\" AS DOUBLE)) AS MaxAcreageSize
        FROM pending_pesticide_rows
        WHERE \"CompanyRegistrationNumber\" IS NOT NULL AND \"Code\" IS NOT NULL
        GROUP BY CAST(CAST(\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR), CAST(CAST(\"Code\" AS BIGINT) AS VARCHAR)
    )
    SELECT COALESCE(SUM(MaxAcreageSize), 0) FROM MaxAreaPerCVRCropPending;
    """
    unallocated_acreage_max_per_cvr_crop_pending = db_manager.execute_query(
        unallocated_acreage_max_per_cvr_crop_pending_query
    )[0][0]
    logger.info(
        f"Total unallocated AcreageSize from 'pending_pesticide_rows' (SUM of MAX(AcreageSize) per CVR/Crop): {unallocated_acreage_max_per_cvr_crop_pending:.2f}"
    )

    # Calculate disaggregated area and percentage based on the "SUM of MAX" figures for total and unallocated
    if total_original_acreage_max_per_cvr_crop > 0:
        disaggregated_acreage_sum_max_basis = (
            total_original_acreage_max_per_cvr_crop - unallocated_acreage_max_per_cvr_crop_pending
        )
        percentage_area_disaggregated_sum_max_basis = (
            disaggregated_acreage_sum_max_basis / total_original_acreage_max_per_cvr_crop
        ) * 100
        logger.info(
            f"Total disaggregated AcreageSize (based on SUM of MAX per CVR/Crop for original and pending): {disaggregated_acreage_sum_max_basis:.2f}"
        )
        logger.info(
            f"Percentage of total area disaggregated (based on SUM of MAX per CVR/Crop for original and pending): {percentage_area_disaggregated_sum_max_basis:.2f}%"
        )
    else:
        logger.info(
            "Total original acreage (SUM of MAX per CVR/Crop) is zero, cannot calculate this disaggregation percentage."
        )

    # Existing calculations based on direct sum (for comparison or if still needed)
    if total_original_acreage > 0:
        disaggregated_acreage = total_original_acreage - total_unallocated_acreage
        percentage_area_disaggregated = (disaggregated_acreage / total_original_acreage) * 100
        logger.info(f"Total disaggregated AcreageSize: {disaggregated_acreage:.2f}")
        logger.info(f"Percentage of total area disaggregated: {percentage_area_disaggregated:.2f}%")
    else:
        logger.info("Total original acreage is zero, cannot calculate area disaggregation percentage.")

    # Export using the standardized exporter
    exporter.export_disaggregated_data()

    # Generate schema documentation
    exporter.generate_schema_documentation()

    # Commit schema to GitHub (optional, based on environment)
    environment = os.getenv("ENVIRONMENT", "dev")
    if environment == "prod":
        exporter.commit_schema_to_github()

    # Get and log export summary
    summary = exporter.get_export_summary()
    logger.info(f"Export Summary: {summary}")

    # Keep the legacy local export for debugging/compatibility
    output_dir = config.RESOLVED_OUTPUT_DIR
    pending_output_path = output_dir / "unallocated_pesticide_rows.parquet"
    db_manager.execute_query(
        f"COPY (SELECT * FROM pending_pesticide_rows) TO '{str(pending_output_path)}' (FORMAT PARQUET)"
    )
    logger.info(f"Saved unallocated pesticide rows to {pending_output_path}")


def analyze_pending_rows(db_manager: DatabaseManager, config: Config):
    """Analyzes the remaining pending pesticide rows for reasons they were not disaggregated."""
    logger.info("Step 5: Analyzing remaining pending pesticide rows...")

    # --- Debugging: Inspect CVR columns and types --- START
    logger.info("--- Finished Data Exploration for CVR columns ---")
    # --- Debugging: Inspect CVR columns and types --- END

    # Total pending rows and area for context
    total_pending_query = 'SELECT COUNT(DISTINCT "OriginalPesticideRowID"), COALESCE(SUM(TRY_CAST("AcreageSize" AS DOUBLE)), 0) FROM pending_pesticide_rows'
    total_pending_count, total_pending_direct_sum_area = db_manager.execute_query(total_pending_query)[0]

    sum_max_total_pending_query = """
    WITH MaxAcreagePerCVRCropInPending_Overall AS (
        SELECT 
            MAX(TRY_CAST("AcreageSize" AS DOUBLE)) AS MaxAcreage
        FROM pending_pesticide_rows
        WHERE "CompanyRegistrationNumber" IS NOT NULL AND "Code" IS NOT NULL
        GROUP BY CAST(CAST("CompanyRegistrationNumber" AS BIGINT) AS VARCHAR), CAST(CAST("Code" AS BIGINT) AS VARCHAR)
    )
    SELECT COALESCE(SUM(MaxAcreage), 0) FROM MaxAcreagePerCVRCropInPending_Overall;
    """
    total_pending_sum_max_area = db_manager.execute_query(sum_max_total_pending_query)[0][0]
    logger.info(
        f"Total pending rows for analysis: {total_pending_count}, Total Direct Sum Area: {total_pending_direct_sum_area:.2f}, Total Sum(Max) Area: {total_pending_sum_max_area:.2f}"
    )

    # Define the SELECT statement part of the MaxAcreagePerCVRCropInPending CTE
    # This will be used to construct combined WITH clauses where needed.
    max_acreage_select_sql = """
        SELECT
            CAST(CAST(\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR) AS CVR_Str,
            CAST(CAST(\"Code\" AS BIGINT) AS VARCHAR) AS Crop_Str,
            MAX(TRY_CAST(\"AcreageSize\" AS DOUBLE)) AS MaxAcreage
        FROM pending_pesticide_rows
        WHERE \"CompanyRegistrationNumber\" IS NOT NULL AND \"Code\" IS NOT NULL
        GROUP BY 1, 2
    """
    # Full CTE definition for cases where it's the only CTE
    max_acreage_cte_definition_standalone = f"WITH MaxAcreagePerCVRCropInPending AS ({max_acreage_select_sql})\n"

    # 1. Unmatched CVRs (CVR in pending_pesticide_rows not in marker OR gkea)
    unmatched_cvr_query = """
    SELECT
        COUNT(DISTINCT ppr."OriginalPesticideRowID") AS UnmatchedCVR_RowCount,
        COALESCE(SUM(TRY_CAST(ppr."AcreageSize" AS DOUBLE)), 0) AS UnmatchedCVR_TotalArea
    FROM pending_pesticide_rows ppr
    WHERE
        ppr."CompanyRegistrationNumber" IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND CAST(CAST(ppr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(m.cvr_number AS VARCHAR)
        )
        -- REMOVED: GKEA CVR check - GKEA data no longer used
        ;
    """
    row_count, direct_sum_area = db_manager.execute_query(unmatched_cvr_query)[0]

    unmatched_cvr_sum_max_query = (
        max_acreage_cte_definition_standalone
        + """
    SELECT COALESCE(SUM(mapc.MaxAcreage), 0)
    FROM MaxAcreagePerCVRCropInPending mapc
    WHERE
        NOT EXISTS (
            SELECT 1 FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND mapc.CVR_Str = CAST(m.cvr_number AS VARCHAR)
        )
        -- REMOVED: GKEA CVR check - GKEA data no longer used
        ;
    """
    )
    sum_max_area = db_manager.execute_query(unmatched_cvr_sum_max_query)[0][0]
    logger.info(
        f"  Pending rows: Unmatched CVR (Pesticide.CVR not in Marker.CVR): {row_count} (Direct Sum Area: {direct_sum_area:.2f}, Sum(Max) Area: {sum_max_area:.2f})"
    )

    # Save details for rows with unmatched CVRs
    if row_count > 0:
        logger.info(f"  Saving details for {row_count} rows with unmatched CVRs...")
        details_unmatched_cvr_query = """
        SELECT ppr.* 
        FROM pending_pesticide_rows ppr
        WHERE
            ppr."CompanyRegistrationNumber" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM marker m
                WHERE m."CVR" IS NOT NULL AND CAST(m."CVR" AS VARCHAR) != ''
                  AND CAST(CAST(ppr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(m."CVR" AS VARCHAR)
            )
            -- REMOVED: GKEA CVR check - GKEA data no longer used
        ORDER BY ppr."CompanyRegistrationNumber", ppr."OriginalPesticideRowID"
        """
        unmatched_cvr_output_path = config.RESOLVED_OUTPUT_DIR / "debug_unmatched_cvr_details.csv"
        try:
            db_manager.execute_query(
                f"COPY ({details_unmatched_cvr_query}) TO '{str(unmatched_cvr_output_path)}' (HEADER, DELIMITER ',');"
            )
            logger.info(f"    Details for unmatched CVRs saved to: {unmatched_cvr_output_path}")
        except Exception as e:
            logger.error(f"    Could not save details for unmatched CVRs to CSV: {e}")

    # 2. Unmatched CVR/Crop combinations
    unmatched_cvr_crop_query = """
    WITH MatchedCVRsInPending AS ( 
        SELECT DISTINCT ppr."OriginalPesticideRowID", ppr."CompanyRegistrationNumber", ppr."Code", ppr."AcreageSize"
        FROM pending_pesticide_rows ppr
        WHERE ppr."CompanyRegistrationNumber" IS NOT NULL AND (
            EXISTS (
                SELECT 1 FROM marker m
                WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
                  AND CAST(CAST(ppr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(m.cvr_number AS VARCHAR)
            )
            -- REMOVED: GKEA CVR check - GKEA data no longer used
        )
    )
    SELECT
        COUNT(DISTINCT mcpr."OriginalPesticideRowID") AS UnmatchedCVRCrop_RowCount,
        COALESCE(SUM(TRY_CAST(mcpr."AcreageSize" AS DOUBLE)), 0) AS UnmatchedCVRCrop_TotalArea
    FROM MatchedCVRsInPending mcpr
    WHERE 
        mcpr."Code" IS NOT NULL -- Ensure pesticide crop code is valid (is DOUBLE)
        AND NOT EXISTS ( -- Not in marker with CVR/Crop
            SELECT 1 FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND m.crop_code IS NOT NULL -- Marker crop_code is DOUBLE
              AND CAST(CAST(mcpr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(m.cvr_number AS VARCHAR)
              AND CAST(CAST(mcpr."Code" AS BIGINT) AS VARCHAR) = CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR)
        )
        -- REMOVED: GKEA CVR/Crop check - GKEA data no longer used
        ;
    """
    row_count, direct_sum_area = db_manager.execute_query(unmatched_cvr_crop_query)[0]

    unmatched_cvr_crop_sum_max_query = (
        max_acreage_cte_definition_standalone
        + """
    SELECT COALESCE(SUM(mapc.MaxAcreage), 0)
    FROM MaxAcreagePerCVRCropInPending mapc
    WHERE
        ( -- CVR is matched in marker or GKEA
            EXISTS (
                SELECT 1 FROM marker m
                WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
                  AND mapc.CVR_Str = CAST(m.cvr_number AS VARCHAR)
            )
            -- REMOVED: GKEA CVR check - GKEA data no longer used
        )
        AND NOT EXISTS ( -- CVR/Crop not in marker
            SELECT 1 FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND m.crop_code IS NOT NULL
              AND mapc.CVR_Str = CAST(m.cvr_number AS VARCHAR)
              AND mapc.Crop_Str = CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR)
        )
        -- REMOVED: GKEA CVR/Crop check - GKEA data no longer used
        ;
    """
    )
    sum_max_area = db_manager.execute_query(unmatched_cvr_crop_sum_max_query)[0][0]
    logger.info(
        f"  Pending rows: CVR matched, but CVR/Crop unmatched (Pesticide.CVR/Code not in Marker for that CVR): {row_count} (Direct Sum Area: {direct_sum_area:.2f}, Sum(Max) Area: {sum_max_area:.2f})"
    )

    # Save details for rows where CVR/Crop is unmatched
    if row_count > 0:
        logger.info(f"  Saving details for {row_count} rows where Pesticide.CVR/Code not in Marker for that CVR...")
        details_unmatched_cvr_crop_query = """
        SELECT ppr.* 
        FROM pending_pesticide_rows ppr
        WHERE
            ppr."CompanyRegistrationNumber" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM marker m
                WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
                  AND CAST(CAST(ppr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(m.cvr_number AS VARCHAR)
            )
            AND NOT EXISTS (
                SELECT 1 FROM gkea g
                WHERE g.unnamed__2 IS NOT NULL AND CAST(g.unnamed__2 AS VARCHAR) != ''
                  AND CAST(CAST(ppr."CompanyRegistrationNumber" AS BIGINT) AS VARCHAR) = CAST(g.unnamed__2 AS VARCHAR)
            )
        ORDER BY ppr."CompanyRegistrationNumber", ppr."OriginalPesticideRowID"
        """
        details_output_path = config.RESOLVED_OUTPUT_DIR / "debug_unmatched_cvr_crop_details.csv"
        try:
            db_manager.execute_query(
                f"COPY ({details_unmatched_cvr_crop_query}) TO '{str(details_output_path)}' (HEADER, DELIMITER ',');"
            )
            logger.info(f"    Details saved to: {details_output_path}")
        except Exception as e:
            logger.error(f"    Could not save details for unmatched CVR/Crop to CSV: {e}")

    # 3. AcreageSize > Total Marker Area (for matched CVR/Crop in Marker)
    # Corrected direct sum query with explicit CTE naming
    acreage_gt_marker_direct_sum_query = """
    WITH MarkerCVRCropTotals_ForDirectSum AS (
        SELECT
            CAST(m.cvr_number AS VARCHAR) AS \"CVR_VARCHAR\", 
            CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) AS \"CropCode_VARCHAR\",
            SUM(TRY_CAST(m.area_ha AS DOUBLE)) AS \"TotalMarkerArea\"
        FROM marker m
        WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
          AND m.crop_code IS NOT NULL
        GROUP BY 1, 2
        HAVING SUM(TRY_CAST(m.area_ha AS DOUBLE)) > 0
    )
    SELECT
        COUNT(DISTINCT ppr.\"OriginalPesticideRowID\") AS AcreageTooLargeMarker_RowCount,
        COALESCE(SUM(TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE)), 0) AS AcreageTooLargeMarker_TotalArea
    FROM pending_pesticide_rows ppr
    JOIN MarkerCVRCropTotals_ForDirectSum m_totals
        ON CAST(CAST(ppr.\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR) = m_totals.\"CVR_VARCHAR\"
       AND CAST(CAST(ppr.\"Code\" AS BIGINT) AS VARCHAR) = m_totals.\"CropCode_VARCHAR\"
    WHERE ppr.\"CompanyRegistrationNumber\" IS NOT NULL AND ppr.\"Code\" IS NOT NULL
      AND TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) > m_totals.\"TotalMarkerArea\";
    """
    row_count, direct_sum_area = db_manager.execute_query(acreage_gt_marker_direct_sum_query)[0]

    acreage_gt_marker_sum_max_query = f"""
    WITH 
        MaxAcreagePerCVRCropInPending AS ({max_acreage_select_sql}),
        MarkerCVRCropTotals_ForSumMax AS (
            SELECT
                CAST(m.cvr_number AS VARCHAR) AS \"CVR_VARCHAR\",
                CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) AS \"CropCode_VARCHAR\",
                SUM(TRY_CAST(m.area_ha AS DOUBLE)) AS \"TotalMarkerArea\"
            FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND m.crop_code IS NOT NULL
            GROUP BY 1, 2
            HAVING SUM(TRY_CAST(m.area_ha AS DOUBLE)) > 0
        )
    SELECT COALESCE(SUM(mapc.MaxAcreage), 0)
    FROM MaxAcreagePerCVRCropInPending mapc
    JOIN MarkerCVRCropTotals_ForSumMax m_totals
        ON mapc.CVR_Str = m_totals.\"CVR_VARCHAR\"
       AND mapc.Crop_Str = m_totals.\"CropCode_VARCHAR\"
    WHERE mapc.MaxAcreage > m_totals.\"TotalMarkerArea\";
    """
    sum_max_area = db_manager.execute_query(acreage_gt_marker_sum_max_query)[0][0]
    logger.info(
        f"  Pending rows: MaxAcreageForCVRCrop > total Marker area (for CVR/Crop): {row_count} (Direct Sum Area: {direct_sum_area:.2f}, Sum(Max) Area: {sum_max_area:.2f})"
    )

    # Save details for rows where AcreageSize > total Marker area
    if row_count > 0:
        logger.info(f"  Saving details for {row_count} rows where Pesticide.AcreageSize > total Marker area...")
        details_acreage_gt_marker_query = """
        WITH MarkerCVRCropTotals AS (
            SELECT
                CAST(m.cvr_number AS VARCHAR) AS \"CVR_VARCHAR\",
                CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) AS \"CropCode_VARCHAR\",
                SUM(TRY_CAST(m.area_ha AS DOUBLE)) AS \"TotalMarkerArea\"
            FROM marker m
            WHERE m.cvr_number IS NOT NULL AND CAST(m.cvr_number AS VARCHAR) != ''
              AND m.crop_code IS NOT NULL
            GROUP BY 1, 2
            HAVING SUM(TRY_CAST(m.area_ha AS DOUBLE)) > 0
        ),
        GKEACVRCropTotals AS (
            SELECT
                CAST(g.unnamed__2 AS VARCHAR) AS \"CVR_VARCHAR\",
                CAST(CAST(g.unnamed__13 AS BIGINT) AS VARCHAR) AS \"CropCode_VARCHAR\",
                SUM(TRY_CAST(g.unnamed__5 AS DOUBLE)) AS \"TotalGKEAArea\"
            FROM gkea g
            WHERE g.unnamed__2 IS NOT NULL AND CAST(g.unnamed__2 AS VARCHAR) != ''
              AND g.unnamed__13 IS NOT NULL
            GROUP BY 1, 2
            HAVING SUM(TRY_CAST(g.unnamed__5 AS DOUBLE)) > 0
        )
        SELECT
            ppr.*, -- Selects all columns from pending_pesticide_rows
            m_totals.\"TotalMarkerArea\",
            (TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) - m_totals.\"TotalMarkerArea\") AS \"Difference_PesticideArea_MarkerArea\",
            CASE
                WHEN m_totals.\"TotalMarkerArea\" IS NOT NULL AND m_totals.\"TotalMarkerArea\" != 0
                THEN ((TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) - m_totals.\"TotalMarkerArea\") / m_totals.\"TotalMarkerArea\") * 100
                ELSE NULL
            END AS \"Percentage_Difference_Marker\",
            g_totals.\"TotalGKEAArea\",
            (TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) - g_totals.\"TotalGKEAArea\") AS \"Difference_PesticideArea_GKEAArea\",
            CASE
                WHEN g_totals.\"TotalGKEAArea\" IS NOT NULL AND g_totals.\"TotalGKEAArea\" != 0
                THEN ((TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) - g_totals.\"TotalGKEAArea\") / g_totals.\"TotalGKEAArea\") * 100
                ELSE NULL
            END AS \"Percentage_Difference_GKEA\"
        FROM pending_pesticide_rows ppr
        LEFT JOIN MarkerCVRCropTotals m_totals -- Changed to LEFT JOIN, but WHERE clause makes it behave like INNER for these rows
            ON CAST(CAST(ppr.\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR) = m_totals.\"CVR_VARCHAR\"
           AND CAST(CAST(ppr.\"Code\" AS BIGINT) AS VARCHAR) = m_totals.\"CropCode_VARCHAR\"
        LEFT JOIN GKEACVRCropTotals g_totals
            ON CAST(CAST(ppr.\"CompanyRegistrationNumber\" AS BIGINT) AS VARCHAR) = g_totals.\"CVR_VARCHAR\"
           AND CAST(CAST(ppr.\"Code\" AS BIGINT) AS VARCHAR) = g_totals.\"CropCode_VARCHAR\"
        WHERE ppr.\"CompanyRegistrationNumber\" IS NOT NULL AND ppr.\"Code\" IS NOT NULL
          AND m_totals.\"TotalMarkerArea\" IS NOT NULL -- Ensures we only get rows that had a marker match
          AND TRY_CAST(ppr.\"AcreageSize\" AS DOUBLE) > m_totals.\"TotalMarkerArea\" -- The primary filter for this debug file
        ORDER BY \"Difference_PesticideArea_MarkerArea\" DESC
        """
        details_output_path = config.RESOLVED_OUTPUT_DIR / "debug_acreage_gt_marker_details.csv"
        try:
            db_manager.execute_query(
                f"COPY ({details_acreage_gt_marker_query}) TO '{str(details_output_path)}' (HEADER, DELIMITER ',');"
            )
            logger.info(f"    Details saved to: {details_output_path}")
        except Exception as e:
            logger.error(f"    Could not save details for AcreageSize > Marker area to CSV: {e}")

    logger.info("  GKEA area comparison analysis skipped - GKEA data removed from pipeline")

    logger.info("Analysis of pending rows complete.")


def main_orchestrator(pesticide_year: int = None):
    """Main function to orchestrate the analysis pipeline."""
    try:
        pipeline_start_time = datetime.now()
        logger.info(f"Starting pesticide analysis pipeline orchestration for year {pesticide_year or 'default'}...")
        config = Config()

        # Override pesticide year if provided
        if pesticide_year:
            config.PESTICIDE_YEAR = pesticide_year
            logger.info(f"Using pesticide year: {pesticide_year} (field year: {pesticide_year + 1})")

        # Resolve and set the output directory on the config object
        actual_output_dir = PROJECT_ROOT / config.OUTPUT_DIR
        actual_output_dir.mkdir(parents=True, exist_ok=True)
        config.RESOLVED_OUTPUT_DIR = actual_output_dir  # Set it on the config instance
        logger.info(f"Using output directory: {actual_output_dir}")

        db_manager = DatabaseManager()
        dataset_loader = DatasetLoader(db_manager, config)
        cvr_matcher = CVRMatcher(db_manager, config)
        field_analyzer = FieldDatasetAnalyzer(db_manager)
        disaggregator_for_strategies = PesticideDisaggregator(db_manager, config)

        # Run pipeline steps
        initial_pending_count = setup_and_load_data(config, db_manager, dataset_loader, pesticide_year)
        perform_initial_analysis(cvr_matcher, field_analyzer)
        run_disaggregation_strategies(disaggregator_for_strategies, db_manager, initial_pending_count)
        finalize_and_save_results(db_manager, config, pipeline_start_time)
        analyze_pending_rows(db_manager, config)

        logger.info("Analysis pipeline completed successfully")

    except FileNotFoundError as e:
        logger.error(f"A dataset file was not found: {str(e)}")
        logger.error("Please check the DATA_DIR path in config.py and ensure all Parquet files are present.")
    except Exception as e:
        logger.error(f"Error in main analysis pipeline: {str(e)}", exc_info=True)


def main():
    """Command-line interface for the pesticide disaggregation pipeline."""
    parser = argparse.ArgumentParser(
        description="Pesticide Spatial Disaggregation Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --year 2021          # Process 2021 pesticides with 2022 fields
  python main.py --year 2022          # Process 2022 pesticides with 2023 fields
  python main.py                      # Use default year from config (2021)

The pipeline implements the Y+1 temporal pattern discovered through analysis:
pesticide year X uses field boundaries from year X+1 for optimal accuracy.

Data is automatically loaded from GCS silver layer. Ensure GCS_BUCKET environment
variable is set to access the data.
        """,
    )

    parser.add_argument("--year", type=int, help="Pesticide year to process (will use field boundaries from year+1)")

    args = parser.parse_args()

    # Verify GCS configuration
    if not os.getenv("GCS_BUCKET"):
        logger.error("GCS_BUCKET environment variable must be set to access silver data")
        logger.error("Example: export GCS_BUCKET=landbrugsdata-raw-data")
        raise EnvironmentError("GCS_BUCKET environment variable not set")

    main_orchestrator(args.year)


if __name__ == "__main__":
    main()
