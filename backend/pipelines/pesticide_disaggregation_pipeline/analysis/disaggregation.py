import logging
from typing import List, Set

# from ..database import DatabaseManager
# from ..config import Config

logger = logging.getLogger(__name__)


# DEPRECATED: Subset sum helper functions removed - no longer needed
# These functions were used for subset sum matching which has been removed due to spurious correlations


class PesticideDisaggregator:
    """Handles disaggregation of pesticide data from company to field level."""

    def __init__(self, db_manager, config):
        """Initialize with database manager and configuration."""
        self.db = db_manager
        self.config = config
        self._organic_marker_field_ids: Set[str] = set()

    def _get_organic_marker_field_ids(self) -> Set[str]:
        """
        Identifies marker field IDs that are considered organic using the direct organic_farming column.
        Much more efficient than spatial joins with oekologiske_arealer.
        Results are cached.
        Returns a set of marker.field_id strings.
        """
        if self._organic_marker_field_ids:
            logger.debug("Returning cached organic marker field IDs.")
            return self._organic_marker_field_ids

        logger.info("Identifying organic marker fields using direct organic_farming column...")
        query = """
        SELECT DISTINCT m.field_id 
        FROM marker m
        WHERE m.organic_farming IS NOT NULL AND UPPER(TRIM(m.organic_farming)) IN ('JA', 'YES', 'TRUE', '1');
        """
        try:
            result_tuples = self.db.execute_query(query)
            self._organic_marker_field_ids = {row[0] for row in result_tuples}
            logger.info(
                f"Identified {len(self._organic_marker_field_ids)} organic marker field IDs using organic_farming column."
            )
        except Exception as e:
            logger.error(
                f"Error identifying organic marker fields: {e}. Proceeding without organic field exclusion for this run."
            )
            self._organic_marker_field_ids = set()
        return self._organic_marker_field_ids

    def create_disaggregated_table(self) -> None:
        """Create the table to store disaggregated pesticide application records."""
        try:
            self.db.execute_query("""
                CREATE TABLE IF NOT EXISTS disaggregated_pesticide_applications (
                    DisaggregatedID VARCHAR PRIMARY KEY,
                    OriginalPesticideRowID BIGINT,
                    CompanyRegistrationNumber VARCHAR,
                    PesticideName VARCHAR,
                    PesticideRegistrationNumber VARCHAR,
                    DosageQuantity DOUBLE,
                    DosageUnit VARCHAR,
                    MatchedFieldID VARCHAR,
                    MatchedBlockID VARCHAR,
                    AllocatedArea DOUBLE,
                    AllocationMethod VARCHAR,
                    MatchConfidence DOUBLE,
                    IsPartialFieldCoverage BOOLEAN DEFAULT FALSE,
                    DisaggregationDate TIMESTAMP
                )
            """)
            logger.info("Table 'disaggregated_pesticide_applications' created or already exists.")
        except Exception as e:
            logger.error(f"Error creating disaggregated_pesticide_applications table: {str(e)}")
            raise

    # REMOVED: GKEA matching strategy
    # GKEA data has been removed from the pipeline as it lacks spatial geometry data
    # and provides only minimal business value (1.30% improvement) with quality concerns.
    def analyze_potential_gkea_matches(self, pending_rows_table: str = "pending_pesticide_rows") -> List[int]:
        """
        DEPRECATED: GKEA matching strategy has been removed.

        GKEA data was removed because:
        - No spatial geometry data available
        - Only 1.30% improvement in match rates
        - 26.2% of matches had poor area alignment (>50% difference)
        - Does not support spatial analysis workflows

        Returns empty list to maintain API compatibility.
        """
        logger.info("GKEA matching strategy has been disabled - GKEA data removed from pipeline")
        return []

    def disaggregate_by_marker_match(self, pending_rows_table: str = "pending_pesticide_rows") -> List[int]:
        """
        Disaggregates individual pesticide rows if their AcreageSize matches the
        total Marker field area for that CVR & Crop.
        """
        processed_row_ids = []
        try:
            insert_query = f"""
                WITH MarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                    PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                    MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence, IsPartialFieldCoverage, DisaggregationDate
                )
                SELECT
                    uuid() as DisaggregatedID,
                    p.OriginalPesticideRowID,
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
                    p.PesticideName, p.PesticideRegistrationNumber, p.DosageQuantity, p.DosageUnit,
                    'marker_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN MarkerFieldCVRCropTotals marker_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = marker_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                JOIN marker m_fields 
                    ON marker_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND marker_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0;
            """
            self.db.execute_query(insert_query)

            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0
                    GROUP BY CVR, CropCode
                ) marker_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = marker_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT};
            """
            processed_id_tuples = self.db.execute_query(select_processed_ids_query)
            processed_row_ids = [pid[0] for pid in processed_id_tuples]

            if processed_row_ids:
                logger.info(
                    f"Marker (PesticideRowArea vs TotalFieldArea): Matched and disaggregated {len(processed_row_ids)} pesticide rows."
                )
            else:
                logger.info(
                    "Marker (PesticideRowArea vs TotalFieldArea): No pesticide rows met the area criteria for disaggregation."
                )
        except Exception as e:
            logger.error(f"Error in Marker (PesticideRowArea vs TotalFieldArea) matching: {str(e)}")
        return processed_row_ids

    def disaggregate_by_marker_non_organic_match(self, pending_rows_table: str = "pending_pesticide_rows") -> List[int]:
        """
        Disaggregates individual pesticide rows if their AcreageSize matches the
        total *non-organic* Marker field area for that CVR & Crop.
        Organic fields are identified using the direct organic_farming column.
        """
        processed_row_ids = []

        logger.info("Attempting disaggregation by Marker Non-Organic match using direct organic_farming column filter.")

        # Get organic field IDs and convert to SQL tuple format
        organic_field_ids = self._get_organic_marker_field_ids()
        if organic_field_ids:
            organic_ids_list = [f"'{field_id}'" for field_id in organic_field_ids]
            organic_ids_sql_tuple = f"({', '.join(organic_ids_list)})"
        else:
            organic_ids_sql_tuple = "('')"

        try:
            insert_query = f"""
                WITH NonOrganicMarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0
                          AND m.field_id NOT IN {organic_ids_sql_tuple} 
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                    PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                    MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence, IsPartialFieldCoverage, DisaggregationDate
                )
                SELECT
                    uuid() as DisaggregatedID,
                    p.OriginalPesticideRowID,
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
                    p.PesticideName, p.PesticideRegistrationNumber, p.DosageQuantity, p.DosageUnit,
                    'marker_non_organic_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = non_organic_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0
                    AND m_fields.field_id NOT IN {organic_ids_sql_tuple}; 
            """
            self.db.execute_query(insert_query)

            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT
                        TRIM(CAST(m.cvr_number AS VARCHAR)) as CVR,
                        TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                        SUM(m.area_ha) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.cvr_number IS NOT NULL 
                          AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                          AND m.crop_code IS NOT NULL AND m.area_ha > 0
                          AND m.field_id NOT IN {organic_ids_sql_tuple}
                    GROUP BY CVR, CropCode
                ) non_organic_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = non_organic_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT};
            """
            processed_id_tuples = self.db.execute_query(select_processed_ids_query)
            processed_row_ids = [pid[0] for pid in processed_id_tuples]

            if processed_row_ids:
                logger.info(
                    f"Marker Non-Organic (PesticideRowArea vs TotalNonOrganicFieldArea): Matched and disaggregated {len(processed_row_ids)} pesticide rows."
                )
            else:
                logger.info(
                    "Marker Non-Organic (PesticideRowArea vs TotalNonOrganicFieldArea): No pesticide rows met the area criteria for disaggregation."
                )
        except Exception as e:
            logger.error(
                f"Error in Marker Non-Organic (PesticideRowArea vs TotalNonOrganicFieldArea) matching: {str(e)}"
            )
        return processed_row_ids

    def save_cvr_crop_totals_for_debugging(self, pending_rows_table: str = "pending_pesticide_rows"):
        """Calculates and saves CVR-Crop total areas from pesticide and Marker for debugging."""
        try:
            output_path = self.config.RESOLVED_OUTPUT_DIR
            if output_path is None:
                logger.error("RESOLVED_OUTPUT_DIR not set in config! Attempting fallback.")
                output_path = self.config.DATA_DIR / self.config.OUTPUT_DIR  # Fallback

            output_path.mkdir(parents=True, exist_ok=True)

            pesticide_totals_query = f"""
                SELECT
                    CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR,
                    TRY_CAST(p.Code AS BIGINT) as CropCode,
                    SUM(p.AcreageSize) as TotalSumPesticideApplicationArea, 
                    COUNT(*) as PesticideApplicationCount,
                    COUNT(DISTINCT p.AcreageSize) as DistinctAcreageValuesCount,
                    MAX(p.AcreageSize) as MaxPesticideApplicationArea
                FROM {pending_rows_table} p
                WHERE p.CompanyRegistrationNumber IS NOT NULL AND p.Code IS NOT NULL AND p.AcreageSize > 0
                GROUP BY CVR, CropCode
                ORDER BY CVR, CropCode
            """
            pesticide_totals_csv = output_path / "debug_pesticide_cvr_crop_totals.csv"
            self.db.execute_query(
                f"COPY ({pesticide_totals_query}) TO '{str(pesticide_totals_csv)}' (HEADER, DELIMITER ',')"
            )
            logger.info(f"Saved pesticide CVR-Crop totals (debug) to {pesticide_totals_csv}")

            # REMOVED: GKEA totals debug output - GKEA data no longer used
            logger.info("GKEA debug output skipped - GKEA data removed from pipeline")

            marker_totals_query = """
                SELECT
                    m.cvr_number as CVR, 
                    TRY_CAST(m.crop_code AS BIGINT) as CropCode,
                    SUM(m.area_ha) as TotalMarkerArea,
                    COUNT(DISTINCT m.field_id) as MarkerFieldCount
                FROM marker m
                WHERE m.cvr_number IS NOT NULL AND TRIM(m.cvr_number) != '' AND REGEXP_MATCHES(m.cvr_number, '^[0-9]+$')
                      AND m.crop_code IS NOT NULL AND m.area_ha > 0
                GROUP BY CVR, CropCode
                ORDER BY CVR, CropCode
            """
            marker_totals_csv = output_path / "debug_marker_cvr_crop_totals.csv"
            self.db.execute_query(f"COPY ({marker_totals_query}) TO '{str(marker_totals_csv)}' (HEADER, DELIMITER ',')")
            logger.info(f"Saved Marker CVR-Crop totals (debug) to {marker_totals_csv}")

        except Exception as e:
            logger.error(f"Error saving CVR-Crop totals for debugging: {str(e)}")
            # No raise here, allow main script to continue if debugging fails

    # DEPRECATED: Subset sum disaggregation strategy - REMOVED due to spurious correlations
    def disaggregate_by_subset_sum(self, pending_rows_table: str = "pending_pesticide_rows") -> Set[int]:
        logger.warning(
            "DEPRECATED: Subset sum matching has been removed due to spurious correlations. Returning empty set."
        )
        return set()

    def disaggregate_by_partial_field_coverage(self, pending_rows_table: str = "pending_pesticide_rows") -> List[int]:
        """
        Strategy: Partial Field Coverage for single-field CVR/crop combinations.

        Handles cases where:
        1. CVR/Crop combination has exactly one field in marker dataset
        2. Pesticide application area is significantly smaller than field area
        3. We allocate to the single field but flag as "partial coverage" with spatial uncertainty

        Args:
            pending_rows_table: Name of the table containing pending pesticide rows

        Returns:
            List of processed original pesticide row IDs
        """
        logger.info("Running Partial Field Coverage disaggregation strategy...")

        processed_ids = []

        # Find single-field CVR/crop combinations where pesticide area < field area
        query = f"""
        WITH MarkerSingleFieldCVRCrop AS (
            SELECT 
                CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                COUNT(*) as FieldCount,
                m.field_id as FieldID,
                m.area_ha as FieldArea,
                m.field_id as FieldIdentifier
            FROM marker m
            WHERE m.cvr_number IS NOT NULL 
              AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
              AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
              AND m.crop_code IS NOT NULL 
              AND m.area_ha > 0
            GROUP BY 1, 2, 4, 5, 6
            HAVING COUNT(*) = 1  -- Only single field per CVR/Crop
        ),
        PendingForSingleFields AS (
            SELECT 
                p.OriginalPesticideRowID,
                CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR_Str,
                CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                p.AcreageSize,
                p.CompanyName,
                p.Name as CropName
            FROM {pending_rows_table} p
            WHERE p.CompanyRegistrationNumber IS NOT NULL 
              AND p.Code IS NOT NULL
              AND p.AcreageSize > 0
        )
        SELECT 
            pf.OriginalPesticideRowID,
            pf.CVR_Str,
            pf.Crop_Str,
            pf.AcreageSize,
            pf.CompanyName,
            pf.CropName,
            sf.FieldID,
            sf.FieldArea,
            sf.FieldIdentifier,
            (pf.AcreageSize / sf.FieldArea) * 100 as CoveragePercent
        FROM MarkerSingleFieldCVRCrop sf
        JOIN PendingForSingleFields pf 
            ON sf.CVR_Str = pf.CVR_Str 
            AND sf.Crop_Str = pf.Crop_Str
        WHERE pf.AcreageSize < sf.FieldArea  -- Pesticide area smaller than field area
        ORDER BY CoveragePercent ASC  -- Process smallest coverage first
        """

        candidates = self.db.execute_query(query)

        if not candidates:
            logger.info("Partial Field Coverage: No single-field candidates found.")
            return processed_ids

        logger.info(f"Partial Field Coverage: Found {len(candidates)} single-field candidates to process.")

        for (
            original_id,
            cvr_str,
            crop_str,
            acreage_size,
            company_name,
            crop_name,
            field_id,
            field_area,
            field_identifier,
            coverage_percent,
        ) in candidates:
            # Create disaggregated entry with partial coverage flags
            disaggregated_entry = {
                "OriginalPesticideRowID": original_id,
                "FieldID": f"marker_{field_identifier}",
                "FieldSource": "marker",
                "CVR": cvr_str,
                "CropCode": crop_str,
                "CropName": crop_name,
                "CompanyName": company_name,
                "FieldArea": float(field_area),
                "AllocatedPesticideArea": float(acreage_size),  # Use actual pesticide area, not field area
                "AllocationMethod": "Partial_Field_Coverage_SingleField",
                "AreaDifference": float(field_area - acreage_size),
                "AreaDifferencePercent": float(((field_area - acreage_size) / acreage_size) * 100),
                "Confidence": 0.8,  # High confidence in field assignment, but spatial uncertainty
                "Notes": f"Partial field coverage: {coverage_percent:.1f}% of field area. Spatial location within field unknown.",
            }

            # Get original pesticide data for this row
            original_data_query = f"""
            SELECT * FROM {pending_rows_table} 
            WHERE OriginalPesticideRowID = ?
            """
            original_data = self.db.execute_query(original_data_query, [original_id])[0]

            # Insert into disaggregated table using the actual schema
            insert_query = """
            INSERT INTO disaggregated_pesticide_applications 
            (DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
             PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
             MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence, DisaggregationDate)
            VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            """

            # Extract block ID from field identifier
            block_id = field_identifier.split("-")[0] if "-" in str(field_identifier) else str(field_identifier)

            self.db.execute_query(
                insert_query,
                [
                    original_id,  # OriginalPesticideRowID
                    cvr_str,  # CompanyRegistrationNumber
                    original_data[13] if len(original_data) > 13 else None,  # PesticideName
                    original_data[14] if len(original_data) > 14 else None,  # PesticideRegistrationNumber
                    original_data[15] if len(original_data) > 15 else None,  # DosageQuantity
                    original_data[16] if len(original_data) > 16 else None,  # DosageUnit
                    f"marker_{field_identifier}",  # MatchedFieldID
                    f"block_{block_id}",  # MatchedBlockID
                    float(acreage_size),  # AllocatedArea (use pesticide area, not field area)
                    "Partial_Field_Coverage_SingleField",  # AllocationMethod
                    0.8,  # MatchConfidence
                ],
            )

            processed_ids.append(original_id)

        logger.info(
            f"Partial Field Coverage: Processed {len(processed_ids)} pesticide applications with partial field coverage."
        )
        return processed_ids

    def disaggregate_by_adjacent_fields_single_cluster(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ) -> List[int]:
        """
        Strategy: Adjacent Fields Single Cluster for multi-field CVR/crop combinations.

        Handles cases where:
        1. CVR/Crop combination has multiple fields in marker dataset
        2. ALL fields form a single connected cluster (within 10m or touching)
        3. Pesticide application area is smaller than total cluster area
        4. We allocate proportionally to all fields in the cluster with high confidence

        This strategy eliminates spatial ambiguity by only processing cases where
        all fields form one connected cluster, ensuring we don't incorrectly
        allocate pesticide to distant, untreated field groups.

        Args:
            pending_rows_table: Name of the table containing pending pesticide rows

        Returns:
            List of processed original pesticide row IDs
        """
        logger.info("Running Adjacent Fields Single Cluster disaggregation strategy...")

        processed_ids = []
        max_distance_m = 10.0

        # Find CVR/crop combinations where ALL fields form a single connected cluster
        query = f"""
        WITH MarkerFieldsWithGeometry AS (
            SELECT 
                CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                m.field_id as FieldID,
                m.area_ha as FieldArea,
                m.field_id as FieldIdentifier,
                m.geometry as FieldGeometry
            FROM marker m
            WHERE m.cvr_number IS NOT NULL 
              AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
              AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
              AND m.crop_code IS NOT NULL 
              AND m.area_ha > 0
              AND m.geometry IS NOT NULL
        ),
        CVRCropFieldCounts AS (
            SELECT 
                CVR_Str,
                Crop_Str,
                COUNT(*) as FieldCount,
                SUM(FieldArea) as TotalFieldArea
            FROM MarkerFieldsWithGeometry
            GROUP BY CVR_Str, Crop_Str
            HAVING COUNT(*) > 1  -- Only multi-field combinations
        ),
        PendingForAnalysis AS (
            SELECT 
                p.OriginalPesticideRowID,
                CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR_Str,
                CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                p.AcreageSize,
                p.CompanyName,
                p.Name as CropName
            FROM {pending_rows_table} p
            WHERE p.CompanyRegistrationNumber IS NOT NULL 
              AND p.Code IS NOT NULL
              AND p.AcreageSize > 0
        ),
        -- Find all field pairs that are adjacent (within 10m or touching)
        AdjacentPairs AS (
            SELECT DISTINCT
                f1.CVR_Str,
                f1.Crop_Str,
                f1.FieldID as Field1,
                f2.FieldID as Field2
            FROM MarkerFieldsWithGeometry f1
            JOIN MarkerFieldsWithGeometry f2 
                ON f1.CVR_Str = f2.CVR_Str 
                AND f1.Crop_Str = f2.Crop_Str
                AND f1.FieldID < f2.FieldID
            JOIN CVRCropFieldCounts cc 
                ON f1.CVR_Str = cc.CVR_Str 
                AND f1.Crop_Str = cc.Crop_Str
            JOIN PendingForAnalysis pfa
                ON f1.CVR_Str = pfa.CVR_Str 
                AND f1.Crop_Str = pfa.Crop_Str
            WHERE ST_Distance(f1.FieldGeometry, f2.FieldGeometry) <= {max_distance_m}
               OR ST_Touches(f1.FieldGeometry, f2.FieldGeometry)
        ),
        -- Count total fields and connected fields for each CVR/Crop
        FieldConnectivity AS (
            SELECT 
                cc.CVR_Str,
                cc.Crop_Str,
                cc.FieldCount as TotalFields,
                cc.TotalFieldArea,
                COALESCE(COUNT(DISTINCT ap.Field1), 0) + COALESCE(COUNT(DISTINCT ap.Field2), 0) as ConnectedFields,
                COALESCE(COUNT(*), 0) as AdjacentPairs
            FROM CVRCropFieldCounts cc
            LEFT JOIN AdjacentPairs ap 
                ON cc.CVR_Str = ap.CVR_Str 
                AND cc.Crop_Str = ap.Crop_Str
            GROUP BY cc.CVR_Str, cc.Crop_Str, cc.FieldCount, cc.TotalFieldArea
        ),
        -- Identify cases where ALL fields are connected (single cluster)
        SingleClusterCases AS (
            SELECT 
                fc.CVR_Str,
                fc.Crop_Str,
                fc.TotalFields,
                fc.TotalFieldArea,
                fc.ConnectedFields,
                fc.AdjacentPairs,
                pfa.OriginalPesticideRowID,
                pfa.AcreageSize,
                pfa.CompanyName,
                pfa.CropName,
                (pfa.AcreageSize / fc.TotalFieldArea) * 100 as CoveragePercent
            FROM FieldConnectivity fc
            JOIN PendingForAnalysis pfa 
                ON fc.CVR_Str = pfa.CVR_Str 
                AND fc.Crop_Str = pfa.Crop_Str
            WHERE (fc.ConnectedFields = fc.TotalFields OR (fc.TotalFields = 2 AND fc.AdjacentPairs >= 1))
              AND pfa.AcreageSize < fc.TotalFieldArea
        )
        SELECT 
            scc.OriginalPesticideRowID,
            scc.CVR_Str,
            scc.Crop_Str,
            scc.AcreageSize,
            scc.CompanyName,
            scc.CropName,
            scc.TotalFields,
            scc.TotalFieldArea,
            scc.CoveragePercent
        FROM SingleClusterCases scc
        ORDER BY scc.TotalFields ASC, scc.CoveragePercent DESC  -- Process smaller clusters first
        """

        candidates = self.db.execute_query(query)

        if not candidates:
            logger.info("Adjacent Fields Single Cluster: No single-cluster candidates found.")
            return processed_ids

        logger.info(f"Adjacent Fields Single Cluster: Found {len(candidates)} single-cluster candidates to process.")

        # Process each candidate by allocating proportionally to all fields in the cluster
        for (
            original_id,
            cvr_str,
            crop_str,
            acreage_size,
            company_name,
            crop_name,
            total_fields,
            total_field_area,
            coverage_percent,
        ) in candidates:
            # Get all fields in this cluster
            fields_query = """
            SELECT 
                m.field_id as FieldID,
                m.area_ha as FieldArea,
                m.field_id as FieldIdentifier
            FROM marker m
            WHERE CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) = ?
              AND CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) = ?
              AND m.cvr_number IS NOT NULL 
              AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
              AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
              AND m.crop_code IS NOT NULL 
              AND m.area_ha > 0
            ORDER BY m.area_ha DESC
            """

            fields = self.db.execute_query(fields_query, [cvr_str, crop_str])

            if not fields:
                logger.warning(f"No fields found for CVR {cvr_str}, Crop {crop_str}")
                continue

            # Get original pesticide data for this row
            original_data_query = f"""
            SELECT * FROM {pending_rows_table} 
            WHERE OriginalPesticideRowID = ?
            """
            original_data = self.db.execute_query(original_data_query, [original_id])[0]

            # Allocate pesticide proportionally to each field in the cluster
            for field_id, field_area, field_identifier in fields:
                # Calculate proportional allocation
                field_proportion = float(field_area) / float(total_field_area)
                allocated_area = float(acreage_size) * field_proportion

                # Insert into disaggregated table
                insert_query = """
                INSERT INTO disaggregated_pesticide_applications 
                (DisaggregatedID, OriginalPesticideRowID, CompanyName, CompanyRegistrationNumber, 
                 StreetName, StreetBuildingIdentifier, FloorIdentifier, PostCodeIdentifier, City,
                 AcreageSize, AcreageUnit, Name, Code, PesticideName, PesticideRegistrationNumber, 
                 DosageQuantity, DosageUnit, NoPesticides, MatchedFieldID, MatchedDataset, 
                 AllocatedArea, AllocationMethod, MatchConfidence, DisaggregationDate)
                VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                """

                self.db.execute_query(
                    insert_query,
                    [
                        original_id,  # OriginalPesticideRowID
                        company_name,  # CompanyName
                        cvr_str,  # CompanyRegistrationNumber
                        original_data[4] if len(original_data) > 4 else None,  # StreetName
                        original_data[5] if len(original_data) > 5 else None,  # StreetBuildingIdentifier
                        original_data[6] if len(original_data) > 6 else None,  # FloorIdentifier
                        original_data[7] if len(original_data) > 7 else None,  # PostCodeIdentifier
                        original_data[8] if len(original_data) > 8 else None,  # City
                        allocated_area,  # AcreageSize (proportional allocation)
                        original_data[10] if len(original_data) > 10 else None,  # AcreageUnit
                        crop_name,  # Name
                        crop_str,  # Code
                        original_data[13] if len(original_data) > 13 else None,  # PesticideName
                        original_data[14] if len(original_data) > 14 else None,  # PesticideRegistrationNumber
                        original_data[15] if len(original_data) > 15 else None,  # DosageQuantity
                        original_data[16] if len(original_data) > 16 else None,  # DosageUnit
                        original_data[17] if len(original_data) > 17 else None,  # NoPesticides
                        f"marker_{field_identifier}",  # MatchedFieldID
                        "marker",  # MatchedDataset
                        allocated_area,  # AllocatedArea
                        f"Adjacent_Fields_Single_Cluster_Partial_{coverage_percent:.1f}pct",  # AllocationMethod with spatial uncertainty note
                        0.9,  # MatchConfidence (high confidence due to single cluster)
                    ],
                )

            processed_ids.append(original_id)

        logger.info(
            f"Adjacent Fields Single Cluster: Processed {len(processed_ids)} pesticide applications across single-cluster field groups."
        )
        return processed_ids


# For brevity, the long SQL queries within the methods are referenced as "Original SQL".
