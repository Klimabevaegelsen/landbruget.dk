import logging
import uuid
from typing import Dict, List, Set, Tuple

# from ..database import DatabaseManager
# from ..config import Config

logger = logging.getLogger(__name__)


# --- Helper functions for subset sum (to be part of the class or static) ---
# Placed outside for now, can be moved into class as staticmethods or helper methods
def _find_area_subsets_recursive_impl_static(
    fields: list[tuple[str, float]],
    current_index: int,
    current_sum: float,
    current_subset_details: list[tuple[str, float]],
    solutions: list[list[tuple[str, float]]],
    lower_bound: float,
    upper_bound: float,
    suffix_sums: list[float],
):
    sum_of_remaining = suffix_sums[current_index] if current_index < len(fields) else 0.0
    if current_sum + sum_of_remaining < lower_bound - 1e-9:
        return
    if current_sum > upper_bound + 1e-9:
        return
    if lower_bound - 1e-9 <= current_sum <= upper_bound + 1e-9 and current_subset_details:
        solutions.append(list(current_subset_details))
    if current_index == len(fields):
        return

    field_id, field_area = fields[current_index]
    current_subset_details.append((field_id, field_area))
    _find_area_subsets_recursive_impl_static(
        fields,
        current_index + 1,
        current_sum + field_area,
        current_subset_details,
        solutions,
        lower_bound,
        upper_bound,
        suffix_sums,
    )
    current_subset_details.pop()
    _find_area_subsets_recursive_impl_static(
        fields,
        current_index + 1,
        current_sum,
        current_subset_details,
        solutions,
        lower_bound,
        upper_bound,
        suffix_sums,
    )


def _find_area_subsets_static(
    fields_with_ids: list[tuple[str, float]],
    target_sum: float,
    tolerance_pct: float,
    max_fields_to_consider: int,
) -> list[list[tuple[str, float]]]:
    if not fields_with_ids or target_sum <= 0:
        return []
    actual_fields_to_use = sorted(fields_with_ids, key=lambda x: x[1])
    if len(actual_fields_to_use) > max_fields_to_consider:
        # logger.debug(f"SubsetSum: Considering only smallest {max_fields_to_consider} of {len(actual_fields_to_use)} fields.")
        actual_fields_to_use = actual_fields_to_use[:max_fields_to_consider]
    if not actual_fields_to_use:
        return []
    solutions = []
    lower_bound = target_sum * (1 - tolerance_pct / 100.0)
    upper_bound = target_sum * (1 + tolerance_pct / 100.0)
    suffix_sums = [0.0] * (len(actual_fields_to_use) + 1)
    for i in range(len(actual_fields_to_use) - 1, -1, -1):
        suffix_sums[i] = actual_fields_to_use[i][1] + suffix_sums[i + 1]
    _find_area_subsets_recursive_impl_static(
        actual_fields_to_use,
        0,
        0.0,
        [],
        solutions,
        lower_bound,
        upper_bound,
        suffix_sums,
    )
    return solutions


def _get_closest_subset_static(subsets: list[list[tuple[str, float]]], target_val: float) -> list[tuple[str, float]]:
    if not subsets:
        return []
    if len(subsets) == 1:
        return subsets[0]
    closest_subset = []
    min_diff = float("inf")
    for subset_item in subsets:
        current_sum = sum(s[1] for s in subset_item)
        diff = abs(current_sum - target_val)
        if diff < min_diff:
            min_diff = diff
            closest_subset = subset_item
        elif diff == min_diff:  # Ambiguous if multiple are equally close
            return []
    return closest_subset


# --- End helper functions ---


class PesticideDisaggregator:
    """Handles disaggregation of pesticide data from company to field level."""

    def __init__(self, db_manager, config):
        """Initialize with database manager and configuration."""
        self.db = db_manager
        self.config = config
        self._organic_marker_field_ids: Set[str] = set()

    def _get_organic_marker_field_ids(self) -> Set[str]:
        """
        Identifies marker field IDs that are considered organic by spatially joining
        with oekologiske_arealer. Results are cached.
        Returns a set of marker.id strings.
        """
        if self._organic_marker_field_ids:
            logger.debug("Returning cached organic marker field IDs.")
            return self._organic_marker_field_ids

        logger.info("Identifying organic marker fields by spatial join with oekologiske_arealer...")
        query = """
        SELECT DISTINCT m.id 
        FROM marker m
        JOIN oekologiske_arealer oa ON ST_Intersects(m.geometry, oa.geometry)
        WHERE m.geometry IS NOT NULL AND oa.geometry IS NOT NULL;
        """
        try:
            result_tuples = self.db.execute_query(query)
            self._organic_marker_field_ids = {row[0] for row in result_tuples}
            logger.info(f"Identified {len(self._organic_marker_field_ids)} organic marker field IDs.")
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
                    CompanyName VARCHAR,
                    CompanyRegistrationNumber VARCHAR,
                    StreetName VARCHAR,
                    StreetBuildingIdentifier VARCHAR,
                    FloorIdentifier VARCHAR,
                    PostCodeIdentifier VARCHAR,
                    City VARCHAR,
                    AcreageSize DOUBLE,
                    AcreageUnit VARCHAR,
                    Name VARCHAR,
                    Code VARCHAR,
                    PesticideName VARCHAR,
                    PesticideRegistrationNumber VARCHAR,
                    DosageQuantity DOUBLE,
                    DosageUnit VARCHAR,
                    NoPesticides BOOLEAN,
                    MatchedFieldID VARCHAR,
                    MatchedDataset VARCHAR,
                    AllocatedArea DOUBLE,
                    AllocationMethod VARCHAR,
                    MatchConfidence DOUBLE,
                    DisaggregationDate TIMESTAMP
                )
            """)
            logger.info("Table 'disaggregated_pesticide_applications' created or already exists.")
        except Exception as e:
            logger.error(f"Error creating disaggregated_pesticide_applications table: {str(e)}")
            raise

    def analyze_potential_gkea_matches(self, pending_rows_table: str = "pending_pesticide_rows") -> List[int]:
        """
        Disaggregates individual pesticide rows if their AcreageSize matches the
        total GKEA field area for that CVR & Crop.
        """
        processed_row_ids = []
        try:
            insert_query = f"""
                WITH GKEAFieldCVRCropTotals AS (
                    SELECT 
                        CAST(g.unnamed__1 AS VARCHAR) as CVR,
                        TRY_CAST(g.unnamed__12 AS BIGINT) as CropCode,
                        SUM(TRY_CAST(g.unnamed__4 AS DOUBLE)) as TotalGKEAAreaForCVRCrop
                    FROM gkea g
                    WHERE g.unnamed__1 IS NOT NULL AND g.unnamed__1 != 'CVR' AND REGEXP_MATCHES(CAST(g.unnamed__1 AS VARCHAR), '^[0-9]+$')
                          AND g.unnamed__12 IS NOT NULL AND TRY_CAST(g.unnamed__4 AS DOUBLE) > 0
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID, OriginalPesticideRowID,
                    CompanyName, CompanyRegistrationNumber, StreetName, StreetBuildingIdentifier, FloorIdentifier, PostCodeIdentifier, City,
                    AcreageSize, AcreageUnit, Name, Code, PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit, NoPesticides,
                    MatchedFieldID, MatchedDataset, AllocatedArea, AllocationMethod, MatchConfidence, DisaggregationDate
                )
                SELECT
                    uuid() as DisaggregatedID,
                    p.OriginalPesticideRowID,
                    p.CompanyName, CAST(p.CompanyRegistrationNumber AS VARCHAR), p.StreetName, p.StreetBuildingIdentifier, p.FloorIdentifier, p.PostCodeIdentifier, p.City,
                    p.AcreageSize, p.AcreageUnit, p.Name, p.Code, p.PesticideName, p.PesticideRegistrationNumber, p.DosageQuantity, p.DosageUnit, p.NoPesticides,
                    'gkea_' || CAST(g_fields.unnamed__3 AS VARCHAR) as MatchedFieldID,
                    'gkea' as MatchedDataset,
                    p.AcreageSize * (TRY_CAST(g_fields.unnamed__4 AS DOUBLE) / gkea_totals.TotalGKEAAreaForCVRCrop) as AllocatedArea,
                    'GKEA_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - gkea_totals.TotalGKEAAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence, 
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN GKEAFieldCVRCropTotals gkea_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = gkea_totals.CVR
                    AND TRY_CAST(p.Code AS BIGINT) = gkea_totals.CropCode
                JOIN gkea g_fields 
                    ON gkea_totals.CVR = CAST(g_fields.unnamed__1 AS VARCHAR) 
                    AND gkea_totals.CropCode = TRY_CAST(g_fields.unnamed__12 AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND gkea_totals.TotalGKEAAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - gkea_totals.TotalGKEAAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND TRY_CAST(g_fields.unnamed__4 AS DOUBLE) > 0;
            """
            self.db.execute_query(insert_query)
            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT 
                        CAST(g.unnamed__1 AS VARCHAR) as CVR,
                        TRY_CAST(g.unnamed__12 AS BIGINT) as CropCode,
                        SUM(TRY_CAST(g.unnamed__4 AS DOUBLE)) as TotalGKEAAreaForCVRCrop
                    FROM gkea g
                    WHERE g.unnamed__1 IS NOT NULL AND g.unnamed__1 != 'CVR' AND REGEXP_MATCHES(CAST(g.unnamed__1 AS VARCHAR), '^[0-9]+$')
                          AND g.unnamed__12 IS NOT NULL AND TRY_CAST(g.unnamed__4 AS DOUBLE) > 0
                    GROUP BY CVR, CropCode
                ) gkea_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = gkea_totals.CVR
                    AND TRY_CAST(p.Code AS BIGINT) = gkea_totals.CropCode
                WHERE 
                    p.AcreageSize > 0 AND gkea_totals.TotalGKEAAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - gkea_totals.TotalGKEAAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT};
            """
            processed_id_tuples = self.db.execute_query(select_processed_ids_query)
            processed_row_ids = [pid[0] for pid in processed_id_tuples]
            if processed_row_ids:
                logger.info(
                    f"GKEA (PesticideRowArea vs TotalFieldArea): Matched and disaggregated {len(processed_row_ids)} pesticide rows."
                )
            else:
                logger.info(
                    "GKEA (PesticideRowArea vs TotalFieldArea): No pesticide rows met the area criteria for disaggregation."
                )
        except Exception as e:
            logger.error(f"Error in GKEA (PesticideRowArea vs TotalFieldArea) matching: {str(e)}")
        return processed_row_ids

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
                    DisaggregatedID, OriginalPesticideRowID,
                    CompanyName, CompanyRegistrationNumber, StreetName, StreetBuildingIdentifier, FloorIdentifier, PostCodeIdentifier, City,
                    AcreageSize, AcreageUnit, Name, Code, PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit, NoPesticides,
                    MatchedFieldID, MatchedDataset, AllocatedArea, AllocationMethod, MatchConfidence, DisaggregationDate
                )
                SELECT
                    uuid() as DisaggregatedID,
                    p.OriginalPesticideRowID,
                    p.CompanyName, CAST(p.CompanyRegistrationNumber AS VARCHAR), p.StreetName, p.StreetBuildingIdentifier, p.FloorIdentifier, p.PostCodeIdentifier, p.City,
                    p.AcreageSize, p.AcreageUnit, p.Name, p.Code, p.PesticideName, p.PesticideRegistrationNumber, p.DosageQuantity, p.DosageUnit, p.NoPesticides,
                    'marker_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'marker' as MatchedDataset,
                    p.AcreageSize * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence,
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
        total *non-organic* Marker field area (IMK_areal) for that CVR & Crop.
        Organic fields are identified by spatial join with oekologiske_arealer.
        """
        processed_row_ids = []
        organic_field_ids = self._get_organic_marker_field_ids()

        organic_ids_sql_tuple = "('dummy_id_if_empty')"
        if organic_field_ids:
            organic_ids_sql_tuple = str(tuple(organic_field_ids))
            if len(organic_field_ids) == 1:
                organic_ids_sql_tuple = f"('{next(iter(organic_field_ids))}')"

        logger.info(
            f"Attempting disaggregation by Marker Non-Organic match. Excluding {len(organic_field_ids)} organic marker IDs from consideration."
        )

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
                          AND m.id NOT IN {organic_ids_sql_tuple} 
                    GROUP BY CVR, CropCode
                )
                INSERT INTO disaggregated_pesticide_applications (
                    DisaggregatedID, OriginalPesticideRowID,
                    CompanyName, CompanyRegistrationNumber, StreetName, StreetBuildingIdentifier, FloorIdentifier, PostCodeIdentifier, City,
                    AcreageSize, AcreageUnit, Name, Code, PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit, NoPesticides,
                    MatchedFieldID, MatchedDataset, AllocatedArea, AllocationMethod, MatchConfidence, DisaggregationDate
                )
                SELECT
                    uuid() as DisaggregatedID,
                    p.OriginalPesticideRowID,
                    p.CompanyName, CAST(p.CompanyRegistrationNumber AS VARCHAR), p.StreetName, p.StreetBuildingIdentifier, p.FloorIdentifier, p.PostCodeIdentifier, p.City,
                    p.AcreageSize, p.AcreageUnit, p.Name, p.Code, p.PesticideName, p.PesticideRegistrationNumber, p.DosageQuantity, p.DosageUnit, p.NoPesticides,
                    'marker_non_organic_' || CAST(m_fields.Markblok AS VARCHAR) || '_' || CAST(m_fields.Marknr AS VARCHAR) as MatchedFieldID,
                    'marker_non_organic' as MatchedDataset,
                    p.AcreageSize * (m_fields.IMK_areal / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence,
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = non_organic_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.CVR AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.Afgkode AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND m_fields.CVR IS NOT NULL 
                    AND TRIM(CAST(m_fields.CVR AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.CVR AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.IMK_areal > 0
                    AND m_fields.id NOT IN {organic_ids_sql_tuple}; 
            """
            self.db.execute_query(insert_query)

            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT
                        TRIM(CAST(m.CVR AS VARCHAR)) as CVR,
                        TRY_CAST(m.Afgkode AS BIGINT) as CropCode,
                        SUM(m.IMK_areal) as TotalNonOrganicMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.CVR IS NOT NULL 
                          AND TRIM(CAST(m.CVR AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.CVR AS VARCHAR)), '^[0-9]+$')
                          AND m.Afgkode IS NOT NULL AND m.IMK_areal > 0
                          AND m.id NOT IN {organic_ids_sql_tuple}
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
        """Calculates and saves CVR-Crop total areas from pesticide, GKEA, and Marker for debugging."""
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

            gkea_totals_query = """
                SELECT
                    g.unnamed__1 as CVR, 
                    TRY_CAST(g.unnamed__12 AS BIGINT) as CropCode,
                    SUM(TRY_CAST(g.unnamed__4 AS DOUBLE)) as TotalGKEAArea,
                    COUNT(DISTINCT g.unnamed__3) as GKEAFieldCount
                FROM gkea g
                WHERE g.unnamed__1 IS NOT NULL AND TRIM(g.unnamed__1) != '' AND g.unnamed__1 != 'CVR' AND REGEXP_MATCHES(g.unnamed__1, '^[0-9]+$')
                      AND g.unnamed__12 IS NOT NULL AND TRY_CAST(g.unnamed__4 AS DOUBLE) > 0
                GROUP BY CVR, CropCode
                ORDER BY CVR, CropCode
            """
            gkea_totals_csv = output_path / "debug_gkea_cvr_crop_totals.csv"
            self.db.execute_query(f"COPY ({gkea_totals_query}) TO '{str(gkea_totals_csv)}' (HEADER, DELIMITER ',')")
            logger.info(f"Saved GKEA CVR-Crop totals (debug) to {gkea_totals_csv}")

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

    # New disaggregation strategy using subset sum
    def disaggregate_by_subset_sum(self, pending_rows_table: str = "pending_pesticide_rows") -> Set[int]:
        logger.info("Running Subset Sum disaggregation strategy...")
        processed_original_pesticide_row_ids: Set[int] = set()

        # Get all unique CVR/CropCode combinations from pending rows
        candidate_query = f"""
        SELECT DISTINCT
            CAST(CAST(CompanyRegistrationNumber AS BIGINT) AS VARCHAR) AS CVR, 
            TRY_CAST(Code AS BIGINT) AS CropCode
        FROM {pending_rows_table}
        WHERE CompanyRegistrationNumber IS NOT NULL AND Code IS NOT NULL
        ORDER BY CVR, CropCode;
        """
        candidates = self.db.execute_query(candidate_query)

        if not candidates:
            logger.info("SubsetSum: No CVR/Crop candidates found in pending rows.")
            return processed_original_pesticide_row_ids

        logger.info(f"SubsetSum: Found {len(candidates)} CVR/Crop candidates to analyze.")

        # For collecting rows to insert in bulk (list of tuples)
        rows_to_insert_into_disaggregated = []

        # Using tqdm like progress logging if possible, or just log every N candidates
        num_candidates = len(candidates)
        log_interval = max(1, num_candidates // 10)  # Log roughly 10 times

        for idx, (target_cvr, target_cropcode) in enumerate(candidates):
            if idx % log_interval == 0:
                logger.info(
                    f"SubsetSum: Processing candidate {idx + 1}/{num_candidates} (CVR: {target_cvr}, Crop: {target_cropcode})"
                )

            # 1. Pending AcreageSizes for this CVR/Crop
            pending_acreages_query = f"""
            SELECT DISTINCT OriginalPesticideRowID, AcreageSize 
            FROM {pending_rows_table} 
            WHERE CAST(CAST(CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = '{target_cvr}' 
              AND TRY_CAST(Code AS BIGINT) = {target_cropcode}
              AND AcreageSize > 0
              AND OriginalPesticideRowID NOT IN (SELECT OriginalPesticideRowID FROM disaggregated_pesticide_applications WHERE OriginalPesticideRowID IS NOT NULL)
            ORDER BY AcreageSize;
            """

            pending_acreages_for_cvr_crop = self.db.execute_query(pending_acreages_query)
            if not pending_acreages_for_cvr_crop:
                continue

            # 2. GKEA Field Areas for this CVR/Crop
            gkea_fields_query = f"""
            SELECT CAST(unnamed__3 AS VARCHAR) as FieldID, TRY_CAST(unnamed__4 AS DOUBLE) as Area
            FROM gkea 
            WHERE TRIM(CAST(unnamed__1 AS VARCHAR)) = '{target_cvr}' 
              AND TRY_CAST(unnamed__12 AS BIGINT) = {target_cropcode} AND TRY_CAST(unnamed__4 AS DOUBLE) > 0
            ORDER BY Area;
            """
            gkea_fields_raw = self.db.execute_query(gkea_fields_query)
            gkea_fields_with_ids = []
            temp_gkea_ids: Dict[str, int] = {}
            for i, (fid, area) in enumerate(gkea_fields_raw):
                unique_fid = str(fid)
                if unique_fid in temp_gkea_ids:
                    temp_gkea_ids[unique_fid] += 1
                    unique_fid = f"{unique_fid}_{temp_gkea_ids[unique_fid]}"
                else:
                    temp_gkea_ids[unique_fid] = 0
                gkea_fields_with_ids.append((unique_fid, area))

            # 3. Marker Field Areas for this CVR/Crop (ALL marker fields, not just non-organic)
            marker_fields_query = f"""
            SELECT CAST(field_id AS VARCHAR) as FieldID, area_ha as Area
            FROM marker 
            WHERE TRIM(CAST(cvr_number AS VARCHAR)) = '{target_cvr}' 
              AND TRY_CAST(crop_code AS BIGINT) = {target_cropcode} AND area_ha > 0
            ORDER BY area_ha;
            """
            marker_fields_raw = self.db.execute_query(marker_fields_query)
            marker_fields_with_ids = [(str(fid), area) for fid, area in marker_fields_raw]

            for p_row_id, target_area in pending_acreages_for_cvr_crop:
                if (
                    p_row_id in processed_original_pesticide_row_ids
                ):  # Already handled by a previous CVR/Crop iteration if somehow overlapping
                    continue

                matched_this_acreage = False
                best_subset: List[Tuple[str, float]] = []
                dataset_source = ""

                # --- Try Marker First ---
                if len(marker_fields_with_ids) > 1:  # Subset sum needs at least 2 fields
                    marker_subsets = _find_area_subsets_static(
                        marker_fields_with_ids,
                        target_area,
                        self.config.AREA_TOLERANCE_PCT,
                        self.config.MAX_FIELDS_FOR_SUBSET_SUM,
                    )
                    if len(marker_subsets) == 1:
                        best_subset = marker_subsets[0]
                        dataset_source = "marker"
                        matched_this_acreage = True
                    elif len(marker_subsets) > 1:
                        closest_marker_subset = _get_closest_subset_static(marker_subsets, target_area)
                        if closest_marker_subset:
                            best_subset = closest_marker_subset
                            dataset_source = "marker"
                            matched_this_acreage = True

                # --- If Marker didn't match, Try GKEA ---
                if not matched_this_acreage and len(gkea_fields_with_ids) > 1:  # Subset sum needs at least 2 fields
                    gkea_subsets = _find_area_subsets_static(
                        gkea_fields_with_ids,
                        target_area,
                        self.config.AREA_TOLERANCE_PCT,
                        self.config.MAX_FIELDS_FOR_SUBSET_SUM,
                    )
                    if len(gkea_subsets) == 1:
                        best_subset = gkea_subsets[0]
                        dataset_source = "gkea"
                        matched_this_acreage = True
                    elif len(gkea_subsets) > 1:
                        closest_gkea_subset = _get_closest_subset_static(gkea_subsets, target_area)
                        if closest_gkea_subset:
                            best_subset = closest_gkea_subset
                            dataset_source = "gkea"
                            matched_this_acreage = True

                if matched_this_acreage and best_subset:
                    # Fetch original pesticide row details
                    original_pesticide_row_details_query = (
                        f"SELECT * FROM {pending_rows_table} WHERE OriginalPesticideRowID = {p_row_id}"
                    )
                    original_row_data = self.db.execute_query(original_pesticide_row_details_query)

                    if not original_row_data:
                        logger.warning(
                            f"SubsetSum: Could not fetch original pesticide row details for ID {p_row_id}. Skipping."
                        )
                        continue

                    # Assuming original_row_data[0] is a tuple with columns in order of 'pesticide' table
                    # Need to map these to disaggregated_pesticide_applications columns.
                    # Let's assume pesticide table columns are: OriginalPesticideRowID, CompanyName, CompanyRegistrationNumber, ...
                    # This mapping needs to be robust or based on column names if db_manager returns dicts.
                    # For now, assuming positional, which is fragile.

                    p_cols_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{pending_rows_table}' ORDER BY ordinal_position;"
                    p_cols = [col[0] for col in self.db.execute_query(p_cols_query)]
                    original_row_dict = dict(zip(p_cols, original_row_data[0]))

                    subset_sum_area = sum(s_area for _, s_area in best_subset)
                    match_confidence = max(
                        0.0,
                        1.0
                        - (abs(target_area - subset_sum_area) / target_area / (self.config.AREA_TOLERANCE_PCT / 100.0)),
                    )

                    for field_id_in_subset, field_area_in_subset in best_subset:
                        allocated_area = (
                            target_area * (field_area_in_subset / subset_sum_area) if subset_sum_area > 0 else 0
                        )

                        disaggregated_row = (
                            str(uuid.uuid4()),  # DisaggregatedID
                            original_row_dict.get("OriginalPesticideRowID"),
                            original_row_dict.get("CompanyName"),
                            str(original_row_dict.get("CompanyRegistrationNumber")),  # Ensure string
                            original_row_dict.get("StreetName"),
                            original_row_dict.get("StreetBuildingIdentifier"),
                            original_row_dict.get("FloorIdentifier"),
                            original_row_dict.get("PostCodeIdentifier"),
                            original_row_dict.get("City"),
                            original_row_dict.get("AcreageSize"),
                            original_row_dict.get("AcreageUnit"),
                            original_row_dict.get("Name"),
                            original_row_dict.get("Code"),
                            original_row_dict.get("PesticideName"),
                            original_row_dict.get("PesticideRegistrationNumber"),
                            original_row_dict.get("DosageQuantity"),
                            original_row_dict.get("DosageUnit"),
                            original_row_dict.get("NoPesticides"),
                            f"{dataset_source}_{field_id_in_subset}",  # MatchedFieldID
                            dataset_source,  # MatchedDataset
                            allocated_area,  # AllocatedArea
                            f"{dataset_source.upper()}_SubsetSum_Proportional",  # AllocationMethod
                            match_confidence,  # MatchConfidence
                            # DisaggregationDate will be set by DB default (NOW()) or explicit insert time
                        )
                        rows_to_insert_into_disaggregated.append(disaggregated_row)

                    processed_original_pesticide_row_ids.add(p_row_id)

        # Bulk insert collected rows
        if rows_to_insert_into_disaggregated:
            # Ensure DisaggregationDate is handled: Add placeholder for NOW() if table DDL has it
            # Or, add NOW() to each tuple if inserting manually.
            # The DDL has DisaggregationDate but no default NOW(). Add it here.
            # For simplicity, let's use DuckDB's NOW() in the INSERT statement itself.

            # Prepare for executemany: get column names for disaggregated_pesticide_applications
            disagg_cols_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'disaggregated_pesticide_applications' AND column_name != 'DisaggregationDate' ORDER BY ordinal_position;"
            disagg_cols = [col[0] for col in self.db.execute_query(disagg_cols_query)]

            # Create the placeholder string like (?, ?, ?, ...)
            placeholders = ", ".join(["?"] * len(disagg_cols))

            # Add NOW() for DisaggregationDate
            final_insert_sql = f"INSERT INTO disaggregated_pesticide_applications ({', '.join(disagg_cols)}, DisaggregationDate) VALUES ({placeholders}, NOW());"

            self.db.executemany(final_insert_sql, rows_to_insert_into_disaggregated)
            logger.info(
                f"SubsetSum: Inserted {len(rows_to_insert_into_disaggregated)} disaggregated field parts from {len(processed_original_pesticide_row_ids)} original pesticide rows."
            )
        else:
            logger.info("SubsetSum: No pesticide rows were disaggregated by this strategy.")

        return processed_original_pesticide_row_ids

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
                    acreage_size,  # AcreageSize
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
