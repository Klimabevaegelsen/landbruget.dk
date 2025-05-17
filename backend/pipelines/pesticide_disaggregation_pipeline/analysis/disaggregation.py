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
    sum_of_remaining = (
        suffix_sums[current_index] if current_index < len(fields) else 0.0
    )
    if current_sum + sum_of_remaining < lower_bound - 1e-9:
        return
    if current_sum > upper_bound + 1e-9:
        return
    if (
        lower_bound - 1e-9 <= current_sum <= upper_bound + 1e-9
        and current_subset_details
    ):
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


def _get_closest_subset_static(
    subsets: list[list[tuple[str, float]]], target_val: float
) -> list[tuple[str, float]]:
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

        logger.info(
            "Identifying organic marker fields by spatial join with oekologiske_arealer..."
        )
        query = """
        SELECT DISTINCT m.id 
        FROM marker m
        JOIN oekologiske_arealer oa ON ST_Intersects(m.geometry, oa.geometry)
        WHERE m.geometry IS NOT NULL AND oa.geometry IS NOT NULL;
        """
        try:
            result_tuples = self.db.execute_query(query)
            self._organic_marker_field_ids = {row[0] for row in result_tuples}
            logger.info(
                f"Identified {len(self._organic_marker_field_ids)} organic marker field IDs."
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
            logger.info(
                "Table 'disaggregated_pesticide_applications' created or already exists."
            )
        except Exception as e:
            logger.error(
                f"Error creating disaggregated_pesticide_applications table: {str(e)}"
            )
            raise

    def analyze_potential_gkea_matches(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ) -> List[int]:
        """
        Disaggregates individual pesticide rows if their AcreageSize matches the
        total GKEA field area for that CVR & Crop.
        """
        processed_row_ids = []
        try:
            insert_query = f"""
                WITH GKEAFieldCVRCropTotals AS (
                    SELECT 
                        CAST(g."CVR" AS VARCHAR) as CVR,
                        TRY_CAST(g."Hovedafgrøde" AS BIGINT) as CropCode,
                        SUM(g."Areal") as TotalGKEAAreaForCVRCrop
                    FROM gkea g
                    WHERE g."CVR" IS NOT NULL AND REGEXP_MATCHES(CAST(g."CVR" AS VARCHAR), '^[0-9]+$')
                          AND g."Hovedafgrøde" IS NOT NULL AND g."Areal" > 0
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
                    'gkea_' || CAST(g_fields."Marknummer" AS VARCHAR) as MatchedFieldID,
                    'gkea' as MatchedDataset,
                    p.AcreageSize * (g_fields."Areal" / gkea_totals.TotalGKEAAreaForCVRCrop) as AllocatedArea,
                    'GKEA_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - gkea_totals.TotalGKEAAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence, 
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN GKEAFieldCVRCropTotals gkea_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = gkea_totals.CVR
                    AND TRY_CAST(p.Code AS BIGINT) = gkea_totals.CropCode
                JOIN gkea g_fields 
                    ON gkea_totals.CVR = CAST(g_fields."CVR" AS VARCHAR) 
                    AND gkea_totals.CropCode = TRY_CAST(g_fields."Hovedafgrøde" AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND gkea_totals.TotalGKEAAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - gkea_totals.TotalGKEAAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND g_fields."Areal" > 0;
            """
            self.db.execute_query(insert_query)
            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT 
                        CAST(g."CVR" AS VARCHAR) as CVR,
                        TRY_CAST(g."Hovedafgrøde" AS BIGINT) as CropCode,
                        SUM(g."Areal") as TotalGKEAAreaForCVRCrop
                    FROM gkea g
                    WHERE g."CVR" IS NOT NULL AND REGEXP_MATCHES(CAST(g."CVR" AS VARCHAR), '^[0-9]+$')
                          AND g."Hovedafgrøde" IS NOT NULL AND g."Areal" > 0
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
            logger.error(
                f"Error in GKEA (PesticideRowArea vs TotalFieldArea) matching: {str(e)}"
            )
        return processed_row_ids

    def disaggregate_by_marker_match(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ) -> List[int]:
        """
        Disaggregates individual pesticide rows if their AcreageSize matches the
        total Marker field area for that CVR & Crop.
        """
        processed_row_ids = []
        try:
            insert_query = f"""
                WITH MarkerFieldCVRCropTotals AS (
                    SELECT
                        TRIM(CAST(m.CVR AS VARCHAR)) as CVR,
                        TRY_CAST(m.Afgkode AS BIGINT) as CropCode,
                        SUM(m.IMK_areal) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.CVR IS NOT NULL 
                          AND TRIM(CAST(m.CVR AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.CVR AS VARCHAR)), '^[0-9]+$')
                          AND m.Afgkode IS NOT NULL AND m.IMK_areal > 0
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
                    'marker_' || CAST(m_fields.Markblok AS VARCHAR) || '_' || CAST(m_fields.Marknr AS VARCHAR) as MatchedFieldID,
                    'marker' as MatchedDataset,
                    p.AcreageSize * (m_fields.IMK_areal / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.AREA_TOLERANCE_PCT}/100.0))) as MatchConfidence,
                    NOW() as DisaggregationDate
                FROM {pending_rows_table} p
                JOIN MarkerFieldCVRCropTotals marker_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = marker_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                JOIN marker m_fields 
                    ON marker_totals.CVR = TRIM(CAST(m_fields.CVR AS VARCHAR))
                    AND marker_totals.CropCode = TRY_CAST(m_fields.Afgkode AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.AREA_TOLERANCE_PCT}
                    AND m_fields.CVR IS NOT NULL 
                    AND TRIM(CAST(m_fields.CVR AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.CVR AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.IMK_areal > 0;
            """
            self.db.execute_query(insert_query)

            select_processed_ids_query = f"""
                SELECT DISTINCT p.OriginalPesticideRowID
                FROM {pending_rows_table} p
                JOIN (
                    SELECT
                        TRIM(CAST(m.CVR AS VARCHAR)) as CVR,
                        TRY_CAST(m.Afgkode AS BIGINT) as CropCode,
                        SUM(m.IMK_areal) as TotalMarkerAreaForCVRCrop
                    FROM marker m
                    WHERE m.CVR IS NOT NULL 
                          AND TRIM(CAST(m.CVR AS VARCHAR)) != '' 
                          AND REGEXP_MATCHES(TRIM(CAST(m.CVR AS VARCHAR)), '^[0-9]+$')
                          AND m.Afgkode IS NOT NULL AND m.IMK_areal > 0
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
            logger.error(
                f"Error in Marker (PesticideRowArea vs TotalFieldArea) matching: {str(e)}"
            )
        return processed_row_ids

    def disaggregate_by_marker_non_organic_match(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ) -> List[int]:
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

    def save_cvr_crop_totals_for_debugging(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ):
        """Calculates and saves CVR-Crop total areas from pesticide, GKEA, and Marker for debugging."""
        try:
            output_path = self.config.RESOLVED_OUTPUT_DIR
            if output_path is None:
                logger.error(
                    "RESOLVED_OUTPUT_DIR not set in config! Attempting fallback."
                )
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
            logger.info(
                f"Saved pesticide CVR-Crop totals (debug) to {pesticide_totals_csv}"
            )

            gkea_totals_query = """
                SELECT
                    g."CVR" as CVR, 
                    TRY_CAST(g."Hovedafgrøde" AS BIGINT) as CropCode,
                    SUM(g."Areal") as TotalGKEAArea,
                    COUNT(DISTINCT g."Marknummer") as GKEAFieldCount
                FROM gkea g
                WHERE g."CVR" IS NOT NULL AND TRIM(g."CVR") != '' AND REGEXP_MATCHES(g."CVR", '^[0-9]+$')
                      AND g."Hovedafgrøde" IS NOT NULL AND g."Areal" > 0
                GROUP BY CVR, CropCode
                ORDER BY CVR, CropCode
            """
            gkea_totals_csv = output_path / "debug_gkea_cvr_crop_totals.csv"
            self.db.execute_query(
                f"COPY ({gkea_totals_query}) TO '{str(gkea_totals_csv)}' (HEADER, DELIMITER ',')"
            )
            logger.info(f"Saved GKEA CVR-Crop totals (debug) to {gkea_totals_csv}")

            marker_totals_query = """
                SELECT
                    m.CVR as CVR, 
                    TRY_CAST(m.Afgkode AS BIGINT) as CropCode,
                    SUM(m.IMK_areal) as TotalMarkerArea,
                    COUNT(DISTINCT m.Markblok || '_' || m.Marknr) as MarkerFieldCount
                FROM marker m
                WHERE m.CVR IS NOT NULL AND TRIM(m.CVR) != '' AND REGEXP_MATCHES(m.CVR, '^[0-9]+$')
                      AND m.Afgkode IS NOT NULL AND m.IMK_areal > 0
                GROUP BY CVR, CropCode
                ORDER BY CVR, CropCode
            """
            marker_totals_csv = output_path / "debug_marker_cvr_crop_totals.csv"
            self.db.execute_query(
                f"COPY ({marker_totals_query}) TO '{str(marker_totals_csv)}' (HEADER, DELIMITER ',')"
            )
            logger.info(f"Saved Marker CVR-Crop totals (debug) to {marker_totals_csv}")

        except Exception as e:
            logger.error(f"Error saving CVR-Crop totals for debugging: {str(e)}")
            # No raise here, allow main script to continue if debugging fails

    # New disaggregation strategy using subset sum
    def disaggregate_by_subset_sum(
        self, pending_rows_table: str = "pending_pesticide_rows"
    ) -> Set[int]:
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

        logger.info(
            f"SubsetSum: Found {len(candidates)} CVR/Crop candidates to analyze."
        )

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

            pending_acreages_for_cvr_crop = self.db.execute_query(
                pending_acreages_query
            )
            if not pending_acreages_for_cvr_crop:
                continue

            # 2. GKEA Field Areas for this CVR/Crop
            gkea_fields_query = f"""
            SELECT CAST("Marknummer" AS VARCHAR) as FieldID, "Areal" as Area
            FROM gkea 
            WHERE TRIM(CAST("CVR" AS VARCHAR)) = '{target_cvr}' 
              AND TRY_CAST("Hovedafgrøde" AS BIGINT) = {target_cropcode} AND "Areal" > 0
            ORDER BY "Areal";
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
            SELECT CAST(id AS VARCHAR) as FieldID, IMK_areal as Area
            FROM marker 
            WHERE TRIM(CAST(CVR AS VARCHAR)) = '{target_cvr}' 
              AND TRY_CAST(Afgkode AS BIGINT) = {target_cropcode} AND IMK_areal > 0
            ORDER BY IMK_areal;
            """
            marker_fields_raw = self.db.execute_query(marker_fields_query)
            marker_fields_with_ids = [
                (str(fid), area) for fid, area in marker_fields_raw
            ]

            for p_row_id, target_area in pending_acreages_for_cvr_crop:
                if (
                    p_row_id in processed_original_pesticide_row_ids
                ):  # Already handled by a previous CVR/Crop iteration if somehow overlapping
                    continue

                matched_this_acreage = False
                best_subset: List[Tuple[str, float]] = []
                dataset_source = ""

                # --- Try Marker First ---
                if (
                    len(marker_fields_with_ids) > 1
                ):  # Subset sum needs at least 2 fields
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
                        closest_marker_subset = _get_closest_subset_static(
                            marker_subsets, target_area
                        )
                        if closest_marker_subset:
                            best_subset = closest_marker_subset
                            dataset_source = "marker"
                            matched_this_acreage = True

                # --- If Marker didn't match, Try GKEA ---
                if (
                    not matched_this_acreage and len(gkea_fields_with_ids) > 1
                ):  # Subset sum needs at least 2 fields
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
                        closest_gkea_subset = _get_closest_subset_static(
                            gkea_subsets, target_area
                        )
                        if closest_gkea_subset:
                            best_subset = closest_gkea_subset
                            dataset_source = "gkea"
                            matched_this_acreage = True

                if matched_this_acreage and best_subset:
                    # Fetch original pesticide row details
                    original_pesticide_row_details_query = f"SELECT * FROM {pending_rows_table} WHERE OriginalPesticideRowID = {p_row_id}"
                    original_row_data = self.db.execute_query(
                        original_pesticide_row_details_query
                    )

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
                        - (
                            abs(target_area - subset_sum_area)
                            / target_area
                            / (self.config.AREA_TOLERANCE_PCT / 100.0)
                        ),
                    )

                    for field_id_in_subset, field_area_in_subset in best_subset:
                        allocated_area = (
                            target_area * (field_area_in_subset / subset_sum_area)
                            if subset_sum_area > 0
                            else 0
                        )

                        disaggregated_row = (
                            str(uuid.uuid4()),  # DisaggregatedID
                            original_row_dict.get("OriginalPesticideRowID"),
                            original_row_dict.get("CompanyName"),
                            str(
                                original_row_dict.get("CompanyRegistrationNumber")
                            ),  # Ensure string
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
            logger.info(
                "SubsetSum: No pesticide rows were disaggregated by this strategy."
            )

        return processed_original_pesticide_row_ids


# For brevity, the long SQL queries within the methods are referenced as "Original SQL".
# They are identical to those in the original pesticide_analysis.py script (including the recent CVR fix for marker).
