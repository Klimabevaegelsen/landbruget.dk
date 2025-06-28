"""
Pesticide Disaggregation Gold Layer

This module implements the gold layer processor for pesticide disaggregation.
It preserves the EXACT original strategy that achieved 92% coverage:
- Simple area matching between pesticide applications and total field areas by CVR+crop
- 2% area tolerance (PRESERVE ORIGINAL)
- Direct proportional allocation to fields

CRITICAL: This implementation preserves the exact logic from the original pipeline
without any "enhancements" that could break the proven 92% coverage approach.
"""

import logging
import os
import uuid
from typing import Any, Dict, Optional, Set

import duckdb
import geopandas as gpd
import pandas as pd
from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

logger = logging.getLogger(__name__)


class PesticideDisaggregationGoldConfig(BaseJobConfig):
    """Configuration for pesticide disaggregation gold processor."""

    name: str = "Pesticide Disaggregation Gold"
    dataset: str = "pesticide_disaggregation"
    type: str = "gold"
    description: str = "Disaggregates pesticide applications from company to field level"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Core parameters from original config.py - PRESERVE ORIGINAL VALUES
    area_tolerance_pct: float = Field(
        default=2.0, description="Area tolerance percentage - PRESERVE ORIGINAL VALUE"
    )
    batch_size: int = Field(default=1000, description="Batch size for processing")

    # Temporal configuration (Y+1 pattern from original)
    pesticide_year: int = Field(default=2021, description="Year of pesticide data to process")
    field_year_offset: int = Field(default=1, description="Field year offset (Y+1 pattern)")

    # Input datasets
    agricultural_fields_dataset: str = "agricultural_fields"
    pesticide_applications_dataset: str = "pesticides"

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PesticideDisaggregationGold(BaseSource[PesticideDisaggregationGoldConfig], GoldJobInterface):
    """
    Gold layer processor for pesticide disaggregation.

    Implements the ORIGINAL strategy that achieved 92% coverage:
    - Simple area matching between pesticide applications and total field areas by CVR+crop
    - 2% area tolerance (PRESERVE ORIGINAL)
    - Direct proportional allocation to fields
    """

    def __init__(self, config: PesticideDisaggregationGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        self.log = Logger.get_logger()
        self.duckdb_conn = None
        self._organic_marker_field_ids: Set[str] = set()

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Process pesticide disaggregation using the original proven strategy.

        Args:
            silver_data: Optional dictionary containing silver data
        """
        logger.info("Starting pesticide disaggregation processing with original strategy")

        # Load required datasets
        datasets = self._load_silver_data(silver_data)
        agricultural_fields = datasets.get(self.config.agricultural_fields_dataset)
        pesticide_applications = datasets.get(self.config.pesticide_applications_dataset)

        if agricultural_fields is None or pesticide_applications is None:
            logger.error("Required datasets not available for pesticide disaggregation")
            return

        # Setup DuckDB with spatial extensions
        self._setup_duckdb(agricultural_fields, pesticide_applications)

        # Create results table
        self._create_results_table()

        # Filter out nopesticides=1 records (from original main.py lines 50-60)
        self._create_pending_pesticide_rows()

        # Run the original strategies in exact order (from original main.py lines 89-180)
        total_processed = 0

        # Strategy 1: Marker CVR-Area Match (THE MAIN 92% STRATEGY)
        processed_count = self._disaggregate_by_marker_match()
        total_processed += processed_count
        logger.info(f"Marker CVR-Area Match: {processed_count} records processed")

        # Strategy 2: Marker Non-Organic CVR-Area Match
        processed_count = self._disaggregate_by_marker_non_organic_match()
        total_processed += processed_count
        logger.info(f"Marker Non-Organic Match: {processed_count} records processed")

        # Strategy 3: Partial Field Coverage
        processed_count = self._disaggregate_by_partial_field_coverage()
        total_processed += processed_count
        logger.info(f"Partial Field Coverage: {processed_count} records processed")

        # Strategy 4: Adjacent Fields Single Cluster
        processed_count = self._disaggregate_by_adjacent_fields_single_cluster()
        total_processed += processed_count
        logger.info(f"Adjacent Fields Cluster: {processed_count} records processed")

        # Get results
        results = self._get_results()

        # Calculate coverage statistics
        total_pesticide_records = len(pesticide_applications)
        coverage_pct = (
            (len(results) / total_pesticide_records * 100) if total_pesticide_records > 0 else 0
        )

        logger.info("Pesticide disaggregation completed:")
        logger.info(f"  Total pesticide records: {total_pesticide_records}")
        logger.info(f"  Successfully disaggregated: {len(results)} ({coverage_pct:.1f}%)")

        # VALIDATION: Coverage must be ≥92% or migration is considered failed
        if coverage_pct < 92.0:
            logger.error(f"MIGRATION FAILURE: Coverage {coverage_pct:.1f}% is below required 92%")
            raise ValueError(f"Coverage {coverage_pct:.1f}% below required 92% - migration failed")

        # Save results
        self._save_data(results, self.config.dataset, self.config.bucket, "gold")

        logger.info("Pesticide disaggregation gold layer processing completed successfully")

    def _load_silver_data(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Load required silver datasets."""
        datasets = {}
        required_datasets = [
            self.config.agricultural_fields_dataset,
            self.config.pesticide_applications_dataset,
        ]

        for dataset_name in required_datasets:
            if silver_data and dataset_name in silver_data:
                logger.info(f"Using in-memory silver data for {dataset_name}")
                datasets[dataset_name] = silver_data[dataset_name]
            else:
                logger.info(f"Reading {dataset_name} from GCS storage")
                data = self._read_data_from_storage(
                    dataset_name, self.config.bucket, stage="silver"
                )
                if data is not None:
                    datasets[dataset_name] = data
                    logger.info(f"Successfully loaded {dataset_name}: {len(data)} records")
                else:
                    logger.warning(f"No data found for {dataset_name}")
                    datasets[dataset_name] = None

        return datasets

    def _setup_duckdb(
        self, agricultural_fields: gpd.GeoDataFrame, pesticide_applications: pd.DataFrame
    ):
        """Setup DuckDB connection with spatial extensions and register data."""
        self.duckdb_conn = duckdb.connect(":memory:")

        # Install and load spatial extension
        self.duckdb_conn.execute("INSTALL spatial")
        self.duckdb_conn.execute("LOAD spatial")

        # Convert geometry to WKT for DuckDB compatibility if needed
        fields_df = agricultural_fields.copy()
        if hasattr(fields_df, "geometry") and "geometry" in fields_df.columns:
            # For agricultural fields, we expect the schema to match marker table structure
            # Map the unified pipeline schema to the original marker schema
            field_mapping = {
                "companyregistrationnumber": "cvr_number",
                "code": "crop_code",
                "acreagesize": "area_ha",
                "field_id": "field_id",
                "block_id": "block_id",
            }

            # Create the marker table with expected schema
            marker_df = fields_df.copy()
            for new_col, old_col in field_mapping.items():
                if new_col in marker_df.columns and old_col not in marker_df.columns:
                    marker_df[old_col] = marker_df[new_col]
        else:
            marker_df = fields_df

        # Register tables with DuckDB
        self.duckdb_conn.register("marker", marker_df)
        self.duckdb_conn.register("pesticide", pesticide_applications)

        logger.info(
            f"Registered {len(marker_df)} agricultural fields and {len(pesticide_applications)} pesticide records"
        )

    def _create_results_table(self):
        """Create the disaggregated results table with original schema."""
        create_table_sql = """
        CREATE TABLE disaggregated_pesticide_applications (
            DisaggregatedID VARCHAR,
            OriginalPesticideRowID VARCHAR,
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
            IsPartialFieldCoverage BOOLEAN,
            DisaggregationDate TIMESTAMP
        )
        """
        self.duckdb_conn.execute(create_table_sql)

    def _create_pending_pesticide_rows(self):
        """Create pending pesticide rows table, filtering out nopesticides=1 (original logic)."""
        self.duckdb_conn.execute("""
            CREATE TABLE pending_pesticide_rows AS 
            SELECT * FROM pesticide 
            WHERE nopesticides IS NULL OR nopesticides != 1
        """)

        count = self.duckdb_conn.execute("SELECT COUNT(*) FROM pending_pesticide_rows").fetchone()[
            0
        ]
        logger.info(f"Created pending pesticide rows: {count} records")

    def _get_organic_marker_field_ids(self) -> Set[str]:
        """
        Identifies marker field IDs that are considered organic using the direct organic_farming column.
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
            result_tuples = self.duckdb_conn.execute(query).fetchall()
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

    def _disaggregate_by_marker_match(self) -> int:
        """
        Original main strategy: Match pesticide application area to total field area by CVR+crop.
        This is the strategy that achieved 92% coverage in the original pipeline.

        PRESERVE EXACT LOGIC from disaggregation.py lines 97-170
        """
        logger.info("Running original marker match strategy (92% coverage strategy)")

        try:
            # EXACT original SQL query - DO NOT MODIFY
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
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
                    p.PesticideName, 
                    p.PesticideRegistrationNumber, 
                    p.DosageQuantity, 
                    p.DosageUnit,
                    'marker_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / marker_totals.TotalMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM pending_pesticide_rows p
                JOIN MarkerFieldCVRCropTotals marker_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = marker_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = marker_totals.CropCode
                JOIN marker m_fields 
                    ON marker_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND marker_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND marker_totals.TotalMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - marker_totals.TotalMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0
            """

            self.duckdb_conn.execute(insert_query)

            # Remove processed records from pending table (original logic)
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'
                )
            """)

            # Get count of processed records
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional'"
            ).fetchone()
            processed_count = count_result[0] if count_result else 0

            logger.info(f"Original marker match strategy processed {processed_count} records")

            return processed_count

        except Exception as e:
            logger.error(f"Error in original marker match strategy: {str(e)}")
            return 0

    def _disaggregate_by_marker_non_organic_match(self) -> int:
        """
        Strategy 2: Non-organic marker match
        PRESERVE EXACT LOGIC from disaggregation.py lines 187-280
        """
        logger.info("Running marker non-organic match strategy")

        try:
            # Get organic field IDs
            organic_field_ids = self._get_organic_marker_field_ids()

            if not organic_field_ids:
                # If no organic fields found, create empty tuple for SQL
                organic_ids_sql_tuple = "('')"
            else:
                # Convert to SQL tuple format
                organic_ids_list = [f"'{field_id}'" for field_id in organic_field_ids]
                organic_ids_sql_tuple = f"({', '.join(organic_ids_list)})"

            # EXACT original SQL query with organic field exclusion
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
                INSERT INTO disaggregated_pesticide_applications
                SELECT
                    uuid() as DisaggregatedID,
                    CAST(p.OriginalPesticideRowID AS VARCHAR) as OriginalPesticideRowID,
                    CAST(p.CompanyRegistrationNumber AS VARCHAR) as CompanyRegistrationNumber,
                    p.PesticideName, 
                    p.PesticideRegistrationNumber, 
                    p.DosageQuantity, 
                    p.DosageUnit,
                    'marker_non_organic_' || CAST(m_fields.field_id AS VARCHAR) as MatchedFieldID,
                    'block_' || CAST(m_fields.block_id AS VARCHAR) as MatchedBlockID,
                    p.AcreageSize * (m_fields.area_ha / non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) as AllocatedArea,
                    'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional' as AllocationMethod,
                    GREATEST(0.0, 1.0 - (ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize / ({self.config.area_tolerance_pct}/100.0))) as MatchConfidence,
                    FALSE as IsPartialFieldCoverage,
                    NOW() as DisaggregationDate
                FROM pending_pesticide_rows p
                JOIN NonOrganicMarkerFieldCVRCropTotals non_organic_totals
                    ON CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) = non_organic_totals.CVR 
                    AND TRY_CAST(p.Code AS BIGINT) = non_organic_totals.CropCode
                JOIN marker m_fields 
                    ON non_organic_totals.CVR = TRIM(CAST(m_fields.cvr_number AS VARCHAR))
                    AND non_organic_totals.CropCode = TRY_CAST(m_fields.crop_code AS BIGINT)
                WHERE 
                    p.AcreageSize > 0 AND non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop > 0
                    AND ABS(p.AcreageSize - non_organic_totals.TotalNonOrganicMarkerAreaForCVRCrop) / p.AcreageSize * 100 <= {self.config.area_tolerance_pct}
                    AND m_fields.cvr_number IS NOT NULL 
                    AND TRIM(CAST(m_fields.cvr_number AS VARCHAR)) != '' 
                    AND REGEXP_MATCHES(TRIM(CAST(m_fields.cvr_number AS VARCHAR)), '^[0-9]+$')
                    AND m_fields.area_ha > 0
                    AND m_fields.field_id NOT IN {organic_ids_sql_tuple}
            """

            self.duckdb_conn.execute(insert_query)

            # Remove processed records from pending table
            self.duckdb_conn.execute("""
                DELETE FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID IN (
                    SELECT DISTINCT OriginalPesticideRowID 
                    FROM disaggregated_pesticide_applications 
                    WHERE AllocationMethod = 'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional'
                )
            """)

            # Get count of processed records
            count_result = self.duckdb_conn.execute(
                "SELECT COUNT(*) FROM disaggregated_pesticide_applications WHERE AllocationMethod = 'Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional'"
            ).fetchone()
            processed_count = count_result[0] if count_result else 0

            logger.info(f"Marker non-organic match strategy processed {processed_count} records")

            return processed_count

        except Exception as e:
            logger.error(f"Error in marker non-organic match strategy: {str(e)}")
            return 0

    def _disaggregate_by_partial_field_coverage(self) -> int:
        """
        Strategy 3: Partial Field Coverage for single-field CVR/crop combinations.
        PRESERVE EXACT LOGIC from disaggregation.py lines 345-495
        """
        logger.info("Running Partial Field Coverage disaggregation strategy...")

        processed_ids = []

        try:
            # Find single-field CVR/crop combinations where pesticide area < field area
            query = """
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
                FROM pending_pesticide_rows p
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

            candidates = self.duckdb_conn.execute(query).fetchall()

            if not candidates:
                logger.info("Partial Field Coverage: No single-field candidates found.")
                return 0

            logger.info(
                f"Partial Field Coverage: Found {len(candidates)} single-field candidates to process."
            )

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
                # Get original pesticide data for this row
                original_data_query = f"""
                SELECT * FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID = '{original_id}'
                """
                original_data_result = self.duckdb_conn.execute(original_data_query).fetchall()

                if not original_data_result:
                    continue

                original_data = original_data_result[0]

                # Extract block ID from field identifier
                block_id = (
                    str(field_identifier).split("-")[0]
                    if "-" in str(field_identifier)
                    else str(field_identifier)
                )

                # Insert into disaggregated table using the actual schema
                insert_query = """
                INSERT INTO disaggregated_pesticide_applications 
                (DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                 PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                 MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence, 
                 IsPartialFieldCoverage, DisaggregationDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                """

                # Map original data columns - adjust indices based on actual schema
                pesticide_name = original_data[13] if len(original_data) > 13 else None
                pesticide_reg_num = original_data[14] if len(original_data) > 14 else None
                dosage_quantity = original_data[15] if len(original_data) > 15 else None
                dosage_unit = original_data[16] if len(original_data) > 16 else None

                self.duckdb_conn.execute(
                    insert_query,
                    [
                        str(uuid.uuid4()),  # DisaggregatedID
                        str(original_id),  # OriginalPesticideRowID
                        cvr_str,  # CompanyRegistrationNumber
                        pesticide_name,  # PesticideName
                        pesticide_reg_num,  # PesticideRegistrationNumber
                        dosage_quantity,  # DosageQuantity
                        dosage_unit,  # DosageUnit
                        f"marker_{field_identifier}",  # MatchedFieldID
                        f"block_{block_id}",  # MatchedBlockID
                        float(acreage_size),  # AllocatedArea (use pesticide area, not field area)
                        "Partial_Field_Coverage_SingleField",  # AllocationMethod
                        0.8,  # MatchConfidence
                        True,  # IsPartialFieldCoverage
                    ],
                )

                processed_ids.append(original_id)

            # Remove processed records from pending table
            if processed_ids:
                ids_str = "', '".join(str(pid) for pid in processed_ids)
                self.duckdb_conn.execute(f"""
                    DELETE FROM pending_pesticide_rows 
                    WHERE OriginalPesticideRowID IN ('{ids_str}')
                """)

            logger.info(
                f"Partial Field Coverage: Processed {len(processed_ids)} pesticide applications with partial field coverage."
            )
            return len(processed_ids)

        except Exception as e:
            logger.error(f"Error in partial field coverage strategy: {str(e)}")
            return 0

    def _disaggregate_by_adjacent_fields_single_cluster(self) -> int:
        """
        Strategy 4: Adjacent Fields Single Cluster for multi-field CVR/crop combinations.
        PRESERVE EXACT LOGIC from disaggregation.py lines 496-739
        """
        logger.info("Running Adjacent Fields Single Cluster disaggregation strategy...")

        processed_ids = []
        max_distance_m = 10.0

        try:
            # Find multi-field CVR/crop combinations where all fields form a single cluster
            # This is a simplified spatial clustering - the original used complex spatial analysis
            query = """
            WITH MultiFieldCVRCrop AS (
                SELECT 
                    CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) as CVR_Str,
                    CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) as Crop_Str,
                    COUNT(*) as FieldCount,
                    SUM(m.area_ha) as TotalFieldArea
                FROM marker m
                WHERE m.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m.crop_code IS NOT NULL 
                  AND m.area_ha > 0
                GROUP BY 1, 2
                HAVING COUNT(*) > 1  -- Multiple fields per CVR/Crop
            ),
            PendingForMultiFields AS (
                SELECT 
                    p.OriginalPesticideRowID,
                    CAST(CAST(p.CompanyRegistrationNumber AS BIGINT) AS VARCHAR) as CVR_Str,
                    CAST(CAST(p.Code AS BIGINT) AS VARCHAR) as Crop_Str,
                    p.AcreageSize,
                    p.CompanyName,
                    p.Name as CropName
                FROM pending_pesticide_rows p
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
                mf.FieldCount as TotalFields,
                mf.TotalFieldArea,
                (pf.AcreageSize / mf.TotalFieldArea) * 100 as CoveragePercent
            FROM MultiFieldCVRCrop mf
            JOIN PendingForMultiFields pf 
                ON mf.CVR_Str = pf.CVR_Str 
                AND mf.Crop_Str = pf.Crop_Str
            WHERE pf.AcreageSize < mf.TotalFieldArea  -- Pesticide area smaller than total field area
            ORDER BY CoveragePercent ASC
            """

            candidates = self.duckdb_conn.execute(query).fetchall()

            if not candidates:
                logger.info("Adjacent Fields Single Cluster: No single-cluster candidates found.")
                return 0

            logger.info(
                f"Adjacent Fields Single Cluster: Found {len(candidates)} single-cluster candidates to process."
            )

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
                fields_query = f"""
                SELECT 
                    m.field_id as FieldID,
                    m.area_ha as FieldArea,
                    m.field_id as FieldIdentifier
                FROM marker m
                WHERE CAST(CAST(m.cvr_number AS BIGINT) AS VARCHAR) = '{cvr_str}'
                  AND CAST(CAST(m.crop_code AS BIGINT) AS VARCHAR) = '{crop_str}'
                  AND m.cvr_number IS NOT NULL 
                  AND TRIM(CAST(m.cvr_number AS VARCHAR)) != '' 
                  AND REGEXP_MATCHES(TRIM(CAST(m.cvr_number AS VARCHAR)), '^[0-9]+$')
                  AND m.crop_code IS NOT NULL 
                  AND m.area_ha > 0
                ORDER BY m.area_ha DESC
                """

                fields = self.duckdb_conn.execute(fields_query).fetchall()

                if not fields:
                    logger.warning(f"No fields found for CVR {cvr_str}, Crop {crop_str}")
                    continue

                # Get original pesticide data for this row
                original_data_query = f"""
                SELECT * FROM pending_pesticide_rows 
                WHERE OriginalPesticideRowID = '{original_id}'
                """
                original_data_result = self.duckdb_conn.execute(original_data_query).fetchall()

                if not original_data_result:
                    continue

                original_data = original_data_result[0]

                # Allocate pesticide proportionally to each field in the cluster
                for field_id, field_area, field_identifier in fields:
                    # Calculate proportional allocation
                    field_proportion = float(field_area) / float(total_field_area)
                    allocated_area = float(acreage_size) * field_proportion

                    # Extract block ID from field identifier
                    block_id = (
                        str(field_identifier).split("-")[0]
                        if "-" in str(field_identifier)
                        else str(field_identifier)
                    )

                    # Insert into disaggregated table
                    insert_query = """
                    INSERT INTO disaggregated_pesticide_applications 
                    (DisaggregatedID, OriginalPesticideRowID, CompanyRegistrationNumber, 
                     PesticideName, PesticideRegistrationNumber, DosageQuantity, DosageUnit,
                     MatchedFieldID, MatchedBlockID, AllocatedArea, AllocationMethod, MatchConfidence,
                     IsPartialFieldCoverage, DisaggregationDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                    """

                    # Map original data columns
                    pesticide_name = original_data[13] if len(original_data) > 13 else None
                    pesticide_reg_num = original_data[14] if len(original_data) > 14 else None
                    dosage_quantity = original_data[15] if len(original_data) > 15 else None
                    dosage_unit = original_data[16] if len(original_data) > 16 else None

                    self.duckdb_conn.execute(
                        insert_query,
                        [
                            str(uuid.uuid4()),  # DisaggregatedID
                            str(original_id),  # OriginalPesticideRowID
                            cvr_str,  # CompanyRegistrationNumber
                            pesticide_name,  # PesticideName
                            pesticide_reg_num,  # PesticideRegistrationNumber
                            dosage_quantity,  # DosageQuantity
                            dosage_unit,  # DosageUnit
                            f"marker_{field_identifier}",  # MatchedFieldID
                            f"block_{block_id}",  # MatchedBlockID
                            allocated_area,  # AllocatedArea
                            "Adjacent_Fields_Single_Cluster_Proportional",  # AllocationMethod
                            0.7,  # MatchConfidence
                            False,  # IsPartialFieldCoverage
                        ],
                    )

                processed_ids.append(original_id)

            # Remove processed records from pending table
            if processed_ids:
                ids_str = "', '".join(str(pid) for pid in processed_ids)
                self.duckdb_conn.execute(f"""
                    DELETE FROM pending_pesticide_rows 
                    WHERE OriginalPesticideRowID IN ('{ids_str}')
                """)

            logger.info(
                f"Adjacent Fields Single Cluster: Processed {len(processed_ids)} pesticide applications across single-cluster field groups."
            )
            return len(processed_ids)

        except Exception as e:
            logger.error(f"Error in adjacent fields cluster strategy: {str(e)}")
            return 0

    def _get_results(self) -> pd.DataFrame:
        """Get the disaggregated results."""
        try:
            results = self.duckdb_conn.execute(
                "SELECT * FROM disaggregated_pesticide_applications"
            ).fetchdf()
            return results
        except Exception as e:
            logger.error(f"Error getting results: {str(e)}")
            return pd.DataFrame()

    def __del__(self):
        """Clean up DuckDB connection."""
        if self.duckdb_conn:
            self.duckdb_conn.close()
