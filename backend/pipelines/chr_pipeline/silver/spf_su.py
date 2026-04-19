"""
SPF-SU Silver Processing Module for CHR Pipeline

This module processes SPF-SU (Specific Pathogen Free - Swine Unit) data from bronze to silver layer.
SPF-SU is the Danish pig health surveillance system that tracks disease-free status and health controls.

Based on SPF-SU documentation:
- Security levels: Red (breeding), Blue (production), Green (establishing)
- Health status: Declared free from SPF diseases with appendices for positive diseases
- Supplementary status: Special health, biosecurity, and sales conditions
- Conditional status: Temporary restrictions due to suspected infections
"""

import logging
from pathlib import Path

import duckdb
from common.logging_utils import get_pipeline_logger

from . import export

logger = get_pipeline_logger(__name__)


def create_spf_su_herds_table(
    con: duckdb.DuckDBPyConnection, spf_su_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """
    Create the main SPF-SU herds table with basic farm information.

    Args:
        con: DuckDB connection
        spf_su_raw_table: Name of the raw SPF-SU data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed SPF-SU herds data or None if failed
    """
    if spf_su_raw_table is None:
        logging.warning("Cannot create SPF-SU herds table: spf_su_raw_table is None")
        return None

    logging.info("Creating SPF-SU herds table...")

    try:
        # Create the cleaned and processed herds table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE spf_su_herds AS
            WITH raw_data AS (
                SELECT
                    ownerDetailInfo.chrNumber AS chr_number_raw,
                    ownerDetailInfo.herdNumber AS herd_number_raw,
                    ownerDetailInfo.ownerNumber AS owner_number_raw,
                    ownerDetailInfo.name AS farm_name_raw,
                    ownerDetailInfo.address.farmName AS address_farm_name_raw,
                    ownerDetailInfo.address.line1 AS address_line1_raw,
                    ownerDetailInfo.address.postalCode AS postal_code_raw,
                    ownerDetailInfo.address.city AS city_raw,
                    ownerDetailInfo.address.name AS address_name_raw,
                    ownerDetailInfo.danishCertificate.approved AS certificate_approved_raw,
                    ownerDetailInfo.danishCertificate.pdfFileName AS certificate_pdf_url_raw,
                    ownerDetailInfo.danishCertificate.date AS certificate_date_raw,
                    ownerDetailInfo.danishCertificate.expiryDate AS certificate_expiry_date_raw,
                    ownerDetailInfo.danishCertificate.isExpired AS certificate_is_expired_raw,
                    ownerDetailInfo.healthData.conditionalStatus AS conditional_status_raw,
                    ownerDetailInfo.healthData.healthStatus AS health_status_raw,
                    ownerDetailInfo.healthData.healthStatusColor AS health_status_color_raw,
                    ownerDetailInfo.healthData.supplementaryStatus AS supplementary_status_raw,
                    ownerDetailInfo.salmonellaData.salmonellaDate AS salmonella_date_raw,
                    ownerDetailInfo.salmonellaData.salmonellaStatus AS salmonella_status_raw,
                    ownerDetailInfo.salmonellaData.hasIndexDetails AS salmonella_has_index_details_raw,
                    ownerDetailInfo.salmonellaData.showData AS salmonella_show_data_raw,
                    _export_timestamp
                FROM {spf_su_raw_table}
                WHERE ownerDetailInfo IS NOT NULL
            )
            SELECT
                uuid() AS spf_su_id,
                -- Basic identifiers: cast to string, trim, nullif empty, then cast to int64
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(owner_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS owner_number,
                -- Farm information
                NULLIF(TRIM(CAST(farm_name_raw AS VARCHAR)), '') AS farm_name,
                NULLIF(TRIM(CAST(address_farm_name_raw AS VARCHAR)), '') AS address_farm_name,
                NULLIF(TRIM(CAST(address_line1_raw AS VARCHAR)), '') AS address_line1,
                NULLIF(TRIM(CAST(postal_code_raw AS VARCHAR)), '') AS postal_code,
                NULLIF(TRIM(CAST(city_raw AS VARCHAR)), '') AS city,
                NULLIF(TRIM(CAST(address_name_raw AS VARCHAR)), '') AS address_name,
                -- Certificate information
                CAST(certificate_approved_raw AS BOOLEAN) AS certificate_approved,
                NULLIF(TRIM(CAST(certificate_pdf_url_raw AS VARCHAR)), '') AS certificate_pdf_url,
                TRY_CAST(certificate_date_raw AS TIMESTAMP) AS certificate_date,
                TRY_CAST(certificate_expiry_date_raw AS TIMESTAMP) AS certificate_expiry_date,
                CAST(certificate_is_expired_raw AS BOOLEAN) AS certificate_is_expired,
                -- Health status information
                NULLIF(TRIM(CAST(conditional_status_raw AS VARCHAR)), '') AS conditional_status,
                NULLIF(TRIM(CAST(health_status_raw AS VARCHAR)), '') AS health_status,
                NULLIF(TRIM(CAST(health_status_color_raw AS VARCHAR)), '') AS health_status_color,
                NULLIF(TRIM(CAST(supplementary_status_raw AS VARCHAR)), '') AS supplementary_status,
                -- Salmonella information
                TRY_CAST(salmonella_date_raw AS TIMESTAMP) AS salmonella_date,
                NULLIF(TRIM(CAST(salmonella_status_raw AS VARCHAR)), '') AS salmonella_status,
                CAST(salmonella_has_index_details_raw AS BOOLEAN) AS salmonella_has_index_details,
                CAST(salmonella_show_data_raw AS BOOLEAN) AS salmonella_show_data,
                -- Metadata
                CAST(_export_timestamp AS VARCHAR) AS export_timestamp
            FROM raw_data
            WHERE
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
                AND COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM spf_su_herds").fetchone()[0]

        if rows == 0:
            logging.warning("SPF-SU herds table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU herds table with {rows} rows")

        # Save to parquet
        output_path = silver_dir / "spf_su_herds.parquet"
        saved_path = export.save_table(output_path, "spf_su_herds", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU herds table - no path returned")
            return None

        logging.info(f"Saved SPF-SU herds table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM spf_su_herds")

    except Exception as e:
        logging.error(f"Failed to create SPF-SU herds table: {e}", exc_info=True)
        return None


def create_spf_su_health_controls_table(
    con: duckdb.DuckDBPyConnection, spf_su_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """
    Create SPF-SU health controls table with disease-specific information.

    This table contains the appendices (disease codes) that farms are controlled for.
    Based on SPF-SU documentation:
    - Myc: Mycoplasma lung disease
    - Ap: Malignant lung disease (Actinobacillus pleuropneumoniae)
    - PRRS1/PRRS2: PRRS virus (European/American)
    - Dys: Swine dysentery
    - Nys: Atrophic rhinitis
    - Skab: Mange
    - Lus: Lice

    Args:
        con: DuckDB connection
        spf_su_raw_table: Name of the raw SPF-SU data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed SPF-SU health controls data or None if failed
    """
    if spf_su_raw_table is None:
        logging.warning("Cannot create SPF-SU health controls table: spf_su_raw_table is None")
        return None

    logging.info("Creating SPF-SU health controls table...")

    try:
        # Create the health controls table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE spf_su_health_controls AS
            WITH unnested_controls AS (
                SELECT
                    ownerDetailInfo.chrNumber AS chr_number_raw,
                    ownerDetailInfo.herdNumber AS herd_number_raw,
                    UNNEST(healthStatus.healthControlInfo) AS health_control_raw,
                    _export_timestamp
                FROM {spf_su_raw_table}
                WHERE ownerDetailInfo IS NOT NULL
                  AND healthStatus.healthControlInfo IS NOT NULL
                  AND len(healthStatus.healthControlInfo) > 0
            ),
            expanded_controls AS (
                SELECT
                    chr_number_raw,
                    herd_number_raw,
                    health_control_raw.disease AS disease_code_raw,
                    _export_timestamp
                FROM unnested_controls
                WHERE health_control_raw.disease IS NOT NULL
            )
            SELECT
                uuid() AS health_control_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                -- Disease information
                NULLIF(TRIM(CAST(disease_code_raw AS VARCHAR)), '') AS disease_code,
                -- Disease name mapping based on SPF-SU documentation
                CASE TRIM(CAST(disease_code_raw AS VARCHAR))
                    WHEN 'Myc' THEN 'Mycoplasma lung disease'
                    WHEN 'PRRS1' THEN 'PRRS virus (European)'
                    WHEN 'PRRS2' THEN 'PRRS virus (American)'
                    WHEN 'Dys' THEN 'Swine dysentery'
                    WHEN 'Nys' THEN 'Atrophic rhinitis'
                    WHEN 'Skab' THEN 'Mange'
                    WHEN 'Lus' THEN 'Lice'
                    ELSE
                        CASE
                            WHEN TRIM(CAST(disease_code_raw AS VARCHAR)) LIKE 'Ap%'
                            THEN 'Malignant lung disease (Actinobacillus)'
                            ELSE TRIM(CAST(disease_code_raw AS VARCHAR))
                        END
                END AS disease_name,
                -- Metadata
                CAST(_export_timestamp AS VARCHAR) AS export_timestamp
            FROM expanded_controls
            WHERE
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
                AND COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
                AND NULLIF(TRIM(CAST(disease_code_raw AS VARCHAR)), '') IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM spf_su_health_controls").fetchone()[0]

        if rows == 0:
            logging.warning("SPF-SU health controls table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU health controls table with {rows} rows")

        # Save to parquet
        output_path = silver_dir / "spf_su_health_controls.parquet"
        saved_path = export.save_table(output_path, "spf_su_health_controls", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU health controls table - no path returned")
            return None

        logging.info(f"Saved SPF-SU health controls table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM spf_su_health_controls")

    except Exception as e:
        logging.error(f"Failed to create SPF-SU health controls table: {e}", exc_info=True)
        return None


def create_spf_su_salmonella_data_table(
    con: duckdb.DuckDBPyConnection, spf_su_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """
    Create SPF-SU salmonella data table with detailed salmonella information.

    This table contains salmonella levels, indexes, and test results.

    Args:
        con: DuckDB connection
        spf_su_raw_table: Name of the raw SPF-SU data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed SPF-SU salmonella data or None if failed
    """
    if spf_su_raw_table is None:
        logging.warning("Cannot create SPF-SU salmonella data table: spf_su_raw_table is None")
        return None

    logging.info("Creating SPF-SU salmonella data table...")

    try:
        # Create the salmonella data table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE spf_su_salmonella_data AS
            WITH raw_salmonella AS (
                SELECT
                    ownerDetailInfo.chrNumber AS chr_number_raw,
                    ownerDetailInfo.herdNumber AS herd_number_raw,
                    ownerDetailInfo.salmonellaData.salmonellaLevel AS salmonella_level_array_raw,
                    ownerDetailInfo.salmonellaData.salmonellaIndexes AS salmonella_indexes_array_raw,
                    ownerDetailInfo.salmonellaData.salmonellaTestResults AS salmonella_test_results_array_raw,
                    ownerDetailInfo.salmonellaData.salmonellaDate AS salmonella_date_raw,
                    ownerDetailInfo.salmonellaData.salmonellaStatus AS salmonella_status_raw,
                    ownerDetailInfo.salmonellaData.hasIndexDetails AS has_index_details_raw,
                    ownerDetailInfo.salmonellaData.showData AS show_data_raw,
                    _export_timestamp
                FROM {spf_su_raw_table}
                WHERE ownerDetailInfo IS NOT NULL
                  AND ownerDetailInfo.salmonellaData IS NOT NULL
            )
            SELECT
                uuid() AS salmonella_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                -- Salmonella information
                TRY_CAST(salmonella_date_raw AS TIMESTAMP) AS salmonella_date,
                NULLIF(TRIM(CAST(salmonella_status_raw AS VARCHAR)), '') AS salmonella_status,
                CAST(has_index_details_raw AS BOOLEAN) AS has_index_details,
                CAST(show_data_raw AS BOOLEAN) AS show_data,
                -- Convert arrays to JSON strings for storage
                CAST(salmonella_level_array_raw AS VARCHAR) AS salmonella_level_json,
                CAST(salmonella_indexes_array_raw AS VARCHAR) AS salmonella_indexes_json,
                CAST(salmonella_test_results_array_raw AS VARCHAR) AS salmonella_test_results_json,
                -- Metadata
                CAST(_export_timestamp AS VARCHAR) AS export_timestamp
            FROM raw_salmonella
            WHERE
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
                AND COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM spf_su_salmonella_data").fetchone()[0]

        if rows == 0:
            logging.warning("SPF-SU salmonella data table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU salmonella data table with {rows} rows")

        # Save to parquet
        output_path = silver_dir / "spf_su_salmonella_data.parquet"
        saved_path = export.save_table(output_path, "spf_su_salmonella_data", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU salmonella data table - no path returned")
            return None

        logging.info(f"Saved SPF-SU salmonella data table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM spf_su_salmonella_data")

    except Exception as e:
        logging.error(f"Failed to create SPF-SU salmonella data table: {e}", exc_info=True)
        return None
