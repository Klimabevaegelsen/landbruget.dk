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
from typing import Optional

import ibis
import ibis.expr.datatypes as dt

from . import export

logger = logging.getLogger(__name__)


def create_spf_su_herds_table(
    con: ibis.BaseBackend, spf_su_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """
    Create the main SPF-SU herds table with basic farm information.

    Args:
        con: Database connection
        spf_su_raw: Raw SPF-SU data table
        silver_dir: Output directory for silver files

    Returns:
        Processed SPF-SU herds table or None if failed
    """
    if spf_su_raw is None:
        logging.warning("Cannot create SPF-SU herds table: spf_su_raw is None")
        return None

    logging.info("Creating SPF-SU herds table...")

    try:
        # Register table for SQL operations
        con.create_table("spf_su_raw", spf_su_raw, overwrite=True)

        # Extract basic herd information
        spf_su_herds = con.sql("""
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
            FROM spf_su_raw
            WHERE ownerDetailInfo IS NOT NULL
        """)

        # Clean and cast columns
        spf_su_herds_clean = spf_su_herds.mutate(
            # Generate unique ID
            spf_su_id=ibis.uuid(),
            # Basic identifiers
            chr_number=ibis.coalesce(
                spf_su_herds.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            herd_number=ibis.coalesce(
                spf_su_herds.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            owner_number=ibis.coalesce(
                spf_su_herds.owner_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # Farm information
            farm_name=spf_su_herds.farm_name_raw.cast(dt.string).strip().nullif(""),
            address_farm_name=spf_su_herds.address_farm_name_raw.cast(dt.string).strip().nullif(""),
            address_line1=spf_su_herds.address_line1_raw.cast(dt.string).strip().nullif(""),
            postal_code=spf_su_herds.postal_code_raw.cast(dt.string).strip().nullif(""),
            city=spf_su_herds.city_raw.cast(dt.string).strip().nullif(""),
            address_name=spf_su_herds.address_name_raw.cast(dt.string).strip().nullif(""),
            # Certificate information
            certificate_approved=spf_su_herds.certificate_approved_raw.cast(dt.boolean),
            certificate_pdf_url=spf_su_herds.certificate_pdf_url_raw.cast(dt.string).strip().nullif(""),
            certificate_date=spf_su_herds.certificate_date_raw.cast(dt.timestamp),
            certificate_expiry_date=spf_su_herds.certificate_expiry_date_raw.cast(dt.timestamp),
            certificate_is_expired=spf_su_herds.certificate_is_expired_raw.cast(dt.boolean),
            # Health status information
            conditional_status=spf_su_herds.conditional_status_raw.cast(dt.string).strip().nullif(""),
            health_status=spf_su_herds.health_status_raw.cast(dt.string).strip().nullif(""),
            health_status_color=spf_su_herds.health_status_color_raw.cast(dt.string).strip().nullif(""),
            supplementary_status=spf_su_herds.supplementary_status_raw.cast(dt.string).strip().nullif(""),
            # Salmonella information
            salmonella_date=spf_su_herds.salmonella_date_raw.cast(dt.timestamp),
            salmonella_status=spf_su_herds.salmonella_status_raw.cast(dt.string).strip().nullif(""),
            salmonella_has_index_details=spf_su_herds.salmonella_has_index_details_raw.cast(dt.boolean),
            salmonella_show_data=spf_su_herds.salmonella_show_data_raw.cast(dt.boolean),
            # Metadata
            export_timestamp=spf_su_herds._export_timestamp.cast(dt.string),
        )

        # Select final columns
        final_cols = [
            "spf_su_id",
            "chr_number",
            "herd_number",
            "owner_number",
            "farm_name",
            "address_farm_name",
            "address_line1",
            "postal_code",
            "city",
            "address_name",
            "certificate_approved",
            "certificate_pdf_url",
            "certificate_date",
            "certificate_expiry_date",
            "certificate_is_expired",
            "conditional_status",
            "health_status",
            "health_status_color",
            "supplementary_status",
            "salmonella_date",
            "salmonella_status",
            "salmonella_has_index_details",
            "salmonella_show_data",
            "export_timestamp",
        ]

        spf_su_herds_final = spf_su_herds_clean.select(*final_cols)

        # Filter out rows with null identifiers
        spf_su_herds_final = spf_su_herds_final.filter(
            spf_su_herds_final.chr_number.notnull() & spf_su_herds_final.herd_number.notnull()
        )

        # Save to parquet
        output_path = silver_dir / "spf_su_herds.parquet"
        rows = spf_su_herds_final.count().execute()

        if rows == 0:
            logging.warning("SPF-SU herds table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU herds table with {rows} rows")
        saved_path = export.save_table(output_path, spf_su_herds_final, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU herds table - no path returned")
            return None

        logging.info(f"Saved SPF-SU herds table to {saved_path}")

        # Clean up
        try:
            con.drop_table("spf_su_raw", force=True)
        except Exception:
            pass

        return spf_su_herds_final

    except Exception as e:
        logging.error(f"Failed to create SPF-SU herds table: {e}", exc_info=True)
        try:
            con.drop_table("spf_su_raw", force=True)
        except Exception:
            pass
        return None


def create_spf_su_health_controls_table(
    con: ibis.BaseBackend, spf_su_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
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
    """
    if spf_su_raw is None:
        logging.warning("Cannot create SPF-SU health controls table: spf_su_raw is None")
        return None

    logging.info("Creating SPF-SU health controls table...")

    try:
        # Register table for SQL operations
        con.create_table("spf_su_raw", spf_su_raw, overwrite=True)

        # Extract health control information by unnesting the array
        health_controls = con.sql("""
            SELECT
                ownerDetailInfo.chrNumber AS chr_number_raw,
                ownerDetailInfo.herdNumber AS herd_number_raw,
                UNNEST(healthStatus.healthControlInfo) AS health_control_raw,
                _export_timestamp
            FROM spf_su_raw
            WHERE ownerDetailInfo IS NOT NULL 
              AND healthStatus.healthControlInfo IS NOT NULL
              AND len(healthStatus.healthControlInfo) > 0
        """)

        # Extract disease information from the unnested health control data
        health_controls_expanded = con.sql("""
            SELECT
                chr_number_raw,
                herd_number_raw,
                health_control_raw.disease AS disease_code_raw,
                _export_timestamp
            FROM health_controls
            WHERE health_control_raw.disease IS NOT NULL
        """)

        # Clean and cast columns
        health_controls_clean = health_controls_expanded.mutate(
            # Generate unique ID
            health_control_id=ibis.uuid(),
            # Basic identifiers
            chr_number=ibis.coalesce(
                health_controls_expanded.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            herd_number=ibis.coalesce(
                health_controls_expanded.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # Disease information
            disease_code=health_controls_expanded.disease_code_raw.cast(dt.string).strip().nullif(""),
            # Add disease name mapping based on SPF-SU documentation
            disease_name=ibis.case()
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "Myc", "Mycoplasma lung disease")
            .when(
                health_controls_expanded.disease_code_raw.cast(dt.string).strip().startswith("Ap"),
                "Malignant lung disease (Actinobacillus)",
            )
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "PRRS1", "PRRS virus (European)")
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "PRRS2", "PRRS virus (American)")
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "Dys", "Swine dysentery")
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "Nys", "Atrophic rhinitis")
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "Skab", "Mange")
            .when(health_controls_expanded.disease_code_raw.cast(dt.string).strip() == "Lus", "Lice")
            .else_(health_controls_expanded.disease_code_raw.cast(dt.string).strip())
            .end(),
            # Metadata
            export_timestamp=health_controls_expanded._export_timestamp.cast(dt.string),
        )

        # Select final columns
        final_cols = [
            "health_control_id",
            "chr_number",
            "herd_number",
            "disease_code",
            "disease_name",
            "export_timestamp",
        ]

        health_controls_final = health_controls_clean.select(*final_cols)

        # Filter out rows with null identifiers
        health_controls_final = health_controls_final.filter(
            health_controls_final.chr_number.notnull()
            & health_controls_final.herd_number.notnull()
            & health_controls_final.disease_code.notnull()
        )

        # Save to parquet
        output_path = silver_dir / "spf_su_health_controls.parquet"
        rows = health_controls_final.count().execute()

        if rows == 0:
            logging.warning("SPF-SU health controls table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU health controls table with {rows} rows")
        saved_path = export.save_table(output_path, health_controls_final, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU health controls table - no path returned")
            return None

        logging.info(f"Saved SPF-SU health controls table to {saved_path}")

        # Clean up
        try:
            con.drop_table("spf_su_raw", force=True)
            con.drop_table("health_controls", force=True)
        except Exception:
            pass

        return health_controls_final

    except Exception as e:
        logging.error(f"Failed to create SPF-SU health controls table: {e}", exc_info=True)
        try:
            con.drop_table("spf_su_raw", force=True)
            con.drop_table("health_controls", force=True)
        except Exception:
            pass
        return None


def create_spf_su_salmonella_data_table(
    con: ibis.BaseBackend, spf_su_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """
    Create SPF-SU salmonella data table with detailed salmonella information.

    This table contains salmonella levels, indexes, and test results.
    """
    if spf_su_raw is None:
        logging.warning("Cannot create SPF-SU salmonella data table: spf_su_raw is None")
        return None

    logging.info("Creating SPF-SU salmonella data table...")

    try:
        # Register table for SQL operations
        con.create_table("spf_su_raw", spf_su_raw, overwrite=True)

        # Extract salmonella data - this is more complex due to nested arrays
        salmonella_data = con.sql("""
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
            FROM spf_su_raw
            WHERE ownerDetailInfo IS NOT NULL 
              AND ownerDetailInfo.salmonellaData IS NOT NULL
        """)

        # Clean and cast columns
        salmonella_data_clean = salmonella_data.mutate(
            # Generate unique ID
            salmonella_id=ibis.uuid(),
            # Basic identifiers
            chr_number=ibis.coalesce(
                salmonella_data.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            herd_number=ibis.coalesce(
                salmonella_data.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # Salmonella information
            salmonella_date=salmonella_data.salmonella_date_raw.cast(dt.timestamp),
            salmonella_status=salmonella_data.salmonella_status_raw.cast(dt.string).strip().nullif(""),
            has_index_details=salmonella_data.has_index_details_raw.cast(dt.boolean),
            show_data=salmonella_data.show_data_raw.cast(dt.boolean),
            # Convert arrays to JSON strings for storage (DuckDB/Ibis handling of complex nested data)
            salmonella_level_json=salmonella_data.salmonella_level_array_raw.cast(dt.string),
            salmonella_indexes_json=salmonella_data.salmonella_indexes_array_raw.cast(dt.string),
            salmonella_test_results_json=salmonella_data.salmonella_test_results_array_raw.cast(dt.string),
            # Metadata
            export_timestamp=salmonella_data._export_timestamp.cast(dt.string),
        )

        # Select final columns
        final_cols = [
            "salmonella_id",
            "chr_number",
            "herd_number",
            "salmonella_date",
            "salmonella_status",
            "has_index_details",
            "show_data",
            "salmonella_level_json",
            "salmonella_indexes_json",
            "salmonella_test_results_json",
            "export_timestamp",
        ]

        salmonella_data_final = salmonella_data_clean.select(*final_cols)

        # Filter out rows with null identifiers
        salmonella_data_final = salmonella_data_final.filter(
            salmonella_data_final.chr_number.notnull() & salmonella_data_final.herd_number.notnull()
        )

        # Save to parquet
        output_path = silver_dir / "spf_su_salmonella_data.parquet"
        rows = salmonella_data_final.count().execute()

        if rows == 0:
            logging.warning("SPF-SU salmonella data table is empty after processing")
            return None

        logging.info(f"Saving SPF-SU salmonella data table with {rows} rows")
        saved_path = export.save_table(output_path, salmonella_data_final, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save SPF-SU salmonella data table - no path returned")
            return None

        logging.info(f"Saved SPF-SU salmonella data table to {saved_path}")

        # Clean up
        try:
            con.drop_table("spf_su_raw", force=True)
        except Exception:
            pass

        return salmonella_data_final

    except Exception as e:
        logging.error(f"Failed to create SPF-SU salmonella data table: {e}", exc_info=True)
        try:
            con.drop_table("spf_su_raw", force=True)
        except Exception:
            pass
        return None
