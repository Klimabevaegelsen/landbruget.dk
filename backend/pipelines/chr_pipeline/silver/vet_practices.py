import logging
from pathlib import Path

import duckdb

# Import export module
from . import export


def create_vet_practices_table(
    con: duckdb.DuckDBPyConnection, bes_details_raw: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the vet_practices table from besaetning details.

    Args:
        con: The DuckDB connection
        bes_details_raw: Name of the raw besaetning details table in DuckDB (or None if not available)
        silver_dir: Directory to save the output

    Returns:
        DuckDB relation containing the vet practices data, or None if no data available
    """
    if bes_details_raw is None:
        logging.warning("Skipping vet_practices table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of vet_practices table.")

    try:
        # Create the vet_practices table with SQL - cleaning and casting in one step
        con.execute(f"""
            CREATE OR REPLACE TABLE vet_practices AS
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM {bes_details_raw}
                WHERE Response IS NOT NULL
            ),
            raw_vet_practices AS (
                SELECT DISTINCT
                    r.Besaetning.BesPraksis.PraksisNr AS practice_number_raw,
                    r.Besaetning.BesPraksis.PraksisNavn AS practice_name_raw,
                    r.Besaetning.BesPraksis.PraksisAdresse AS address_raw,
                    r.Besaetning.BesPraksis.PraksisByNavn AS city_raw,
                    r.Besaetning.BesPraksis.PraksisPostNummer AS postal_code_raw,
                    r.Besaetning.BesPraksis.PraksisPostDistrikt AS postal_district_raw,
                    r.Besaetning.BesPraksis.PraksisTelefonNummer AS phone_raw,
                    r.Besaetning.BesPraksis.PraksisMobilNummer AS mobile_raw,
                    r.Besaetning.BesPraksis.PraksisEmail AS email_raw
                FROM unnested_response
                WHERE r.Besaetning.BesPraksis.PraksisNr IS NOT NULL
            )
            SELECT
                -- Integer field with safe casting
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(practice_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS practice_number,
                -- String fields with cleaning
                NULLIF(TRIM(CAST(practice_name_raw AS VARCHAR)), '') AS practice_name,
                NULLIF(TRIM(CAST(address_raw AS VARCHAR)), '') AS address,
                NULLIF(TRIM(CAST(city_raw AS VARCHAR)), '') AS city,
                NULLIF(TRIM(CAST(postal_code_raw AS VARCHAR)), '') AS postal_code,
                NULLIF(TRIM(CAST(postal_district_raw AS VARCHAR)), '') AS postal_district,
                NULLIF(TRIM(CAST(phone_raw AS VARCHAR)), '') AS phone,
                NULLIF(TRIM(CAST(mobile_raw AS VARCHAR)), '') AS mobile,
                NULLIF(TRIM(CAST(email_raw AS VARCHAR)), '') AS email
            FROM raw_vet_practices
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(practice_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM vet_practices").fetchone()[0]

        if rows == 0:
            logging.warning("Vet practices table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving vet_practices table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "vet_practices.parquet"
        saved_path = export.save_table(output_path, "vet_practices", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save vet_practices table - no path returned")
            return None

        logging.info(f"Saved vet_practices table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM vet_practices")

    except Exception as e:
        logging.error(f"Failed to create vet_practices table: {e}", exc_info=True)
        return None
