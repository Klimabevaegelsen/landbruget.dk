import logging
from pathlib import Path

import duckdb

# Import export module
from . import export


def create_property_vet_events_table(
    con: duckdb.DuckDBPyConnection,
    ejendom_vet_raw: str | None,
    lookup_tables: dict[str, str | None],
    silver_dir: Path,
) -> duckdb.DuckDBPyRelation | None:
    """Creates the property_vet_events table from the nested structure in ejendom_vet_events.

    Args:
        con: The DuckDB connection
        ejendom_vet_raw: Name of the raw ejendom vet table in DuckDB (or None if not available)
        lookup_tables: Dictionary of lookup table names (species, diseases, vet_statuses)
        silver_dir: Directory to save the output
    """
    logging.info("Starting creation of property_vet_events table.")

    if ejendom_vet_raw is None:
        logging.warning("Cannot create property_vet_events: ejendom_vet_raw is None.")
        return None

    try:
        # Check if Response column exists
        columns = con.execute(f"DESCRIBE {ejendom_vet_raw}").fetchall()
        column_names = [col[0] for col in columns]
        if "Response" not in column_names:
            logging.warning(
                "Cannot create property_vet_events: 'Response' column missing in ejendom_vet_raw."
            )
            return None

        # Build the main SQL query to extract and clean the vet events
        # First, unnest the VeterinaerHaendelse array from the nested structure
        con.execute(f"""
            CREATE OR REPLACE TABLE property_vet_events AS
            WITH base AS (
                SELECT
                    CAST(Response.ChrNummer AS VARCHAR) AS chr_number_raw,
                    Response.VeterinaereHaendelser.VeterinaereProblemer AS has_vet_problems_raw,
                    UNNEST(Response.VeterinaereHaendelser.VeterinaerHaendelse) AS event_info
                FROM {ejendom_vet_raw}
                WHERE Response.VeterinaereHaendelser.VeterinaerHaendelse IS NOT NULL
            ),
            extracted_events AS (
                SELECT
                    chr_number_raw,
                    has_vet_problems_raw,
                    event_info.DyreArtKode AS species_code_raw,
                    event_info.DyreArtTekst AS species_name_raw,
                    event_info.SygdomsKode AS disease_code_raw,
                    event_info.SygdomsTekst AS disease_name_raw,
                    event_info.VeterinaerStatusKode AS vet_status_code_raw,
                    event_info.VeterinaerStatusTekst AS vet_status_name_raw,
                    event_info.SygdomsNiveauKode AS disease_level_code_raw,
                    event_info.SygdomsNiveauTekst AS disease_level_name_raw,
                    event_info.DatoVeterinaerStatus AS vet_status_date_raw,
                    event_info.VeterinaerHaendelseBemaerkning AS remark_raw
                FROM base
                WHERE event_info IS NOT NULL
            )
            SELECT
                uuid() AS event_id,
                -- CHR number as BIGINT
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT),
                    NULL
                ) AS chr_number,
                -- Species code as INTEGER
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(species_code_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS species_code,
                NULLIF(TRIM(CAST(species_name_raw AS VARCHAR)), '') AS species_name,
                -- Disease code as STRING (can contain non-numeric values)
                NULLIF(TRIM(CAST(disease_code_raw AS VARCHAR)), '') AS disease_code,
                NULLIF(TRIM(CAST(disease_name_raw AS VARCHAR)), '') AS disease_name,
                -- Disease level code as STRING
                NULLIF(TRIM(CAST(disease_level_code_raw AS VARCHAR)), '') AS disease_level_code,
                NULLIF(TRIM(CAST(disease_level_name_raw AS VARCHAR)), '') AS disease_level_name,
                -- Vet status code as STRING
                NULLIF(TRIM(CAST(vet_status_code_raw AS VARCHAR)), '') AS vet_status_code,
                NULLIF(TRIM(CAST(vet_status_name_raw AS VARCHAR)), '') AS vet_status_name,
                -- Date field with safe casting
                TRY_CAST(
                    NULLIF(TRIM(CAST(vet_status_date_raw AS VARCHAR)), '') AS DATE
                ) AS vet_status_date,
                -- Boolean field: check if lower-cased value is 'ja'
                LOWER(NULLIF(TRIM(CAST(has_vet_problems_raw AS VARCHAR)), '')) = 'ja'
                    AS has_vet_problems,
                NULLIF(TRIM(CAST(remark_raw AS VARCHAR)), '') AS remark
            FROM extracted_events
        """)

        # Optionally join with lookup tables if they exist
        # Note: In the original ibis code, lookups were left joined but column selection
        # at the end only kept the core columns, so the joins didn't add new columns to output.
        # We maintain the same behavior by not adding lookup columns.

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM property_vet_events").fetchone()[0]

        if rows == 0:
            logging.warning("Property vet events table is empty after processing.")
            return None

        logging.info(f"Saving property_vet_events table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "property_vet_events.parquet"
        saved_path = export.save_table(output_path, "property_vet_events", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save property_vet_events table - no path returned")
            return None

        logging.info(f"Saved property_vet_events table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM property_vet_events")

    except Exception as e:
        logging.error(f"Failed to create property_vet_events table: {e}", exc_info=True)
        return None
