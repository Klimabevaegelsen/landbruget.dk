"""
Animal Movements Silver Processing Module for CHR Pipeline

This module processes animal movement data from bronze to silver layer using vanilla DuckDB.
It handles:
- CHR_dyr movement summaries (aggregated movement data)
- CHR_dyr animal movements (individual animal records)
- DIKO flytninger (animal movements from DIKO service)
"""

import logging
from pathlib import Path

import duckdb

# Import export module
from . import export

logger = logging.getLogger(__name__)


def create_chr_dyr_movement_summaries_table(
    con: duckdb.DuckDBPyConnection, chr_dyr_summaries_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the chr_dyr_movement_summaries table from aggregated CHR_dyr movement data.

    Args:
        con: DuckDB connection
        chr_dyr_summaries_table: Name of the raw CHR_dyr summaries table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed movement summaries data or None if failed
    """
    logging.info("Starting creation of chr_dyr_movement_summaries table from aggregated data.")

    if chr_dyr_summaries_table is None:
        logging.warning("Cannot create chr_dyr_movement_summaries: input table is None.")
        return None

    try:
        # Check for required columns
        columns_result = con.execute(f"DESCRIBE {chr_dyr_summaries_table}").fetchall()
        column_names = [col[0] for col in columns_result]

        required_columns = [
            "reporting_herd_number",
            "movement_date",
            "counterparty_herd",
            "movement_type",
            "animal_count",
        ]

        missing_columns = [col for col in required_columns if col not in column_names]
        if missing_columns:
            logging.warning(
                f"Cannot create chr_dyr_movement_summaries: Missing required columns: {missing_columns}"
            )
            return None

        # Build SQL for optional columns
        has_cattle_type = "cattle_type_breakdown" in column_names
        has_nation_from = "nation_codes_from" in column_names
        has_nation_to = "nation_codes_to" in column_names
        has_international = "is_international" in column_names

        # Create the cleaned and processed movement summaries table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE chr_dyr_movement_summaries AS
            WITH raw_data AS (
                SELECT
                    reporting_herd_number,
                    movement_date,
                    counterparty_herd,
                    movement_type,
                    animal_count,
                    movement_reasons,
                    {"cattle_type_breakdown," if has_cattle_type else ""}
                    {"nation_codes_from," if has_nation_from else ""}
                    {"nation_codes_to," if has_nation_to else ""}
                    {"is_international" if has_international else "FALSE AS is_international"}
                FROM {chr_dyr_summaries_table}
            )
            SELECT
                uuid() AS movement_summary_id,
                -- Basic identifiers: cast to string, trim, nullif empty, then cast to int64
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(reporting_herd_number AS VARCHAR)), '') AS BIGINT), NULL) AS reporting_herd_number,
                TRY_CAST(movement_date AS DATE) AS movement_date,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(counterparty_herd AS VARCHAR)), '') AS BIGINT), NULL) AS counterparty_herd,
                NULLIF(TRIM(CAST(movement_type AS VARCHAR)), '') AS movement_type,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_count AS VARCHAR)), '') AS BIGINT), 0) AS animal_count,
                -- JSON fields
                COALESCE(NULLIF(TRIM(CAST(movement_reasons AS VARCHAR)), ''), '[]') AS movement_reasons,
                {"COALESCE(NULLIF(TRIM(CAST(cattle_type_breakdown AS VARCHAR)), ''), '{}')" if has_cattle_type else "'{}'"} AS cattle_type_breakdown,
                {"COALESCE(NULLIF(TRIM(CAST(nation_codes_from AS VARCHAR)), ''), '[]')" if has_nation_from else "'[]'"} AS nation_codes_from,
                {"COALESCE(NULLIF(TRIM(CAST(nation_codes_to AS VARCHAR)), ''), '[]')" if has_nation_to else "'[]'"} AS nation_codes_to,
                COALESCE(TRY_CAST(is_international AS BOOLEAN), FALSE) AS is_international
            FROM raw_data
            WHERE
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_count AS VARCHAR)), '') AS BIGINT), 0) > 0
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM chr_dyr_movement_summaries").fetchone()[0]

        if rows == 0:
            logging.warning("CHR_dyr movement summaries table is empty after processing.")
            return None

        logging.info(f"Saving chr_dyr_movement_summaries table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "chr_dyr_movement_summaries.parquet"
        saved_path = export.save_table(output_path, "chr_dyr_movement_summaries", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save chr_dyr_movement_summaries table - no path returned")
            return None

        logging.info(f"Successfully saved CHR_dyr movement summaries to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM chr_dyr_movement_summaries")

    except Exception as e:
        logging.error(f"Error creating chr_dyr_movement_summaries table: {e}", exc_info=True)
        return None


def create_chr_dyr_animal_movements_table(
    con: duckdb.DuckDBPyConnection, chr_dyr_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the chr_dyr_animal_movements table from CHR_dyr service besListAktOms responses.

    NOTE: This function handles the old individual animal records format.
    For aggregated movement summaries, use create_chr_dyr_movement_summaries_table instead.

    Args:
        con: DuckDB connection
        chr_dyr_raw_table: Name of the raw CHR_dyr table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed animal movements data or None if failed
    """
    logging.info("Starting creation of chr_dyr_animal_movements table (individual records format).")

    if chr_dyr_raw_table is None:
        logging.warning("Cannot create chr_dyr_animal_movements: chr_dyr_raw_table is None.")
        return None

    try:
        # Check if table has Response column with nested structure
        columns_result = con.execute(f"DESCRIBE {chr_dyr_raw_table}").fetchall()
        column_names = [col[0] for col in columns_result]

        if "Response" not in column_names:
            logging.warning(
                "Cannot create chr_dyr_animal_movements: 'Response' column missing in chr_dyr_raw."
            )
            return None

        # Try to detect nested structure
        try:
            # Check if Response is an array with nested data
            test_result = con.execute(f"""
                SELECT typeof(Response) AS response_type
                FROM {chr_dyr_raw_table}
                LIMIT 1
            """).fetchone()

            if test_result is None:
                logging.warning("Cannot create chr_dyr_animal_movements: Table is empty.")
                return None

            response_type = test_result[0] if test_result else ""
            if not response_type.startswith("STRUCT") and "[]" not in response_type:
                logging.warning(
                    f"Cannot create chr_dyr_animal_movements: 'Response' column is not expected type "
                    f"(Type: {response_type}). Skipping."
                )
                return None

        except Exception as e:
            logging.warning(f"Cannot detect Response type: {e}")
            return None

        # Create the animal movements table with SQL
        # Using DuckDB's nested data access syntax
        con.execute(f"""
            CREATE OR REPLACE TABLE chr_dyr_animal_movements AS
            WITH unnested_response AS (
                SELECT
                    UNNEST(Response) AS r
                FROM {chr_dyr_raw_table}
                WHERE Response IS NOT NULL
            ),
            base_data AS (
                SELECT
                    r.BesaetningsNummer AS reporting_herd_number_raw,
                    r.PeriodeFra AS period_fra_raw,
                    r.PeriodeTil AS period_til_raw,
                    r.Enkeltdyrsoplysninger AS animals_list
                FROM unnested_response
                WHERE r.Enkeltdyrsoplysninger IS NOT NULL
            ),
            unnested_animals AS (
                SELECT
                    reporting_herd_number_raw,
                    period_fra_raw,
                    period_til_raw,
                    UNNEST(animals_list) AS animal_info
                FROM base_data
                WHERE animals_list IS NOT NULL
            )
            SELECT
                uuid() AS animal_movement_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(reporting_herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS reporting_herd_number,
                TRY_CAST(NULLIF(TRIM(CAST(period_fra_raw AS VARCHAR)), '') AS DATE) AS period_fra,
                TRY_CAST(NULLIF(TRIM(CAST(period_til_raw AS VARCHAR)), '') AS DATE) AS period_til,
                -- Animal information
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_info.CkrNr AS VARCHAR)), '') AS BIGINT), NULL) AS ckr_number,
                TRY_CAST(NULLIF(TRIM(CAST(animal_info.DatoFoedt AS VARCHAR)), '') AS DATE) AS birth_date,
                TRY_CAST(NULLIF(TRIM(CAST(animal_info.DatoIndgaaet AS VARCHAR)), '') AS DATE) AS entry_date,
                TRY_CAST(NULLIF(TRIM(CAST(animal_info.DatoAfgaaet AS VARCHAR)), '') AS DATE) AS exit_date,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_info.KildeBesaetning AS VARCHAR)), '') AS BIGINT), NULL) AS source_herd,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_info.DestinationBesaetning AS VARCHAR)), '') AS BIGINT), NULL) AS destination_herd,
                NULLIF(TRIM(CAST(animal_info.Koen AS VARCHAR)), '') AS gender,
                NULLIF(TRIM(CAST(animal_info.Race AS VARCHAR)), '') AS breed,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(animal_info.MorCkrNr AS VARCHAR)), '') AS BIGINT), NULL) AS mother_ckr_number
            FROM unnested_animals
            WHERE animal_info IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM chr_dyr_animal_movements").fetchone()[0]

        if rows == 0:
            logging.warning("CHR_dyr animal movements table is empty after processing.")
            return None

        logging.info(f"Saving chr_dyr_animal_movements table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "chr_dyr_animal_movements.parquet"
        saved_path = export.save_table(output_path, "chr_dyr_animal_movements", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save chr_dyr_animal_movements table - no path returned")
            return None

        logging.info(f"Saved chr_dyr_animal_movements table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM chr_dyr_animal_movements")

    except Exception as e:
        logging.error(f"Failed to create chr_dyr_animal_movements table: {e}", exc_info=True)
        return None


def create_animal_movements_table(
    con: duckdb.DuckDBPyConnection, diko_flyt_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the animal_movements table from the nested Flytninger list in diko_flytninger.

    Args:
        con: DuckDB connection
        diko_flyt_raw_table: Name of the raw DIKO flytninger table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed animal movements data or None if failed
    """
    logging.info("Starting creation of animal_movements table.")

    if diko_flyt_raw_table is None:
        logging.warning("Cannot create animal_movements: diko_flyt_raw_table is None.")
        return None

    try:
        # Check if table has Response column with nested structure
        columns_result = con.execute(f"DESCRIBE {diko_flyt_raw_table}").fetchall()
        column_names = [col[0] for col in columns_result]

        if "Response" not in column_names:
            logging.warning(
                "Cannot create animal_movements: 'Response' column missing in diko_flyt_raw."
            )
            return None

        # Try to detect nested structure
        try:
            # Check if Response is an array with nested data
            test_result = con.execute(f"""
                SELECT typeof(Response) AS response_type
                FROM {diko_flyt_raw_table}
                LIMIT 1
            """).fetchone()

            if test_result is None:
                logging.warning("Cannot create animal_movements: Table is empty.")
                return None

            response_type = test_result[0] if test_result else ""
            if not response_type.startswith("STRUCT") and "[]" not in response_type:
                logging.warning(
                    f"Cannot create animal_movements: 'Response' column is not expected type "
                    f"(Type: {response_type}). Skipping."
                )
                return None

        except Exception as e:
            logging.warning(f"Cannot detect Response type: {e}")
            return None

        # Create the animal movements table with SQL
        # Using DuckDB's nested data access syntax
        con.execute(f"""
            CREATE OR REPLACE TABLE animal_movements AS
            WITH unnested_response AS (
                SELECT
                    UNNEST(Response) AS r
                FROM {diko_flyt_raw_table}
                WHERE Response IS NOT NULL
            ),
            base_data AS (
                SELECT
                    r.BesaetningsNummer AS reporting_herd_number_raw,
                    r.Flytninger AS flytninger_list
                FROM unnested_response
                WHERE r.Flytninger IS NOT NULL
            ),
            unnested_movements AS (
                SELECT
                    reporting_herd_number_raw,
                    UNNEST(flytninger_list) AS movement_info
                FROM base_data
                WHERE flytninger_list IS NOT NULL
            )
            SELECT
                uuid() AS movement_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(reporting_herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS reporting_herd_number,
                -- Movement information
                TRY_CAST(NULLIF(TRIM(CAST(movement_info.FlytteDato AS VARCHAR)), '') AS DATE) AS movement_date,
                NULLIF(TRIM(CAST(movement_info.KontaktType AS VARCHAR)), '') AS contact_type,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(movement_info.ChrNummer AS VARCHAR)), '') AS BIGINT), NULL) AS counterparty_chr_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(movement_info.BesaetningsNummer AS VARCHAR)), '') AS BIGINT), NULL) AS counterparty_herd_number,
                NULLIF(TRIM(CAST(movement_info.VirksomhedsArt AS VARCHAR)), '') AS counterparty_business_type
            FROM unnested_movements
            WHERE movement_info IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM animal_movements").fetchone()[0]

        if rows == 0:
            logging.warning("Animal movements table is empty after processing.")
            return None

        logging.info(f"Saving animal_movements table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "animal_movements.parquet"
        saved_path = export.save_table(output_path, "animal_movements", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save animal_movements table - no path returned")
            return None

        logging.info(f"Saved animal_movements table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM animal_movements")

    except Exception as e:
        logging.error(f"Failed to create animal_movements table: {e}", exc_info=True)
        return None
