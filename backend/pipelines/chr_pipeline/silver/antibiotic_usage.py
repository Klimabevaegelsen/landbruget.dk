"""
Antibiotic Usage Silver Processing Module for CHR Pipeline

This module processes VetStat antibiotic usage data from bronze to silver layer.
"""

import contextlib
import logging
from pathlib import Path

import duckdb

from . import export


def create_antibiotic_usage_table(
    con: duckdb.DuckDBPyConnection,
    vetstat_raw_table: str | None,
    lookup_tables: dict[str, str | None],
    silver_dir: Path,
) -> duckdb.DuckDBPyRelation | None:
    """Creates the antibiotic_usage table from parsed VetStat data.

    Args:
        con: DuckDB connection
        vetstat_raw_table: Name of the raw VetStat data table in DuckDB (or None if not available)
        lookup_tables: Dictionary mapping lookup table names to their DuckDB table names
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed antibiotic usage data or None if failed
    """
    logging.info("Starting creation of antibiotic_usage table.")

    if vetstat_raw_table is None:
        logging.warning(
            "Cannot create antibiotic_usage: vetstat_raw table missing. Creating empty table with schema."
        )
        # Create empty table with schema
        con.execute("""
            CREATE OR REPLACE TABLE empty_antibiotic_usage AS
            SELECT
                CAST(NULL AS VARCHAR) as usage_id,
                CAST(NULL AS VARCHAR) as cvr_number,
                CAST(NULL AS BIGINT) as chr_number,
                CAST(NULL AS INTEGER) as year,
                CAST(NULL AS INTEGER) as month,
                CAST(NULL AS INTEGER) as species_code,
                CAST(NULL AS INTEGER) as age_group_code,
                CAST(NULL AS DOUBLE) as avg_usage_rolling_9m,
                CAST(NULL AS DOUBLE) as avg_usage_rolling_12m,
                CAST(NULL AS DOUBLE) as animal_days,
                CAST(NULL AS DOUBLE) as animal_doses,
                CAST(NULL AS DOUBLE) as add_per_100_dyr_per_dag,
                CAST(NULL AS DOUBLE) as limit_value,
                CAST(NULL AS INTEGER) as municipality_code,
                CAST(NULL AS VARCHAR) as municipality_name,
                CAST(NULL AS INTEGER) as region_code,
                CAST(NULL AS VARCHAR) as region_name
            WHERE 1=0
        """)
        output_path = silver_dir / "antibiotic_usage.parquet"
        con.execute(f"COPY empty_antibiotic_usage TO '{output_path}' (FORMAT PARQUET)")
        if not output_path.exists():
            logging.error("Failed to save empty antibiotic_usage table - file not created")
            return None
        logging.info(f"Saved empty antibiotic_usage table with schema to {output_path}")
        return None

    # Check if vetstat_raw has columns by trying to describe it
    try:
        columns_result = con.execute(f"DESCRIBE {vetstat_raw_table}").fetchall()
        if not columns_result:
            logging.warning(
                "Cannot create antibiotic_usage: vetstat_raw table exists but has no columns "
                "(likely empty source). Creating empty table with schema."
            )
            # Create empty table with schema
            con.execute("""
                CREATE OR REPLACE TABLE empty_antibiotic_usage_2 AS
                SELECT
                    CAST(NULL AS VARCHAR) as usage_id,
                    CAST(NULL AS VARCHAR) as cvr_number,
                    CAST(NULL AS BIGINT) as chr_number,
                    CAST(NULL AS INTEGER) as year,
                    CAST(NULL AS INTEGER) as month,
                    CAST(NULL AS INTEGER) as species_code,
                    CAST(NULL AS INTEGER) as age_group_code,
                    CAST(NULL AS DOUBLE) as avg_usage_rolling_9m,
                    CAST(NULL AS DOUBLE) as avg_usage_rolling_12m,
                    CAST(NULL AS DOUBLE) as animal_days,
                    CAST(NULL AS DOUBLE) as animal_doses,
                    CAST(NULL AS DOUBLE) as add_per_100_dyr_per_dag,
                    CAST(NULL AS DOUBLE) as limit_value,
                    CAST(NULL AS INTEGER) as municipality_code,
                    CAST(NULL AS VARCHAR) as municipality_name,
                    CAST(NULL AS INTEGER) as region_code,
                    CAST(NULL AS VARCHAR) as region_name
                WHERE 1=0
            """)
            output_path = silver_dir / "antibiotic_usage.parquet"
            con.execute(f"COPY empty_antibiotic_usage_2 TO '{output_path}' (FORMAT PARQUET)")
            if not output_path.exists():
                logging.error("Failed to save empty antibiotic_usage table - file not created")
                return None
            logging.info(f"Saved empty antibiotic_usage table with schema to {output_path}")
            return None
        available_columns = [col[0] for col in columns_result]
    except Exception as e:
        logging.error(f"Failed to describe vetstat_raw table: {e}")
        return None

    # Define source -> target mapping (adjust based on actual parsed XML->JSONL structure)
    usage_cols = {
        "CVRNummer": "cvr_number_raw",
        "CHRNummer": "chr_number_raw",
        "Aar": "year_raw",
        "Maaned": "month_raw",
        "DyreArtKode": "species_code_raw",
        "Aldersgruppekode": "age_group_code_raw",
        "Rul9MdrGns": "avg_usage_rolling_9m_raw",
        "Rul12MdrGns": "avg_usage_rolling_12m_raw",
        "Dyredage": "animal_days_raw",
        "Dyredoser": "animal_doses_raw",
        "ADDPer100DyrPerDag": "add_per_100_dyr_per_dag_raw",
        "Graensevaerdi": "limit_value_raw",
        "Kommunenr": "municipality_code_raw",
        "Kommunenavn": "municipality_name_raw",
        "Regionsnr": "region_code_raw",
        "Regionsnavn": "region_name_raw",
    }

    try:
        # Build the SELECT clause for available columns
        select_parts = []
        for source, target in usage_cols.items():
            if source in available_columns:
                select_parts.append(f'"{source}" AS {target}')
            else:
                select_parts.append(f"NULL AS {target}")
                logging.warning(
                    f"Column '{source}' missing in source vetstat data, adding as null."
                )

        select_clause = ", ".join(select_parts)

        # Create base table with renamed columns
        con.execute(f"""
            CREATE OR REPLACE TABLE usage_base AS
            SELECT {select_clause}
            FROM {vetstat_raw_table}
        """)

        # Create the cleaned and processed antibiotic_usage table with SQL
        con.execute("""
            CREATE OR REPLACE TABLE antibiotic_usage AS
            SELECT
                uuid() AS usage_id,
                -- CVR number: string field
                NULLIF(TRIM(CAST(cvr_number_raw AS VARCHAR)), '') AS cvr_number,
                -- CHR number: integer field
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT),
                    NULL
                ) AS chr_number,
                -- Year and month
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(year_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS year,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(month_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS month,
                -- Species and age group codes (FKs)
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(species_code_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS species_code,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(age_group_code_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS age_group_code,
                -- Usage metrics
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(avg_usage_rolling_9m_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS avg_usage_rolling_9m,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(avg_usage_rolling_12m_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS avg_usage_rolling_12m,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(animal_days_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS animal_days,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(animal_doses_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS animal_doses,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(add_per_100_dyr_per_dag_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS add_per_100_dyr_per_dag,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(limit_value_raw AS VARCHAR)), '') AS DOUBLE),
                    NULL
                ) AS limit_value,
                -- Location information
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(municipality_code_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS municipality_code,
                NULLIF(TRIM(CAST(municipality_name_raw AS VARCHAR)), '') AS municipality_name,
                COALESCE(
                    TRY_CAST(NULLIF(TRIM(CAST(region_code_raw AS VARCHAR)), '') AS INTEGER),
                    NULL
                ) AS region_code,
                NULLIF(TRIM(CAST(region_name_raw AS VARCHAR)), '') AS region_name
            FROM usage_base
        """)

        # Optional: Join with lookup tables if available
        species_table = lookup_tables.get("species")
        age_groups_table = lookup_tables.get("age_groups")

        if species_table is not None:
            try:
                con.execute(f"""
                    CREATE OR REPLACE TABLE antibiotic_usage AS
                    SELECT au.*, sp.* EXCLUDE (species_code)
                    FROM antibiotic_usage au
                    LEFT JOIN {species_table} sp ON au.species_code = sp.species_code
                """)
                logging.info("Joined antibiotic_usage with species lookup table")
            except Exception as e:
                logging.warning(f"Failed to join with species lookup: {e}")

        if age_groups_table is not None:
            try:
                # Check if the age_groups table uses 'age_groups_code' or 'age_group_code'
                age_groups_cols = [
                    col[0] for col in con.execute(f"DESCRIBE {age_groups_table}").fetchall()
                ]
                join_col = (
                    "age_groups_code" if "age_groups_code" in age_groups_cols else "age_group_code"
                )
                con.execute(f"""
                    CREATE OR REPLACE TABLE antibiotic_usage AS
                    SELECT au.*, ag.* EXCLUDE ({join_col})
                    FROM antibiotic_usage au
                    LEFT JOIN {age_groups_table} ag ON au.age_group_code = ag.{join_col}
                """)
                logging.info("Joined antibiotic_usage with age_groups lookup table")
            except Exception as e:
                logging.warning(f"Failed to join with age_groups lookup: {e}")

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM antibiotic_usage").fetchone()[0]

        if rows == 0:
            logging.warning("Antibiotic usage table is empty after processing.")
            return None

        logging.info(f"Saving antibiotic_usage table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "antibiotic_usage.parquet"
        saved_path = export.save_table(output_path, "antibiotic_usage", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save antibiotic_usage table - no path returned")
            return None

        logging.info(f"Saved antibiotic_usage table to {saved_path}")

        # Clean up temporary table
        with contextlib.suppress(Exception):
            con.execute("DROP TABLE IF EXISTS usage_base")

        # Return the relation
        return con.sql("SELECT * FROM antibiotic_usage")

    except Exception as e:
        logging.error(f"Failed to create antibiotic_usage table: {e}", exc_info=True)
        # Clean up temporary table on error
        with contextlib.suppress(Exception):
            con.execute("DROP TABLE IF EXISTS usage_base")
        return None
