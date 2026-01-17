import logging
from pathlib import Path

import duckdb

# Import export module
from . import export


def create_herds_table(
    con: duckdb.DuckDBPyConnection, bes_details_raw: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the herds table (excluding owner/user identifiers) from besaetning details.

    Args:
        con: The DuckDB connection
        bes_details_raw: Name of the raw besaetning details table in DuckDB (or None if not available)
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herds table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herds table.")

    try:
        # Create the herds table with SQL - cleaning and casting in one step
        con.execute(f"""
            CREATE OR REPLACE TABLE herds AS
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM {bes_details_raw}
            ),
            raw_herds AS (
                SELECT DISTINCT
                    r.Besaetning.BesaetningsNummer AS herd_number_raw,
                    r.Besaetning.ChrNummer AS chr_number_raw,
                    r.Besaetning.DyreArtKode AS species_code_raw,
                    r.Besaetning.DyreArtTekst AS species_name_raw,
                    r.Besaetning.BrugsArtKode AS usage_type_code_raw,
                    r.Besaetning.BrugsArtTekst AS usage_type_name_raw,
                    r.Besaetning.VirksomhedsArtTekst AS business_type_name_raw,
                    r.Besaetning.OmsaetningsKode AS turnover_code_raw,
                    r.Besaetning.OmsaetningsTekst AS turnover_text_raw,
                    r.Besaetning.LeveringsErklaeringer AS delivery_declarations_raw,
                    r.Besaetning.DatoOphoer AS date_ceased_raw,
                    r.Besaetning.Oekologisk AS is_organic_raw,
                    r.Besaetning.DatoOpret AS date_created_raw,
                    r.Besaetning.DatoOpdatering AS date_updated_raw
                FROM unnested_response
                WHERE r.Besaetning.BesaetningsNummer IS NOT NULL
            )
            SELECT
                -- Integer fields with safe casting
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(species_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS species_code,
                NULLIF(TRIM(CAST(species_name_raw AS VARCHAR)), '') AS species_name,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(usage_type_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS usage_type_code,
                NULLIF(TRIM(CAST(usage_type_name_raw AS VARCHAR)), '') AS usage_type_name,
                NULLIF(TRIM(CAST(business_type_name_raw AS VARCHAR)), '') AS business_type_name,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(turnover_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS turnover_code,
                NULLIF(TRIM(CAST(turnover_text_raw AS VARCHAR)), '') AS turnover_text,
                NULLIF(TRIM(CAST(delivery_declarations_raw AS VARCHAR)), '') AS delivery_declarations,
                -- Boolean field: check if lower-cased value is 'ja'
                LOWER(NULLIF(TRIM(CAST(is_organic_raw AS VARCHAR)), '')) = 'ja' AS is_organic,
                -- Date fields with safe casting
                TRY_CAST(NULLIF(TRIM(CAST(date_created_raw AS VARCHAR)), '') AS DATE) AS date_created,
                TRY_CAST(NULLIF(TRIM(CAST(date_updated_raw AS VARCHAR)), '') AS DATE) AS date_updated,
                TRY_CAST(NULLIF(TRIM(CAST(date_ceased_raw AS VARCHAR)), '') AS DATE) AS date_ceased
            FROM raw_herds
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM herds").fetchone()[0]

        if rows == 0:
            logging.warning("Herds table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herds table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "herds.parquet"
        saved_path = export.save_table(output_path, "herds", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save herds table - no path returned")
            return None

        logging.info(f"Saved herds table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM herds")

    except Exception as e:
        logging.error(f"Failed to create herds table: {e}", exc_info=True)
        return None


def create_herd_owners_table(
    con: duckdb.DuckDBPyConnection, bes_details_raw: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the herd_owners table including attributes from besaetning details.

    Args:
        con: The DuckDB connection
        bes_details_raw: Name of the raw besaetning details table in DuckDB (or None if not available)
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_owners table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_owners table with attributes.")

    try:
        # Create the herd_owners table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE herd_owners AS
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM {bes_details_raw}
            ),
            raw_owners AS (
                SELECT DISTINCT
                    r.Besaetning.BesaetningsNummer AS herd_number_raw,
                    r.Besaetning.Ejer.CvrNummer AS owner_cvr_raw,
                    r.Besaetning.Ejer.CprNummer AS owner_cpr_raw,
                    r.Besaetning.Ejer.Navn AS owner_name_raw,
                    r.Besaetning.Ejer.Adresse AS owner_address_raw,
                    r.Besaetning.Ejer.PostNummer AS owner_postal_code_raw,
                    r.Besaetning.Ejer.PostDistrikt AS owner_postal_district_raw,
                    r.Besaetning.Ejer.ByNavn AS owner_city_raw,
                    r.Besaetning.Ejer.KommuneNummer AS owner_municipality_code_raw,
                    r.Besaetning.Ejer.KommuneNavn AS owner_municipality_name_raw,
                    r.Besaetning.Ejer.Land AS owner_country_raw,
                    r.Besaetning.Ejer.TelefonNummer AS owner_phone_raw,
                    r.Besaetning.Ejer.MobilNummer AS owner_mobile_raw,
                    r.Besaetning.Ejer.Email AS owner_email_raw,
                    r.Besaetning.Ejer.Adressebeskyttelse AS owner_address_protection_raw,
                    r.Besaetning.Ejer.Reklamebeskyttelse AS owner_advertising_protection_raw
                FROM unnested_response
                WHERE r.Besaetning.BesaetningsNummer IS NOT NULL
                  AND r.Besaetning.Ejer IS NOT NULL
                  AND (
                      (r.Besaetning.Ejer.CvrNummer IS NOT NULL OR r.Besaetning.Ejer.CprNummer IS NOT NULL)
                      OR
                      (r.Besaetning.Ejer.Navn IS NOT NULL
                       AND r.Besaetning.Ejer.Adresse IS NOT NULL
                       AND r.Besaetning.Ejer.PostNummer IS NOT NULL)
                  )
            )
            SELECT
                -- Integer field with safe casting
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                -- String fields
                NULLIF(TRIM(CAST(owner_cvr_raw AS VARCHAR)), '') AS owner_cvr,
                NULLIF(TRIM(CAST(owner_cpr_raw AS VARCHAR)), '') AS owner_cpr,
                NULLIF(TRIM(CAST(owner_name_raw AS VARCHAR)), '') AS owner_name,
                NULLIF(TRIM(CAST(owner_address_raw AS VARCHAR)), '') AS owner_address,
                NULLIF(TRIM(CAST(owner_postal_code_raw AS VARCHAR)), '') AS owner_postal_code,
                NULLIF(TRIM(CAST(owner_postal_district_raw AS VARCHAR)), '') AS owner_postal_district,
                NULLIF(TRIM(CAST(owner_city_raw AS VARCHAR)), '') AS owner_city,
                -- Integer field for municipality code
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(owner_municipality_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS owner_municipality_code,
                NULLIF(TRIM(CAST(owner_municipality_name_raw AS VARCHAR)), '') AS owner_municipality_name,
                NULLIF(TRIM(CAST(owner_country_raw AS VARCHAR)), '') AS owner_country,
                NULLIF(TRIM(CAST(owner_phone_raw AS VARCHAR)), '') AS owner_phone,
                NULLIF(TRIM(CAST(owner_mobile_raw AS VARCHAR)), '') AS owner_mobile,
                NULLIF(TRIM(CAST(owner_email_raw AS VARCHAR)), '') AS owner_email,
                NULLIF(TRIM(CAST(owner_address_protection_raw AS VARCHAR)), '') AS owner_address_protection,
                NULLIF(TRIM(CAST(owner_advertising_protection_raw AS VARCHAR)), '') AS owner_advertising_protection
            FROM raw_owners
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM herd_owners").fetchone()[0]

        if rows == 0:
            logging.warning("Herd owners table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_owners table with attributes ({rows} rows).")

        # Save to parquet
        output_path = silver_dir / "herd_owners.parquet"
        saved_path = export.save_table(output_path, "herd_owners", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save herd_owners table - no path returned")
            return None

        logging.info(f"Saved herd_owners table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM herd_owners")

    except Exception as e:
        logging.error(f"Failed to create herd_owners table with attributes: {e}", exc_info=True)
        return None


def create_herd_users_table(
    con: duckdb.DuckDBPyConnection, bes_details_raw: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the herd_users table including attributes from besaetning details.

    Args:
        con: The DuckDB connection
        bes_details_raw: Name of the raw besaetning details table in DuckDB (or None if not available)
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_users table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_users table with attributes.")

    try:
        # Create the herd_users table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE herd_users AS
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM {bes_details_raw}
            ),
            raw_users AS (
                SELECT DISTINCT
                    r.Besaetning.BesaetningsNummer AS herd_number_raw,
                    r.Besaetning.Bruger.CvrNummer AS user_cvr_raw,
                    r.Besaetning.Bruger.CprNummer AS user_cpr_raw,
                    r.Besaetning.Bruger.Navn AS user_name_raw,
                    r.Besaetning.Bruger.Adresse AS user_address_raw,
                    r.Besaetning.Bruger.PostNummer AS user_postal_code_raw,
                    r.Besaetning.Bruger.PostDistrikt AS user_postal_district_raw,
                    r.Besaetning.Bruger.ByNavn AS user_city_raw,
                    r.Besaetning.Bruger.KommuneNummer AS user_municipality_code_raw,
                    r.Besaetning.Bruger.KommuneNavn AS user_municipality_name_raw,
                    r.Besaetning.Bruger.Land AS user_country_raw,
                    r.Besaetning.Bruger.TelefonNummer AS user_phone_raw,
                    r.Besaetning.Bruger.MobilNummer AS user_mobile_raw,
                    r.Besaetning.Bruger.Email AS user_email_raw,
                    r.Besaetning.Bruger.Adressebeskyttelse AS user_address_protection_raw,
                    r.Besaetning.Bruger.Reklamebeskyttelse AS user_advertising_protection_raw
                FROM unnested_response
                WHERE r.Besaetning.BesaetningsNummer IS NOT NULL
                  AND r.Besaetning.Bruger IS NOT NULL
                  AND (
                      (r.Besaetning.Bruger.CvrNummer IS NOT NULL OR r.Besaetning.Bruger.CprNummer IS NOT NULL)
                      OR
                      (r.Besaetning.Bruger.Navn IS NOT NULL
                       AND r.Besaetning.Bruger.Adresse IS NOT NULL
                       AND r.Besaetning.Bruger.PostNummer IS NOT NULL)
                  )
            )
            SELECT
                -- Integer field with safe casting
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                -- String fields
                NULLIF(TRIM(CAST(user_cvr_raw AS VARCHAR)), '') AS user_cvr,
                NULLIF(TRIM(CAST(user_cpr_raw AS VARCHAR)), '') AS user_cpr,
                NULLIF(TRIM(CAST(user_name_raw AS VARCHAR)), '') AS user_name,
                NULLIF(TRIM(CAST(user_address_raw AS VARCHAR)), '') AS user_address,
                NULLIF(TRIM(CAST(user_postal_code_raw AS VARCHAR)), '') AS user_postal_code,
                NULLIF(TRIM(CAST(user_postal_district_raw AS VARCHAR)), '') AS user_postal_district,
                NULLIF(TRIM(CAST(user_city_raw AS VARCHAR)), '') AS user_city,
                -- Integer field for municipality code
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(user_municipality_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS user_municipality_code,
                NULLIF(TRIM(CAST(user_municipality_name_raw AS VARCHAR)), '') AS user_municipality_name,
                NULLIF(TRIM(CAST(user_country_raw AS VARCHAR)), '') AS user_country,
                NULLIF(TRIM(CAST(user_phone_raw AS VARCHAR)), '') AS user_phone,
                NULLIF(TRIM(CAST(user_mobile_raw AS VARCHAR)), '') AS user_mobile,
                NULLIF(TRIM(CAST(user_email_raw AS VARCHAR)), '') AS user_email,
                NULLIF(TRIM(CAST(user_address_protection_raw AS VARCHAR)), '') AS user_address_protection,
                NULLIF(TRIM(CAST(user_advertising_protection_raw AS VARCHAR)), '') AS user_advertising_protection
            FROM raw_users
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM herd_users").fetchone()[0]

        if rows == 0:
            logging.warning("Herd users table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_users table with attributes ({rows} rows).")

        # Save to parquet
        output_path = silver_dir / "herd_users.parquet"
        saved_path = export.save_table(output_path, "herd_users", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save herd_users table - no path returned")
            return None

        logging.info(f"Saved herd_users table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM herd_users")

    except Exception as e:
        logging.error(f"Failed to create herd_users table with attributes: {e}", exc_info=True)
        return None


def create_herd_sizes_table(
    con: duckdb.DuckDBPyConnection, bes_details_raw: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the herd_sizes table from the nested BesStr list in besaetning details.

    Args:
        con: The DuckDB connection
        bes_details_raw: Name of the raw besaetning details table in DuckDB (or None if not available)
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_sizes table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_sizes table.")

    try:
        # Create the herd_sizes table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE herd_sizes AS
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM {bes_details_raw}
            ),
            unnested_sizes AS (
                SELECT
                    r.Besaetning.BesaetningsNummer AS herd_number_raw,
                    r.Besaetning.ChrNummer AS chr_number_raw,
                    r.Besaetning.DyreArtKode AS species_code_raw,
                    r.Besaetning.DyreArtTekst AS species_name_raw,
                    UNNEST(r.Besaetning.BesStr) AS size_info,
                    r.Besaetning.BesStrDatoAjourfoert AS size_update_date_raw
                FROM unnested_response
                WHERE r.Besaetning.BesStr IS NOT NULL
                  AND array_length(r.Besaetning.BesStr) > 0
            ),
            raw_sizes AS (
                SELECT DISTINCT
                    herd_number_raw,
                    chr_number_raw,
                    species_code_raw,
                    species_name_raw,
                    size_info.BesaetningsStoerrelseTekst AS category_raw,
                    size_info.BesaetningsStoerrelse AS count_raw,
                    size_update_date_raw
                FROM unnested_sizes
            )
            SELECT
                uuid() AS size_id,
                -- Integer fields with safe casting
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS herd_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr_number,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(species_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS species_code,
                NULLIF(TRIM(CAST(species_name_raw AS VARCHAR)), '') AS species_name,
                NULLIF(TRIM(CAST(category_raw AS VARCHAR)), '') AS category,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(count_raw AS VARCHAR)), '') AS INTEGER), NULL) AS count,
                -- Date field with safe casting
                TRY_CAST(NULLIF(TRIM(CAST(size_update_date_raw AS VARCHAR)), '') AS DATE) AS size_update_date
            FROM raw_sizes
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(herd_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM herd_sizes").fetchone()[0]

        if rows == 0:
            logging.warning("Herd sizes table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_sizes table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "herd_sizes.parquet"
        saved_path = export.save_table(output_path, "herd_sizes", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save herd_sizes table - no path returned")
            return None

        logging.info(f"Saved herd_sizes table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM herd_sizes")

    except Exception as e:
        logging.error(f"Failed to create herd_sizes table: {e}", exc_info=True)
        return None
