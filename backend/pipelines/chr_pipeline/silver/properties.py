"""CHR Properties processing using vanilla DuckDB."""

import logging
from pathlib import Path

import duckdb

from . import export

# Import config constants and export module
from .config import SOURCE_CRS, TARGET_CRS


def create_properties_table(
    con: duckdb.DuckDBPyConnection, ejendom_oplys_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the properties table from ejendom_oplysninger data using vanilla DuckDB.

    Args:
        con: DuckDB connection
        ejendom_oplys_raw_table: Name of the raw ejendom_oplysninger data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed properties data or None if failed
    """
    logging.info("Starting creation of properties table.")

    if ejendom_oplys_raw_table is None:
        logging.warning("Cannot create properties: ejendom_oplys_raw_table is None.")
        return None

    try:
        # Install and load spatial extension for geometry operations
        try:
            con.execute("INSTALL spatial")
            con.execute("LOAD spatial")
        except Exception:
            pass  # Extensions might already be loaded

        # Create the cleaned and processed properties table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE properties AS
            WITH raw_data AS (
                SELECT
                    Response.EjendomsOplysninger.ChrNummer AS chr_number_raw,
                    Response.EjendomsOplysninger.Ejendom.Adresse AS address_raw,
                    Response.EjendomsOplysninger.Ejendom.PostNummer AS postal_code_raw,
                    Response.EjendomsOplysninger.Ejendom.PostDistrikt AS postal_district_raw,
                    Response.EjendomsOplysninger.Ejendom.ByNavn AS city_raw,
                    Response.EjendomsOplysninger.Ejendom.KommuneNummer AS municipality_code_raw,
                    Response.EjendomsOplysninger.Ejendom.KommuneNavn AS municipality_name_raw,
                    'Danmark' AS country_raw,
                    NULL AS phone_raw,
                    NULL AS mobile_raw,
                    NULL AS email_raw,
                    Response.EjendomsOplysninger.StaldKoordinater.StaldKoordinatX AS geo_coord_x_source_raw,
                    Response.EjendomsOplysninger.StaldKoordinater.StaldKoordinatY AS geo_coord_y_source_raw,
                    NULL AS geo_coord_x_measured_raw,
                    NULL AS geo_coord_y_measured_raw
                FROM {ejendom_oplys_raw_table}
                WHERE Response.EjendomsOplysninger IS NOT NULL
            ),
            cleaned_data AS (
                SELECT
                    uuid() AS property_id,
                    -- Basic identifiers: cast to string, trim, nullif empty, then cast to int64
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                    -- Address fields
                    NULLIF(TRIM(CAST(address_raw AS VARCHAR)), '') AS address,
                    NULLIF(TRIM(CAST(postal_code_raw AS VARCHAR)), '') AS postal_code,
                    NULLIF(TRIM(CAST(postal_district_raw AS VARCHAR)), '') AS postal_district,
                    NULLIF(TRIM(CAST(city_raw AS VARCHAR)), '') AS city,
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(municipality_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS municipality_code,
                    NULLIF(TRIM(CAST(municipality_name_raw AS VARCHAR)), '') AS municipality,
                    NULLIF(TRIM(CAST(country_raw AS VARCHAR)), '') AS country,
                    -- Contact fields
                    NULLIF(TRIM(CAST(phone_raw AS VARCHAR)), '') AS phone,
                    NULLIF(TRIM(CAST(mobile_raw AS VARCHAR)), '') AS mobile,
                    NULLIF(TRIM(CAST(email_raw AS VARCHAR)), '') AS email,
                    -- Coordinate fields
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(geo_coord_x_source_raw AS VARCHAR)), '') AS DOUBLE), NULL) AS geo_coord_x_source,
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(geo_coord_y_source_raw AS VARCHAR)), '') AS DOUBLE), NULL) AS geo_coord_y_source,
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(geo_coord_x_measured_raw AS VARCHAR)), '') AS DOUBLE), NULL) AS geo_coord_x_measured,
                    COALESCE(TRY_CAST(NULLIF(TRIM(CAST(geo_coord_y_measured_raw AS VARCHAR)), '') AS DOUBLE), NULL) AS geo_coord_y_measured
                FROM raw_data
            )
            SELECT
                property_id,
                chr,
                address,
                postal_code,
                postal_district,
                city,
                municipality_code,
                municipality,
                country,
                phone,
                mobile,
                email,
                -- Transform UTM coordinates to WGS84 latitude and longitude
                CASE
                    WHEN geo_coord_x_source IS NOT NULL AND geo_coord_y_source IS NOT NULL THEN
                        ST_X(ST_Transform(ST_Point(geo_coord_x_source, geo_coord_y_source), '{SOURCE_CRS}', '{TARGET_CRS}'))
                    WHEN geo_coord_x_measured IS NOT NULL AND geo_coord_y_measured IS NOT NULL THEN
                        ST_X(ST_Transform(ST_Point(geo_coord_x_measured, geo_coord_y_measured), '{SOURCE_CRS}', '{TARGET_CRS}'))
                    ELSE NULL
                END AS latitude,
                CASE
                    WHEN geo_coord_x_source IS NOT NULL AND geo_coord_y_source IS NOT NULL THEN
                        ST_Y(ST_Transform(ST_Point(geo_coord_x_source, geo_coord_y_source), '{SOURCE_CRS}', '{TARGET_CRS}'))
                    WHEN geo_coord_x_measured IS NOT NULL AND geo_coord_y_measured IS NOT NULL THEN
                        ST_Y(ST_Transform(ST_Point(geo_coord_x_measured, geo_coord_y_measured), '{SOURCE_CRS}', '{TARGET_CRS}'))
                    ELSE NULL
                END AS longitude,
                -- Create WGS84 geometry
                CASE
                    WHEN geo_coord_x_source IS NOT NULL AND geo_coord_y_source IS NOT NULL THEN
                        ST_Transform(ST_Point(geo_coord_x_source, geo_coord_y_source), '{SOURCE_CRS}', '{TARGET_CRS}')
                    WHEN geo_coord_x_measured IS NOT NULL AND geo_coord_y_measured IS NOT NULL THEN
                        ST_Transform(ST_Point(geo_coord_x_measured, geo_coord_y_measured), '{SOURCE_CRS}', '{TARGET_CRS}')
                    ELSE NULL
                END AS location_geom
            FROM cleaned_data
            WHERE chr IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM properties").fetchone()[0]

        if rows == 0:
            logging.warning("Properties table is empty after processing.")
            return None

        logging.info(f"Saving properties table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "properties.parquet"
        saved_path = export.save_table(output_path, "properties", con, is_geo=True)

        if saved_path is None:
            logging.error("Failed to save properties table - no path returned")
            return None

        logging.info(f"Saved properties table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM properties")

    except Exception as e:
        logging.error(f"Failed to create properties table: {e}", exc_info=True)
        return None


def create_property_owners_table(
    con: duckdb.DuckDBPyConnection, ejendom_oplys_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the property_owners table from ejendom_oplysninger data using vanilla DuckDB.

    Args:
        con: DuckDB connection
        ejendom_oplys_raw_table: Name of the raw ejendom_oplysninger data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed property owners data or None if failed
    """
    logging.info("Starting creation of property_owners table.")

    if ejendom_oplys_raw_table is None:
        logging.warning("Cannot create property_owners: ejendom_oplys_raw_table is None.")
        return None

    try:
        # Create the cleaned and processed property owners table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE property_owners AS
            WITH unnested_besaetninger AS (
                SELECT
                    Response.EjendomsOplysninger.ChrNummer AS chr_number,
                    UNNEST(Response.EjendomsOplysninger.Besaetninger.Besaetning) AS besaetning
                FROM {ejendom_oplys_raw_table}
                WHERE Response.EjendomsOplysninger.Besaetninger.Besaetning IS NOT NULL
            ),
            raw_owners AS (
                SELECT DISTINCT
                    chr_number AS chr_number_raw,
                    besaetning.Ejer.CvrNummer AS owner_cvr_raw,
                    besaetning.Ejer.CprNummer AS owner_cpr_raw,
                    besaetning.Ejer.Navn AS owner_name_raw,
                    besaetning.Ejer.Adresse AS owner_address_raw,
                    besaetning.Ejer.PostNummer AS owner_postal_code_raw,
                    besaetning.Ejer.PostDistrikt AS owner_postal_district_raw,
                    besaetning.Ejer.ByNavn AS owner_city_raw,
                    besaetning.Ejer.KommuneNummer AS owner_municipality_code_raw,
                    besaetning.Ejer.KommuneNavn AS owner_municipality_name_raw,
                    besaetning.Ejer.Land AS owner_country_raw,
                    besaetning.Ejer.TelefonNummer AS owner_phone_raw,
                    besaetning.Ejer.MobilNummer AS owner_mobile_raw,
                    besaetning.Ejer.Email AS owner_email_raw,
                    besaetning.Ejer.Adressebeskyttelse AS owner_address_protection_raw,
                    besaetning.Ejer.Reklamebeskyttelse AS owner_advertising_protection_raw
                FROM unnested_besaetninger
                WHERE besaetning.Ejer IS NOT NULL
            )
            SELECT
                uuid() AS owner_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                -- Owner identification
                NULLIF(TRIM(CAST(owner_cvr_raw AS VARCHAR)), '') AS owner_cvr,
                NULLIF(TRIM(CAST(owner_cpr_raw AS VARCHAR)), '') AS owner_cpr,
                NULLIF(TRIM(CAST(owner_name_raw AS VARCHAR)), '') AS owner_name,
                -- Owner address
                NULLIF(TRIM(CAST(owner_address_raw AS VARCHAR)), '') AS owner_address,
                NULLIF(TRIM(CAST(owner_postal_code_raw AS VARCHAR)), '') AS owner_postal_code,
                NULLIF(TRIM(CAST(owner_postal_district_raw AS VARCHAR)), '') AS owner_postal_district,
                NULLIF(TRIM(CAST(owner_city_raw AS VARCHAR)), '') AS owner_city,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(owner_municipality_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS owner_municipality_code,
                NULLIF(TRIM(CAST(owner_municipality_name_raw AS VARCHAR)), '') AS owner_municipality_name,
                NULLIF(TRIM(CAST(owner_country_raw AS VARCHAR)), '') AS owner_country,
                -- Owner contact
                NULLIF(TRIM(CAST(owner_phone_raw AS VARCHAR)), '') AS owner_phone,
                NULLIF(TRIM(CAST(owner_mobile_raw AS VARCHAR)), '') AS owner_mobile,
                NULLIF(TRIM(CAST(owner_email_raw AS VARCHAR)), '') AS owner_email,
                -- Owner protection flags
                NULLIF(TRIM(CAST(owner_address_protection_raw AS VARCHAR)), '') AS owner_address_protection,
                NULLIF(TRIM(CAST(owner_advertising_protection_raw AS VARCHAR)), '') AS owner_advertising_protection
            FROM raw_owners
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM property_owners").fetchone()[0]

        if rows == 0:
            logging.warning("Property owners table is empty after processing.")
            return None

        logging.info(f"Saving property_owners table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "property_owners.parquet"
        saved_path = export.save_table(output_path, "property_owners", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save property_owners table - no path returned")
            return None

        logging.info(f"Saved property_owners table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM property_owners")

    except Exception as e:
        logging.error(f"Failed to create property_owners table: {e}", exc_info=True)
        return None


def create_property_users_table(
    con: duckdb.DuckDBPyConnection, ejendom_oplys_raw_table: str | None, silver_dir: Path
) -> duckdb.DuckDBPyRelation | None:
    """Creates the property_users table from ejendom_oplysninger data using vanilla DuckDB.

    Args:
        con: DuckDB connection
        ejendom_oplys_raw_table: Name of the raw ejendom_oplysninger data table in DuckDB (or None if not available)
        silver_dir: Output directory for silver files

    Returns:
        DuckDB relation with processed property users data or None if failed
    """
    logging.info("Starting creation of property_users table.")

    if ejendom_oplys_raw_table is None:
        logging.warning("Cannot create property_users: ejendom_oplys_raw_table is None.")
        return None

    try:
        # Create the cleaned and processed property users table with SQL
        con.execute(f"""
            CREATE OR REPLACE TABLE property_users AS
            WITH unnested_besaetninger AS (
                SELECT
                    Response.EjendomsOplysninger.ChrNummer AS chr_number,
                    UNNEST(Response.EjendomsOplysninger.Besaetninger.Besaetning) AS besaetning
                FROM {ejendom_oplys_raw_table}
                WHERE Response.EjendomsOplysninger.Besaetninger.Besaetning IS NOT NULL
            ),
            raw_users AS (
                SELECT DISTINCT
                    chr_number AS chr_number_raw,
                    besaetning.Bruger.CvrNummer AS user_cvr_raw,
                    besaetning.Bruger.CprNummer AS user_cpr_raw,
                    besaetning.Bruger.Navn AS user_name_raw,
                    besaetning.Bruger.Adresse AS user_address_raw,
                    besaetning.Bruger.PostNummer AS user_postal_code_raw,
                    besaetning.Bruger.PostDistrikt AS user_postal_district_raw,
                    besaetning.Bruger.ByNavn AS user_city_raw,
                    besaetning.Bruger.KommuneNummer AS user_municipality_code_raw,
                    besaetning.Bruger.KommuneNavn AS user_municipality_name_raw,
                    besaetning.Bruger.Land AS user_country_raw,
                    besaetning.Bruger.TelefonNummer AS user_phone_raw,
                    besaetning.Bruger.MobilNummer AS user_mobile_raw,
                    besaetning.Bruger.Email AS user_email_raw,
                    besaetning.Bruger.Adressebeskyttelse AS user_address_protection_raw,
                    besaetning.Bruger.Reklamebeskyttelse AS user_advertising_protection_raw
                FROM unnested_besaetninger
                WHERE besaetning.Bruger IS NOT NULL
            )
            SELECT
                uuid() AS user_id,
                -- Basic identifiers
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) AS chr,
                -- User identification
                NULLIF(TRIM(CAST(user_cvr_raw AS VARCHAR)), '') AS user_cvr,
                NULLIF(TRIM(CAST(user_cpr_raw AS VARCHAR)), '') AS user_cpr,
                NULLIF(TRIM(CAST(user_name_raw AS VARCHAR)), '') AS user_name,
                -- User address
                NULLIF(TRIM(CAST(user_address_raw AS VARCHAR)), '') AS user_address,
                NULLIF(TRIM(CAST(user_postal_code_raw AS VARCHAR)), '') AS user_postal_code,
                NULLIF(TRIM(CAST(user_postal_district_raw AS VARCHAR)), '') AS user_postal_district,
                NULLIF(TRIM(CAST(user_city_raw AS VARCHAR)), '') AS user_city,
                COALESCE(TRY_CAST(NULLIF(TRIM(CAST(user_municipality_code_raw AS VARCHAR)), '') AS INTEGER), NULL) AS user_municipality_code,
                NULLIF(TRIM(CAST(user_municipality_name_raw AS VARCHAR)), '') AS user_municipality_name,
                NULLIF(TRIM(CAST(user_country_raw AS VARCHAR)), '') AS user_country,
                -- User contact
                NULLIF(TRIM(CAST(user_phone_raw AS VARCHAR)), '') AS user_phone,
                NULLIF(TRIM(CAST(user_mobile_raw AS VARCHAR)), '') AS user_mobile,
                NULLIF(TRIM(CAST(user_email_raw AS VARCHAR)), '') AS user_email,
                -- User protection flags
                NULLIF(TRIM(CAST(user_address_protection_raw AS VARCHAR)), '') AS user_address_protection,
                NULLIF(TRIM(CAST(user_advertising_protection_raw AS VARCHAR)), '') AS user_advertising_protection
            FROM raw_users
            WHERE COALESCE(TRY_CAST(NULLIF(TRIM(CAST(chr_number_raw AS VARCHAR)), '') AS BIGINT), NULL) IS NOT NULL
        """)

        # Get row count
        rows = con.execute("SELECT COUNT(*) FROM property_users").fetchone()[0]

        if rows == 0:
            logging.warning("Property users table is empty after processing.")
            return None

        logging.info(f"Saving property_users table with {rows} rows.")

        # Save to parquet
        output_path = silver_dir / "property_users.parquet"
        saved_path = export.save_table(output_path, "property_users", con, is_geo=False)

        if saved_path is None:
            logging.error("Failed to save property_users table - no path returned")
            return None

        logging.info(f"Saved property_users table to {saved_path}")

        # Return the relation
        return con.sql("SELECT * FROM property_users")

    except Exception as e:
        logging.error(f"Failed to create property_users table: {e}", exc_info=True)
        return None
