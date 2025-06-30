"""CHR Properties processing using DuckDB and Ibis."""

import logging
from pathlib import Path
from typing import Optional

import ibis
import ibis.expr.datatypes as dt

# Import export module
from . import export


def create_properties_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the properties table from ejendom_oplysninger data using DuckDB and Ibis."""
    logging.info("Starting creation of properties table.")

    # Check for nested structure Response.EjendomsOplysninger
    if ejendom_oplys_raw is None or "Response" not in ejendom_oplys_raw.columns:
        logging.warning("Cannot create properties: 'Response' column missing in ejendom_oplys_raw.")
        return None

    try:
        # Register the table with DuckDB for SQL operations
        con.create_table("ejendom_oplys_raw", ejendom_oplys_raw, overwrite=True)

        # Extract property information using SQL
        properties_base = con.sql("""
            SELECT
                CAST(Response.ChrNummer AS STRING) AS chr_number_raw,
                Response.EjendomsOplysninger.Adresse AS address_raw,
                Response.EjendomsOplysninger.PostNummer AS postal_code_raw,
                Response.EjendomsOplysninger.PostDistrikt AS postal_district_raw,
                Response.EjendomsOplysninger.By AS city_raw,
                Response.EjendomsOplysninger.KommuneKode AS municipality_code_raw,
                Response.EjendomsOplysninger.KommuneNavn AS municipality_name_raw,
                Response.EjendomsOplysninger.Land AS country_raw,
                Response.EjendomsOplysninger.Telefon AS phone_raw,
                Response.EjendomsOplysninger.Mobil AS mobile_raw,
                Response.EjendomsOplysninger.Email AS email_raw,
                Response.EjendomsOplysninger.GeoKoordXKilde AS geo_coord_x_source_raw,
                Response.EjendomsOplysninger.GeoKoordYKilde AS geo_coord_y_source_raw,
                Response.EjendomsOplysninger.GeoKoordXMaalte AS geo_coord_x_measured_raw,
                Response.EjendomsOplysninger.GeoKoordYMaalte AS geo_coord_y_measured_raw
            FROM ejendom_oplys_raw
            WHERE Response.EjendomsOplysninger IS NOT NULL
        """)

        # Generate UUID and clean/cast columns
        properties = properties_base.mutate(
            property_id=ibis.uuid(),
            chr_number=ibis.coalesce(
                properties_base.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            address=properties_base.address_raw.cast(dt.string).strip().nullif(""),
            postal_code=properties_base.postal_code_raw.cast(dt.string).strip().nullif(""),
            postal_district=properties_base.postal_district_raw.cast(dt.string).strip().nullif(""),
            city=properties_base.city_raw.cast(dt.string).strip().nullif(""),
            municipality_code=ibis.coalesce(
                properties_base.municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            municipality_name=properties_base.municipality_name_raw.cast(dt.string).strip().nullif(""),
            country=properties_base.country_raw.cast(dt.string).strip().nullif(""),
            phone=properties_base.phone_raw.cast(dt.string).strip().nullif(""),
            mobile=properties_base.mobile_raw.cast(dt.string).strip().nullif(""),
            email=properties_base.email_raw.cast(dt.string).strip().nullif(""),
            geo_coord_x_source=ibis.coalesce(
                properties_base.geo_coord_x_source_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            geo_coord_y_source=ibis.coalesce(
                properties_base.geo_coord_y_source_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            geo_coord_x_measured=ibis.coalesce(
                properties_base.geo_coord_x_measured_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            geo_coord_y_measured=ibis.coalesce(
                properties_base.geo_coord_y_measured_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
        )

        # Create temporary table for geometry processing
        con.create_table("properties_temp", properties, overwrite=True)

        # Create geometry from coordinates using DuckDB-spatial SQL
        properties_with_geom = con.sql("""
            SELECT 
                *,
                CASE 
                    WHEN geo_coord_x_source IS NOT NULL AND geo_coord_y_source IS NOT NULL THEN
                        ST_Transform(ST_Point(geo_coord_x_source, geo_coord_y_source), 'EPSG:25832', 'EPSG:4326')
                    WHEN geo_coord_x_measured IS NOT NULL AND geo_coord_y_measured IS NOT NULL THEN
                        ST_Transform(ST_Point(geo_coord_x_measured, geo_coord_y_measured), 'EPSG:25832', 'EPSG:4326')
                    ELSE NULL
                END as geometry
            FROM properties_temp
        """)

        # Select final columns
        final_cols = [
            "property_id",
            "chr_number",
            "address",
            "postal_code",
            "postal_district",
            "city",
            "municipality_code",
            "municipality_name",
            "country",
            "phone",
            "mobile",
            "email",
            "geo_coord_x_source",
            "geo_coord_y_source",
            "geo_coord_x_measured",
            "geo_coord_y_measured",
            "geometry",
        ]
        properties_final = properties_with_geom.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "properties.parquet"
        rows = properties_final.count().execute()
        if rows == 0:
            logging.warning("Properties table is empty after processing.")
            return None

        logging.info(f"Saving properties table with {rows} rows.")
        # ✅ MIGRATION: Pass Ibis table directly instead of executing to pandas
        saved_path = export.save_table(output_path, properties_final, is_geo=True)
        if saved_path is None:
            logging.error("Failed to save properties table - no path returned")
            return None
        logging.info(f"Saved properties table to {saved_path}")

        # Clean up
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
            con.drop_table("properties_temp", force=True)
        except Exception:
            pass

        return properties_final

    except Exception as e:
        logging.error(f"Failed to create properties table: {e}", exc_info=True)
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
        except Exception:
            pass
        return None


def create_property_owners_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the property_owners table from ejendom_oplysninger data using DuckDB and Ibis."""
    logging.info("Starting creation of property_owners table.")

    # Check for nested structure Response.EjendomsOplysninger.Ejer
    if ejendom_oplys_raw is None or "Response" not in ejendom_oplys_raw.columns:
        logging.warning("Cannot create property_owners: 'Response' column missing in ejendom_oplys_raw.")
        return None

    try:
        # Register the table with DuckDB for SQL operations
        con.create_table("ejendom_oplys_raw", ejendom_oplys_raw, overwrite=True)

        # Extract property owner information using SQL
        property_owners_base = con.sql("""
            SELECT
                CAST(Response.ChrNummer AS STRING) AS chr_number_raw,
                Response.EjendomsOplysninger.Ejer.CVR AS owner_cvr_raw,
                Response.EjendomsOplysninger.Ejer.CPR AS owner_cpr_raw,
                Response.EjendomsOplysninger.Ejer.Navn AS owner_name_raw,
                Response.EjendomsOplysninger.Ejer.Adresse AS owner_address_raw,
                Response.EjendomsOplysninger.Ejer.PostNummer AS owner_postal_code_raw,
                Response.EjendomsOplysninger.Ejer.PostDistrikt AS owner_postal_district_raw,
                Response.EjendomsOplysninger.Ejer.By AS owner_city_raw,
                Response.EjendomsOplysninger.Ejer.KommuneKode AS owner_municipality_code_raw,
                Response.EjendomsOplysninger.Ejer.KommuneNavn AS owner_municipality_name_raw,
                Response.EjendomsOplysninger.Ejer.Land AS owner_country_raw,
                Response.EjendomsOplysninger.Ejer.Telefon AS owner_phone_raw,
                Response.EjendomsOplysninger.Ejer.Mobil AS owner_mobile_raw,
                Response.EjendomsOplysninger.Ejer.Email AS owner_email_raw,
                Response.EjendomsOplysninger.Ejer.AdresseBeskyttelse AS owner_address_protection_raw,
                Response.EjendomsOplysninger.Ejer.ReklameBeskyttelse AS owner_advertising_protection_raw
            FROM ejendom_oplys_raw
            WHERE Response.EjendomsOplysninger.Ejer IS NOT NULL
        """)

        # Generate UUID and clean/cast columns
        property_owners = property_owners_base.mutate(
            owner_id=ibis.uuid(),
            chr_number=ibis.coalesce(
                property_owners_base.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            owner_cvr=property_owners_base.owner_cvr_raw.cast(dt.string).strip().nullif(""),
            owner_cpr=property_owners_base.owner_cpr_raw.cast(dt.string).strip().nullif(""),
            owner_name=property_owners_base.owner_name_raw.cast(dt.string).strip().nullif(""),
            owner_address=property_owners_base.owner_address_raw.cast(dt.string).strip().nullif(""),
            owner_postal_code=property_owners_base.owner_postal_code_raw.cast(dt.string).strip().nullif(""),
            owner_postal_district=property_owners_base.owner_postal_district_raw.cast(dt.string).strip().nullif(""),
            owner_city=property_owners_base.owner_city_raw.cast(dt.string).strip().nullif(""),
            owner_municipality_code=ibis.coalesce(
                property_owners_base.owner_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            owner_municipality_name=property_owners_base.owner_municipality_name_raw.cast(dt.string).strip().nullif(""),
            owner_country=property_owners_base.owner_country_raw.cast(dt.string).strip().nullif(""),
            owner_phone=property_owners_base.owner_phone_raw.cast(dt.string).strip().nullif(""),
            owner_mobile=property_owners_base.owner_mobile_raw.cast(dt.string).strip().nullif(""),
            owner_email=property_owners_base.owner_email_raw.cast(dt.string).strip().nullif(""),
            owner_address_protection=property_owners_base.owner_address_protection_raw.cast(dt.string)
            .strip()
            .nullif(""),
            owner_advertising_protection=property_owners_base.owner_advertising_protection_raw.cast(dt.string)
            .strip()
            .nullif(""),
        )

        # Filter out rows with null chr_number after cleaning
        property_owners = property_owners.filter(property_owners.chr_number.notnull())

        # Select final columns
        final_cols = [
            "owner_id",
            "chr_number",
            "owner_cvr",
            "owner_cpr",
            "owner_name",
            "owner_address",
            "owner_postal_code",
            "owner_postal_district",
            "owner_city",
            "owner_municipality_code",
            "owner_municipality_name",
            "owner_country",
            "owner_phone",
            "owner_mobile",
            "owner_email",
            "owner_address_protection",
            "owner_advertising_protection",
        ]
        property_owners_final = property_owners.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "property_owners.parquet"
        rows = property_owners_final.count().execute()
        if rows == 0:
            logging.warning("Property owners table is empty after processing.")
            return None

        logging.info(f"Saving property_owners table with {rows} rows.")
        # ✅ MIGRATION: Pass Ibis table directly instead of executing to pandas
        saved_path = export.save_table(output_path, property_owners_final, is_geo=False)
        if saved_path is None:
            logging.error("Failed to save property_owners table - no path returned")
            return None
        logging.info(f"Saved property_owners table to {saved_path}")

        # Clean up
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
        except Exception:
            pass

        return property_owners_final

    except Exception as e:
        logging.error(f"Failed to create property_owners table: {e}", exc_info=True)
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
        except Exception:
            pass
        return None


def create_property_users_table(
    con: ibis.BaseBackend, ejendom_oplys_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the property_users table from ejendom_oplysninger data using DuckDB and Ibis."""
    logging.info("Starting creation of property_users table.")

    # Check for nested structure Response.EjendomsOplysninger.Bruger
    if ejendom_oplys_raw is None or "Response" not in ejendom_oplys_raw.columns:
        logging.warning("Cannot create property_users: 'Response' column missing in ejendom_oplys_raw.")
        return None

    try:
        # Register the table with DuckDB for SQL operations
        con.create_table("ejendom_oplys_raw", ejendom_oplys_raw, overwrite=True)

        # Extract property user information using SQL
        property_users_base = con.sql("""
            SELECT
                CAST(Response.ChrNummer AS STRING) AS chr_number_raw,
                Response.EjendomsOplysninger.Bruger.CVR AS user_cvr_raw,
                Response.EjendomsOplysninger.Bruger.CPR AS user_cpr_raw,
                Response.EjendomsOplysninger.Bruger.Navn AS user_name_raw,
                Response.EjendomsOplysninger.Bruger.Adresse AS user_address_raw,
                Response.EjendomsOplysninger.Bruger.PostNummer AS user_postal_code_raw,
                Response.EjendomsOplysninger.Bruger.PostDistrikt AS user_postal_district_raw,
                Response.EjendomsOplysninger.Bruger.By AS user_city_raw,
                Response.EjendomsOplysninger.Bruger.KommuneKode AS user_municipality_code_raw,
                Response.EjendomsOplysninger.Bruger.KommuneNavn AS user_municipality_name_raw,
                Response.EjendomsOplysninger.Bruger.Land AS user_country_raw,
                Response.EjendomsOplysninger.Bruger.Telefon AS user_phone_raw,
                Response.EjendomsOplysninger.Bruger.Mobil AS user_mobile_raw,
                Response.EjendomsOplysninger.Bruger.Email AS user_email_raw,
                Response.EjendomsOplysninger.Bruger.AdresseBeskyttelse AS user_address_protection_raw,
                Response.EjendomsOplysninger.Bruger.ReklameBeskyttelse AS user_advertising_protection_raw
            FROM ejendom_oplys_raw
            WHERE Response.EjendomsOplysninger.Bruger IS NOT NULL
        """)

        # Generate UUID and clean/cast columns
        property_users = property_users_base.mutate(
            user_id=ibis.uuid(),
            chr_number=ibis.coalesce(
                property_users_base.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            user_cvr=property_users_base.user_cvr_raw.cast(dt.string).strip().nullif(""),
            user_cpr=property_users_base.user_cpr_raw.cast(dt.string).strip().nullif(""),
            user_name=property_users_base.user_name_raw.cast(dt.string).strip().nullif(""),
            user_address=property_users_base.user_address_raw.cast(dt.string).strip().nullif(""),
            user_postal_code=property_users_base.user_postal_code_raw.cast(dt.string).strip().nullif(""),
            user_postal_district=property_users_base.user_postal_district_raw.cast(dt.string).strip().nullif(""),
            user_city=property_users_base.user_city_raw.cast(dt.string).strip().nullif(""),
            user_municipality_code=ibis.coalesce(
                property_users_base.user_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            user_municipality_name=property_users_base.user_municipality_name_raw.cast(dt.string).strip().nullif(""),
            user_country=property_users_base.user_country_raw.cast(dt.string).strip().nullif(""),
            user_phone=property_users_base.user_phone_raw.cast(dt.string).strip().nullif(""),
            user_mobile=property_users_base.user_mobile_raw.cast(dt.string).strip().nullif(""),
            user_email=property_users_base.user_email_raw.cast(dt.string).strip().nullif(""),
            user_address_protection=property_users_base.user_address_protection_raw.cast(dt.string).strip().nullif(""),
            user_advertising_protection=property_users_base.user_advertising_protection_raw.cast(dt.string)
            .strip()
            .nullif(""),
        )

        # Filter out rows with null chr_number after cleaning
        property_users = property_users.filter(property_users.chr_number.notnull())

        # Select final columns
        final_cols = [
            "user_id",
            "chr_number",
            "user_cvr",
            "user_cpr",
            "user_name",
            "user_address",
            "user_postal_code",
            "user_postal_district",
            "user_city",
            "user_municipality_code",
            "user_municipality_name",
            "user_country",
            "user_phone",
            "user_mobile",
            "user_email",
            "user_address_protection",
            "user_advertising_protection",
        ]
        property_users_final = property_users.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "property_users.parquet"
        rows = property_users_final.count().execute()
        if rows == 0:
            logging.warning("Property users table is empty after processing.")
            return None

        logging.info(f"Saving property_users table with {rows} rows.")
        # ✅ MIGRATION: Pass Ibis table directly instead of executing to pandas
        saved_path = export.save_table(output_path, property_users_final, is_geo=False)
        if saved_path is None:
            logging.error("Failed to save property_users table - no path returned")
            return None
        logging.info(f"Saved property_users table to {saved_path}")

        # Clean up
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
        except Exception:
            pass

        return property_users_final

    except Exception as e:
        logging.error(f"Failed to create property_users table: {e}", exc_info=True)
        try:
            con.drop_table("ejendom_oplys_raw", force=True)
        except Exception:
            pass
        return None
