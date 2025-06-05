import logging
from pathlib import Path
from typing import Optional

import ibis
import ibis.expr.datatypes as dt

# Import export module
from . import export


def create_herds_table(
    con: ibis.BaseBackend, bes_details_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the herds table (excluding owner/user identifiers) from besaetning details.

    Args:
        con: The database connection
        bes_details_raw: Raw besaetning details table
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herds table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herds table.")

    try:
        # SQL query excluding owner/user identifiers
        herds = con.sql("""
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM bes_details -- Assumes bes_details_raw is registered as 'bes_details'
            )
            SELECT DISTINCT -- Added DISTINCT to avoid duplicates if the source has them
                r.Besaetning.BesaetningsNummer AS herd_number,
                r.Besaetning.ChrNummer AS chr_number,
                -- Identifiers removed, handled in separate functions
                r.Besaetning.DyreArtKode AS species_code,
                r.Besaetning.DyreArtTekst AS species_name,
                r.Besaetning.BrugsArtKode AS usage_type_code,
                r.Besaetning.BrugsArtTekst AS usage_type_name,
                r.Besaetning.VirksomhedsArtTekst AS business_type_name,
                r.Besaetning.OmsaetningsKode AS turnover_code,
                r.Besaetning.OmsaetningsTekst AS turnover_text,
                r.Besaetning.LeveringsErklaeringer AS delivery_declarations,
                r.Besaetning.DatoOphoer AS date_ceased,
                r.Besaetning.Oekologisk AS is_organic,
                r.Besaetning.DatoOpret AS date_created,
                r.Besaetning.DatoOpdatering AS date_updated
            FROM unnested_response
            WHERE r.Besaetning.BesaetningsNummer IS NOT NULL
        """)

        # Clean and cast columns, owner/user identifiers removed
        herds = herds.mutate(
            herd_number=ibis.coalesce(
                herds.herd_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            chr_number=ibis.coalesce(
                herds.chr_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # owner_identifier removed
            # user_identifier removed
            species_code=ibis.coalesce(
                herds.species_code.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            species_name=herds.species_name.cast(dt.string).strip().nullif(""),
            usage_type_code=ibis.coalesce(
                herds.usage_type_code.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            usage_type_name=herds.usage_type_name.cast(dt.string).strip().nullif(""),
            business_type_name=herds.business_type_name.cast(dt.string).strip().nullif(""),
            turnover_code=ibis.coalesce(
                herds.turnover_code.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            turnover_text=herds.turnover_text.cast(dt.string).strip().nullif(""),
            delivery_declarations=herds.delivery_declarations.cast(dt.string)
            .strip()
            .nullif(""),  # Assuming string type
            date_ceased=ibis.coalesce(
                herds.date_ceased.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            is_organic=(herds.is_organic.cast(dt.string).strip().nullif("").lower() == "ja"),
            date_created=ibis.coalesce(
                herds.date_created.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            date_updated=ibis.coalesce(
                herds.date_updated.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
        )

        # Define final column order, owner/user identifiers removed
        final_cols = [
            "herd_number",
            "chr_number",
            # owner_identifier removed
            # user_identifier removed
            "species_code",
            "species_name",
            "usage_type_code",
            "usage_type_name",
            "business_type_name",
            "turnover_code",
            "turnover_text",
            "delivery_declarations",
            "is_organic",
            "date_created",
            "date_updated",
            "date_ceased",
        ]
        herds_final = herds.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "herds.parquet"
        rows = herds_final.count().execute()  # Ensure table is not empty before saving
        if rows == 0:
            logging.warning("Herds table is empty after processing. Not saving file.")
            return None  # Return None if no rows

        logging.info(f"Saving herds table with {rows} rows.")
        saved_path = export.save_table(output_path, herds_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save herds table - no path returned")
            return None
        logging.info(f"Saved herds table to {saved_path}")

        return herds_final  # Return the table object

    except Exception as e:
        logging.error(f"Failed to create herds table: {e}", exc_info=True)
        return None


def create_herd_owners_table(
    con: ibis.BaseBackend, bes_details_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the herd_owners table including attributes from besaetning details.

    Args:
        con: The database connection
        bes_details_raw: Raw besaetning details table
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_owners table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_owners table with attributes.")

    try:
        # Extract herd_number, owner_identifier, and owner attributes
        herd_owners = con.sql("""
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM bes_details -- Assumes bes_details_raw is registered as 'bes_details'
            )
            SELECT DISTINCT -- Using DISTINCT on the whole row
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
              AND r.Besaetning.Ejer IS NOT NULL -- Ensure Ejer struct exists
              AND (
                  (r.Besaetning.Ejer.CvrNummer IS NOT NULL OR r.Besaetning.Ejer.CprNummer IS NOT NULL)
                  OR
                  (r.Besaetning.Ejer.Navn IS NOT NULL AND r.Besaetning.Ejer.Adresse IS NOT NULL AND r.Besaetning.Ejer.PostNummer IS NOT NULL)
              )
        """)

        # Clean and cast columns
        herd_owners = herd_owners.mutate(
            herd_number=ibis.coalesce(
                herd_owners.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # owner_identifier removed
            owner_cvr=herd_owners.owner_cvr_raw.cast(dt.string).strip().nullif(""),
            owner_cpr=herd_owners.owner_cpr_raw.cast(dt.string).strip().nullif(""),
            owner_name=herd_owners.owner_name_raw.cast(dt.string).strip().nullif(""),
            owner_address=herd_owners.owner_address_raw.cast(dt.string).strip().nullif(""),
            owner_postal_code=herd_owners.owner_postal_code_raw.cast(dt.string).strip().nullif(""),
            owner_postal_district=herd_owners.owner_postal_district_raw.cast(dt.string).strip().nullif(""),
            owner_city=herd_owners.owner_city_raw.cast(dt.string).strip().nullif(""),
            owner_municipality_code=ibis.coalesce(
                herd_owners.owner_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            owner_municipality_name=herd_owners.owner_municipality_name_raw.cast(dt.string).strip().nullif(""),
            owner_country=herd_owners.owner_country_raw.cast(dt.string).strip().nullif(""),
            owner_phone=herd_owners.owner_phone_raw.cast(dt.string).strip().nullif(""),
            owner_mobile=herd_owners.owner_mobile_raw.cast(dt.string).strip().nullif(""),
            owner_email=herd_owners.owner_email_raw.cast(dt.string).strip().nullif(""),
            owner_address_protection=herd_owners.owner_address_protection_raw.cast(dt.string).strip().nullif(""),
            owner_advertising_protection=herd_owners.owner_advertising_protection_raw.cast(dt.string)
            .strip()
            .nullif(""),
        )

        # Filter out rows with null herd_number or (now removed) owner_identifier after cleaning
        # Filter only on herd_number now, as identifier is removed. The SQL WHERE clause already filters unidentifiable owners.
        herd_owners = herd_owners.filter(herd_owners.herd_number.notnull())

        # Define final columns order
        final_cols = [
            "herd_number",  # owner_identifier removed
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
        herd_owners_final = herd_owners.select(
            *[col for col in final_cols if col in herd_owners.columns]
        )  # Select available columns

        # Save to parquet
        output_path = silver_dir / "herd_owners.parquet"
        rows = herd_owners_final.count().execute()
        if rows == 0:
            logging.warning("Herd owners table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_owners table with attributes ({rows} rows).")
        saved_path = export.save_table(output_path, herd_owners_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save herd_owners table - no path returned")
            return None
        logging.info(f"Saved herd_owners table to {saved_path}")

        return herd_owners_final

    except Exception as e:
        logging.error(f"Failed to create herd_owners table with attributes: {e}", exc_info=True)
        return None


def create_herd_users_table(
    con: ibis.BaseBackend, bes_details_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the herd_users table including attributes from besaetning details.

    Args:
        con: The database connection
        bes_details_raw: Raw besaetning details table
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_users table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_users table with attributes.")

    try:
        # Extract herd_number, user_identifier, and user attributes
        herd_users = con.sql("""
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM bes_details -- Assumes bes_details_raw is registered as 'bes_details'
            )
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
              AND r.Besaetning.Bruger IS NOT NULL -- Ensure Bruger struct exists
              AND (
                  (r.Besaetning.Bruger.CvrNummer IS NOT NULL OR r.Besaetning.Bruger.CprNummer IS NOT NULL)
                  OR
                  (r.Besaetning.Bruger.Navn IS NOT NULL AND r.Besaetning.Bruger.Adresse IS NOT NULL AND r.Besaetning.Bruger.PostNummer IS NOT NULL)
              )
        """)

        # Clean and cast columns
        herd_users = herd_users.mutate(
            herd_number=ibis.coalesce(
                herd_users.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            # user_identifier removed
            user_cvr=herd_users.user_cvr_raw.cast(dt.string).strip().nullif(""),
            user_cpr=herd_users.user_cpr_raw.cast(dt.string).strip().nullif(""),
            user_name=herd_users.user_name_raw.cast(dt.string).strip().nullif(""),
            user_address=herd_users.user_address_raw.cast(dt.string).strip().nullif(""),
            user_postal_code=herd_users.user_postal_code_raw.cast(dt.string).strip().nullif(""),
            user_postal_district=herd_users.user_postal_district_raw.cast(dt.string).strip().nullif(""),
            user_city=herd_users.user_city_raw.cast(dt.string).strip().nullif(""),
            user_municipality_code=ibis.coalesce(
                herd_users.user_municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            user_municipality_name=herd_users.user_municipality_name_raw.cast(dt.string).strip().nullif(""),
            user_country=herd_users.user_country_raw.cast(dt.string).strip().nullif(""),
            user_phone=herd_users.user_phone_raw.cast(dt.string).strip().nullif(""),
            user_mobile=herd_users.user_mobile_raw.cast(dt.string).strip().nullif(""),
            user_email=herd_users.user_email_raw.cast(dt.string).strip().nullif(""),
            user_address_protection=herd_users.user_address_protection_raw.cast(dt.string).strip().nullif(""),
            user_advertising_protection=herd_users.user_advertising_protection_raw.cast(dt.string).strip().nullif(""),
        )

        # Filter out rows with null herd_number or (now removed) user_identifier after cleaning
        # Filter only on herd_number now, as identifier is removed. The SQL WHERE clause already filters unidentifiable users.
        herd_users = herd_users.filter(herd_users.herd_number.notnull())

        # Define final columns order
        final_cols = [
            "herd_number",  # user_identifier removed
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
        herd_users_final = herd_users.select(*[col for col in final_cols if col in herd_users.columns])

        # Save to parquet
        output_path = silver_dir / "herd_users.parquet"
        rows = herd_users_final.count().execute()
        if rows == 0:
            logging.warning("Herd users table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_users table with attributes ({rows} rows).")
        saved_path = export.save_table(output_path, herd_users_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save herd_users table - no path returned")
            return None
        logging.info(f"Saved herd_users table to {saved_path}")

        return herd_users_final

    except Exception as e:
        logging.error(f"Failed to create herd_users table with attributes: {e}", exc_info=True)
        return None


def create_herd_sizes_table(
    con: ibis.BaseBackend, bes_details_raw: Optional[ibis.Table], silver_dir: Path
) -> Optional[ibis.Table]:
    """Creates the herd_sizes table from the nested BesStr list in besaetning details.

    Args:
        con: The database connection
        bes_details_raw: Raw besaetning details table
        silver_dir: Directory to save the output
    """
    if bes_details_raw is None:
        logging.warning("Skipping herd_sizes table creation: bes_details_raw is None.")
        return None

    logging.info("Starting creation of herd_sizes table.")

    try:
        # Extract herd_number and size information
        herd_sizes = con.sql("""
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM bes_details -- Assumes bes_details_raw is registered as 'bes_details'
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
            )
            SELECT DISTINCT
                herd_number_raw,
                chr_number_raw,
                species_code_raw,
                species_name_raw,
                size_info.BesaetningsStoerrelseTekst AS category_raw,
                size_info.BesaetningsStoerrelse AS count_raw,
                size_update_date_raw
            FROM unnested_sizes
        """)

        # Clean and cast columns
        herd_sizes = herd_sizes.mutate(
            size_id=ibis.uuid(),
            herd_number=ibis.coalesce(
                herd_sizes.herd_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            chr_number=ibis.coalesce(
                herd_sizes.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            species_code=ibis.coalesce(
                herd_sizes.species_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            species_name=herd_sizes.species_name_raw.cast(dt.string).strip().nullif(""),
            category=herd_sizes.category_raw.cast(dt.string).strip().nullif(""),
            count=ibis.coalesce(
                herd_sizes.count_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            size_update_date=ibis.coalesce(
                herd_sizes.size_update_date_raw.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
        )

        # Filter out rows with null herd_number after cleaning
        herd_sizes = herd_sizes.filter(herd_sizes.herd_number.notnull())

        # Define final columns order
        final_cols = [
            "size_id",
            "herd_number",
            "chr_number",
            "species_code",
            "species_name",
            "category",
            "count",
            "size_update_date",
        ]
        herd_sizes_final = herd_sizes.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "herd_sizes.parquet"
        rows = herd_sizes_final.count().execute()
        if rows == 0:
            logging.warning("Herd sizes table is empty after processing. Not saving file.")
            return None

        logging.info(f"Saving herd_sizes table with {rows} rows.")
        saved_path = export.save_table(output_path, herd_sizes_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save herd_sizes table - no path returned")
            return None
        logging.info(f"Saved herd_sizes table to {saved_path}")

        return herd_sizes_final

    except Exception as e:
        logging.error(f"Failed to create herd_sizes table: {e}", exc_info=True)
        return None
