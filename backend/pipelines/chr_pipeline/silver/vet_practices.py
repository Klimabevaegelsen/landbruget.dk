import logging

import ibis
import ibis.expr.datatypes as dt

# Import export module
from . import export


def create_vet_practices_table(con, bes_details_raw, silver_dir):
    """Creates the vet_practices table from besaetning details."""
    if bes_details_raw is None:
        return None

    logging.info("Starting creation of vet_practices table.")

    # Extract vet practice information
    vet_practices = con.sql("""
        WITH unnested_response AS (
            SELECT UNNEST(Response) AS response_item
            FROM bes_details
            WHERE Response IS NOT NULL
        )
        SELECT DISTINCT -- Ensure final distinctness
            response_item.Besaetning.BesPraksis.PraksisNavn AS practice_name,
            response_item.Besaetning.BesPraksis.PraksisAdresse AS address,
            CAST(response_item.Besaetning.BesPraksis.PraksisPostNummer AS STRING) AS postal_code,
            -- Cast PostNummer to string
            response_item.Besaetning.BesPraksis.PraksisPostDistrikt AS postal_district,
            response_item.Besaetning.BesPraksis.PraksisTelefonNummer AS phone,
            response_item.Besaetning.BesPraksis.PraksisMobilNummer AS mobile,
            response_item.Besaetning.BesPraksis.PraksisEmail AS email,
            response_item.Besaetning.BesPraksis.PraksisNr AS practice_number, -- Adding PraksisNr as it might be useful
            response_item.Besaetning.BesPraksis.PraksisByNavn AS city -- Adding City Name
        FROM unnested_response
        WHERE response_item.Besaetning.BesPraksis.PraksisNr IS NOT NULL
        -- Filter out null practice numbers after extraction
    """)

    # Add cleaning/casting using mutate
    vet_practices = vet_practices.mutate(
        practice_name=vet_practices.practice_name.cast(dt.string).strip().nullif(""),
        address=vet_practices.address.cast(dt.string).strip().nullif(""),
        postal_code=vet_practices.postal_code.cast(dt.string).strip().nullif(""),  # Already string from CAST
        postal_district=vet_practices.postal_district.cast(dt.string).strip().nullif(""),
        phone=vet_practices.phone.cast(dt.string).strip().nullif(""),
        mobile=vet_practices.mobile.cast(dt.string).strip().nullif(""),
        email=vet_practices.email.cast(dt.string).strip().nullif(""),
        practice_number=ibis.coalesce(
            vet_practices.practice_number.cast(dt.string).strip().nullif("").cast(dt.int64),
            ibis.null().cast(dt.int64),
        ),  # Cast to int64
        city=vet_practices.city.cast(dt.string).strip().nullif(""),
    )

    # Define final columns order
    final_cols = [
        "practice_number",
        "practice_name",
        "address",
        "city",
        "postal_code",
        "postal_district",
        "phone",
        "mobile",
        "email",
    ]
    vet_practices_final = vet_practices.select(*[col for col in final_cols if col in vet_practices.columns])

    # Save to parquet
    output_path = silver_dir / "vet_practices.parquet"
    rows = vet_practices_final.count().execute()
    if rows == 0:
        logging.warning("Vet practices table is empty after processing. Not saving file.")
        return None

    logging.info(f"Saving vet_practices table with {rows} rows.")
    # ✅ MIGRATION: Pass Ibis table directly instead of executing to pandas
    saved_path = export.save_table(output_path, vet_practices_final, is_geo=False)
    if saved_path is None:
        logging.error("Failed to save vet_practices table - no path returned")
        return None
    logging.info(f"Saved vet_practices table to {saved_path}")

    return vet_practices_final  # Return the final table
