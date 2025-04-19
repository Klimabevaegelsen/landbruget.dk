import ibis
import ibis.expr.datatypes as dt
import logging
from pathlib import Path

# Import helpers
from .helpers import _sanitize_string

# Import export module
from . import export

def create_vet_practices_table(con, bes_details_raw, silver_dir):
    """Creates the vet_practices table from besaetning details."""
    if bes_details_raw is None:
        return None

    logging.info("Starting creation of vet_practices table.")

    # Extract vet practice information
    vet_practices = con.sql("""
        WITH BesPraksisInfo AS (
            SELECT DISTINCT
                Response[1].Besaetning.BesPraksis AS PraksisInfo
            FROM bes_details
            WHERE Response[1].Besaetning.BesPraksis IS NOT NULL
        )
        SELECT DISTINCT -- Ensure final distinctness
            PraksisInfo.PraksisNavn AS practice_name,
            PraksisInfo.PraksisAdresse AS address,
            CAST(PraksisInfo.PraksisPostNummer AS STRING) AS postal_code, -- Cast PostNummer to string
            PraksisInfo.PraksisPostDistrikt AS postal_district,
            PraksisInfo.PraksisTelefonNummer AS phone,
            PraksisInfo.PraksisMobilNummer AS mobile,
            PraksisInfo.PraksisEmail AS email,
            PraksisInfo.PraksisNr AS practice_number, -- Adding PraksisNr as it might be useful
            PraksisInfo.PraksisByNavn AS city -- Adding City Name
        FROM BesPraksisInfo
        WHERE PraksisInfo.PraksisNr IS NOT NULL -- Filter out null practice numbers after extraction
    """)

    # Add cleaning/casting using mutate
    vet_practices = vet_practices.mutate(
        practice_name=_sanitize_string(vet_practices.practice_name),
        address=_sanitize_string(vet_practices.address),
        postal_code=_sanitize_string(vet_practices.postal_code), # Already string from CAST
        postal_district=_sanitize_string(vet_practices.postal_district),
        phone=_sanitize_string(vet_practices.phone),
        mobile=_sanitize_string(vet_practices.mobile),
        email=_sanitize_string(vet_practices.email),
        practice_number=ibis.coalesce(vet_practices.practice_number.cast(dt.string).pipe(_sanitize_string).cast(dt.int64), ibis.null().cast(dt.int64)), # Cast to int64
        city=_sanitize_string(vet_practices.city)
    )

    # Define final columns order
    final_cols = [
        'practice_number', 'practice_name',
        'address', 'city', 'postal_code', 'postal_district',
        'phone', 'mobile', 'email'
    ]
    vet_practices_final = vet_practices.select(*[col for col in final_cols if col in vet_practices.columns])

    # Save to parquet
    output_path = silver_dir / "vet_practices.parquet"
    rows = vet_practices_final.count().execute()
    if rows == 0:
        logging.warning("Vet practices table is empty after processing. Not saving file.")
        return None

    logging.info(f"Saving vet_practices table with {rows} rows.")
    saved_path = export.save_table(output_path, vet_practices_final.execute(), is_geo=False)
    if saved_path is None:
        logging.error("Failed to save vet_practices table - no path returned")
        return None
    logging.info(f"Saved vet_practices table to {saved_path}")

    return vet_practices_final # Return the final table 