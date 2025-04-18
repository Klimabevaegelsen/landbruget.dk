import ibis
import ibis.expr.datatypes as dt
import logging
from pathlib import Path

# Import helpers
from .helpers import _sanitize_string

def create_herd_sizes_table(con, bes_details_raw, silver_dir):
    """Creates the herd_sizes table from besaetning details."""
    if bes_details_raw is None:
        return None

    logging.info("Starting creation of herd_sizes table.")

    try:
        # --- Query to extract herd size information ---
        herd_sizes = con.sql("""
            WITH unnested_response AS (
                SELECT UNNEST(Response) AS r
                FROM bes_details
            )
            SELECT DISTINCT
                r.Besaetning.BesaetningsNummer AS herd_number_raw,
                r.Besaetning.BesStrDatoAjourfoert AS size_date_raw,
                size_info.unnest.BesaetningsStoerrelseTekst AS size_description_raw,
                size_info.unnest.BesaetningsStoerrelse AS count_raw
            FROM unnested_response,
                 UNNEST(r.Besaetning.BesStr) AS size_info(unnest)
            WHERE r.Besaetning.BesaetningsNummer IS NOT NULL
              AND r.Besaetning.BesStr IS NOT NULL
              AND size_info.unnest.BesaetningsStoerrelseTekst IS NOT NULL
              AND size_info.unnest.BesaetningsStoerrelse IS NOT NULL
        """)

        # Clean and cast columns
        herd_sizes = herd_sizes.mutate(
            herd_number=ibis.coalesce(herd_sizes.herd_number_raw.cast(dt.string).pipe(_sanitize_string).cast(dt.int64), ibis.null().cast(dt.int64)),
            size_date=ibis.coalesce(herd_sizes.size_date_raw.cast(dt.string).pipe(_sanitize_string).cast(dt.date), ibis.null().cast(dt.date)),
            size_description=_sanitize_string(herd_sizes.size_description_raw),
            count=ibis.coalesce(herd_sizes.count_raw.cast(dt.string).pipe(_sanitize_string).cast(dt.int32), ibis.null().cast(dt.int32)) # Assuming count is integer
        )

        # Select final columns
        final_cols = ['herd_number', 'size_date', 'size_description', 'count']
        herd_sizes_final = herd_sizes.select(*final_cols)

        # Save to parquet
        output_path = silver_dir / "herd_sizes.parquet"
        rows = herd_sizes_final.count().execute()
        if rows == 0:
            logging.warning("Herd sizes table is empty after processing.")
            return None # Still return None if empty

        logging.info(f"Saving herd_sizes table with {rows} rows.")
        herd_sizes_final.to_parquet(output_path)
        logging.info(f"Saved herd_sizes table to {output_path}")

        return herd_sizes_final

    except Exception as e:
        logging.error(f"Failed to create herd_sizes table: {e}", exc_info=True)
        return None 