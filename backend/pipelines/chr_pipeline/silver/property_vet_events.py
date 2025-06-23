import logging
from pathlib import Path

import ibis
import ibis.expr.datatypes as dt

# Import export module
from . import export


# --- Property Vet Events Table ---
def create_property_vet_events_table(
    con: ibis.BaseBackend,
    ejendom_vet_raw: ibis.Table | None,
    lookup_tables: dict[str, ibis.Table | None],
    silver_dir: Path,
) -> ibis.Table | None:
    """Creates the property_vet_events table from the nested structure in ejendom_vet_events."""
    logging.info("Starting creation of property_vet_events table.")

    # Check for nested structure Response.VeterinaereHaendelser
    if ejendom_vet_raw is None or "Response" not in ejendom_vet_raw.columns:
        logging.warning("Cannot create property_vet_events: 'Response' column missing in ejendom_vet_raw.")
        return None

    try:
        # Register the table with DuckDB for SQL operations
        con.create_table("ejendom_vet_raw", ejendom_vet_raw, overwrite=True)

        # Extract base columns and unnest the event list using SQL
        base = con.sql("""
            SELECT
                CAST(Response.ChrNummer AS STRING) AS chr_number_raw,
                Response.VeterinaereHaendelser.VeterinaereProblemer AS has_vet_problems_raw,
                UNNEST(Response.VeterinaereHaendelser.VeterinaerHaendelse) AS event_info
            FROM ejendom_vet_raw
            WHERE Response.VeterinaereHaendelser.VeterinaerHaendelse IS NOT NULL
        """)

        # == START Fix: Register base table as view ==
        try:
            con.create_view("base", base, overwrite=True)
        except Exception as e_view:
            logging.error(f"Failed to create temporary view 'base': {e_view}")
            raise  # Re-raise to stop processing if view creation fails
        # == END Fix ==

        # Define source -> target mapping for fields inside the event_info struct
        event_cols = {
            "DyreArtKode": "species_code",
            "DyreArtTekst": "species_name",
            "SygdomsKode": "disease_code",
            "SygdomsTekst": "disease_name",
            "VeterinaerStatusKode": "vet_status_code",
            "VeterinaerStatusTekst": "vet_status_name",
            "SygdomsNiveauKode": "disease_level_code",
            "SygdomsNiveauTekst": "disease_level_name",
            "DatoVeterinaerStatus": "vet_status_date",
            "VeterinaerHaendelseBemaerkning": "remark",
        }

        # Extract event fields
        vet_events = con.sql(f"""
            SELECT
                CAST(chr_number_raw AS STRING) AS chr_number_raw,
                has_vet_problems_raw,
                {", ".join(f"event_info.{source} AS {target}" for source, target in event_cols.items())}
            FROM base
            WHERE event_info IS NOT NULL
        """)

        # Generate UUID and clean/cast
        vet_events = vet_events.mutate(
            event_id=ibis.uuid(),
            chr_number=ibis.coalesce(
                vet_events.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            has_vet_problems=(vet_events.has_vet_problems_raw.cast(dt.string).strip().nullif("").lower() == "ja"),
            species_code=ibis.coalesce(
                vet_events.species_code.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            species_name=vet_events.species_name.cast(dt.string).strip().nullif(""),
            disease_code=vet_events.disease_code.cast(dt.string).strip().nullif(""),
            disease_name=vet_events.disease_name.cast(dt.string).strip().nullif(""),
            vet_status_code=vet_events.vet_status_code.cast(dt.string).strip().nullif(""),
            vet_status_name=vet_events.vet_status_name.cast(dt.string).strip().nullif(""),
            disease_level_code=vet_events.disease_level_code.cast(dt.string).strip().nullif(""),
            disease_level_name=vet_events.disease_level_name.cast(dt.string).strip().nullif(""),
            vet_status_date=ibis.coalesce(
                vet_events.vet_status_date.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            remark=vet_events.remark.cast(dt.string).strip().nullif(""),
        )

        # Join with lookups (optional)
        if "species" in lookup_tables and lookup_tables["species"] is not None:
            vet_events = vet_events.left_join(lookup_tables["species"], ["species_code"])
        if "diseases" in lookup_tables and lookup_tables["diseases"] is not None:
            vet_events = vet_events.left_join(lookup_tables["diseases"], ["disease_code"])
        if "vet_statuses" in lookup_tables and lookup_tables["vet_statuses"] is not None:
            vet_events = vet_events.left_join(lookup_tables["vet_statuses"], ["vet_status_code"])

        # Select final columns
        final_cols = [
            "event_id",
            "chr_number",
            "species_code",
            "species_name",
            "disease_code",
            "disease_name",
            "disease_level_code",
            "disease_level_name",
            "vet_status_code",
            "vet_status_name",
            "vet_status_date",
            "has_vet_problems",
            "remark",
        ]
        vet_events_final = vet_events.select(*final_cols)

        # --- Save to Parquet ---
        output_path = silver_dir / "property_vet_events.parquet"
        rows = vet_events_final.count().execute()
        if rows == 0:
            logging.warning("Property vet events table is empty after processing.")
            return None

        logging.info(f"Saving property_vet_events table with {rows} rows.")
        saved_path = export.save_table(output_path, vet_events_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save property_vet_events table - no path returned")
            return None
        logging.info(f"Saved property_vet_events table to {saved_path}")

        # Clean up
        try:
            con.drop_table("ejendom_vet_raw", force=True)
            con.drop_view("base", force=True)  # Add cleanup for the view
        except Exception:
            pass

        return vet_events_final

    except Exception as e:
        logging.error(f"Failed to create property_vet_events table: {e}", exc_info=True)
        try:
            con.drop_table("ejendom_vet_raw", force=True)
            con.drop_view("base", force=True)  # Add cleanup for the view in error case
        except Exception:
            pass
        return None
