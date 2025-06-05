import logging
from pathlib import Path

import ibis
import ibis.expr.datatypes as dt
import pandas as pd  # Import pandas for empty df case

# Import export module
from . import export

# Import helpers
# from .helpers import _sanitize_string


# --- Antibiotic Usage Table ---
def create_antibiotic_usage_table(
    con: ibis.BaseBackend,
    vetstat_raw: ibis.Table | None,
    lookup_tables: dict[str, ibis.Table | None],
    silver_dir: Path,
) -> ibis.Table | None:
    """Creates the antibiotic_usage table from parsed VetStat data."""
    logging.info("Starting creation of antibiotic_usage table.")
    if vetstat_raw is None:
        logging.warning("Cannot create antibiotic_usage: vetstat_raw table missing. Creating empty table with schema.")
        # Create an empty antibiotic_usage table with the correct schema
        empty_data = {
            "entity_id": [],
            "cvr_number": [],
            "chr_number": [],
            "year": [],
            "month": [],
            "species_code": [],
            "age_group_code": [],
            "avg_usage_rolling_9m": [],
            "avg_usage_rolling_12m": [],
            "animal_days": [],
            "animal_doses": [],
            "add_per_100_dyr_per_dag": [],
            "limit_value": [],
            "municipality_code": [],
            "municipality_name": [],
            "region_code": [],
            "region_name": [],
        }
        empty_df = pd.DataFrame(empty_data)
        output_path = silver_dir / "antibiotic_usage.parquet"
        saved_path = export.save_table(output_path, empty_df, is_geo=False)
        if saved_path is None:
            logging.error("Failed to save empty antibiotic_usage table - no path returned")
            return None
        logging.info(f"Saved empty antibiotic_usage table with schema to {saved_path}")
        return None

    # --- START Check if vetstat_raw has columns ---
    if not vetstat_raw.columns:
        logging.warning(
            "Cannot create antibiotic_usage: vetstat_raw table exists but has no columns (likely empty source). Creating empty table with schema."
        )
        # Reuse the empty table creation logic from the None check
        empty_data = {
            "entity_id": [],
            "cvr_number": [],
            "chr_number": [],
            "year": [],
            "month": [],
            "species_code": [],
            "age_group_code": [],
            "avg_usage_rolling_9m": [],
            "avg_usage_rolling_12m": [],
            "animal_days": [],
            "animal_doses": [],
            "add_per_100_dyr_per_dag": [],
            "limit_value": [],
            "municipality_code": [],
            "municipality_name": [],
            "region_code": [],
            "region_name": [],
        }
        empty_df = pd.DataFrame(empty_data)
        output_path = silver_dir / "antibiotic_usage.parquet"
        saved_path = export.save_table(output_path, empty_df, is_geo=False)
        if saved_path is None:
            logging.error("Failed to save empty antibiotic_usage table - no path returned")
            return None
        logging.info(f"Saved empty antibiotic_usage table with schema to {saved_path}")
        return None
    # --- END Check ---

    # Define source -> target mapping (adjust based on actual parsed XML->JSONL structure)
    usage_cols = {
        "CVRNummer": "cvr_number_raw",
        "CHRNummer": "chr_number_raw",
        "Aar": "year_raw",
        "Maaned": "month_raw",
        "DyreArtKode": "species_code_raw",  # FK
        "Aldersgruppekode": "age_group_code_raw",  # FK
        "Rul9MdrGns": "avg_usage_rolling_9m_raw",  # Corrected source key
        "Rul12MdrGns": "avg_usage_rolling_12m_raw",  # Corrected source key
        "Dyredage": "animal_days_raw",  # Corrected source key
        "Dyredoser": "animal_doses_raw",  # Corrected source key
        "ADDPer100DyrPerDag": "add_per_100_dyr_per_dag_raw",  # Corrected source key
        "Graensevaerdi": "limit_value_raw",
        "Kommunenr": "municipality_code_raw",
        "Kommunenavn": "municipality_name_raw",
        "Regionsnr": "region_code_raw",
        "Regionsnavn": "region_name_raw",
        # Add more fields if available and needed
    }
    available_source_cols = vetstat_raw.columns

    try:
        # Select and rename base columns
        usage_base = vetstat_raw.select(
            **{
                target: vetstat_raw[source].name(target)
                for source, target in usage_cols.items()
                if source in available_source_cols
            }
        )

        # Add null columns if source was missing
        for target in usage_cols.values():
            if target not in usage_base.columns:
                usage_base = usage_base.mutate(**{target: ibis.null()})
                logging.warning(f"Column for '{target}' missing in source vetstat data, adding as null.")

        # == START: Load entities table (REMOVED) ==
        # entities_table = None
        # entities_table_path = silver_dir / "entities.parquet"
        # if entities_table_path.exists():
        #     try:
        #         entities_table = con.read_parquet(str(entities_table_path))
        #         # Create a view for joining to potentially avoid issues with direct file reads in joins
        #         con.create_view('entities_for_join_view', entities_table, overwrite=True)
        #         logging.info("Loaded entities table and created view for joining.")
        #     except Exception as e_read:
        #         logging.warning(f"Could not read entities table from {entities_table_path}: {e_read}")
        #         entities_table = None # Ensure it's None if read fails
        # else:
        #     logging.warning(f"Entities table not found at {entities_table_path}, cannot join for entity_id.")
        # == END: Load entities table (REMOVED) ==

        # Generate UUID and clean/cast
        # Use ibis.coalesce for safe casting, especially for numerics
        usage_cleaned = usage_base.mutate(
            # Replace ibis.sql('uuid()') with ibis.uuid()
            usage_id=ibis.uuid(),
            cvr_number=usage_base.cvr_number_raw.cast(dt.string).strip().nullif(""),  # Keep as string FK for now
            chr_number=ibis.coalesce(
                usage_base.chr_number_raw.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),  # FK
            year=ibis.coalesce(
                usage_base.year_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            month=ibis.coalesce(
                usage_base.month_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            species_code=ibis.coalesce(
                usage_base.species_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),  # FK
            age_group_code=ibis.coalesce(
                usage_base.age_group_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),  # FK
            avg_usage_rolling_9m=ibis.coalesce(
                usage_base.avg_usage_rolling_9m_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            avg_usage_rolling_12m=ibis.coalesce(
                usage_base.avg_usage_rolling_12m_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            animal_days=ibis.coalesce(
                usage_base.animal_days_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),  # Or decimal
            animal_doses=ibis.coalesce(
                usage_base.animal_doses_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),  # Or decimal
            add_per_100_dyr_per_dag=ibis.coalesce(
                usage_base.add_per_100_dyr_per_dag_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            limit_value=ibis.coalesce(
                usage_base.limit_value_raw.cast(dt.string).strip().nullif("").cast(dt.float64),
                ibis.null().cast(dt.float64),
            ),
            municipality_code=ibis.coalesce(
                usage_base.municipality_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            municipality_name=usage_base.municipality_name_raw.cast(dt.string).strip().nullif(""),
            region_code=ibis.coalesce(
                usage_base.region_code_raw.cast(dt.string).strip().nullif("").cast(dt.int32),
                ibis.null().cast(dt.int32),
            ),
            region_name=usage_base.region_name_raw.cast(dt.string).strip().nullif(""),
        )

        # Join with lookups (optional, as with vet_events)
        if "species" in lookup_tables and lookup_tables["species"] is not None:
            usage_cleaned = usage_cleaned.left_join(lookup_tables["species"], ["species_code"])
        if "age_groups" in lookup_tables and lookup_tables["age_groups"] is not None:
            # Rename the column before joining
            usage_cleaned = usage_cleaned.mutate(age_groups_code=usage_cleaned.age_group_code)
            usage_cleaned = usage_cleaned.left_join(lookup_tables["age_groups"], ["age_groups_code"])
            # Drop the temporary column after join if needed
            usage_cleaned = usage_cleaned.drop("age_groups_code")

        # Optional: Join with entities table to get entity_id (REMOVED)
        # if entities_table is not None:
        #     # Reference the view using .table()
        #     entities_for_join = con.table('entities_for_join_view').select(
        #         cvr_entities=ibis._.cvr, # Select the CVR column
        #         entity_id=ibis._.entity_id
        #     ).filter(ibis._.cvr_entities.notnull()) # Ensure CVR is not null for joining
        #
        #     usage_cleaned = usage_cleaned.left_join(
        #         entities_for_join,
        #         usage_cleaned.cvr_number == entities_for_join.cvr_entities # Join on CVR number
        #     )
        #     logging.info("Joined antibiotic usage with entities table on CVR number.")
        # else:
        #     # Add a null entity_id column if join couldn't happen
        #     if 'entity_id' not in usage_cleaned.columns:
        #          usage_cleaned = usage_cleaned.mutate(entity_id=ibis.null().cast(dt.string))
        #          logging.warning("Adding null entity_id column as entities table was not available for join.")

        # Select final columns in desired order
        final_cols = [
            "usage_id",
            "cvr_number",
            "chr_number",
            "year",
            "month",
            "species_code",
            "age_group_code",
            "avg_usage_rolling_9m",
            "avg_usage_rolling_12m",
            "animal_days",
            "animal_doses",
            "add_per_100_dyr_per_dag",
            "limit_value",
            "municipality_code",
            "municipality_name",
            "region_code",
            "region_name",
            # Add joined name columns if included: 'species_name', 'age_group_name'
            # Add 'entity_id' if joined or added as null (REMOVED)
        ]
        # Insert entity_id if it exists (REMOVED)
        # if 'entity_id' in usage_cleaned.columns:
        #      final_cols.insert(1, 'entity_id')

        # Ensure all selected columns exist, adding nulls if they don't
        final_select_cols = {}
        for col in final_cols:
            if col in usage_cleaned.columns:
                final_select_cols[col] = usage_cleaned[col]
            else:
                # Infer type or default to string/null
                col_type = dt.string  # Default
                # Removed entity_id specific type inference
                # Add more type inference if needed
                final_select_cols[col] = ibis.null().cast(col_type).name(col)
                logging.warning(f"Column '{col}' missing after processing/joins, adding as null.")

        usage_final = usage_cleaned.select(**final_select_cols)

        # --- Save to Parquet ---
        output_path = silver_dir / "antibiotic_usage.parquet"
        rows = usage_final.count().execute()
        if rows == 0:
            logging.warning("Antibiotic usage table is empty after processing.")
            return None

        logging.info(f"Saving antibiotic_usage table with {rows} rows.")
        saved_path = export.save_table(output_path, usage_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save antibiotic_usage table - no path returned")
            return None
        logging.info(f"Saved antibiotic_usage table to {saved_path}")

        return usage_final

    except Exception as e:
        logging.error(f"Failed to create antibiotic_usage table: {e}", exc_info=True)
        # Clean up entities view if loaded (REMOVED)
        # try:
        #      if entities_table is not None:
        #          con.drop_view('entities_for_join_view', force=True)
        # except Exception:
        #     pass
        return None
    finally:
        # Ensure cleanup happens even on success (REMOVED entity view cleanup)
        # try:
        #      if entities_table is not None:
        #          con.drop_view('entities_for_join_view', force=True)
        # except Exception:
        #     pass
        pass  # Keep finally block but remove specific cleanup
