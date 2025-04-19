import ibis
import ibis.expr.datatypes as dt
import logging
from pathlib import Path
from typing import Optional

# Import helpers (assuming helpers.py is in the same directory)
from .helpers import _sanitize_string

# Import export module
from . import export


def create_animal_movements_table(con: ibis.BaseBackend, diko_flyt_raw: ibis.Table | None, silver_dir: Path) -> ibis.Table | None:
    """Creates the animal_movements table from the nested Flytninger list in diko_flytninger."""
    logging.info("Starting creation of animal_movements table.")
    
    # Check for nested structure
    if diko_flyt_raw is None or 'Response' not in diko_flyt_raw.columns:
        logging.warning("Cannot create animal_movements: 'Response' column missing in diko_flyt_raw.")
        return None

    try:
        # Access the nested structure Response[0]
        if not isinstance(diko_flyt_raw['Response'].type(), dt.Array):
             logging.warning(f"Cannot create animal_movements: 'Response' column is not an Array (Type: {diko_flyt_raw['Response'].type()}). Skipping.")
             return None
        # Define path
        response_struct_path = diko_flyt_raw.Response[0]

        # Check for required fields within the response struct path
        if not isinstance(response_struct_path.type(), dt.Struct) or 'BesaetningsNummer' not in response_struct_path.type().names or 'Flytninger' not in response_struct_path.type().names:
            logging.warning("Cannot create animal_movements: Missing 'BesaetningsNummer' or 'Flytninger' in Response[0] path. Skipping.")
            return None

        # Check Flytninger is array using path
        if not isinstance(response_struct_path.Flytninger.type(), dt.Array):
             logging.warning(f"Cannot create animal_movements: Response[0].Flytninger path is not an Array (Type: {response_struct_path.Flytninger.type()}). Skipping.")
             return None

        # Select base fields and the list to unnest using path
        base = diko_flyt_raw.select( # Select from base table
             reporting_herd_number_raw=response_struct_path.BesaetningsNummer,
             flytninger_list=response_struct_path.Flytninger
        )
        
        # Filter before unnesting
        base = base.filter(base.flytninger_list.notnull())

        # Unnest the Flytninger list.
        unpacked = base.select(
            reporting_herd_number=base.reporting_herd_number_raw,
            movement_info=base.flytninger_list.unnest() # movement_info is a struct
        )
        
        # Filter after unnesting
        unpacked = unpacked.filter(unpacked.movement_info.notnull())

        # Define source -> target mapping for fields inside the movement_info struct
        # Using field names from the head output
        movement_cols = {
            # 'IndberetningsBesaetning' is handled above
            'FlytteDato': 'movement_date',
            'KontaktType': 'contact_type', # 'Til' or 'Fra'
            'ChrNummer': 'counterparty_chr_number', # Was ModpartCHRnr
            'BesaetningsNummer': 'counterparty_herd_number', # Was ModpartBesaetningsnr
            'VirksomhedsArt': 'counterparty_business_type' # Was ModpartForretningstype
        }
        available_struct_cols = unpacked.movement_info.type().names

        movements = unpacked.select(
            reporting_herd_number=unpacked.reporting_herd_number, # Carry reporting herd number through
            **{target: unpacked.movement_info[source].name(target)
               for source, target in movement_cols.items()
               if source in available_struct_cols}
        )

        # Add null columns if source was missing
        for target in movement_cols.values():
            if target not in movements.columns:
                movements = movements.mutate(**{target: ibis.null()})
                logging.warning(f"Column for '{target}' missing in source Flytninger struct element, adding as null.")

        # Generate UUID and clean/cast
        movements = movements.mutate(
            # Replace ibis.sql('uuid()') with ibis.uuid()
            movement_id=ibis.uuid(), # Generate UUID
            reporting_herd_number=ibis.coalesce(movements.reporting_herd_number.cast(dt.string).pipe(_sanitize_string).cast(dt.int64), ibis.null().cast(dt.int64)), # FK
            movement_date=ibis.coalesce(movements.movement_date.cast(dt.string).pipe(_sanitize_string).cast(dt.date), ibis.null().cast(dt.date)),
            contact_type=_sanitize_string(movements.contact_type),
            counterparty_chr_number=ibis.coalesce(movements.counterparty_chr_number.cast(dt.string).pipe(_sanitize_string).cast(dt.int64), ibis.null().cast(dt.int64)),
            counterparty_herd_number=ibis.coalesce(movements.counterparty_herd_number.cast(dt.string).pipe(_sanitize_string).cast(dt.int64), ibis.null().cast(dt.int64)),
            counterparty_business_type=_sanitize_string(movements.counterparty_business_type)
        )

        # Select final columns in desired order
        final_cols = [
            'movement_id', 'reporting_herd_number', 'movement_date', 'contact_type',
            'counterparty_chr_number', 'counterparty_herd_number', 'counterparty_business_type'
        ]
        movements_final = movements.select(*final_cols)

        # --- Save to Parquet ---
        output_path = silver_dir / "animal_movements.parquet"
        rows = movements_final.count().execute()
        if rows == 0:
            logging.warning("Animal movements table is empty after processing.")
            return None

        logging.info(f"Saving animal_movements table with {rows} rows.")
        saved_path = export.save_table(output_path, movements_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save animal_movements table - no path returned")
            return None
        logging.info(f"Saved animal_movements table to {saved_path}")

        return movements_final

    except Exception as e:
        logging.error(f"Failed to create animal_movements table: {e}", exc_info=True)
        return None 