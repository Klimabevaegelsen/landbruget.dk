import logging
from pathlib import Path

import ibis
import ibis.expr.datatypes as dt

# Import export module
from . import export

# Import helpers (assuming helpers.py is in the same directory)


def create_chr_dyr_animal_movements_table(
    con: ibis.BaseBackend, chr_dyr_raw: ibis.Table | None, silver_dir: Path
) -> ibis.Table | None:
    """Creates the chr_dyr_animal_movements table from CHR_dyr service besListAktOms responses."""
    logging.info("Starting creation of chr_dyr_animal_movements table.")

    # Check for nested structure
    if chr_dyr_raw is None or "Response" not in chr_dyr_raw.columns:
        logging.warning("Cannot create chr_dyr_animal_movements: 'Response' column missing in chr_dyr_raw.")
        return None

    try:
        # Access the nested structure Response[0]
        if not isinstance(chr_dyr_raw["Response"].type(), dt.Array):
            logging.warning(
                f"Cannot create chr_dyr_animal_movements: 'Response' column is not an Array (Type: {chr_dyr_raw['Response'].type()}). Skipping."
            )
            return None

        response_struct_path = chr_dyr_raw.Response[0]

        # Check for required fields within the response struct path
        if (
            not isinstance(response_struct_path.type(), dt.Struct)
            or "BesaetningsNummer" not in response_struct_path.type().names
            or "Enkeltdyrsoplysninger" not in response_struct_path.type().names
        ):
            logging.warning(
                "Cannot create chr_dyr_animal_movements: Missing 'BesaetningsNummer' or 'Enkeltdyrsoplysninger' in Response[0] path. Skipping."
            )
            return None

        # Check Enkeltdyrsoplysninger is array using path
        if not isinstance(response_struct_path.Enkeltdyrsoplysninger.type(), dt.Array):
            logging.warning(
                f"Cannot create chr_dyr_animal_movements: Response[0].Enkeltdyrsoplysninger path is not an Array (Type: {response_struct_path.Enkeltdyrsoplysninger.type()}). Skipping."
            )
            return None

        # Select base fields and the list to unnest using path
        base = chr_dyr_raw.select(
            reporting_herd_number_raw=response_struct_path.BesaetningsNummer,
            period_fra=response_struct_path.PeriodeFra,
            period_til=response_struct_path.PeriodeTil,
            animals_list=response_struct_path.Enkeltdyrsoplysninger,
        )

        # Filter before unnesting
        base = base.filter(base.animals_list.notnull())

        # Unnest the animals list
        unpacked = base.select(
            reporting_herd_number=base.reporting_herd_number_raw,
            period_fra=base.period_fra,
            period_til=base.period_til,
            animal_info=base.animals_list.unnest(),  # animal_info is a struct
        )

        # Filter after unnesting
        unpacked = unpacked.filter(unpacked.animal_info.notnull())

        # Define source -> target mapping for fields inside the animal_info struct
        animal_cols = {
            "CkrNr": "ckr_number",
            "DatoFoedt": "birth_date",
            "DatoIndgaaet": "entry_date",
            "DatoAfgaaet": "exit_date",
            "KildeBesaetning": "source_herd",
            "DestinationBesaetning": "destination_herd",
            "Koen": "gender",
            "Race": "breed",
            "MorCkrNr": "mother_ckr_number",
        }
        available_struct_cols = unpacked.animal_info.type().names

        animals = unpacked.select(
            reporting_herd_number=unpacked.reporting_herd_number,
            period_fra=unpacked.period_fra,
            period_til=unpacked.period_til,
            **{
                target: unpacked.animal_info[source].name(target)
                for source, target in animal_cols.items()
                if source in available_struct_cols
            },
        )

        # Add null columns if source was missing
        for target in animal_cols.values():
            if target not in animals.columns:
                animals = animals.mutate(**{target: ibis.null()})
                logging.warning(f"Column for '{target}' missing in source animal struct element, adding as null.")

        # Generate UUID and clean/cast
        animals = animals.mutate(
            animal_movement_id=ibis.uuid(),  # Generate UUID
            reporting_herd_number=ibis.coalesce(
                animals.reporting_herd_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            period_fra=ibis.coalesce(
                animals.period_fra.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            period_til=ibis.coalesce(
                animals.period_til.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            ckr_number=ibis.coalesce(
                animals.ckr_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            birth_date=ibis.coalesce(
                animals.birth_date.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            entry_date=ibis.coalesce(
                animals.entry_date.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            exit_date=ibis.coalesce(
                animals.exit_date.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            source_herd=ibis.coalesce(
                animals.source_herd.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            destination_herd=ibis.coalesce(
                animals.destination_herd.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            gender=animals.gender.cast(dt.string).strip().nullif(""),
            breed=animals.breed.cast(dt.string).strip().nullif(""),
            mother_ckr_number=ibis.coalesce(
                animals.mother_ckr_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
        )

        # Select final columns in desired order
        final_cols = [
            "animal_movement_id",
            "reporting_herd_number",
            "period_fra",
            "period_til",
            "ckr_number",
            "birth_date",
            "entry_date",
            "exit_date",
            "source_herd",
            "destination_herd",
            "gender",
            "breed",
            "mother_ckr_number",
        ]
        animals_final = animals.select(*final_cols)

        # --- Save to Parquet ---
        output_path = silver_dir / "chr_dyr_animal_movements.parquet"
        rows = animals_final.count().execute()
        if rows == 0:
            logging.warning("CHR_dyr animal movements table is empty after processing.")
            return None

        logging.info(f"Saving chr_dyr_animal_movements table with {rows} rows.")
        saved_path = export.save_table(output_path, animals_final.execute(), is_geo=False)
        if saved_path is None:
            logging.error("Failed to save chr_dyr_animal_movements table - no path returned")
            return None
        logging.info(f"Saved chr_dyr_animal_movements table to {saved_path}")

        return animals_final

    except Exception as e:
        logging.error(f"Failed to create chr_dyr_animal_movements table: {e}", exc_info=True)
        return None


def create_animal_movements_table(
    con: ibis.BaseBackend, diko_flyt_raw: ibis.Table | None, silver_dir: Path
) -> ibis.Table | None:
    """Creates the animal_movements table from the nested Flytninger list in diko_flytninger."""
    logging.info("Starting creation of animal_movements table.")

    # Check for nested structure
    if diko_flyt_raw is None or "Response" not in diko_flyt_raw.columns:
        logging.warning("Cannot create animal_movements: 'Response' column missing in diko_flyt_raw.")
        return None

    try:
        # Access the nested structure Response[0]
        if not isinstance(diko_flyt_raw["Response"].type(), dt.Array):
            logging.warning(
                f"Cannot create animal_movements: 'Response' column is not an Array (Type: {diko_flyt_raw['Response'].type()}). Skipping."
            )
            return None
        # Define path
        response_struct_path = diko_flyt_raw.Response[0]

        # Check for required fields within the response struct path
        if (
            not isinstance(response_struct_path.type(), dt.Struct)
            or "BesaetningsNummer" not in response_struct_path.type().names
            or "Flytninger" not in response_struct_path.type().names
        ):
            logging.warning(
                "Cannot create animal_movements: Missing 'BesaetningsNummer' or 'Flytninger' in Response[0] path. Skipping."
            )
            return None

        # Check Flytninger is array using path
        if not isinstance(response_struct_path.Flytninger.type(), dt.Array):
            logging.warning(
                f"Cannot create animal_movements: Response[0].Flytninger path is not an Array (Type: {response_struct_path.Flytninger.type()}). Skipping."
            )
            return None

        # Select base fields and the list to unnest using path
        base = diko_flyt_raw.select(  # Select from base table
            reporting_herd_number_raw=response_struct_path.BesaetningsNummer,
            flytninger_list=response_struct_path.Flytninger,
        )

        # Filter before unnesting
        base = base.filter(base.flytninger_list.notnull())

        # Unnest the Flytninger list.
        unpacked = base.select(
            reporting_herd_number=base.reporting_herd_number_raw,
            movement_info=base.flytninger_list.unnest(),  # movement_info is a struct
        )

        # Filter after unnesting
        unpacked = unpacked.filter(unpacked.movement_info.notnull())

        # Define source -> target mapping for fields inside the movement_info struct
        # Using field names from the head output
        movement_cols = {
            # 'IndberetningsBesaetning' is handled above
            "FlytteDato": "movement_date",
            "KontaktType": "contact_type",  # 'Til' or 'Fra'
            "ChrNummer": "counterparty_chr_number",  # Was ModpartCHRnr
            "BesaetningsNummer": "counterparty_herd_number",  # Was ModpartBesaetningsnr
            "VirksomhedsArt": "counterparty_business_type",  # Was ModpartForretningstype
        }
        available_struct_cols = unpacked.movement_info.type().names

        movements = unpacked.select(
            reporting_herd_number=unpacked.reporting_herd_number,  # Carry reporting herd number through
            **{
                target: unpacked.movement_info[source].name(target)
                for source, target in movement_cols.items()
                if source in available_struct_cols
            },
        )

        # Add null columns if source was missing
        for target in movement_cols.values():
            if target not in movements.columns:
                movements = movements.mutate(**{target: ibis.null()})
                logging.warning(f"Column for '{target}' missing in source Flytninger struct element, adding as null.")

        # Generate UUID and clean/cast
        movements = movements.mutate(
            # Replace ibis.sql('uuid()') with ibis.uuid()
            movement_id=ibis.uuid(),  # Generate UUID
            reporting_herd_number=ibis.coalesce(
                movements.reporting_herd_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),  # FK
            movement_date=ibis.coalesce(
                movements.movement_date.cast(dt.string).strip().nullif("").cast(dt.date),
                ibis.null().cast(dt.date),
            ),
            contact_type=movements.contact_type.cast(dt.string).strip().nullif(""),
            counterparty_chr_number=ibis.coalesce(
                movements.counterparty_chr_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            counterparty_herd_number=ibis.coalesce(
                movements.counterparty_herd_number.cast(dt.string).strip().nullif("").cast(dt.int64),
                ibis.null().cast(dt.int64),
            ),
            counterparty_business_type=movements.counterparty_business_type.cast(dt.string).strip().nullif(""),
        )

        # Select final columns in desired order
        final_cols = [
            "movement_id",
            "reporting_herd_number",
            "movement_date",
            "contact_type",
            "counterparty_chr_number",
            "counterparty_herd_number",
            "counterparty_business_type",
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
