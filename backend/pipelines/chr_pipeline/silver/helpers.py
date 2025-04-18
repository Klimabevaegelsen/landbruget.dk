import ibis
import ibis.expr.datatypes as dt
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Import config for PIPELINE_DIR
from . import config

# --- Helper Functions ---

def get_latest_bronze_dir(base_dir: Path) -> Path:
    """Finds the latest dated folder (YYYYMMDD_HHMMSS format) in the bronze directory."""
    dated_dirs = []
    for item in base_dir.iterdir():
        if item.is_dir():
            try:
                # Attempt to parse the directory name
                datetime.strptime(item.name, '%Y%m%d_%H%M%S')
                dated_dirs.append(item)
            except ValueError:
                # Ignore directories that don't match the format
                continue

    if not dated_dirs:
        raise FileNotFoundError(f"No directories matching YYYYMMDD_HHMMSS format found in {base_dir}")

    latest_dir = max(dated_dirs, key=lambda d: datetime.strptime(d.name, '%Y%m%d_%H%M%S'))
    logging.info(f"Using latest bronze data directory: {latest_dir.name}")
    return latest_dir

def run_xml_parser(input_xml: Path, output_jsonl: Path) -> None:
    """Runs the parse_vetstat_xml.py script using the same Python interpreter."""
    # Use config.PIPELINE_DIR instead of PIPELINE_DIR
    parser_script = config.PIPELINE_DIR / "parse_vetstat_xml.py"
    if not parser_script.exists():
        raise FileNotFoundError(f"XML Parser script not found at {parser_script}")

    logging.info(f"Running XML parser for {input_xml} -> {output_jsonl}")
    # Ensure paths are passed as strings
    command = [sys.executable, str(parser_script), str(input_xml), str(output_jsonl)]
    try:
        # Use utf-8 encoding for output
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        logging.info("XML parser executed successfully.")
        # Log stdout/stderr only if they contain content
        if result.stdout:
            logging.debug(f"XML parser stdout:\n{result.stdout.strip()}")
        if result.stderr:
            logging.warning(f"XML parser stderr:\n{result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        logging.error(f"XML parser script failed with exit code {e.returncode}")
        logging.error(f"Command: {' '.join(map(str, e.cmd))}") # Ensure command parts are strings
        # Log stderr and stdout decoded properly
        stderr_output = e.stderr.strip() if e.stderr else "N/A"
        stdout_output = e.stdout.strip() if e.stdout else "N/A"
        logging.error(f"Stderr: {stderr_output}")
        logging.error(f"Stdout: {stdout_output}")
        raise RuntimeError("XML parsing failed.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while running the XML parser: {e}", exc_info=True)
        raise

def _sanitize_string(col):
    """Helper to clean string columns: trim whitespace and treat empty strings as null."""
    if isinstance(col, str): # Add check if input is actually a string
        stripped = col.strip()
        return stripped if stripped else None # Return None if empty after strip
    return col # Return original value if not a string (e.g., already None or NaN)

def _create_and_save_lookup(con, table: ibis.Table, pk_col: str, name_col: str, output_path: Path, table_name: str) -> ibis.Table | None:
    """Creates a distinct lookup table from columns and saves it to Parquet."""
    if table is None or pk_col not in table.columns or name_col not in table.columns:
        logging.warning(f"Cannot create lookup '{table_name}': Input table or columns missing.")
        return None

    try:
        # Ensure correct types and clean strings before distinct
        lookup = table.select(
            pk=table[pk_col].cast(dt.string).pipe(_sanitize_string), # Cast code to string initially for safety
            name=table[name_col].cast(dt.string).pipe(_sanitize_string)
        ).distinct()
        # Filter out rows where either pk or name ended up null after cleaning
        lookup = lookup.filter(lookup.pk.notnull() & lookup.name.notnull())

        # Attempt to cast pk back to integer if appropriate, warn on failure
        try:
            # Try casting to bigint first, then int if it fits.
            # Check if all values can actually be cast to INT64
            # This check might be expensive, alternative: try-cast during save?
            # For now, let's attempt the cast and catch the specific error during save or earlier if possible.
            # We will keep the pk as string if casting fails later or if table_name suggests string keys
            if table_name not in ['diseases', 'vet_statuses']: # Explicitly keep strings for these known cases
                 # Try casting numeric codes
                 lookup = lookup.mutate(pk=lookup['pk'].cast(dt.int64))
                 # If cast succeeds without error (implies data is numeric), use the cast version
                 # Note: This requires execution or a robust check. Simpler: Keep as string if unsure.
                 # Let's stick to the explicit check for now.
                 lookup = lookup.mutate(pk=lookup['pk'].cast(dt.int64))
            else:
                # Keep as string for known string-based codes
                lookup = lookup.mutate(pk=lookup['pk'].cast(dt.string))

            # Check if values fit in INT32, if so cast down?
            # max_pk = lookup.agg(max_pk=lookup.pk.max()).execute()['max_pk'][0]
            # if max_pk is not None and max_pk < 2**31:
            #      lookup = lookup.mutate(pk=lookup['pk'].cast(dt.int32))
        except Exception as cast_err:
            logging.warning(f"Could not cast primary key '{pk_col}' to integer for lookup '{table_name}'. Keeping as string. Error: {cast_err}")
            # Keep pk as string if cast fails
            lookup = lookup.mutate(pk=lookup['pk'].cast(dt.string))

        if lookup.count().execute() == 0:
            logging.warning(f"Lookup table '{table_name}' is empty after processing.")
            return None

        # Rename columns to final schema
        final_pk_name = f"{table_name}_code"
        final_name_col = "name" # Keep 'name' consistent? Or use table_name_name? Let's use name.
        # Example log shows `diseases_code` but just `name`, let's follow that pattern.
        if table_name == 'species': # Handle specific naming for species
            final_pk_name = "species_code"
            final_name_col = "species_name" # Special case for species
        elif table_name == 'usage_types':
             final_pk_name = "usage_code"
             # final_name_col = "usage_name" # Keep as name
        # ... add more specific renames if needed ...
        elif table_name == 'age_groups':
            final_pk_name = "age_group_code"
            # final_name_col = "age_group_name" # Keep as name
        elif table_name == 'diseases':
            final_pk_name = "disease_code"
            # final_name_col = "disease_name" # Keep as name
        elif table_name == 'vet_statuses':
            final_pk_name = "vet_status_code"
            # final_name_col = "vet_status_name" # Keep as name

        # Rename flexibility: handle cases where final_name_col might differ
        rename_map = {final_pk_name: "pk"}
        if final_name_col != "name": # If we decided to rename the 'name' column too
            rename_map[final_name_col] = "name"

        lookup = lookup.rename(rename_map)

        logging.info(f"Saving lookup table '{table_name}' with {lookup.count().execute()} distinct rows.")
        lookup.to_parquet(output_path)
        logging.info(f"Saved {table_name} lookup to {output_path}")
        # Return the ibis table for potential use (e.g., FK mapping)
        # Read back from Parquet to ensure consistency?
        return con.read_parquet(str(output_path))

    except Exception as e:
        logging.error(f"Failed to create or save lookup table '{table_name}': {e}", exc_info=True)
        return None 