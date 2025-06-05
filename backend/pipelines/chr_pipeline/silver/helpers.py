import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import ibis
import ibis.expr.datatypes as dt

# Import config for PIPELINE_DIR
from . import config

# Import export module

# --- Helper Functions ---


def get_latest_bronze_dir(base_dir: Path) -> Path:
    """Finds the latest dated folder (YYYYMMDD_HHMMSS format) in the bronze directory."""
    dated_dirs = []
    for item in base_dir.iterdir():
        if item.is_dir():
            try:
                # Attempt to parse the directory name
                datetime.strptime(item.name, "%Y%m%d_%H%M%S")
                dated_dirs.append(item)
            except ValueError:
                # Ignore directories that don't match the format
                continue

    if not dated_dirs:
        raise FileNotFoundError(f"No directories matching YYYYMMDD_HHMMSS format found in {base_dir}")

    latest_dir = max(dated_dirs, key=lambda d: datetime.strptime(d.name, "%Y%m%d_%H%M%S"))
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
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding="utf-8")
        logging.info("XML parser executed successfully.")
        # Log stdout/stderr only if they contain content
        if result.stdout:
            logging.debug(f"XML parser stdout:\n{result.stdout.strip()}")
        if result.stderr:
            logging.warning(f"XML parser stderr:\n{result.stderr.strip()}")
    except subprocess.CalledProcessError as e:
        logging.error(f"XML parser script failed with exit code {e.returncode}")
        logging.error(f"Command: {' '.join(map(str, e.cmd))}")  # Ensure command parts are strings
        # Log stderr and stdout decoded properly
        stderr_output = e.stderr.strip() if e.stderr else "N/A"
        stdout_output = e.stdout.strip() if e.stdout else "N/A"
        logging.error(f"Stderr: {stderr_output}")
        logging.error(f"Stdout: {stdout_output}")
        raise RuntimeError("XML parsing failed.")
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while running the XML parser: {e}",
            exc_info=True,
        )
        raise


def _sanitize_string(col):
    """DEPRECATED: Use native Ibis functions instead:
    col.cast(dt.string).strip().nullif('')
    """
    if isinstance(col, str):  # Add check if input is actually a string
        stripped = col.strip()
        return stripped if stripped else None  # Return None if empty after strip
    return col  # Return original value if not a string (e.g., already None or NaN)


def sanitize_string_ibis(col):
    """Helper to clean string columns using native Ibis functions:
    - Trims whitespace
    - Treats empty strings as null
    """
    return col.cast(dt.string).strip().nullif("")


def _create_and_save_lookup(
    con,
    table: ibis.Table,
    pk_col: str,
    name_col: str,
    output_path: Path,
    table_name: str,
) -> ibis.Table | None:
    """Creates a distinct lookup table from columns and saves it locally (temporary use during processing)."""
    if table is None or pk_col not in table.columns or name_col not in table.columns:
        logging.warning(f"Cannot create lookup '{table_name}': Input table or columns missing.")
        return None

    try:
        # Create lookup table with final column names directly
        lookup = table.select(
            **{
                f"{table_name}_code": table[pk_col].cast(dt.string).strip().nullif(""),
                f"{table_name}_name": table[name_col].cast(dt.string).strip().nullif(""),
            }
        ).distinct()

        # Filter out rows where either code or name ended up null after cleaning
        lookup = lookup.filter(lookup[f"{table_name}_code"].notnull() & lookup[f"{table_name}_name"].notnull())

        # Attempt to cast code back to integer if appropriate
        try:
            if table_name not in [
                "diseases",
                "vet_statuses",
            ]:  # Keep strings for these known cases
                lookup = lookup.mutate(**{f"{table_name}_code": lookup[f"{table_name}_code"].cast(dt.int64)})
        except Exception as cast_err:
            logging.warning(
                f"Could not cast code to integer for lookup '{table_name}'. Keeping as string. Error: {cast_err}"
            )

        # Save locally only since this is a temporary lookup table
        if lookup.count().execute() == 0:
            logging.warning(f"Lookup table '{table_name}' is empty after processing.")
            return None

        # Execute to DataFrame and save
        df = lookup.execute()
        df.to_parquet(output_path, index=False)
        logging.info(f"Saved temporary lookup table '{table_name}' locally to {output_path}")

        # Convert back to ibis table
        lookup = con.read_parquet(output_path)
        return lookup

    except Exception as e:
        logging.error(f"Failed to create or save lookup table '{table_name}': {e}", exc_info=True)
        return None
