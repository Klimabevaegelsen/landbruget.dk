import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb

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
        raise FileNotFoundError(
            f"No directories matching YYYYMMDD_HHMMSS format found in {base_dir}"
        )

    latest_dir = max(dated_dirs, key=lambda d: datetime.strptime(d.name, "%Y%m%d_%H%M%S"))
    logging.info(f"Using latest bronze data directory: {latest_dir.name}")
    return latest_dir


def run_xml_parser(input_xml: Path, output_jsonl: Path) -> bool:
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
        result = subprocess.run(
            command, check=True, capture_output=True, text=True, encoding="utf-8"
        )
        logging.info("XML parser executed successfully.")
        # Log stdout/stderr only if they contain content
        if result.stdout:
            logging.debug(f"XML parser stdout:\n{result.stdout.strip()}")
        if result.stderr:
            logging.warning(f"XML parser stderr:\n{result.stderr.strip()}")
        return True  # Return True on successful execution
    except subprocess.CalledProcessError as e:
        logging.error(f"XML parser script failed with exit code {e.returncode}")
        logging.error(f"Command: {' '.join(map(str, e.cmd))}")  # Ensure command parts are strings
        # Log stderr and stdout decoded properly
        stderr_output = e.stderr.strip() if e.stderr else "N/A"
        stdout_output = e.stdout.strip() if e.stdout else "N/A"
        logging.error(f"Stderr: {stderr_output}")
        logging.error(f"Stdout: {stdout_output}")
        return False  # Return False on subprocess failure
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while running the XML parser: {e}",
            exc_info=True,
        )
        return False  # Return False on unexpected error


def _sanitize_string(col):
    """DEPRECATED: Use SQL TRIM and NULLIF functions instead:
    NULLIF(TRIM(CAST(col AS VARCHAR)), '')
    """
    if isinstance(col, str):  # Add check if input is actually a string
        stripped = col.strip()
        return stripped if stripped else None  # Return None if empty after strip
    return col  # Return original value if not a string (e.g., already None or NaN)


def sanitize_string_sql(col_name: str) -> str:
    """Helper to generate SQL expression for cleaning string columns:
    - Trims whitespace
    - Treats empty strings as null

    Args:
        col_name: The column name to sanitize

    Returns:
        SQL expression string that sanitizes the column
    """
    return f"NULLIF(TRIM(CAST({col_name} AS VARCHAR)), '')"


def _create_and_save_lookup(
    con,
    table_or_name,
    pk_col: str,
    name_col: str,
    output_path: Path,
    table_name: str,
) -> str | None:
    """Creates a distinct lookup table from columns and saves it locally (temporary use during processing).

    Args:
        con: DuckDB connection or a wrapper exposing the raw connection on `.con`
        table_or_name: Either a table name string registered in DuckDB, a table-like object, or None
        pk_col: Primary key column name in the source table
        name_col: Name column in the source table
        output_path: Path where to save the parquet file
        table_name: Name for the output lookup table

    Returns:
        The lookup table name if successful, None otherwise
    """
    try:
        # Get the raw DuckDB connection if we have a wrapper object
        if hasattr(con, "con"):
            raw_con = con.con
        else:
            raw_con = con

        # Determine the source table name
        source_table = None

        if table_or_name is None:
            logging.warning(f"Cannot create lookup '{table_name}': Input table is None.")
            return None

        if isinstance(table_or_name, str):
            source_table = table_or_name
        elif hasattr(table_or_name, "get_name"):
            # Table-like object exposing a name
            source_table = table_or_name.get_name()
        elif hasattr(table_or_name, "columns"):
            # Fallback for table-like objects without get_name
            try:
                if hasattr(table_or_name, "op") and hasattr(table_or_name.op(), "name"):
                    source_table = table_or_name.op().name
            except Exception:
                pass

        if source_table is None:
            logging.warning(
                f"Cannot create lookup '{table_name}': Could not determine source table name."
            )
            return None

        # Check if table exists in DuckDB
        try:
            columns_result = raw_con.execute(f"DESCRIBE {source_table}").fetchall()
            column_names = [col[0] for col in columns_result]
        except duckdb.CatalogException:
            logging.warning(
                f"Cannot create lookup '{table_name}': Table '{source_table}' not found in DuckDB."
            )
            return None

        if pk_col not in column_names or name_col not in column_names:
            logging.warning(
                f"Cannot create lookup '{table_name}': "
                f"Required columns '{pk_col}' or '{name_col}' not found in table '{source_table}'. "
                f"Available columns: {column_names}"
            )
            return None

        # Create lookup table with sanitized columns using SQL
        code_col_name = f"{table_name}_code"
        name_col_name = f"{table_name}_name"

        # Build the SQL to create the distinct lookup
        create_sql = f"""
            CREATE OR REPLACE TABLE temp_lookup_{table_name} AS
            SELECT DISTINCT
                {sanitize_string_sql(pk_col)} AS {code_col_name},
                {sanitize_string_sql(name_col)} AS {name_col_name}
            FROM {source_table}
            WHERE {sanitize_string_sql(pk_col)} IS NOT NULL
              AND {sanitize_string_sql(name_col)} IS NOT NULL
        """
        raw_con.execute(create_sql)

        # Attempt to cast code back to integer if appropriate
        if table_name not in ["diseases", "vet_statuses"]:  # Keep strings for these known cases
            try:
                # Check if all values can be cast to integer
                check_sql = f"""
                    SELECT COUNT(*) FROM temp_lookup_{table_name}
                    WHERE TRY_CAST({code_col_name} AS BIGINT) IS NULL
                      AND {code_col_name} IS NOT NULL
                """
                non_castable_count = raw_con.execute(check_sql).fetchone()[0]

                if non_castable_count == 0:
                    # All values can be cast to integer
                    raw_con.execute(f"""
                        CREATE OR REPLACE TABLE temp_lookup_{table_name}_int AS
                        SELECT
                            CAST({code_col_name} AS BIGINT) AS {code_col_name},
                            {name_col_name}
                        FROM temp_lookup_{table_name}
                    """)
                    raw_con.execute(f"DROP TABLE temp_lookup_{table_name}")
                    raw_con.execute(
                        f"ALTER TABLE temp_lookup_{table_name}_int RENAME TO temp_lookup_{table_name}"
                    )
            except Exception as cast_err:
                logging.warning(
                    f"Could not cast code to integer for lookup '{table_name}'. Keeping as string. Error: {cast_err}"
                )

        # Check if lookup table has any rows
        count_result = raw_con.execute(f"SELECT COUNT(*) FROM temp_lookup_{table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        if row_count == 0:
            logging.warning(f"Lookup table '{table_name}' is empty after processing.")
            raw_con.execute(f"DROP TABLE IF EXISTS temp_lookup_{table_name}")
            return None

        # Save to parquet using DuckDB COPY
        raw_con.execute(f"COPY temp_lookup_{table_name} TO '{output_path}' (FORMAT PARQUET)")
        logging.info(f"Saved temporary lookup table '{table_name}' locally to {output_path}")

        # Clean up temporary table
        raw_con.execute(f"DROP TABLE temp_lookup_{table_name}")

        # Return the table name - caller can read from parquet if needed
        return table_name

    except Exception as e:
        logging.error(f"Failed to create or save lookup table '{table_name}': {e}", exc_info=True)
        # Clean up on failure
        try:
            # Get raw connection for cleanup if available
            cleanup_con = (
                raw_con if "raw_con" in dir() else (con.con if hasattr(con, "con") else con)
            )
            cleanup_con.execute(f"DROP TABLE IF EXISTS temp_lookup_{table_name}")
            cleanup_con.execute(f"DROP TABLE IF EXISTS temp_lookup_{table_name}_int")
        except Exception:
            pass
        return None


def sanitize_string_ibis(col):
    """DEPRECATED: Use sanitize_string_sql() for SQL expressions instead.

    This function is kept for backwards compatibility but will raise a warning.
    For SQL-based processing, use sanitize_string_sql(col_name) which returns
    a SQL expression string.
    """
    logging.warning(
        "sanitize_string_ibis() is deprecated. Use sanitize_string_sql() for SQL expressions "
        "or apply sanitization directly in SQL queries."
    )
    # If called with a string column name, return the SQL expression
    if isinstance(col, str):
        return sanitize_string_sql(col)
    raise TypeError(
        f"sanitize_string_ibis() expected a column name string, got {type(col)}. "
        "Use sanitize_string_sql(col_name) for SQL-based processing."
    )
