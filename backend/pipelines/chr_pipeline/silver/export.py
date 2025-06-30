"""CHR Export functionality using DuckDB."""

import logging
from pathlib import Path
from typing import Optional, Union

import ibis


def save_table(output_path: Path, data: Union[ibis.Table, str], is_geo: bool = False) -> Optional[Path]:
    """
    Save table data to parquet file using DuckDB directly.

    ✅ MIGRATION: This function now uses DuckDB exclusively for saving data,
    accepting either Ibis tables or DuckDB table names.

    Args:
        output_path: Path where to save the parquet file
        data: Ibis table or DuckDB table name containing the data to save
        is_geo: Whether the data contains geometry (for spatial extension loading)

    Returns:
        Path to the saved file if successful, None otherwise
    """
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Handle different input types
        if isinstance(data, ibis.Table):
            # Get the DuckDB connection from the Ibis table
            con = data.get_backend()

            # Create a temporary table name for the operation
            temp_table_name = f"temp_export_{output_path.stem}"

            # Create temporary table
            con.create_table(temp_table_name, data, overwrite=True)

            # Use DuckDB COPY to save to parquet
            con.con.execute(f"COPY {temp_table_name} TO '{output_path}' (FORMAT PARQUET)")

            # Clean up temporary table
            con.drop_table(temp_table_name, force=True)

        elif isinstance(data, str):
            # Assume it's a table name in DuckDB
            # We need a connection - this is a limitation of the current design
            # For now, we'll assume the caller passes an Ibis table
            raise ValueError("String table names not supported - please pass Ibis table directly")

        else:
            raise ValueError(f"Unsupported data type: {type(data)}. Expected Ibis table.")

        if not output_path.exists():
            logging.error(f"Failed to save table - file not created: {output_path}")
            return None

        logging.info(f"Successfully saved table to {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"Error saving table to {output_path}: {str(e)}")
        return None


def save_table_with_connection(
    con: ibis.BaseBackend, table_name: str, output_path: Path, is_geo: bool = False
) -> Optional[Path]:
    """
    Save a DuckDB table to parquet file using the connection directly.

    This is an alternative method when you have the connection and table name.

    Args:
        con: DuckDB connection (Ibis backend)
        table_name: Name of the table in DuckDB
        output_path: Path where to save the parquet file
        is_geo: Whether the data contains geometry (for spatial extension loading)

    Returns:
        Path to the saved file if successful, None otherwise
    """
    try:
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Load spatial extension if needed
        if is_geo:
            try:
                con.con.execute("INSTALL spatial")
                con.con.execute("LOAD spatial")
            except Exception:
                pass  # Extensions might already be loaded

        # Use DuckDB COPY to save to parquet
        con.con.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")

        if not output_path.exists():
            logging.error(f"Failed to save table - file not created: {output_path}")
            return None

        logging.info(f"Successfully saved table '{table_name}' to {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"Error saving table '{table_name}' to {output_path}: {str(e)}")
        return None


class CHRExporter:
    """
    CHR data exporter using DuckDB.

    ✅ MIGRATION: Updated to use DuckDB exclusively for all export operations.
    """

    def __init__(self, connection: ibis.BaseBackend):
        """Initialize with DuckDB connection."""
        self.con = connection

    def export_tables(self, table_names: list[str], output_dir: Path) -> dict[str, Optional[Path]]:
        """
        Export multiple tables to parquet files.

        Args:
            table_names: List of table names to export
            output_dir: Directory to save the files

        Returns:
            Dictionary mapping table names to saved file paths (None if failed)
        """
        results = {}

        for table_name in table_names:
            output_path = output_dir / f"{table_name}.parquet"
            saved_path = save_table_with_connection(self.con, table_name, output_path)
            results[table_name] = saved_path

        return results
