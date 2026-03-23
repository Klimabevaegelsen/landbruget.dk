"""CHR Export functionality using vanilla DuckDB."""

import logging
import os
from pathlib import Path

import duckdb

# Try to import GCS utilities
try:
    from common.gcs import GCSDataAccess

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None


def save_table(
    output_path: Path,
    data: duckdb.DuckDBPyRelation | str,
    con: duckdb.DuckDBPyConnection,
    is_geo: bool = False,
) -> Path | None:
    """
    Save table data to parquet file using DuckDB directly.

    Args:
        output_path: Path where to save the parquet file
        data: DuckDB relation or table name string containing the data to save
        con: DuckDB connection
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
                con.execute("INSTALL spatial")
                con.execute("LOAD spatial")
            except Exception:
                pass  # Extensions might already be loaded

        # Handle different input types
        if isinstance(data, duckdb.DuckDBPyRelation):
            # Create a temporary table from the relation
            temp_table_name = f"temp_export_{output_path.stem}"
            con.execute(f"CREATE OR REPLACE TABLE {temp_table_name} AS SELECT * FROM data")

            # Use DuckDB COPY to save to parquet
            con.execute(f"COPY {temp_table_name} TO '{output_path}' (FORMAT PARQUET)")

            # Clean up temporary table
            con.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

        elif isinstance(data, str):
            # It's a table name in DuckDB - copy directly
            con.execute(f"COPY {data} TO '{output_path}' (FORMAT PARQUET)")

        else:
            raise ValueError(
                f"Unsupported data type: {type(data)}. Expected DuckDB relation or table name string."
            )

        if not output_path.exists():
            logging.error(f"Failed to save table - file not created: {output_path}")
            return None

        logging.info(f"Successfully saved table to {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"Error saving table to {output_path}: {e!s}")
        return None


def save_table_with_connection(
    con: duckdb.DuckDBPyConnection, table_name: str, output_path: Path, is_geo: bool = False
) -> Path | None:
    """
    Save a DuckDB table to parquet file using the connection directly.

    This is an alternative method when you have the connection and table name.

    Args:
        con: DuckDB connection
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
                con.execute("INSTALL spatial")
                con.execute("LOAD spatial")
            except Exception:
                pass  # Extensions might already be loaded

        # Use DuckDB COPY to save to parquet
        con.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")

        if not output_path.exists():
            logging.error(f"Failed to save table - file not created: {output_path}")
            return None

        logging.info(f"Successfully saved table '{table_name}' to {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"Error saving table '{table_name}' to {output_path}: {e!s}")
        return None


def upload_silver_data_to_gcs(silver_dir: Path, export_timestamp: str) -> bool:
    """
    Upload all silver parquet files to GCS.

    Args:
        silver_dir: Local directory containing silver parquet files
        export_timestamp: Timestamp string for the export (YYYYMMDD_HHMMSS)

    Returns:
        True if upload successful, False otherwise
    """
    if not GCS_AVAILABLE:
        logging.warning("GCS utilities not available - skipping silver data upload")
        return False

    bucket_name = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET")
    if not bucket_name:
        logging.warning("GCS_BUCKET not set - skipping silver data upload")
        return False

    try:
        gcs_access = GCSDataAccess()

        # Find all parquet files in the silver directory
        parquet_files = list(silver_dir.glob("*.parquet"))

        if not parquet_files:
            logging.warning(f"No parquet files found in {silver_dir}")
            return False

        logging.info(f"Uploading {len(parquet_files)} silver files to GCS bucket '{bucket_name}'")

        uploaded_count = 0
        for parquet_file in parquet_files:
            try:
                # Create GCS path: silver/chr/{timestamp}/{filename}
                gcs_path = f"gs://{bucket_name}/silver/chr/{export_timestamp}/{parquet_file.name}"

                # Upload file using streaming (strip gs:// for raw fs access)
                fs_path = gcs_path.replace("gs://", "", 1)
                with open(parquet_file, "rb") as src, gcs_access.fs.open(fs_path, "wb") as dst:
                    import shutil

                    shutil.copyfileobj(src, dst)

                logging.info(f"Uploaded {parquet_file.name} to {gcs_path}")
                uploaded_count += 1

            except Exception as e:
                logging.error(f"Failed to upload {parquet_file.name}: {e}")

        if uploaded_count == len(parquet_files):
            logging.info(f"Successfully uploaded all {uploaded_count} silver files to GCS")
            return True
        logging.warning(f"Only uploaded {uploaded_count}/{len(parquet_files)} silver files")
        return False

    except Exception as e:
        logging.error(f"Error uploading silver data to GCS: {e}")
        return False


class CHRExporter:
    """
    CHR data exporter using vanilla DuckDB.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        """Initialize with DuckDB connection."""
        self.con = connection

    def export_tables(self, table_names: list[str], output_dir: Path) -> dict[str, Path | None]:
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
