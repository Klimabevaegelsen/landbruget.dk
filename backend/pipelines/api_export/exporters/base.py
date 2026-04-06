"""Base class for API exporters that generate JSON files for R2."""

import json
import logging
import os
from pathlib import Path

import duckdb
import s3fs

logger = logging.getLogger("api_export")


class BaseExporter:
    """Base class for all API export modules.

    Reads gold-layer parquet from GCS via DuckDB, writes JSON to R2.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, output_dir: str | None = None):
        self.conn = conn
        self.output_dir = output_dir  # Local output dir for testing
        self._r2_fs: s3fs.S3FileSystem | None = None
        self._r2_bucket = os.getenv("R2_BUCKET", "landbruget-data")
        self._r2_base_path = "api/v1"

    @property
    def r2_fs(self) -> s3fs.S3FileSystem:
        if self._r2_fs is None:
            r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
            r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
            r2_account_id = os.getenv("R2_ACCOUNT_ID")
            if not (r2_access_key and r2_secret_key and r2_account_id):
                raise OSError("R2 credentials not configured")
            self._r2_fs = s3fs.S3FileSystem(
                key=r2_access_key,
                secret=r2_secret_key,
                client_kwargs={"endpoint_url": f"https://{r2_account_id}.r2.cloudflarestorage.com"},
            )
        return self._r2_fs

    def write_json(self, data: dict | list, path: str) -> None:
        """Write JSON to local output dir or R2.

        Args:
            data: JSON-serializable data
            path: Relative path under api/v1/, e.g. "homepage/statistics.json"
        """
        json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

        if self.output_dir:
            # Local mode for testing
            full_path = Path(self.output_dir) / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(json_str, encoding="utf-8")
            logger.info(f"Wrote {full_path} ({len(json_str)} bytes)")
        else:
            # R2 upload
            r2_path = f"{self._r2_bucket}/{self._r2_base_path}/{path}"
            with self.r2_fs.open(r2_path, "w") as f:
                f.write(json_str)
            logger.info(f"Uploaded s3://{r2_path} ({len(json_str)} bytes)")

    def load_parquet_table(self, parquet_path: str, table_name: str) -> int:
        """Load a parquet file into a DuckDB table.

        Args:
            parquet_path: Path to parquet file (cloud or local)
            table_name: Name for the DuckDB table

        Returns:
            Row count of loaded table
        """
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{parquet_path}'")
        count = self.conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {table_name}: {count:,} rows from {parquet_path}")
        return count

    def query_to_dicts(self, sql: str) -> list[dict]:
        """Execute SQL and return results as list of dicts."""
        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def export(self) -> dict:
        """Run the export. Must be implemented by subclasses.

        Returns:
            Dict with export stats (files_written, etc.)
        """
        raise NotImplementedError
