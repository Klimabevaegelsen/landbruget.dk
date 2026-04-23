"""Base class for API exporters that generate JSON files for R2."""

import json
import os
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import s3fs
from common.logging_utils import get_pipeline_logger

logger = get_pipeline_logger("api_export")


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
        json_str = json.dumps(
            self.to_json_compatible(data),
            ensure_ascii=False,
            separators=(",", ":"),
        )

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

    def r2_uri(self, path: str) -> str:
        """Build an R2 URI for the configured bucket."""
        if path.startswith("r2://"):
            return path
        return f"r2://{self._r2_bucket}/{path}"

    def _bucket_path(self, path: str) -> str:
        if path.startswith(f"{self._r2_bucket}/"):
            return path
        return f"{self._r2_bucket}/{path.lstrip('/')}"

    def r2_glob(self, pattern: str) -> list[str]:
        """Resolve an R2 glob to sorted r2:// URIs."""
        matches = self.r2_fs.glob(self._bucket_path(pattern))
        return [f"r2://{match}" for match in sorted(matches)]

    def latest_r2_match(self, pattern: str) -> str | None:
        """Resolve the latest object matching an arbitrary R2 glob."""
        matches = self.r2_glob(pattern)
        if not matches:
            return None
        return matches[-1]

    def latest_r2_nested_parquet(
        self,
        dataset_prefix_glob: str,
        filename: str = "data.parquet",
    ) -> str | None:
        """Resolve the newest parquet in datasets shaped like `<prefix>/<timestamp>/<filename>`.

        This avoids deep recursive globs such as `foo_*/*/data.parquet`, which are slow
        against the live R2 bucket. Instead we list matching dataset prefixes first, then
        inspect each prefix's timestamp directories directly.
        """
        try:
            dataset_prefixes = sorted(self.r2_fs.glob(self._bucket_path(dataset_prefix_glob)))
        except Exception:
            logger.warning(f"Could not list nested R2 dataset prefixes for {dataset_prefix_glob}")
            return None

        candidates: list[str] = []
        for dataset_prefix in dataset_prefixes:
            try:
                versions = sorted(self.r2_fs.ls(dataset_prefix, detail=False))
            except Exception:
                logger.warning(f"Could not list R2 versions under {dataset_prefix}")
                continue
            for version in reversed(versions):
                candidate = f"{version.rstrip('/')}/{filename}"
                try:
                    if self.r2_fs.exists(candidate):
                        candidates.append(f"r2://{candidate}")
                        break
                except Exception:
                    logger.warning(f"Could not check nested R2 object: {candidate}")

        if not candidates:
            return None

        return sorted(candidates)[-1]

    def all_r2_parquet(self, dataset_prefix: str, filename_pattern: str = "*.parquet") -> list[str]:
        """Resolve all parquet files under a versioned dataset prefix."""
        return self.r2_glob(f"{dataset_prefix}/*/{filename_pattern}")

    def latest_r2_parquet(
        self,
        dataset_prefix: str,
        filename: str = "data.parquet",
        *,
        include_unversioned: bool = False,
    ) -> str | None:
        """Resolve the latest parquet object for a timestamped dataset prefix.

        Most gold datasets live under:
        `<prefix>/<timestamp>/<filename>`

        Some older exporters still pointed at a now-missing unversioned path. This
        helper discovers the newest timestamped parquet lexicographically, which is
        safe because the directory names use sortable timestamps.
        """
        candidates: list[str] = []

        if include_unversioned:
            unversioned = self._bucket_path(f"{dataset_prefix}/{filename}")
            try:
                if self.r2_fs.exists(unversioned):
                    candidates.append(f"r2://{unversioned}")
            except Exception:
                logger.warning(f"Could not check unversioned R2 object: {unversioned}")

        try:
            candidates.extend(self.all_r2_parquet(dataset_prefix, filename))
        except Exception:
            logger.warning(f"Could not list R2 objects for {dataset_prefix}/*/{filename}")

        if not candidates:
            return None

        return sorted(candidates)[-1]

    def count_parquet_rows(self, parquet_paths: str | Iterable[str]) -> int:
        """Count parquet rows using footer metadata instead of scanning full files."""
        if isinstance(parquet_paths, str):
            paths = [parquet_paths]
        else:
            paths = list(parquet_paths)

        if not paths:
            return 0

        return int(
            self.conn.execute(
                f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_file_metadata({paths!r})"
            ).fetchone()[0]
        )

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

    def to_json_compatible(self, value):
        """Normalize DuckDB/Python values before JSON serialization."""
        if isinstance(value, dict):
            return {key: self.to_json_compatible(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.to_json_compatible(item) for item in value]
        if isinstance(value, tuple):
            return [self.to_json_compatible(item) for item in value]
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, datetime | date):
            return value.isoformat()
        return value

    def export(self) -> dict:
        """Run the export. Must be implemented by subclasses.

        Returns:
            Dict with export stats (files_written, etc.)
        """
        raise NotImplementedError
