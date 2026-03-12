import json
import os
from pathlib import Path
from typing import Any, Never

# Import the optimized GCS access layer
try:
    from common.gcs import GCSDataAccess
except ImportError:
    # Fallback for when common.gcs is not available
    GCSDataAccess = None

# DuckDB is required - no fallback
try:
    import duckdb
except ImportError as e:
    raise ImportError("DuckDB is required for storage operations") from e

# google.cloud.storage no longer needed — using s3fs via GCSDataAccess (R2)
storage = None

DEFAULT_BUCKET = "landbruget-data"


class StoragePath:
    """Build cloud storage paths consistently across all pipelines.

    Centralizes bucket name, protocol prefix, and medallion-layer path construction
    so pipelines never build URLs ad-hoc.

    Usage:
        paths = StoragePath()  # reads bucket from R2_BUCKET or GCS_BUCKET env
        paths.bronze("chr_pipeline", "2025-03-07", "raw.parquet")
        # -> "gs://landbruget-data/bronze/chr_pipeline/2025-03-07/raw.parquet"

        paths.silver("unified_pipeline", "agricultural_fields.parquet")
        # -> "gs://landbruget-data/silver/unified_pipeline/agricultural_fields.parquet"
    """

    def __init__(self, bucket: str | None = None) -> None:
        self.bucket = bucket or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET") or DEFAULT_BUCKET

    def _build(self, *parts: str) -> str:
        """Build a gs:// path from parts, stripping extra slashes."""
        joined = "/".join(p.strip("/") for p in parts if p)
        return f"gs://{self.bucket}/{joined}"

    def bronze(self, source: str, *parts: str) -> str:
        return self._build("bronze", source, *parts)

    def silver(self, source: str, *parts: str) -> str:
        return self._build("silver", source, *parts)

    def gold(self, source: str, *parts: str) -> str:
        return self._build("gold", source, *parts)

    def raw(self, *parts: str) -> str:
        """Build a path without a medallion layer prefix."""
        return self._build(*parts)


class StorageInterface:
    """Interface for saving JSON data to different storage backends."""

    def save_json(self, data: Any, dst_path: str) -> Never:
        raise NotImplementedError("save_json must be implemented by subclasses")

    def save_parquet(self, data: Any, dst_path: str) -> Never:
        """Save data as a Parquet file."""
        raise NotImplementedError("save_parquet must be implemented by subclasses")

    def read_json(self, src_path: str) -> Never:
        """Load JSON data from the storage backend."""
        raise NotImplementedError("read_json must be implemented by subclasses")


class LocalStorage(StorageInterface):
    """Save JSON files to the local filesystem."""

    def __init__(self, base_dir: str) -> None:
        self.base_dir = base_dir

    def save_json(self, data: Any, dst_path: str) -> None:
        full_path = Path(self.base_dir) / dst_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with full_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def save_parquet(self, data: Any, dst_path: str) -> None:
        """Save data as Parquet locally using DuckDB."""
        full_path = Path(self.base_dir) / dst_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect()

        if isinstance(data, str):
            # Assume it's a table name in DuckDB
            conn.execute(f"COPY {data} TO '{full_path}' (FORMAT PARQUET)")
        elif isinstance(data, dict):
            # Convert dict to table
            conn.register("temp_data", [data])
            conn.execute(f"COPY temp_data TO '{full_path}' (FORMAT PARQUET)")
        elif isinstance(data, list):
            # Convert list to table - handle list of dicts properly
            if data and isinstance(data[0], dict):
                # This is a list of dictionaries, create table manually
                columns = list(data[0].keys())
                conn.execute(f"""
                    CREATE OR REPLACE TABLE temp_data (
                        {", ".join([f"{col} VARCHAR" for col in columns])}
                    )
                """)

                # Insert data in batches using parameterized queries
                batch_size = 1000
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    for item in batch:
                        values = [item.get(col) for col in columns]
                        placeholders = ", ".join(["?" for _ in columns])

                        conn.execute(
                            f"""
                            INSERT INTO temp_data ({", ".join(columns)})
                            VALUES ({placeholders})
                        """,
                            values,
                        )
            else:
                # Regular list, try to register directly
                conn.register("temp_data", data)
            conn.execute(f"COPY temp_data TO '{full_path}' (FORMAT PARQUET)")
        else:
            raise ValueError(
                f"Unsupported data type for parquet export: {type(data)}. "
                f"Only DuckDB tables, dicts, and lists are supported."
            )

        conn.close()

    def read_json(self, src_path: str) -> Any:
        full_path = Path(self.base_dir) / src_path
        with full_path.open(encoding="utf-8") as f:
            return json.load(f)


class CloudStorage(StorageInterface):
    """Save JSON files to cloud object storage using optimized GCSDataAccess."""

    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name

        # Use optimized GCS access layer if available
        if GCSDataAccess:
            self.gcs_access = GCSDataAccess()
            self.optimized = True
        else:
            # Fallback to legacy Google Cloud Storage client
            if storage:
                self.client = storage.Client()
                self.bucket = self.client.bucket(bucket_name)
                self.optimized = False
            else:
                raise ImportError("Neither GCSDataAccess nor google.cloud.storage is available")

    def save_json(self, data: Any, dst_path: str) -> None:
        """Save JSON data using optimized streaming approach."""
        gcs_path = f"gs://{self.bucket_name}/{dst_path}"

        if self.optimized:
            # ✅ OPTIMIZED: Use gcs_access.py streaming JSON upload
            self.gcs_access.upload_json(data, gcs_path)
        else:
            # Fallback to legacy approach
            blob = self.bucket.blob(dst_path)
            blob.upload_from_string(json.dumps(data), content_type="application/json")

    def save_parquet(self, data: Any, dst_path: str) -> None:
        """Save data as Parquet to GCS using DuckDB approach."""
        gcs_path = f"gs://{self.bucket_name}/{dst_path}"

        if self.optimized:
            # ✅ OPTIMIZED: Use gcs_access.py DuckDB-based upload
            if isinstance(data, str):
                # Assume it's a DuckDB table name
                self.gcs_access.upload_from_duckdb_table(data, gcs_path)
            else:
                # Convert data to DuckDB table and upload
                conn = self.gcs_access.duckdb_conn
                if isinstance(data, dict):
                    conn.register("temp_parquet_data", [data])
                elif isinstance(data, list):
                    # Handle list of dicts properly
                    if data and isinstance(data[0], dict):
                        # This is a list of dictionaries, create table manually
                        columns = list(data[0].keys())
                        conn.execute(f"""
                            CREATE OR REPLACE TABLE temp_parquet_data (
                                {", ".join([f"{col} VARCHAR" for col in columns])}
                            )
                        """)

                        # Insert data in batches using parameterized queries
                        batch_size = 1000
                        for i in range(0, len(data), batch_size):
                            batch = data[i : i + batch_size]
                            for item in batch:
                                values = [item.get(col) for col in columns]
                                placeholders = ", ".join(["?" for _ in columns])

                                conn.execute(
                                    f"""
                                    INSERT INTO temp_parquet_data ({", ".join(columns)})
                                    VALUES ({placeholders})
                                """,
                                    values,
                                )
                    else:
                        # Regular list, try to register directly
                        conn.register("temp_parquet_data", data)
                else:
                    raise ValueError(
                        f"Unsupported data type for parquet export: {type(data)}. "
                        f"Only DuckDB tables, dicts, and lists are supported."
                    )

                self.gcs_access.upload_from_duckdb_table("temp_parquet_data", gcs_path)
        else:
            # Fallback: Use DuckDB to create parquet and upload as bytes
            conn = duckdb.connect()

            if isinstance(data, str):
                # Table name - export to memory buffer
                # Note: This approach would need modification for actual buffer writing
                conn.execute(f"COPY {data} TO 'buffer' (FORMAT PARQUET)")
                # For now, raise an error suggesting the optimized path
                raise ValueError(
                    "Non-optimized GCS storage requires optimized GCS access layer for DuckDB table uploads"
                )
            if isinstance(data, dict | list):
                # Convert to table first
                if isinstance(data, dict):
                    conn.register("temp_data", [data])
                else:
                    conn.register("temp_data", data)

                # Same limitation as above
                raise ValueError("Non-optimized GCS storage requires optimized GCS access layer for DuckDB operations")
            raise ValueError(
                f"Unsupported data type for parquet export: {type(data)}. "
                f"Only DuckDB tables, dicts, and lists are supported."
            )

            conn.close()

    def read_json(self, src_path: str) -> Any:
        """Read JSON data using optimized streaming approach."""
        gcs_path = f"gs://{self.bucket_name}/{src_path}"

        if self.optimized:
            # ✅ OPTIMIZED: Use gcs_access.py streaming JSON download
            return self.gcs_access.download_json(gcs_path)
        # Fallback to legacy approach
        blob = self.bucket.blob(src_path)
        content = blob.download_as_string()
        return json.loads(content)


# Backward-compatible alias (deprecated name)
GCSStorage = CloudStorage
