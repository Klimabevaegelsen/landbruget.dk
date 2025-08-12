import json
import os

# Import the optimized GCS access layer
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess
except ImportError:
    # Fallback for when unified_pipeline is not available
    GCSDataAccess = None

# DuckDB is required - no fallback
try:
    import duckdb
except ImportError:
    raise ImportError("DuckDB is required for storage operations")

try:
    from google.cloud import storage
except ImportError:
    storage = None


class StorageInterface:
    """Interface for saving JSON data to different storage backends."""

    def save_json(self, data, dst_path):
        raise NotImplementedError("save_json must be implemented by subclasses")

    def save_parquet(self, data, dst_path):
        """Save data as a Parquet file."""
        raise NotImplementedError("save_parquet must be implemented by subclasses")

    def read_json(self, src_path):
        """Load JSON data from the storage backend."""
        raise NotImplementedError("read_json must be implemented by subclasses")


class LocalStorage(StorageInterface):
    """Save JSON files to the local filesystem."""

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def save_json(self, data, dst_path):
        full_path = os.path.join(self.base_dir, dst_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def save_parquet(self, data, dst_path):
        """Save data as Parquet locally using DuckDB."""
        full_path = os.path.join(self.base_dir, dst_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

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

    def read_json(self, src_path):
        full_path = os.path.join(self.base_dir, src_path)
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)


class GCSStorage(StorageInterface):
    """Save JSON files to a Google Cloud Storage bucket using optimized gcs_access.py."""

    def __init__(self, bucket_name):
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

    def save_json(self, data, dst_path):
        """Save JSON data using optimized streaming approach."""
        gcs_path = f"gs://{self.bucket_name}/{dst_path}"

        if self.optimized:
            # ✅ OPTIMIZED: Use gcs_access.py streaming JSON upload
            self.gcs_access.upload_json(data, gcs_path)
        else:
            # Fallback to legacy approach
            blob = self.bucket.blob(dst_path)
            blob.upload_from_string(json.dumps(data), content_type="application/json")

    def save_parquet(self, data, dst_path):
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
                conn.execute(f"COPY {data} TO 'buffer' (FORMAT PARQUET)")
                # Note: This approach would need modification for actual buffer writing
                # For now, raise an error suggesting the optimized path
                raise ValueError(
                    "Non-optimized GCS storage requires optimized GCS access layer for DuckDB table uploads"
                )
            elif isinstance(data, (dict, list)):
                # Convert to table first
                if isinstance(data, dict):
                    conn.register("temp_data", [data])
                else:
                    conn.register("temp_data", data)

                # Same limitation as above
                raise ValueError("Non-optimized GCS storage requires optimized GCS access layer for DuckDB operations")
            else:
                raise ValueError(
                    f"Unsupported data type for parquet export: {type(data)}. "
                    f"Only DuckDB tables, dicts, and lists are supported."
                )

            conn.close()

    def read_json(self, src_path):
        """Read JSON data using optimized streaming approach."""
        gcs_path = f"gs://{self.bucket_name}/{src_path}"

        if self.optimized:
            # ✅ OPTIMIZED: Use gcs_access.py streaming JSON download
            return self.gcs_access.download_json(gcs_path)
        else:
            # Fallback to legacy approach
            blob = self.bucket.blob(src_path)
            content = blob.download_as_string()
            return json.loads(content)
