"""
Base classes for data sources in the unified pipeline.

This module defines the abstract base classes that all data sources in
the unified pipeline must implement. It provides common functionality and
enforces a consistent interface across different data sources and stages.
"""

import json
import os
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, Optional, TypeVar

import duckdb
from pydantic import BaseModel

from unified_pipeline.common.native_schema_manager import NativeSchemaManager
from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed

# Import schema documentation
try:
    from backend.common.schema_documentation import SchemaDocumentationManager
except ImportError:
    # Fallback for when running standalone
    SchemaDocumentationManager = None


class BaseJobConfig(BaseModel):
    """
    Base configuration model for all data sources.

    This class defines common configuration properties that all data sources
    share and serves as a foundation for source-specific configuration models.
    It uses Pydantic for validation and type checking.

    All specific source configurations should inherit from this class and
    add their own specific configuration parameters.

    Example:
        >>> class MySourceConfig(BaseJobConfig):
        >>>     input_path: str
        >>>     output_bucket: str
    """

    # Option to save data locally without uploading to GCS
    save_local: bool = False

    # Dev mode options for schema generation
    dev_mode: bool = False
    generate_schemas: bool = False
    save_schemas_locally: bool = True


T = TypeVar("T", bound=BaseJobConfig)


class BronzeJobInterface(ABC):
    """
    Interface for bronze layer jobs that support in-memory data passing.

    Bronze jobs should implement this interface to return processed data
    that can be passed directly to silver jobs without disk I/O.
    """

    @abstractmethod
    async def run(self) -> Optional[Any]:
        """
        Run bronze processing and return data for silver stage.

        Returns:
            Optional[Any]: Processed data that can be passed to silver stage,
                          or None if processing fails.
        """
        pass


class SilverJobInterface(ABC):
    """
    Interface for silver layer jobs that support in-memory data passing.

    Silver jobs should implement this interface to accept data directly
    from bronze jobs without requiring disk I/O and return processed data
    for potential gold layer consumption.
    """

    @abstractmethod
    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run silver processing with optional in-memory bronze data.

        Args:
            bronze_data: Optional data from bronze stage. If None,
                        silver job should read from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails or no data to return.
        """
        pass


class GoldJobInterface(ABC):
    """
    Interface for Gold layer jobs that combine multiple silver datasets.

    Gold jobs implement business logic that requires data from multiple
    silver sources to create analytics-ready datasets.
    """

    @abstractmethod
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Run gold processing with optional in-memory silver data.

        Args:
            silver_data: Optional dictionary mapping dataset names to silver data.
                        If provided, this data will be used instead of reading from storage.
                        Format: {"dataset_name": data, ...}
        """
        pass


class BaseSource(Generic[T], ABC):
    """
    Abstract base class for all data sources in the unified pipeline.

    This class defines the common interface and shared functionality that
    all data sources must implement. It handles configuration management,
    logging, and access to GCS utilities.

    Type parameter:
        T: Configuration type that extends BaseJobConfig

    Attributes:
        config: Source-specific configuration object
        gcs_util: Google Cloud Storage utility instance (legacy)
        gcs_access: High-performance GCS data access layer (new)
        log: Logger instance for this source
    """

    def __init__(self, config: T, gcs_util: GCSUtil) -> None:
        """
        Initialize a new data source.

        Args:
            config: Source-specific configuration object
            gcs_util: Google Cloud Storage utility instance for cloud storage operations
        """
        self.config = config
        # Keep existing for backward compatibility during migration
        self.gcs_util = gcs_util
        # Add new high-performance access layer
        self.gcs_access = GCSDataAccess()
        self.log = Logger.get_logger()
        self.conn = duckdb.connect(database=":memory:")
        self._configure_duckdb()

        # Use pipeline start time consistently (not save time)
        self.pipeline_start_time = datetime.now()
        self.date_pattern = self.pipeline_start_time.strftime("%Y%m%d_%H%M%S")

        # Initialize schema manager for dev mode
        if self.config.dev_mode or self.config.generate_schemas:
            self._schema_manager = NativeSchemaManager(self.conn, self.log)
        else:
            self._schema_manager = None

        # Initialize standardized schema documentation manager
        if SchemaDocumentationManager:
            self._standardized_schema_manager = SchemaDocumentationManager(
                connection=self.conn,
                pipeline_name=self.__class__.__name__.lower()
                .replace("source", "")
                .replace("job", ""),
                pipeline_start_time=self.pipeline_start_time,
                logger=self.log,
            )
        else:
            self._standardized_schema_manager = None

    def _configure_duckdb(self):
        """
        Configure DuckDB connection for optimal performance.

        ✅ MIGRATION: Single configuration method called once during initialization
        to avoid repeated extension installation and connection overhead.
        """
        try:
            # Memory and performance settings
            self.conn.execute("SET memory_limit = '12GB'")
            self.conn.execute("SET max_memory = '12GB'")
            self.conn.execute("SET threads = 4")
            self.conn.execute("SET enable_progress_bar = true")
            self.conn.execute("SET preserve_insertion_order = false")
            self.conn.execute("SET temp_directory = '/tmp/duckdb'")
            self.conn.execute("SET default_order = 'ASC'")

            # ✅ MIGRATION: Install spatial extension once
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")

            self.log.info("✅ DuckDB configured with spatial extensions")
        except Exception as e:
            self.log.warning(f"DuckDB configuration warning: {e}")

    def __del__(self):
        """Clean up DuckDB connection."""
        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
            except:
                pass

    @abstractmethod
    async def run(self) -> None:
        """
        Run the data source processing pipeline.

        This method must be implemented by all concrete source classes.
        It should handle the entire process of fetching, transforming, and
        storing data according to the source's specific requirements.

        Returns:
            None

        Raises:
            NotImplementedError: If the concrete class does not implement this method
        """
        pass

    @timed(name="Saving processed data")  # type: ignore
    def _save_data(
        self,
        data: Any,
        dataset: str,
        bucket: str,
        stage: str,
        subdataset: str = None,
        conn: Any = None,
    ) -> None:
        """
        Save data with support for bronze, silver, and gold stages.

        ✅ MIGRATION: Enhanced to handle both table names and DataFrames
        for efficient DuckDB-first operations while maintaining compatibility.

        Args:
            data: Data to save - can be DataFrame, table name (str), dict, or list
            dataset: Primary dataset name
            bucket: GCS bucket name
            stage: Pipeline stage (bronze/silver/gold)
            subdataset: Optional subdataset name for multi-table outputs
            conn: Optional DuckDB connection when data is a table name
        """

        valid_stages = ["bronze", "silver", "gold"]
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage '{stage}'. Must be one of {valid_stages}")

        # Use pipeline start time (not save time) for consistent timestamping
        timestamp = self.date_pattern

        # Handle table name input (new DuckDB-first approach)
        if isinstance(data, str) and conn is not None:
            # ✅ MIGRATION: Direct table operations - no DataFrame conversion needed
            table_name = data

            # Determine final dataset name
            final_dataset = f"{dataset}_{subdataset}" if subdataset else dataset

            # Create path with timestamp subdirectory
            filename = f"{final_dataset}.parquet"
            path = f"{stage}/{final_dataset}/{timestamp}/{filename}"

            if self.config.save_local:
                # ✅ MIGRATION: Save directly from table using DuckDB (no DataFrame conversion)
                local_path = f"/tmp/{filename}"
                try:
                    conn.execute(f"COPY {table_name} TO '{local_path}' (FORMAT PARQUET)")
                    self.log.info(f"✅ Table saved directly using DuckDB: {local_path}")
                except Exception as e:
                    self.log.error(f"Failed to save table {table_name}: {e}")
                    raise
            else:
                # ✅ MIGRATION: Save to GCS directly from table (no DataFrame conversion)
                try:
                    # Create temporary file for GCS upload
                    import tempfile

                    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                        temp_path = tmp_file.name

                    # Export table to temporary file
                    conn.execute(f"COPY {table_name} TO '{temp_path}' (FORMAT PARQUET)")

                    # Upload to GCS
                    self.gcs_util.upload_file(
                        bucket_name=bucket, source_file_path=temp_path, destination_blob_name=path
                    )

                    # Clean up temporary file
                    import os

                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

                    self.log.info(f"✅ Table saved directly to GCS: gs://{bucket}/{path}")
                except Exception as e:
                    self.log.error(f"Failed to save table {table_name} to GCS: {e}")
                    raise

            return

        # Handle traditional DataFrame/data inputs (backward compatibility)
        final_dataset = f"{dataset}_{subdataset}" if subdataset else dataset

        # Determine file extension and format
        if isinstance(data, str):
            # Data is a table name in DuckDB
            file_extension = "parquet"
            filename = f"{final_dataset}.{file_extension}"
        elif isinstance(data, (dict, list)):
            file_extension = "json"
            filename = f"{final_dataset}.{file_extension}"
        else:
            # For backward compatibility, try to handle as table name
            file_extension = "parquet"
            filename = f"{final_dataset}.{file_extension}"

        # Create path with timestamp subdirectory
        path = f"{stage}/{final_dataset}/{timestamp}/{filename}"

        if self.config.save_local:
            # ✅ MIGRATION: Save locally using DuckDB
            local_path = f"/tmp/{filename}"

            if isinstance(data, str):
                # Data is a table name - save directly from DuckDB
                self.conn.execute(f"COPY {data} TO '{local_path}' (FORMAT PARQUET)")
                self.log.info(f"✅ Data saved locally using DuckDB: {local_path}")
            elif isinstance(data, (dict, list)):
                with open(local_path, "w") as f:
                    json.dump(data, f, indent=2, default=str)
                self.log.info(f"Data saved locally to {local_path}")
            else:
                # Try to save as table name
                self.conn.execute(f"COPY {data} TO '{local_path}' (FORMAT PARQUET)")
                self.log.info(f"✅ Data saved locally using DuckDB: {local_path}")
        else:
            # Save to GCS using DuckDB
            if isinstance(data, str):
                # Data is a table name - save to temp file then upload
                import tempfile

                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
                    temp_path = tmp_file.name

                self.conn.execute(f"COPY {data} TO '{temp_path}' (FORMAT PARQUET)")
                self.gcs_util.upload_file(bucket, temp_path, path)

                import os

                os.unlink(temp_path)
            elif isinstance(data, (dict, list)):
                self.gcs_util.upload_json_to_gcs(data=data, bucket_name=bucket, blob_name=path)

            self.log.info(f"Data saved to GCS: gs://{bucket}/{path}")

    def _read_silver_data(self, dataset: str) -> Optional[Any]:
        """Read data from silver layer for gold processing."""

        return self._read_data_from_storage(dataset, self.config.bucket, stage="silver")

    def _read_data_from_storage(self, dataset: str, bucket: str, stage: str) -> Optional[Any]:
        """Read data from storage for any stage."""

        try:
            # List files in the stage/dataset directory
            prefix = f"{stage}/{dataset}/"
            files = self.gcs_util.list_files(bucket_name=bucket, prefix=prefix)

            if not files:
                self.log.warning(f"No files found for {stage}/{dataset}")
                return None

            # Get the most recent file
            latest_file = max(files, key=lambda x: x.time_created)
            self.log.info(f"Reading latest {stage} data: {latest_file.name}")

            # Determine file type and read accordingly
            if latest_file.name.endswith(".parquet"):
                # ✅ MIGRATION: Use GCSDataAccess properly with its own connection
                from unified_pipeline.util.gcs_access import GCSDataAccess

                # Create GCS access instance - it has its own DuckDB connection
                gcs_access = GCSDataAccess()
                gcs_path = f"gs://{bucket}/{latest_file.name}"

                # Create table in GCS connection using the proper method
                table_name = f"bronze_data_{dataset}_{stage}"
                gcs_access.create_table_from_gcs(table_name, gcs_path)

                # Get row count for logging using GCS connection
                row_count = gcs_access.duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                self.log.info(
                    f"✅ Successfully loaded {row_count} records using GCSDataAccess from {latest_file.name}"
                )

                # Return both the GCS access instance and table name
                # This allows the caller to use the same connection
                return {"gcs_access": gcs_access, "table_name": table_name}

            elif latest_file.name.endswith(".json"):
                # ✅ MIGRATION: Use modern GCS access for JSON downloads
                from unified_pipeline.util.gcs_access import GCSDataAccess

                gcs_access = GCSDataAccess()
                gcs_path = f"gs://{bucket}/{latest_file.name}"
                return gcs_access.download_json(gcs_path)
            else:
                self.log.error(f"Unsupported file type: {latest_file.name}")
                return None

        except Exception as e:
            self.log.error(f"Failed to read {stage} data for {dataset}: {e}")
            return None

    @timed(name="Reading bronze data")  # type: ignore
    def _read_bronze_data(
        self, dataset: str, bucket_name: str, bronze_data: Optional[Any] = None
    ) -> Optional[str]:
        """
        Read data from the bronze layer, preferring in-memory data if available.

        This method first checks if bronze_data is provided (in-memory data passing).
        If not available, it falls back to reading from storage using the timestamped
        subdirectory pattern.

        Args:
            dataset: The name of the dataset to read
            bucket_name: The name of the GCS bucket
            bronze_data: Optional in-memory data from bronze stage

        Returns:
            Optional[str]: A table name containing the bronze layer data,
                          or None if no data is found

        Raises:
            Exception: If there are issues reading the data
        """
        # Prefer in-memory data if available
        if bronze_data is not None:
            self.log.info("Using bronze data from memory (in-memory data passing)")
            if isinstance(bronze_data, str):
                # Already a table name
                return bronze_data
            elif hasattr(bronze_data, "to_dict"):
                # Convert DataFrame-like object to table
                table_name = f"bronze_data_{dataset}"
                # Assume it's already a table name if it's a string
                if isinstance(bronze_data, str):
                    return bronze_data
                else:
                    # Try to register as table
                    self.conn.register(table_name, bronze_data)
                    return table_name
            elif isinstance(bronze_data, (dict, list)):
                # Convert JSON data to table
                table_name = f"bronze_data_{dataset}"
                if isinstance(bronze_data, list) and len(bronze_data) > 0:
                    if isinstance(bronze_data[0], str):
                        # List of strings (e.g., XML payloads) - create table directly in DuckDB
                        # First create the table structure
                        self.conn.execute(f"CREATE TABLE {table_name} (payload VARCHAR)")
                        # Insert data using parameterized queries to avoid SQL injection
                        for item in bronze_data:
                            self.conn.execute(f"INSERT INTO {table_name} VALUES (?)", [item])
                        return table_name
                    else:
                        # List of dicts - use DuckDB JSON functions
                        json_data = json.dumps(bronze_data)
                        self.conn.execute(f"""
                            CREATE TABLE {table_name} AS 
                            SELECT * FROM read_json_auto('{json_data}')
                        """)
                        return table_name
                elif isinstance(bronze_data, dict):
                    # Single dict - create table with one row
                    json_data = json.dumps([bronze_data])
                    self.conn.execute(f"""
                        CREATE TABLE {table_name} AS 
                        SELECT * FROM read_json_auto('{json_data}')
                    """)
                    return table_name
            else:
                self.log.warning(f"Unsupported bronze_data type: {type(bronze_data)}")
                return None

        # Fallback to reading from storage
        self.log.info("Reading bronze data from storage (fallback)")
        return self._read_bronze_data_from_storage(dataset, bucket_name)

    def _read_bronze_data_from_storage(self, dataset: str, bucket_name: str) -> Optional[str]:
        """
        Read bronze data from storage using the latest timestamped directory.

        Args:
            dataset: The name of the dataset to read
            bucket_name: The name of the GCS bucket

        Returns:
            Optional[str]: A table name containing the bronze layer data,
                          or None if no data is found
        """
        if self.config.save_local:
            # Read from local storage - find latest timestamped directory
            bronze_base_dir = f"/tmp/bronze/{dataset}"
            if not os.path.exists(bronze_base_dir):
                self.log.error(f"Bronze directory not found: {bronze_base_dir}")
                return None

            # List all timestamped subdirectories
            subdirs = [
                d
                for d in os.listdir(bronze_base_dir)
                if os.path.isdir(os.path.join(bronze_base_dir, d))
            ]

            if not subdirs:
                self.log.error(f"No timestamped subdirectories found in {bronze_base_dir}")
                return None

            # Find the latest directory
            latest_dir = max(
                subdirs, key=lambda x: os.path.getmtime(os.path.join(bronze_base_dir, x))
            )
            latest_dir_path = os.path.join(bronze_base_dir, latest_dir)

            # Look for data files in the latest directory
            for filename in ["data.parquet", "data.json"]:
                file_path = os.path.join(latest_dir_path, filename)
                if os.path.exists(file_path):
                    self.log.info(f"Reading bronze data from local path: {file_path}")
                    if filename.endswith(".parquet"):
                        # ✅ MIGRATION: Use DuckDB for efficient parquet reading with table creation
                        table_name = f"local_bronze_data_{dataset}"
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * FROM read_parquet('{file_path}')
                        """)
                        # ✅ MIGRATION: Return table name instead of DataFrame
                        return table_name
                    else:
                        with open(file_path, "r") as f:
                            data = json.load(f)
                        table_name = f"local_bronze_data_{dataset}"
                        if isinstance(data, list):
                            # Create table from list using DuckDB
                            json_str = json.dumps(data)
                            self.conn.execute(f"""
                                CREATE TABLE {table_name} AS 
                                SELECT * FROM read_json_auto('{json_str}')
                            """)
                            return table_name
                        else:
                            # Create table from single item using DuckDB
                            json_str = json.dumps([data])
                            self.conn.execute(f"""
                                CREATE TABLE {table_name} AS 
                                SELECT * FROM read_json_auto('{json_str}')
                            """)
                            return table_name

            self.log.error(f"No data files found in {latest_dir_path}")
            return None
        else:
            # Read from GCS - find latest timestamped directory
            bronze_prefix = f"bronze/{dataset}/"
            try:
                blobs = list(
                    self.gcs_util.get_gcs_client().list_blobs(bucket_name, prefix=bronze_prefix)
                )
                if not blobs:
                    self.log.error(f"No bronze data found in GCS at {bronze_prefix}")
                    return None

                # Find the latest timestamped directory
                timestamped_blobs = [blob for blob in blobs if len(blob.name.split("/")) >= 4]
                if not timestamped_blobs:
                    self.log.error(f"No timestamped bronze data found in GCS at {bronze_prefix}")
                    return None

                latest_blob = max(timestamped_blobs, key=lambda x: x.updated)
                self.log.info(
                    f"Reading bronze data from GCS: gs://{bucket_name}/{latest_blob.name}"
                )

                if latest_blob.name.endswith(".parquet"):
                    # ✅ MIGRATION: Use modern GCS access instead of deprecated DataFrame approach
                    from unified_pipeline.util.gcs_access import GCSDataAccess

                    gcs_access = GCSDataAccess()
                    gcs_path = f"gs://{bucket_name}/{latest_blob.name}"

                    # Use the optimized temp download with base class DuckDB connection
                    with gcs_access._temp_download(gcs_path) as temp_file:
                        table_name = f"gcs_bronze_data_{dataset}"
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * FROM read_parquet('{temp_file}')
                        """)
                        return table_name
                elif latest_blob.name.endswith(".json"):
                    # Download JSON file and parse
                    temp_file = f"/tmp/bronze_temp_{dataset}.json"
                    latest_blob.download_to_filename(temp_file)
                    with open(temp_file, "r") as f:
                        data = json.load(f)
                    os.remove(temp_file)  # Clean up temp file

                    table_name = f"gcs_bronze_data_{dataset}"
                    if isinstance(data, list):
                        # Create table from list using DuckDB
                        json_str = json.dumps(data)
                        self.conn.execute(f"""
                            CREATE TABLE {table_name} AS 
                            SELECT * FROM read_json_auto('{json_str}')
                        """)
                        return table_name
                    else:
                        # Create table from single item using DuckDB
                        json_str = json.dumps([data])
                        self.conn.execute(f"""
                            CREATE TABLE {table_name} AS 
                            SELECT * FROM read_json_auto('{json_str}')
                        """)
                        return table_name
                else:
                    self.log.error(f"Unsupported file type: {latest_blob.name}")
                    return None

            except Exception as e:
                self.log.error(f"Failed to read bronze data from GCS: {e}")
                return None

    # Legacy methods - keeping for backward compatibility during transition
    @timed(name="Saving raw data")  # type: ignore
    def _save_raw_data(self, table_name: str, dataset: str, bucket_name: str) -> None:
        """
        Legacy method for saving raw data.

        DEPRECATED: Use _save_data() instead for consistent timestamped structure.
        This method is kept for backward compatibility during the refactoring transition.
        """
        self.log.warning("_save_raw_data is deprecated. Use _save_data() instead.")
        self._save_data(table_name, dataset, bucket=bucket_name, stage="bronze")

    def _save_raw_json(
        self, raw_data: list[str], dataset: str, bucket_name: str, filename="data"
    ) -> None:
        """
        Legacy method for saving raw JSON data.

        DEPRECATED: Use _save_data() instead for consistent timestamped structure.
        This method is kept for backward compatibility during the refactoring transition.
        """
        self.log.warning("_save_raw_json is deprecated. Use _save_data() instead.")
        self._save_data(raw_data, dataset, bucket=bucket_name, stage="bronze")

    # Legacy path methods - keeping for backward compatibility
    def _get_bronze_path(self, dataset: str, bucket_name: str, path: str) -> Optional[str]:
        """
        Legacy method for getting bronze data paths.

        DEPRECATED: This method is kept for backward compatibility during the refactoring transition.
        New code should use _read_bronze_data() which handles both in-memory and storage fallback.
        """
        self.log.warning("_get_bronze_path is deprecated. Use _read_bronze_data() instead.")
        # ✅ MIGRATION: Use DuckDB date functions instead of pandas
        current_date = self.conn.execute("SELECT strftime(current_date, '%Y-%m-%d')").fetchone()[0]

        # Download to temporary file
        temp_dir = f"/tmp/bronze/{dataset}"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = f"{temp_dir}/{current_date}.parquet"

        if self.config.save_local:
            return temp_file

        bucket = self.gcs_util.get_gcs_client().bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            self.log.error(f"Bronze data not found at {path}")
            return None
        blob.download_to_filename(temp_file)
        return temp_file

    def _get_latest_bronze_path(self, dataset: str, bucket_name: str):
        """
        Legacy method for getting the latest bronze data path.

        DEPRECATED: This method is kept for backward compatibility during the refactoring transition.
        New code should use _read_bronze_data() which handles both in-memory and storage fallback.
        """
        self.log.warning("_get_latest_bronze_path is deprecated. Use _read_bronze_data() instead.")
        bronze_path = f"bronze/{dataset}/"
        temp_dir = f"/tmp/bronze/{dataset}"

        if self.config.save_local:
            if not os.path.exists(temp_dir):
                self.log.error(f"Bronze data directory not found at {temp_dir}")
                return None

            # List all subdirectories (timestamp-based directories)
            subdirs = [d for d in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, d))]

            if not subdirs:
                self.log.error(f"No subdirectories found in {temp_dir}")
                return None

            # Find the latest directory by modification time
            latest_dir = max(
                subdirs,
                key=lambda x: os.path.getmtime(os.path.join(temp_dir, x)),
            )

            latest_dir_path = os.path.join(temp_dir, latest_dir)

            # List all files in the latest directory
            files = [
                f
                for f in os.listdir(latest_dir_path)
                if os.path.isfile(os.path.join(latest_dir_path, f))
            ]

            if not files:
                self.log.error(f"No files found in {latest_dir_path}")
                return None

            # Find the latest file by modification time
            latest_file = max(
                files,
                key=lambda x: os.path.getmtime(os.path.join(latest_dir_path, x)),
            )

            full_path = os.path.join(latest_dir_path, latest_file)
            self.log.info(f"Found latest bronze data at {full_path}")
            return full_path

        blobs = self.gcs_util.get_gcs_client().list_blobs(bucket_name, prefix=bronze_path)
        if not blobs:
            return None
        latest_blob = max(blobs, key=lambda x: x.updated)
        self.log.info(f"Latest bronze data found at {latest_blob.name}")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = f"{temp_dir}/{latest_blob.name.split('/')[-1]}"
        latest_blob.download_to_filename(temp_file)
        return temp_file

    def _generate_schema_if_enabled(
        self, data: Any, dataset: str, stage: str
    ) -> Optional[Dict[str, Any]]:
        """
        Generate and save schema information if dev mode or schema generation is enabled.

        Args:
            data: The data that was just processed/saved
            dataset: Name of the dataset
            stage: Processing stage (bronze, silver, gold)

        Returns:
            Schema information dict if generated, None otherwise
        """
        if not (self.config.dev_mode or self.config.generate_schemas) or not self._schema_manager:
            return None

        try:
            # Create a temporary table from the data to analyze
            temp_table_name = f"temp_{dataset}_{stage}_{self.date_pattern}"

            if isinstance(data, str):
                # Data is already a table name
                temp_table_name = data
            elif isinstance(data, (dict, list)):
                # Convert dict/list to table using DuckDB
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict):
                        json_data = json.dumps(data)
                        self.conn.execute(f"""
                            CREATE TABLE {temp_table_name} AS 
                            SELECT * FROM read_json_auto('{json_data}')
                        """)
                    else:
                        # List of values
                        values_str = ", ".join([f"'{v}'" for v in data])
                        self.conn.execute(f"""
                            CREATE TABLE {temp_table_name} AS 
                            SELECT unnest([{values_str}]) as value
                        """)
                elif isinstance(data, dict):
                    json_data = json.dumps([data])
                    self.conn.execute(f"""
                        CREATE TABLE {temp_table_name} AS 
                        SELECT * FROM read_json_auto('{json_data}')
                    """)
                else:
                    self.log.warning(f"Cannot generate schema for data type: {type(data)}")
                    return None
            else:
                self.log.warning(f"Cannot generate schema for data type: {type(data)}")
                return None

            # Generate comprehensive schema
            schema_info = self._schema_manager.get_table_schema(
                temp_table_name, include_summary=True
            )

            # Add metadata about the pipeline stage and dataset
            schema_info.update(
                {
                    "pipeline_stage": stage,
                    "dataset_name": dataset,
                    "source_class": self.__class__.__name__,
                    "data_type": str(type(data).__name__),
                    "row_count": len(data) if hasattr(data, "__len__") else None,
                }
            )

            # Save schema locally if enabled
            if self.config.save_schemas_locally:
                schema_dir = f"schemas/{stage}"
                self._schema_manager.save_schema_locally(
                    f"{dataset}_{stage}", schema_info, schema_dir
                )

            # Save schema to GCS if not in local-only mode
            if not self.config.save_local:
                gcs_path = f"schemas/{stage}/{self.__class__.__name__}"
                self._schema_manager.save_schema_to_gcs(f"{dataset}_{stage}", schema_info, gcs_path)

            # Clean up temporary table
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

            self.log.info(f"Schema generated for {stage}/{dataset}")
            return schema_info

        except Exception as e:
            self.log.error(f"Failed to generate schema for {stage}/{dataset}: {e}")
            return None

    def get_schema_for_llm(self, dataset: str, stage: str = "silver") -> Optional[str]:
        """
        Get a concise schema description optimized for LLM consumption.

        This method reads the most recent data for the specified dataset and stage,
        creates a temporary table, and returns a formatted schema description.

        Args:
            dataset: Name of the dataset
            stage: Processing stage (bronze, silver, gold)

        Returns:
            Formatted schema string for LLM consumption, or None if unavailable
        """
        if not self._schema_manager:
            self._schema_manager = NativeSchemaManager(self.conn, self.log)

        try:
            # Try to read the data from storage
            data = self._read_data_from_storage(dataset, self.config.bucket, stage)
            if data is None:
                self.log.warning(f"No data found for {stage}/{dataset}")
                return None

            # Create temporary table
            temp_table_name = f"temp_{dataset}_{stage}_llm"

            if isinstance(data, str):
                # Data is already a table name
                temp_table_name = data
            else:
                self.log.warning(f"Cannot create schema for data type: {type(data)}")
                return None

            # Use DuckDB's DESCRIBE for a clean, simple format
            query = f"DESCRIBE {temp_table_name}"
            results = self.conn.execute(query).fetchall()

            schema_lines = [f"Table: {dataset} ({stage} stage)"]
            schema_lines.append("Columns:")

            for row in results:
                col_name, col_type, nullable, key, default, extra = row
                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                key_str = f" ({key})" if key else ""
                default_str = f" DEFAULT {default}" if default else ""

                schema_lines.append(
                    f"  - {col_name}: {col_type} {nullable_str}{key_str}{default_str}"
                )

            # Add row count info
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {temp_table_name}").fetchone()
            if count_result:
                schema_lines.append(f"Row count: {count_result[0]:,}")

            # Clean up
            self.conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

            return "\n".join(schema_lines)

        except Exception as e:
            self.log.error(f"Failed to generate LLM schema for {stage}/{dataset}: {e}")
            return None

    def list_available_schemas(self) -> Dict[str, Any]:
        """
        List all available schemas in the local schemas directory and GCS.

        Returns:
            Dictionary with information about available schemas
        """
        schemas_info = {"local_schemas": {}, "gcs_schemas": {}, "available_datasets": set()}

        # Check local schemas
        local_schema_dir = "schemas"
        if os.path.exists(local_schema_dir):
            for stage in ["bronze", "silver", "gold"]:
                stage_dir = os.path.join(local_schema_dir, stage)
                if os.path.exists(stage_dir):
                    schemas_info["local_schemas"][stage] = []
                    for file in os.listdir(stage_dir):
                        if file.endswith("_schema_*.json"):
                            dataset_name = file.split("_schema_")[0]
                            schemas_info["available_datasets"].add(dataset_name)
                            schemas_info["local_schemas"][stage].append(
                                {
                                    "dataset": dataset_name,
                                    "file": file,
                                    "path": os.path.join(stage_dir, file),
                                }
                            )

        # Check GCS schemas if not in local-only mode
        if not self.config.save_local:
            try:
                for stage in ["bronze", "silver", "gold"]:
                    prefix = f"schemas/{stage}/"
                    files = self.gcs_util.list_files(bucket_name=self.config.bucket, prefix=prefix)
                    if files:
                        schemas_info["gcs_schemas"][stage] = []
                        for file in files:
                            if "_schema_" in file.name and file.name.endswith(".json"):
                                dataset_name = file.name.split("/")[-1].split("_schema_")[0]
                                schemas_info["available_datasets"].add(dataset_name)
                                schemas_info["gcs_schemas"][stage].append(
                                    {
                                        "dataset": dataset_name,
                                        "file": file.name,
                                        "gcs_path": f"gs://{self.config.bucket}/{file.name}",
                                        "created": file.time_created,
                                    }
                                )
            except Exception as e:
                self.log.warning(f"Could not list GCS schemas: {e}")

        schemas_info["available_datasets"] = list(schemas_info["available_datasets"])
        return schemas_info

    def generate_standardized_schema_documentation(self, tables: list[str], stage: str) -> None:
        """
        Generate standardized schema documentation using the unified schema documentation system.

        This method integrates with the project-wide schema documentation system to create
        consistent documentation that gets committed to GitHub.

        Args:
            tables: List of table names to document
            stage: Processing stage (should NOT be 'bronze' due to memory constraints)

        Note:
            Bronze stage documentation is not supported due to memory constraints with large raw datasets.
            Use 'silver', 'gold', or other processed stages instead.
        """
        if not self._standardized_schema_manager:
            self.log.warning("Standardized schema documentation not available")
            return

        # Safeguard against bronze documentation
        if stage.lower() == "bronze":
            self.log.warning(
                "Skipping schema documentation for bronze stage due to memory constraints. "
                "Bronze layers often contain massive raw datasets that would cause memory issues."
            )
            return

        try:
            self.log.info(f"Generating standardized schema documentation for {stage} stage...")
            self._standardized_schema_manager.generate_all_documentation(tables, stage=stage)
            self._standardized_schema_manager.commit_to_github()
            self.log.info("Standardized schema documentation generated and committed")

        except Exception as e:
            self.log.warning(f"Standardized schema documentation failed (non-critical): {e}")
            # Don't fail the pipeline if documentation fails

    def read_silver_data_direct(self, dataset: str, table_name: str = None) -> str:
        """
        ✅ OPTIMAL: Read silver data directly into DuckDB table - NO DataFrame conversion.

        This is the recommended method for maximum performance:
        - No DataFrame conversion bottleneck
        - Direct DuckDB table creation
        - Can be used for further DuckDB operations

        Args:
            dataset: Dataset name to read
            table_name: Name for DuckDB table (defaults to dataset name)

        Returns:
            str: Name of the created DuckDB table
        """
        gcs_path = self._get_latest_silver_path(dataset)
        table_name = table_name or f"silver_{dataset}"

        # Create table directly in DuckDB without DataFrame conversion
        self.gcs_access.query_parquet_direct(gcs_path, "SELECT *", table_name)
        return table_name

    def read_silver_data_with_filter_direct(
        self, dataset: str, filter_condition: str, table_name: str = None
    ) -> str:
        """
        ✅ OPTIMAL: Read silver data with filtering directly into DuckDB table - NO DataFrame conversion.
        """
        gcs_path = self._get_latest_silver_path(dataset)
        table_name = table_name or f"silver_{dataset}_filtered"

        query = f"SELECT * WHERE {filter_condition}"
        self.gcs_access.query_parquet_direct(gcs_path, query, table_name)
        return table_name

    def save_data_direct(self, table_name: str, dataset: str, bucket: str, stage: str) -> str:
        """
        ✅ OPTIMIZED: Save DuckDB table directly to GCS without DataFrame conversion.

        This method provides maximum performance by:
        - Avoiding DataFrame conversion bottleneck (2-3x faster)
        - Using optimized Parquet export settings
        - Direct streaming to GCS with gcsfs

        Args:
            table_name: Name of DuckDB table to export
            dataset: Dataset name for GCS path
            bucket: GCS bucket name
            stage: Processing stage (bronze, silver, gold)

        Returns:
            str: GCS path where data was saved
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_path = f"gs://{bucket}/{stage}/{dataset}/{timestamp}/data.parquet"

        # ✅ OPTIMIZED: Export directly from instance's DuckDB connection
        import shutil
        import tempfile

        # Build COPY options for optimal Parquet export
        copy_options = ["FORMAT PARQUET"]
        copy_options.append("COMPRESSION zstd")
        copy_options.append("ROW_GROUP_SIZE 100000")
        options_str = ", ".join(copy_options)

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            # Export from instance's DuckDB connection where table was created
            self.conn.execute(f"COPY {table_name} TO '{tmp.name}' ({options_str})")

            # Stream copy to GCS without loading into memory
            with open(tmp.name, "rb") as src:
                with self.gcs_access.fs.open(gcs_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)

        self.log.info(f"✅ Saved {table_name} directly to {gcs_path} (optimized)")
        return gcs_path

    def query_data_direct(self, dataset: str, query: str, table_name: str) -> str:
        """
        ✅ OPTIMIZED: Query silver data and create DuckDB table directly (no DataFrame).

        Args:
            dataset: Dataset name to query
            query: SQL query to execute
            table_name: Name for the resulting DuckDB table

        Returns:
            str: Name of created DuckDB table
        """
        gcs_path = self._get_latest_silver_path(dataset)
        self.gcs_access.query_parquet_direct(gcs_path, query, table_name)
        return table_name

    def _get_latest_silver_path(self, dataset: str) -> str:
        """Get path to latest silver data file."""
        pattern = f"gs://{self.config.bucket}/silver/{dataset}/*/data.parquet"
        files = self.gcs_access.list_files(pattern)
        if not files:
            raise FileNotFoundError(f"No silver data found for {dataset}")
        return sorted(files)[-1]  # Latest by timestamp

    def _get_available_fvm_marker_years(self) -> list[int]:
        """Get all available fvm_marker years from GCS storage."""
        try:
            # List all files in silver layer to extract directory names
            files = self.gcs_util.list_files(bucket_name=self.config.bucket, prefix="silver/")
            years = set()

            for file_blob in files:
                # Extract years from blob names like "silver/fvm_marker_2021/timestamp/data.parquet"
                match = re.search(r"silver/fvm_marker_(\d{4})/", file_blob.name)
                if match:
                    year = int(match.group(1))
                    years.add(year)

            return sorted(list(years))
        except Exception as e:
            self.log.error(f"Error discovering fvm_marker years: {e}")
            return []
