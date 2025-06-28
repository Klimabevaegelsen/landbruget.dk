"""
Base classes for data sources in the unified pipeline.

This module defines the abstract base classes that all data sources in
the unified pipeline must implement. It provides common functionality and
enforces a consistent interface across different data sources and stages.
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, Optional, TypeVar

import duckdb
import geopandas as gpd
import pandas as pd
from pydantic import BaseModel

from unified_pipeline.common.native_schema_manager import NativeSchemaManager
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
        gcs_util: Google Cloud Storage utility instance
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
        self.gcs_util = gcs_util
        self.log = Logger.get_logger()
        self.conn = duckdb.connect(database=":memory:")
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
    def _save_data(self, data: Any, dataset: str, bucket: str, stage: str) -> None:
        """Save data with support for gold stage."""

        valid_stages = ["bronze", "silver", "gold"]
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage '{stage}'. Must be one of {valid_stages}")

        # Rest of implementation...
        # Use pipeline start time (not save time) for consistent timestamping
        timestamp = self.date_pattern

        # Determine file extension and format
        if isinstance(data, (gpd.GeoDataFrame, pd.DataFrame)):
            file_extension = "parquet"
            filename = f"{dataset}.{file_extension}"
        elif isinstance(data, (dict, list)):
            file_extension = "json"
            filename = f"{dataset}.{file_extension}"
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        # Create path with timestamp subdirectory
        path = f"{stage}/{dataset}/{timestamp}/{filename}"

        if self.config.save_local:
            # Save locally
            local_path = f"/tmp/{filename}"

            if isinstance(data, gpd.GeoDataFrame):
                data.to_parquet(local_path)
            elif isinstance(data, pd.DataFrame):
                data.to_parquet(local_path)
            elif isinstance(data, (dict, list)):
                with open(local_path, "w") as f:
                    json.dump(data, f, indent=2, default=str)

            self.log.info(f"Data saved locally to {local_path}")
        else:
            # Save to GCS
            if isinstance(data, gpd.GeoDataFrame):
                self.gcs_util.upload_geopandas_to_gcs(
                    gdf=data, bucket_name=bucket, blob_name=path, file_format="parquet"
                )
            elif isinstance(data, pd.DataFrame):
                self.gcs_util.upload_pandas_to_gcs(
                    df=data, bucket_name=bucket, blob_name=path, file_format="parquet"
                )
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
                # Try reading as GeoDataFrame first, fallback to regular DataFrame
                try:
                    return self.gcs_util.download_geopandas_from_gcs(
                        bucket_name=bucket, blob_name=latest_file.name
                    )
                except Exception as geo_error:
                    if "Missing geo metadata" in str(geo_error):
                        # Fallback to regular pandas DataFrame for non-geo data
                        import os
                        import tempfile

                        import pandas as pd

                        with tempfile.NamedTemporaryFile(
                            suffix=".parquet", delete=False
                        ) as tmp_file:
                            temp_path = tmp_file.name

                        try:
                            self.gcs_util.download_file(bucket, latest_file.name, temp_path)
                            df = pd.read_parquet(temp_path)
                            self.log.info(
                                f"Successfully loaded {len(df)} records from {latest_file.name} as regular DataFrame"
                            )
                            return df
                        finally:
                            if os.path.exists(temp_path):
                                os.unlink(temp_path)
                    else:
                        raise geo_error

            elif latest_file.name.endswith(".json"):
                return self.gcs_util.download_json_from_gcs(
                    bucket_name=bucket, blob_name=latest_file.name
                )
            else:
                self.log.error(f"Unsupported file type: {latest_file.name}")
                return None

        except Exception as e:
            self.log.error(f"Failed to read {stage} data for {dataset}: {e}")
            return None

    @timed(name="Reading bronze data")  # type: ignore
    def _read_bronze_data(
        self, dataset: str, bucket_name: str, bronze_data: Optional[Any] = None
    ) -> Optional[pd.DataFrame]:
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
            Optional[pd.DataFrame]: A DataFrame containing the bronze layer data,
                                   or None if no data is found

        Raises:
            Exception: If there are issues reading the data
        """
        # Prefer in-memory data if available
        if bronze_data is not None:
            self.log.info("Using bronze data from memory (in-memory data passing)")
            if isinstance(bronze_data, pd.DataFrame):
                return bronze_data
            elif isinstance(bronze_data, (dict, list)):
                # Convert JSON data to DataFrame if needed
                if isinstance(bronze_data, list) and len(bronze_data) > 0:
                    if isinstance(bronze_data[0], str):
                        # List of strings (e.g., XML payloads)
                        return pd.DataFrame({"payload": bronze_data})
                    else:
                        # List of dicts
                        return pd.DataFrame(bronze_data)
                elif isinstance(bronze_data, dict):
                    return pd.DataFrame([bronze_data])
            else:
                self.log.warning(f"Unsupported bronze_data type: {type(bronze_data)}")
                return None

        # Fallback to reading from storage
        self.log.info("Reading bronze data from storage (fallback)")
        return self._read_bronze_data_from_storage(dataset, bucket_name)

    def _read_bronze_data_from_storage(
        self, dataset: str, bucket_name: str
    ) -> Optional[pd.DataFrame]:
        """
        Read bronze data from storage using the latest timestamped directory.

        Args:
            dataset: The name of the dataset to read
            bucket_name: The name of the GCS bucket

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the bronze layer data,
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
                        return pd.read_parquet(file_path)
                    else:
                        with open(file_path, "r") as f:
                            data = json.load(f)
                        if isinstance(data, list):
                            return pd.DataFrame({"payload": data})
                        else:
                            return pd.DataFrame([data])

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
                    # Get the file path and read with DuckDB
                    parquet_path = self.gcs_util.read_dataframe_from_gcs(
                        bucket_name, latest_blob.name
                    )
                    # Read parquet file using DuckDB and convert to pandas DataFrame for compatibility
                    df = self.conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
                    # Clean up temporary file
                    import os
                    import shutil

                    temp_dir = os.path.dirname(parquet_path)
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    return df
                elif latest_blob.name.endswith(".json"):
                    # Download JSON file and parse
                    temp_file = f"/tmp/bronze_temp_{dataset}.json"
                    latest_blob.download_to_filename(temp_file)
                    with open(temp_file, "r") as f:
                        data = json.load(f)
                    os.remove(temp_file)  # Clean up temp file

                    if isinstance(data, list):
                        return pd.DataFrame({"payload": data})
                    else:
                        return pd.DataFrame([data])
                else:
                    self.log.error(f"Unsupported file type: {latest_blob.name}")
                    return None

            except Exception as e:
                self.log.error(f"Failed to read bronze data from GCS: {e}")
                return None

    # Legacy methods - keeping for backward compatibility during transition
    @timed(name="Saving raw data")  # type: ignore
    def _save_raw_data(self, df: pd.DataFrame, dataset: str, bucket_name: str) -> None:
        """
        Legacy method for saving raw data.

        DEPRECATED: Use _save_data() instead for consistent timestamped structure.
        This method is kept for backward compatibility during the refactoring transition.
        """
        self.log.warning("_save_raw_data is deprecated. Use _save_data() instead.")
        self._save_data(df, dataset, bucket=bucket_name, stage="bronze")

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
        # Define the path to the bronze data
        current_date = pd.Timestamp.now().strftime("%Y-%m-%d")

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

            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                # Register DataFrame as a temporary table in DuckDB
                self.conn.register(temp_table_name, data)
            elif isinstance(data, (dict, list)):
                # Convert dict/list to DataFrame and register
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict):
                        temp_df = pd.DataFrame(data)
                    else:
                        temp_df = pd.DataFrame({"value": data})
                elif isinstance(data, dict):
                    temp_df = pd.DataFrame([data])
                else:
                    self.log.warning(f"Cannot generate schema for data type: {type(data)}")
                    return None

                self.conn.register(temp_table_name, temp_df)
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

            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                self.conn.register(temp_table_name, data)
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
