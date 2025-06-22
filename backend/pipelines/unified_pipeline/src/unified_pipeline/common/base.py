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
from typing import Any, Generic, Optional, TypeVar, Union

import duckdb
import geopandas as gpd
import pandas as pd
from pydantic import BaseModel

from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger
from unified_pipeline.util.timing import timed


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
    from bronze jobs without requiring disk I/O.
    """

    @abstractmethod
    async def run(self, bronze_data: Optional[Any] = None) -> None:
        """
        Run silver processing with optional in-memory bronze data.

        Args:
            bronze_data: Optional data from bronze stage. If None,
                        silver job should read from storage.
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
        self.date_pattern = datetime.now().strftime("%Y%m%d_%H%M%S")

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
        data: Union[pd.DataFrame, gpd.GeoDataFrame, dict, list],
        dataset: str,
        bucket_name: str,
        stage: str = "bronze",
        filename: str = "data",
    ) -> str:
        """
        Save processed data to Google Cloud Storage using consistent timestamped structure.

        This method saves data to GCS using the standardized timestamped subdirectory pattern:
        {stage}/{dataset}/{timestamp}/{filename}.{ext}

        Args:
            data: The data to save (DataFrame, GeoDataFrame, dict, or list)
            dataset: The name of the dataset, used to determine the save path
            bucket_name: The name of the GCS bucket to save the data
            stage: The processing stage (bronze, silver, etc.)
            filename: The filename to use (without extension)

        Returns:
            str: The path where the data was saved for in-memory passing

        Raises:
            Exception: If there are issues saving the data
        """
        if data is None:
            self.log.warning("No data to save")
            return ""

        # Create timestamped directory structure
        temp_dir = f"/tmp/{stage}/{dataset}/{self.date_pattern}"
        os.makedirs(temp_dir, exist_ok=True)

        # Determine file extension and save method based on data type
        if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
            temp_file = f"{temp_dir}/{filename}.parquet"
            data.to_parquet(temp_file)
            self.log.info(f"Saving {stage} data: records: {data.shape[0]:,}")
        elif isinstance(data, (dict, list)):
            temp_file = f"{temp_dir}/{filename}.json"
            with open(temp_file, "w") as f:
                json.dump(data, f)
            self.log.info(
                f"Saving {stage} data: JSON with {len(data) if isinstance(data, list) else len(data.keys())} items"
            )
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        # Save locally or upload to GCS
        if self.config.save_local:
            self.log.info(f"Saved {stage} data locally at {temp_file}")
        else:
            # Upload to GCS with timestamped structure
            bucket = self.gcs_util.get_gcs_client().bucket(bucket_name)
            blob_path = f"{stage}/{dataset}/{self.date_pattern}/{filename}.{'parquet' if temp_file.endswith('.parquet') else 'json'}"
            working_blob = bucket.blob(blob_path)
            working_blob.upload_from_filename(temp_file)
            self.log.info(f"Uploaded {stage} data to: gs://{bucket_name}/{blob_path}")

        return temp_file

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
                    return self.gcs_util.read_dataframe_from_gcs(bucket_name, latest_blob.name)
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
        self._save_data(df, dataset, bucket_name, stage="bronze", filename="data")

    def _save_raw_json(
        self, raw_data: list[str], dataset: str, bucket_name: str, filename="data"
    ) -> None:
        """
        Legacy method for saving raw JSON data.

        DEPRECATED: Use _save_data() instead for consistent timestamped structure.
        This method is kept for backward compatibility during the refactoring transition.
        """
        self.log.warning("_save_raw_json is deprecated. Use _save_data() instead.")
        self._save_data(raw_data, dataset, bucket_name, stage="bronze", filename=filename)

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
