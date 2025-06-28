"""
Google Cloud Storage utility module for interacting with GCS resources.

This module provides a simplified interface for common GCS operations such as
retrieving files, accessing buckets and working with blobs. It implements
a singleton pattern to ensure only one GCS client exists throughout the application.
"""

import json
import os
import tempfile
from typing import Optional, Union

import geopandas as gpd
import pandas as pd
from google.auth import exceptions
from google.cloud import storage
from google.cloud.storage import Blob, Client
from google.cloud.storage.bucket import Bucket
from simple_singleton import Singleton

from unified_pipeline.model.app_config import GCSConfig
from unified_pipeline.util.log_util import Logger


class GCSUtil(metaclass=Singleton):
    """
    Singleton utility class for Google Cloud Storage operations.

    This class handles authentication and provides methods for interacting with
    GCS resources including buckets and blobs. It supports authentication via
    Application Default Credentials (ADC) or service account key files.

    Attributes:
        log: Logger instance for logging operations and errors
        gcs_config: Configuration containing GCS authentication settings
        gcs_client: Singleton instance of the Google Cloud Storage client
    """

    def __init__(self, gcs_config: Optional[GCSConfig] = None) -> None:
        """
        Initialize the GCSUtil with optional configuration.

        Args:
            gcs_config (Optional[GCSConfig]): Configuration for GCS authentication
        """
        self.log = Logger.get_logger()
        self.gcs_config = gcs_config
        self.gcs_client: Optional[Client] = None

    def get_gcs_client(self) -> Client:
        """
        Get or create a singleton instance of the Google Cloud Storage client.

        This method first attempts to authenticate using Application Default
        Credentials (ADC). If ADC fails, it falls back to using a service account
        key file specified in the configuration.

        Returns:
            Client: Authenticated Google Cloud Storage client

        Raises:
            ValueError: If authentication fails using both ADC and service account key
        """
        if self.gcs_client is None:
            self.log.info("Initializing google cloud storage client...")
            self.gcs_client = self._get_gcs_client_using_adc()
            if self.gcs_client is None:
                self.log.info(
                    "Unable to initialize google cloud storage client using ADC. "
                    "Attempting to initialize using service account key file."
                )
                if self.gcs_config is None:
                    raise ValueError(
                        "Google cloud configs are not set. Unable to initialize "
                        "google cloud storage client."
                    )
                if self.gcs_config.credentials_path is None:
                    raise ValueError(
                        "Google cloud storage credentials_path is not set. "
                        "Set the path to the service account key file."
                        "Unable to initialize google cloud storage client."
                    )
                self.gcs_client = self._get_gcs_client_using_file(self.gcs_config.credentials_path)
                if self.gcs_client is None:
                    raise ValueError(
                        "Unable to initialize google cloud storage client "
                        "using service account key file."
                    )
        return self.gcs_client

    def _get_gcs_client_using_adc(self) -> Optional[Client]:
        """
        Get Google Cloud Storage client using Application Default Credentials.

        This method attempts to authenticate using the credentials of the service
        account associated with the Cloud Run service or local environment.

        Returns:
            Optional[Client]: Authenticated client if successful, None otherwise

        Example:
            >>> client = GCSUtil()._get_gcs_client_using_adc()
            >>> if client:
            >>>     print("Successfully authenticated using ADC")
        """
        try:
            # Attempt to use Application Default Credentials (ADC)
            return storage.Client(project="landbrugsdata-1")
        except exceptions.DefaultCredentialsError as e:
            self.log.error(f"Unable to obtain credentials using ADC. Error: {e}")
            return None

    def _get_gcs_client_using_file(self, file: str) -> Optional[Client]:
        """
        Get Google Cloud Storage client using a service account key file.

        This method authenticates using credentials from a JSON key file
        downloaded from the Google Cloud Console.

        Args:
            file (str): Path to the service account JSON key file

        Returns:
            Optional[Client]: Authenticated client if successful, None otherwise

        Example:
            >>> client = GCSUtil()._get_gcs_client_using_file("/path/to/key.json")
        """
        try:
            client: Client = Client.from_service_account_json(file)
            return client
        except Exception as e:
            self.log.error(f"Unable to obtain credentials from file={file}. Error: {e}")
            return None

    def get_bucket(self, bucket_name: str) -> Bucket:
        """
        Get a GCS bucket by name.

        Args:
            bucket_name (str): Name of the bucket to retrieve

        Returns:
            Bucket: The requested GCS bucket

        Raises:
            ValueError: If the bucket doesn't exist
            Exception: If there's an error accessing the bucket

        Example:
            >>> bucket = GCSUtil().get_bucket("my-data-bucket")
            >>> print(f"Bucket {bucket.name} exists")
        """
        bucket: Optional[Bucket] = None
        try:
            bucket = self.get_gcs_client().get_bucket(bucket_name)
        except Exception as e:
            self.log.error(f"Error getting bucket: {e}")
            raise e
        if bucket is None:
            self.log.error(f"Bucket {bucket_name} not found.")
            raise ValueError(f"Bucket {bucket_name} not found.")
        return bucket

    def get_blob(self, bucket_name: str, blob_name: str) -> Blob:
        """
        Get a blob (file) from a GCS bucket.

        Args:
            bucket_name (str): Name of the bucket containing the blob
            blob_name (str): Path to the blob within the bucket

        Returns:
            Blob: The requested GCS blob object

        Raises:
            ValueError: If the blob doesn't exist

        Example:
            >>> blob = GCSUtil().get_blob("my-bucket", "data/file.json")
            >>> print(f"Blob size: {blob.size} bytes")
        """
        bucket = self.get_bucket(bucket_name)
        blob: Optional[Blob] = bucket.get_blob(blob_name)
        if blob is None:
            self.log.error(f"Blob {blob_name} not found in bucket {bucket_name}.")
            raise ValueError(f"Blob {blob_name} not found in bucket {bucket_name}.")
        return blob

    def get_file_as_string(self, bucket_name: str, blob_name: str) -> str:
        """
        Download a file from GCS and return its contents as a string.

        Args:
            bucket_name (str): Name of the bucket containing the file
            blob_name (str): Path to the file within the bucket

        Returns:
            str: Contents of the file as a UTF-8 string

        Example:
            >>> content = GCSUtil().get_file_as_string("my-bucket", "data/config.json")
            >>> print(f"File content: {content[:100]}...")
        """
        blob = self.get_blob(bucket_name, blob_name)
        return str(blob.download_as_text(encoding="utf-8"))

    def get_file_as_string_from_url(self, url: str) -> str:
        """
        Download a file from a GCS URL and return its contents as a string.

        The URL should be in the format "gs://bucket-name/path/to/file".

        Args:
            url (str): GCS URL to the file

        Returns:
            str: Contents of the file as a UTF-8 string

        Example:
            >>> content = GCSUtil().get_file_as_string_from_url("gs://my-bucket/data/config.json")
            >>> print(f"File content: {content[:100]}...")
        """
        bucket_name, blob_name = self.get_bucket_and_blob_name_from_url(url)
        self.log.info(f"Fetching file from bucket_name={bucket_name}, blob_name={blob_name}")
        return self.get_file_as_string(bucket_name, blob_name)

    def get_bucket_and_blob_name_from_url(self, url: str) -> tuple[str, str]:
        """
        Parse a GCS URL into bucket name and blob path components.

        Args:
            url (str): GCS URL in the format "gs://bucket-name/path/to/file"

        Returns:
            tuple[str, str]: A tuple containing (bucket_name, blob_name)

        Example:
            >>> bucket, blob = GCSUtil().get_bucket_and_blob_name_from_url("gs://my-bucket/data/file.txt")
            >>> print(f"Bucket: {bucket}, Blob: {blob}")
            Bucket: my-bucket, Blob: data/file.txt
        """
        # Remove the leading gs://
        url = url[5:]
        # Split the url by the first occurrence of /
        bucket_name, blob_name = url.split("/", 1)
        return bucket_name, blob_name

    def list_files(self, bucket_name: str, prefix: str):
        """
        List files in a GCS bucket with a given prefix.

        Args:
            bucket_name (str): Name of the bucket
            prefix (str): Prefix to filter files

        Returns:
            List of blob objects
        """
        bucket = self.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return list(blobs)

    def download_file(self, bucket_name: str, source_blob_name: str, destination_file_name: str):
        """
        Download a file from GCS to local filesystem with timeout and retry support.

        Args:
            bucket_name (str): Name of the bucket
            source_blob_name (str): Name of the source blob
            destination_file_name (str): Path to save the file locally
        """
        import time

        from google.api_core import exceptions as gcs_exceptions
        from google.api_core import retry

        blob = self.get_blob(bucket_name, source_blob_name)

        # Get file size for progress tracking
        file_size = blob.size
        file_size_mb = file_size / (1024 * 1024) if file_size else 0

        self.log.info(
            f"Starting download of {source_blob_name} ({file_size_mb:.1f} MB) to {destination_file_name}"
        )

        # Configure retry strategy for large files
        download_retry = retry.Retry(
            predicate=retry.if_exception_type(
                gcs_exceptions.TooManyRequests,
                gcs_exceptions.InternalServerError,
                gcs_exceptions.ServiceUnavailable,
                ConnectionError,
                TimeoutError,
            ),
            deadline=1800,  # 30 minutes total timeout for large files
            initial=1.0,
            maximum=60.0,
            multiplier=2.0,
        )

        start_time = time.time()

        try:
            # Download with retry logic and timeout
            blob.download_to_filename(destination_file_name, retry=download_retry, timeout=1800)

            elapsed_time = time.time() - start_time
            download_speed = file_size_mb / elapsed_time if elapsed_time > 0 else 0

            self.log.info(f"Downloaded {source_blob_name} to {destination_file_name}")
            self.log.info(f"Download completed in {elapsed_time:.1f}s at {download_speed:.1f} MB/s")

        except Exception as e:
            self.log.error(f"Failed to download {source_blob_name}: {e}")
            # Clean up partial download
            import os

            if os.path.exists(destination_file_name):
                try:
                    os.unlink(destination_file_name)
                    self.log.info(f"Cleaned up partial download: {destination_file_name}")
                except Exception as cleanup_e:
                    self.log.warning(f"Failed to clean up partial download: {cleanup_e}")
            raise

    def download_geopandas_from_gcs(self, bucket_name: str, blob_name: str):
        """
        Download a parquet file from GCS and return as GeoDataFrame.

        Args:
            bucket_name (str): Name of the GCS bucket
            blob_name (str): Path to the parquet file in the bucket

        Returns:
            GeoDataFrame: The downloaded data as a GeoPandas DataFrame
        """
        import os
        import tempfile

        import geopandas as gpd

        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            # Download file to temporary location
            self.download_file(bucket_name, blob_name, temp_path)

            # Read as GeoDataFrame
            gdf = gpd.read_parquet(temp_path)
            self.log.info(f"Successfully loaded {len(gdf)} records from {blob_name}")
            return gdf

        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def upload_pandas_to_gcs(
        self, df: pd.DataFrame, bucket_name: str, blob_name: str, file_format: str = "parquet"
    ) -> None:
        """
        Upload a pandas DataFrame to GCS.

        Args:
            df: DataFrame to upload
            bucket_name: Name of the GCS bucket
            blob_name: Path to save the file in the bucket
            file_format: Format to save the file ('parquet' or 'csv')
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        with tempfile.NamedTemporaryFile(suffix=f".{file_format}", delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            if file_format == "parquet":
                df.to_parquet(temp_path, index=False)
            elif file_format == "csv":
                df.to_csv(temp_path, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            blob.upload_from_filename(temp_path)
            self.log.info(f"Uploaded DataFrame ({len(df)} rows) to gs://{bucket_name}/{blob_name}")

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def upload_geopandas_to_gcs(
        self, gdf: gpd.GeoDataFrame, bucket_name: str, blob_name: str, file_format: str = "parquet"
    ) -> None:
        """
        Upload a GeoPandas GeoDataFrame to GCS.

        Args:
            gdf: GeoDataFrame to upload
            bucket_name: Name of the GCS bucket
            blob_name: Path to save the file in the bucket
            file_format: Format to save the file ('parquet' or 'geojson')
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        with tempfile.NamedTemporaryFile(suffix=f".{file_format}", delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            if file_format == "parquet":
                gdf.to_parquet(temp_path, index=False)
            elif file_format == "geojson":
                gdf.to_file(temp_path, driver="GeoJSON")
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            blob.upload_from_filename(temp_path)
            self.log.info(
                f"Uploaded GeoDataFrame ({len(gdf)} rows) to gs://{bucket_name}/{blob_name}"
            )

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def upload_json_to_gcs(self, data: Union[dict, list], bucket_name: str, blob_name: str) -> None:
        """
        Upload JSON data to GCS.

        Args:
            data: Data to upload (dict or list)
            bucket_name: Name of the GCS bucket
            blob_name: Path to save the file in the bucket
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        json_str = json.dumps(data, indent=2, default=str)
        blob.upload_from_string(json_str, content_type="application/json")
        self.log.info(f"Uploaded JSON data to gs://{bucket_name}/{blob_name}")

    def upload_file(
        self, bucket_name: str, source_file_path: str, destination_blob_name: str
    ) -> None:
        """
        Upload a local file to GCS.

        Args:
            bucket_name: Name of the GCS bucket
            source_file_path: Path to the local file to upload
            destination_blob_name: Path to save the file in the bucket
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Get file size for logging
        file_size = os.path.getsize(source_file_path)
        file_size_mb = file_size / (1024 * 1024)

        self.log.info(
            f"Uploading {source_file_path} ({file_size_mb:.1f} MB) to gs://{bucket_name}/{destination_blob_name}"
        )

        blob.upload_from_filename(source_file_path)
        self.log.info(f"Successfully uploaded file to gs://{bucket_name}/{destination_blob_name}")

    def download_json_from_gcs(self, bucket_name: str, blob_name: str) -> Union[dict, list]:
        """
        Download JSON data from GCS.

        Args:
            bucket_name: Name of the GCS bucket
            blob_name: Path to the file in the bucket

        Returns:
            The JSON data as a dict or list
        """
        blob = self.get_blob(bucket_name, blob_name)
        json_str = blob.download_as_text(encoding="utf-8")
        return json.loads(json_str)

    def read_dataframe_from_gcs(self, bucket_name: str, blob_name: str) -> str:
        """
        Download a parquet file from GCS and return the local path for DuckDB to read directly.

        Args:
            bucket_name (str): Name of the GCS bucket
            blob_name (str): Path to the parquet file in the bucket

        Returns:
            str: Path to the downloaded parquet file that DuckDB can read directly
        """
        import os
        import tempfile

        # Create temporary file that persists for DuckDB to read
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, f"data_{hash(blob_name)}.parquet")

        # Download file to temporary location
        self.download_file(bucket_name, blob_name, temp_path)

        self.log.info(f"Downloaded {blob_name} to {temp_path} for DuckDB processing")
        return temp_path
