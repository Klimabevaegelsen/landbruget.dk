"""
Google Cloud Storage utility module for interacting with GCS resources.

This module provides a simplified interface for common GCS operations such as
retrieving files, accessing buckets and working with blobs. It implements
a singleton pattern to ensure only one GCS client exists throughout the application.
"""

from typing import Optional

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
