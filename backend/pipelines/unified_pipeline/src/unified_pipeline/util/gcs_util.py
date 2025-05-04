from typing import Optional

from google.auth import exceptions
from google.cloud import storage
from google.cloud.storage import Blob, Client
from google.cloud.storage.bucket import Bucket
from simple_singleton import Singleton

from unified_pipeline.model.app_config import GCSConfig
from unified_pipeline.util.log_util import Logger


class GCSUtil(metaclass=Singleton):
    def __init__(self, gcs_config: Optional[GCSConfig] = None) -> None:
        self.log = Logger.get_logger()
        self.gcs_config = gcs_config
        self.gcs_client: Optional[Client] = None

    def get_gcs_client(self) -> Client:
        """
        Returns a singleton instance of the google cloud storage client.
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
                        "Google cloud configs are not set. Unable to initialize google cloud storage client."
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
                        "Unable to initialize google cloud storage client using service account key file."
                    )
        return self.gcs_client

    def _get_gcs_client_using_adc(self) -> Optional[Client]:
        """
        Get google cloud storage client using Application Default Credentials (ADC) provided by Google Cloud.
        ADC allows the application to use the credentials of the service account associated with the Cloud Run service,
        making it a secure and convenient way to authenticate.
        """
        try:
            # Attempt to use Application Default Credentials (ADC)
            return storage.Client()
        except exceptions.DefaultCredentialsError as e:
            self.log.error(f"Unable to obtain credentials using ADC. Error: {e}")
            return None

    def _get_gcs_client_using_file(self, file: str) -> Optional[Client]:
        """
        Get google cloud storage client using a service account key file.
        """
        try:
            client: Client = Client.from_service_account_json(file)
            return client
        except Exception as e:
            self.log.error(f"Unable to obtain credentials from file={file}. Error: {e}")
            return None

    def get_bucket(self, bucket_name: str) -> Bucket:
        """
        Returns the bucket.
        :param bucket_name: The name of the bucket.
        :return: The bucket.
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
        Returns the blob.
        :param bucket_name: The name of the bucket.
        :param blob_name: The name of the blob.
        :return: The blob.
        """
        bucket = self.get_bucket(bucket_name)
        blob: Optional[Blob] = bucket.get_blob(blob_name)
        if blob is None:
            self.log.error(f"Blob {blob_name} not found in bucket {bucket_name}.")
            raise ValueError(f"Blob {blob_name} not found in bucket {bucket_name}.")
        return blob

    def get_file_as_string(self, bucket_name: str, blob_name: str) -> str:
        """
        Returns the file as string.
        :param bucket_name: The name of the bucket.
        :param blob_name: The name of the blob.
        :return: The file as string.
        """
        blob = self.get_blob(bucket_name, blob_name)
        return str(blob.download_as_text(encoding="utf-8"))

    def get_file_as_string_from_url(self, url: str) -> str:
        """
        Returns the file as string from the url.
        :param url: The url of the file in GCS.
        :return: The file as string.
        """
        bucket_name, blob_name = self.get_bucket_and_blob_name_from_url(url)
        self.log.info(f"Fetching file from bucket_name={bucket_name}, blob_name={blob_name}")
        return self.get_file_as_string(bucket_name, blob_name)

    def get_bucket_and_blob_name_from_url(self, url: str) -> tuple[str, str]:
        """
        Returns a tuple of bucket name and blob name from the url.
        :param url: The url of the file in GCS.
        :return: A tuple of bucket name and blob name.
        """
        # Remove the leading gs://
        url = url[5:]
        # Split the url by the first occurrence of /
        bucket_name, blob_name = url.split("/", 1)
        return bucket_name, blob_name
