import json
import logging
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _get_optimized_gcs_access() -> type | None:
    """
    Get optimized GCS access with robust import handling.

    Returns GCSDataAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when unified_pipeline is properly installed
        from unified_pipeline.util.gcs_access import GCSDataAccess

        logging.info("✅ Successfully imported optimized GCSDataAccess")
        return GCSDataAccess
    except ImportError as e:
        logging.warning(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        logging.warning(
            "⚠️ Falling back to basic storage - ensure unified_pipeline is installed "
            "for optimal performance"
        )
        return None


# Get optimized GCS access class or None if not available
OptimizedGCSDataAccess = _get_optimized_gcs_access()


class BMDScraper:
    def __init__(self, base_url="https://bmd.mst.dk", output_dir="data"):
        self.base_url = base_url
        self.output_dir = output_dir
        self.session = requests.Session()
        self.pipeline_start_time = datetime.now()
        self.timestamp = self.pipeline_start_time.strftime("%Y%m%d_%H%M%S")

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def get_verification_token(self) -> str | None:
        """Get the __RequestVerificationToken from the export dialog."""
        url = f"{self.base_url}/External/Entry/ExportDialog"
        params = {
            "Page": "0",
            "PageSize": "10",
            "SortingKey": "ProductName",
            "SortingDirection": "Ascending",
            "ProductName": "",
            "RegNo": "",
            "PestControlType": "",
            "ActiveSubstanceType": "",
            "CASNumber": "",
            "ProductGroupPesticide": "",
            "ProductGroupBiocide": "",
            "ProductType": "",
            "AuthorizationHolder": "",
            "AuthorizationDateInterval": "",
            "LoadTaxDateInterval": "",
            "MinorUse": "",
            "Use": "",
        }

        logging.info("Requesting export dialog to get verification token...")
        response = self.session.get(url, params=params)

        if response.status_code != 200:
            logging.error(f"Failed to get export dialog: {response.status_code}")
            return None

        # Parse the HTML to extract the token
        soup = BeautifulSoup(response.text, "html.parser")
        token_input = soup.find("input", attrs={"name": "__RequestVerificationToken"})

        if not token_input:
            logging.error("Could not find verification token in response")
            return None

        token = token_input.get("value")
        logging.info(f"Verification token obtained: {token[:10]}...")
        return token

    def generate_document(self, token) -> str | None:
        """Request document generation using the verification token."""
        url = f"{self.base_url}/External/Entry/GenerateDocument"

        form_data = {
            "__RequestVerificationToken": token,
            "ActiveSubstanceType": "",
            "AuthorizationDateInterval": "",
            "AuthorizationHolder": "",
            "CASNumber": "",
            "IsIframeOffline": "False",
            "LoadTaxDateInterval": "",
            "MinorUse": "",
            "Page": "0",
            "PageSize": "10",
            "PestControlType": "",
            "ProductGroupBiocide": "",
            "ProductGroupPesticide": "",
            "ProductName": "",
            "ProductType": "",
            "RegNo": "",
            "Use": "",
            "SortingDirection": "Ascending",
            "SortingKey": "ProductName",
            "ExportFormat": "3",  # Excel format
        }

        logging.info("Requesting document generation...")
        response = self.session.post(url, data=form_data)

        if response.status_code != 200:
            logging.error(f"Failed to generate document: {response.status_code}")
            return None

        try:
            result = response.json()
            if not result.get("Validated", False):
                logging.error(f"Document generation not validated: {result}")
                return None

            download_url = result.get("Url")
            if not download_url:
                logging.error("No download URL in response")
                return None

            logging.info(f"Document generation successful. Download URL: {download_url}")
            return download_url
        except json.JSONDecodeError:
            logging.error("Failed to parse JSON response")
            return None

    def download_file(self, download_url):
        """Download the Excel file using the provided URL."""
        full_url = f"{self.base_url}{download_url}"

        logging.info(f"Downloading file from {full_url}...")
        response = self.session.get(full_url, stream=True)

        if response.status_code != 200:
            logging.error(f"Failed to download file: {response.status_code}")
            return None

        # Create timestamp directory
        timestamp_dir = os.path.join(self.output_dir, self.timestamp)
        os.makedirs(timestamp_dir, exist_ok=True)

        # Save as bmd_raw.xlsx in the timestamp directory
        file_path = os.path.join(timestamp_dir, "bmd_raw.xlsx")

        # Save the file
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logging.info(f"File downloaded successfully to {file_path}")

        # Generate and save metadata
        self.save_metadata(download_url, full_url, file_path)

        return file_path

    def save_metadata(self, download_url, full_url, file_path):
        """Save metadata about the downloaded file."""
        metadata = {
            "fetch_timestamp": self.pipeline_start_time.isoformat(),
            "source_url": full_url,
            "download_url_path": download_url,
            "base_url": self.base_url,
            "file_name": os.path.basename(file_path),
            "file_path": file_path,
            "file_size_bytes": os.path.getsize(file_path) if os.path.exists(file_path) else None,
        }

        # Try to get a rough estimate of record count without parsing the whole file
        # This is a placeholder - Excel parsing could be added later if needed
        metadata["estimated_record_count"] = "unknown"

        # Save metadata to JSON file
        metadata_path = os.path.join(os.path.dirname(file_path), "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logging.info(f"Metadata saved to {metadata_path}")
        return metadata

    def scrape(self):
        """Execute the full scraping process."""
        # Step 1: Get verification token
        token = self.get_verification_token()
        if not token:
            logging.error("Failed to get verification token. Aborting.")
            return None

        # Step 2: Generate document
        download_url = self.generate_document(token)
        if not download_url:
            logging.error("Failed to generate document. Aborting.")
            return None

        # Step 3: Download the file
        file_path = self.download_file(download_url)
        if not file_path:
            logging.error("Failed to download file. Aborting.")
            return None

        return file_path


class GCSStorage:
    """Google Cloud Storage backend for BMD files - OPTIMIZED VERSION."""

    def __init__(self, bucket_name, prefix="bronze/bmd"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.is_available = self._check_gcs_available()

        # ✅ OPTIMIZED: Initialize optimized GCS access
        self.gcs_access = None
        self.use_optimized = False

        if self.is_available and OptimizedGCSDataAccess:
            try:
                self.gcs_access = OptimizedGCSDataAccess()
                self.use_optimized = True
                logging.info("✅ BMD GCSStorage: Initialized optimized GCS access")
            except Exception as e:
                logging.warning(f"Failed to initialize optimized GCS access: {e}")
                self.gcs_access = None
                self.use_optimized = False

    def _check_gcs_available(self):
        """Check if GCS is available (Google Cloud Storage library is installed)."""
        try:
            from google.cloud import storage  # noqa: F401

            return True
        except ImportError:
            logging.warning("Google Cloud Storage library not available. Using local storage only.")
            return False

    def upload_file(self, local_path, gcs_path=None):
        """Upload a file to GCS bucket using optimized streaming."""
        if not self.is_available:
            logging.warning("GCS not available, skipping upload")
            return False

        if gcs_path is None:
            # Use the file structure from local path but with GCS prefix
            relative_path = os.path.relpath(
                local_path, start=os.path.dirname(os.path.dirname(local_path))
            )
            gcs_path = f"{self.prefix}/{relative_path}"

        try:
            # ✅ OPTIMIZED: Use streaming upload if available
            if self.use_optimized and self.gcs_access:
                full_gcs_path = f"gs://{self.bucket_name}/{gcs_path}"

                # Stream file directly without loading into memory
                with open(local_path, "rb") as file_obj:
                    with self.gcs_access.fs.open(full_gcs_path, "wb") as gcs_file:
                        import shutil

                        shutil.copyfileobj(file_obj, gcs_file)

                logging.info(f"✅ Uploaded {local_path} to {full_gcs_path} (optimized streaming)")
                return True
            else:
                # Fallback to old method if optimized access failed
                from google.cloud import storage

                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_path)
                logging.info(
                    f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path} (fallback)"
                )
                return True

        except Exception as e:
            logging.error(f"Failed to upload to GCS: {e}")
            return False


def main() -> None:
    # Example usage for local development
    scraper = BMDScraper(output_dir="bronze/bmd")
    file_path = scraper.scrape()

    if file_path:
        logging.info(f"BMD scraping completed successfully. File saved to {file_path}")

        # Example of how to use GCS storage in production
        # Uncomment to use if GCS integration is needed
        # storage = GCSStorage(bucket_name="your-bucket-name")
        # storage.upload_file(file_path)
    else:
        logging.error("BMD scraping failed.")


if __name__ == "__main__":
    main()
