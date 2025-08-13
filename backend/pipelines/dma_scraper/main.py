import argparse
import asyncio
import os
import sys
import time
from datetime import datetime

import nest_asyncio

# inside backend/pipelines/dma_scraper/fetch_company_data.py
ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
sys.path.insert(0, ROOT)

from bronze.fetch_company_data import DMAScraper  # noqa: E402
from bronze.fetch_company_detail import DMACompanyDetailScraper  # noqa: E402
from silver.transformation import transform_dma_json  # noqa: E402


def _get_optimized_storage():
    """
    Get optimized storage with robust import handling for different environments.

    Returns GCSDataAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when unified_pipeline is properly installed
        from unified_pipeline.util.gcs_access import GCSDataAccess

        print("✅ Successfully imported optimized GCSDataAccess")
        return GCSDataAccess
    except ImportError as e:
        print(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        print("⚠️ Falling back to basic storage - ensure unified_pipeline is installed for optimal performance")
        return None


def _get_cvr_collection_utility():
    """
    Get CVR collection utility with robust import handling.

    Returns CVR collection functions if available, otherwise None.
    """
    try:
        from unified_pipeline.util.cvr_collection import extract_cvr_numbers_from_table, save_pipeline_cvr_numbers

        print("✅ Successfully imported CVR collection utilities")
        return extract_cvr_numbers_from_table, save_pipeline_cvr_numbers
    except ImportError as e:
        print(f"⚠️ Could not import CVR collection utilities: {e}")
        print("⚠️ CVR numbers will not be saved for enrichment")
        return None, None


# Get optimized GCS access class or None if not available
OptimizedGCSDataAccess = _get_optimized_storage()

# Get CVR collection utilities
extract_cvr_numbers_from_table, save_pipeline_cvr_numbers = _get_cvr_collection_utility()


class OptimizedStorageBackend:
    """Storage backend that uses optimized GCS access when available, with fallback."""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.use_optimized = False

        # Try to use optimized storage
        if OptimizedGCSDataAccess:
            try:
                self.gcs_access = OptimizedGCSDataAccess()
                self.use_optimized = True
                print(f"✅ DMA Storage: Using optimized GCS access for bucket: {bucket_name}")
            except Exception as e:
                print(f"⚠️ Failed to initialize optimized GCS access: {e}")
                self._init_fallback(bucket_name)
        else:
            self._init_fallback(bucket_name)

    def _init_fallback(self, bucket_name: str):
        """Initialize fallback storage using google-cloud-storage."""
        try:
            from google.cloud import storage

            self.gcs_client = storage.Client()
            self.gcs_bucket = self.gcs_client.bucket(bucket_name)
            print(f"✅ DMA Storage: Using fallback GCS for bucket: {bucket_name}")
        except ImportError as e:
            raise ImportError("google-cloud-storage is required but not available") from e
        except Exception as e:
            raise RuntimeError(f"Failed to initialize GCS storage: {e}") from e

    def save_json(self, data, blob_name: str):
        """Save JSON data to GCS."""
        try:
            if self.use_optimized:
                gcs_path = f"gs://{self.bucket_name}/{blob_name}"
                self.gcs_access.upload_json(data, gcs_path)
                print(f"Saved {blob_name} to optimized GCS storage")
            else:
                import json

                json_str = json.dumps(data, indent=2, ensure_ascii=False)
                blob = self.gcs_bucket.blob(blob_name)
                blob.upload_from_string(json_str, content_type="application/json")
                print(f"Saved {blob_name} to fallback GCS storage")
        except Exception as e:
            print(f"Error saving JSON to {blob_name}: {e}")
            raise

    def save_parquet(self, data, blob_name: str):
        """Save Parquet data to GCS."""
        try:
            if self.use_optimized:
                # For optimized storage, we need to use DuckDB integration
                # This requires the data to be in a DuckDB table first
                # For now, use fallback for parquet files
                self._save_parquet_fallback(data, blob_name)
            else:
                self._save_parquet_fallback(data, blob_name)
        except Exception as e:
            print(f"Error saving Parquet to {blob_name}: {e}")
            raise

    def _save_parquet_fallback(self, data, blob_name: str):
        """Save Parquet using fallback method."""
        import os
        import tempfile

        # Save to temporary file first
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            tmp_path = tmp_file.name
            data.to_parquet(tmp_path, index=False)

        try:
            # Upload to GCS
            if self.use_optimized:
                # Use optimized file upload
                with open(tmp_path, "rb") as f:
                    file_data = f.read()
                gcs_path = f"gs://{self.bucket_name}/{blob_name}"
                with self.gcs_access.fs.open(gcs_path, "wb") as gcs_file:
                    gcs_file.write(file_data)
                print(f"Saved {blob_name} to optimized GCS storage (parquet)")
            else:
                # Use fallback upload
                blob = self.gcs_bucket.blob(blob_name)
                blob.upload_from_filename(tmp_path)
                print(f"Saved {blob_name} to fallback GCS storage (parquet)")
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def read_json(self, blob_name: str):
        """Read JSON data from GCS."""
        try:
            if self.use_optimized:
                gcs_path = f"gs://{self.bucket_name}/{blob_name}"
                return self.gcs_access.download_json(gcs_path)
            else:
                blob = self.gcs_bucket.blob(blob_name)
                json_bytes = blob.download_as_bytes()
                import json

                return json.loads(json_bytes.decode("utf-8"))
        except Exception as e:
            print(f"Error reading JSON from {blob_name}: {e}")
            raise


class LocalStorageBackend:
    """Local storage backend for development."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def save_json(self, data, blob_name: str):
        """Save JSON data locally."""
        import json

        file_path = os.path.join(self.base_dir, blob_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved {blob_name} to local storage")

    def save_parquet(self, data, blob_name: str):
        """Save Parquet data locally."""
        file_path = os.path.join(self.base_dir, blob_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        data.to_parquet(file_path, index=False)
        print(f"Saved {blob_name} to local storage")

    def read_json(self, blob_name: str):
        """Read JSON data from local storage."""
        import json

        file_path = os.path.join(self.base_dir, blob_name)

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)


nest_asyncio.apply()

PREFIX_BRONZE_SAVE_PATH = os.environ.get("BRONZE_OUTPUT_DIR", "bronze/dma")
PREFIX_SILVER_SAVE_PATH = os.environ.get("SILVER_OUTPUT_DIR", "silver/dma")

# Initialize storage backend based on environment
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
if ENVIRONMENT.lower() in ("production", "container"):
    storage_backend = OptimizedStorageBackend(os.environ.get("GCS_BUCKET", "landbrugsdata-raw-data"))
else:
    storage_backend = LocalStorageBackend(os.environ.get("BRONZE_OUTPUT_DIR", "."))

scraper = DMAScraper()


def save_data(data, timestamp, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/environmental_companies_raw.json"
    storage_backend.save_json(data, blob_name)


def save_parquet(data, timestamp, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/environmental_companies.parquet"
    storage_backend.save_parquet(data, blob_name)


def parse_args():
    parser = argparse.ArgumentParser(description="DMA Scraper Pipeline")
    parser.add_argument(
        "--total-pages",
        type=int,
        default=None,
        help="Total number of pages to scrape",
    )
    parser.add_argument(
        "--silver",
        action="store_true",
        help="Run silver transformation stage",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        help="Timestamp directory for silver stage",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for filtering Tilsynsdato (format: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for filtering Tilsynsdato (format: YYYY-MM-DD)",
    )
    return parser.parse_args()


def _save_discovered_cvr_numbers(data, timestamp: str):
    """
    Extract and save CVR numbers discovered in the DMA scraper data.

    Args:
        data: Raw DMA data containing company information
        timestamp: Pipeline timestamp for identification
    """
    try:
        print("📊 Extracting CVR numbers from DMA scraper data")

        # Extract CVR numbers from the raw data
        cvr_numbers = []
        for company in data:
            # Look for CVR numbers in the detailed data sections
            for section in ["Tilsyn", "Håndhævelser", "Afgørelser"]:
                section_data = company.get(section, [])
                for item in section_data:
                    cvr = item.get("cvr_number")
                    if cvr and isinstance(cvr, str) and len(cvr.strip()) == 8 and cvr.strip().isdigit():
                        cvr_numbers.append(cvr.strip())

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(list(set(cvr_numbers)))

        if unique_cvr_numbers:
            # Save CVR numbers using the collection utility (with automatic GCS access initialization)
            gcs_path = save_pipeline_cvr_numbers(
                pipeline_name="dma_scraper",
                cvr_numbers=unique_cvr_numbers,
                gcs_access=None,  # Will create default GCS access
                bucket="landbrugsdata-raw-data",
                timestamp=timestamp,
            )

            print(f"✅ Saved {len(unique_cvr_numbers)} unique CVR numbers to {gcs_path}")
        else:
            print("⚠️ No CVR numbers found in DMA scraper data")

    except Exception as e:
        print(f"❌ Failed to save CVR numbers for DMA scraper: {e}")


def silver(data, timestamp: str):
    df = transform_dma_json(data)
    save_parquet(df, timestamp, PREFIX_SILVER_SAVE_PATH)

    # Save CVR numbers for enrichment if utilities are available
    if save_pipeline_cvr_numbers is not None:
        _save_discovered_cvr_numbers(data, timestamp)


def bronze(timestamp: str):
    args = parse_args()
    page = 1
    total_pages = args.total_pages
    all_page_results = []
    while total_pages is None or page <= total_pages:
        print(f"Fetching page {page}...")
        data = scraper.fetch_data(page)

        if total_pages is None:
            total_pages = data["pagination"]["antalSider"]
            print(f"Total pages: {total_pages}")

        page_results = scraper.extract_info(data)
        time.sleep(1)  # Add a delay to avoid overwhelming the server
        all_page_results.extend(page_results)
        page += 1

    # Convert date strings to datetime objects if provided
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    detail_scraper = DMACompanyDetailScraper(all_page_results, start_date=start_date, end_date=end_date)
    loop = asyncio.get_event_loop()
    detailed_data = loop.run_until_complete(detail_scraper.process_miljoeaktoer_for_company_file_path())
    # Merge base and detail dicts by 'miljoeaktoerUrl'
    detail_lookup = {item.get("miljoeaktoerUrl"): item for item in detailed_data if item}
    merged_results = []
    for base in all_page_results:
        url = base.get("miljoeaktoerUrl")
        merged_results.append({**base, **detail_lookup.get(url, {})})
    save_data(merged_results, timestamp, PREFIX_BRONZE_SAVE_PATH)
    return merged_results


if __name__ == "__main__":
    pipeline_start_time = datetime.now()
    timestamp = pipeline_start_time.strftime("%Y%m%d_%H%M%S")
    args = parse_args()
    if args.silver:
        if not args.timestamp:
            print("Error: --timestamp is required for silver stage")
            sys.exit(1)
        data = storage_backend.read_json(
            os.path.join(PREFIX_BRONZE_SAVE_PATH, args.timestamp, "environmental_companies_raw.json")
        )
        silver(data, args.timestamp)
    else:
        data = bronze(timestamp)
        silver(data, timestamp)

        # Generate schema documentation after silver processing
        try:
            print("Generating schema documentation for DMA data")

            # Import schema documentation (with path adjustment)
            import sys
            from pathlib import Path

            # Find the project root (directory containing 'backend' folder)
            current_file = Path(__file__).resolve()
            project_root = None

            # Go up the directory tree to find the project root
            for parent in current_file.parents:
                if (parent / "backend").is_dir():
                    project_root = parent
                    break

            if project_root and str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            # Use the pipeline start time we already have
            # Create DuckDB connection and load the parquet file
            import duckdb

            try:
                from backend.common.schema_documentation import SchemaDocumentationManager
            except ImportError as e:
                import warnings

                warnings.warn(f"Schema documentation not available: {e}")
                SchemaDocumentationManager = None

            conn = duckdb.connect()

            # Construct path to the silver parquet file
            parquet_path = os.path.join(PREFIX_SILVER_SAVE_PATH, timestamp, "environmental_companies.parquet")

            # Check if running locally or in GCS environment
            if ENVIRONMENT.lower() in ("production", "container"):
                # For GCS, we need to download the file first or use a different approach
                print("Note: Schema documentation for GCS files not yet implemented")
            else:
                # For local files
                if os.path.exists(parquet_path) and SchemaDocumentationManager is not None:
                    table_name = "dma_processed"
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")

                    # Initialize schema documentation manager
                    schema_manager = SchemaDocumentationManager(
                        connection=conn,
                        pipeline_name="dma_scraper",
                        pipeline_start_time=pipeline_start_time,
                        logger=None,  # No logger available in this pipeline
                    )

                    # Generate documentation for DMA table
                    schema_files = schema_manager.generate_all_documentation([table_name], stage="silver")
                    print("Generated schema documentation for DMA data")

                else:
                    print("Parquet file not found or schema documentation not available")

        except Exception as e:
            print(f"Warning: Could not generate schema documentation: {e}")
            import traceback

            traceback.print_exc()
