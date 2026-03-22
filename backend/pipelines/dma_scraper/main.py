import asyncio
import os
import sys
import time
from datetime import datetime

import click
import nest_asyncio

# inside backend/pipelines/dma_scraper/fetch_company_data.py
ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
sys.path.insert(0, ROOT)

from common.cli import PipelineRun, common_options  # noqa: E402

from bronze.fetch_company_data import DMAScraper  # noqa: E402
from bronze.fetch_company_detail import DMACompanyDetailScraper  # noqa: E402
from silver.transformation import transform_dma_json  # noqa: E402


def _get_optimized_storage():
    """
    Get optimized storage with robust import handling for different environments.

    Returns GCSDataAccess if available, otherwise None for fallback.
    """
    try:
        # Primary import path - should work when common.gcs is properly installed
        from common.gcs import GCSDataAccess

        print("✅ Successfully imported optimized GCSDataAccess")
        return GCSDataAccess
    except ImportError as e:
        print(f"⚠️ Could not import optimized GCSDataAccess: {e}")
        print(
            "⚠️ Falling back to basic storage - ensure common.gcs is installed for optimal performance"
        )
        return None


def _get_cvr_collection_utility():
    """
    Get CVR collection utility with robust import handling.

    Returns CVR collection functions if available, otherwise None.
    """
    try:
        from unified_pipeline.util.cvr_collection import (
            extract_cvr_numbers_from_table,
            save_pipeline_cvr_numbers,
        )

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
        """Initialize fallback storage using s3fs via common.gcs.filesystem."""
        try:
            from common.gcs.filesystem import get_r2_filesystem

            self.fs = get_r2_filesystem()
            print(f"✅ DMA Storage: Using s3fs fallback for bucket: {bucket_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize storage: {e}") from e

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
                r2_path = f"{self.bucket_name}/{blob_name}"
                with self.fs.open(r2_path, "w") as f:
                    f.write(json_str)
                print(f"Saved {blob_name} to fallback storage")
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
                # Use fallback upload via s3fs
                r2_path = f"{self.bucket_name}/{blob_name}"
                self.fs.put(tmp_path, r2_path)
                print(f"Saved {blob_name} to fallback storage (parquet)")
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
            import json

            r2_path = f"{self.bucket_name}/{blob_name}"
            with self.fs.open(r2_path, "r") as f:
                return json.loads(f.read())
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

        with open(file_path, encoding="utf-8") as f:
            return json.load(f)


nest_asyncio.apply()

PREFIX_BRONZE_SAVE_PATH = os.environ.get("BRONZE_OUTPUT_DIR", "bronze/dma")
PREFIX_SILVER_SAVE_PATH = os.environ.get("SILVER_OUTPUT_DIR", "silver/dma")

# Initialize storage backend based on environment
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
if ENVIRONMENT.lower() in ("production", "container"):
    storage_backend = OptimizedStorageBackend(
        os.environ.get("R2_BUCKET") or os.environ.get("GCS_BUCKET", "landbruget-data")
    )
else:
    storage_backend = LocalStorageBackend(os.environ.get("BRONZE_OUTPUT_DIR", "."))

scraper = DMAScraper()


def save_data(data, timestamp, path):
    timestamp_dir = os.path.join(path, timestamp)
    blob_name = f"{timestamp_dir}/environmental_companies_raw.json"
    storage_backend.save_json(data, blob_name)


def save_parquet(data, timestamp, path):
    timestamp_dir = os.path.join(path, timestamp)
    blob_name = f"{timestamp_dir}/environmental_companies.parquet"
    storage_backend.save_parquet(data, blob_name)


pass  # parse_args replaced by Click CLI below


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
                    if (
                        cvr
                        and isinstance(cvr, str)
                        and len(cvr.strip()) == 8
                        and cvr.strip().isdigit()
                    ):
                        cvr_numbers.append(cvr.strip())

        # Remove duplicates and sort
        unique_cvr_numbers = sorted(set(cvr_numbers))

        if unique_cvr_numbers:
            # Save CVR numbers using the collection utility (with automatic GCS access initialization)
            gcs_path = save_pipeline_cvr_numbers(
                pipeline_name="dma_scraper",
                cvr_numbers=unique_cvr_numbers,
                gcs_access=None,  # Will create default GCS access
                bucket="landbruget-data",
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


def bronze(timestamp: str, total_pages=None, start_date=None, end_date=None):
    page = 1
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
    parsed_start_date = None
    parsed_end_date = None
    if start_date:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")

    detail_scraper = DMACompanyDetailScraper(
        all_page_results, start_date=parsed_start_date, end_date=parsed_end_date
    )
    loop = asyncio.get_event_loop()
    detailed_data = loop.run_until_complete(
        detail_scraper.process_miljoeaktoer_for_company_file_path()
    )
    # Merge base and detail dicts by 'miljoeaktoerUrl'
    detail_lookup = {item.get("miljoeaktoerUrl"): item for item in detailed_data if item}
    merged_results = []
    for base in all_page_results:
        url = base.get("miljoeaktoerUrl")
        merged_results.append({**base, **detail_lookup.get(url, {})})
    save_data(merged_results, timestamp, PREFIX_BRONZE_SAVE_PATH)
    return merged_results


@click.command()
@click.option("--total-pages", type=int, default=None, help="Total number of pages to scrape")
@click.option("--silver", "run_silver", is_flag=True, help="Run silver transformation stage")
@click.option("--timestamp", type=str, default=None, help="Timestamp directory for silver stage")
@click.option(
    "--start-date",
    type=str,
    default=None,
    help="Start date for filtering Tilsynsdato (format: YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=str,
    default=None,
    help="End date for filtering Tilsynsdato (format: YYYY-MM-DD)",
)
@common_options
def main(total_pages, run_silver, timestamp, start_date, end_date, log_level):
    """DMA Scraper Pipeline."""
    pipeline_start_time = datetime.now()
    ts = pipeline_start_time.strftime("%Y%m%d_%H%M%S")

    # Initialize pipeline metadata tracking
    pipeline_run = PipelineRun("dma_companies")

    data = None

    if run_silver:
        if not timestamp:
            print("Error: --timestamp is required for silver stage")
            sys.exit(1)
        data = storage_backend.read_json(
            os.path.join(PREFIX_BRONZE_SAVE_PATH, timestamp, "environmental_companies_raw.json")
        )
        silver(data, timestamp)
    else:
        data = bronze(ts, total_pages=total_pages, start_date=start_date, end_date=end_date)
        silver(data, ts)

        # Generate schema documentation after silver processing
        try:
            from pathlib import Path

            parquet_path = os.path.join(
                PREFIX_SILVER_SAVE_PATH, ts, "environmental_companies.parquet"
            )

            if ENVIRONMENT.lower() in ("production", "container"):
                print("Note: Schema documentation for GCS files not yet implemented")
            elif os.path.exists(parquet_path):
                from common.schema_utils import generate_schema_docs

                generate_schema_docs(
                    parquet_path=parquet_path,
                    pipeline_name="dma_scraper",
                    table_name="dma_processed",
                    pipeline_start_time=pipeline_start_time,
                    stage="silver",
                )
            else:
                print("Parquet file not found, skipping schema documentation")

        except Exception as e:
            print(f"Warning: Could not generate schema documentation: {e}")
            import traceback

            traceback.print_exc()

    # Save pipeline metadata
    if data:
        try:
            from pathlib import Path

            record_count = len(data) if data else None

            if ENVIRONMENT.lower() in ("production", "container"):
                metadata_path = f"{PREFIX_SILVER_SAVE_PATH}/{ts}/dma_companies_metadata.json"
                metadata = pipeline_run.manager.create_metadata(
                    source_key="dma_companies",
                    record_count=record_count,
                    processing_duration=pipeline_run.elapsed,
                )
                storage_backend.save_json(metadata.model_dump(), metadata_path)
                print(f"DMA companies metadata saved to GCS: {metadata_path}")
            else:
                metadata_dir = os.path.join(PREFIX_SILVER_SAVE_PATH, ts)
                os.makedirs(metadata_dir, exist_ok=True)

                pipeline_run.finish(
                    record_count=record_count,
                    output_path=Path(metadata_dir) / "dma_companies_metadata.json",
                )
        except Exception as e:
            print(f"Failed to create DMA pipeline metadata: {e}")


if __name__ == "__main__":
    main()
