import asyncio
import json
import logging
import sys
import tempfile
from typing import Optional

from google.cloud import bigquery, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def load_to_bigquery() -> Optional[int]:
    """Load latest property owners file from GCS to BigQuery"""
    try:
        # Initialize clients
        storage_client = storage.Client()
        bq_client = bigquery.Client()

        # Get latest file from GCS
        bucket = storage_client.bucket("landbrugsdata-raw-data")
        blobs = list(bucket.list_blobs(prefix="raw/property_owners_"))
        if not blobs:
            raise ValueError("No property owners files found in GCS")

        latest_blob = max(blobs, key=lambda b: b.time_created)
        gcs_uri = f"gs://{bucket.name}/{latest_blob.name}"
        logger.info(f"Found latest file: {latest_blob.name}")

        # Download a small sample to check structure
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".ndjson") as temp_file:
            sample_data = latest_blob.download_as_string(start=0, end=10000).decode("utf-8")
            sample_lines = sample_data.splitlines()[:5]  # Take first 5 lines

            logger.info("Sample data structure:")
            for line in sample_lines:
                data = json.loads(line)
                logger.info(f"Fields: {list(data.keys())}")
                break  # Just show first line's structure

        # Load to BigQuery
        logger.info("Loading to BigQuery...")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            max_bad_records=1000,  # Allow more errors
            ignore_unknown_values=True,  # Skip unknown fields
        )

        table_ref = "land_data.property_owners_raw"
        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        # Wait and log any errors
        job = load_job.result()
        if job.errors:
            for error in job.errors:
                logger.error(f"Load error: {error}")

        # Get number of rows loaded
        table = bq_client.get_table(table_ref)
        logger.info(f"Loaded {table.num_rows:,} rows to {table_ref}")
        return table.num_rows

    except Exception as e:
        logger.error(f"Error loading to BigQuery: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(load_to_bigquery())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)
