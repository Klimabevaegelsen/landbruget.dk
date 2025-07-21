#!/usr/bin/env python3
"""Standalone script to run GeoDanmark bulk fetch for GitHub Actions."""

import os
from datetime import datetime

import duckdb

from bronze.bulk_geodanmark_fetcher import BulkGeoDanmarkFetcher

# GCS upload functionality
try:
    from google.cloud import storage
    from unified_pipeline.util.gcs_access import GCSDataAccess

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


def upload_to_gcs(file_path: str, gcs_bucket: str, gcs_path: str):
    """Upload file to GCS using the standard pattern from other pipelines."""
    if not GCS_AVAILABLE:
        print("⚠️ GCS not available - skipping upload")
        return False

    try:
        print(f"📤 Uploading {file_path} to gs://{gcs_bucket}/{gcs_path}")

        # Use the same pattern as other pipelines
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_path)

        blob.upload_from_filename(file_path)
        print(f"✅ Successfully uploaded to gs://{gcs_bucket}/{gcs_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}")
        return False


def main():
    username = os.getenv("DATAFORDELER_USERNAME")
    password = os.getenv("DATAFORDELER_PASSWORD")
    gcs_bucket = os.getenv("GCS_BUCKET")

    if not username or not password:
        raise ValueError("Missing Datafordeleren credentials")

    print("📦 Starting bulk GeoDanmark download...")
    fetcher = BulkGeoDanmarkFetcher(username, password)
    fetcher.bulk_download_buildings(batch_size=30000)

    # Get final count
    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    try:
        result = conn.execute(
            'SELECT COUNT(*) FROM read_parquet("data/geodanmark_buildings_complete.geoparquet")'
        ).fetchone()
        total_count = result[0] if result else 0
        print(f"🏢 Total GeoDanmark buildings downloaded: {total_count:,}")

        # Get shared timestamp from artifact or generate new one
        timestamp = None
        if os.path.exists("/tmp/bronze_timestamp.txt"):
            with open("/tmp/bronze_timestamp.txt") as f:
                timestamp = f.read().strip()
            print(f"Using shared bronze timestamp: {timestamp}")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"No shared timestamp found, generated: {timestamp}")

        # Upload to GCS if in production environment
        if gcs_bucket and os.getenv("ENVIRONMENT") == "production":
            gcs_path = f"bronze/bbr_buildings/geodanmark/{timestamp}/geodanmark_buildings_complete.geoparquet"

            success = upload_to_gcs(
                "data/geodanmark_buildings_complete.geoparquet", gcs_bucket, gcs_path
            )

            if success:
                print(f"✅ GeoDanmark data uploaded to GCS: gs://{gcs_bucket}/{gcs_path}")
            else:
                print("⚠️ Failed to upload GeoDanmark data to GCS")
        else:
            print("ℹ️ Not uploading to GCS (no bucket specified or not in production)")

        # Set GitHub Actions output
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"buildings-count={total_count}\n")
            f.write(f"bronze-timestamp={timestamp}\n")
    except Exception as e:
        print(f"❌ Error counting buildings: {e}")
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("buildings-count=0\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
