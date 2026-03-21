#!/usr/bin/env python3
"""Standalone script to run GeoDanmark bulk fetch for GitHub Actions."""

import os
from datetime import datetime

import duckdb

from bronze.bulk_geodanmark_fetcher import BulkGeoDanmarkFetcher
from bronze.bulk_geodanmark_graphql_fetcher import BulkGeoDanmarkGraphQLFetcher

# Storage upload functionality
try:
    from common.gcs import GCSDataAccess  # noqa: F401
    from common.gcs.filesystem import get_r2_filesystem

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


def upload_to_gcs(file_path: str, gcs_bucket: str, gcs_path: str) -> bool | None:
    """Upload file to storage using s3fs."""
    if not GCS_AVAILABLE:
        print("⚠️ Storage not available - skipping upload")
        return False

    try:
        print(f"📤 Uploading {file_path} to {gcs_bucket}/{gcs_path}")

        fs = get_r2_filesystem()
        r2_path = f"{gcs_bucket}/{gcs_path}"
        fs.put(file_path, r2_path)
        print(f"✅ Successfully uploaded to {r2_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to upload: {e}")
        return False


def main() -> None:
    api_key = os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
    username = os.getenv("DATAFORDELER_USERNAME")
    password = os.getenv("DATAFORDELER_PASSWORD")
    gcs_bucket = os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET")

    # Prefer GraphQL API (WFS endpoint is broken)
    if api_key:
        print("📦 Starting bulk GeoDanmark download via GraphQL API...")
        fetcher = BulkGeoDanmarkGraphQLFetcher(api_key)
        fetcher.bulk_download_buildings(batch_size=10)
    elif username and password:
        print("📦 Starting bulk GeoDanmark download via WFS (fallback)...")
        fetcher = BulkGeoDanmarkFetcher(username, password)
        fetcher.bulk_download_buildings(batch_size=30000)
    else:
        raise ValueError(
            "Missing credentials: set DATAFORDELER_GRAPHQL_API_KEY "
            "or DATAFORDELER_USERNAME/DATAFORDELER_PASSWORD"
        )

    # Get final count
    conn = duckdb.connect()
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
            gcs_path = (
                f"bronze/bbr_buildings/geodanmark/{timestamp}/"
                "geodanmark_buildings_complete.geoparquet"
            )

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
        print(f"❌ Error: no buildings data produced: {e}")
        raise SystemExit(1) from e
    finally:
        conn.close()

    if total_count == 0:
        print("❌ Error: downloaded 0 buildings — API may be returning errors")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
