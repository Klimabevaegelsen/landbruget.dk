#!/usr/bin/env python3
"""Standalone script to run GeoDanmark bulk fetch for GitHub Actions."""

import os

import duckdb

from bronze.bulk_geodanmark_fetcher import BulkGeoDanmarkFetcher


def main():
    username = os.getenv("DATAFORDELER_USERNAME")
    password = os.getenv("DATAFORDELER_PASSWORD")

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
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"buildings-count={total_count}\n")
    except Exception as e:
        print(f"❌ Error counting buildings: {e}")
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("buildings-count=0\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
