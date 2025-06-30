#!/usr/bin/env python3
"""Standalone script to run INSPIRE BBR fetch for GitHub Actions."""

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from bronze.inspire_bbr_fetcher import InspireBBRFetcher
from config.settings import get_settings
from utils.logger import setup_logger


def main():
    try:
        settings = get_settings()
        # Use WARNING level by default to reduce log output for GitHub Actions
        logger = setup_logger(level="WARNING")
        output_dir = Path("data/bronze")

        # Set sample size if specified
        sample_size = None
        if len(sys.argv) > 1:
            try:
                sample_size = int(sys.argv[1])
                if sample_size <= 0:
                    sample_size = None
            except ValueError:
                pass

        if sample_size:
            print(f"🧪 Using sample size: {sample_size}")
        else:
            print("🔄 Processing full INSPIRE BBR dataset")
            # For GitHub Actions free runner, we should use a reasonable limit
            # to avoid memory/time constraints (6GB RAM, 6 hours max)
            if "GITHUB_ACTIONS" in os.environ:
                # Limit to ~100k buildings for free runner constraints
                sample_size = 100000
                print(
                    f"🚀 GitHub Actions detected: limiting to {sample_size:,} buildings for resource constraints"
                )

        print("📦 Starting INSPIRE BBR fetch...")
        inspire_fetcher = InspireBBRFetcher(settings, logger)

        pipeline_start_time = datetime.now()
        inspire_result = inspire_fetcher.fetch_data(
            output_dir,
            sample_size=sample_size,
            return_data=True,
            pipeline_start_time=pipeline_start_time,
        )

        if inspire_result and "data" in inspire_result:
            building_ids = inspire_result["data"].get("building_ids", [])
            print(f"🏢 Total INSPIRE BBR buildings fetched: {len(building_ids):,}")

            # Only write to GITHUB_OUTPUT if running in GitHub Actions
            if "GITHUB_OUTPUT" in os.environ:
                with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                    f.write(f"buildings-count={len(building_ids)}\n")

            # Save building IDs for the join job
            with open("data/inspire_building_ids.json", "w") as f:
                json.dump(building_ids, f)

            # Save attributes data if available
            if "attributes_df" in inspire_result["data"]:
                import duckdb

                # Convert list of dictionaries to parquet using DuckDB
                conn = duckdb.connect(":memory:")

                # Convert the list of dictionaries to JSON and use DuckDB's JSON functions
                attributes_data = inspire_result["data"]["attributes_df"]

                # Create a temporary JSON file for DuckDB to read
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                    json.dump(attributes_data, f)
                    temp_json_path = f.name

                try:
                    # Read JSON file into DuckDB table
                    conn.execute(
                        f"CREATE TABLE buildings AS SELECT * FROM read_json('{temp_json_path}')"
                    )
                    conn.execute(
                        "COPY buildings TO 'data/inspire_attributes.parquet' (FORMAT PARQUET)"
                    )
                    print("💾 Saved INSPIRE attributes to parquet")
                finally:
                    # Clean up temporary file
                    os.unlink(temp_json_path)
                    conn.close()
        else:
            print("❌ No INSPIRE BBR data fetched")
            if "GITHUB_OUTPUT" in os.environ:
                with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                    f.write("buildings-count=0\n")
    except Exception as e:
        print("❌ INSPIRE BBR fetch failed")
        print(f"Error: {e}")
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write("buildings-count=0\n")
        raise


if __name__ == "__main__":
    main()
