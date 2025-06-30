#!/usr/bin/env python3
"""Standalone script to run INSPIRE BBR fetch for GitHub Actions."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from bronze.inspire_bbr_fetcher import InspireBBRFetcher
from config.settings import get_settings
from utils.logger import setup_logger


def main():
    try:
        settings = get_settings()
        logger = setup_logger(level="INFO")
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
                conn.execute(
                    "CREATE TABLE buildings AS SELECT * FROM ?",
                    [inspire_result["data"]["attributes_df"]],
                )
                conn.execute("COPY buildings TO 'data/inspire_attributes.parquet' (FORMAT PARQUET)")
                conn.close()
                print("💾 Saved INSPIRE attributes to parquet")
        else:
            print("❌ No INSPIRE BBR data fetched")
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write("buildings-count=0\n")
    except Exception as e:
        print("❌ INSPIRE BBR fetch failed")
        print(f"Error: {e}")
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write("buildings-count=0\n")
        raise


if __name__ == "__main__":
    main()
