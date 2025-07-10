#!/usr/bin/env python3
"""
Simple script to run bronze layer spatial join for GitHub Actions.
Uses DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for optimal performance.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from config.settings import get_settings
from main import check_memory_usage, perform_chunked_spatial_join, perform_spatial_join_optimized


def main():
    """Run bronze layer spatial join with DuckDB Spatial v1.2.2 optimizations."""
    settings = get_settings()

    print("🚀 Starting SPATIAL_JOIN optimized processing (DuckDB Spatial v1.2.2)...")
    print(f"🔧 Using spatial optimizations: {settings.spatial_optimization_config}")
    print("📖 Reference: https://github.com/duckdb/duckdb-spatial/pull/545")

    # Check initial memory usage
    check_memory_usage()

    # Load building IDs
    building_ids_file = Path("data/inspire_building_ids.json")
    if not building_ids_file.exists():
        print(f"❌ Error: {building_ids_file} not found")
        sys.exit(1)

    with open(building_ids_file) as f:
        building_ids = json.load(f)

    print(f"🔍 Processing {len(building_ids):,} INSPIRE BBR buildings with GeoDanmark data...")

    # Check GeoDanmark data exists
    geodanmark_path = "data/geodanmark_buildings_complete.geoparquet"
    if not Path(geodanmark_path).exists():
        print(f"❌ Error: {geodanmark_path} not found")
        sys.exit(1)

    # Create output directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(f"data/bronze/{timestamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Try SPATIAL_JOIN operator first, fallback to chunked if needed
    try:
        print("🔥 Attempting SPATIAL_JOIN operator approach...")
        join_result = perform_spatial_join_optimized(
            building_ids=building_ids,
            geodanmark_path=geodanmark_path,
            output_dir=output_dir,
            use_spatial_join_operator=True,
        )

        if join_result["success"]:
            print(
                f"✅ SPATIAL_JOIN approach succeeded: {join_result['joined_buildings_count']:,} buildings"
            )

            # Show optimization details
            if join_result.get("spatial_join_operator_used"):
                print("🎯 SPATIAL_JOIN operator was successfully used!")
            else:
                print("ℹ️  Standard JOIN was used (UUID matching doesn't trigger SPATIAL_JOIN)")

            print(f"📊 Performance optimization: {join_result.get('optimization_used', 'Unknown')}")

        else:
            print("⚠️  SPATIAL_JOIN approach failed, trying chunked fallback...")
            join_result = perform_chunked_spatial_join(
                building_ids=building_ids,
                geodanmark_path=geodanmark_path,
                output_dir=output_dir,
                chunk_size=settings.spatial_chunk_size,
            )

    except Exception as e:
        print(f"❌ SPATIAL_JOIN approach failed: {e}")
        print("🔄 Falling back to chunked processing...")

        try:
            join_result = perform_chunked_spatial_join(
                building_ids=building_ids,
                geodanmark_path=geodanmark_path,
                output_dir=output_dir,
                chunk_size=settings.spatial_chunk_size,
            )
        except Exception as fallback_error:
            print(f"❌ Fallback also failed: {fallback_error}")
            sys.exit(1)

    # Process results
    if join_result["success"]:
        print(f"✅ Successfully processed {join_result['joined_buildings_count']:,} buildings")

        # Copy INSPIRE attributes if available
        inspire_attrs_path = Path("data/inspire_attributes.parquet")
        if inspire_attrs_path.exists():
            import pandas as pd

            inspire_attrs = pd.read_parquet(inspire_attrs_path)
            inspire_attrs.to_parquet(output_dir / "inspire_attributes.parquet")
            print("💾 Saved INSPIRE attributes")

        # Output for GitHub Actions
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"bronze-output-dir={output_dir}\n")
                f.write(f"joined-buildings-count={join_result['joined_buildings_count']}\n")
                f.write(
                    f"spatial-join-used={join_result.get('spatial_join_operator_used', False)}\n"
                )
                f.write(f"optimization-used={join_result.get('optimization_used', 'Unknown')}\n")

        print("🎉 Bronze layer processing completed successfully!")
        print(f"📈 Used optimization: {join_result.get('optimization_used', 'Unknown')}")

    else:
        print(f"❌ Processing failed: {join_result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
