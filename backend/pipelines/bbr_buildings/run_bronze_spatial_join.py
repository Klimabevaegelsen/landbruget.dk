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

    # Load building IDs - check multiple possible locations
    possible_paths = [
        Path("data/inspire_building_ids.json"),
        Path("inspire_building_ids.json"),
        Path("data/bronze/inspire_building_ids.json"),
    ]

    building_ids_file = None
    for path in possible_paths:
        if path.exists():
            building_ids_file = path
            break

    if building_ids_file is None:
        print("❌ Error: inspire_building_ids.json not found in any of these locations:")
        for path in possible_paths:
            print(f"  - {path}")
        print("\nCurrent directory contents:")
        for item in Path(".").iterdir():
            print(f"  {item}")
        print("\nData directory contents:")
        data_dir = Path("data")
        if data_dir.exists():
            for item in data_dir.iterdir():
                print(f"  {item}")
        sys.exit(1)

    print(f"📋 Loading building IDs from: {building_ids_file}")
    with open(building_ids_file) as f:
        building_ids = json.load(f)

    print(f"🔍 Processing {len(building_ids):,} INSPIRE BBR buildings with GeoDanmark data...")

    # Check GeoDanmark data exists - check multiple possible locations
    geodanmark_paths = [
        "data/geodanmark_buildings_complete.geoparquet",
        "geodanmark_buildings_complete.geoparquet",
    ]

    geodanmark_path = None
    for path in geodanmark_paths:
        if Path(path).exists():
            geodanmark_path = path
            break

    if geodanmark_path is None:
        print(
            "❌ Error: geodanmark_buildings_complete.geoparquet not found in any of these locations:"
        )
        for path in geodanmark_paths:
            print(f"  - {path}")
        sys.exit(1)

    print(f"📊 Using GeoDanmark data from: {geodanmark_path}")

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
        inspire_attrs_paths = [
            Path("data/inspire_attributes.parquet"),
            Path("inspire_attributes.parquet"),
            Path("data/bronze/inspire_attributes.parquet"),
        ]

        inspire_attrs_path = None
        for path in inspire_attrs_paths:
            if path.exists():
                inspire_attrs_path = path
                break

        if inspire_attrs_path:
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
