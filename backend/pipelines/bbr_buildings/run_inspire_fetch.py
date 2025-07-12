#!/usr/bin/env python3
"""Standalone script to run INSPIRE BBR fetch for GitHub Actions."""

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from bronze.inspire_bbr_fetcher import InspireBBRFetcher
from config.settings import get_settings
from utils.logger import setup_logger


def _save_attributes_to_parquet(attributes_data, output_dir):
    """Save attributes data to parquet using DuckDB streaming approach."""
    if not attributes_data:
        print("⚠️ No attributes data to save")
        return

    print(f"💾 Saving {len(attributes_data):,} building attributes to parquet...")

    # Use streaming approach for large datasets
    if len(attributes_data) > 100000:
        print("🌊 Large dataset - using streaming parquet write")
        _save_attributes_streaming(attributes_data, output_dir)
    else:
        print("📝 Small dataset - using standard parquet write")
        _save_attributes_standard(attributes_data, output_dir)

    # Also save a copy in the data root for GitHub Actions compatibility
    data_root = Path("data")
    data_root.mkdir(exist_ok=True)
    parquet_file = output_dir / "inspire_attributes.parquet"
    if parquet_file.exists():
        import shutil

        shutil.copy2(parquet_file, data_root / "inspire_attributes.parquet")
        print("💾 Copied attributes to data root for GitHub Actions compatibility")


def _save_attributes_streaming(attributes_data, output_dir):
    """Save large attributes dataset using streaming approach with better memory management."""
    import duckdb

    # Process in smaller chunks to avoid memory issues
    chunk_size = 25000  # Reduced chunk size for better memory management
    total_chunks = (len(attributes_data) + chunk_size - 1) // chunk_size

    print(
        f"🌊 Streaming {len(attributes_data):,} records in {total_chunks} chunks of {chunk_size:,}"
    )

    conn = duckdb.connect(":memory:")
    output_dir.mkdir(exist_ok=True)
    parquet_file = output_dir / "inspire_attributes.parquet"

    try:
        # Process first chunk to create initial parquet file
        first_chunk = attributes_data[:chunk_size]
        if first_chunk:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(first_chunk, f)
                temp_json_path = f.name

            try:
                conn.execute(
                    f"CREATE TABLE buildings AS SELECT * FROM read_json('{temp_json_path}')"
                )
                conn.execute(f"COPY buildings TO '{parquet_file}' (FORMAT PARQUET)")
                print(f"   ✅ Created parquet with chunk 1/{total_chunks}")
            finally:
                os.unlink(temp_json_path)

        # Process remaining chunks and append
        for chunk_idx in tqdm(range(1, total_chunks), desc="Processing chunks"):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(attributes_data))
            chunk = attributes_data[start_idx:end_idx]

            if not chunk:
                break

            # Write chunk to temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(chunk, f)
                temp_json_path = f.name

            try:
                # Create temporary table and append to parquet
                conn.execute("DROP TABLE IF EXISTS temp_chunk")
                conn.execute(
                    f"CREATE TABLE temp_chunk AS SELECT * FROM read_json('{temp_json_path}')"
                )

                # Append to existing parquet file
                conn.execute("""
                    INSERT INTO buildings 
                    SELECT * FROM temp_chunk
                """)

                # Periodically rewrite parquet to avoid memory buildup
                if chunk_idx % 20 == 0:  # Every 20 chunks
                    conn.execute(f"COPY buildings TO '{parquet_file}' (FORMAT PARQUET)")
                    print(f"   💾 Checkpoint: Saved progress at chunk {chunk_idx + 1}")

            finally:
                os.unlink(temp_json_path)

            # Aggressive memory cleanup for large datasets
            if chunk_idx % 10 == 0:
                import gc

                gc.collect()

        # Final write to parquet
        conn.execute(f"COPY buildings TO '{parquet_file}' (FORMAT PARQUET)")
        print("💾 ✅ Completed streaming parquet write")

    except Exception as e:
        print(f"❌ Error in streaming parquet write: {e}")
        raise
    finally:
        conn.close()


def _save_attributes_standard(attributes_data, output_dir):
    """Save small attributes dataset using standard approach."""
    import duckdb

    conn = duckdb.connect(":memory:")
    output_dir.mkdir(exist_ok=True)
    parquet_file = output_dir / "inspire_attributes.parquet"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(attributes_data, f)
        temp_json_path = f.name

    try:
        conn.execute(f"CREATE TABLE buildings AS SELECT * FROM read_json('{temp_json_path}')")
        conn.execute(f"COPY buildings TO '{parquet_file}' (FORMAT PARQUET)")
        print(f"💾 Saved INSPIRE attributes to {parquet_file}")
    finally:
        os.unlink(temp_json_path)
        conn.close()


def _process_with_streaming(inspire_fetcher, output_dir, sample_size, pipeline_start_time):
    """Process INSPIRE BBR data using streaming approach to manage memory."""
    print("🌊 Using streaming processing for large dataset...")

    try:
        # For large datasets, we need to use a different approach since return_data=False
        # doesn't save the building data to files. Instead, we'll process with return_data=True
        # but immediately stream the data to files and clear from memory.

        print("⚡ Processing with memory-optimized approach...")

        if sample_size is None or sample_size > 100000:
            print("🔄 Large dataset detected - using chunked processing with immediate streaming")

            # For very large datasets, we need to process in a memory-efficient way
            # The InspireBBRFetcher doesn't have a built-in streaming mode that saves to files
            # So we'll use return_data=True but process and save data immediately

            result = inspire_fetcher.fetch_data(
                output_dir,
                sample_size=sample_size,
                return_data=True,  # We need the data to save it
                pipeline_start_time=pipeline_start_time,
            )

            if result and "data" in result:
                building_ids = result["data"].get("building_ids", [])
                attributes_data = result["data"].get("attributes_df", [])
                building_count = len(building_ids)

                print(f"📊 Processing {building_count:,} buildings with streaming saves...")

                # Immediately save building IDs to file
                print("💾 Saving building IDs...")
                output_dir.mkdir(exist_ok=True)
                with open(output_dir / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Also save a copy in the data root for GitHub Actions compatibility
                data_root = Path("data")
                data_root.mkdir(exist_ok=True)
                with open(data_root / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Save attributes using streaming approach for large datasets
                if attributes_data:
                    print(
                        f"💾 Saving {len(attributes_data):,} building attributes with streaming..."
                    )
                    _save_attributes_to_parquet(attributes_data, output_dir)

                # Immediately clear data from memory
                del building_ids, attributes_data, result
                import gc

                gc.collect()

                print("🧹 Cleared data from memory after saving")

            else:
                print("⚠️ No data returned from fetcher")
                building_count = 0

        else:
            # Medium-sized dataset - use standard processing with memory optimizations
            print(
                f"📊 Medium dataset ({sample_size:,}) - using standard processing with optimizations"
            )
            result = inspire_fetcher.fetch_data(
                output_dir,
                sample_size=sample_size,
                return_data=True,
                pipeline_start_time=pipeline_start_time,
            )

            if result and "data" in result:
                building_count = len(result["data"].get("building_ids", []))

                # Save data and immediately clear from memory
                building_ids = result["data"].get("building_ids", [])
                attributes_data = result["data"].get("attributes_df", [])

                # Save building IDs
                output_dir.mkdir(exist_ok=True)
                with open(output_dir / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Also save a copy in the data root for GitHub Actions compatibility
                data_root = Path("data")
                data_root.mkdir(exist_ok=True)
                with open(data_root / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Save attributes using streaming approach
                _save_attributes_to_parquet(attributes_data, output_dir)

                # Clear data from memory immediately
                del building_ids, attributes_data, result
                import gc

                gc.collect()
            else:
                building_count = 0

        return {
            "building_count": building_count,
            "streaming_mode": True,
            "source": "inspire_bbr_streaming",
            "timestamp": pipeline_start_time.strftime("%Y%m%d_%H%M%S"),
        }

    except Exception as e:
        print(f"❌ Streaming processing failed: {e}")
        # Try to get building count from any files that might have been saved
        building_count = _get_building_count_from_files(output_dir)
        if building_count > 0:
            print(f"⚠️ Partial success: Found {building_count:,} buildings in saved files")
            return {
                "building_count": building_count,
                "streaming_mode": True,
                "source": "inspire_bbr_partial",
                "timestamp": pipeline_start_time.strftime("%Y%m%d_%H%M%S"),
                "error": str(e),
            }
        else:
            raise


def _get_building_count_from_files(output_dir=None):
    """Get building count from saved files after streaming processing."""
    building_count = 0

    if output_dir is None:
        output_dir = Path("data")

    # Check for building IDs file
    building_ids_file = output_dir / "inspire_building_ids.json"
    if building_ids_file.exists():
        try:
            with open(building_ids_file) as f:
                building_ids = json.load(f)
                building_count = len(building_ids)
        except Exception as e:
            print(f"⚠️ Could not read building IDs file: {e}")

    # If no building IDs file, try to count from parquet
    if building_count == 0:
        parquet_file = output_dir / "inspire_attributes.parquet"
        if parquet_file.exists():
            try:
                import duckdb

                conn = duckdb.connect(":memory:")
                result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')"
                ).fetchone()
                building_count = result[0] if result else 0
                conn.close()
            except Exception as e:
                print(f"⚠️ Could not count from parquet file: {e}")

    return building_count


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
            # Process all buildings - no artificial limits

        print("📦 Starting INSPIRE BBR fetch...")
        inspire_fetcher = InspireBBRFetcher(settings, logger)

        pipeline_start_time = datetime.now()

        # Use streaming approach for large datasets to avoid memory issues
        if sample_size and sample_size <= 10000:
            # Small sample - use traditional approach
            print(f"🧪 Small sample ({sample_size:,}) - using traditional processing")
            inspire_result = inspire_fetcher.fetch_data(
                output_dir,
                sample_size=sample_size,
                return_data=True,
                pipeline_start_time=pipeline_start_time,
            )

            if inspire_result and "data" in inspire_result:
                building_ids = inspire_result["data"].get("building_ids", [])
                print(f"🏢 Total INSPIRE BBR buildings fetched: {len(building_ids):,}")

                # Save building IDs for the join job
                output_dir.mkdir(exist_ok=True)
                with open(output_dir / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Also save a copy in the data root for GitHub Actions compatibility
                data_root = Path("data")
                data_root.mkdir(exist_ok=True)
                with open(data_root / "inspire_building_ids.json", "w") as f:
                    json.dump(building_ids, f)

                # Save attributes data if available
                _save_attributes_to_parquet(
                    inspire_result["data"].get("attributes_df", []), output_dir
                )

        else:
            # Large dataset or full processing - use streaming approach
            print("🌊 Large dataset - using streaming processing to manage memory")
            inspire_result = _process_with_streaming(
                inspire_fetcher, output_dir, sample_size, pipeline_start_time
            )

        if inspire_result:
            # Extract building count from result
            if isinstance(inspire_result, dict) and "data" in inspire_result:
                building_count = len(inspire_result["data"].get("building_ids", []))
            else:
                # For streaming results, building count should be in the result metadata
                building_count = inspire_result.get("building_count", 0)

            print(f"🏢 Total INSPIRE BBR buildings processed: {building_count:,}")

            # Write to GITHUB_OUTPUT if running in GitHub Actions
            if "GITHUB_OUTPUT" in os.environ:
                with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                    f.write(f"buildings-count={building_count}\n")
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
