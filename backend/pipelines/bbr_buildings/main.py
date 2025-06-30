#!/usr/bin/env python3
"""
BBR Buildings Pipeline - Main Entry Point

This pipeline fetches and processes Danish building data from Bygnings- og Boligregistret (BBR)
to support agricultural and public health analyses.

Updated to use bulk GeoDanmark download + local joins for improved performance.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Updated imports for bulk approach
from bronze.bulk_geodanmark_fetcher import BulkGeoDanmarkFetcher
from bronze.inspire_bbr_fetcher import InspireBBRFetcher
from config import Settings, get_settings
from silver.building_processor import BuildingProcessor
from utils.logger import setup_logger


def main():
    """Main entry point for the BBR buildings pipeline."""
    parser = argparse.ArgumentParser(
        description="BBR Buildings Data Pipeline - Now with bulk GeoDanmark download!",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "both"],
        required=True,
        help="Pipeline layer to execute",
    )

    parser.add_argument(
        "--input-dir", type=Path, help="Input directory (required for silver layer)"
    )

    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"), help="Output directory (default: data)"
    )

    parser.add_argument("--sample-size", type=int, help="Sample size for testing")

    parser.add_argument(
        "--bulk-download",
        action="store_true",
        default=True,
        help="Use bulk GeoDanmark download (default: True, much faster!)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logger(level=args.log_level)

    # Load configuration
    settings = get_settings()

    # Track pipeline start time for consistent timestamping
    pipeline_start_time = datetime.now()

    try:
        if args.layer == "bronze":
            run_bronze_layer_bulk(args, settings, logger, pipeline_start_time)

        elif args.layer == "silver":
            if not args.input_dir:
                logger.error("--input-dir is required for silver layer")
                sys.exit(1)

            run_silver_layer(args, settings, logger)

        elif args.layer == "both":
            # Run bronze layer and get data in memory
            logger.info(
                "Running both layers - bronze will export and pass data to silver in memory"
            )
            bronze_data = run_bronze_layer_bulk(
                args, settings, logger, pipeline_start_time, return_data=True
            )

            # Run silver layer with in-memory data
            run_silver_layer(args, settings, logger, bronze_data=bronze_data)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


def run_bronze_layer_bulk(
    args: argparse.Namespace,
    settings: Settings,
    logger: logging.Logger,
    pipeline_start_time: datetime,
    return_data: bool = False,
):
    """Execute bronze layer processing with bulk GeoDanmark download + local joins."""
    logger.info("🚀 Starting bronze layer with BULK GeoDanmark download + local joins")

    output_dir = args.output_dir / "bronze"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Bulk download ALL GeoDanmark buildings
    logger.info("📦 Step 1: Bulk downloading GeoDanmark buildings...")

    if not settings.has_datafordeler_credentials:
        raise ValueError(
            "DATAFORDELER_USERNAME and DATAFORDELER_PASSWORD environment variables required"
        )

    bulk_fetcher = BulkGeoDanmarkFetcher(
        settings.datafordeler_username, settings.datafordeler_password
    )

    # Download buildings
    bulk_fetcher.bulk_download_buildings(batch_size=30000)

    logger.info("✅ Bulk GeoDanmark download completed!")

    # Step 2: Fetch and enrich INSPIRE BBR data
    logger.info("🏢 Step 2: Fetching INSPIRE BBR building attributes with GraphQL enrichment...")
    inspire_fetcher = InspireBBRFetcher(settings, logger)
    inspire_result = inspire_fetcher.fetch_data(
        output_dir,
        sample_size=args.sample_size,  # None = all buildings, or specify for testing
        return_data=True,  # Always return data for local joins
        pipeline_start_time=pipeline_start_time,
    )

    # Step 3: Perform local spatial join
    logger.info("🔗 Step 3: Performing local spatial join...")

    if inspire_result and "data" in inspire_result:
        # Load GeoDanmark buildings
        import duckdb

        conn = duckdb.connect()
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        # Load both datasets
        geodanmark_path = "data/geodanmark_buildings_complete.geoparquet"

        # Extract INSPIRE BBR building IDs
        inspire_data = inspire_result["data"]
        if "building_ids" in inspire_data and "attributes_df" in inspire_data:
            building_ids = inspire_data["building_ids"]
            attributes_df = inspire_data["attributes_df"]
        else:
            logger.warning("INSPIRE BBR data structure not as expected")
            building_ids = []
            attributes_df = None

        logger.info(
            f"🔍 Joining {len(building_ids):,} INSPIRE BBR buildings with GeoDanmark data..."
        )

        # Perform the join using BBRUUID
        if len(building_ids) > 0:
            # Convert building_ids to SQL-compatible format
            uuid_list = "', '".join(building_ids)

            join_query = f"""
            SELECT 
                g.*,
                'matched' as join_status
            FROM read_parquet('{geodanmark_path}') g
            WHERE g.BBRUUID IN ('{uuid_list}')
            """

            try:
                joined_buildings = conn.execute(join_query).fetchdf()
                logger.info(f"✅ Successfully joined {len(joined_buildings):,} buildings")

                # Save joined results
                output_file = (
                    output_dir
                    / f"joined_buildings_{pipeline_start_time.strftime('%Y%m%d_%H%M%S')}.geoparquet"
                )
                joined_buildings.to_parquet(output_file)
                logger.info(f"💾 Saved joined results to {output_file}")

                result = {
                    "data": {
                        "joined_buildings": joined_buildings,
                        "attributes_df": attributes_df,
                        "building_ids": building_ids,
                    },
                    "metadata": {
                        "inspire_metadata": inspire_result.get("metadata", None),
                        "geodanmark_buildings_total": conn.execute(
                            f"SELECT COUNT(*) FROM read_parquet('{geodanmark_path}')"
                        ).fetchone()[0],
                        "joined_buildings_count": len(joined_buildings),
                        "source": "bulk_geodanmark_with_inspire_bbr_join",
                        "join_method": "local_spatial_join_by_bbruuid",
                    },
                }

            except Exception as e:
                logger.error(f"❌ Join failed: {e}")
                result = None
        else:
            logger.warning("⚠️ No building IDs to join")
            result = None

        conn.close()
    else:
        logger.error("❌ INSPIRE BBR data not available for joining")
        result = None

    logger.info("🎉 Bronze layer processing completed successfully with bulk approach!")

    if return_data:
        return result
    return None


def run_silver_layer(
    args: argparse.Namespace, settings: Settings, logger: logging.Logger, bronze_data=None
):
    """Execute silver layer processing."""
    logger.info("Starting silver layer processing")

    output_dir = args.output_dir / "silver"
    output_dir.mkdir(parents=True, exist_ok=True)

    processor = BuildingProcessor(settings, logger)

    if bronze_data is not None:
        # Use data directly from bronze layer (in-memory processing)
        logger.info("Using bronze data from memory - skipping disk I/O")
        processor.process_buildings_from_data(
            bronze_data=bronze_data,
            output_dir=output_dir,
        )
    else:
        # Traditional mode: read from disk
        processor.process_buildings(
            input_dir=args.input_dir,
            output_dir=output_dir,
        )

    logger.info("Silver layer processing completed successfully")


if __name__ == "__main__":
    main()
