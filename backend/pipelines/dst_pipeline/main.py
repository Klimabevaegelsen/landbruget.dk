#!/usr/bin/env python3
"""
Danish Statistics API Pipeline - Main Orchestrator

This is the main entry point for the DST API pipeline that orchestrates
the bronze and silver layers following the medallion architecture.

The pipeline flow:
1. Bronze Layer: Fetch raw data from DST API and store as JSON
2. Silver Layer: Process and clean the bronze data to Parquet format
3. Export/Gold Layer: (Future) Aggregate and prepare for analytics

Usage:
    python main.py --tables HST77 GARTN1 FRO HALM1
    python main.py --bronze-only  # Run only bronze layer
    python main.py --silver-only  # Run only silver layer
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

# Import the layer modules
import bronze
import silver


def setup_logging(level: str) -> None:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="DST API Pipeline - Danish Statistics Agricultural Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --tables HST77 GARTN1         # Process specific tables
  python main.py --bronze-only                 # Run only bronze layer  
  python main.py --silver-only                 # Run only silver layer
        """,
    )

    # Pipeline control
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Run only the bronze layer (data fetching)",
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Run only the silver layer (data processing)",
    )

    # Data selection
    parser.add_argument(
        "--tables",
        nargs="*",
        default=["HST77", "GARTN1", "FRO", "HALM1"],
        help="Tables to process (default: all available tables)",
    )

    # Bronze layer options
    parser.add_argument(
        "--lang", choices=["da", "en"], default="da", help="Language for API requests"
    )
    parser.add_argument("--variables", nargs="*", help="Specific variables to fetch")
    parser.add_argument("--start-time", help="Start time for data (YYYY format)")
    parser.add_argument("--end-time", help="End time for data (YYYY format)")

    # Silver layer options
    parser.add_argument(
        "--bronze-dir", default="bronze/dst", help="Bronze layer input directory"
    )
    parser.add_argument(
        "--silver-dir", default="silver/dst", help="Silver layer output directory"
    )
    parser.add_argument("--force", action="store_true", help="Force reprocessing")

    # General options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def run_bronze_layer(args: argparse.Namespace) -> bool:
    """Run the bronze layer for specified tables"""
    logging.info("=" * 60)
    logging.info("STARTING BRONZE LAYER - Data Fetching")
    logging.info("=" * 60)

    success_count = 0
    total_tables = len(args.tables)

    for table_id in args.tables:
        try:
            logging.info(f"Processing bronze layer for table: {table_id}")

            # Create bronze layer arguments
            bronze_args = argparse.Namespace(
                table_id=table_id,
                lang=args.lang,
                variables=args.variables,
                start_time=args.start_time,
                end_time=args.end_time,
                log_level=args.log_level,
            )

            # Run bronze layer
            bronze.main_with_args(bronze_args)
            success_count += 1
            logging.info(f"✅ Successfully processed bronze layer for {table_id}")

        except Exception as e:
            logging.error(f"❌ Failed to process bronze layer for {table_id}: {e}")
            continue

    logging.info("=" * 60)
    logging.info(
        f"BRONZE LAYER COMPLETE: {success_count}/{total_tables} tables processed"
    )
    logging.info("=" * 60)

    return success_count > 0


def run_silver_layer(args: argparse.Namespace) -> bool:
    """Run the silver layer for specified tables"""
    logging.info("=" * 60)
    logging.info("STARTING SILVER LAYER - Data Processing")
    logging.info("=" * 60)

    try:
        # Create silver layer arguments
        silver_args = argparse.Namespace(
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            log_level=args.log_level,
            tables=args.tables,
            force=args.force,
        )

        # Run silver layer
        result = silver.main_with_args(silver_args)

        logging.info("=" * 60)
        logging.info("SILVER LAYER COMPLETE")
        logging.info("=" * 60)

        return result

    except Exception as e:
        logging.error(f"❌ Failed to run silver layer: {e}")
        return False


def main():
    """Main pipeline orchestrator"""
    args = parse_args()
    setup_logging(args.log_level)

    start_time = datetime.now()
    logging.info("🚀 Starting DST API Pipeline")
    logging.info(f"📊 Processing tables: {', '.join(args.tables)}")
    logging.info(f"⏰ Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    success = True

    # Validate arguments
    if args.bronze_only and args.silver_only:
        logging.error("❌ Cannot specify both --bronze-only and --silver-only")
        sys.exit(1)

    try:
        # Run bronze layer (unless silver-only)
        if not args.silver_only:
            bronze_success = run_bronze_layer(args)
            if not bronze_success:
                logging.warning("⚠️ Bronze layer had failures, but continuing...")

        # Run silver layer (unless bronze-only)
        if not args.bronze_only:
            silver_success = run_silver_layer(args)
            if not silver_success:
                logging.error("❌ Silver layer failed")
                success = False

        # Pipeline summary
        end_time = datetime.now()
        duration = end_time - start_time

        logging.info("=" * 60)
        if success:
            logging.info("🎉 DST API PIPELINE COMPLETED SUCCESSFULLY")
        else:
            logging.error("❌ DST API PIPELINE COMPLETED WITH ERRORS")

        logging.info(f"⏰ Duration: {duration}")
        logging.info(f"📊 Tables processed: {', '.join(args.tables)}")
        logging.info("=" * 60)

    except KeyboardInterrupt:
        logging.warning("⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"❌ Pipeline failed with unexpected error: {e}")
        sys.exit(1)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
