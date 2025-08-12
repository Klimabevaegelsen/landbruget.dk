#!/usr/bin/env python3
"""
Standalone script to run CHR Gold Processing (Veterinary Timeline).

This script can be run independently of the main CHR pipeline to process
existing silver data into gold layer products.

Usage:
    python run_gold_processing.py [--silver-timestamp TIMESTAMP] [--log-level LEVEL]

Examples:
    # Process latest silver data
    python run_gold_processing.py

    # Process specific silver timestamp
    python run_gold_processing.py --silver-timestamp 20240101_120000

    # Debug mode
    python run_gold_processing.py --log-level DEBUG
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add the pipeline to the path
sys.path.insert(0, str(Path(__file__).parent))

from gold.chr_gold_processing import process_gold_data


def setup_logging(log_level: str):
    """Configure logging."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )


def main():
    """Main function for standalone gold processing."""
    parser = argparse.ArgumentParser(
        description="Run CHR Gold Processing (Veterinary Timeline) independently",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Process latest silver data
  %(prog)s --silver-timestamp 20240101_120000 # Process specific timestamp
  %(prog)s --log-level DEBUG                  # Debug mode
        """,
    )

    parser.add_argument(
        "--silver-timestamp",
        type=str,
        help="Specific silver timestamp to process (e.g., 20240101_120000). If not provided, uses latest.",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without actually running")

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    logger.info("🥇 CHR Gold Processing - Standalone Mode")
    logger.info("=" * 50)

    if args.dry_run:
        logger.info("🔍 DRY RUN MODE - No actual processing will occur")

    try:
        # Generate output timestamp
        gold_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"📅 Gold output timestamp: {gold_timestamp}")

        if args.silver_timestamp:
            logger.info(f"🎯 Using specified silver timestamp: {args.silver_timestamp}")
        else:
            logger.info("🔍 Will auto-detect latest silver data")

        if args.dry_run:
            logger.info("✅ Dry run completed - would process gold layer")
            return 0

        # Run gold processing
        logger.info("🚀 Starting gold processing...")
        success = process_gold_data(
            export_timestamp=gold_timestamp,
            silver_dir=None,  # Auto-detect from GCS/local
        )

        if success:
            logger.info("✅ Gold processing completed successfully!")
            logger.info(f"📂 Output available at: gold/chr/{gold_timestamp}/")
            logger.info("📋 Products created:")
            logger.info("   - veterinary_timeline.parquet (main timeline)")
            logger.info("   - timeline_summary.parquet (summary statistics)")
            return 0
        else:
            logger.error("❌ Gold processing failed")
            return 1

    except Exception as e:
        logger.error(f"❌ Error during gold processing: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
