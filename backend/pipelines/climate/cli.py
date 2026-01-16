"""
CLI Entry Point for Climate Pipeline

This module provides command-line interface for running climate calculations
in both single-CVR and batch modes, designed for GitHub Actions runners.

Usage:
    # Single CVR mode
    python -m climate --cvr 12345678 --year 2023

    # Batch mode (for GH Actions matrix jobs)
    python -m climate --batch 0 --batch-size 500 --year 2023 --cvr-file /tmp/cvrs.json

    # Discovery mode (find CVRs with data)
    python -m climate --discover --year 2023 --output /tmp/cvrs.json

    # Test mode (limit CVRs for testing)
    python -m climate --discover --year 2023 --test-limit 100
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Add current directory to path for local imports
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))


def discover_cvrs(year: int, test_limit: Optional[int] = None) -> dict:
    """
    Discover all CVRs with agricultural data for a given year.

    Queries Green Accounts (livestock) and FVM (fields) to find all CVRs
    that have data available for climate calculations.

    Args:
        year: Year to discover CVRs for
        test_limit: Optional limit for testing (processes only N CVRs)

    Returns:
        Dictionary with:
        - cvr_numbers: List of CVR strings
        - count: Total CVR count
        - batch_count: Number of batches (at 500 per batch)
        - sources: Dict with counts from each source
    """
    from data_loader import ClimateDataLoader

    loader = ClimateDataLoader()
    cvr_set = set()
    sources = {"green_accounts": 0, "fvm_fields": 0}

    # 1. Query Green Accounts for CVRs with livestock
    logger.info(f"Discovering CVRs with livestock data (year {year})...")
    try:
        pattern = f"gs://{loader.bucket}/silver/gr {year}/*/V_4061GR_*_DYRERK_*_pii_handled.parquet"
        files = loader.gcs.list_files(pattern)

        if files:
            latest_file = sorted(files)[-1]
            table_name = "gr_discover_temp"
            loader.gcs.create_table_from_gcs(table_name, latest_file)

            query = f"SELECT DISTINCT cvr_number FROM {table_name} WHERE cvr_number IS NOT NULL"
            result_df = loader.gcs.duckdb_conn.execute(query).df()
            loader.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            livestock_cvrs = set(result_df["cvr_number"].astype(str).str.zfill(8).tolist())
            cvr_set.update(livestock_cvrs)
            sources["green_accounts"] = len(livestock_cvrs)
            logger.info(f"  Found {len(livestock_cvrs)} CVRs with livestock")
    except Exception as e:
        logger.warning(f"Failed to query Green Accounts: {e}")

    # 2. Query FVM for CVRs with fields
    logger.info(f"Discovering CVRs with field data (year {year})...")
    try:
        pattern = f"gs://{loader.bucket}/silver/fvm_marker_{year}/*/data.parquet"
        files = loader.gcs.list_files(pattern)

        if files:
            latest_file = sorted(files)[-1]
            table_name = "fvm_discover_temp"
            loader.gcs.create_table_from_gcs(table_name, latest_file)

            # Check column name (cvr vs cvr_number)
            sample = loader.gcs.duckdb_conn.execute(f"SELECT * FROM {table_name} LIMIT 0").df()
            cvr_col = "cvr_number" if "cvr_number" in sample.columns else "cvr"

            query = f"SELECT DISTINCT {cvr_col} as cvr_number FROM {table_name} WHERE {cvr_col} IS NOT NULL"
            result_df = loader.gcs.duckdb_conn.execute(query).df()
            loader.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            field_cvrs = set(result_df["cvr_number"].astype(str).str.zfill(8).tolist())
            cvr_set.update(field_cvrs)
            sources["fvm_fields"] = len(field_cvrs)
            logger.info(f"  Found {len(field_cvrs)} CVRs with fields")
    except Exception as e:
        logger.warning(f"Failed to query FVM: {e}")

    # Convert to sorted list
    cvr_list = sorted(list(cvr_set))

    # Apply test limit if specified
    if test_limit and test_limit > 0:
        cvr_list = cvr_list[:test_limit]
        logger.info(f"Test limit applied: using {len(cvr_list)} CVRs")

    # Calculate batch count (500 CVRs per batch)
    batch_size = 500
    batch_count = (len(cvr_list) + batch_size - 1) // batch_size

    result = {
        "cvr_numbers": cvr_list,
        "count": len(cvr_list),
        "batch_count": batch_count,
        "batch_size": batch_size,
        "year": year,
        "sources": sources,
        "discovered_at": datetime.utcnow().isoformat(),
    }

    logger.info(f"Discovery complete: {result['count']} unique CVRs, {batch_count} batches")
    return result


def run_single_cvr(cvr: str, year: int) -> dict:
    """
    Run climate calculation for a single CVR.

    Args:
        cvr: CVR number (8 digits)
        year: Year to calculate

    Returns:
        Emission report as dictionary
    """
    from climate_calculator import FarmClimateCalculator
    from data_loader import ClimateDataLoader

    loader = ClimateDataLoader()
    calculator = FarmClimateCalculator(loader)

    logger.info(f"Calculating emissions for CVR {cvr}, year {year}")
    report = calculator.calculate_emissions(cvr=cvr, year=year)

    return report.to_dict()


def run_batch(
    batch_number: int,
    batch_size: int,
    year: int,
    cvr_file: str,
    output_dir: str,
) -> dict:
    """
    Run climate calculation for a batch of CVRs.

    Args:
        batch_number: Batch index (0-based)
        batch_size: Number of CVRs per batch
        year: Year to calculate
        cvr_file: Path to JSON file with CVR list (from discovery)
        output_dir: Directory to write batch output

    Returns:
        Summary of batch processing
    """
    from batch_processor import BatchProcessor

    processor = BatchProcessor(output_dir=output_dir)

    return processor.process_batch(
        batch_number=batch_number,
        batch_size=batch_size,
        year=year,
        cvr_file=cvr_file,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Climate Pipeline - Calculate farm emissions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover CVRs with data
  python -m climate --discover --year 2023 --output /tmp/cvrs.json

  # Run single CVR
  python -m climate --cvr 31373077 --year 2023

  # Run batch (for GH Actions)
  python -m climate --batch 0 --batch-size 500 --year 2023 --cvr-file /tmp/cvrs.json
        """,
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--discover", action="store_true", help="Discover CVRs with agricultural data")
    mode_group.add_argument("--cvr", type=str, help="Single CVR number to process")
    mode_group.add_argument("--batch", type=int, help="Batch number (0-indexed) for parallel processing")

    # Common arguments
    parser.add_argument("--year", type=int, required=True, help="Year to calculate emissions for")
    parser.add_argument("--output", type=str, help="Output file path (for discover/single modes)")
    parser.add_argument("--output-dir", type=str, default="/tmp/climate_output", help="Output directory for batch mode")

    # Batch mode arguments
    parser.add_argument("--batch-size", type=int, default=500, help="CVRs per batch (default: 500)")
    parser.add_argument("--cvr-file", type=str, help="Path to CVR list JSON (from discover mode)")

    # Testing arguments
    parser.add_argument("--test-limit", type=int, help="Limit CVRs for testing (discover mode only)")

    args = parser.parse_args()

    try:
        if args.discover:
            # Discovery mode
            result = discover_cvrs(year=args.year, test_limit=args.test_limit)

            # Write output
            if args.output:
                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    json.dump(result, f, indent=2)
                logger.info(f"Discovery results written to {output_path}")

                # Also output for GH Actions
                print(f"::set-output name=cvr_count::{result['count']}")
                print(f"::set-output name=batch_count::{result['batch_count']}")
            else:
                # Print to stdout
                print(json.dumps(result, indent=2))

        elif args.cvr:
            # Single CVR mode
            result = run_single_cvr(cvr=args.cvr, year=args.year)

            if args.output:
                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    json.dump(result, f, indent=2)
                logger.info(f"Report written to {output_path}")
            else:
                print(json.dumps(result, indent=2))

        elif args.batch is not None:
            # Batch mode
            if not args.cvr_file:
                parser.error("--cvr-file is required for batch mode")

            result = run_batch(
                batch_number=args.batch,
                batch_size=args.batch_size,
                year=args.year,
                cvr_file=args.cvr_file,
                output_dir=args.output_dir,
            )

            print(json.dumps(result, indent=2))

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
