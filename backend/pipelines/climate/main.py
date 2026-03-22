#!/usr/bin/env python3
"""
Climate Tool Main CLI Entry Point

This CLI calculates farm-level climate emissions for Danish agricultural operations.
It integrates data from CHR (livestock), FVM (fields), and fertilizer sources to
compute CO2e emissions across multiple categories.

Usage:
    python main.py --cvr 12345678 --year 2024
    python main.py --cvr all --year 2024 --output gold
    python main.py --cvr 12345678 --year 2024 --dry-run

Requirements:
    - GCS_BUCKET environment variable set
    - Access to silver layer data (chr, fvm_marker_YYYY, fertiliser)
"""

import logging
import sys
from pathlib import Path

import click
from common.cli import common_options

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from climate_calculator import EmissionReport, FarmClimateCalculator
from data_loader import ClimateDataLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_cvr_list(
    loader: ClimateDataLoader, cvr_arg: str, year: int, limit: int | None = None
) -> list[str]:
    """
    Get list of CVR numbers to process.

    Args:
        loader: ClimateDataLoader instance
        cvr_arg: Either a CVR number or 'all'
        year: Year to check for data availability
        limit: Maximum number of CVRs to return

    Returns:
        List of CVR numbers (8-digit strings)
    """
    if cvr_arg.lower() == "all":
        logger.info(f"Finding all CVRs with data for year {year}...")
        cvrs = get_all_cvrs_with_data(loader, year)

        if limit:
            cvrs = cvrs[:limit]
            logger.info(f"Limited to {limit} CVRs")

        logger.info(f"Found {len(cvrs)} CVRs with data")
        return cvrs
    # Validate single CVR format
    cvr_str = str(cvr_arg).zfill(8)
    if len(cvr_str) != 8 or not cvr_str.isdigit():
        raise ValueError(f"Invalid CVR format: {cvr_arg}. Must be 8 digits.")
    return [cvr_str]


def get_all_cvrs_with_data(loader: ClimateDataLoader, year: int) -> list[str]:
    """
    Get all CVRs that have data available for the specified year.

    This queries the field data (FVM) as it's the most comprehensive source.

    Args:
        loader: ClimateDataLoader instance
        year: Year to check

    Returns:
        List of CVR numbers
    """
    try:
        # Query field data to get all CVRs with data
        pattern = f"gs://{loader.bucket}/silver/fvm_marker_{year}/*/data.parquet"
        files = loader.gcs.list_files(pattern)

        if not files:
            logger.warning(f"No field data found for year {year}")
            return []

        # Use latest file
        latest_file = sorted(files)[-1]
        logger.info(f"Loading CVR list from: {latest_file}")

        # Create table and query for unique CVRs
        table_name = "fvm_cvr_list"
        loader.gcs.create_table_from_gcs(table_name, latest_file)

        query = f"SELECT DISTINCT cvr FROM {table_name} WHERE cvr IS NOT NULL ORDER BY cvr"
        result_df = loader.gcs.duckdb_conn.execute(query).df()

        # Cleanup
        loader.gcs.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        cvrs = result_df["cvr"].tolist()
        logger.info(f"Found {len(cvrs)} CVRs with field data for year {year}")

        return cvrs

    except Exception as e:
        logger.error(f"Error getting CVR list: {e}")
        return []


def print_emission_report(report: EmissionReport, verbose: bool = True):
    """
    Print emission report to console in a human-readable format.

    Args:
        report: EmissionReport to display
        verbose: If True, show detailed breakdown
    """
    print(f"\n{'=' * 70}")
    print(f"Climate Emission Report - CVR {report.cvr}, Year {report.year}")
    print(f"{'=' * 70}")

    print(
        f"\nTotal CO2e: {report.total_co2e_kg:,.0f} kg ({report.total_co2e_kg / 1000:,.1f} tonnes)"
    )
    print(f"Data Completeness: {report.data_completeness:.1%}")

    if report.categories:
        print("\nEmission Categories:")
        print(f"{'-' * 70}")

        for cat in report.categories:
            print(f"\n  {cat.name.upper()}")
            print(f"    Total: {cat.co2e_kg:,.0f} kg CO2e")
            print(f"    Data Quality: {cat.data_quality}")

            if verbose and cat.sub_sources:
                print("    Sub-sources:")
                for source_name, source_co2e in cat.sub_sources.items():
                    pct = (source_co2e / cat.co2e_kg * 100) if cat.co2e_kg > 0 else 0
                    print(f"      - {source_name}: {source_co2e:,.0f} kg CO2e ({pct:.1f}%)")

    if report.intensity_metrics:
        print("\nIntensity Metrics:")
        print(f"{'-' * 70}")
        for metric_name, metric_value in report.intensity_metrics.items():
            print(f"  {metric_name}: {metric_value:.2f}")

    print(f"\n{'=' * 70}\n")


def write_output(reports: list[EmissionReport], output_mode: str):
    """
    Write emission reports to specified output destination.

    Args:
        reports: List of EmissionReport objects
        output_mode: 'gold', 'supabase', or 'both'
    """
    # TODO: Implement output_writer module
    # For now, this is a placeholder

    if output_mode in ["gold", "both"]:
        logger.info(f"Writing {len(reports)} reports to GCS gold layer...")
        # TODO: Implement GCS gold layer writing
        logger.warning("GCS gold layer output not yet implemented")

    if output_mode in ["supabase", "both"]:
        logger.info(f"Syncing {len(reports)} reports to Supabase...")
        # TODO: Implement Supabase sync
        logger.warning("Supabase sync not yet implemented")


@click.command()
@click.option(
    "--cvr", required=True, help="CVR number (8 digits) or 'all' to process all farms with data"
)
@click.option("--year", type=int, required=True, help="Year to calculate emissions for (YYYY)")
@click.option(
    "--output",
    type=click.Choice(["gold", "supabase", "both"]),
    default="both",
    help="Output destination (default: both)",
)
@click.option(
    "--dry-run", is_flag=True, help="Calculate emissions but don't write output (for testing)"
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit number of CVRs to process (useful with --cvr all)",
)
@common_options
def main(cvr, year, output, dry_run, limit, log_level):
    """Calculate farm climate emissions from agricultural data."""
    # Set log level
    logging.getLogger().setLevel(getattr(logging, log_level))

    logger.info("Starting Climate Tool")
    logger.info(f"CVR: {cvr}, Year: {year}, Output: {output}, Dry Run: {dry_run}")

    # Initialize components
    try:
        loader = ClimateDataLoader()
        calculator = FarmClimateCalculator(loader)
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        sys.exit(1)

    # Get list of CVRs to process
    try:
        cvrs = get_cvr_list(loader, cvr, year, limit)

        if not cvrs:
            logger.error("No CVRs to process")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error getting CVR list: {e}")
        sys.exit(1)

    # Process each CVR
    reports = []
    successful = 0
    failed = 0

    logger.info(f"Processing {len(cvrs)} farm(s)...")

    for i, cvr_num in enumerate(cvrs, 1):
        try:
            logger.info(f"[{i}/{len(cvrs)}] Processing CVR {cvr_num}...")

            # Calculate emissions
            report = calculator.calculate_emissions(cvr_num, year)
            reports.append(report)

            # Print summary
            print(
                f"  {cvr_num}: {report.total_co2e_kg:,.0f} kg CO2e ({report.data_completeness:.0%} complete)"
            )

            successful += 1

        except Exception as e:
            logger.error(f"Failed to process CVR {cvr_num}: {e}")
            failed += 1
            continue

    # Summary statistics
    print(f"\n{'=' * 70}")
    print("Processing Complete")
    print(f"{'=' * 70}")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(cvrs)}")

    if reports:
        total_emissions = sum(r.total_co2e_kg for r in reports)
        avg_emissions = total_emissions / len(reports)
        avg_completeness = sum(r.data_completeness for r in reports) / len(reports)

        print("\nAggregate Statistics:")
        print(
            f"  Total Emissions: {total_emissions:,.0f} kg CO2e ({total_emissions / 1000:,.1f} tonnes)"
        )
        print(f"  Average per Farm: {avg_emissions:,.0f} kg CO2e")
        print(f"  Average Completeness: {avg_completeness:.1%}")

    # Show detailed report for single CVR
    if len(reports) == 1:
        print_emission_report(reports[0], verbose=True)

    # Write output (unless dry run)
    if not dry_run and reports:
        try:
            write_output(reports, output)
            logger.info("Output writing complete")
        except Exception as e:
            logger.error(f"Failed to write output: {e}")
            sys.exit(1)
    elif dry_run:
        logger.info("Dry run - no output written")

    logger.info("Climate Tool finished successfully")


if __name__ == "__main__":
    main()
