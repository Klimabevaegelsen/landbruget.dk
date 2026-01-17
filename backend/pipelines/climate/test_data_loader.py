"""
Test script for ClimateDataLoader

This script demonstrates how to use the ClimateDataLoader to fetch data
from GCS silver/gold layers for climate calculations.

Usage:
    python test_data_loader.py [--cvr CVR_NUMBER] [--year YEAR]
"""

import argparse

from data_loader import ClimateDataLoader


def test_loader(cvr: str = "31373077", year: int = 2024):
    """
    Test the ClimateDataLoader with a specific CVR and year.

    Args:
        cvr: Company CVR number (8 digits)
        year: Agricultural year
    """
    print(f"\n{'=' * 60}")
    print(f"Testing ClimateDataLoader for CVR {cvr}, Year {year}")
    print(f"{'=' * 60}\n")

    # Initialize loader
    loader = ClimateDataLoader()

    # Test 1: List available years for FVM marker data
    print("\n[TEST 1] Available FVM marker years:")
    print("-" * 40)
    available_years = loader.list_available_years("fvm_marker")
    print(f"Found years: {available_years}")

    # Test 2: Load livestock data
    print("\n[TEST 2] Loading livestock data:")
    print("-" * 40)
    livestock_rel = loader.load_livestock(cvr=cvr, year=year)
    if livestock_rel is not None:
        livestock_df = livestock_rel.df()
        print(f"✅ Loaded {len(livestock_df)} livestock records")
        print(f"Columns: {livestock_df.columns.tolist()}")
        print("\nFirst few rows:")
        print(livestock_df.head())
    else:
        print("⚠️  No livestock data found")

    # Test 3: Load field data
    print("\n[TEST 3] Loading field data:")
    print("-" * 40)
    fields_rel = loader.load_fields(cvr=cvr, year=year)
    if fields_rel is not None:
        fields_df = fields_rel.df()
        print(f"✅ Loaded {len(fields_df)} field records")
        print(f"Columns: {fields_df.columns.tolist()}")
        print("\nFirst few rows:")
        print(fields_df.head())

        # Summary statistics
        if "areal_ha" in fields_df.columns or "areal" in fields_df.columns:
            area_col = "areal_ha" if "areal_ha" in fields_df.columns else "areal"
            total_area = fields_df[area_col].sum()
            print(f"\nTotal field area: {total_area:.2f} ha")
    else:
        print("⚠️  No field data found")

    # Test 4: Load fertilizer data
    print("\n[TEST 4] Loading fertilizer data:")
    print("-" * 40)
    fertilizer_rel = loader.load_fertilizer(cvr=cvr, year=year)
    if fertilizer_rel is not None:
        fertilizer_df = fertilizer_rel.df()
        print(f"✅ Loaded {len(fertilizer_df)} fertilizer records")
        print(f"Columns: {fertilizer_df.columns.tolist()}")
        print("\nFirst few rows:")
        print(fertilizer_df.head())
    else:
        print("⚠️  No fertilizer data found (may not be available)")

    # Test 5: Load climate data (optional)
    print("\n[TEST 5] Loading climate data (optional):")
    print("-" * 40)
    climate_rel = loader.load_climate_data(cvr=cvr, year=year)
    if climate_rel is not None:
        climate_df = climate_rel.df()
        print(f"✅ Loaded {len(climate_df)} climate records")
        print(f"Columns: {climate_df.columns.tolist()}")
    else:
        print("ℹ️  Climate data not available (optional)")

    # Test 6: Get latest data timestamps
    print("\n[TEST 6] Latest data timestamps:")
    print("-" * 40)
    chr_timestamp = loader.get_latest_data_timestamp("chr")
    fvm_timestamp = loader.get_latest_data_timestamp(f"fvm_marker_{year}")
    print(f"CHR latest: {chr_timestamp}")
    print(f"FVM {year} latest: {fvm_timestamp}")

    print(f"\n{'=' * 60}")
    print("Testing complete!")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="Test ClimateDataLoader with sample data")
    parser.add_argument(
        "--cvr",
        type=str,
        default="31373077",
        help="CVR number to test (8 digits)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to test",
    )

    args = parser.parse_args()

    # Validate CVR format
    cvr = str(args.cvr).zfill(8)
    if len(cvr) != 8 or not cvr.isdigit():
        print(f"Error: Invalid CVR format: {args.cvr}. Must be 8 digits.")
        return 1

    test_loader(cvr=cvr, year=args.year)
    return 0


if __name__ == "__main__":
    exit(main())
