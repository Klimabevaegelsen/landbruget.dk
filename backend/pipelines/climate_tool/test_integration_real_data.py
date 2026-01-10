"""
Integration test using REAL GCS data structures.

This test attempts to load actual data and run calculations to identify
all schema mismatches and implementation gaps.
"""

import sys
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "unified_pipeline" / "src"))

from data_loader import ClimateDataLoader
from climate_calculator import FarmClimateCalculator


def test_data_loader_with_real_schema():
    """Test that data loader can handle real GCS schemas."""
    print("\n" + "=" * 80)
    print("INTEGRATION TEST: Real GCS Data Schema")
    print("=" * 80)

    loader = ClimateDataLoader()

    # Use a sample CVR from the gr 2023 data (we know these exist from exploration)
    test_cvr = "31373077"  # Pick one from exploration
    test_year = 2023

    print(f"\n1. Testing livestock data load for CVR {test_cvr}, year {test_year}")
    print("-" * 80)

    livestock = loader.load_livestock(cvr=test_cvr, year=test_year)

    print(f"Livestock DataFrame shape: {livestock.shape}")
    if not livestock.empty:
        print(f"Columns: {livestock.columns.tolist()}")
        print(f"\nFirst row sample:")
        print(livestock.head(1).T)

        # Check for Green Accounts expected columns
        expected_cols = ["cvr_number", "c_2001", "c_2004", "c_2006", "c_2016"]
        missing = [col for col in expected_cols if col not in livestock.columns]
        if missing:
            print(f"\n❌ MISSING EXPECTED COLUMNS: {missing}")
        else:
            print(f"\n✅ All expected Green Accounts columns present")

            # Show species distribution
            if "c_2001" in livestock.columns:
                print(f"\nSpecies distribution (c_2001):")
                print(livestock["c_2001"].value_counts().head())
    else:
        print("⚠️  No livestock data found (empty DataFrame)")

    print(f"\n2. Testing field data load for CVR {test_cvr}, year {test_year}")
    print("-" * 80)

    fields = loader.load_fields(cvr=test_cvr, year=test_year)

    print(f"Fields DataFrame shape: {fields.shape}")
    if not fields.empty:
        print(f"Columns: {fields.columns.tolist()}")
        print(f"\nFirst row sample:")
        print(fields.head(1).T)
    else:
        print("⚠️  No field data found (empty DataFrame)")

    print(f"\n3. Testing fertilizer data load for CVR {test_cvr}, year {test_year}")
    print("-" * 80)

    fertilizer = loader.load_fertilizer(cvr=test_cvr, year=test_year)

    print(f"Fertilizer DataFrame shape: {fertilizer.shape}")
    if not fertilizer.empty:
        print(f"Columns: {fertilizer.columns.tolist()}")
        print(f"\nFirst row sample:")
        print(fertilizer.head(1).T)

        # Check for GKEA expected columns
        expected_cols = ["cvr_number", "total_n_kvote", "faktisk_areal_ha", "year"]
        missing = [col for col in expected_cols if col not in fertilizer.columns]
        if missing:
            print(f"\n❌ MISSING EXPECTED COLUMNS: {missing}")
        else:
            print(f"\n✅ All expected GKEA columns present")

            if "total_n_kvote" in fertilizer.columns:
                total_n = fertilizer["total_n_kvote"].sum()
                print(f"\nTotal N quota for this CVR: {total_n:,.0f} kg N")
                # Calculate potential N2O emissions
                n2o_kg = total_n * 0.01 * (44 / 28)  # IPCC Tier 1
                co2e_kg = n2o_kg * 298  # GWP AR4 (or 265 for AR5)
                print(f"Estimated N2O emissions: {n2o_kg:,.1f} kg N2O")
                print(f"Estimated CO2e from N2O: {co2e_kg:,.0f} kg CO2e")
    else:
        print("⚠️  No fertilizer data found (empty DataFrame)")

    print("\n" + "=" * 80)
    print("SCHEMA COMPATIBILITY ANALYSIS")
    print("=" * 80)

    # Check if calculator can work with this data
    print("\n4. Testing calculator with real data")
    print("-" * 80)

    print("\n⚠️  KNOWN ISSUES:")
    print("1. climate_calculator.py expects livestock_data['cattle'] but we have DataFrame")
    print("2. Need to map Green Accounts columns (c_2001, c_2006) to calculator inputs")
    print("3. Formula modules expect specific field names that don't match GCS")
    print("4. Need transformation layer: GCS schema → calculator schema")

    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("""
1. CREATE DATA TRANSFORMER:
   - Transform Green Accounts (c_2001, c_2006, c_2016) → livestock structure expected by formulas
   - Map species codes to animal types (Kvæg → Cattle, Svin → Pigs)
   - Extract animal counts, housing systems, N production

2. FIX CLIMATE_CALCULATOR:
   - Accept DataFrames directly, not dict['cattle']
   - Add preprocessing to match formula input requirements
   - Handle missing data categories gracefully

3. UPDATE FORMULA MODULES:
   - Document exact input field requirements
   - Add validation for expected columns
   - Provide clear error messages for schema mismatches

4. ADD SCHEMA TESTS:
   - Test each formula module with real GCS data
   - Validate transformations are correct
   - Check emission factors are loaded properly
    """)


if __name__ == "__main__":
    try:
        test_data_loader_with_real_schema()
    except Exception as e:
        print(f"\n❌ TEST FAILED WITH ERROR:")
        print(f"{type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
