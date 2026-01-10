"""
Validation script for data_transformer.py

Demonstrates the transformers work correctly with mock data that matches
the actual GCS schema structure.
"""

import pandas as pd
import sys
from pathlib import Path

# Add climate_tool to path
climate_tool_path = Path(__file__).parent
if str(climate_tool_path) not in sys.path:
    sys.path.insert(0, str(climate_tool_path))

from data_transformer import (
    GreenAccountsTransformer,
    GKEATransformer,
    FVMTransformer,
    IntegratedFarmTransformer,
)


def validate_green_accounts_transformer():
    """Validate livestock data transformation."""
    print("\n" + "=" * 60)
    print("1. Testing GreenAccountsTransformer (Livestock Data)")
    print("=" * 60)

    # Mock data matching actual GCS schema from Green Accounts
    df = pd.DataFrame(
        {
            "cvr_number": ["31373077"] * 5,
            "c_2001": ["Kvæg", "Kvæg", "Kvæg", "Svin", "Høns"],  # Species
            "c_2004": ["Malkekøer", "Kvier", "Kalve", "Søer", "Hønniker"],  # Type detail
            "c_2006": [120, 25, 18, 250, 8000],  # Animal count
            "c_2016": [14400.0, 1000.0, 360.0, 5000.0, 1600.0],  # N production kg
            "c_2005": ["Løsdrift", "Løsdrift", "Stald", "Delvist spalter", "Strøelse"],
        }
    )

    print("\nInput DataFrame (Danish schema):")
    print(df[["c_2001", "c_2004", "c_2006", "c_2016"]].to_string())

    result = GreenAccountsTransformer.transform(df)

    print("\n✓ Transformation successful!")
    print(f"✓ Found {len(result)} species:")

    for species, summary in result.items():
        print(f"\n  {species.upper()}:")
        print(f"    Total animals: {summary.total_count}")
        print(f"    N production: {summary.total_n_production_kg:.1f} kg")
        print(f"    Subtypes: {summary.subtypes}")
        if summary.housing_systems:
            print(f"    Housing: {list(summary.housing_systems.keys())}")

    # Validate results
    assert "cattle" in result, "Missing cattle species"
    assert result["cattle"].total_count == 163, f"Wrong cattle count: {result['cattle'].total_count}"
    assert "pigs" in result, "Missing pigs species"
    assert result["pigs"].total_count == 250, f"Wrong pig count: {result['pigs'].total_count}"
    assert "poultry" in result, "Missing poultry species"
    assert result["poultry"].total_count == 8000, f"Wrong poultry count: {result['poultry'].total_count}"

    print("\n✓ All assertions passed!")


def validate_gkea_transformer():
    """Validate fertilizer data transformation."""
    print("\n" + "=" * 60)
    print("2. Testing GKEATransformer (Fertilizer Data)")
    print("=" * 60)

    # Mock data matching actual GKEA schema
    df = pd.DataFrame(
        {
            "cvr_number": ["31373077"] * 4,
            "total_n_kvote": [2400.0, 3200.0, 1800.0, 2100.0],  # Total N applied
            "faktisk_areal_ha": [20.0, 25.0, 15.0, 18.0],  # Actual field area
            "marknummer": ["M001", "M002", "M003", "M004"],
            "year": [2024, 2024, 2024, 2024],
        }
    )

    print("\nInput DataFrame (GKEA schema):")
    print(df[["marknummer", "total_n_kvote", "faktisk_areal_ha"]].to_string())

    result = GKEATransformer.transform(df)

    print("\n✓ Transformation successful!")
    print(f"  Total N applied: {result.total_n_kg:.1f} kg")
    print(f"  Total area: {result.total_area_ha:.1f} ha")
    print(f"  Average N/ha: {result.avg_n_kg_per_ha:.1f} kg/ha")
    print(f"  Number of fields: {result.field_count}")

    # Calculate N2O emissions
    n2o_co2e = GKEATransformer.calculate_n2o_emissions(result)
    print(f"\n  N2O emissions: {n2o_co2e:.1f} kg CO2e")

    # Validate results
    assert result.total_n_kg == 9500.0, f"Wrong total N: {result.total_n_kg}"
    assert result.total_area_ha == 78.0, f"Wrong total area: {result.total_area_ha}"
    assert result.field_count == 4, f"Wrong field count: {result.field_count}"

    # Validate N2O calculation: 9500 * 0.01 * (44/28) * 298
    expected_n2o = 9500.0 * 0.01 * (44 / 28) * 298
    assert abs(n2o_co2e - expected_n2o) < 0.1, f"Wrong N2O: {n2o_co2e} vs {expected_n2o}"

    print("\n✓ All assertions passed!")


def validate_fvm_transformer():
    """Validate field data transformation."""
    print("\n" + "=" * 60)
    print("3. Testing FVMTransformer (Field Data)")
    print("=" * 60)

    # Mock data matching actual FVM schema
    df = pd.DataFrame(
        {
            "cvr": ["31373077"] * 6,
            "bfe_nummer": ["BFE001", "BFE002", "BFE003", "BFE004", "BFE005", "BFE006"],
            "afgroede": ["Vinterhvede", "Vinterhvede", "Vårbyg", "Græs", "Græs", "Majs"],
            "areal_ha": [22.5, 18.3, 15.0, 12.5, 10.0, 8.5],
        }
    )

    print("\nInput DataFrame (FVM schema):")
    print(df[["bfe_nummer", "afgroede", "areal_ha"]].to_string())

    result = FVMTransformer.transform(df)

    print("\n✓ Transformation successful!")
    print(f"✓ Found {len(result)} crop types:")

    total_area = 0
    for field in result:
        print(f"\n  {field.crop_type}: {field.total_area_ha:.1f} ha ({field.field_count} fields)")
        total_area += field.total_area_ha

    print(f"\n  Total area: {total_area:.1f} ha")

    # Validate results
    assert len(result) == 4, f"Wrong number of crop types: {len(result)}"

    # Find winter wheat
    winter_wheat = next((f for f in result if f.crop_type == "winter_wheat"), None)
    assert winter_wheat is not None, "Missing winter_wheat"
    assert winter_wheat.total_area_ha == 40.8, f"Wrong wheat area: {winter_wheat.total_area_ha}"
    assert winter_wheat.field_count == 2, f"Wrong wheat field count: {winter_wheat.field_count}"

    # Find grass
    grass = next((f for f in result if f.crop_type == "grass"), None)
    assert grass is not None, "Missing grass"
    assert grass.total_area_ha == 22.5, f"Wrong grass area: {grass.total_area_ha}"

    print("\n✓ All assertions passed!")


def validate_integrated_transformer():
    """Validate integrated transformation."""
    print("\n" + "=" * 60)
    print("4. Testing IntegratedFarmTransformer (Complete Farm)")
    print("=" * 60)

    # Create mock data for all sources
    livestock_df = pd.DataFrame(
        {
            "cvr_number": ["31373077"] * 3,
            "c_2001": ["Kvæg", "Kvæg", "Kvæg"],
            "c_2004": ["Malkekøer", "Kvier", "Kalve"],
            "c_2006": [120, 25, 18],
            "c_2016": [14400.0, 1000.0, 360.0],
        }
    )

    field_df = pd.DataFrame(
        {
            "cvr": ["31373077"] * 3,
            "bfe_nummer": ["BFE001", "BFE002", "BFE003"],
            "afgroede": ["Vinterhvede", "Vårbyg", "Græs"],
            "areal_ha": [25.0, 18.0, 15.0],
        }
    )

    fertilizer_df = pd.DataFrame(
        {
            "cvr_number": ["31373077"] * 3,
            "total_n_kvote": [3000.0, 2160.0, 1800.0],
            "faktisk_areal_ha": [25.0, 18.0, 15.0],
        }
    )

    result = IntegratedFarmTransformer.transform_all(livestock_df, field_df, fertilizer_df)

    print("\n✓ Integration successful!")
    print("\nFarm Metadata:")
    for key, value in result["metadata"].items():
        print(f"  {key}: {value}")

    # Validate metadata
    assert result["metadata"]["has_livestock"] is True
    assert result["metadata"]["has_fields"] is True
    assert result["metadata"]["has_fertilizer"] is True
    assert result["metadata"]["total_area_ha"] == 58.0

    # Convert to FarmData object
    farm_data = IntegratedFarmTransformer.to_farm_data_object(result)

    print("\n✓ Converted to FarmData object:")
    print(f"  Animal counts: {dict(list(farm_data.animal_counts.items())[:3])}")
    print(f"  Field areas: {farm_data.field_areas}")

    # Validate FarmData
    assert "cattle_dairy_cows" in farm_data.animal_counts
    assert farm_data.animal_counts["cattle_dairy_cows"] == 120
    assert "winter_wheat" in farm_data.field_areas
    assert farm_data.field_areas["winter_wheat"] == 25.0

    print("\n✓ All assertions passed!")


def main():
    """Run all validation tests."""
    print("\n" + "=" * 60)
    print("DATA TRANSFORMER VALIDATION")
    print("=" * 60)
    print("\nValidating transformation from Danish GCS schema to English calculator schema...")

    try:
        validate_green_accounts_transformer()
        validate_gkea_transformer()
        validate_fvm_transformer()
        validate_integrated_transformer()

        print("\n" + "=" * 60)
        print("✓ ALL VALIDATIONS PASSED!")
        print("=" * 60)
        print("\nThe data transformer is working correctly and ready to use.")
        print("It successfully bridges the Danish GCS schema to the calculator input schema.\n")

    except AssertionError as e:
        print(f"\n✗ VALIDATION FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
