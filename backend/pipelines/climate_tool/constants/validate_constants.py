#!/usr/bin/env python3
"""
Validation script for climate tool constants.

This script checks that:
1. JSON files are valid and loadable
2. Required fields are present
3. Values are within expected ranges
4. References are documented
"""

import json
from pathlib import Path


def validate_gwp_factors():
    """Validate gwp_factors.json structure and values."""
    print("Validating gwp_factors.json...")

    with open(Path(__file__).parent / "gwp_factors.json") as f:
        data = json.load(f)

    # Check required top-level keys
    required_keys = ["metadata", "gwp_100", "molecular_weights", "indirect_n2o_factors"]
    for key in required_keys:
        assert key in data, f"Missing required key: {key}"

    # Check GWP values
    assert data["gwp_100"]["CO2"] == 1, "CO2 GWP should be 1"
    assert data["gwp_100"]["CH4"] > 0, "CH4 GWP should be positive"
    assert data["gwp_100"]["N2O"] > 0, "N2O GWP should be positive"
    assert data["gwp_100"]["CH4"] == 28, "CH4 GWP-100 (AR5) should be 28"
    assert data["gwp_100"]["N2O"] == 265, "N2O GWP-100 (AR5) should be 265"

    # Check molecular weights
    mw = data["molecular_weights"]
    assert mw["N2O"] == 44.0, "N2O molecular weight should be 44"
    assert mw["N2"] == 28.0, "N2 molecular weight should be 28"
    assert abs(mw["N2O_N_factor"] - 44.0 / 28.0) < 0.001, "N2O_N_factor should be 44/28"

    # Check indirect N2O factors
    assert 0 < data["indirect_n2o_factors"]["atmospheric_deposition"] < 1, (
        "Atmospheric deposition EF should be between 0 and 1"
    )
    assert 0 < data["indirect_n2o_factors"]["leaching_runoff"] < 1, "Leaching/runoff EF should be between 0 and 1"

    # Check metadata
    assert "source" in data["metadata"], "Missing source in metadata"
    assert "IPCC" in data["metadata"]["source"], "Should reference IPCC"

    print("✓ gwp_factors.json is valid")


def validate_emission_factors():
    """Validate emission_factors.json structure and values."""
    print("Validating emission_factors.json...")

    with open(Path(__file__).parent / "emission_factors.json") as f:
        data = json.load(f)

    # Check required top-level keys
    required_keys = ["metadata", "manure_storage", "housing_emissions", "field_application", "grazing", "crop_residues"]
    for key in required_keys:
        assert key in data, f"Missing required key: {key}"

    # Check manure storage MCF values are reasonable
    mcf_gylle = data["manure_storage"]["mcf"]["gylle"]["value"]
    assert 0 < mcf_gylle < 100, f"MCF for slurry should be between 0-100%, got {mcf_gylle}"

    # Check B0 factors are reasonable
    b0_dairy = data["manure_storage"]["b0_factors"]["malkekøer"]["value"]
    assert 0 < b0_dairy < 1, f"B0 for dairy should be between 0-1, got {b0_dairy}"

    # Check NH3 emission factors are reasonable (should be small fractions)
    nh3_grazing = data["grazing"]["nh3"]["kvæg"]["value"]
    assert 0 < nh3_grazing < 1, f"NH3 EF should be between 0-1, got {nh3_grazing}"

    # Check all entries have required fields
    def check_entry_fields(entry, name):
        if isinstance(entry, dict) and "value" in entry:
            assert "unit" in entry, f"{name} missing 'unit' field"
            assert "description" in entry, f"{name} missing 'description' field"
            # Note: reference is optional for some derived values

    # Sample check on housing emissions
    for stald_type, value in data["housing_emissions"]["n2o"].items():
        check_entry_fields(value, f"housing_emissions.n2o.{stald_type}")

    # Check metadata
    assert "source" in data["metadata"], "Missing source in metadata"
    assert "Danish" in data["metadata"]["source"], "Should reference Danish Climate Tool"

    print("✓ emission_factors.json is valid")


def validate_loader_functions():
    """Validate that loader functions work correctly."""
    print("Validating loader functions...")

    from loader import get_gwp, get_mcf, get_b0_factor, get_molecular_weight_factor

    # Test GWP lookup
    assert get_gwp("CH4") == 28, "CH4 GWP lookup failed"
    assert get_gwp("N2O") == 265, "N2O GWP lookup failed"

    # Test molecular weight lookup
    n2o_factor = get_molecular_weight_factor("N2O_N_factor")
    assert abs(n2o_factor - 1.5714) < 0.001, "N2O_N_factor lookup failed"

    # Test MCF lookup
    mcf_gylle = get_mcf("gylle")
    assert mcf_gylle["value"] == 12.4, "MCF slurry lookup failed"
    assert "reference" in mcf_gylle, "MCF missing reference"

    # Test B0 lookup
    b0_dairy = get_b0_factor("malkekøer")
    assert b0_dairy["value"] == 0.24, "B0 dairy cows lookup failed"

    print("✓ Loader functions work correctly")


def validate_conversions_module():
    """Validate that conversions module loads constants correctly."""
    print("Validating conversions module...")

    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))

    from utils.conversions import CH4_GWP, N2O_GWP, N2O_N_FACTOR, ch4_to_co2e, n2o_to_co2e

    # Check constants are loaded
    assert CH4_GWP == 28, "CH4_GWP not loaded correctly"
    assert N2O_GWP == 265, "N2O_GWP not loaded correctly"
    assert abs(N2O_N_FACTOR - 1.5714) < 0.001, "N2O_N_FACTOR not loaded correctly"

    # Check conversion functions
    assert ch4_to_co2e(100) == 2800, "CH4 to CO2e conversion failed"
    assert n2o_to_co2e(10) == 2650, "N2O to CO2e conversion failed"

    print("✓ Conversions module integration works")


def main():
    """Run all validation checks."""
    print("=" * 60)
    print("Climate Tool Constants Validation")
    print("=" * 60)
    print()

    try:
        validate_gwp_factors()
        validate_emission_factors()
        validate_loader_functions()
        validate_conversions_module()

        print()
        print("=" * 60)
        print("✓ ALL VALIDATIONS PASSED")
        print("=" * 60)
        return 0

    except AssertionError as e:
        print()
        print("=" * 60)
        print(f"✗ VALIDATION FAILED: {e}")
        print("=" * 60)
        return 1
    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ ERROR: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    exit(main())
