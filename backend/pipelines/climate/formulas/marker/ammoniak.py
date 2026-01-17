"""
Field NH3 (ammonia) emissions from synthetic fertilizer application.

Based on:
- KB_21_5397_AP2: Notat beregningsgrundlag (SEGES 2021)
- Table 22 (Side 80): NH3 emission factors for synthetic fertilizers (2015-2017)
- Includes volatilization emissions from field application of mineral fertilizers

IMPORTANT: NH3 emissions are NOT converted to CO2e. NH3 contributes to acidification
and eutrophication but is not a direct greenhouse gas. It is reported separately
in kg NH3 for environmental footprint analysis.

Source Attribution:
- Emission factors: KB_21_5397_AP2 Table 22 (SEGES 2021)
- Methodology: Danish GHG inventory aligned with IPCC guidelines
"""

import json
from pathlib import Path


def load_nh3_factors() -> dict[str, dict]:
    """
    Load NH3 emission factors for synthetic fertilizer application.

    Returns NH3 emission factors in kg NH3-N per kg N applied.

    Source: KB_21_5397_AP2 Table 22 (SEGES 2021)
    """
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(base_path / "nh3_factors_field_application.json") as f:
        return json.load(f)



# Load constants once at module import
NH3_FACTORS_DATA = load_nh3_factors()
NH3_FERTILIZER_TYPES = NH3_FACTORS_DATA["fertilizer_types"]
NH3_DEFAULT_FACTOR = NH3_FACTORS_DATA["default_factor"]["ef_nh3_n_per_kg_n"]
NH3_N_TO_NH3_CONVERSION = NH3_FACTORS_DATA["conversion"]["nh3_n_to_nh3"]


def calculate_nh3_field_fertilizer(
    n_applied_kg: float,
    fertilizer_type: str | None = None,
) -> dict[str, float]:
    """
    Calculate NH3 emissions from synthetic fertilizer application to fields.

    Formula:
        NH3-N (kg) = N_applied × EF_nh3_n
        NH3 (kg) = NH3-N × 1.214 (conversion from NH3-N to NH3)

    Where:
        - N_applied: Total nitrogen applied in synthetic fertilizer (kg N)
        - EF_nh3_n: NH3-N emission factor (kg NH3-N per kg N applied)
        - 1.214: Conversion factor from NH3-N to NH3 (17/14)

    Args:
        n_applied_kg: Total nitrogen applied in synthetic fertilizer (kg N)
        fertilizer_type: Type of fertilizer (optional). If None, uses default weighted average.
                        Options: 'calcium_ammonium_nitrat', 'npk_gødninger', 'urea', etc.

    Returns:
        Dict with:
            - nh3_kg: Total NH3 emissions (kg NH3)
            - nh3_n_kg: NH3-N emissions (kg NH3-N)
            - n_applied_kg: Nitrogen applied (kg N)
            - emission_factor_nh3_n: Applied EF in kg NH3-N/kg N
            - emission_factor_nh3: Applied EF in kg NH3/kg N
            - fertilizer_type: Fertilizer type used (or 'default')
            - source: Source attribution

    IMPORTANT: Returns NH3 in kg NH3 (NOT CO2e). NH3 is not a greenhouse gas
    but contributes to acidification and eutrophication.

    Source: KB_21_5397_AP2 Table 22 (SEGES 2021)

    Example:
        >>> # 5000 kg N applied, calcium ammonium nitrate (most common in Denmark)
        >>> result = calculate_nh3_field_fertilizer(5000, 'calcium_ammonium_nitrat')
        >>> print(f"NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
        NH3 emissions: 48.56 kg NH3
    """
    if n_applied_kg <= 0:
        return {
            "nh3_kg": 0.0,
            "nh3_n_kg": 0.0,
            "n_applied_kg": 0.0,
            "emission_factor_nh3_n": 0.0,
            "emission_factor_nh3": 0.0,
            "fertilizer_type": "none",
            "source": "N/A",
        }

    # Get emission factor based on fertilizer type
    if fertilizer_type is None or fertilizer_type not in NH3_FERTILIZER_TYPES:
        # Use default weighted average factor
        ef_nh3_n = NH3_DEFAULT_FACTOR
        fert_type_name = "default (weighted average)"
        fert_description = NH3_FACTORS_DATA["default_factor"]["description"]
    else:
        # Use specific fertilizer type factor
        fert_data = NH3_FERTILIZER_TYPES[fertilizer_type]
        ef_nh3_n = fert_data["ef_nh3_n_per_kg_n"]
        fert_type_name = fertilizer_type
        fert_description = fert_data["description"]

    # Calculate NH3-N emissions
    nh3_n_kg = n_applied_kg * ef_nh3_n

    # Convert to NH3 (multiply by 17/14 = 1.214)
    nh3_kg = nh3_n_kg * NH3_N_TO_NH3_CONVERSION

    # Calculate emission factor in kg NH3/kg N for reference
    ef_nh3 = ef_nh3_n * NH3_N_TO_NH3_CONVERSION

    return {
        "nh3_kg": nh3_kg,
        "nh3_n_kg": nh3_n_kg,
        "n_applied_kg": n_applied_kg,
        "emission_factor_nh3_n": ef_nh3_n,
        "emission_factor_nh3": ef_nh3,
        "fertilizer_type": fert_type_name,
        "fertilizer_description": fert_description,
        "source": NH3_FACTORS_DATA["metadata"]["source"],
        "unit": "kg NH3",
    }


def calculate_nh3_field_per_ha(
    n_kg_per_ha: float,
    area_ha: float,
    fertilizer_type: str | None = None,
) -> dict[str, float]:
    """
    Calculate NH3 emissions from synthetic fertilizer per hectare basis.

    Convenience function that calculates total N applied and calls calculate_nh3_field_fertilizer.

    Args:
        n_kg_per_ha: Nitrogen application rate (kg N per ha)
        area_ha: Total area fertilized (ha)
        fertilizer_type: Type of fertilizer (optional, see calculate_nh3_field_fertilizer)

    Returns:
        Dict with emission results (see calculate_nh3_field_fertilizer)
        Plus:
            - n_kg_per_ha: Application rate
            - area_ha: Area fertilized

    Example:
        >>> # 150 kg N/ha on 50 ha, default fertilizer type
        >>> result = calculate_nh3_field_per_ha(150, 50)
        >>> print(f"NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
        NH3 emissions: 236.73 kg NH3
    """
    if n_kg_per_ha <= 0 or area_ha <= 0:
        return {
            "nh3_kg": 0.0,
            "nh3_n_kg": 0.0,
            "n_applied_kg": 0.0,
            "n_kg_per_ha": n_kg_per_ha,
            "area_ha": area_ha,
            "emission_factor_nh3_n": 0.0,
            "emission_factor_nh3": 0.0,
            "fertilizer_type": "none",
            "source": "N/A",
        }

    # Calculate total N applied
    n_total_kg = n_kg_per_ha * area_ha

    # Calculate emissions
    result = calculate_nh3_field_fertilizer(n_total_kg, fertilizer_type)

    # Add per-ha information
    result["n_kg_per_ha"] = n_kg_per_ha
    result["area_ha"] = area_ha
    result["nh3_kg_per_ha"] = result["nh3_kg"] / area_ha if area_ha > 0 else 0.0

    return result


def get_available_fertilizer_types() -> dict[str, str]:
    """
    Get dictionary of available fertilizer types and their descriptions.

    Returns:
        Dict mapping fertilizer type key to description

    Example:
        >>> types = get_available_fertilizer_types()
        >>> print(types['calcium_ammonium_nitrat'])
        Calcium ammonium nitrate (most common fertilizer in Denmark)
    """
    return {key: data["description"] for key, data in NH3_FERTILIZER_TYPES.items()}


if __name__ == "__main__":
    # Test with example farm data
    print("=== Field NH3 Emissions from Synthetic Fertilizer Test ===\n")
    print("Source: KB_21_5397_AP2 Table 22 (SEGES 2021)")
    print("Note: NH3 is NOT converted to CO2e - reported separately in kg NH3\n")

    # Test 1: Default fertilizer type (weighted average)
    print("Test 1: Default fertilizer type (weighted average)")
    result = calculate_nh3_field_fertilizer(5000)
    print(f"  N applied: {result['n_applied_kg']:.0f} kg N")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  NH3-N emissions: {result['nh3_n_kg']:.2f} kg NH3-N")
    print(f"  Emission factor: {result['emission_factor_nh3']:.4f} kg NH3/kg N")
    print(f"  Fertilizer type: {result['fertilizer_type']}")
    print()

    # Test 2: Calcium ammonium nitrate (most common in Denmark)
    print("Test 2: Calcium ammonium nitrate (most common in Denmark)")
    result = calculate_nh3_field_fertilizer(5000, "calcium_ammonium_nitrat")
    print(f"  N applied: {result['n_applied_kg']:.0f} kg N")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  Emission factor: {result['emission_factor_nh3']:.4f} kg NH3/kg N")
    print(f"  Description: {result['fertilizer_description']}")
    print()

    # Test 3: Urea (highest emission factor)
    print("Test 3: Urea (highest NH3 emission factor)")
    result = calculate_nh3_field_fertilizer(5000, "urea")
    print(f"  N applied: {result['n_applied_kg']:.0f} kg N")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  Emission factor: {result['emission_factor_nh3']:.4f} kg NH3/kg N")
    print(f"  Description: {result['fertilizer_description']}")
    print()

    # Test 4: Per hectare calculation
    print("Test 4: Per hectare calculation (150 kg N/ha on 50 ha)")
    result = calculate_nh3_field_per_ha(150, 50)
    print(f"  Application rate: {result['n_kg_per_ha']:.0f} kg N/ha")
    print(f"  Area: {result['area_ha']:.0f} ha")
    print(f"  Total N applied: {result['n_applied_kg']:.0f} kg N")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  NH3 per hectare: {result['nh3_kg_per_ha']:.2f} kg NH3/ha")
    print()

    # Test 5: Comparison of fertilizer types
    print("Test 5: Comparison of NH3 emissions for 1000 kg N applied")
    print("  Fertilizer Type                              | NH3 (kg) | EF (kg NH3/kg N)")
    print("  " + "-" * 80)
    test_types = [
        ("calcium_ammonium_nitrat", "Calcium ammonium nitrate (lowest)"),
        ("npk_gødninger", "NPK compound fertilizers"),
        ("default", "Default weighted average"),
        ("ammoniumsulfat", "Ammonium sulfate"),
        ("urea", "Urea (highest)"),
    ]
    for fert_type, label in test_types:
        if fert_type == "default":
            result = calculate_nh3_field_fertilizer(1000)
        else:
            result = calculate_nh3_field_fertilizer(1000, fert_type)
        print(f"  {label:42} | {result['nh3_kg']:8.2f} | {result['emission_factor_nh3']:6.4f}")
    print()

    print("Environmental Impact:")
    print("  NH3 from synthetic fertilizer contributes to:")
    print("  - Acidification of soils and water")
    print("  - Eutrophication of aquatic ecosystems")
    print("  - Formation of particulate matter (PM2.5)")
    print("  - Human health impacts (air quality)")
    print()
    print("  NH3 does NOT contribute directly to climate change (not a GHG)")
    print("  Reported separately for environmental footprint analysis")
    print()
    print("  Field NH3 is SEPARATE from livestock manure NH3 emissions")
    print("  Both should be tracked independently for complete farm footprint")
