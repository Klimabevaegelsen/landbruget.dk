"""
Cattle NH3 (ammonia) emissions from manure management.

Based on:
- SR671: Forbedret grundlag for opgørelser af landbrugets emissioner (Aarhus University DCE, 2025)
- National emissions inventory data (Gennemsnitlig_emission_sub.csv, 2023 values)
- Includes emissions from housing, storage, and field application

IMPORTANT: NH3 emissions are NOT converted to CO2e. NH3 contributes to acidification
and eutrophication but is not a direct greenhouse gas. It is reported separately
in kg NH3 for environmental footprint analysis.

Source Attribution:
- Emission factors: SR671 2023 national data (DCE Aarhus University, 2025)
- Methodology: Danish GHG inventory aligned with IPCC guidelines
"""

import json
from pathlib import Path


def load_nh3_factors() -> dict[str, dict[str, float]]:
    """
    Load NH3 emission factors from SR671 2023 national data.

    Returns NH3 emission factors in kg NH3 per animal.

    Source: SR671 (Aarhus University DCE, 2025)
    """
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(base_path / "nh3_factors_sr671_2023.json") as f:
        data = json.load(f)

    # Extract cattle factors
    return dict(data["data"]["kvæg"].items())


# Load constants once at module import
NH3_FACTORS = load_nh3_factors()


def calculate_nh3_emissions_kvaeg(
    cattle_type: str,
    antal_dyr: float,
) -> dict[str, float]:
    """
    Calculate NH3 emissions from cattle manure management.

    Formula:
        NH3 (kg) = antal_dyr × EF_nh3

    Where:
        - antal_dyr: Number of animals
        - EF_nh3: NH3 emission factor (kg NH3 per animal)

    Args:
        cattle_type: Type of cattle ('malkekøer', 'ammekøer', 'opdræt_0_6_mdr',
                     'opdræt_6_mdr_kælvning', 'tyre_0_6_mdr', 'tyre_6_mdr_slagtning')
        antal_dyr: Number of animals

    Returns:
        Dict with:
            - nh3_kg: Total NH3 emissions (kg NH3/year)
            - nh3_per_animal: Emissions per animal (kg NH3)
            - emission_factor: Applied emission factor (kg NH3/animal)
            - production_type: Type (årsdyr or produceret_dyr)
            - source: Source attribution

    IMPORTANT: Returns NH3 in kg NH3 (NOT CO2e). NH3 is not a greenhouse gas
    but contributes to acidification and eutrophication.

    Source: SR671 2023 national data (DCE Aarhus University, 2025)

    Example:
        >>> # 100 dairy cows (årsdyr)
        >>> result = calculate_nh3_emissions_kvaeg('malkekøer', 100)
        >>> print(f"NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
        NH3 emissions: 1328.00 kg NH3
    """
    if antal_dyr <= 0:
        return {
            "nh3_kg": 0.0,
            "nh3_per_animal": 0.0,
            "emission_factor": 0.0,
            "production_type": "unknown",
            "source": "N/A",
        }

    # Normalize cattle type
    cattle_type_clean = cattle_type.lower().strip().replace(" ", "_")

    # Map to standard names
    type_mapping = {
        "malkekøer": "malkekøer",
        "malkek�er": "malkekøer",
        "dairy": "malkekøer",
        "dairy_cow": "malkekøer",
        "dairy_cows": "malkekøer",
        "ammekøer": "ammekøer",
        "ammek�er": "ammekøer",
        "suckler": "ammekøer",
        "suckler_cow": "ammekøer",
        "suckler_cows": "ammekøer",
        "opdræt_0_6_mdr": "opdræt_0_6_mdr",
        "opdr�t_0_6_mdr": "opdræt_0_6_mdr",
        "heifer_0_6": "opdræt_0_6_mdr",
        "calf": "opdræt_0_6_mdr",
        "calves": "opdræt_0_6_mdr",
        "opdræt_6_mdr_kælvning": "opdræt_6_mdr_kælvning",
        "opdr�t_6_mdr_k�lvning": "opdræt_6_mdr_kælvning",
        "heifer_6_plus": "opdræt_6_mdr_kælvning",
        "heifer": "opdræt_6_mdr_kælvning",
        "heifers": "opdræt_6_mdr_kælvning",
        "kvier": "opdræt_6_mdr_kælvning",  # Default heifers to 6+ months
        "tyre_0_6_mdr": "tyre_0_6_mdr",
        "bull_0_6": "tyre_0_6_mdr",
        "tyre_6_mdr_slagtning": "tyre_6_mdr_slagtning",
        "bull_6_plus": "tyre_6_mdr_slagtning",
        "bull": "tyre_6_mdr_slagtning",
        "bulls": "tyre_6_mdr_slagtning",
        "steer": "tyre_6_mdr_slagtning",
        "steers": "tyre_6_mdr_slagtning",
        "tyre_stude": "tyre_6_mdr_slagtning",  # Generic bulls/steers
    }

    standard_type = type_mapping.get(cattle_type_clean, cattle_type_clean)

    if standard_type not in NH3_FACTORS:
        raise ValueError(
            f"Unknown cattle type: {cattle_type}. Available: {list(NH3_FACTORS.keys())}"
        )

    factor_data = NH3_FACTORS[standard_type]

    # Get emission factor
    if "nh3_kg_per_year" in factor_data:
        ef_nh3 = factor_data["nh3_kg_per_year"]
        production_type = "årsdyr"
    elif "nh3_kg_per_animal" in factor_data:
        ef_nh3 = factor_data["nh3_kg_per_animal"]
        production_type = "produceret_dyr"
    else:
        raise ValueError(f"No NH3 emission factor found for {cattle_type}")

    # Calculate total NH3 emissions
    total_nh3 = antal_dyr * ef_nh3

    return {
        "nh3_kg": total_nh3,
        "nh3_per_animal": ef_nh3,
        "emission_factor": ef_nh3,
        "production_type": production_type,
        "source": factor_data["source"],
        "unit": factor_data["unit"],
        "description": factor_data["description"],
    }


def calculate_all_cattle_nh3(
    livestock_data: dict[str, dict[str, float]],
) -> dict[str, dict]:
    """
    Calculate NH3 emissions for all cattle types in a farm.

    Args:
        livestock_data: Dict mapping cattle type to count, e.g.:
            {
                "malkekøer": {"count": 100},
                "kvier": {"count": 50},
                "tyre_stude": {"count": 30}
            }

    Returns:
        Dict with results per animal type and totals:
            {
                "malkekøer": {...},
                "kvier": {...},
                "tyre_stude": {...},
                "total": {"nh3_kg": X}
            }

    Source: SR671 2023 national data (DCE Aarhus University, 2025)
    """
    results = {}
    total_nh3 = 0.0

    for cattle_type, data in livestock_data.items():
        count = data["count"]

        if count > 0:
            result = calculate_nh3_emissions_kvaeg(cattle_type, count)
            results[cattle_type] = result
            total_nh3 += result["nh3_kg"]

    results["total"] = {
        "nh3_kg": total_nh3,
    }

    return results


if __name__ == "__main__":
    # Test with example farm
    print("=== Cattle NH3 Emissions Test ===\n")
    print("Source: SR671 2023 national data (DCE Aarhus University, 2025)")
    print("Note: NH3 is NOT converted to CO2e - reported separately in kg NH3\n")

    # Test 1: Dairy cows (årsdyr)
    result = calculate_nh3_emissions_kvaeg("malkekøer", 100)
    print("100 dairy cows (årsdyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3/year")
    print(f"  Per cow: {result['nh3_per_animal']:.2f} kg NH3/year")
    print(f"  Source: {result['source']}")
    print()

    # Test 2: Heifers 0-6 months (årsdyr)
    result = calculate_nh3_emissions_kvaeg("opdræt_0_6_mdr", 30)
    print("30 heifers 0-6 months (årsdyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3/year")
    print(f"  Per heifer: {result['nh3_per_animal']:.3f} kg NH3/year")
    print(f"  Source: {result['source']}")
    print()

    # Test 3: Heifers 6+ months (årsdyr)
    result = calculate_nh3_emissions_kvaeg("opdræt_6_mdr_kælvning", 50)
    print("50 heifers 6+ months (årsdyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3/year")
    print(f"  Per heifer: {result['nh3_per_animal']:.3f} kg NH3/year")
    print(f"  Source: {result['source']}")
    print()

    # Test 4: Bulls 6 months to slaughter (produceret dyr)
    result = calculate_nh3_emissions_kvaeg("tyre_6_mdr_slagtning", 30)
    print("30 bulls 6 months to slaughter (produceret dyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  Per bull: {result['nh3_per_animal']:.3f} kg NH3")
    print(f"  Source: {result['source']}")
    print()

    # Test 5: Full farm
    farm_data = {
        "malkekøer": {"count": 100},
        "kvier": {"count": 50},  # Maps to opdræt_6_mdr_kælvning
        "tyre_stude": {"count": 30},  # Maps to tyre_6_mdr_slagtning
    }
    results = calculate_all_cattle_nh3(farm_data)
    print("Full farm (100 dairy cows, 50 heifers, 30 bulls):")
    print(f"  Total NH3 emissions: {results['total']['nh3_kg']:.2f} kg NH3/year")
    total_animals = 100 + 50 + 30
    print(f"  Per animal (average): {results['total']['nh3_kg'] / total_animals:.2f} kg NH3")
    print()

    print("Environmental Impact:")
    print("  NH3 contributes to:")
    print("  - Acidification of soils and water")
    print("  - Eutrophication of aquatic ecosystems")
    print("  - Formation of particulate matter (PM2.5)")
    print("  - Human health impacts (air quality)")
    print()
    print("  NH3 does NOT contribute directly to climate change (not a GHG)")
    print("  Reported separately for environmental footprint analysis")
