"""
Pig NH3 (ammonia) emissions from manure management.

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

from typing import Dict
from pathlib import Path
import json


def load_nh3_factors() -> Dict[str, Dict[str, float]]:
    """
    Load NH3 emission factors from SR671 2023 national data.

    Returns NH3 emission factors in kg NH3 per animal.

    Source: SR671 (Aarhus University DCE, 2025)
    """
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(base_path / "nh3_factors_sr671_2023.json") as f:
        data = json.load(f)

    # Extract pig factors
    pig_factors = {}
    for pig_type, factor_data in data["data"]["grise"].items():
        pig_factors[pig_type] = factor_data

    return pig_factors


# Load constants once at module import
NH3_FACTORS = load_nh3_factors()


def calculate_nh3_emissions_svin(
    dyretype: str,
    antal_dyr: float,
) -> Dict[str, float]:
    """
    Calculate NH3 emissions from pig manure management.

    Formula:
        NH3 (kg) = antal_dyr × EF_nh3

    Where:
        - antal_dyr: Number of animals (sows: årsdyr, weaners/finishers: produceret dyr)
        - EF_nh3: NH3 emission factor (kg NH3 per animal)

    Args:
        dyretype: Type of pig ('årssøer', 'smågrise', 'slagtesvin')
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
        >>> # 200 sows (årsdyr)
        >>> result = calculate_nh3_emissions_svin('årssøer', 200)
        >>> print(f"NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
        NH3 emissions: 677.20 kg NH3
    """
    if antal_dyr <= 0:
        return {
            "nh3_kg": 0.0,
            "nh3_per_animal": 0.0,
            "emission_factor": 0.0,
            "production_type": "unknown",
            "source": "N/A",
        }

    # Normalize pig type
    dyretype_clean = dyretype.lower().strip().replace(" ", "_")

    # Map to standard names
    type_mapping = {
        "årssøer": "årssøer",
        "årss�er": "årssøer",
        "soer": "årssøer",
        "sow": "årssøer",
        "sows": "årssøer",
        "smågrise": "smågrise",
        "sm�grise": "smågrise",
        "weaner": "smågrise",
        "weaners": "smågrise",
        "slagtesvin": "slagtesvin",
        "finisher": "slagtesvin",
        "finishers": "slagtesvin",
        "frats": "slagtesvin",  # FRATS uses finisher factors
    }

    standard_type = type_mapping.get(dyretype_clean, dyretype_clean)

    if standard_type not in NH3_FACTORS:
        raise ValueError(f"Unknown pig type: {dyretype}. Available: {list(NH3_FACTORS.keys())}")

    factor_data = NH3_FACTORS[standard_type]

    # Get emission factor
    if "nh3_kg_per_year" in factor_data:
        ef_nh3 = factor_data["nh3_kg_per_year"]
        production_type = "årsdyr"
    elif "nh3_kg_per_animal" in factor_data:
        ef_nh3 = factor_data["nh3_kg_per_animal"]
        production_type = "produceret_dyr"
    else:
        raise ValueError(f"No NH3 emission factor found for {dyretype}")

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


def calculate_all_pig_nh3(
    livestock_data: Dict[str, Dict[str, float]],
) -> Dict[str, Dict]:
    """
    Calculate NH3 emissions for all pig types in a farm.

    Args:
        livestock_data: Dict mapping pig type to count, e.g.:
            {
                "årssøer": {"count": 200},
                "smågrise": {"count": 500},
                "slagtesvin": {"count": 1500}
            }

    Returns:
        Dict with results per animal type and totals:
            {
                "årssøer": {...},
                "smågrise": {...},
                "slagtesvin": {...},
                "total": {"nh3_kg": X}
            }

    Source: SR671 2023 national data (DCE Aarhus University, 2025)
    """
    results = {}
    total_nh3 = 0.0

    for pig_type, data in livestock_data.items():
        count = data["count"]

        if count > 0:
            result = calculate_nh3_emissions_svin(pig_type, count)
            results[pig_type] = result
            total_nh3 += result["nh3_kg"]

    results["total"] = {
        "nh3_kg": total_nh3,
    }

    return results


if __name__ == "__main__":
    # Test with example farm
    print("=== Pig NH3 Emissions Test ===\n")
    print("Source: SR671 2023 national data (DCE Aarhus University, 2025)")
    print("Note: NH3 is NOT converted to CO2e - reported separately in kg NH3\n")

    # Test 1: Sows (årsdyr)
    result = calculate_nh3_emissions_svin("årssøer", 200)
    print(f"200 sows (årsdyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3/year")
    print(f"  Per sow: {result['nh3_per_animal']:.4f} kg NH3/year")
    print(f"  Source: {result['source']}")
    print()

    # Test 2: Weaners (produceret dyr)
    result = calculate_nh3_emissions_svin("smågrise", 500)
    print(f"500 weaners (produceret dyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  Per weaner: {result['nh3_per_animal']:.5f} kg NH3")
    print(f"  Source: {result['source']}")
    print()

    # Test 3: Finishers (produceret dyr)
    result = calculate_nh3_emissions_svin("slagtesvin", 1500)
    print(f"1500 finishers (produceret dyr):")
    print(f"  NH3 emissions: {result['nh3_kg']:.2f} kg NH3")
    print(f"  Per finisher: {result['nh3_per_animal']:.4f} kg NH3")
    print(f"  Source: {result['source']}")
    print()

    # Test 4: Full farm
    farm_data = {
        "årssøer": {"count": 200},
        "smågrise": {"count": 500},
        "slagtesvin": {"count": 1500},
    }
    results = calculate_all_pig_nh3(farm_data)
    print("Full farm (200 sows, 500 weaners, 1500 finishers):")
    print(f"  Total NH3 emissions: {results['total']['nh3_kg']:.2f} kg NH3/year")
    total_animals = 200 + 500 + 1500
    print(f"  Per animal (average): {results['total']['nh3_kg']/total_animals:.4f} kg NH3")
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
