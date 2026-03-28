"""
Pig enteric fermentation CH4 emissions.

Based on:
- IPCC 2006 Guidelines (Tier 1)
- Table 4 & 5: Danish standard values for pig feed energy
"""

import json
from pathlib import Path

# GWP for CH4 (IPCC AR6, 100-year)
# Note: AR4 uses 25, but we use AR6 (27) for biogenic CH4
# Livestock CH4 is biogenic (non-fossil), so we use 27 not 30
GWP_CH4 = 27


def load_reference_values() -> tuple[dict, dict]:
    """Load reference values from JSON tables."""
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(
        base_path
        / "tabel_4_standardværdierne_for_fe_pr_dyretype_svineproduktion_-_fordøjelse_side_27-28.json"
    ) as f:
        table_4 = json.load(f)

    with open(
        base_path
        / "tabel_5_yderligere_input_til_beregning_af_udledningen_af_metan_fra_svins_fordøjelse_side_28.json"
    ) as f:
        table_5 = json.load(f)

    # Parse Table 4 - FE per animal type
    fe_values = {}
    for row in table_4["data"]:
        animal_type = row["Dyretype"].lower()
        fe_str = row["FE_pr_dyr"]
        fe_values[animal_type] = fe_str

    # Parse Table 5 - Energy conversion factors
    constants = {}
    for row in table_5["data"]:
        key = row["Input_faktor"]
        value = row["Standardværdi"]
        constants[key] = value

    return fe_values, constants


# Load constants once at module import
FE_VALUES, CONSTANTS = load_reference_values()

# Energy content per feed unit (MJ/FE)
MJ_PER_FE = {
    "søer": CONSTANTS["Sofoder"],  # 17.5 MJ/FE (sow feed)
    "smågrise": CONSTANTS["Smågrisefoder"],  # 16.5 MJ/FE (weaner feed)
    "slagtesvin": CONSTANTS["Slagtesvinefoder"],  # 17.3 MJ/FE (finisher feed)
}

# IPCC Tier 1 constants
YM_FACTOR = CONSTANTS["Ym-faktor"]  # 0.006 (methane conversion factor)
MJ_TO_CH4 = CONSTANTS["Omregningsfaktor MJ til CH4"]  # 55.56

# Weight gain per animal (kg)
WEIGHT_GAIN = {
    "smågrise_konventionel": CONSTANTS["Tilvækst pr smågris konventionel"],  # 24.3 kg
    "smågrise_økologisk": CONSTANTS["Tilvækst pr smågris økologisk"],  # 16.0 kg
    "slagtesvin_konventionel": CONSTANTS["Tilvækst pr slagtesvin konventionel"],  # 82.0 kg
    "slagtesvin_økologisk": CONSTANTS["Tilvækst pr slagtesvin økologisk"],  # 82.0 kg
    "frats": CONSTANTS["Tilvækst pr. FRATS grise"],  # 106.3 kg
}


def calculate_ch4_enteric_svin(
    dyretype: str,
    antal_dyr: float,
    is_organic: bool = False,
    weight_gain_kg: float | None = None,
) -> dict[str, float]:
    """
    Calculate CH4 emissions from pig enteric fermentation (IPCC Tier 1).

    Formula:
        CH4 (kg/year) = (antal_dyr × FE_per_dyr × MJ_per_FE × Ym_factor) / 55.56
        CO2e = CH4 × 28 (GWP-100)

    Args:
        dyretype: Type of pig ('søer', 'smågrise', 'slagtesvin', 'frats')
        antal_dyr: Number of animals
        is_organic: Whether organic production (affects FE values)
        weight_gain_kg: Custom weight gain per animal (optional, uses defaults if None)

    Returns:
        Dict with:
            - ch4_kg: CH4 emissions (kg/year)
            - co2e_kg: CO2 equivalent (kg/year)
            - fe_per_animal: Feed energy per animal (FE)
            - gross_energy_mj: Total gross energy (MJ)

    Example:
        >>> # 1000 conventional finishers
        >>> result = calculate_ch4_enteric_svin('slagtesvin', 1000, is_organic=False)
        >>> print(f"CH4: {result['ch4_kg']:.2f} kg, CO2e: {result['co2e_kg']:.2f} kg")
    """
    if antal_dyr <= 0:
        return {
            "ch4_kg": 0.0,
            "co2e_kg": 0.0,
            "fe_per_animal": 0.0,
            "gross_energy_mj": 0.0,
        }

    # Normalize animal type
    dyretype_clean = dyretype.lower().strip()

    # Determine production system
    production = "økologiske" if is_organic else "konventionelle"

    # Get FE per animal
    if dyretype_clean in ["søer", "so", "sow", "sows"]:
        # Sows: FE per year-sow
        fe_per_animal = 1492 if production == "konventionelle" else 1843
        mj_per_fe = MJ_PER_FE["søer"]

    elif dyretype_clean in ["smågrise", "smågris", "weaner", "weaners"]:
        # Weaners: FE per kg weight gain
        fe_per_kg = 1.87 if production == "konventionelle" else 2.11
        production_key = "konventionel" if production == "konventionelle" else "økologisk"
        weight_gain = weight_gain_kg or WEIGHT_GAIN[f"smågrise_{production_key}"]
        fe_per_animal = fe_per_kg * weight_gain
        mj_per_fe = MJ_PER_FE["smågrise"]

    elif dyretype_clean in ["slagtesvin", "slagtegris", "finisher", "finishers"]:
        # Finishers: FE per kg weight gain
        fe_per_kg = 2.77 if production == "konventionelle" else 2.94
        production_key = "konventionel" if production == "konventionelle" else "økologisk"
        weight_gain = weight_gain_kg or WEIGHT_GAIN[f"slagtesvin_{production_key}"]
        fe_per_animal = fe_per_kg * weight_gain
        mj_per_fe = MJ_PER_FE["slagtesvin"]

    elif dyretype_clean in ["frats", "frats grise"]:
        # FRATS pigs (special category)
        fe_per_kg = 2.56
        weight_gain = weight_gain_kg or WEIGHT_GAIN["frats"]
        fe_per_animal = fe_per_kg * weight_gain
        mj_per_fe = MJ_PER_FE["slagtesvin"]

    else:
        raise ValueError(
            f"Unknown pig type: {dyretype}. Must be one of: søer, smågrise, slagtesvin, frats"
        )

    # Calculate gross energy intake (MJ/year)
    gross_energy_mj = antal_dyr * fe_per_animal * mj_per_fe

    # Calculate CH4 (kg/year) using IPCC Tier 1
    # Formula: GE × Ym / MJ_to_CH4
    ch4_kg = (gross_energy_mj * YM_FACTOR) / MJ_TO_CH4

    # Convert to CO2e
    co2e_kg = ch4_kg * GWP_CH4

    return {
        "ch4_kg": ch4_kg,
        "co2e_kg": co2e_kg,
        "fe_per_animal": fe_per_animal,
        "gross_energy_mj": gross_energy_mj,
    }


def calculate_all_pig_types(
    livestock_data: dict[str, int], is_organic: bool = False
) -> dict[str, dict[str, float]]:
    """
    Calculate enteric CH4 for all pig types in a farm.

    Args:
        livestock_data: Dict mapping pig type to count, e.g.:
            {
                "søer": 200,
                "smågrise": 500,
                "slagtesvin": 1500
            }
        is_organic: Whether organic production

    Returns:
        Dict with results per animal type and totals:
            {
                "søer": {...},
                "smågrise": {...},
                "slagtesvin": {...},
                "total": {"ch4_kg": X, "co2e_kg": Y}
            }
    """
    results = {}
    total_ch4 = 0.0
    total_co2e = 0.0

    for pig_type, count in livestock_data.items():
        if count > 0:
            result = calculate_ch4_enteric_svin(pig_type, count, is_organic)
            results[pig_type] = result
            total_ch4 += result["ch4_kg"]
            total_co2e += result["co2e_kg"]

    results["total"] = {
        "ch4_kg": total_ch4,
        "co2e_kg": total_co2e,
    }

    return results


if __name__ == "__main__":
    # Test with example farm
    print("=== Pig Enteric Fermentation Test ===\n")

    # Test 1: Conventional sows
    result = calculate_ch4_enteric_svin("søer", 200, is_organic=False)
    print("200 conventional sows:")
    print(f"  CH4: {result['ch4_kg']:.2f} kg/year")
    print(f"  CO2e: {result['co2e_kg']:.2f} kg/year")
    print(f"  Per sow: {result['co2e_kg'] / 200:.2f} kg CO2e/year\n")

    # Test 2: Conventional finishers
    result = calculate_ch4_enteric_svin("slagtesvin", 1000, is_organic=False)
    print("1000 conventional finishers:")
    print(f"  CH4: {result['ch4_kg']:.2f} kg/year")
    print(f"  CO2e: {result['co2e_kg']:.2f} kg/year")
    print(f"  Per pig: {result['co2e_kg'] / 1000:.2f} kg CO2e/year\n")

    # Test 3: Full farm
    farm_data = {
        "søer": 200,
        "smågrise": 500,
        "slagtesvin": 1500,
    }
    results = calculate_all_pig_types(farm_data, is_organic=False)
    print("Full farm (200 sows, 500 weaners, 1500 finishers):")
    print(f"  Total CH4: {results['total']['ch4_kg']:.2f} kg/year")
    print(f"  Total CO2e: {results['total']['co2e_kg']:.2f} kg/year")
    print(f"  Per animal: {results['total']['co2e_kg'] / (200 + 500 + 1500):.2f} kg CO2e/year")
