"""
Pig housing and manure storage emissions (CH4 and N2O).

Based on:
- IPCC 2006 Guidelines
- Table 11: Housing emission factors (NH3, N2O)
- Table 13: MCF values for manure systems
- Table 33: Manure composition (N content, VS)
"""

import json
from pathlib import Path

# GWP factors (IPCC AR6, 2021)
GWP_CH4 = 27  # Biogenic methane
GWP_N2O = 273

# Molecular weight conversion factor (N2O-N to N2O)
N2O_TO_N2O_N = 44.0 / 28.0


def load_reference_values() -> tuple[dict, dict, dict]:
    """Load reference values from JSON tables."""
    base_path = Path(__file__).parent.parent.parent / "reference_values"

    with open(
        base_path
        / "tabel_11_emissionsfaktorer_for_ammoniak_og_lattergas_til_beregning_af_emissioner_i_stalden_fra_gylle.json"
    ) as f:
        table_11 = json.load(f)

    with open(
        base_path
        / "tabel_13_mcf_for_gylle_og_dybstrøelse_samt_reduktionen_i_mcf_hvis_der_anvendes_staldforsuring_eller_.json"
    ) as f:
        table_13 = json.load(f)

    with open(
        base_path
        / "tabel_33_normtal_for_kvælstof_tons_gødning_og_ts_kulstofbalancen_i_jorden_side_102.json"
    ) as f:
        table_33 = json.load(f)

    # Parse Table 11 - Housing emission factors
    housing_factors = {}
    for row in table_11["data"]:
        housing_type = row["Staldtype"]
        housing_factors[housing_type] = {
            "n2o_pct_gylle": row.get("N2O_pct_af_N_Gylle"),
            "n2o_pct_dybstrøelse": row.get("N2O_pct_af_N_Dybstrøelse"),
            "nh3_pct_tan_gylle": row.get("NH3_pct_af_TAN_ab_dyr_Gylle"),
            "nh3_pct_total_n_dybstrøelse": row.get("NH3_pct_af_total_N_ab_dyr_Dybstrøelse"),
        }

    # Parse Table 13 - MCF values
    mcf_values = {}
    for row in table_13["data"]:
        system_type = row["Type"]
        mcf_val = row["MCF"]
        mcf_values[system_type.lower()] = {
            "mcf": mcf_val if isinstance(mcf_val, int | float) else None,
            "days_in_barn": row.get("Antal_dage_i_stalden"),
        }

    # Parse Table 33 - Manure composition
    manure_comp = {}
    for row in table_33["data"]:
        manure_type = row["Gødningstype"]
        manure_comp[manure_type] = {
            "ts_pct": row["tørstof_pct"],
            "kg_n_per_ton": row["kg_N_t"],
            "kg_ts_per_kg_n": row["kg_ts_kg_N"],
            "kg_c_per_kg_n": row["faktor_kg_C_kg_N"],
        }

    return housing_factors, mcf_values, manure_comp


# Load constants once at module import
HOUSING_FACTORS, MCF_VALUES, MANURE_COMP = load_reference_values()

# IPCC default values for pigs
IPCC_PIG_DEFAULTS = {
    "vs_excretion_kg_per_kg_bw": 0.016,  # kg VS per kg body weight per day (IPCC Table 10.14)
    "n_excretion_kg_per_kg_bw": 0.016,  # kg N per kg body weight per year (IPCC Table 10.19)
    "b0_m3_ch4_per_kg_vs": 0.45,  # Maximum methane potential for pigs (IPCC Table 10.14)
    "tan_fraction": 0.7,  # Total Ammoniacal Nitrogen fraction (typical for slurry)
}


def calculate_manure_ch4(
    antal_dyr: float,
    average_body_weight_kg: float,
    housing_system: str = "gylle",
    days_in_barn: float | None = None,
    use_acidification: bool = False,
    use_biogas: bool = False,
) -> dict[str, float]:
    """
    Calculate CH4 emissions from pig manure management.

    Formula (IPCC 2006):
        CH4 (kg) = antal_dyr × VS_excretion × (MCF/100) × B0 × (days_in_barn/365)

    Where:
        - VS_excretion: Volatile solids excretion (kg/animal/year)
        - MCF: Methane conversion factor (%)
        - B0: Maximum methane potential (m³ CH4/kg VS) = 0.45 for pigs
        - days_in_barn: Days in housing (default 182.5 = 50% of year)

    Args:
        antal_dyr: Number of animals
        average_body_weight_kg: Average body weight (kg)
        housing_system: 'gylle' (slurry), 'dybstrøelse' (deep litter), or 'afgræsning' (grazing)
        days_in_barn: Days in housing (default from Table 13)
        use_acidification: Apply 60% MCF reduction
        use_biogas: Apply 40% MCF reduction

    Returns:
        Dict with:
            - ch4_kg: CH4 emissions (kg/year)
            - co2e_kg: CO2 equivalent (kg/year)
            - vs_excretion_kg: VS excretion (kg/year)
            - mcf_pct: MCF used (%)
    """
    if antal_dyr <= 0 or average_body_weight_kg <= 0:
        return {"ch4_kg": 0.0, "co2e_kg": 0.0, "vs_excretion_kg": 0.0, "mcf_pct": 0.0}

    # Get MCF for housing system
    system_key = housing_system.lower()
    if system_key not in MCF_VALUES:
        raise ValueError(
            f"Unknown housing system: {housing_system}. Must be one of: {list(MCF_VALUES.keys())}"
        )

    mcf_data = MCF_VALUES[system_key]
    mcf_pct = mcf_data["mcf"]

    if mcf_pct is None:
        raise ValueError(f"No MCF value available for {housing_system}")

    # Apply technology adjustments
    if use_acidification:
        mcf_pct *= 0.4  # 60% reduction
    elif use_biogas:
        mcf_pct *= 0.6  # 40% reduction

    # Get days in barn (default from table)
    if days_in_barn is None:
        days_in_barn = mcf_data["days_in_barn"] or 182.5

    # Calculate VS excretion (kg/year)
    # IPCC default: 0.016 kg VS/kg body weight/day
    vs_per_animal_per_day = average_body_weight_kg * IPCC_PIG_DEFAULTS["vs_excretion_kg_per_kg_bw"]
    vs_per_animal_year = vs_per_animal_per_day * 365
    total_vs_kg = antal_dyr * vs_per_animal_year

    # Calculate CH4 emissions (IPCC 2006)
    # CH4 = VS × (MCF/100) × B0 × (days_in_barn/365) × 0.67
    # Note: 0.67 converts m³ CH4 to kg CH4 (density)
    ch4_m3 = (
        total_vs_kg
        * (mcf_pct / 100)
        * IPCC_PIG_DEFAULTS["b0_m3_ch4_per_kg_vs"]
        * (days_in_barn / 365)
    )
    ch4_kg = ch4_m3 * 0.67  # Convert m³ to kg

    # Convert to CO2e
    co2e_kg = ch4_kg * GWP_CH4

    return {
        "ch4_kg": ch4_kg,
        "co2e_kg": co2e_kg,
        "vs_excretion_kg": total_vs_kg,
        "mcf_pct": mcf_pct,
    }


def calculate_manure_n2o(
    antal_dyr: float,
    n_excretion_kg_per_animal: float,
    housing_type: str = "Bindestald med riste",
    system_type: str = "gylle",
) -> dict[str, float]:
    """
    Calculate N2O emissions from pig manure in housing.

    Formula (IPCC 2006):
        N2O (kg) = antal_dyr × N_excretion × EF_N2O × (44/28)

    Where:
        - N_excretion: Nitrogen excretion (kg N/animal/year)
        - EF_N2O: Emission factor (% of N) from Table 11
        - 44/28: Molecular weight conversion (N2O-N to N2O)

    Args:
        antal_dyr: Number of animals
        n_excretion_kg_per_animal: N excretion per animal (kg N/year)
        housing_type: Danish housing system name (from Table 11)
        system_type: 'gylle' (slurry) or 'dybstrøelse' (deep litter)

    Returns:
        Dict with:
            - n2o_kg: N2O emissions (kg/year)
            - co2e_kg: CO2 equivalent (kg/year)
            - n_excretion_kg: Total N excretion (kg/year)
            - ef_n2o_pct: Emission factor used (%)
    """
    if antal_dyr <= 0 or n_excretion_kg_per_animal <= 0:
        return {"n2o_kg": 0.0, "co2e_kg": 0.0, "n_excretion_kg": 0.0, "ef_n2o_pct": 0.0}

    # Get emission factor from Table 11
    if housing_type not in HOUSING_FACTORS:
        # Default to slurry system with slats
        housing_type = "Bindestald med riste"

    factors = HOUSING_FACTORS[housing_type]

    # Select appropriate EF based on system type
    if system_type.lower() == "gylle":
        ef_n2o_pct = factors["n2o_pct_gylle"]
    elif system_type.lower() == "dybstrøelse":
        ef_n2o_pct = factors["n2o_pct_dybstrøelse"]
    else:
        raise ValueError(f"Unknown system type: {system_type}. Must be 'gylle' or 'dybstrøelse'")

    if ef_n2o_pct is None:
        raise ValueError(f"No N2O emission factor for {housing_type} with {system_type}")

    # Calculate total N excretion
    total_n_kg = antal_dyr * n_excretion_kg_per_animal

    # Calculate N2O emissions (IPCC 2006)
    n2o_n_kg = total_n_kg * (ef_n2o_pct / 100)
    n2o_kg = n2o_n_kg * N2O_TO_N2O_N

    # Convert to CO2e
    co2e_kg = n2o_kg * GWP_N2O

    return {
        "n2o_kg": n2o_kg,
        "co2e_kg": co2e_kg,
        "n_excretion_kg": total_n_kg,
        "ef_n2o_pct": ef_n2o_pct,
    }


def calculate_manure_emissions_svin(
    antal_dyr: float,
    average_body_weight_kg: float,
    n_excretion_kg_per_animal: float | None = None,
    housing_system: str = "gylle",
    housing_type: str = "Bindestald med riste",
    use_acidification: bool = False,
    use_biogas: bool = False,
) -> dict[str, float]:
    """
    Calculate total manure emissions (CH4 + N2O) for pigs.

    Args:
        antal_dyr: Number of animals
        average_body_weight_kg: Average body weight (kg)
        n_excretion_kg_per_animal: N excretion per animal (kg N/year), uses IPCC default if None
        housing_system: 'gylle', 'dybstrøelse', or 'afgræsning'
        housing_type: Specific housing type from Table 11
        use_acidification: Apply acidification technology
        use_biogas: Apply biogas treatment

    Returns:
        Dict with:
            - ch4_co2e_kg: CH4 emissions in CO2e (kg/year)
            - n2o_co2e_kg: N2O emissions in CO2e (kg/year)
            - total_co2e_kg: Total emissions (kg/year)
            - ch4_details: Detailed CH4 results
            - n2o_details: Detailed N2O results
    """
    # Calculate CH4 emissions
    ch4_result = calculate_manure_ch4(
        antal_dyr=antal_dyr,
        average_body_weight_kg=average_body_weight_kg,
        housing_system=housing_system,
        use_acidification=use_acidification,
        use_biogas=use_biogas,
    )

    # Estimate N excretion if not provided (IPCC default)
    if n_excretion_kg_per_animal is None:
        n_excretion_kg_per_animal = (
            average_body_weight_kg * IPCC_PIG_DEFAULTS["n_excretion_kg_per_kg_bw"]
        )

    # Calculate N2O emissions
    system_type = "dybstrøelse" if housing_system.lower() == "dybstrøelse" else "gylle"
    n2o_result = calculate_manure_n2o(
        antal_dyr=antal_dyr,
        n_excretion_kg_per_animal=n_excretion_kg_per_animal,
        housing_type=housing_type,
        system_type=system_type,
    )

    return {
        "ch4_co2e_kg": ch4_result["co2e_kg"],
        "n2o_co2e_kg": n2o_result["co2e_kg"],
        "total_co2e_kg": ch4_result["co2e_kg"] + n2o_result["co2e_kg"],
        "ch4_details": ch4_result,
        "n2o_details": n2o_result,
    }


if __name__ == "__main__":
    # Test with example farm
    print("=== Pig Manure Emissions Test ===\n")

    # Test 1: Conventional finishers in slurry system
    result = calculate_manure_emissions_svin(
        antal_dyr=1000,
        average_body_weight_kg=100,  # 100 kg average weight
        housing_system="gylle",
        housing_type="Sengestald med spalter (kanal, bagskyl eller ringkanal)",
    )
    print("1000 finishers (100 kg avg) in slatted floor barn:")
    print(f"  CH4: {result['ch4_co2e_kg']:.2f} kg CO2e/year")
    print(f"  N2O: {result['n2o_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Total: {result['total_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per pig: {result['total_co2e_kg'] / 1000:.2f} kg CO2e/year\n")

    # Test 2: Sows in deep litter
    result = calculate_manure_emissions_svin(
        antal_dyr=200,
        average_body_weight_kg=220,  # 220 kg average weight
        housing_system="dybstrøelse",
        housing_type="Dybstrøelse",
    )
    print("200 sows (220 kg avg) in deep litter:")
    print(f"  CH4: {result['ch4_co2e_kg']:.2f} kg CO2e/year")
    print(f"  N2O: {result['n2o_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Total: {result['total_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per sow: {result['total_co2e_kg'] / 200:.2f} kg CO2e/year\n")

    # Test 3: With acidification technology
    result = calculate_manure_emissions_svin(
        antal_dyr=1000,
        average_body_weight_kg=100,
        housing_system="gylle",
        use_acidification=True,
    )
    print("1000 finishers with acidification (60% CH4 reduction):")
    print(f"  CH4: {result['ch4_co2e_kg']:.2f} kg CO2e/year")
    print(f"  N2O: {result['n2o_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Total: {result['total_co2e_kg']:.2f} kg CO2e/year")
    print(f"  Per pig: {result['total_co2e_kg'] / 1000:.2f} kg CO2e/year")
