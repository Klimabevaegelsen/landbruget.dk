"""
Detailed calculation of emissions from cattle manure in stable and storage,
based on the note with calculation basis, pages 42-50.
"""

import json
from pathlib import Path
from typing import Any, Dict

# --- Constants ---
MOL_WEIGHT_N2O_N_FACTOR = 44.0 / 28.0  # (M_N2O / M_N2)
EF_INDIRECT_N2O_FROM_NH3_NOX = 0.01 # IPCC 2006 factor for N2O from volatilized N
# THETA_N2O_CO2 should be loaded or defined consistently (e.g., 298.0 from Markdown page 4)
# THETA_CH4_CO2 should be loaded or defined consistently (e.g., 25.0 from Markdown page 4)

# --- Utility to load data (similar to other files) ---
# In a real package, this would be a shared utility
def load_reference_data(file_name: str) -> Any:
    base_path = Path(__file__).resolve().parent.parent / "reference_values"
    full_path = base_path / file_name
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {full_path}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {full_path}")
        raise

# --- Data Loading Functions (examples, assuming JSON structures) ---
# These would load data from tabel_11_emissionsfaktorer_ammoniak_lattergas_stald.json, etc.
# For now, we'll use placeholders for factors or assume they are passed directly.

def get_stable_emission_factors(stable_type_key: str, manure_system_type: str) -> Dict[str, float]:
    """
    Placeholder: Fetches emission factors for a given stable type from a table (e.g., Table 11).
    Manure system type can be 'gylle' or 'dybstrøelse'.
    Returns a dict like: {'nh3_tan_percent': X, 'n2o_total_n_percent': Y}
    """
    # This would look up in a structured representation of Table 11
    # Example for "Sengestald med spalter (kanal, bagskyl eller ringkanal)" (Gylle):
    if stable_type_key == "Sengestald med spalter (kanal, bagskyl eller ringkanal)" and manure_system_type == "gylle":
        return {'nh3_tan_percent': 13.5, 'n2o_total_n_percent': 0.2} # NH3 % of TAN, N2O % of Total N
    elif stable_type_key == "Dybstrøelse" and manure_system_type == "dybstrøelse":
        return {'nh3_total_n_percent': 6.0, 'n2o_total_n_percent': 1.0} # NH3 % of Total N for deep litter, N2O % of Total N
    # Add more stable types and gylle/dybstrøelse differentiation based on Table 11
    print(f"Warning: Using placeholder/default EFs for stable {stable_type_key}, {manure_system_type}")
    return {'nh3_tan_percent': 0.0, 'nh3_total_n_percent': 0.0, 'n2o_total_n_percent': 0.0}


# --- STABLE EMISSIONS ---

def calculate_stable_nh3_kg_n_pr_dyr_pr_aar(
    kg_tan_n_in_stable_pr_dyr_pr_aar: float, # For gylle systems
    kg_total_n_in_stable_pr_dyr_pr_aar: float, # For dybstrøelse systems (N from animal + bedding)
    stable_type_key: str, # Key to look up factors in Table 11
    manure_system_type: str # 'gylle' or 'dybstrøelse' within the stable part being calculated
) -> float:
    """
    Calculates NH3-N emission from stable per animal per year (kg NH3-N).
    Markdown page 43.
    'kg TAN N in stable' or 'kg N i stalden' (total N for deep litter)
    'NH3, % af TAN N' or 'NH3, % af total-N ab dyr' (for deep litter)
    """
    ef_data = get_stable_emission_factors(stable_type_key, manure_system_type)

    nh3_n_kg = 0.0
    if manure_system_type == 'gylle':
        # NH3_N = kg_TAN_N_in_stable * (NH3_%_of_TAN_N / 100)
        nh3_n_kg = kg_tan_n_in_stable_pr_dyr_pr_aar * (ef_data.get('nh3_tan_percent', 0.0) / 100.0)
    elif manure_system_type == 'dybstrøelse':
        # For deep litter, NH3 emission is based on total N (animal + bedding)
        # Table 11 shows "NH3, % af total-N ab dyr" for Dybstrøelse, implying this is N from animal.
        # The text below mentions N from strøelse is added to N from animal for N2O, but not explicitly for NH3 calc from deep litter.
        # However, the general principle for solid manure is often based on total N.
        # Let's assume kg_total_n_in_stable_pr_dyr_pr_aar includes N from bedding for this system type if NH3 EF is % of total N.
        # Table 11's "NH3, % af total-N ab dyr" for Dybstrøelse seems to be the EF for the total N in the deep litter system part.
        nh3_n_kg = kg_total_n_in_stable_pr_dyr_pr_aar * (ef_data.get('nh3_total_n_percent', 0.0) / 100.0)
    else:
        raise ValueError(f"Unknown manure_system_type: {manure_system_type}")

    return nh3_n_kg

def calculate_indirect_co2e_from_nh3_kg(
    nh3_n_kg: float,
    theta_n2o_co2: float
) -> float:
    """
    Calculates indirect CO2e from NH3-N emissions.
    N2O_from_NH3 = NH3_N_kg * EFN2O_indirect * (44/28)
    CO2e = N2O_from_NH3 * theta_N2O_CO2
    Markdown page 44.
    """
    # Amount of N2O produced is NH3_N_kg * EF_INDIRECT_N2O_FROM_NH3_NOX
    # This N2O is in terms of N mass (kg N2O-N)
    # To convert to kg N2O: (kg N2O-N) * (44/28)
    n2o_mass_from_nh3_kg = nh3_n_kg * EF_INDIRECT_N2O_FROM_NH3_NOX * MOL_WEIGHT_N2O_N_FACTOR
    co2e = n2o_mass_from_nh3_kg * theta_n2o_co2
    return co2e

# Further functions for stable direct N2O, stable CH4,
# manure ab stald characteristics, and storage emissions will follow.

def calculate_stable_direct_n2o_kg_pr_dyr_pr_aar(
    kg_total_n_in_stable_pr_dyr_pr_aar: float, # N from animal + N from bedding
    stable_type_key: str, # Key to look up factors in Table 11
    manure_system_type: str # 'gylle' or 'dybstrøelse'
) -> float:
    """
    Calculates direct N2O emission (as kg N2O) from stable per animal per year.
    Markdown page 44: "NEH_N2O,kg" = ("kg N i stalden" + "kg N strøelse") x "N2O, % af N"/100
    The result NEH_N2O is in kg N. This function will convert to kg N2O.
    """
    ef_data = get_stable_emission_factors(stable_type_key, manure_system_type)
    # "N2O, % af N" is the emission factor for N2O-N from total N.
    n2o_n_kg = kg_total_n_in_stable_pr_dyr_pr_aar * (ef_data.get('n2o_total_n_percent', 0.0) / 100.0)
    n2o_kg = n2o_n_kg * MOL_WEIGHT_N2O_N_FACTOR # Convert N2O-N to N2O mass
    return n2o_kg

def calculate_co2e_from_n2o_kg(n2o_kg: float, theta_n2o_co2: float) -> float:
    """
    Converts N2O mass (kg) to CO2e (kg).
    """
    return n2o_kg * theta_n2o_co2

def get_methane_emission_factors(manure_system_type: str, is_milk_cow: bool) -> Dict[str, float]:
    """
    Placeholder: Fetches methane (CH4) emission factors (B0, MCF_percent) for a given manure system.
    B0 from Table 12 (0.24 for dairy, 0.18 for other cattle).
    MCF from Table 13 (e.g., Gylle 12.4%, Dybstrøelse 17%).
    """
    b0_factor = 0.24 if is_milk_cow else 0.18 # From Table 12
    mcf_percent = 0.0
    if manure_system_type == 'gylle':
        mcf_percent = 12.4 # From Table 13 for Gylle
    elif manure_system_type == 'dybstrøelse':
        mcf_percent = 17.0 # From Table 13 for Dybstrøelse
    # Add other conditions like staldforsuring, biogas from Table 13 if applicable at this stage
    else:
        print(f"Warning: Using default MCF for CH4 for manure system {manure_system_type}")

    return {'b0': b0_factor, 'mcf_percent': mcf_percent}

def calculate_stable_ch4_kg_pr_dyr_pr_aar(
    vs_goedning_kg_pr_dyr_pr_aar: float, # Volatile solids from manure
    vs_stroelse_kg_pr_dyr_pr_aar: float, # Volatile solids from bedding
    stable_type_key: str, # For context if EFs vary by full stable type
    manure_system_type: str, # 'gylle' or 'dybstrøelse' for selecting B0/MCF
    is_milk_cow: bool,
    days_in_stable_for_methane_calc: float = 182.5 # Default 50% split as per Markdown page 45
) -> float:
    """
    Calculates CH4 emission (as kg CH4) from stable per animal per year.
    Markdown page 45: "NE_CH4, kg" = (Vs gødning + Vs strøelse/365) x "0,67" x"B0" x "MCF, %" / 100 * antal dage i stalden
    Note: Vs strøelse is annual, so Vs strøelse/365 gives daily. Then multiplied by days in stable.
    Here, inputs vs_goedning and vs_stroelse are annual per animal.
    We calculate daily VS and then multiply by days_in_stable.
    """
    methane_efs = get_methane_emission_factors(manure_system_type, is_milk_cow)
    b0 = methane_efs['b0']
    mcf_stable_percent = methane_efs['mcf_percent'] # Assuming this MCF is for the combined stable/storage, and split by days

    # Daily VS input
    vs_total_daily_kg = (vs_goedning_kg_pr_dyr_pr_aar / 365.0) + (vs_stroelse_kg_pr_dyr_pr_aar / 365.0)

    # CH4 emission = Total_VS_for_period * B0 * (MCF_percent / 100) * density_CH4
    # Total_VS_for_period = vs_total_daily_kg * days_in_stable_for_methane_calc
    # Density of CH4 = 0.67 kg/m3 (IPCC default for 20 C, used in formula)
    # B0 is in m3 CH4/kg VS

    ch4_kg = vs_total_daily_kg * days_in_stable_for_methane_calc * b0 * (mcf_stable_percent / 100.0) * 0.67
    return ch4_kg

def calculate_co2e_from_ch4_kg(ch4_kg: float, theta_ch4_co2: float) -> float:
    """
    Converts CH4 mass (kg) to CO2e (kg).
    """
    return ch4_kg * theta_ch4_co2

# --- MANURE CHARACTERISTICS AB STALD (leaving stable) ---
# Markdown page 47

def calculate_manure_ab_stald(
    initial_total_n_pr_dyr_pr_aar: float, # N from animal + N from bedding
    initial_tan_n_pr_dyr_pr_aar: float,   # TAN from animal (bedding usually low TAN)
    stable_nh3_n_loss_kg: float,          # Result from calculate_stable_nh3_kg_n_pr_dyr_pr_aar
    stable_direct_n2o_n_loss_kg: float    # This is N2O-N (e.g. kg_total_n * EF_N2O_percent/100)
) -> Dict[str, float]:
    """
    Calculates N and TAN content in manure leaving the stable (ab stald) per animal per year.
    VS content change in stable is often complex; for now, assume VS ab stald is VS input if not otherwise specified.
    """
    # kg N i gylle ab stald = Total kg N ab dyr + kg N strøelse - NEH_N2O (as N) - NEH_NH3 (as N)
    n_ab_stald_kg = initial_total_n_pr_dyr_pr_aar - stable_direct_n2o_n_loss_kg - stable_nh3_n_loss_kg

    # kg TAN i gylle ab stald = Total kg TAN N ab dyr - NEH_NH3 (as N)
    tan_ab_stald_kg = initial_tan_n_pr_dyr_pr_aar - stable_nh3_n_loss_kg

    return {
        'total_n_ab_stald_kg_pr_dyr_pr_aar': n_ab_stald_kg,
        'tan_n_ab_stald_kg_pr_dyr_pr_aar': tan_ab_stald_kg
    }

# --- STORAGE EMISSIONS ---
# Markdown pages 47-50. Factors for storage (NH3, N2O) might differ from stable.
# CH4 from storage uses the same B0, MCF principles but for remaining days.

def get_storage_emission_factors(storage_type_key: str, manure_system_type: str) -> Dict[str, float]:
    """
    Placeholder: Fetches emission factors for a given storage type.
    Example from page 48 (Hansen et al., 2018 for NH3; IPCC 2006 for N2O).
    Dybstrøelse storage has a 0.35 factor for proportion stored vs direct spread.
    """
    # This needs to be populated from relevant tables/sources for storage-specific EFs.
    # For gylle storage, NH3 EF (e.g. % of TAN), N2O EF (e.g. % of N).
    # For dybstrøelse storage, similar, plus the 0.35 factor application.
    print(f"Warning: Using placeholder/default EFs for storage {storage_type_key}, {manure_system_type}")
    if manure_system_type == 'gylle':
        return {'nh3_tan_percent_storage': 5.0, 'n2o_total_n_percent_storage': 0.5} # Example values
    elif manure_system_type == 'dybstrøelse':
        return {'nh3_total_n_percent_storage': 2.0, 'n2o_total_n_percent_storage': 0.5, 'proportion_stored_factor': 0.35}
    return {'nh3_tan_percent_storage': 0.0, 'nh3_total_n_percent_storage': 0.0, 'n2o_total_n_percent_storage': 0.0, 'proportion_stored_factor': 1.0}

def calculate_storage_nh3_kg_n_pr_dyr_pr_aar(
    tan_n_ab_stald_kg_pr_dyr: float,  # For gylle systems from calculate_manure_ab_stald
    total_n_ab_stald_kg_pr_dyr: float, # For dybstrøelse systems from calculate_manure_ab_stald
    storage_type_key: str,
    manure_system_type: str # 'gylle' or 'dybstrøelse' being stored
) -> float:
    """
    Calculates NH3-N emission from storage per animal per year.
    Markdown page 48.
    """
    ef_data = get_storage_emission_factors(storage_type_key, manure_system_type)
    nh3_n_kg = 0.0
    proportion_factor = ef_data.get('proportion_stored_factor', 1.0)

    if manure_system_type == 'gylle':
        nh3_n_kg = tan_n_ab_stald_kg_pr_dyr * (ef_data.get('nh3_tan_percent_storage', 0.0) / 100.0)
    elif manure_system_type == 'dybstrøelse':
        # 'NES_NH3, kg' = ('kg N dybstrøelse ab stald' x 'NH3, % af N' / 100) * 0,35
        nh3_n_kg = total_n_ab_stald_kg_pr_dyr * (ef_data.get('nh3_total_n_percent_storage', 0.0) / 100.0) * proportion_factor
    else:
        raise ValueError(f"Unknown manure_system_type for storage: {manure_system_type}")
    return nh3_n_kg

def calculate_storage_direct_n2o_kg_pr_dyr_pr_aar(
    total_n_ab_stald_kg_pr_dyr: float,
    storage_type_key: str,
    manure_system_type: str # 'gylle' or 'dybstrøelse' being stored
) -> float:
    """
    Calculates direct N2O emission (as kg N2O) from storage per animal per year.
    Markdown page 48-49.
    """
    ef_data = get_storage_emission_factors(storage_type_key, manure_system_type)
    proportion_factor = ef_data.get('proportion_stored_factor', 1.0)
    n2o_n_kg = 0.0

    if manure_system_type == 'gylle':
        # 'NES_N2O Kg N' = 'kg N i gylle ab stald' x 'N2O, % af N'/100
        n2o_n_kg = total_n_ab_stald_kg_pr_dyr * (ef_data.get('n2o_total_n_percent_storage', 0.0) / 100.0)
    elif manure_system_type == 'dybstrøelse':
        # 'NES_N2O Kg N' = ('kg N dybstrøelse ab stald' x 'N2O, % af N'/100)*0,35
        n2o_n_kg = total_n_ab_stald_kg_pr_dyr * (ef_data.get('n2o_total_n_percent_storage', 0.0) / 100.0) * proportion_factor
    else:
        raise ValueError(f"Unknown manure_system_type for storage: {manure_system_type}")

    n2o_kg = n2o_n_kg * MOL_WEIGHT_N2O_N_FACTOR # Convert N2O-N to N2O mass
    return n2o_kg

def calculate_storage_ch4_kg_pr_dyr_pr_aar(
    vs_goedning_kg_pr_dyr_pr_aar: float, # Annual VS from animal manure
    vs_stroelse_kg_pr_dyr_pr_aar: float, # Annual VS from bedding
    manure_system_type: str, # For selecting B0/MCF
    is_milk_cow: bool,
    days_in_storage_for_methane_calc: float = 182.5 # Default 50% split (365 - 182.5 stable days)
) -> float:
    """
    Calculates CH4 emission (as kg CH4) from storage per animal per year.
    Markdown page 49. Uses same B0, MCF as stable, but for remaining days.
    VS ab stald is assumed to be the initial VS if not otherwise modeled.
    """
    methane_efs = get_methane_emission_factors(manure_system_type, is_milk_cow)
    b0 = methane_efs['b0']
    # Assuming the MCF from Table 13 applies to the whole manure path and is split by time
    mcf_storage_percent = methane_efs['mcf_percent']

    vs_total_daily_kg = (vs_goedning_kg_pr_dyr_pr_aar / 365.0) + (vs_stroelse_kg_pr_dyr_pr_aar / 365.0)

    ch4_kg = vs_total_daily_kg * days_in_storage_for_methane_calc * b0 * (mcf_storage_percent / 100.0) * 0.67
    return ch4_kg

# Global GWP factors based on Markdown page 4 (to be used by calling functions)
THETA_N2O_CO2_GLOBAL = 298.0
THETA_CH4_CO2_GLOBAL = 25.0

# Further functions for stable direct N2O, stable CH4,
# manure ab stald characteristics, and storage emissions will follow.