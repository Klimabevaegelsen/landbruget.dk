"""
Utility functions for converting between different greenhouse gases and CO2e.
These conversion factors are loaded from centralized constants files.
"""

import json
import os
from pathlib import Path

# Get constants directory path
CONSTANTS_DIR = Path(__file__).parent.parent / "constants"


# Load GWP factors from JSON
def load_gwp_factors():
    """Load Global Warming Potential factors from JSON file."""
    gwp_file = CONSTANTS_DIR / "gwp_factors.json"
    with open(gwp_file, "r") as f:
        return json.load(f)


# Cache the loaded factors
_GWP_FACTORS = load_gwp_factors()

# Global Warming Potentials (GWP) from IPCC AR5
# These are the 100-year time horizon values
CH4_GWP = _GWP_FACTORS["gwp_100"]["CH4"]  # kg CO2e per kg CH4
N2O_GWP = _GWP_FACTORS["gwp_100"]["N2O"]  # kg CO2e per kg N2O
CO2_GWP = _GWP_FACTORS["gwp_100"]["CO2"]  # kg CO2e per kg CO2

# Molecular weight conversion factors
N2O_MW = _GWP_FACTORS["molecular_weights"]["N2O"]  # 44.0
N2_MW = _GWP_FACTORS["molecular_weights"]["N2"]  # 28.0
N2O_N_FACTOR = _GWP_FACTORS["molecular_weights"]["N2O_N_factor"]  # 44/28 = 1.5714
NOX_N_FACTOR = _GWP_FACTORS["molecular_weights"]["NOx_N_factor"]  # 46/14 = 3.2857
CO2_C_FACTOR = _GWP_FACTORS["molecular_weights"]["CO2_C_factor"]  # 44/12 = 3.6667

# Indirect N2O emission factors
INDIRECT_N2O_ATMOSPHERIC = _GWP_FACTORS["indirect_n2o_factors"]["atmospheric_deposition"]  # 0.01
INDIRECT_N2O_LEACHING = _GWP_FACTORS["indirect_n2o_factors"]["leaching_runoff"]  # 0.0075


def ch4_to_co2e(ch4_kg: float, config=None) -> float:
    """
    Convert CH4 mass to CO2 equivalent using IPCC AR5 GWP.

    Args:
        ch4_kg (float): Mass of CH4 in kg
        config: Optional ConfigLoader instance (for backwards compatibility)

    Returns:
        float: CO2 equivalent in kg
    """
    if config:
        # Use config if provided (for backwards compatibility)
        try:
            gwp = config.get_factor("gwp_factors", "gwp_100.CH4")
            return ch4_kg * gwp
        except:
            pass
    # Use loaded constant
    return ch4_kg * CH4_GWP


def n2o_to_co2e(n2o_kg: float, config=None) -> float:
    """
    Convert N2O mass to CO2 equivalent using IPCC AR5 GWP.

    Args:
        n2o_kg (float): Mass of N2O in kg
        config: Optional ConfigLoader instance (for backwards compatibility)

    Returns:
        float: CO2 equivalent in kg
    """
    if config:
        # Use config if provided (for backwards compatibility)
        try:
            gwp = config.get_factor("gwp_factors", "gwp_100.N2O")
            return n2o_kg * gwp
        except:
            pass
    # Use loaded constant
    return n2o_kg * N2O_GWP


def co2_to_co2e(co2_kg: float) -> float:
    """
    Convert CO2 mass to CO2 equivalent (identity function for completeness).

    Args:
        co2_kg (float): Mass of CO2 in kg

    Returns:
        float: CO2 equivalent in kg (same as input)
    """
    return co2_kg * CO2_GWP


def nh3_to_indirect_co2e(nh3_n_kg: float, config=None) -> float:
    """
    Convert NH3-N mass to indirect N2O emissions and then to CO2 equivalent.
    Uses IPCC methodology for indirect N2O from atmospheric deposition.

    Args:
        nh3_n_kg (float): Mass of NH3-N in kg
        config: Optional ConfigLoader instance (for backwards compatibility)

    Returns:
        float: CO2 equivalent in kg
    """
    if config:
        # Use config if provided (for backwards compatibility)
        try:
            n2o_ef = config.get_factor("gwp_factors", "indirect_n2o_factors.atmospheric_deposition")
            n2o_mw = config.get_factor("gwp_factors", "molecular_weights.N2O")
            n2_mw = config.get_factor("gwp_factors", "molecular_weights.N2")

            # Calculate N2O emissions
            n2o_n_kg = nh3_n_kg * n2o_ef
            n2o_kg = n2o_n_kg * (n2o_mw / n2_mw)

            # Convert to CO2e
            return n2o_to_co2e(n2o_kg, config)
        except:
            pass

    # Use loaded constants
    # Calculate N2O emissions from atmospheric deposition
    n2o_n_kg = nh3_n_kg * INDIRECT_N2O_ATMOSPHERIC
    n2o_kg = n2o_n_kg * N2O_N_FACTOR

    # Convert to CO2e
    return n2o_to_co2e(n2o_kg)


def n_to_n2o(n_kg: float) -> float:
    """
    Convert N mass to N2O mass using molecular weight conversion.

    Args:
        n_kg (float): Mass of N in kg

    Returns:
        float: Mass of N2O in kg
    """
    return n_kg * N2O_N_FACTOR


def c_to_co2(c_kg: float) -> float:
    """
    Convert C mass to CO2 mass using molecular weight conversion.

    Args:
        c_kg (float): Mass of C in kg

    Returns:
        float: Mass of CO2 in kg
    """
    return c_kg * CO2_C_FACTOR
