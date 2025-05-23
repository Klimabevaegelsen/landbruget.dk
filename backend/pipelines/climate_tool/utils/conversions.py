"""
Utility functions for converting between different greenhouse gases and CO2e.
These conversion factors are based on IPCC AR6 (2021) Global Warming Potentials (GWP).
"""

# Global Warming Potentials (GWP) from IPCC AR6 (2021)
# These are the 100-year time horizon values
CH4_GWP = 27.0  # kg CO2e per kg CH4
N2O_GWP = 273.0  # kg CO2e per kg N2O

def ch4_to_co2e(ch4_kg: float, config) -> float:
    """
    Convert CH4 mass to CO2 equivalent using IPCC AR6 (2021) GWP.

    Args:
        ch4_kg (float): Mass of CH4 in kg
        config: ConfigLoader instance to get GWP factors

    Returns:
        float: CO2 equivalent in kg
    """
    gwp = config.get_factor('gwp_factors', 'gwp.ch4')
    return ch4_kg * gwp

def n2o_to_co2e(n2o_kg: float, config) -> float:
    """
    Convert N2O mass to CO2 equivalent using IPCC AR6 (2021) GWP.

    Args:
        n2o_kg (float): Mass of N2O in kg
        config: ConfigLoader instance to get GWP factors

    Returns:
        float: CO2 equivalent in kg
    """
    gwp = config.get_factor('gwp_factors', 'gwp.n2o')
    return n2o_kg * gwp

def nh3_to_indirect_co2e(nh3_n_kg: float, config) -> float:
    """
    Convert NH3-N mass to indirect N2O emissions and then to CO2 equivalent.
    Uses IPCC methodology for indirect N2O from atmospheric deposition.

    Args:
        nh3_n_kg (float): Mass of NH3-N in kg
        config: ConfigLoader instance to get conversion factors

    Returns:
        float: CO2 equivalent in kg
    """
    # Get conversion factors
    n2o_ef = config.get_factor('gwp_factors', 'indirect_n2o_factors.atmospheric_deposition')
    n2o_mw = config.get_factor('gwp_factors', 'molecular_weights.n2o')
    n2_mw = config.get_factor('gwp_factors', 'molecular_weights.n2')

    # Calculate N2O emissions
    n2o_n_kg = nh3_n_kg * n2o_ef
    n2o_kg = n2o_n_kg * (n2o_mw / n2_mw)

    # Convert to CO2e
    return n2o_to_co2e(n2o_kg, config)