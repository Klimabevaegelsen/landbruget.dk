"""
Helper module for loading constants and emission factors.

This module provides convenient functions to access the centralized
constants and emission factors stored in JSON files.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional

# Get constants directory path
CONSTANTS_DIR = Path(__file__).parent

# Cache loaded data
_CACHE: Dict[str, Any] = {}


def load_gwp_factors() -> Dict[str, Any]:
    """
    Load Global Warming Potential factors from JSON file.

    Returns:
        dict: GWP factors including gwp_100, molecular_weights, and indirect_n2o_factors
    """
    if "gwp_factors" not in _CACHE:
        with open(CONSTANTS_DIR / "gwp_factors.json", "r") as f:
            _CACHE["gwp_factors"] = json.load(f)
    return _CACHE["gwp_factors"]


def load_emission_factors() -> Dict[str, Any]:
    """
    Load emission factors from JSON file.

    Returns:
        dict: Emission factors organized by source type (manure_storage, housing_emissions, etc.)
    """
    if "emission_factors" not in _CACHE:
        with open(CONSTANTS_DIR / "emission_factors.json", "r") as f:
            _CACHE["emission_factors"] = json.load(f)
    return _CACHE["emission_factors"]


def get_gwp(gas: str) -> float:
    """
    Get GWP-100 value for a greenhouse gas.

    Args:
        gas (str): Gas name ('CO2', 'CH4', or 'N2O')

    Returns:
        float: GWP-100 value

    Example:
        >>> get_gwp('CH4')
        28
    """
    factors = load_gwp_factors()
    return factors["gwp_100"][gas]


def get_molecular_weight_factor(factor_name: str) -> float:
    """
    Get molecular weight conversion factor.

    Args:
        factor_name (str): Factor name (e.g., 'N2O_N_factor', 'CO2_C_factor')

    Returns:
        float: Conversion factor value

    Example:
        >>> get_molecular_weight_factor('N2O_N_factor')
        1.5714
    """
    factors = load_gwp_factors()
    return factors["molecular_weights"][factor_name]


def get_mcf(storage_type: str) -> Dict[str, Any]:
    """
    Get Methane Conversion Factor for a storage type.

    Args:
        storage_type (str): Storage type ('gylle', 'dybstrøelse', 'afgræsning', etc.)

    Returns:
        dict: MCF information including value, unit, description, and reference

    Example:
        >>> mcf = get_mcf('gylle')
        >>> mcf['value']
        12.4
    """
    factors = load_emission_factors()
    return factors["manure_storage"]["mcf"][storage_type]


def get_b0_factor(animal_type: str) -> Dict[str, Any]:
    """
    Get B0 factor (maximum CH4 producing capacity) for an animal type.

    Args:
        animal_type (str): Animal type ('malkekøer', 'andre_kvæg')

    Returns:
        dict: B0 factor information including value, unit, description, and reference

    Example:
        >>> b0 = get_b0_factor('malkekøer')
        >>> b0['value']
        0.24
    """
    factors = load_emission_factors()
    return factors["manure_storage"]["b0_factors"][animal_type]


def get_nh3_emission_factor(source: str, sub_type: str) -> Dict[str, Any]:
    """
    Get NH3 emission factor for a specific source and sub-type.

    Args:
        source (str): Source category ('housing_emissions', 'field_application', 'grazing')
        sub_type (str): Specific type within the source

    Returns:
        dict: NH3 emission factor information

    Example:
        >>> ef = get_nh3_emission_factor('grazing', 'kvæg')
        >>> ef['value']
        0.14
    """
    factors = load_emission_factors()
    return factors[source]["nh3"][sub_type]


def get_n2o_emission_factor(source: str, sub_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Get N2O emission factor for a specific source and optional sub-type.

    Args:
        source (str): Source category ('housing_emissions', 'field_application', 'grazing')
        sub_type (str, optional): Specific type within the source

    Returns:
        dict: N2O emission factor information

    Example:
        >>> ef = get_n2o_emission_factor('housing_emissions', 'gylle')
        >>> ef['value']
        0.2
    """
    factors = load_emission_factors()
    if sub_type:
        return factors[source]["n2o"][sub_type]
    return factors[source]["n2o"]


def get_crop_residue_factors(crop: str, factor_type: str = "ipcc_2006_dry_matter") -> Dict[str, Any]:
    """
    Get crop residue calculation factors.

    Args:
        crop (str): Crop name (e.g., 'wheat', 'barley', 'maize')
        factor_type (str): Factor type ('ipcc_2006_dry_matter' or 'nitrogen_content')

    Returns:
        dict: Crop residue factors

    Example:
        >>> factors = get_crop_residue_factors('wheat')
        >>> factors['dry_content']
        0.89
    """
    factors = load_emission_factors()
    return factors["crop_residues"][factor_type][crop]


def clear_cache():
    """Clear the cached data. Use this if JSON files are updated during runtime."""
    _CACHE.clear()
