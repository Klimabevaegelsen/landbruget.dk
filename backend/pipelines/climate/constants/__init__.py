"""
Climate Tool Constants Package

This package provides centralized access to constants and emission factors
used throughout the climate tool calculations.

Quick access imports:
    from constants import get_gwp, get_mcf, get_b0_factor

JSON files:
    - gwp_factors.json: GWP values and molecular weight conversions
    - emission_factors.json: Danish-specific emission factors
"""

from .loader import (
    clear_cache,
    get_b0_factor,
    get_crop_residue_factors,
    get_gwp,
    get_mcf,
    get_molecular_weight_factor,
    get_n2o_emission_factor,
    get_nh3_emission_factor,
    load_emission_factors,
    load_gwp_factors,
)

__all__ = [
    "clear_cache",
    "get_b0_factor",
    "get_crop_residue_factors",
    "get_gwp",
    "get_mcf",
    "get_molecular_weight_factor",
    "get_n2o_emission_factor",
    "get_nh3_emission_factor",
    "load_emission_factors",
    "load_gwp_factors",
]
