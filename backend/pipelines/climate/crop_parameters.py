"""
Manual crop code to emission parameters mapping.

Maps FVM crop codes to IPCC emission parameters from Tables 23-25.
Used for crop residue and carbon balance calculations.

Parameters:
- slope: Above-ground residue slope (Table 23)
- intercept: Above-ground residue intercept in tonnes ts (Table 23)
- n_over: N content above-ground residue kg N/kg ts (Table 24)
- n_under: N content below-ground residue kg N/kg ts (Table 24)
- f_under: Below-ground factor (Table 25)
- dry_matter: Dry matter content of harvested product (Table 23)
"""

from typing import Any

# Manual mapping by crop_code - exact matches only
# Source: pesticide_compliance.py crop codes + IPCC Tables 23-25
CROP_PARAMETERS: dict[int, dict[str, Any]] = {
    # Cereals - Barley
    1: {
        "name": "Vårbyg",
        "slope": 0.95,
        "intercept": 0.29,  # tonnes ts
        "n_over": 0.007,
        "n_under": 0.014,
        "f_under": 0.22,
        "dry_matter": 0.89,
    },
    10: {
        "name": "Vinterbyg",
        "slope": 0.95,
        "intercept": 0.29,
        "n_over": 0.007,
        "n_under": 0.014,
        "f_under": 0.22,
        "dry_matter": 0.89,
    },
    # Cereals - Wheat
    2: {
        "name": "Vårhvede",
        "slope": 1.36,
        "intercept": 0.46,
        "n_over": 0.006,
        "n_under": 0.009,
        "f_under": 0.28,
        "dry_matter": 0.89,
    },
    11: {
        "name": "Vinterhvede",
        "slope": 1.61,
        "intercept": 0.40,
        "n_over": 0.006,
        "n_under": 0.009,
        "f_under": 0.23,
        "dry_matter": 0.89,
    },
    # Cereals - Oats
    3: {
        "name": "Havre",
        "slope": 0.91,
        "intercept": 0.89,
        "n_over": 0.007,
        "n_under": 0.008,
        "f_under": 0.25,
        "dry_matter": 0.89,
    },
    # Cereals - Rye
    14: {
        "name": "Vinterrug",
        "slope": 1.09,
        "intercept": 0.88,
        "n_over": 0.005,
        "n_under": 0.011,
        "f_under": 0.22,
        "dry_matter": 0.88,
    },
    # Cereals - Triticale (use rye/grains parameters)
    16: {
        "name": "Triticale",
        "slope": 1.09,
        "intercept": 0.88,
        "n_over": 0.006,
        "n_under": 0.009,
        "f_under": 0.22,
        "dry_matter": 0.88,
    },
    # Cereals - Maize
    5: {
        "name": "Majs",
        "slope": 1.03,
        "intercept": 0.61,
        "n_over": 0.006,
        "n_under": 0.007,
        "f_under": 0.22,
        "dry_matter": 0.87,
    },
    216: {
        "name": "Majs (silomajs)",
        "slope": 1.03,
        "intercept": 0.61,
        "n_over": 0.006,
        "n_under": 0.007,
        "f_under": 0.22,
        "dry_matter": 0.87,
    },
    218: {
        "name": "Majs (kolbemajs)",
        "slope": 1.03,
        "intercept": 0.61,
        "n_over": 0.006,
        "n_under": 0.007,
        "f_under": 0.22,
        "dry_matter": 0.87,
    },
    # Oil crops - Rapeseed (use beans & pulses parameters)
    21: {
        "name": "Vårraps",
        "slope": 1.13,
        "intercept": 0.85,
        "n_over": 0.008,
        "n_under": 0.008,
        "f_under": 0.19,
        "dry_matter": 0.91,
    },
    22: {
        "name": "Vinterraps",
        "slope": 1.13,
        "intercept": 0.85,
        "n_over": 0.008,
        "n_under": 0.008,
        "f_under": 0.19,
        "dry_matter": 0.91,
    },
    # Legumes - Peas and Beans
    30: {
        "name": "Markærter",
        "slope": 1.13,
        "intercept": 0.85,
        "n_over": 0.008,
        "n_under": 0.008,
        "f_under": 0.19,
        "dry_matter": 0.91,
    },
    31: {
        "name": "Hestebønner",
        "slope": 1.13,
        "intercept": 0.85,
        "n_over": 0.008,
        "n_under": 0.008,
        "f_under": 0.19,
        "dry_matter": 0.91,
    },
    # Root crops - Potato
    151: {
        "name": "Kartofler",
        "slope": 0.39,
        "intercept": 1.06,
        "n_over": 0.019,
        "n_under": 0.014,
        "f_under": 0.20,
        "dry_matter": 0.22,
    },
    # Root crops - Sugar beet
    160: {
        "name": "Sukkerroer",
        "slope": 1.07,
        "intercept": 1.54,
        "n_over": 0.016,
        "n_under": 0.014,
        "f_under": 0.20,
        "dry_matter": 0.94,
    },
    # Grass/Forage - Perennial grasses
    252: {
        "name": "Græs (permanent)",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.015,
        "n_under": 0.012,
        "f_under": 0.54,
        "dry_matter": 0.90,
    },
    254: {
        "name": "Græs (omdrift)",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.015,
        "n_under": 0.012,
        "f_under": 0.54,
        "dry_matter": 0.90,
    },
    263: {
        "name": "Græs",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.015,
        "n_under": 0.012,
        "f_under": 0.54,
        "dry_matter": 0.90,
    },
    # Grass/Forage - Grass-clover mixtures
    260: {
        "name": "Kløvergræs",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.025,
        "n_under": 0.016,
        "f_under": 0.80,
        "dry_matter": 0.90,
    },
    276: {
        "name": "Kløvergræs (omdrift)",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.025,
        "n_under": 0.016,
        "f_under": 0.80,
        "dry_matter": 0.90,
    },
    # Seed production crops
    101: {
        "name": "Alm. rajgræs til frøavl",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.015,
        "n_under": 0.012,
        "f_under": 0.54,
        "dry_matter": 0.90,
    },
    108: {
        "name": "Rødsvingel til frøavl",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.015,
        "n_under": 0.012,
        "f_under": 0.54,
        "dry_matter": 0.90,
    },
    120: {
        "name": "Hvidkløver til frøavl",
        "slope": 0.30,
        "intercept": 0.00,
        "n_over": 0.025,
        "n_under": 0.016,
        "f_under": 0.80,
        "dry_matter": 0.90,
    },
}


def get_crop_params(crop_code: int) -> dict[str, Any] | None:
    """
    Returns emission parameters for exact crop code match.

    Args:
        crop_code: FVM crop code (numeric)

    Returns:
        Dict with parameters (slope, intercept, n_over, n_under, f_under, dry_matter)
        or None if crop code not in mapping
    """
    return CROP_PARAMETERS.get(crop_code)


def get_crop_params_by_name(crop_name: str) -> dict[str, Any] | None:
    """
    Fallback: Returns emission parameters by exact crop name match.

    Args:
        crop_name: Danish crop name (e.g., "Vinterhvede")

    Returns:
        Dict with parameters or None if not found
    """
    for params in CROP_PARAMETERS.values():
        if params["name"] == crop_name:
            return params
    return None


def list_supported_crops() -> list:
    """Returns list of supported crop codes and names."""
    return [(code, params["name"]) for code, params in CROP_PARAMETERS.items()]
