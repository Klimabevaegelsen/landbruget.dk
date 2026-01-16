"""
Pig (Svin) emission calculation formulas.

Based on IPCC 2006 Guidelines.
"""

from .enterisk_metan import calculate_ch4_enteric_svin
from .stald_og_lager import calculate_manure_emissions_svin
from .foder import calculate_feed_emissions_svin

__all__ = [
    'calculate_ch4_enteric_svin',
    'calculate_manure_emissions_svin',
    'calculate_feed_emissions_svin',
]
