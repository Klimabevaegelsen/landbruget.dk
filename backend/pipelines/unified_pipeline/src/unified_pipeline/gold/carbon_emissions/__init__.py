"""
Carbon Emissions Gold Layer Module

Farm-level carbon emission estimates from livestock, fields, and fertilizer data.
Consumes silver layer data (CHR livestock, FVM fields, fertiliser) and produces
gold layer output with CO2e emission reports per CVR.
"""

from .config import CarbonEmissionsGoldConfig
from .main import CarbonEmissionsGold

__all__ = [
    "CarbonEmissionsGold",
    "CarbonEmissionsGoldConfig",
]
