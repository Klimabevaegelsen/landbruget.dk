"""
Silver layer processors for Danish agricultural subsidies.

This package handles:
- stoetteoplysninger: EU CAP payment data (EAGF + EAFRD)
- deminimis: National de minimis aid
- slagtepraemie: Slaughter premium (voluntary coupled support)
"""

from .deminimis import DeminimisSilver, DeminimisSilverConfig
from .slagtepraemie import SlagtepraemieSilver, SlagtepraemieSilverConfig
from .stoetteoplysninger import StoetteoplysningerSilver, StoetteoplysningerSilverConfig

__all__ = [
    "StoetteoplysningerSilver",
    "StoetteoplysningerSilverConfig",
    "DeminimisSilver",
    "DeminimisSilverConfig",
    "SlagtepraemieSilver",
    "SlagtepraemieSilverConfig",
]
