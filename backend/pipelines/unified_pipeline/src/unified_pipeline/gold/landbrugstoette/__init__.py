"""
Gold layer processors for Danish agricultural subsidies (Landbrugsstøtte).

This package produces analysis-ready tables:
- landbrugstoette_eu_betalinger: EU CAP payments (EAGF + EAFRD)
- landbrugstoette_national: De minimis and slaughter premium
- landbrugstoette_tilsagn_arealer: Spatial subsidies (organic, grassland, environmental)
- landbrugstoette_cvr_oversigt: Materialized view for CVR lookup

Total coverage: ~8.2B DKK annual subsidies
"""

from .eu_betalinger import LandbrugstoetteEUGold, LandbrugstoetteEUGoldConfig
from .national_stoette import LandbrugstoetteNationalGold, LandbrugstoetteNationalGoldConfig
from .tilsagn_arealer import TilsagnArealerGold, TilsagnArealerGoldConfig

__all__ = [
    "LandbrugstoetteEUGold",
    "LandbrugstoetteEUGoldConfig",
    "LandbrugstoetteNationalGold",
    "LandbrugstoetteNationalGoldConfig",
    "TilsagnArealerGold",
    "TilsagnArealerGoldConfig",
]
