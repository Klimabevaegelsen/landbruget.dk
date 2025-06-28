# Gold layer module initialization

from .field_area_analysis import FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig
from .field_production import FieldProductionGold, FieldProductionGoldConfig
from .pesticide_disaggregation import PesticideDisaggregationGold, PesticideDisaggregationGoldConfig
from .property_cadastral_merge import PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig

# Export all gold processors and configs
__all__ = [
    "FieldAreaAnalysisGold",
    "FieldAreaAnalysisGoldConfig",
    "FieldProductionGold",
    "FieldProductionGoldConfig",
    "PropertyCadastralMergeGold",
    "PropertyCadastralMergeGoldConfig",
    "PesticideDisaggregationGold",
    "PesticideDisaggregationGoldConfig",
]
