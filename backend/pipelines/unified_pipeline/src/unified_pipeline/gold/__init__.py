# Gold layer module initialization

# Note: field_area_analysis is now a multi-stage pipeline package
# Temporarily commented out to test new multi-stage pipeline
# from .field_area_analysis_redesigned import FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig
from .field_production import FieldProductionGold, FieldProductionGoldConfig
from .pesticide_disaggregation import PesticideDisaggregationGold, PesticideDisaggregationGoldConfig
from .pesticide_proximity import PesticideProximityGold, PesticideProximityGoldConfig
from .property_cadastral_merge import PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig
from .nles5_nitrogen_estimation import NLES5NitrogenEstimationGold, NLES5NitrogenEstimationGoldConfig
from .work_permits import WorkPermitsGold, WorkPermitsGoldConfig
from .worker_safety import WorkerSafetyGold, WorkerSafetyGoldConfig

# Export all gold processors and configs
__all__ = [
    # "FieldAreaAnalysisGold",
    # "FieldAreaAnalysisGoldConfig",
    "FieldProductionGold",
    "FieldProductionGoldConfig",
    "PropertyCadastralMergeGold",
    "PropertyCadastralMergeGoldConfig",
    "PesticideDisaggregationGold",
    "PesticideDisaggregationGoldConfig",
    "NLES5NitrogenEstimationGold",
    "NLES5NitrogenEstimationGoldConfig",
    "PesticideProximityGold",
    "PesticideProximityGoldConfig",
    "WorkerSafetyGold",
    "WorkerSafetyGoldConfig",
    "WorkPermitsGold",
    "WorkPermitsGoldConfig",
]
