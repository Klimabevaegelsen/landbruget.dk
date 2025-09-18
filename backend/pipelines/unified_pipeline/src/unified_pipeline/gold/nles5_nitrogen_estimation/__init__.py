"""
NLES5 Nitrogen Estimation Gold Layer Module

This module provides comprehensive nitrogen washout estimation using the NLES5 model
with real climate data, soil types, and fertilizer application data.

The module is organized into several components:
- config: Configuration classes and parameters
- parameters: NLES5 model parameters and constants  
- data_loader: Data loading and preprocessing utilities
- climate_processor: Climate data processing and tessellation
- spatial_operations: Spatial joins and optimization
- nles5_calculator: Core NLES5 calculations
- validator: Data validation and quality assurance
- memory_utils: Memory management utilities
"""

from .config import NLES5NitrogenEstimationGoldConfig
from .main import NLES5NitrogenEstimationGold

__all__ = [
    'NLES5NitrogenEstimationGoldConfig',
    'NLES5NitrogenEstimationGold',
]
