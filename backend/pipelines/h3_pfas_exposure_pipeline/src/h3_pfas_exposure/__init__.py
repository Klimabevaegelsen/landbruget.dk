"""
H3 PFAS Exposure Pipeline
========================

This pipeline creates H3-based PFAS exposure analysis by joining:
- Pesticide disaggregation data (from gold layer)
- Field geometries (from silver layer)
- BMD pesticide data with PFAS indicators (from silver layer)
- H3 hexagons at resolution 10 (~1.5 hectares per hexagon)

The pipeline follows the unified architecture with bronze/silver/gold layers
and uses the refactored H3 spatial processor for optimized performance.
"""

__version__ = "1.0.0"
