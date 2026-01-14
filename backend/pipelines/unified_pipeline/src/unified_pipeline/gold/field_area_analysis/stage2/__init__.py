"""Stage 2: Environmental Coverage Analysis

- Fields × BNBO water coverage analysis
- Fields × Wetland water coverage analysis
- Fields × Grukos (groundwater action areas) analysis

SPEED OPTIMIZATION: Uses pre-computed intersection geometries from Stage 1A/1B.
No longer recreates expensive spatial intersections - reuses Stage 1 results.
"""

from .fields_bnbo_water import FieldsBNBOWaterCoverage
from .fields_grukos import FieldsGrukosCoverage
from .fields_wetland_water import FieldsWetlandWaterCoverage

__all__ = ["FieldsBNBOWaterCoverage", "FieldsWetlandWaterCoverage", "FieldsGrukosCoverage"]
