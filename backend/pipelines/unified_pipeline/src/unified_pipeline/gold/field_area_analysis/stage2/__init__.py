"""Stage 2: Environmental Coverage Analysis

- Fields × BNBO water coverage analysis
- Fields × Wetland water coverage analysis

SPEED OPTIMIZATION: Uses pre-computed intersection geometries from Stage 1A/1B.
No longer recreates expensive spatial intersections - reuses Stage 1 results.
"""
