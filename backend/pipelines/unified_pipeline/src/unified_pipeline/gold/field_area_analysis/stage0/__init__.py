"""Stage 0: Pre-filtering (Probe Size Reduction)

This stage dramatically reduces the size of all datasets by pre-filtering to only include
geometries that intersect with agricultural fields. This optimization is critical for
performance on GitHub Actions runners with limited memory.

PERFORMANCE OPTIMIZATION STRATEGY:
- Fields (600K) as BUILD side for spatial indexing
- Pre-filter Properties: 6.5M → ~500K (90%+ reduction)
- Pre-filter BNBO: 3.7K → ~1K (estimated 70% reduction)
- Pre-filter Wetlands: 1.6M → ~200K (estimated 85% reduction)
- Pre-filter Water Projects: ~2.4K → ~500 (estimated 80% reduction)

This reduces subsequent stage complexity from:
- Stage 1: 600K × 6.5M = 3.9B combinations → 600K × 500K = 300M combinations (13x reduction)
- Stage 2: Field × Environmental joins become much more manageable
- Stage 3: Property-level analysis operates on pre-filtered datasets

All subsequent stages will use these pre-filtered datasets as input.
"""
