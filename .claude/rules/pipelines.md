# Pipeline Development Rules

## Running Pipelines

```bash
cd backend && source venv/bin/activate   # Always activate first
cd pipelines/<name> && python main.py    # Run specific pipeline
```

## Key Pipelines

| Pipeline | Purpose | Schedule |
|----------|---------|----------|
| `unified_pipeline` | 18+ Danish govt data sources | Weekly (Mon 2 AM UTC) |
| `chr_pipeline` | Livestock tracking (CHR registry) | Weekly |
| `svineflytning_pipeline` | Pig movement tracking | Weekly |
| `drive_data_pipeline` | Regulatory compliance data | On demand |
| `bmd_scraper` | Pesticide database | On demand |

## Pipeline Debugging

1. Verify `.env` variables are set (check `.env.example` in pipeline dir)
2. Check GCS credentials path exists
3. Activate venv: `cd backend && source venv/bin/activate`
4. Check `docs/troubleshooting/` for known issues
5. Check pipeline-specific README: `backend/pipelines/<name>/README.md`

## Common Pipeline Failures

- **GCS auth**: Set `GOOGLE_APPLICATION_CREDENTIALS` or `GCS_CREDENTIALS`
- **Memory**: Use DuckDB or chunked processing for large files
- **DuckDB 1.5 breaking changes**: Use `delim` not `DELIMITER`, pin `>=1.5.0`
- **Geometry errors**: Wrap with `TRY()` for DuckDB 1.5, ensure EPSG:4326 for Supabase

## Cost Awareness

- Cache GCS files locally when testing repeatedly
- Use `gsutil -m` for parallel transfers
- Avoid re-uploading unchanged files
- Be mindful of egress costs on large datasets

## Pipeline Dependency Graph

**The full dependency graph is defined in `pipeline_dependencies.yml` at the repo root.**

When modifying or running any pipeline, always check downstream dependencies.

### Cascade Chains (what to re-run when X changes)

```
unified_pipeline →
  field_area_analysis, field_production, pesticide_disaggregation
  → pesticide_proximity, pesticide_compliance, h3_pfas_analysis
  → generate_pmtiles → pmtiles_cache_warmup

bmd_scraper →
  pesticide_disaggregation
  → pesticide_proximity, pesticide_compliance, h3_pfas_analysis
  → generate_pmtiles → pmtiles_cache_warmup

drive_data_pipeline →
  property_cadastral_merge → field_area_analysis
  → generate_pmtiles → pmtiles_cache_warmup

bbr_buildings_pipeline →
  pesticide_proximity
  → generate_pmtiles → pmtiles_cache_warmup

chr_pipeline, svineflytning_pipeline, dma_scraper → (no downstream)
```

### Rules

1. **When modifying pipeline code**: After changes, list all downstream pipelines that need re-running. Check `pipeline_dependencies.yml` for the full chain. Offer to trigger them via `gh workflow run <workflow>.yml --ref main`.
2. **When a pipeline fails in CI**: Warn that all downstream data is now stale. List affected pipelines.
3. **When running /run-pipeline**: After execution, show downstream pipelines and offer to trigger them.
4. **When reviewing PRs touching pipeline code**: If data contracts changed (output schema, GCS paths), verify downstream consumers are updated too.
5. **Cross-pipeline dependencies**: `pesticide_disaggregation` needs BOTH `unified_pipeline` (fvm_marker) AND `bmd_scraper` (pesticides). Both must be fresh.
6. **Y+1 pattern**: Pesticide year Y uses field data from year Y+1. When re-running pesticide pipelines for year Y, ensure fvm_marker_{Y+1} is available.
