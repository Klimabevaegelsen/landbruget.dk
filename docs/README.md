# Documentation

Project documentation for Landbruget.dk covering data pipelines, frontend testing, operations guides, and troubleshooting.

## Directory Structure

```
docs/
├── PIPELINE_INDEX.md                          # Master index of all data pipelines
├── DATA_LINEAGE_COMPREHENSIVE.md              # End-to-end data provenance documentation
├── PIPELINE_DOCUMENTATION_PLAN.md             # Plan for journalist-friendly pipeline docs
├── FRONTEND_TESTING_INTEGRATION.md            # Pre-commit hook Playwright test setup
├── PLAYWRIGHT_REALTIME_TESTING_WORKFLOW.md     # Real-time Playwright MCP testing workflow
├── PMTILES_CACHING_OPTIMIZATION.md            # Cloudflare R2/CDN caching strategy for PMTiles
├── analysis/
│   ├── FIELD_PESTICIDE_DETAILS_IMPLEMENTATION_PLAN.md   # Adding pesticide product details to field analysis
│   ├── agricultural_cvr_identification_integration.md   # ML-based CVR identification for FVM fields
│   ├── cvr_pnumber_address_discovery.md                 # CVR P-number address API discovery
│   ├── financial_pipeline_fix_summary.md                # Fix for discarded XBRL financial data
│   └── parquet_to_supabase_schema_analysis.md           # Schema comparison: 159+ parquet datasets vs 22 Supabase tables
├── operations/
│   └── CHR_BACKFILL_FROM_GCS_GUIDE.md         # One-time backfill of historical CHR data from GCS
├── templates/
│   └── PIPELINE_README_TEMPLATE.md            # Template for journalist-friendly pipeline READMEs
└── troubleshooting/
    ├── MAPLIBRE_INTEGRATION_ISSUE.md           # MapLibre GL map dragging issue on /markanalyse
    ├── buildings_pmtiles_investigation.md      # Buildings not rendering due to CRS/spatial join issues
    └── company_page_missing_data_analysis.md   # Missing fields/addresses on company pages
```

## Key Entry Points

- **Pipeline overview**: Start with `PIPELINE_INDEX.md` for a full list of data pipelines and their status.
- **Data traceability**: See `DATA_LINEAGE_COMPREHENSIVE.md` for how data flows from government sources through Bronze/Silver/Gold layers.
- **Known issues**: Check `troubleshooting/` for investigated bugs with root causes and fixes.

## Adding New Documentation

1. Choose the appropriate subdirectory:
   - `analysis/` -- investigation notes, implementation plans, data discoveries
   - `operations/` -- runbooks and operational guides
   - `templates/` -- reusable document templates
   - `troubleshooting/` -- bug investigations with symptoms, root causes, and fixes
   - Root `docs/` -- cross-cutting topics (testing workflows, caching, pipeline index)

2. Use `SCREAMING_SNAKE_CASE.md` for filenames (matching existing convention) or `snake_case.md` for analysis docs.

3. For new pipeline documentation, use `templates/PIPELINE_README_TEMPLATE.md` as a starting point.
