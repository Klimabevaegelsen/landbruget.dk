# Architecture

## Monorepo Structure

```
frontend/        # Next.js 16 (App Router, React 19, TypeScript)
backend/         # Python data pipelines (medallion architecture)
  pipelines/     # Individual data pipelines (unified, chr, api_export, etc.)
  common/        # Shared utilities (storage, crs_utils)
supabase/        # Historical migrations (database mostly deprecated)
docs/            # Documentation, troubleshooting, pipeline index
scripts/         # Utility scripts
```

## Core Principles

1. **Data-Centric**: All data joinable on CVR (company), CHR (herd), BFE (cadastral), or geospatial
2. **Medallion**: Bronze (raw, immutable) → Silver (cleaned) → Gold (analysis-ready)
3. **Separation**: Backend = data pipelines + R2 export, Frontend = visualization via R2 CDN JSON
4. **TDD**: Write test first, confirm fail, implement minimum, confirm pass, refactor

## Key Data Sources (18+)

- Landbrugsstyrelsen (field boundaries, crop data)
- CHR Registry (livestock tracking)
- Geodatastyrelsen (cadastre, land ownership)
- Miljøstyrelsen (pesticides, compliance)
- Danmarks Statistik (agricultural statistics)
- DMI (weather/climate data)

Full list: `docs/PIPELINE_INDEX.md`

## Frontend Architecture

- App Router with Server Components (default) and Client Components (`'use client'`)
- Zustand for global state (map viewport, filters, selections)
- MapLibre GL + PMTiles for geospatial visualization
- Radix UI primitives for accessible components
- R2 CDN for data fetching (pre-computed JSON via `services/data/config.ts`)

## Backend Architecture

- Each pipeline: `main.py` → `bronze/` → `silver/` → `gold/`
- DuckDB for large file processing (replaces in-memory Pandas for big datasets)
- R2 for data storage (bronze/silver/gold parquet layers)
- `api_export` pipeline reads gold parquet → writes JSON to R2 CDN for frontend
- GitHub Actions for pipeline orchestration (weekly/monthly schedules)

## Data Serving (R2 CDN)

- Pre-computed JSON files on Cloudflare R2, served via `NEXT_PUBLIC_DATA_URL`
- `api_export` pipeline generates: company profiles, rankings, municipality data, pesticide analysis
- Frontend API routes in `app/api/data/` cache and proxy R2 JSON
- Server-side caching via `unstable_cache` with weekly revalidation
