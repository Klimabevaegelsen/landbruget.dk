# Architecture

## Monorepo Structure

```
frontend/        # Next.js 15 (App Router, React 19, TypeScript)
backend/         # Python data pipelines (medallion architecture)
  pipelines/     # Individual data pipelines (unified, chr, svineflytning, etc.)
  common/        # Shared utilities (gcs_utils, supabase_utils, crs_utils)
  api/           # FastAPI endpoints (when needed)
supabase/        # Migrations and Edge Functions
docs/            # Documentation, troubleshooting, pipeline index
scripts/         # Utility scripts
```

## Core Principles

1. **Data-Centric**: All data joinable on CVR (company), CHR (herd), BFE (cadastral), or geospatial
2. **Medallion**: Bronze (raw, immutable) → Silver (cleaned) → Gold (analysis-ready)
3. **Separation**: Backend = data pipelines, Frontend = visualization, Supabase = storage + RLS
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
- Supabase client for data fetching

## Backend Architecture

- Each pipeline: `main.py` → `bronze/` → `silver/` → `gold/`
- DuckDB for large file processing (replaces in-memory Pandas for big datasets)
- GCS for data storage (bronze/silver/gold layers)
- Supabase for final queryable data (Gold layer upload)
- GitHub Actions for pipeline orchestration (weekly/monthly schedules)

## Database

- PostgreSQL 15 + PostGIS on Supabase
- RLS enabled on all tables
- Materialized views for complex aggregations
- Indexes on CVR, CHR, BFE columns
- All schema changes via `supabase/migrations/`
