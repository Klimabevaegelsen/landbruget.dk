# Supabase — Landbruget.dk

PostgreSQL 15 + PostGIS database. 23 migrations, 2 edge functions. All schema changes MUST go through migration files.

## Commands

```bash
supabase migration new <name>    # Create new migration
supabase db push                 # Push migrations to remote
supabase db pull                 # Pull remote schema
supabase db reset                # Reset local (DESTRUCTIVE)
supabase functions serve         # Local edge function dev
supabase functions deploy <name> # Deploy edge function
```

## Migration Naming

Format: `YYYYMMDDHHMMSS_description.sql`
Files are applied in alphabetical order — timestamp prefix ensures correct sequencing.
Always check existing files (`ls migrations/`) before choosing a timestamp.

## Edge Functions (`functions/`)

### `homepage-rankings/index.ts` (~1700 lines)
25 ranking tables across 5 categories (financial, field, environment, animal, worker).
In-memory cache with 1-week TTL. Query params: `category`, `limit` (max 50), `rankingId`.

### `municipality-rankings/index.ts` (~250 lines)
Municipality-level rankings. Only `land_use` and `production` active — environmental, animal_health, and worker_safety are **temporarily disabled** (views deleted due to nonsensical composite scores).

Both use:
```typescript
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
```

## Schema Conventions

- **RLS** enabled on ALL tables with public read: `USING (true)` + `GRANT SELECT` to anon, authenticated
- **CVR validation**: CHECK constraint `^\d{8}$` on all CVR columns
- **Timestamps**: `created_at TIMESTAMPTZ DEFAULT NOW()`, `updated_at` with trigger
- **Geometry**: Always `GEOMETRY(Point/MultiPolygon, 4326)` — pipelines transform from EPSG:25832 on upload
- **Indexes**: GIST on geometry, btree on CVR/CHR/year/codes, composite indexes for common query patterns
- **Materialized views**: For aggregations — need explicit `REFRESH` via dedicated functions
- **Comments**: `COMMENT ON TABLE/COLUMN/VIEW` for documentation

## Key Tables (recent migrations)

- `geus_dataverse_pesticides` — 633 substances, 4.2M+ analyses (1981-2025)
- `geus_dataverse_pfas` — 26 PFAS substances, 397k+ analyses (2012-2025)
- `landbrugstoette_eu_betalinger` — EU CAP payments (Pillar 1 + 2)
- `landbrugstoette_tilsagn_arealer` — Spatial subsidies with geometry
- `farm_carbon_emissions` — Farm-level carbon emission calculations

## Gotchas

- Never modify data directly — always use migrations
- Edge functions use Deno imports (pinned `@0.168.0` for std, `@2` for supabase-js)
- Edge function responses MUST include `corsHeaders` spread in headers
- Materialized views need manual refresh — call `refresh_*_views()` functions
- Some migrations share timestamps (e.g., both `20250103000002_*`) — check for conflicts
- `landbrugstoette_cvr_oversigt` view excludes `is_summary_row = TRUE` to prevent double-counting
