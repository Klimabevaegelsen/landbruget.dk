# supabase/

PostgreSQL 15 + PostGIS database managed via [Supabase](https://supabase.com). Contains schema migrations and Deno-based Edge Functions.

## Migrations

Migration files live in `migrations/` and follow the naming convention `YYYYMMDDHHMMSS_description.sql`. They are applied in chronological order.

### Workflow

```bash
supabase migration new <name>   # Create a new migration file
supabase db push                # Apply pending migrations to remote
supabase db pull                # Pull remote schema changes locally
supabase db reset               # Reset local database (destructive)
```

### What the migrations cover

- Tables and RPC functions for pesticide analysis, environmental compliance, land use, animal production, and financial data
- Materialized views for municipality-level summaries
- Support for GEUS Dataverse groundwater chemistry data (pesticides and PFAS)
- Farm carbon emissions and EU agricultural subsidy (landbrugstoette) tables
- Incremental processing infrastructure

## Edge Functions

Deno-based serverless functions deployed to Supabase. Located in `functions/`.

### `homepage-rankings`

Returns company-level rankings across multiple categories for the homepage. Supports filtering by category and an in-memory cache (1-week TTL). Categories:

- **financial** - profit, assets, employee count
- **field** - land area, organic area/percentage, field count
- **environment** - pesticide burden (total, PFAS, glyphosate, diquat), BNBO status, wetland restoration status
- **animal** - pig/cattle production capacity, antibiotic usage, production site count, animal transport volumes
- **worker** - employee count, foreign worker visa applications, work injuries, workplace inspections, urgent violations

Query parameters: `category`, `limit` (max 50), `rankingId`.

### `municipality-rankings`

Returns municipality-level rankings. Currently serves two active categories:

- **land_use** - total agricultural area, field count, organic percentage, unique companies/crops
- **production** - total animal capacity, production site count

Environmental, animal health, and worker safety categories are temporarily disabled pending replacement with more meaningful metrics.

Query parameters: `category`, `year`, `limit`, `sort_by`, `sort_direction`.

## Notes

- **RLS**: Row Level Security is enabled on all tables. Most tables use a public read policy since this is a transparency project.
- **CRS**: All geometry in Supabase is stored as EPSG:4326 (WGS84). Data is processed in EPSG:25832 (UTM 32N) and transformed to 4326 only at the final upload step.
- **Identifiers**: Tables join on CVR (company), CHR (herd), BFE (cadastral), or geospatial coordinates.
