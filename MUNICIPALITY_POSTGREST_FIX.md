# Municipality Rankings PostgREST Fix

## The Problem

After 1+ hours of troubleshooting, the municipality rankings page at `/kommuner` was returning a 500 error:

```
column municipality_land_use_summary.avg_n_leached_kg does not exist
```

**The Root Cause:** PostgreSQL's `information_schema` does NOT expose MATERIALIZED VIEWS!

## Technical Details

### How PostgREST Discovers Tables

1. PostgREST (used by Supabase JS Client) introspects database schema using `information_schema.tables`
2. `information_schema.tables` only shows:
   - `BASE TABLE` (regular tables)
   - `VIEW` (regular views)
   - `FOREIGN TABLE` (foreign data wrappers)
3. **MATERIALIZED VIEWS are NOT included** in `information_schema.tables`

### Verification

```sql
-- This returns the actual columns
SELECT attname FROM pg_attribute 
WHERE attrelid = 'municipality_land_use_summary'::regclass;
-- ✅ Returns 15 columns including avg_n_leached_kg

-- This is what PostgREST sees
SELECT * FROM information_schema.columns 
WHERE table_name = 'municipality_land_use_summary';
-- ❌ Returns 0 rows!

-- PostgREST cannot see it
SELECT * FROM information_schema.tables 
WHERE table_name LIKE 'municipality%';
-- ❌ Returns 0 rows!
```

### Why NOTIFY Didn't Work

We sent `NOTIFY pgrst, 'reload schema'` dozens of times, but it didn't help because:
- Event triggers (`pgrst_ddl_watch`, `pgrst_drop_watch`) correctly fired
- PostgREST correctly reloaded its schema cache
- But `information_schema` never exposed the materialized views in the first place!

## The Solution

Create regular VIEWs that wrap the materialized views:

```sql
-- Create VIEWs (not materialized) that PostgREST can see
CREATE VIEW v_municipality_land_use_summary AS
SELECT * FROM municipality_land_use_summary;

CREATE VIEW v_municipality_production_summary AS
SELECT * FROM municipality_production_summary;

-- Grant permissions
GRANT SELECT ON v_municipality_land_use_summary TO anon, authenticated, service_role;
GRANT SELECT ON v_municipality_production_summary TO anon, authenticated, service_role;
```

### Verification After Fix

```sql
SET ROLE anon;
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_name LIKE 'v_municipality%';
-- ✅ Returns 2 rows with table_type = 'VIEW'
```

## Implementation

### 1. Database Changes (Applied)

- Created `v_municipality_land_use_summary` view
- Created `v_municipality_production_summary` view
- Granted SELECT permissions to anon, authenticated, service_role

### 2. Edge Function Changes (Applied)

Updated `backend/api/supabase/functions/municipality-rankings/index.ts`:

```typescript
// Changed all references from:
.from("municipality_land_use_summary")
// To:
.from("v_municipality_land_use_summary")

// Changed all references from:
.from("municipality_production_summary")
// To:
.from("v_municipality_production_summary")
```

### 3. Caching Issues

The Next.js API route caches for 7 days. After the fix, we had to:
- Clear browser cache
- Set no-cache headers
- Wait for Vercel CDN cache to expire

## Results

✅ All 10 municipality ranking categories now working:

1. Størst landbrugsareal
2. Højest økologisk andel
3. Størst dyreproduktion
4. Højest pesticidbelastning
5. Højest PFAS-pesticid belastning
6. Højest glyfosat belastning
7. Højest antibiotikaforbrug
8. Højest kvælstofudledning
9. Flest arbejdsulykker
10. Flest hændelser

## Key Learnings

1. **Always check what PostgREST actually sees** using `information_schema` as the `anon` role
2. **Materialized views are invisible to PostgREST** - wrap them in regular views
3. **information_schema vs pg_catalog** - they can show different things!
4. **Cache layers everywhere** - Database cache, PostgREST cache, Next.js cache, CDN cache, Browser cache
5. **Test the Edge Function directly** first before debugging Next.js/frontend

## Migration Required

A migration file should be created to persist this fix:

```sql
-- Migration: Add PostgREST-visible views for municipality summaries
CREATE VIEW v_municipality_land_use_summary AS SELECT * FROM municipality_land_use_summary;
CREATE VIEW v_municipality_production_summary AS SELECT * FROM municipality_production_summary;

GRANT SELECT ON v_municipality_land_use_summary TO anon, authenticated, service_role;
GRANT SELECT ON v_municipality_production_summary TO anon, authenticated, service_role;
```

## Date

October 1, 2025

