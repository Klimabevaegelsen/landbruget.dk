-- Migration: Add missing ranking columns to land_use_summary
-- Date: 2025-09-23
-- Fixes: "Database error: column land_use_summary.rank_dk_total_area does not exist"
-- Priority: CRITICAL - Fixes land-use-kpis component immediately

-- Step 1: Add missing ranking columns
ALTER TABLE land_use_summary
ADD COLUMN IF NOT EXISTS rank_dk_total_area INTEGER,
ADD COLUMN IF NOT EXISTS rank_municipality_total_area INTEGER,
ADD COLUMN IF NOT EXISTS rank_dk_organic_total_area INTEGER,
ADD COLUMN IF NOT EXISTS rank_municipality_organic_total_area INTEGER;

-- Step 2: Calculate and populate rankings
WITH ranked_data AS (
  SELECT
    l.company_id,
    l.year,
    RANK() OVER (PARTITION BY l.year ORDER BY l.total_area_ha DESC NULLS LAST) as rank_dk_total,
    RANK() OVER (PARTITION BY l.year, c.municipality ORDER BY l.total_area_ha DESC NULLS LAST) as rank_mun_total,
    RANK() OVER (PARTITION BY l.year ORDER BY l.organic_area_ha DESC NULLS LAST) as rank_dk_organic,
    RANK() OVER (PARTITION BY l.year, c.municipality ORDER BY l.organic_area_ha DESC NULLS LAST) as rank_mun_organic
  FROM land_use_summary l
  JOIN companies c ON l.company_id = c.id
  WHERE l.total_area_ha IS NOT NULL
)
UPDATE land_use_summary
SET
  rank_dk_total_area = r.rank_dk_total,
  rank_municipality_total_area = r.rank_mun_total,
  rank_dk_organic_total_area = r.rank_dk_organic,
  rank_municipality_organic_total_area = r.rank_mun_organic
FROM ranked_data r
WHERE land_use_summary.company_id = r.company_id
  AND land_use_summary.year = r.year;

-- Step 3: Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_land_use_summary_rank_dk_total
  ON land_use_summary (rank_dk_total_area);
CREATE INDEX IF NOT EXISTS idx_land_use_summary_rank_mun_total
  ON land_use_summary (municipality, rank_municipality_total_area);

-- Add comment
COMMENT ON COLUMN land_use_summary.rank_dk_total_area IS 'National ranking by total area (descending)';
COMMENT ON COLUMN land_use_summary.rank_municipality_total_area IS 'Municipality ranking by total area (descending)';
COMMENT ON COLUMN land_use_summary.rank_dk_organic_total_area IS 'National ranking by organic area (descending)';
COMMENT ON COLUMN land_use_summary.rank_municipality_organic_total_area IS 'Municipality ranking by organic area (descending)';
