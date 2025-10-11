-- Migration: Add missing municipality column to land_use_summary
-- Date: 2025-09-23
-- Fixes: "Database error: column land_use_summary.municipality does not exist"
-- Priority: CRITICAL - Completes the land-use-kpis component fix

-- Step 1: Add municipality column
ALTER TABLE land_use_summary
ADD COLUMN IF NOT EXISTS municipality VARCHAR;

-- Step 2: Populate municipality column by joining with companies table
UPDATE land_use_summary
SET municipality = c.municipality
FROM companies c
WHERE land_use_summary.company_id = c.id
  AND land_use_summary.municipality IS NULL;

-- Step 3: Add index for performance
CREATE INDEX IF NOT EXISTS idx_land_use_summary_municipality
  ON land_use_summary (municipality);

-- Step 4: Update the municipality ranking calculation to use the new column
-- (This should now work correctly since we have the municipality column)

-- Add comment
COMMENT ON COLUMN land_use_summary.municipality IS 'Municipality name from companies table for regional rankings';
