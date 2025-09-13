-- Migration: Add field-level municipality support
-- Date: September 13, 2025
-- Purpose: Enable field-level municipality assignment from FVM silver pipeline

-- Add municipality column to field_yearly_data table
ALTER TABLE "public"."field_yearly_data"
ADD COLUMN IF NOT EXISTS "municipality" character varying(100);

-- Add index for performance on municipality queries
CREATE INDEX IF NOT EXISTS "idx_field_yearly_data_municipality"
ON "public"."field_yearly_data"("municipality");

-- Add index for performance on municipality + year queries
CREATE INDEX IF NOT EXISTS "idx_field_yearly_data_municipality_year"
ON "public"."field_yearly_data"("municipality", "year");

-- Update land_use_summary view to use field-level municipality
DROP MATERIALIZED VIEW IF EXISTS "public"."land_use_summary";

CREATE MATERIALIZED VIEW "public"."land_use_summary" AS
WITH field_municipality_aggregates AS (
  SELECT
    fyd.company_id,
    COALESCE(fyd.municipality, c.municipality) as municipality,
    fyd.year,
    SUM(fyd.area_ha) as total_area_ha,
    SUM(CASE WHEN fyd.is_organic = true THEN fyd.area_ha ELSE 0 END) as organic_area_ha,
    COUNT(DISTINCT fyd.field_uuid) as total_fields,
    COUNT(DISTINCT CASE WHEN fyd.is_organic = true THEN fyd.field_uuid END) as organic_fields
  FROM field_yearly_data fyd
  LEFT JOIN companies c ON fyd.company_id = c.id
  WHERE COALESCE(fyd.municipality, c.municipality) IS NOT NULL
  GROUP BY fyd.company_id, COALESCE(fyd.municipality, c.municipality), fyd.year
),
municipality_totals AS (
  SELECT
    municipality,
    year,
    SUM(total_area_ha) as municipality_total_area_ha,
    SUM(organic_area_ha) as municipality_organic_area_ha,
    SUM(total_fields) as municipality_total_fields,
    SUM(organic_fields) as municipality_organic_fields
  FROM field_municipality_aggregates
  GROUP BY municipality, year
)
SELECT
  fma.company_id,
  fma.municipality,
  fma.year,
  fma.total_area_ha,
  fma.organic_area_ha,
  fma.total_fields,
  fma.organic_fields,
  CASE
    WHEN fma.total_area_ha > 0
    THEN ROUND((fma.organic_area_ha / fma.total_area_ha * 100)::numeric, 2)
    ELSE 0
  END as organic_percentage,
  mt.municipality_total_area_ha,
  mt.municipality_organic_area_ha,
  mt.municipality_total_fields,
  mt.municipality_organic_fields,
  CASE
    WHEN mt.municipality_total_area_ha > 0
    THEN ROUND((fma.total_area_ha / mt.municipality_total_area_ha * 100)::numeric, 2)
    ELSE 0
  END as company_share_of_municipality_area,
  now() as created_at,
  now() as updated_at
FROM field_municipality_aggregates fma
JOIN municipality_totals mt ON fma.municipality = mt.municipality AND fma.year = mt.year
WITH NO DATA;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS "idx_land_use_summary_municipality_year"
ON "public"."land_use_summary"("municipality", "year");

CREATE INDEX IF NOT EXISTS "idx_land_use_summary_company_year"
ON "public"."land_use_summary"("company_id", "year");

-- Add comment explaining the new field-level municipality approach
COMMENT ON MATERIALIZED VIEW "public"."land_use_summary" IS
'Land use summary with field-level municipality assignment. Uses field_yearly_data.municipality when available, falls back to companies.municipality. Updated in migration 20250913000000 to support FVM silver pipeline municipality assignment.';

-- Refresh the materialized view to populate with current data
REFRESH MATERIALIZED VIEW "public"."land_use_summary";
