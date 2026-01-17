-- Migration: Fix pesticide applications view to use correct source table
-- Date: 2025-09-23
-- Fixes: Root cause of 0 pesticide data - view was using wrong table
-- CRITICAL: pesticide_applications (0 records) vs company_pesticide_applications (4.8M records)

-- Drop the incorrect view
DROP VIEW IF EXISTS pesticide_applications_with_field_details CASCADE;

-- Create corrected view using company_pesticide_applications (the table with actual data)
CREATE VIEW pesticide_applications_with_field_details AS
SELECT
    cpa.id,
    fb.field_uuid,
    cpa.application_year as year,
    NULL::date as application_date,  -- Not available in company_pesticide_applications
    cpa.pesticide_name as product_name,
    cpa.registration_number,
    (cpa.dosage_quantity)::numeric as application_rate_kg_per_ha,
    cpa.treated_area_ha,
    NULL::text as risk_category,  -- Will need to be computed/joined from other tables
    NULL::text as risk_details,   -- Will need to be computed/joined from other tables
    NULL::boolean as contains_pfas,     -- Will need to be computed
    NULL::boolean as contains_diquat,   -- Will need to be computed
    NULL::boolean as contains_glyphosate, -- Will need to be computed
    NULL::numeric as health_burden_score, -- Will need to be computed
    NULL::numeric as total_burden_score,  -- Will need to be computed
    NULL::text as product_status,         -- Will need to be joined
    NULL::date as approval_date,          -- Will need to be joined
    NULL::date as expiry_date,           -- Will need to be joined
    NULL::numeric as proximity_housing_m,  -- Will need to be computed
    NULL::numeric as proximity_school_m,   -- Will need to be computed
    NULL::numeric as proximity_water_m,    -- Will need to be computed
    NOW() as created_at,
    NOW() as updated_at,
    cpa.company_id,
    cpa.pesticide_name,
    fb.municipality,
    1 as municipality_assignment_method,
    fb.municipality as field_municipality_from_boundaries,
    fb.area_ha as field_area_ha,
    fb.geom as field_geometry,
    -- Add centroid columns (our previous fix)
    ST_Y(ST_Centroid(fb.geom)) as centroid_lat,
    ST_X(ST_Centroid(fb.geom)) as centroid_lng,
    -- Add computed skraafoto URL (our previous fix)
    CASE
        WHEN fb.geom IS NOT NULL THEN
            'https://skraafoto.dataforsyningen.dk/?center=' ||
            ROUND(ST_X(ST_Transform(ST_Centroid(fb.geom), 25832)))::text ||
            '%2C' ||
            ROUND(ST_Y(ST_Transform(ST_Centroid(fb.geom), 25832)))::text
        ELSE NULL
    END as skraafoto_url
FROM company_pesticide_applications cpa
LEFT JOIN field_boundaries fb ON cpa.company_id = fb.company_id
WHERE cpa.treated_area_ha > 0  -- Only include records with actual treated area
  AND fb.geom IS NOT NULL;     -- Only include records with field geometry

-- Grant permissions
GRANT SELECT ON pesticide_applications_with_field_details TO anon, authenticated;

-- Add comment
COMMENT ON VIEW pesticide_applications_with_field_details IS
'Pesticide applications with field details - CORRECTED to use company_pesticide_applications (4.8M records) instead of empty pesticide_applications table';

-- Add index on the underlying table for performance
CREATE INDEX IF NOT EXISTS idx_company_pesticide_applications_company_id
ON company_pesticide_applications(company_id);

CREATE INDEX IF NOT EXISTS idx_company_pesticide_applications_year
ON company_pesticide_applications(application_year);
