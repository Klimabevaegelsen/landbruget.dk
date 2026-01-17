-- Create enhanced view for pesticide applications with field details and spatial information
-- Migration: 20250830083522_create_pesticide_applications_with_field_details_view.sql

-- Drop the view if it exists
DROP VIEW IF EXISTS pesticide_applications_with_field_details;

-- Create the enhanced view with field details, centroids, and Skraafoto links
CREATE VIEW pesticide_applications_with_field_details AS
SELECT
    pa.id,
    pa.year,
    pa.application_date,

    -- Field identification and details
    pa.field_uuid,
    fb.field_identifier,
    fb.field_name,
    fb.area_ha as field_area_ha,
    fb.cvr_number,
    fb.block_id,
    fb.field_id,
    fb.company_id,  -- Required by API for company filtering

    -- Pesticide application details
    pa.product_name,
    pa.registration_number,
    pa.application_rate_kg_per_ha,
    pa.treated_area_ha,

    -- Risk and chemical information
    pa.risk_category,
    pa.risk_details,
    pa.contains_pfas,
    pa.contains_diquat,
    pa.contains_glyphosate,
    pa.health_burden_score,
    pa.total_burden_score,

    -- Proximity analysis
    pa.proximity_housing_m,
    pa.proximity_school_m,
    pa.proximity_water_m,

    -- Spatial information - centroid coordinates
    ST_Y(ST_Centroid(fb.geom)) as centroid_lat,
    ST_X(ST_Centroid(fb.geom)) as centroid_lng,

    -- Generate Skraafoto URL for aerial imagery
    CONCAT(
        'https://skraafoto.kortforsyningen.dk/?x=',
        ROUND(ST_X(ST_Centroid(fb.geom))::numeric, 6),
        '&y=',
        ROUND(ST_Y(ST_Centroid(fb.geom))::numeric, 6),
        '&zoom=17'
    ) as skraafoto_url,

    -- Metadata
    pa.created_at,
    pa.updated_at

FROM pesticide_applications pa
INNER JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid
WHERE pa.year IS NOT NULL
  AND pa.product_name IS NOT NULL
  AND fb.geom IS NOT NULL;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_pesticide_field_details_year ON pesticide_applications(year);
CREATE INDEX IF NOT EXISTS idx_pesticide_field_details_field_uuid ON pesticide_applications(field_uuid);
CREATE INDEX IF NOT EXISTS idx_field_boundaries_spatial ON field_boundaries USING GIST(geom);

-- Grant permissions
GRANT SELECT ON pesticide_applications_with_field_details TO authenticated;
GRANT SELECT ON pesticide_applications_with_field_details TO anon;

-- Add comment
COMMENT ON VIEW pesticide_applications_with_field_details IS
'Enhanced pesticide applications view with field details, spatial centroids, and Skraafoto links for aerial imagery visualization';
