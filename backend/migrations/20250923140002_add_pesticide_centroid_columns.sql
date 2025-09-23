-- Migration: Add missing centroid columns to pesticide view
-- Date: 2025-09-23
-- Fixes: "column pesticide_applications_with_field_details.centroid_lat does not exist"
-- Priority: CRITICAL - Fixes environment-pesticide-risks component

-- Recreate view with centroid calculations
CREATE OR REPLACE VIEW pesticide_applications_with_field_details AS
SELECT
    pa.*,
    fb.geom as field_geometry,
    -- Add missing centroid columns
    ST_Y(ST_Centroid(fb.geom)) as centroid_lat,
    ST_X(ST_Centroid(fb.geom)) as centroid_lng
FROM pesticide_applications pa
LEFT JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid;

-- Grant permissions
GRANT SELECT ON pesticide_applications_with_field_details TO anon, authenticated;

-- Add comment
COMMENT ON VIEW pesticide_applications_with_field_details IS 'Pesticide applications with field details and centroid coordinates';
