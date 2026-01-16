-- Migration: Add skraafoto_url computed column to pesticide view
-- Date: 2025-09-23
-- Fixes: "column pesticide_applications_with_field_details.skraafoto_url does not exist"
-- Priority: CRITICAL - Completes pesticide risks component with aerial photo links

-- Drop and recreate view with skraafoto_url computed column
DROP VIEW IF EXISTS pesticide_applications_with_field_details CASCADE;

CREATE VIEW pesticide_applications_with_field_details AS
SELECT
    pa.*,
    fb.municipality as field_municipality_from_boundaries,
    fb.area_ha as field_area_ha,
    fb.geom as field_geometry,
    -- Add centroid columns
    ST_Y(ST_Centroid(fb.geom)) as centroid_lat,
    ST_X(ST_Centroid(fb.geom)) as centroid_lng,
    -- Add computed skraafoto URL using UTM coordinates
    CASE
        WHEN fb.geom IS NOT NULL THEN
            'https://skraafoto.dataforsyningen.dk/?center=' ||
            ROUND(ST_X(ST_Transform(ST_Centroid(fb.geom), 25832)))::text ||
            '%2C' ||
            ROUND(ST_Y(ST_Transform(ST_Centroid(fb.geom), 25832)))::text
        ELSE NULL
    END as skraafoto_url
FROM pesticide_applications pa
LEFT JOIN field_boundaries fb ON pa.field_uuid = fb.field_uuid;

-- Grant permissions
GRANT SELECT ON pesticide_applications_with_field_details TO anon, authenticated;

-- Add comment
COMMENT ON VIEW pesticide_applications_with_field_details IS 'Pesticide applications with field details, centroid coordinates, and computed aerial photo URLs from Dataforsyningen';
