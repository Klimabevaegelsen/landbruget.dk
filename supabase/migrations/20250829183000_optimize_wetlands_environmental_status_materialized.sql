-- Migration: Optimize Wetlands Environmental Status with Materialized View
-- Date: 2025-08-29 18:30:00
-- Description: Convert wetlands_environmental_status to materialized view for better performance
--              Fixes 500 error caused by 7.6 second query execution time

-- Drop existing view
DROP VIEW IF EXISTS wetlands_environmental_status;

-- Create materialized view for much better performance
CREATE MATERIALIZED VIEW wetlands_environmental_status AS
WITH yearly_wetlands_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        fwa.year,

        -- Areas with NO water coverage = Action Required
        COALESCE(SUM(CASE WHEN fwa.wetlands_status = 'present' AND (fwa.water_covered_hectares IS NULL OR fwa.water_covered_hectares = 0)
                         THEN fwa.area_ha ELSE 0 END), 0) AS action_required_hectares,

        -- Areas WITH water coverage = Completed/Restored
        COALESCE(SUM(CASE WHEN fwa.wetlands_status = 'present' AND fwa.water_covered_hectares > 0
                         THEN fwa.water_covered_hectares ELSE 0 END), 0) AS completed_hectares,

        -- Total water covered hectares (same as completed)
        COALESCE(SUM(fwa.water_covered_hectares), 0) AS water_covered_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid
    WHERE fwa.year IS NOT NULL
    GROUP BY c.id, c.municipality, fwa.year
)
SELECT
    company_id,
    municipality,
    year,
    action_required_hectares,
    completed_hectares,
    water_covered_hectares,

    -- Protection rate: completed / (action_required + completed)
    CASE
        WHEN (action_required_hectares + completed_hectares) > 0
        THEN ROUND(completed_hectares * 100.0 / (action_required_hectares + completed_hectares), 2)
        ELSE 100.0
    END AS protection_rate

FROM yearly_wetlands_data
WHERE year >= 2020 AND (action_required_hectares > 0 OR completed_hectares > 0);

-- Create indexes for common query patterns
CREATE INDEX idx_wetlands_environmental_status_year_water
ON wetlands_environmental_status (year, water_covered_hectares DESC);

CREATE INDEX idx_wetlands_environmental_status_year_action
ON wetlands_environmental_status (year, action_required_hectares DESC);

CREATE INDEX idx_wetlands_environmental_status_company_year
ON wetlands_environmental_status (company_id, year);

-- Add comment explaining the optimization
COMMENT ON MATERIALIZED VIEW wetlands_environmental_status IS 'Materialized view for wetlands environmental status. Optimized for performance to prevent API timeouts. Water coverage = restored areas, no water coverage = action required areas. Refresh periodically with REFRESH MATERIALIZED VIEW.';

-- Initial refresh to populate the materialized view
REFRESH MATERIALIZED VIEW wetlands_environmental_status;
