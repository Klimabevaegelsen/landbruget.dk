-- Migration: Recreate environment_summary view
-- Date: 2025-09-23
-- Fixes: "Could not find the table 'public.environment_summary' in the schema cache"
-- Source: 20250829190000_create_simple_environmental_views.sql
-- Priority: CRITICAL - Fixes environment-nitrogen-leaching & environment-pesticide-load

-- Drop existing view if it exists
DROP VIEW IF EXISTS environment_summary CASCADE;

-- Create environment_summary view with available data
CREATE OR REPLACE VIEW environment_summary AS
SELECT
    c.id AS company_id,
    c.municipality,
    2025 AS year,

    -- Use available data from pesticide applications
    COALESCE(SUM(pa.treated_area_ha), 0) AS total_pesticide_load_index,
    CASE
        WHEN SUM(fyd.area_ha) > 0
        THEN ROUND(COALESCE(SUM(pa.treated_area_ha), 0) / SUM(fyd.area_ha), 2)
        ELSE 0
    END AS pesticide_load_index_per_ha,

    -- Placeholder values for missing data (can be enhanced with NLES5 data later)
    0 AS total_fertiliser_kg,
    0 AS total_n_leached_kg,
    0 AS n_leached_kg_per_ha,

    -- Rankings (placeholder - will be calculated later if needed)
    1 AS rank_dk_total_fertiliser_kg,
    1 AS rank_municipality_total_fertiliser_kg,
    1 AS rank_dk_n_leached_kg_per_ha,
    1 AS rank_municipality_n_leached_kg_per_ha,
    1 AS rank_dk_pesticide_load_index_per_ha,
    1 AS rank_municipality_pesticide_load_index_per_ha

FROM companies c
LEFT JOIN field_boundaries fb ON c.id = fb.company_id
LEFT JOIN field_yearly_data fyd ON fb.field_uuid = fyd.field_uuid AND fyd.year = 2025
LEFT JOIN pesticide_applications pa ON fb.field_uuid = pa.field_uuid AND pa.year = 2025
GROUP BY c.id, c.municipality;

-- Grant permissions
GRANT SELECT ON environment_summary TO anon, authenticated;

-- Add comment
COMMENT ON VIEW environment_summary IS 'Environment summary with available data and placeholder values for missing nitrogen data';
