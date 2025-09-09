-- Migration: Create Simple Environmental Views (Placeholders)
-- Date: 2025-08-29 19:00:00
-- Description: Create simple placeholder views to prevent 500 errors in company pages
--              These views provide basic structure with available data

-- Drop any existing views first
DROP VIEW IF EXISTS environment_summary CASCADE;
DROP VIEW IF EXISTS environmental_compliance_ranking CASCADE;

-- Create simple environment_summary view with available columns only
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

    -- Placeholder values for missing data
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

-- Create environmental_compliance_ranking view
CREATE OR REPLACE VIEW environmental_compliance_ranking AS
SELECT
    company_id,
    municipality,
    year,
    total_problematic_hectares AS total_non_compliant_hectares,

    -- Simple risk score based on problematic areas
    CASE
        WHEN total_problematic_hectares > 100 THEN 'High'
        WHEN total_problematic_hectares > 10 THEN 'Medium'
        ELSE 'Low'
    END AS environmental_risk_score,

    -- Rankings (simplified)
    ROW_NUMBER() OVER (ORDER BY total_problematic_hectares DESC) AS rank_dk_compliance,
    ROW_NUMBER() OVER (PARTITION BY municipality ORDER BY total_problematic_hectares DESC) AS rank_municipality_compliance,
    ROW_NUMBER() OVER (ORDER BY total_water_covered_hectares DESC) AS rank_dk_water_coverage,
    ROW_NUMBER() OVER (PARTITION BY municipality ORDER BY total_water_covered_hectares DESC) AS rank_municipality_water_coverage

FROM environmental_compliance_summary;

-- Grant permissions
GRANT SELECT ON environment_summary TO anon, authenticated;
GRANT SELECT ON environmental_compliance_ranking TO anon, authenticated;

-- Add comments
COMMENT ON VIEW environment_summary IS 'Simplified environment summary with available data and placeholder values to prevent 500 errors';
COMMENT ON VIEW environmental_compliance_ranking IS 'Environmental compliance rankings based on compliance summary to prevent 500 errors';

-- Record migration
INSERT INTO supabase_migrations.schema_migrations (version, name)
VALUES ('20250829190000', 'create_simple_environmental_views')
ON CONFLICT (version) DO NOTHING;

-- Notify PostgREST to reload schema
NOTIFY pgrst, 'reload schema';
