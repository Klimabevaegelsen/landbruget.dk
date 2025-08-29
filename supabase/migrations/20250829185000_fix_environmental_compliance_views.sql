-- Migration: Fix Environmental Compliance Views with Correct Column Names
-- Date: 2025-08-29 18:50:00
-- Description: Fix environmental compliance views with correct column references
--              Fixes the missing views causing 500 errors in company pages

-- Create environmental_compliance_summary view with correct column names
CREATE OR REPLACE VIEW environmental_compliance_summary AS
WITH company_environmental_data AS (
    SELECT
        c.id AS company_id,
        c.municipality,
        2025 AS year, -- Use current year instead of EXTRACT for consistency

        -- BNBO problematic areas (action required)
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Action Required', 'Gennemgået, indsats nødvendig', 'Ikke gennemgået (default værdi)')
                         THEN fba.area_ha ELSE 0 END), 0) AS bnbo_problematic_hectares,

        -- BNBO dealt with areas (completed)
        COALESCE(SUM(CASE WHEN fba.bnbo_status IN ('Completed', 'Gennemgået, indsats ikke nødvendig', 'Indsats gennemført', 'Ingen erhvervsmæssig anvendelse af pesticider')
                         THEN fba.area_ha ELSE 0 END), 0) AS bnbo_dealt_with_hectares,

        -- BNBO miljø- og klimaprojekt covered areas
        COALESCE(SUM(fba.water_covered_hectares), 0) AS bnbo_water_covered_hectares,

        -- Wetlands problematic areas (use wetlands_status instead of status)
        COALESCE(SUM(CASE WHEN fwa.wetlands_status = 'present' AND (fwa.water_covered_hectares IS NULL OR fwa.water_covered_hectares = 0)
                         THEN fwa.area_ha ELSE 0 END), 0) AS wetlands_problematic_hectares,

        -- Wetlands dealt with areas (water covered areas)
        COALESCE(SUM(CASE WHEN fwa.wetlands_status = 'present' AND fwa.water_covered_hectares > 0
                         THEN fwa.water_covered_hectares ELSE 0 END), 0) AS wetlands_dealt_with_hectares,

        -- Wetlands miljø- og klimaprojekt covered areas
        COALESCE(SUM(CASE WHEN fwa.wetlands_status = 'present' THEN fwa.water_covered_hectares ELSE 0 END), 0) AS wetlands_water_covered_hectares

    FROM companies c
    LEFT JOIN field_boundaries fb ON c.id = fb.company_id
    LEFT JOIN field_bnbo_areas fba ON fb.field_uuid = fba.field_uuid AND fba.year = 2025
    LEFT JOIN field_wetland_areas fwa ON fb.field_uuid = fwa.field_uuid AND fwa.year = 2025
    GROUP BY c.id, c.municipality
)
SELECT
    company_id,
    municipality,
    year,

    -- Total problematic areas
    (bnbo_problematic_hectares + wetlands_problematic_hectares) AS total_problematic_hectares,

    -- Total dealt with areas
    (bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) AS total_dealt_with_hectares,

    -- Total water covered areas
    (bnbo_water_covered_hectares + wetlands_water_covered_hectares) AS total_water_covered_hectares,

    -- Compliance percentage
    CASE
        WHEN (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) > 0
        THEN ROUND((bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) * 100.0 /
                   (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares), 2)
        ELSE 100.0
    END AS compliance_percentage,

    -- Water coverage percentage
    CASE
        WHEN (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) > 0
        THEN ROUND((bnbo_water_covered_hectares + wetlands_water_covered_hectares) * 100.0 /
                   (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares), 2)
        ELSE 0.0
    END AS water_coverage_percentage,

    -- Individual components for detailed analysis
    bnbo_problematic_hectares,
    bnbo_dealt_with_hectares,
    bnbo_water_covered_hectares,
    wetlands_problematic_hectares,
    wetlands_dealt_with_hectares,
    wetlands_water_covered_hectares

FROM company_environmental_data
WHERE (bnbo_problematic_hectares + wetlands_problematic_hectares + bnbo_dealt_with_hectares + wetlands_dealt_with_hectares) > 0;

-- Create simplified environment_summary view
CREATE OR REPLACE VIEW environment_summary AS
SELECT
    c.id AS company_id,
    c.municipality,
    2025 AS year,

    -- Simplified metrics based on available data
    COALESCE(SUM(pa.treated_area_ha * pa.load_index), 0) AS total_pesticide_load_index,
    CASE
        WHEN SUM(fyd.area_ha) > 0
        THEN ROUND(COALESCE(SUM(pa.treated_area_ha * pa.load_index), 0) / SUM(fyd.area_ha), 2)
        ELSE 0
    END AS pesticide_load_index_per_ha,

    -- Placeholder values for missing data
    0 AS total_fertiliser_kg,
    0 AS total_n_leached_kg,
    0 AS n_leached_kg_per_ha,

    -- Rankings (placeholder)
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
GRANT SELECT ON environmental_compliance_summary TO anon, authenticated;
GRANT SELECT ON environment_summary TO anon, authenticated;
GRANT SELECT ON environmental_compliance_ranking TO anon, authenticated;

-- Add comments
COMMENT ON VIEW environmental_compliance_summary IS 'Environmental compliance summary with corrected column references for wetlands_status';
COMMENT ON VIEW environment_summary IS 'Simplified environment summary with available data and placeholder values';
COMMENT ON VIEW environmental_compliance_ranking IS 'Environmental compliance rankings based on compliance summary';

-- Notify PostgREST to reload schema
NOTIFY pgrst, 'reload schema';
